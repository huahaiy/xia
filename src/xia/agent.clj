(ns xia.agent
  "Agent loop — the core runtime that processes user messages.

   Loop: user message → update working memory → build context
         → LLM call (with tools) → tool calls? → response

   Skills = markdown instructions injected into the system prompt.
   Tools  = executable functions the LLM can call via function-calling."
  (:require [taoensso.timbre :as log]
            [charred.api :as json]
            [clojure.string :as str]
            [xia.config :as cfg]
            [xia.context :as context]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.prompt :as prompt]
            [xia.schedule :as schedule]
            [xia.ssrf :as ssrf]
            [xia.tool :as tool]
            [xia.working-memory :as wm]))

(def ^:private default-max-tool-rounds 10)
(def ^:private default-max-tool-calls-per-round 12)
(def ^:private default-max-user-message-chars 32768)
(def ^:private default-max-user-message-tokens 8000)
(def ^:private default-max-branch-tasks 5)
(def ^:private default-max-parallel-branches 3)
(def ^:private default-max-branch-tool-rounds 2)
(def ^:private default-parallel-tool-timeout-ms 30000)
(def ^:private default-branch-task-timeout-ms 300000)
(def ^:private session-turn-lock-count 256)
(defonce ^:private session-turn-locks
  (vec (repeatedly session-turn-lock-count #(Object.))))

;; build-messages is now in xia.context

(defn- session-turn-lock
  [session-id]
  (when session-id
    (nth session-turn-locks
         (mod (bit-and Integer/MAX_VALUE (hash session-id))
              session-turn-lock-count))))

(defn- with-session-turn-lock
  [session-id f]
  (if-let [lock (session-turn-lock session-id)]
    (locking lock
      (f))
    (f)))

(defn- max-user-message-chars
  []
  (cfg/positive-long :agent/max-user-message-chars
                     default-max-user-message-chars))

(defn- max-user-message-tokens
  []
  (cfg/positive-long :agent/max-user-message-tokens
                     default-max-user-message-tokens))

(defn- configured-max-tool-rounds
  []
  (cfg/positive-long :agent/max-tool-rounds
                     default-max-tool-rounds))

(defn- max-branch-tasks
  []
  (cfg/positive-long :agent/max-branch-tasks
                     default-max-branch-tasks))

(defn- max-parallel-branches
  []
  (cfg/positive-long :agent/max-parallel-branches
                     default-max-parallel-branches))

(defn- max-branch-tool-rounds
  []
  (cfg/positive-long :agent/max-branch-tool-rounds
                     default-max-branch-tool-rounds))

(defn- parallel-tool-timeout-ms
  []
  (cfg/positive-long :agent/parallel-tool-timeout-ms
                     default-parallel-tool-timeout-ms))

(defn- branch-task-timeout-ms
  []
  (cfg/positive-long :agent/branch-task-timeout-ms
                     default-branch-task-timeout-ms))

(def ^:private future-timeout-sentinel ::future-timeout)

(defn- cancel-futures!
  [futures]
  (doseq [f futures]
    (future-cancel f)))

(defn- await-futures!
  [futures timeout-ms timeout-ex-fn]
  (let [deadline-ms (+ (System/currentTimeMillis) timeout-ms)]
    (loop [idx 0
           results []]
      (if (= idx (count futures))
        results
        (let [remaining-ms (- deadline-ms (System/currentTimeMillis))
              future       (nth futures idx)
              result       (if (pos? remaining-ms)
                             (deref future remaining-ms future-timeout-sentinel)
                             future-timeout-sentinel)]
          (if (= future-timeout-sentinel result)
            (do
              (cancel-futures! futures)
              (throw (timeout-ex-fn idx timeout-ms)))
            (recur (inc idx) (conj results result))))))))

(defn- validate-user-message!
  [user-message]
  (let [message        (or user-message "")
        char-count     (count message)
        token-estimate (context/estimate-tokens message)
        max-chars      (max-user-message-chars)
        max-tokens     (max-user-message-tokens)]
    (cond
      (> char-count max-chars)
      (throw (ex-info (str "User message too large: "
                           char-count
                           " chars (max "
                           max-chars
                           ")")
                      {:type        :user-message-too-large
                       :status      413
                       :error       "user message too large"
                       :char-count  char-count
                       :max-chars   max-chars}))

      (> token-estimate max-tokens)
      (throw (ex-info (str "User message too large: ~"
                           token-estimate
                           " tokens (max "
                           max-tokens
                           ")")
                      {:type           :user-message-too-large
                       :status         413
                       :error          "user message too large"
                       :token-estimate token-estimate
                       :max-tokens     max-tokens})))))

(defn- call-model
  [messages tools provider-id]
  (cond
    (and provider-id (seq tools))
    (llm/chat-with-tools messages tools :provider-id provider-id)

    provider-id
    (llm/chat-simple messages :provider-id provider-id)

    (seq tools)
    (llm/chat-with-tools messages tools)

    :else
    (llm/chat-simple messages)))

(defn- report-status!
  [message & {:as extra}]
  (prompt/status! (merge {:state :running
                          :message message}
                         extra)))

(defn schedule-fact-utility-review!
  [fact-eids user-message assistant-response]
  (when (and (seq fact-eids)
             (seq assistant-response))
    (future
      (try
        (wm/review-fact-utility! fact-eids user-message assistant-response)
        (catch Exception e
          (log/warn "Failed to review fact utility:" (.getMessage e)))))))

(defn- best-effort-update-working-memory!
  [session-id user-message channel opts]
  (try
    (wm/update-wm! user-message session-id channel opts)
    (catch Exception e
      (log/warn e "Working memory update failed; continuing without refreshed WM"
                {:session-id session-id
                 :channel channel})
      nil)))

(defn- launch-fact-utility-review!
  [fact-eids user-message assistant-response]
  (try
    (schedule-fact-utility-review! fact-eids user-message assistant-response)
    (catch Exception e
      (log/warn e "Failed to schedule fact utility review; continuing without it"
                {:fact-count (count fact-eids)})
      nil)))

(defn- normalize-branch-task
  [task]
  (cond
    (string? task)
    {:task task
     :prompt task}

    (map? task)
    (let [label  (or (:task task)
                     (:title task)
                     (get task "task")
                     (get task "title")
                     (get task "label"))
          prompt (or (:prompt task)
                     (:message task)
                     (get task "prompt")
                     (get task "message")
                     label)]
      {:task   (str (or label prompt "branch task"))
       :prompt (str (or prompt label ""))})

    :else
    {:task   (str task)
     :prompt (str task)}))

(defn- branch-task-prompt
  [{:keys [task prompt]} objective]
  (str "You are a temporary branch worker for the main Xia agent.\n"
       "You are not talking directly to the user. Work independently on the assigned subtask and report back to the parent agent.\n"
       "Rules:\n"
       "- Do not ask the user questions.\n"
       "- Use tools only when they help complete this subtask.\n"
       "- Do not create schedules, request approvals, or perform privileged actions.\n"
       "- Focus only on the assigned subtask.\n"
       "- Return concise, factual results for the parent agent.\n"
       "- End with short sections titled Findings, Evidence, and Open Questions.\n\n"
       (when (and objective (not (str/blank? objective)))
         (str "Parent objective:\n" objective "\n\n"))
       "Assigned subtask:\n"
       task
       "\n\n"
       "What to do:\n"
       prompt))

(defn- branch-result-summary
  [results]
  (let [completed (count (filter #(= "completed" (:status %)) results))
        failed    (count (filter #(= "failed" (:status %)) results))]
    (str "Completed " completed " branch task"
         (when (not= 1 completed) "s")
         (when (pos? failed)
           (str "; " failed " failed")))))

(defn- parse-tool-args
  [func-name args-str]
  (cond
    (nil? args-str)
    {}

    (string? args-str)
    (try
      (json/read-json args-str)
      (catch Exception e
        (log/warn e "Failed to parse tool arguments for" func-name "- using empty args map")
        {}))

    :else
    (do
      (log/warn "Tool arguments for" func-name "were not a JSON string - using empty args map")
      {})))

(def ^:private persisted-tool-result-large-keys
  #{:image_data_url "image_data_url"})

(defn- result-value
  [result & ks]
  (some (fn [k]
          (or (get result k)
              (when (keyword? k)
                (get result (name k)))))
        ks))

(defn- public-vision-image-url?
  [url]
  (cond
    (not (string? url))
    false

    (str/starts-with? (str/lower-case url) "data:image/")
    true

    :else
    (try
      (ssrf/validate-url! url)
      true
      (catch Exception _
        false))))

(defn- tool-result-image-urls
  [result]
  (when (map? result)
    (let [image-urls-value (result-value result :image_urls)]
      (->> (concat
            [(result-value result :image_data_url :image-url)]
            [(result-value result :image_url)]
            (cond
              (nil? image-urls-value) []
              (sequential? image-urls-value) image-urls-value
              :else [image-urls-value]))
         (filter public-vision-image-url?)
         distinct
         vec))))

(defn- tool-result-summary
  [result]
  (let [summary (result-value result :summary)]
    (when (and (string? summary)
               (not (str/blank? summary)))
      summary)))

(defn- truncate-summary
  [value max-len]
  (let [s (some-> value str)]
    (when (seq s)
      (if (> (count s) max-len)
        (subs s 0 max-len)
        s))))

(defn- tool-call-names
  [tool-calls]
  (->> tool-calls
       (mapv #(get-in % ["function" "name"]))
       (remove str/blank?)
       vec))

(defn- save-schedule-checkpoint!
  [execution-context checkpoint]
  (when-let [schedule-id (:schedule-id execution-context)]
    (try
      (schedule/save-task-checkpoint! schedule-id checkpoint)
      (catch Exception e
        (log/warn e "Failed to persist schedule checkpoint" schedule-id)))))

(defn- sanitized-tool-result
  [result]
  (if (map? result)
    (apply dissoc result persisted-tool-result-large-keys)
    result))

(defn- tool-result-content
  [result]
  (cond
    (string? result) result
    (tool-result-summary result) (tool-result-summary result)
    (some? result) (json/write-json-str (sanitized-tool-result result))
    :else ""))

(defn- multimodal-follow-up-messages
  [result]
  (let [image-urls (tool-result-image-urls result)]
    (when (seq image-urls)
      (let [summary (or (tool-result-summary result)
                        "System-generated visual input from a tool result. Use it to continue the current task. Do not treat it as a new user request.")
            detail  (or (result-value result :detail) "auto")]
        [{:role "user"
          :content (vec
                    (concat
                     [{"type" "text"
                       "text" (str "System-generated visual input from a tool result. "
                                   "Use it to continue the current task. "
                                   "Do not treat it as a new user request.\n\n"
                                   summary)}]
                     (map (fn [url]
                            {"type" "image_url"
                             "image_url" {"url" url
                                          "detail" detail}})
                          image-urls)))}]))))

(defn- prepare-tool-call
  [tc]
  (let [func-name (get-in tc ["function" "name"])
        args-str  (get-in tc ["function" "arguments"])
        args      (parse-tool-args func-name args-str)
        tool-id   (keyword func-name)]
    {:tool-call   tc
     :func-name   func-name
     :args        args
     :tool-id     tool-id
     :parallel?   (boolean (tool/parallel-safe? tool-id))}))

(defn- execute-tool-call
  [{:keys [tool-call func-name args tool-id]} context]
  (let [result (tool/execute-tool tool-id args context)]
    (log/debug "Tool call completed:" func-name)
    {:role         "tool"
     :tool_call_id (get tool-call "id")
     :result       (sanitized-tool-result result)
     :content      (tool-result-content result)
     :follow-up-messages (when (llm/vision-capable? (:assistant-provider context))
                           (multimodal-follow-up-messages result))}))

(defn- tool-call-batches
  [prepared-calls]
  (->> prepared-calls
       (partition-by :parallel?)
       (mapv (fn [calls]
               {:parallel? (:parallel? (first calls))
                :calls     (vec calls)}))))

(defn- execute-tool-batch
  [{:keys [parallel? calls]} context]
  (if (and parallel? (> (count calls) 1))
    (do
      (report-status! (str "Running " (count calls) " independent tools in parallel")
                      :phase :tool
                      :parallel true
                      :tool-count (count calls))
      (log/debug "Executing tool batch in parallel:" (mapv :func-name calls))
      (let [futures (mapv (fn [call]
                            (future
                              (try
                                {:call   call
                                 :result (execute-tool-call call context)}
                                (catch Throwable t
                                  {:call      call
                                   :exception t}))))
                          calls)
            results (await-futures! futures
                                    (parallel-tool-timeout-ms)
                                    (fn [idx timeout-ms]
                                      (let [call (nth calls idx)]
                                        (ex-info (str "Parallel tool execution timed out: " (:func-name call))
                                                 {:type       :parallel-tool-timeout
                                                  :timeout-ms timeout-ms
                                                  :tool-id    (:tool-id call)
                                                  :func-name  (:func-name call)}))))
            failures (keep #(when-let [t (:exception %)]
                              (assoc % :throwable t))
                           results)]
        (when-let [{:keys [call throwable]} (first failures)]
          (doseq [{suppressed :throwable} (rest failures)]
            (.addSuppressed ^Throwable throwable ^Throwable suppressed))
          (throw (ex-info (str "Parallel tool execution failed: " (:func-name call))
                          {:tool-id   (:tool-id call)
                           :func-name (:func-name call)
                           :failures  (mapv (fn [{:keys [call throwable]}]
                                              {:tool-id   (:tool-id call)
                                               :func-name (:func-name call)
                                               :message   (.getMessage ^Throwable throwable)})
                                            failures)}
                          throwable)))
        (mapv :result results)))
    (mapv #(execute-tool-call % context) calls)))

(defn- execute-tool-calls
  "Execute tool calls from the LLM response, return tool result messages."
  [tool-calls context]
  (let [tool-count               (count tool-calls)
        max-tool-calls-per-round (cfg/positive-long :agent/max-tool-calls-per-round
                                                    default-max-tool-calls-per-round)]
    (when (> tool-count max-tool-calls-per-round)
      (throw (ex-info (str "Too many tool calls in one round: "
                           tool-count
                           " (max "
                           max-tool-calls-per-round
                           ")")
                      {:tool-count               tool-count
                       :max-tool-calls-per-round max-tool-calls-per-round})))
    (->> tool-calls
         (mapv prepare-tool-call)
         tool-call-batches
         (mapcat #(execute-tool-batch % context))
         vec)))

(defn process-message
  "Process a user message in the given session. Returns the assistant's response.

   1. Updates working memory (retrieval pipeline)
   2. Builds context: system prompt (identity + WM context + skills) + history
  3. Calls the LLM with available tools (function-calling)
  4. If the LLM wants to use tools, executes them and loops
  5. Returns the final text response"
  [session-id user-message & {:keys [channel tool-context provider-id local-doc-ids artifact-ids
                                     max-tool-rounds resource-session-id]
                              :or   {channel :terminal
                                     tool-context {}}}]
  (with-session-turn-lock
    session-id
    (fn []
      (binding [prompt/*interaction-context* {:channel    channel
                                              :session-id session-id}]
        (try
          (validate-user-message! user-message)
          (db/add-message! session-id :user user-message
                           :local-doc-ids local-doc-ids
                           :artifact-ids artifact-ids)
          (report-status! "Updating working memory"
                          :phase :working-memory)
          (best-effort-update-working-memory! session-id
                                             user-message
                                             channel
                                             {:resource-session-id resource-session-id})
          (let [{assistant-provider    :provider
                 assistant-provider-id :provider-id}
                (llm/resolve-provider-selection
                  (cond-> {:workload :assistant}
                    provider-id
                    (assoc :provider-id provider-id)))
                execution-context (merge {:session-id session-id
                                          :channel    channel
                                          :user-message user-message
                                          :resource-session-id resource-session-id
                                          :assistant-provider assistant-provider
                                          :assistant-provider-id assistant-provider-id}
                                         tool-context)
                tools (tool/tool-definitions execution-context)
                {:keys [messages used-fact-eids]}
                (context/build-messages-data session-id
                                             {:provider            assistant-provider
                                              :provider-id         assistant-provider-id
                                              :compaction-workload :history-compaction})]
            (save-schedule-checkpoint!
              execution-context
              {:phase :planning
               :round 0
               :summary "Working memory updated and context prepared."
               :message-count (count messages)
               :session-id session-id})
            (loop [messages messages
                   round    0]
              (when (>= round (or max-tool-rounds (configured-max-tool-rounds)))
                (throw (ex-info "Too many tool-calling rounds" {:rounds round})))
              (report-status! (if (zero? round)
                                "Calling model"
                                "Calling model with tool results")
                              :phase :llm
                              :round round)
              (let [response   (call-model messages tools assistant-provider-id)
                    has-tools? (and (map? response) (seq (get response "tool_calls")))]
                (if has-tools?
                  (let [tool-calls    (get response "tool_calls")
                        assistant-msg {:role       "assistant"
                                       :content    (get response "content" "")
                                       :tool_calls tool-calls}
                        tool-count    (count tool-calls)
                        tool-results  (do
                                        (report-status! (str "Model requested "
                                                             tool-count
                                                             " tool"
                                                             (when (not= 1 tool-count) "s"))
                                                        :phase :tool-plan
                                                        :round round
                                                        :tool-count tool-count)
                                        (execute-tool-calls tool-calls
                                                            execution-context))]
                    (let [tool-history (mapv #(select-keys % [:role :tool_call_id :content])
                                             tool-results)
                          follow-up-messages (->> tool-results
                                                  (mapcat :follow-up-messages)
                                                  vec)]
                    ;; Store assistant message with tool calls
                      (db/add-message! session-id :assistant
                                       (get response "content" "")
                                       :tool-calls tool-calls
                                       :local-doc-ids local-doc-ids
                                       :artifact-ids artifact-ids)
                      ;; Store tool results
                      (doseq [tr tool-results]
                        (db/add-message! session-id :tool
                                         nil
                                         :tool-result (:result tr)
                                         :tool-id (:tool_call_id tr)))
                      (save-schedule-checkpoint!
                        execution-context
                        {:phase :tool
                         :round round
                         :tool-count tool-count
                         :tool-ids (tool-call-names tool-calls)
                         :summary (or (truncate-summary (get response "content" "") 240)
                                      (str "Completed tool round with "
                                           tool-count
                                           " tool call"
                                           (when (not= 1 tool-count) "s")
                                           "."))})
                      ;; Continue with updated messages
                      (recur (-> messages
                                 (conj assistant-msg)
                                 (into tool-history)
                                 (into follow-up-messages))
                             (inc round))))
                  ;; Final text response
                  (let [text (if (string? response) response (get response "content" ""))]
                    (report-status! "Preparing response"
                                    :phase :finalizing)
                    (db/add-message! session-id :assistant text
                                     :local-doc-ids local-doc-ids
                                     :artifact-ids artifact-ids)
                    (launch-fact-utility-review! used-fact-eids user-message text)
                    (prompt/status! {:state :done
                                     :phase :complete
                                     :message "Ready"})
                    text)))))
          (catch Exception e
            (prompt/status! {:state :error
                             :phase :error
                             :message (str "Request failed: " (.getMessage e))})
            (throw e)))))))

(defn- run-branch-task*
  [parent-session-id {:keys [task prompt] :as branch-task}
   {:keys [channel provider-id resource-session-id objective
           max-tool-rounds tool-context]
    :or   {channel :terminal}}]
  (let [child-session-id (db/create-session! :branch
                                             {:parent-session-id parent-session-id
                                              :worker? true
                                              :label task})]
    (try
      (wm/create-wm! child-session-id)
      (let [result (process-message child-session-id
                                    (branch-task-prompt branch-task objective)
                                    :channel :branch
                                    :provider-id provider-id
                                    :resource-session-id (or resource-session-id
                                                             parent-session-id)
                                    :max-tool-rounds max-tool-rounds
                                    :tool-context (merge {:branch-worker? true
                                                          :parent-session-id parent-session-id
                                                          :resource-session-id (or resource-session-id
                                                                                   parent-session-id)}
                                                         tool-context))
            wm-context (wm/wm->context child-session-id)]
        {:task       task
         :status     "completed"
         :session-id child-session-id
         :topics     (:topics wm-context)
         :result     result})
      (catch Throwable t
        {:task       task
         :status     "failed"
         :session-id child-session-id
         :error      (.getMessage t)})
      (finally
        (db/set-session-active! child-session-id false)
        (wm/clear-wm! child-session-id)))))

(defn run-branch-tasks
  "Run independent branch tasks in separate worker sessions with isolated
   working memory and shared long-term memory access. Returns structured
   branch results for the parent agent."
  [tasks & {:keys [session-id channel provider-id resource-session-id objective
                   max-parallel max-tool-rounds tool-context]
            :or   {channel :terminal
                   tool-context {}}}]
  (let [parent-context      prompt/*interaction-context*
        parent-session-id   (or session-id (:session-id parent-context))
        channel*            (or channel (:channel parent-context) :terminal)
        provider-id*        (or provider-id
                                (:assistant-provider-id parent-context))
        resource-session-id* (or resource-session-id parent-session-id)
        branch-tasks        (->> tasks (map normalize-branch-task) (remove #(str/blank? (:prompt %))) vec)
        task-count          (count branch-tasks)
        max-tasks           (max-branch-tasks)
        max-parallel*       (min (max 1 (or max-parallel (max-parallel-branches)))
                                 (max 1 max-tasks))]
    (when (zero? task-count)
      (throw (ex-info "Branch tasks require at least one task" {})))
    (when (> task-count max-tasks)
      (throw (ex-info (str "Too many branch tasks: " task-count " (max " max-tasks ")")
                      {:task-count task-count
                       :max-tasks  max-tasks})))
    (report-status! (str "Running " task-count " branch task"
                         (when (not= 1 task-count) "s"))
                    :phase :branch
                    :branch-count task-count
                    :parallel true)
    (let [results (vec
                    (mapcat (fn [batch]
                              (let [futures (mapv (fn [branch-task]
                                                    (future
                                                      (run-branch-task* parent-session-id
                                                                        branch-task
                                                                        {:channel channel*
                                                                         :provider-id provider-id*
                                                                         :resource-session-id resource-session-id*
                                                                         :objective objective
                                                                         :max-tool-rounds (or max-tool-rounds
                                                                                              (max-branch-tool-rounds))
                                                                         :tool-context tool-context})))
                                                  batch)]
                                (await-futures! futures
                                                (branch-task-timeout-ms)
                                                (fn [idx timeout-ms]
                                                  (let [branch-task (nth batch idx)]
                                                    (ex-info (str "Branch task timed out: "
                                                                  (or (:task branch-task)
                                                                      (:prompt branch-task)
                                                                      "unnamed"))
                                                             {:type       :branch-task-timeout
                                                              :timeout-ms timeout-ms
                                                              :task       (:task branch-task)
                                                              :prompt     (:prompt branch-task)}))))))
                            (partition-all max-parallel* branch-tasks)))]
      {:summary            (branch-result-summary results)
       :parent_session_id  parent-session-id
       :branch_count       task-count
       :completed_count    (count (filter #(= "completed" (:status %)) results))
       :failed_count       (count (filter #(= "failed" (:status %)) results))
       :results            results})))
