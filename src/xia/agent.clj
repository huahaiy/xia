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
(def ^:private default-branch-error-stack-frames 12)
(def ^:private session-turn-lock-count 256)
(defonce ^:private session-turn-locks
  (vec (repeatedly session-turn-lock-count #(Object.))))

(def ^:private trace-context-keys
  [:request-id
   :correlation-id
   :parent-request-id
   :session-id
   :resource-session-id
   :parent-session-id
   :schedule-id
   :channel])

;; build-messages is now in xia.context

(defn- session-turn-lock
  [session-id]
  (when session-id
    (nth session-turn-locks
         (mod (bit-and Integer/MAX_VALUE (int (hash session-id)))
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

(defn- configured-max-tool-calls-per-round
  []
  (cfg/positive-long :agent/max-tool-calls-per-round
                     default-max-tool-calls-per-round))

(defn- validate-tool-round-call-count!
  [tool-calls]
  (let [tool-count               (count tool-calls)
        max-tool-calls-per-round (configured-max-tool-calls-per-round)]
    (when (> (long tool-count) (long max-tool-calls-per-round))
      (throw (ex-info (str "Too many tool calls in one round: "
                           tool-count
                           " (max "
                           max-tool-calls-per-round
                           ")")
                      {:tool-count               tool-count
                       :max-tool-calls-per-round max-tool-calls-per-round})))
    tool-count))

(defn- branch-error-stack-frames
  []
  (cfg/positive-long :agent/branch-error-stack-frames
                     default-branch-error-stack-frames))

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

(defn- new-request-id
  []
  (str (random-uuid)))

(defn- trace-context
  [m]
  (select-keys m trace-context-keys))

(defn- derive-request-context
  [session-id channel tool-context]
  (let [parent-context    prompt/*interaction-context*
        request-id        (or (:request-id tool-context)
                              (new-request-id))
        correlation-id    (or (:correlation-id tool-context)
                              (:correlation-id parent-context)
                              (:request-id parent-context)
                              request-id)
        parent-request-id (or (:parent-request-id tool-context)
                              (:request-id parent-context))]
    (cond-> (merge (trace-context parent-context)
                   (trace-context tool-context)
                   {:channel channel
                    :session-id session-id
                    :request-id request-id
                    :correlation-id correlation-id})
      parent-request-id
      (assoc :parent-request-id parent-request-id))))

(defn- summarize-error-value
  [value]
  (cond
    (nil? value) nil
    (or (string? value) (keyword? value) (number? value) (boolean? value)
        (uuid? value))
    value
    :else
    (pr-str value)))

(defn- summarize-error-data
  [data]
  (when (map? data)
    (into {}
          (map (fn [[k v]]
                 [k (summarize-error-value v)]))
          (take 8 data))))

(defn- throwable-detail
  [^Throwable t]
  (let [{:keys [cause via]} (Throwable->map t)
        causes            (into []
                                (map (fn [{:keys [type message data]}]
                                       (cond-> {:class (str type)
                                                :message message}
                                         (seq data)
                                         (assoc :data (summarize-error-data data)))))
                                via)
        root-cause        (last causes)]
    {:message     (or (.getMessage t) cause (str t))
     :class       (.getName (class t))
     :causes      causes
     :root-cause  root-cause
     :stack-trace (into []
                        (map str)
                        (take (branch-error-stack-frames) (.getStackTrace t)))}))

(def ^:private future-timeout-sentinel ::future-timeout)

(defn- cancel-futures!
  [futures]
  (doseq [f futures]
    (future-cancel f)))

(defn- await-futures!
  [futures timeout-ms timeout-ex-fn]
  (let [deadline-ms (+ (long (System/currentTimeMillis)) (long timeout-ms))]
    (loop [idx 0
           results []]
      (if (= idx (count futures))
        results
        (let [remaining-ms (- deadline-ms (long (System/currentTimeMillis)))
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
        char-count     (long (count message))
        token-estimate (long (context/estimate-tokens message))
        max-chars      (long (max-user-message-chars))
        max-tokens     (long (max-user-message-tokens))]
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
  (let [s       (some-> value str)
        max-len (long max-len)]
    (when (seq s)
      (if (> (long (count s)) max-len)
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
      (schedule/save-task-checkpoint! schedule-id
                                      (merge (trace-context execution-context)
                                             checkpoint))
      (catch Exception e
        (log/warn e "Failed to persist schedule checkpoint"
                  (merge {:schedule-id schedule-id}
                         (trace-context execution-context)))))))

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
  [result context]
  (when (or (llm/vision-capable? (:assistant-provider context))
            (llm/vision-capable? (:assistant-provider-id context)))
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
                             image-urls)))}])))))

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
    (log/debug "Tool call completed"
               (merge {:func-name func-name
                       :tool-id tool-id
                       :tool-call-id (get tool-call "id")}
                      (trace-context context)))
    {:role         "tool"
     :tool_call_id (get tool-call "id")
     :result       (sanitized-tool-result result)
     :content      (tool-result-content result)
     :follow-up-messages (multimodal-follow-up-messages result context)}))

(defn- bind-original-tool-call-ids
  [prepared-calls tool-results]
  (let [prepared-count (count prepared-calls)
        result-count   (count tool-results)]
    (when (not= prepared-count result-count)
      (throw (ex-info "Tool execution result count mismatch"
                      {:prepared-count prepared-count
                       :result-count   result-count})))
    (mapv (fn [{:keys [tool-call func-name tool-id]} result]
            (let [original-id  (get tool-call "id")
                  result-id    (:tool_call_id result)
                  effective-id (or original-id result-id)]
              (when (and (some? original-id)
                         (some? result-id)
                         (not= original-id result-id))
                (log/warn "Tool result returned mismatched tool_call_id; using original call id"
                          (merge {:func-name func-name
                                  :tool-id tool-id
                                  :tool-call-id effective-id
                                  :result-tool-call-id result-id}
                                 (trace-context prompt/*interaction-context*))))
              (cond-> result
                effective-id (assoc :tool_call_id effective-id)
                (nil? (:role result)) (assoc :role "tool"))))
          prepared-calls
          tool-results)))

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
      (log/debug "Executing tool batch in parallel"
                 (merge {:tool-count (count calls)
                         :tool-names (mapv :func-name calls)}
                        (trace-context context)))
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
                                                 (merge (trace-context context)
                                                        {:type       :parallel-tool-timeout
                                                         :timeout-ms timeout-ms
                                                         :tool-id    (:tool-id call)
                                                         :func-name  (:func-name call)})))))
            failures (keep #(when-let [t (:exception %)]
                              (assoc % :throwable t))
                           results)]
        (when-let [{:keys [call throwable]} (first failures)]
          (doseq [{suppressed :throwable} (rest failures)]
            (.addSuppressed ^Throwable throwable ^Throwable suppressed))
          (throw (ex-info (str "Parallel tool execution failed: " (:func-name call))
                          (merge (trace-context context)
                                 {:tool-id   (:tool-id call)
                                  :func-name (:func-name call)
                                  :failures  (mapv (fn [{:keys [call throwable]}]
                                                     (merge (trace-context context)
                                                            {:tool-id   (:tool-id call)
                                                             :func-name (:func-name call)
                                                             :message   (.getMessage ^Throwable throwable)}))
                                                   failures)})
                          throwable)))
        (mapv :result results)))
    (mapv #(execute-tool-call % context) calls)))

(defn- execute-tool-calls
  "Execute tool calls from the LLM response, return tool result messages."
  [tool-calls context]
  (validate-tool-round-call-count! tool-calls)
  (let [prepared-calls (mapv prepare-tool-call tool-calls)
        tool-results   (->> prepared-calls
                            tool-call-batches
                            (mapcat #(execute-tool-batch % context))
                            vec)]
    (bind-original-tool-call-ids prepared-calls tool-results)))

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
      (let [request-context (derive-request-context session-id channel tool-context)]
        (binding [prompt/*interaction-context* request-context]
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
                  execution-context (merge tool-context
                                           request-context
                                           {:session-id session-id
                                            :channel    channel
                                            :user-message user-message
                                            :resource-session-id resource-session-id
                                            :assistant-provider assistant-provider
                                            :assistant-provider-id assistant-provider-id})
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
              (let [max-tool-rounds* (long (or max-tool-rounds
                                               (configured-max-tool-rounds)))]
                (loop [messages messages
                       round    0]
                (report-status! (if (zero? round)
                                  "Calling model"
                                  "Calling model with tool results")
                                :phase :llm
                                :round round)
                (let [response   (call-model messages tools assistant-provider-id)
                      has-tools? (and (map? response) (seq (get response "tool_calls")))]
                  (if has-tools?
                    (do
                      (when (>= (long round) max-tool-rounds*)
                        (throw (ex-info "Too many tool-calling rounds"
                                        {:rounds         round
                                         :max-tool-rounds max-tool-rounds*})))
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
                                                              execution-context))
                          tool-history  (mapv #(select-keys % [:role :tool_call_id :content])
                                              tool-results)
                          follow-up-messages (->> tool-results
                                                  (mapcat :follow-up-messages)
                                                  vec)]
                      (db/add-message! session-id :assistant
                                       (get response "content" "")
                                       :tool-calls tool-calls
                                       :local-doc-ids local-doc-ids
                                       :artifact-ids artifact-ids)
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
                      (recur (-> messages
                                 (conj assistant-msg)
                                 (into tool-history)
                                 (into follow-up-messages))
                             (inc round))))
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
                      text))))))
            (catch Exception e
              (prompt/status! {:state :error
                               :phase :error
                               :message (str "Request failed: " (.getMessage e))})
              (throw e))))))))

(defn- run-branch-task*
  [parent-session-id {:keys [task prompt] :as branch-task}
   {:keys [channel provider-id resource-session-id objective
           max-tool-rounds tool-context]
    :or   {channel :terminal}}]
  (let [parent-trace      (trace-context prompt/*interaction-context*)
        branch-request-id (new-request-id)
        branch-trace      (cond-> (merge parent-trace
                                         {:channel :branch
                                          :request-id branch-request-id
                                          :correlation-id (or (:correlation-id parent-trace)
                                                              (:request-id parent-trace)
                                                              branch-request-id)})
                            (:request-id parent-trace)
                            (assoc :parent-request-id (:request-id parent-trace)))
        child-session-id  (db/create-session! :branch
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
                                                         branch-trace
                                                         tool-context))
            wm-context (wm/wm->context child-session-id)]
        (merge branch-trace
               {:task       task
                :status     "completed"
                :session-id child-session-id
                :topics     (:topics wm-context)
                :result     result}))
      (catch Throwable t
        (log/error t "Branch task failed"
                   (merge {:task task
                           :session-id child-session-id
                           :parent-session-id parent-session-id}
                          branch-trace))
        (merge branch-trace
               {:task         task
                :status       "failed"
                :session-id   child-session-id
                :error        (.getMessage t)
                :error-detail (throwable-detail t)}))
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
        max-parallel*       (clojure.core/min (clojure.core/max 1 (long (or max-parallel (max-parallel-branches))))
                                              (clojure.core/max 1 (long max-tasks)))]
    (when (zero? task-count)
      (throw (ex-info "Branch tasks require at least one task" {})))
    (when (> (long task-count) (long max-tasks))
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
                                                             (merge (trace-context parent-context)
                                                                    {:type       :branch-task-timeout
                                                                     :timeout-ms timeout-ms
                                                                     :task       (:task branch-task)
                                                                     :prompt     (:prompt branch-task)})))))))
                            (partition-all max-parallel* branch-tasks)))]
      {:summary            (branch-result-summary results)
       :parent_session_id  parent-session-id
       :request_id         (:request-id parent-context)
       :correlation_id     (or (:correlation-id parent-context)
                               (:request-id parent-context))
       :branch_count       task-count
       :completed_count    (count (filter #(= "completed" (:status %)) results))
       :failed_count       (count (filter #(= "failed" (:status %)) results))
       :results            results})))
