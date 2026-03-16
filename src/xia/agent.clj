(ns xia.agent
  "Agent loop — the core runtime that processes user messages.

   Loop: user message → update working memory → build context
         → LLM call (with tools) → tool calls? → response

   Skills = markdown instructions injected into the system prompt.
   Tools  = executable functions the LLM can call via function-calling."
  (:require [taoensso.timbre :as log]
            [charred.api :as json]
            [xia.config :as cfg]
            [xia.context :as context]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.prompt :as prompt]
            [xia.tool :as tool]
            [xia.working-memory :as wm]))

(def ^:private max-tool-rounds 10)
(def ^:private default-max-tool-calls-per-round 12)
(def ^:private default-max-user-message-chars 32768)
(def ^:private default-max-user-message-tokens 8000)
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
     :result       result
     :content      (if (string? result) result (json/write-json-str result))}))

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
      (let [results (->> calls
                         (mapv (fn [call]
                                 (future
                                   (try
                                     {:call   call
                                      :result (execute-tool-call call context)}
                                     (catch Throwable t
                                       {:call      call
                                        :exception t})))))
                         (mapv deref))
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
  [session-id user-message & {:keys [channel tool-context provider-id]
                              :or   {channel :terminal
                                     tool-context {}}}]
  (with-session-turn-lock
    session-id
    (fn []
      (binding [prompt/*interaction-context* {:channel    channel
                                              :session-id session-id}]
        (try
          (validate-user-message! user-message)
          (db/add-message! session-id :user user-message)
          (report-status! "Updating working memory"
                          :phase :working-memory)
          (wm/update-wm! user-message session-id channel)
          (let [{assistant-provider    :provider
                 assistant-provider-id :provider-id}
                (llm/resolve-provider-selection
                  (cond-> {:workload :assistant}
                    provider-id
                    (assoc :provider-id provider-id)))
                execution-context (merge {:session-id session-id
                                          :channel    channel
                                          :user-message user-message}
                                         tool-context)
                tools (tool/tool-definitions execution-context)
                {:keys [messages used-fact-eids]}
                (context/build-messages-data session-id
                                             {:provider            assistant-provider
                                              :provider-id         assistant-provider-id
                                              :compaction-workload :history-compaction})]
            (loop [messages messages
                   round    0]
              (when (>= round max-tool-rounds)
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
                    ;; Store assistant message with tool calls
                    (db/add-message! session-id :assistant
                                     (get response "content" "")
                                     :tool-calls tool-calls)
                    ;; Store tool results
                    (doseq [tr tool-results]
                      (db/add-message! session-id :tool
                                       nil
                                       :tool-result (:result tr)
                                       :tool-id (:tool_call_id tr)))
                    ;; Continue with updated messages
                    (recur (-> messages
                               (conj assistant-msg)
                               (into tool-results))
                           (inc round)))
                  ;; Final text response
                  (let [text (if (string? response) response (get response "content" ""))]
                    (report-status! "Preparing response"
                                    :phase :finalizing)
                    (db/add-message! session-id :assistant text)
                    (schedule-fact-utility-review! used-fact-eids user-message text)
                    (prompt/status! {:state :done
                                     :phase :complete
                                     :message "Ready"})
                    text)))))
          (catch Exception e
            (prompt/status! {:state :error
                             :phase :error
                             :message (str "Request failed: " (.getMessage e))})
            (throw e)))))))
