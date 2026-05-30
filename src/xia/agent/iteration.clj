(ns xia.agent.iteration
  "Run one supervised LLM/tool iteration inside an autonomous turn."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.agent.recorder :as recorder]
            [xia.agent.supervisor :as supervisor]
            [xia.agent.tools :as agent-tools]
            [xia.autonomous :as autonomous]
            [xia.context :as context]
            [xia.limits :as limits]
            [xia.llm :as llm]
            [xia.plugin :as plugin]
            [xia.prompt :as prompt]
            [xia.retrieval-state :as retrieval-state]
            [xia.policy :as task-policy]
            [xia.tool :as tool]
            [xia.working-memory :as wm]))

(defn- call-model
  [messages tools provider-id & {:keys [on-delta session-id]}]
  (cond
    (and provider-id (seq tools))
    (llm/chat-message messages :provider-id provider-id :tools tools :session-id session-id :on-delta on-delta)

    provider-id
    (llm/chat-message messages :provider-id provider-id :session-id session-id :on-delta on-delta)

    (seq tools)
    (llm/chat-message messages :tools tools :session-id session-id :on-delta on-delta)

    :else
    (llm/chat-message messages :session-id session-id :on-delta on-delta)))

(defn- llm-preview-text
  [content]
  (let [text (some-> content str str/trim)]
    (when (and (seq text)
               (not (str/includes? text (autonomous/intent-marker-text)))
               (not (str/includes? text (autonomous/control-marker-text))))
      (agent-tools/truncate-summary text (task-policy/llm-status-preview-chars)))))

(defn- make-llm-progress-reporter
  [round emit-event!]
  (let [last-report-ms (volatile! 0)]
    (fn [{:keys [content]}]
      (when-let [preview (llm-preview-text content)]
        (let [now-ms (long (System/currentTimeMillis))
              last-ms (long @last-report-ms)
              interval-ms (long (task-policy/llm-status-update-interval-ms))
              should-report (or (zero? last-ms)
                                (>= (- now-ms last-ms) interval-ms))]
          (when should-report
            (vreset! last-report-ms now-ms)
            (emit-event! {:phase :llm
                          :message "Calling model"
                          :round round
                          :partial-content preview})))))))

(defn- best-effort-update-working-memory!
  [session-id user-message channel opts]
  (when-let [message (some-> user-message str str/trim not-empty)]
    (try
      (wm/update-wm! message session-id channel opts)
      (catch Exception e
        (log/warn e "Working memory update failed; continuing without refreshed WM"
                  {:session-id session-id
                   :channel channel})
        nil))))

(defn- best-effort-refresh-working-memory!
  [session-id user-message channel opts]
  (when-let [message (some-> user-message str str/trim not-empty)]
    (try
      (wm/refresh-wm! message session-id channel opts)
      (catch Exception e
        (log/warn e "Working memory refresh failed; continuing without refreshed WM"
                  {:session-id session-id
                   :channel channel})
        nil))))

(defn- inject-transient-messages
  [messages transient-messages]
  (let [transient* (->> transient-messages
                        (filter map?)
                        vec)
        system-transient (->> transient*
                              (filter #(= "system" (:role %)))
                              vec)
        other-transient (->> transient*
                             (remove #(= "system" (:role %)))
                             vec)
        join-content (fn [parts]
                       (->> parts
                            (keep :content)
                            (map str)
                            (map str/trim)
                            (remove str/blank?)
                            (str/join "\n\n")))]
    (cond
      (empty? transient*)
      messages

      (and (seq system-transient)
           (seq messages)
           (= "system" (:role (first messages))))
      (let [merged-system (assoc (first messages)
                                 :content
                                 (join-content (into [(first messages)]
                                                     system-transient)))]
        (into [merged-system]
              (concat other-transient (rest messages))))

      (seq system-transient)
      (into [{:role "system"
              :content (join-content system-transient)}]
            (concat other-transient messages))

      (empty? messages)
      other-transient

      :else
      (into [(first messages)]
            (concat other-transient (rest messages))))))

(defn- intent-status-fields
  [intent]
  {:intent-focus (some-> intent :focus)
   :intent-agenda-item (some-> intent :agenda-item)
   :intent-plan-step (some-> intent :plan-step)
   :intent-why (some-> intent :why)
   :intent-tool-name (some-> intent :tool-name)
   :intent-tool-args-summary (some-> intent :tool-args-summary)})

(defn- emit-intent-event!
  [emit-event! execution-context parsed-response]
  (when-let [intent (:intent parsed-response)]
    (emit-event! (merge {:phase :intent
                         :message (autonomous/intent-status-line intent)
                         :iteration (:iteration execution-context)
                         :round 0
                         :checkpoint {:phase :intent
                                      :iteration (:iteration execution-context)
                                      :summary (or (:plan-step intent)
                                                   (:agenda-item intent)
                                                   (:focus intent)
                                                   "Prepared the next action.")
                                      :session-id (:session-id execution-context)
                                      :intent-focus (:focus intent)
                                      :intent-agenda-item (:agenda-item intent)
                                      :intent-plan-step (:plan-step intent)
                                      :intent-why (:why intent)
                                      :intent-tool-name (:tool-name intent)
                                      :intent-tool-args-summary (:tool-args-summary intent)}}
                        (intent-status-fields intent)))))

(defn- actionable-agenda-item
  [tip]
  (some (fn [{:keys [item status]}]
          (when (and (some-> item str str/blank? not)
                     (not (contains? #{:completed :skipped} status)))
            item))
        (:agenda tip)))

(defn- synthesize-tool-call-intent
  [session-id execution-context tool-calls]
  (let [autonomy-state (when session-id
                         (wm/autonomy-state session-id))
        tip (some-> autonomy-state autonomous/current-frame)
        tool-names (agent-tools/tool-call-names tool-calls)
        tool-name (when (seq tool-names)
                    (str/join ", " tool-names))
        tool-count (count tool-calls)
        plan-step (cond
                    (= 1 tool-count)
                    (str "Call " (or (first tool-names) "the requested tool"))

                    (pos? tool-count)
                    (str "Call " tool-count " requested tools")

                    :else
                    "Call the requested tool")
        args-summary (some-> (agent-tools/tool-call-summary tool-calls)
                             pr-str
                             (agent-tools/truncate-summary 240))]
    {:focus (or (some-> tip :title str str/trim not-empty)
                (some-> execution-context :user-message str str/trim not-empty)
                "Current task")
     :agenda-item (or (actionable-agenda-item tip)
                      (some-> tip :next-step str str/trim not-empty))
     :plan-step plan-step
     :why "The model requested tool execution for the current task."
     :tool-name tool-name
     :tool-args-summary args-summary}))

(defn- ensure-tool-call-intent
  [session-id execution-context round parsed-response tool-calls]
  (if (and (zero? round)
           (= :missing (:intent-status parsed-response))
           (seq tool-calls))
    (assoc parsed-response
           :intent-status :synthesized
           :intent (synthesize-tool-call-intent session-id
                                                execution-context
                                                tool-calls))
    parsed-response))

(defn- autonomous-protocol-ex
  [session-id execution-context round parsed-response message]
  (ex-info message
           {:type :autonomous-protocol-invalid
            :session-id session-id
            :channel (:channel execution-context)
            :iteration (:iteration execution-context)
            :round round
            :intent-status (:intent-status parsed-response)
            :control-status (:control-status parsed-response)}))

(defn- validate-tool-round-protocol!
  [session-id execution-context round parsed-response]
  (when (and (zero? round)
             (= :malformed (:intent-status parsed-response)))
    (throw (autonomous-protocol-ex
            session-id
            execution-context
            round
            parsed-response
            "First tool-calling response has a malformed ACTION_INTENT_JSON envelope")))
  (when (contains? #{:parsed :malformed} (:control-status parsed-response))
    (throw (autonomous-protocol-ex
            session-id
            execution-context
            round
            parsed-response
            "Tool-calling response must not include AUTONOMOUS_STATUS_JSON"))))

(defn- explicit-fact-ref->eid
  [used-fact-refs]
  (into {}
        (keep (fn [{:keys [eid ref]}]
                (when (and eid ref)
                  [(-> ref str str/trim str/upper-case) eid])))
        used-fact-refs))

(defn- explicit-used-fact-eids
  [used-fact-refs parsed-response]
  (let [fact-refs (explicit-fact-ref->eid used-fact-refs)]
    (->> (get-in parsed-response [:control :used-facts])
         (keep (fn [ref]
                 (get fact-refs
                      (some-> ref str str/trim str/upper-case))))
         distinct
         vec)))

(defn- execute-tool-calls
  [deps tool-calls context]
  (agent-tools/execute-tool-calls (:tool-deps deps) tool-calls context))

(defn- record-tool-round!
  [session-id execution-context provenance assistant-content tool-calls tool-results
   local-doc-ids artifact-ids]
  (recorder/record-tool-round! session-id
                               execution-context
                               provenance
                               assistant-content
                               tool-calls
                               tool-results
                               local-doc-ids
                               artifact-ids))

(defn run-agent-iteration
  [deps session-id channel resource-session-id local-doc-ids artifact-ids
   execution-context assistant-provider assistant-provider-id transient-messages
   working-memory-message update-working-memory? refresh-working-memory?
   max-tool-rounds worker-state system-prompt-cache-entry turn-budget-state]
  (let [emit-event! #(supervisor/emit-worker-event! worker-state %)
        retrieval-session-id (or resource-session-id session-id)]
    (when (or update-working-memory? refresh-working-memory?)
      (emit-event! {:phase :working-memory
                    :message (if update-working-memory?
                               "Updating working memory"
                               "Refreshing working memory")
                    :iteration (:iteration execution-context)})
      (if update-working-memory?
        (best-effort-update-working-memory! session-id
                                            working-memory-message
                                            channel
                                            {:resource-session-id resource-session-id})
        (best-effort-refresh-working-memory! session-id
                                             working-memory-message
                                             channel
                                             {:resource-session-id resource-session-id})))
    ((:throw-if-cancelled! deps) session-id)
    (let [retrieval-version-before (retrieval-state/version retrieval-session-id)
          tools (tool/tool-definitions execution-context)
          {:keys [messages used-fact-eids used-fact-refs system-prompt-cache-entry]}
          (context/build-messages-data session-id
                                       {:provider assistant-provider
                                        :provider-id assistant-provider-id
                                        :system-prompt-cache-entry system-prompt-cache-entry
                                        :compaction-workload :history-compaction})
          messages (inject-transient-messages messages transient-messages)]
      (emit-event! {:phase :planning
                    :message "Planning next step"
                    :iteration (:iteration execution-context)
                    :round 0
                    :message-count (count messages)
                    :checkpoint {:phase :planning
                                 :iteration (:iteration execution-context)
                                 :round 0
                                 :summary "Working memory updated and context prepared."
                                 :message-count (count messages)
                                 :session-id session-id}})
      (loop [messages messages
             round 0
             tool-activity []]
        ((:throw-if-cancelled! deps) session-id)
        (emit-event! {:phase :llm
                      :message (if (zero? round)
                                 "Calling model"
                                 "Calling model with tool results")
                      :iteration (:iteration execution-context)
                      :round round})
        (let [progress-reporter (make-llm-progress-reporter round emit-event!)
              response (call-model messages
                                   tools
                                   assistant-provider-id
                                   :session-id session-id
                                   :on-delta (fn [delta]
                                               ((:throw-if-cancelled! deps) session-id)
                                               (progress-reporter delta)
                                               ((:throw-if-cancelled! deps) session-id)))
              _ ((:throw-if-cancelled! deps) session-id)
              _ (plugin/run-hooks! :post-llm
                                   (assoc execution-context
                                          :round round
                                          :response-content (agent-tools/response-content response)
                                          :response-provenance (agent-tools/response-provenance response)
                                          :tool-calls (if (map? response)
                                                        (vec (or (get response "tool_calls") []))
                                                        [])))
              tool-calls (if (map? response)
                           (vec (or (get response "tool_calls") []))
                           [])
              has-tools? (seq tool-calls)
              parsed-response (ensure-tool-call-intent
                               session-id
                               execution-context
                               round
                               (autonomous/parse-controller-response
                                (agent-tools/response-content response))
                               tool-calls)
              explicit-used-fact-eids (explicit-used-fact-eids used-fact-refs
                                                               parsed-response)
              assistant-content (or (:assistant-text parsed-response)
                                    (agent-tools/response-content response))
              budget-status (or (limits/budget-status (:task-budget-state execution-context))
                                (limits/budget-status turn-budget-state))
              _ (when (zero? round)
                  (emit-intent-event! emit-event!
                                      execution-context
                                      parsed-response))]
          (if has-tools?
            (if budget-status
              (do
                ((:throw-if-cancelled! deps) session-id)
                (emit-event! {:phase :finalizing
                              :message "Stopping before the next tool step"
                              :iteration (:iteration execution-context)})
                {:response response
                 :parsed-response parsed-response
                 :used-fact-eids used-fact-eids
                 :explicit-used-fact-eids explicit-used-fact-eids
                 :tool-activity tool-activity
                 :refresh-needed? (retrieval-state/changed? retrieval-version-before
                                                            retrieval-session-id)
                 :budget-exhausted? true
                 :budget-status budget-status
                 :budget-before-tools? true
                 :system-prompt-cache-entry system-prompt-cache-entry})
              (do
                (validate-tool-round-protocol! session-id
                                               execution-context
                                               round
                                               parsed-response)
                (let [{:keys [allowed? reason rounds max-tool-rounds] :as decision}
                      (task-policy/tool-round-limit-decision round max-tool-rounds)]
                  (when-not allowed?
                    (prompt/policy-decision! (assoc decision :decision-type :tool-round-policy))
                    (throw (ex-info reason
                                    {:type :tool-round-limit-exceeded
                                     :rounds rounds
                                     :max-tool-rounds max-tool-rounds}))))
                (let [provenance (agent-tools/response-provenance response)
                      {:keys [llm-call-id provider-id model workload]} provenance
                      assistant-msg {:role "assistant"
                                     :content assistant-content
                                     :tool_calls tool-calls}
                      tool-count (count tool-calls)
                      _ (emit-event! {:phase :tool-plan
                                      :message (str "Model requested "
                                                    tool-count
                                                    " tool"
                                                    (when (not= 1 tool-count) "s"))
                                      :iteration (:iteration execution-context)
                                      :round round
                                      :tool-count tool-count})
                      tool-results (do
                                     ((:throw-if-cancelled! deps) session-id)
                                     (execute-tool-calls deps
                                                         tool-calls
                                                         (assoc execution-context
                                                                :llm-call-id llm-call-id
                                                                :provider-id provider-id
                                                                :model model
                                                                :workload workload
                                                                :round round
                                                                :tool-count tool-count
                                                                :worker-event! emit-event!)))
                      _ ((:throw-if-cancelled! deps) session-id)
                      tool-history (mapv #(select-keys % [:role :tool_call_id :content])
                                         tool-results)
                      follow-up-messages (->> tool-results
                                              (mapcat :follow-up-messages)
                                              vec)
                      tool-activity* (conj tool-activity
                                           (agent-tools/tool-round-signature tool-calls
                                                                             tool-results))]
                  (record-tool-round! session-id
                                      execution-context
                                      provenance
                                      assistant-content
                                      tool-calls
                                      tool-results
                                      local-doc-ids
                                      artifact-ids)
                  (emit-event! {:phase :tool
                                :message (or (agent-tools/truncate-summary assistant-content 240)
                                             (str "Completed tool round with "
                                                  tool-count
                                                  " tool call"
                                                  (when (not= 1 tool-count) "s")
                                                  "."))
                                :iteration (:iteration execution-context)
                                :round round
                                :tool-count tool-count
                                :tool-ids (agent-tools/tool-call-names tool-calls)
                                :checkpoint {:phase :tool
                                             :iteration (:iteration execution-context)
                                             :round round
                                             :tool-count tool-count
                                             :tool-ids (agent-tools/tool-call-names tool-calls)
                                             :summary (or (agent-tools/truncate-summary assistant-content 240)
                                                          (str "Completed tool round with "
                                                               tool-count
                                                               " tool call"
                                                               (when (not= 1 tool-count) "s")
                                                               "."))}})
                  (recur (-> messages
                             (conj assistant-msg)
                             (into tool-history)
                             (into follow-up-messages))
                         (inc round)
                         tool-activity*))))
            (do
              ((:throw-if-cancelled! deps) session-id)
              (emit-event! {:phase :finalizing
                            :message "Preparing response"
                            :iteration (:iteration execution-context)})
              {:response response
               :parsed-response parsed-response
               :used-fact-eids used-fact-eids
               :explicit-used-fact-eids explicit-used-fact-eids
               :tool-activity tool-activity
               :refresh-needed? (retrieval-state/changed? retrieval-version-before
                                                          retrieval-session-id)
               :budget-exhausted? (boolean budget-status)
               :budget-status budget-status
               :system-prompt-cache-entry system-prompt-cache-entry})))))))
