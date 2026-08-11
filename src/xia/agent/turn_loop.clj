(ns xia.agent.turn-loop
  "Budget-bound autonomous turn loop for an agent session run."
  (:require [xia.agent.loop-guard :as loop-guard]
            [xia.agent.task-runtime :as task-runtime]
            [xia.agent.turn-completion :as turn-completion]
            [xia.agent.turn-observation :as turn-observation]
            [xia.autonomous :as autonomous]
            [xia.limits :as limits]
            [xia.llm :as llm]
            [xia.working-memory :as wm]))

(defn ensure-turn-budget-state
  [existing-state session-id channel]
  (or existing-state
      (atom (limits/new-turn-budget session-id channel))))

(defn- compose-request-limit-guard
  [outer-guard inner-guard]
  (cond
    (and outer-guard inner-guard)
    (fn [request]
      (outer-guard request)
      (inner-guard request))

    outer-guard outer-guard
    inner-guard inner-guard
    :else nil))

(defn- compose-request-limit-observer
  [outer-observer inner-observer]
  (cond
    (and outer-observer inner-observer)
    (fn [request]
      (outer-observer request)
      (inner-observer request))

    outer-observer outer-observer
    inner-observer inner-observer
    :else nil))

(defn- autonomous-iteration-messages
  [autonomy-state iteration max-iterations & {:keys [incoming-message]}]
  [(autonomous/controller-system-message)
   (autonomous/controller-state-message
    {:goal (autonomous/root-goal autonomy-state)
     :iteration iteration
     :max-iterations max-iterations
     :stack (:stack autonomy-state)
     :incoming-message incoming-message})])

(defn- exhausted-result
  [e]
  {:budget-exhausted? true
   :budget-status (select-keys (ex-data e)
                               [:scope :kind :task-id :session-id :channel
                                :llm-call-count :total-tokens
                                :prompt-tokens :completion-tokens
                                :elapsed-ms :llm-total-duration-ms
                                :max-llm-calls :max-total-tokens
                                :max-wall-clock-ms :max-llm-duration-ms])})

(defn- run-supervised-iteration!
  [deps {:keys [session-id channel resource-session-id local-doc-ids artifact-ids
                iteration-context assistant-provider assistant-provider-id transient-messages
                wm-message update-working-memory? refresh-working-memory? max-tool-rounds
                autonomy-state max-iterations system-prompt-cache-entry turn-budget-state]}]
  (try
    ((:run-supervised-agent-iteration deps)
     session-id
     channel
     resource-session-id
     local-doc-ids
     artifact-ids
     iteration-context
     assistant-provider
     assistant-provider-id
     transient-messages
     wm-message
     update-working-memory?
     refresh-working-memory?
     max-tool-rounds
     autonomy-state
     max-iterations
     system-prompt-cache-entry
     turn-budget-state)
    (catch clojure.lang.ExceptionInfo e
      (if (limits/exhausted-exception? e)
        (exhausted-result e)
        (throw e)))))

(defn run-turn-loop!
  [deps {:keys [session-id channel resource-session-id local-doc-ids artifact-ids
                user-message working-memory-message task-id task-turn-id
                assistant-provider assistant-provider-id base-execution-context
                initial-autonomy-state wm-user-message max-tool-rounds transient-messages
                turn-budget-state]}]
  (let [max-tool-rounds* (long (or max-tool-rounds
                                   ((:configured-max-tool-rounds deps))))
        max-iterations* (long (autonomous/max-iterations))
        transient-messages* (vec (filter map? transient-messages))
        initial-wm-message (or working-memory-message
                               wm-user-message)
        initial-wm-query-fingerprint (loop-guard/wm-query-signature initial-wm-message)
        task-budget-state (task-runtime/task-limit-state task-id)
        outer-budget-guard llm/*request-budget-guard*
        outer-request-observer llm/*request-observer*]
    (wm/set-autonomy-state! session-id initial-autonomy-state)
    (binding [llm/*request-budget-guard* (compose-request-limit-guard
                                          outer-budget-guard
                                          (fn [_request]
                                            ((:handle-limit-policy-decision! deps)
                                             base-execution-context
                                             (limits/policy-decision
                                              base-execution-context))
                                            (limits/throw-if-exhausted!
                                             turn-budget-state)
                                            (limits/throw-if-exhausted!
                                             task-budget-state)))
              llm/*request-observer* (compose-request-limit-observer
                                      outer-request-observer
                                      (fn [request]
                                        (limits/record-turn-request!
                                         turn-budget-state
                                         request)
                                        (task-runtime/record-task-limit-request!
                                         task-id
                                         task-budget-state
                                         request)
                                        (limits/log-usage!
                                         base-execution-context
                                         request)))]
      (loop [iteration 1
             fact-eids []
             explicit-fact-eids []
             loop-state nil
             refresh-working-memory? false
             system-prompt-cache-entry nil
             wm-message initial-wm-message
             wm-query-fingerprint initial-wm-query-fingerprint]
        ((:throw-if-cancelled! deps) session-id)
        (let [autonomy-state (or (wm/autonomy-state session-id)
                                 initial-autonomy-state)
              iteration-context (assoc base-execution-context
                                       :task-budget-state task-budget-state
                                       :iteration iteration
                                       :max-iterations max-iterations*)
              controller-messages (autonomous-iteration-messages
                                   autonomy-state
                                   iteration
                                   max-iterations*
                                   :incoming-message (when (= iteration 1)
                                                       user-message))
              transient-messages** (into transient-messages*
                                         controller-messages)
              _ ((:report-autonomy-status! deps)
                 :understanding
                 autonomy-state
                 iteration
                 max-iterations*)
              update-working-memory? (= iteration 1)]
          ((:save-schedule-checkpoint! deps)
           iteration-context
           {:phase :understanding
            :iteration iteration
            :summary (if (= iteration 1)
                       "Understanding the goal and preparing the first plan."
                       "Resuming the autonomous loop with the updated plan.")
            :session-id session-id})
          (let [{:keys [response parsed-response used-fact-eids explicit-used-fact-eids
                        tool-activity refresh-needed? system-prompt-cache-entry
                        budget-exhausted? budget-status budget-before-tools?]}
                (run-supervised-iteration!
                 deps
                 {:session-id session-id
                  :channel channel
                  :resource-session-id resource-session-id
                  :local-doc-ids local-doc-ids
                  :artifact-ids artifact-ids
                  :iteration-context iteration-context
                  :assistant-provider assistant-provider
                  :assistant-provider-id assistant-provider-id
                  :transient-messages transient-messages**
                  :wm-message wm-message
                  :update-working-memory? update-working-memory?
                  :refresh-working-memory? refresh-working-memory?
                  :max-tool-rounds max-tool-rounds*
                  :autonomy-state autonomy-state
                  :max-iterations max-iterations*
                  :system-prompt-cache-entry system-prompt-cache-entry
                  :turn-budget-state turn-budget-state})
                {:keys [parsed control text updated-autonomy-state updated-tip]
                 fact-eids* :fact-eids
                 explicit-fact-eids* :explicit-fact-eids}
                (turn-observation/observe!
                 (:turn-observation-deps deps)
                 {:session-id session-id
                  :task-id task-id
                  :iteration iteration
                  :max-iterations max-iterations*
                  :iteration-context iteration-context
                  :autonomy-state autonomy-state
                  :parsed-response parsed-response
                  :response response
                  :fact-eids fact-eids
                  :used-fact-eids used-fact-eids
                  :explicit-fact-eids explicit-fact-eids
                  :explicit-used-fact-eids explicit-used-fact-eids})]
            (cond
              (and budget-exhausted?
                   (or (nil? response)
                       budget-before-tools?
                       (= :continue (:status control))))
              (turn-completion/budget-pause!
               (:turn-completion-deps deps)
               {:session-id session-id
                :task-id task-id
                :task-turn-id task-turn-id
                :user-message user-message
                :local-doc-ids local-doc-ids
                :artifact-ids artifact-ids
                :iteration iteration
                :max-iterations max-iterations*
                :iteration-context iteration-context
                :response response
                :parsed parsed
                :control control
                :text text
                :fact-eids fact-eids*
                :explicit-fact-eids explicit-fact-eids*
                :updated-autonomy-state updated-autonomy-state
                :updated-tip updated-tip
                :budget-status budget-status
                :budget-before-tools? budget-before-tools?})

              (or (nil? control)
                  (= :complete (:status control)))
              (turn-completion/complete!
               (:turn-completion-deps deps)
               {:session-id session-id
                :task-id task-id
                :task-turn-id task-turn-id
                :user-message user-message
                :local-doc-ids local-doc-ids
                :artifact-ids artifact-ids
                :iteration-context iteration-context
                :response response
                :parsed parsed
                :control control
                :text text
                :fact-eids fact-eids*
                :explicit-fact-eids explicit-fact-eids*
                :updated-autonomy-state updated-autonomy-state})

              (>= iteration max-iterations*)
              (turn-completion/iteration-limit!
               (:turn-completion-deps deps)
               {:session-id session-id
                :task-id task-id
                :task-turn-id task-turn-id
                :user-message user-message
                :local-doc-ids local-doc-ids
                :artifact-ids artifact-ids
                :iteration iteration
                :max-iterations max-iterations*
                :iteration-context iteration-context
                :response response
                :control control
                :text text
                :fact-eids fact-eids*
                :explicit-fact-eids explicit-fact-eids*
                :updated-autonomy-state updated-autonomy-state
                :updated-tip updated-tip})

              :else
              (let [{:keys [iteration loop-state refresh-working-memory?
                            system-prompt-cache-entry wm-message
                            wm-query-fingerprint]}
                    (turn-completion/continue!
                     (:turn-completion-deps deps)
                     {:session-id session-id
                      :channel channel
                      :iteration iteration
                      :max-iterations max-iterations*
                      :iteration-context iteration-context
                      :response response
                      :control control
                      :text text
                      :tool-activity tool-activity
                      :loop-state loop-state
                      :refresh-needed? refresh-needed?
                      :working-memory-message working-memory-message
                      :wm-query-fingerprint wm-query-fingerprint
                      :system-prompt-cache-entry system-prompt-cache-entry
                      :updated-autonomy-state updated-autonomy-state
                      :updated-tip updated-tip})]
                (recur iteration
                       fact-eids*
                       explicit-fact-eids*
                       loop-state
                       refresh-working-memory?
                       system-prompt-cache-entry
                       wm-message
                       wm-query-fingerprint)))))))))
