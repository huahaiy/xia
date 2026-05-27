(ns xia.agent.turn-setup
  "Prepare the persistent task, user message, provider, and execution context for an agent turn."
  (:require [xia.agent.task-runtime :as task-runtime]
            [xia.agent.tools :as agent-tools]
            [xia.audit :as audit]
            [xia.autonomous :as autonomous]
            [xia.constraints :as constraints]
            [xia.db :as db]
            [xia.goal :as goal]
            [xia.limits :as limits]
            [xia.llm :as llm]))

(defn- task-runtime-deps
  []
  {:truncate-summary agent-tools/truncate-summary
   :sanitized-tool-result agent-tools/sanitized-tool-result})

(defn- persistent-goal
  [session-id task-id]
  (let [active-goal* (goal/active-goal session-id)
        task-goal-id (when task-id
                       (some-> task-id
                               db/get-task
                               :meta
                               :persistent-goal
                               :id))]
    (when (and active-goal*
               (or (nil? task-id)
                   (= (:id active-goal*) task-goal-id)))
      active-goal*)))

(defn- persist-user-message!
  [{:keys [session-id request-context task-turn-id user-message persist-message?
           local-doc-ids artifact-ids]}]
  (let [user-message-id (when persist-message?
                          (db/add-message! session-id :user user-message
                                           :local-doc-ids local-doc-ids
                                           :artifact-ids artifact-ids))]
    (when user-message-id
      (audit/log! request-context
                  {:actor :user
                   :type :user-message
                   :message-id user-message-id
                   :data {:local-doc-ids (vec (or local-doc-ids []))
                          :artifact-ids (vec (or artifact-ids []))}}))
    (task-runtime/record-task-message-item!
     (task-runtime-deps)
     task-turn-id
     :user-message
     :user
     user-message
     :message-id user-message-id
     :data (cond-> {}
             (seq local-doc-ids) (assoc :local-doc-ids (vec local-doc-ids))
             (seq artifact-ids) (assoc :artifact-ids (vec artifact-ids))))
    user-message-id))

(defn prepare-turn!
  [deps {:keys [session-id channel user-message task-id runtime-op interrupting-turn-id
                request-context tool-context provider-id resource-session-id
                persist-message? local-doc-ids artifact-ids runtime-task]}]
  (let [persistent-goal* (persistent-goal session-id task-id)
        autonomy-user-message (goal/autonomy-input persistent-goal* user-message)
        wm-user-message (goal/working-memory-input persistent-goal* user-message)
        initial-autonomy-state (autonomous/prepare-turn-state
                                (task-runtime/runtime-autonomy-state session-id task-id)
                                autonomy-user-message)
        {:keys [task-id task-turn-id]} (task-runtime/ensure-runtime-task!
                                        (task-runtime-deps)
                                        session-id
                                        channel
                                        autonomy-user-message
                                        initial-autonomy-state
                                        task-id
                                        runtime-op
                                        interrupting-turn-id
                                        :turn-input user-message)
        _ (goal/attach-task! session-id task-id)
        task-run ((:register-task-run! deps) session-id task-id task-turn-id)
        _ (reset! runtime-task {:task-id task-id
                                :task-turn-id task-turn-id
                                :task-run-id (:task-run-id task-run)})
        user-message-id (persist-user-message!
                         {:session-id session-id
                          :request-context request-context
                          :task-turn-id task-turn-id
                          :user-message user-message
                          :persist-message? persist-message?
                          :local-doc-ids local-doc-ids
                          :artifact-ids artifact-ids})
        operating-envelope (constraints/operating-envelope
                            {:session-id session-id
                             :task-id task-id})
        pre-provider-limit-context (merge tool-context
                                          request-context
                                          {:session-id session-id
                                           :task-id task-id
                                           :task-turn-id task-turn-id
                                           :channel channel
                                           :persistent-goal-id (:id persistent-goal*)
                                           :resource-session-id resource-session-id
                                           :operating-envelope operating-envelope})
        limit-routing-decision (limits/routing-decision pre-provider-limit-context)
        provider-selection-opts (-> (cond-> {:workload :assistant}
                                      provider-id
                                      (assoc :provider-id provider-id))
                                    (limits/apply-routing-decision
                                     limit-routing-decision))
        {assistant-provider :provider
         assistant-provider-id :provider-id}
        (llm/resolve-provider-selection provider-selection-opts)
        base-execution-context (merge tool-context
                                      request-context
                                      {:session-id session-id
                                       :task-id task-id
                                       :task-turn-id task-turn-id
                                       :channel channel
                                       :user-message user-message
                                       :persistent-goal-id (:id persistent-goal*)
                                       :resource-session-id resource-session-id
                                       :assistant-provider assistant-provider
                                       :assistant-provider-id assistant-provider-id
                                       :operating-envelope operating-envelope
                                       :limit-routing-decision limit-routing-decision})]
    {:assistant-provider assistant-provider
     :assistant-provider-id assistant-provider-id
     :autonomy-user-message autonomy-user-message
     :base-execution-context base-execution-context
     :initial-autonomy-state initial-autonomy-state
     :persistent-goal persistent-goal*
     :task-id task-id
     :task-turn-id task-turn-id
     :user-message-id user-message-id
     :wm-user-message wm-user-message}))
