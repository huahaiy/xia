(ns xia.agent.turn-observation
  "State updates for a completed autonomous agent iteration."
  (:require [xia.agent.tools :as agent-tools]
            [xia.agent.task-runtime :as task-runtime]
            [xia.autonomous :as autonomous]
            [xia.working-memory :as wm]))

(defn- merge-fact-eids
  [left right]
  (->> (concat (or left []) (or right []))
       distinct
       vec))

(defn- autonomous-iteration-summary
  [{:keys [assistant-text control]}]
  (or (:summary control)
      (some-> assistant-text str not-empty)
      "Completed an autonomous iteration."))

(defn- final-assistant-text
  [parsed response]
  (or (some-> (:assistant-text parsed) str not-empty)
      (some-> parsed :control :summary str not-empty)
      (some-> response agent-tools/response-content str not-empty)
      ""))

(defn- validate-goal-complete
  [control autonomy-state]
  (let [claimed? (true? (:goal-complete? control))
        valid? (or (not claimed?)
                   (autonomous/structurally-complete? autonomy-state))]
    {:goal-complete-valid? valid?
     :control (if valid?
                control
                (assoc control :goal-complete? false))
     :autonomy-state (if valid?
                       autonomy-state
                       (autonomous/reconcile-invalid-goal-complete autonomy-state))}))

(defn observe!
  [deps {:keys [session-id task-id iteration max-iterations iteration-context
                autonomy-state parsed-response response fact-eids used-fact-eids
                explicit-fact-eids explicit-used-fact-eids]}]
  (let [parsed parsed-response
        control (:control parsed)
        summary (autonomous-iteration-summary parsed)
        fact-eids* (merge-fact-eids fact-eids used-fact-eids)
        explicit-fact-eids* (merge-fact-eids explicit-fact-eids
                                             explicit-used-fact-eids)
        updated-autonomy-state* (if control
                                  (let [next-state (autonomous/apply-control autonomy-state
                                                                             control)]
                                    (wm/set-autonomy-state! session-id next-state)
                                    (or (wm/autonomy-state session-id)
                                        next-state))
                                  autonomy-state)
        {:keys [goal-complete-valid? control autonomy-state]}
        (if control
          (validate-goal-complete control updated-autonomy-state*)
          {:goal-complete-valid? true
           :control control
           :autonomy-state updated-autonomy-state*})
        _ (when (and control (not goal-complete-valid?))
            (wm/set-autonomy-state! session-id autonomy-state))
        updated-autonomy-state autonomy-state
        updated-tip (autonomous/current-frame updated-autonomy-state)
        _ (wm/snapshot! session-id)
        text (final-assistant-text parsed response)]
    (task-runtime/sync-runtime-task! task-id
                                     {:state :running
                                      :summary summary
                                      :autonomy-state updated-autonomy-state})
    ((:report-autonomy-status! deps)
     :observing
     updated-autonomy-state
     iteration
     max-iterations
     :stack-action (some-> control :stack-action))
    ((:save-schedule-checkpoint! deps)
     iteration-context
     {:phase :observing
      :iteration iteration
      :summary summary
      :session-id session-id
      :control-status (:control-status parsed)
      :goal-complete-valid? goal-complete-valid?
      :status (some-> control :status)
      :next-step (some-> control :next-step)
      :progress-status (some-> updated-tip :progress-status)
      :agenda (some-> updated-tip :agenda)
      :stack (some-> updated-autonomy-state :stack)})
    {:parsed parsed
     :control control
     :text text
     :fact-eids fact-eids*
     :explicit-fact-eids explicit-fact-eids*
     :updated-autonomy-state updated-autonomy-state
     :updated-tip updated-tip}))
