(ns xia.agent.turn-outcome
  "Shared task/status finalization for agent turn exits."
  (:require [xia.agent.task-runtime :as task-runtime]
            [xia.goal :as goal]
            [xia.prompt :as prompt]
            [xia.working-memory :as wm])
  (:import [java.util Date]))

(defn cancellation-outcome
  [reason]
  (case reason
    "task pause requested"
    {:turn-state :paused
     :task-state :paused
     :stop-reason :paused
     :summary "Task paused by user"}

    "task interrupt requested"
    {:turn-state :cancelled
     :task-state :paused
     :stop-reason :interrupted
     :summary "Task interrupted by user"}

    "task steer requested"
    {:turn-state :cancelled
     :task-state :paused
     :stop-reason :interrupted
     :summary "Task interrupted by new instruction"}

    "task stop requested"
    {:turn-state :cancelled
     :task-state :cancelled
     :stop-reason :stopped
     :summary "Task stopped by user"}

    {:turn-state :cancelled
     :task-state :cancelled
     :stop-reason :cancelled
     :summary "Request cancelled"}))

(defn- record-persistent-goal-judge!
  [session-id task-id attrs]
  (when (goal/current-goal session-id)
    (goal/judge-after-turn! session-id (assoc attrs :task-id task-id))))

(defn record-task-outcome!
  [session-id {:keys [task-id task-turn-id]} outcome]
  (let [{:keys [turn-state task-state stop-reason summary error guardrail]} outcome
        error-supplied? (contains? outcome :error)
        autonomy-state (wm/autonomy-state session-id)]
    (task-runtime/sync-runtime-task-turn!
     task-turn-id
     (cond-> {:state turn-state
              :summary summary}
       error-supplied?
       (assoc :error error)))
    (task-runtime/sync-runtime-task!
     task-id
     (cond-> {:state task-state
              :summary summary
              :autonomy-state autonomy-state
              :finished-at (Date.)}
       stop-reason
       (assoc :stop-reason stop-reason)
       error-supplied?
       (assoc :error error)))
    (record-persistent-goal-judge!
     session-id
     task-id
     (cond-> {:task-state task-state
              :control nil
              :autonomy-state autonomy-state
              :summary summary}
       guardrail
       (assoc :guardrail guardrail)))))

(defn record-cancellation!
  [session-id runtime-task reason error]
  (let [outcome (cancellation-outcome reason)]
    (record-task-outcome! session-id
                          runtime-task
                          {:turn-state (:turn-state outcome)
                           :task-state (:task-state outcome)
                           :stop-reason (:stop-reason outcome)
                           :summary (:summary outcome)
                           :error error
                           :guardrail (:stop-reason outcome)})
    outcome))

(defn- cancellation-status-state
  [outcome]
  (if (= :paused (:task-state outcome))
    :paused
    :cancelled))

(defn record-cancellation-status!
  [save-checkpoint! request-context session-id outcome]
  (let [state (cancellation-status-state outcome)]
    (save-checkpoint! request-context
                      {:phase :cancelled
                       :summary (or (:summary outcome) "Request cancelled")
                       :session-id session-id})
    (prompt/status! {:state state
                     :phase state
                     :message (:summary outcome)})))

(defn record-stalled-status!
  [save-checkpoint! request-context session-id data message]
  (save-checkpoint! request-context
                    {:phase :stalled
                     :summary message
                     :session-id session-id
                     :iteration (:iteration data)
                     :current-focus (:current-focus data)
                     :progress-status (:progress-status data)})
  (prompt/status! {:state :error
                   :phase :stalled
                   :message (str "Supervisor stopped the run: " message)}))

(defn record-restart-loop-status!
  [save-checkpoint! request-context session-id data message]
  (save-checkpoint! request-context
                    {:phase :paused
                     :summary message
                     :session-id session-id
                     :status :restart-loop
                     :failure-phase (:failure-phase data)
                     :worker-phase (:worker-phase data)})
  (prompt/status! {:state :paused
                   :phase :paused
                   :message message}))

(defn record-error-status!
  [save-checkpoint! request-context session-id message]
  (save-checkpoint! request-context
                    {:phase :error
                     :summary message
                     :session-id session-id})
  (prompt/status! {:state :error
                   :phase :error
                   :message (str "Request failed: " message)}))
