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

(defn- supervisor-restart-grace-ms
  [deps]
  (let [value (:supervisor-restart-grace-ms deps)]
    (if (fn? value)
      (value)
      value)))

(defn- record-failed-outcome!
  [session-id runtime-task message]
  (record-task-outcome!
   session-id
   runtime-task
   {:turn-state :failed
    :task-state :failed
    :stop-reason :error
    :summary message
    :error message
    :guardrail :failed}))

(defn- record-request-cancelled!
  [deps {:keys [session-id request-context runtime-task]} reason message]
  (let [outcome (record-cancellation! session-id
                                      runtime-task
                                      reason
                                      message)]
    (record-cancellation-status!
     (:save-schedule-checkpoint! deps)
     request-context
     session-id
     outcome)
    outcome))

(defn handle-interrupted!
  [deps {:keys [session-id channel request-context runtime-task]} e]
  (record-cancellation! session-id
                        runtime-task
                        "request interrupted"
                        (some-> e .getMessage))
  ((:request-session-cancel! deps) session-id "request interrupted")
  (if ((:stop-worker! deps) session-id)
    (let [cancel-ex ((:request-cancelled-ex deps)
                     session-id
                     ((:cancellation-reason deps) session-id)
                     e)]
      (record-cancellation-status!
       (:save-schedule-checkpoint! deps)
       request-context
       session-id
       (cancellation-outcome (:reason (ex-data cancel-ex))))
      (throw cancel-ex))
    (throw (ex-info "Agent supervisor could not stop the worker after request cancellation"
                    {:type :agent-stop-timeout
                     :session-id session-id
                     :channel channel
                     :grace-ms (supervisor-restart-grace-ms deps)}
                    e))))

(defn handle-ex-info!
  [deps {:keys [session-id request-context runtime-task] :as context} e]
  (let [data (ex-data e)
        message (.getMessage e)]
    (cond
      (= :request-cancelled (:type data))
      (do
        (record-request-cancelled! deps
                                   context
                                   (:reason data)
                                   message)
        (throw e))

      (contains? #{:agent-stalled :autonomous-loop-stalled :agent-stop-timeout} (:type data))
      (do
        (record-task-outcome!
         session-id
         runtime-task
         {:turn-state :failed
          :task-state :failed
          :stop-reason :stalled
          :summary message
          :error message
          :guardrail :stalled})
        (record-stalled-status!
         (:save-schedule-checkpoint! deps)
         request-context
         session-id
         data
         message)
        (throw e))

      (= :task-restart-loop (:type data))
      (do
        (record-task-outcome!
         session-id
         runtime-task
         {:turn-state :completed
          :task-state :resumable
          :stop-reason :restart-loop
          :summary message
          :error message
          :guardrail :restart-loop})
        (record-restart-loop-status!
         (:save-schedule-checkpoint! deps)
         request-context
         session-id
         data
         message)
        (throw e))

      :else
      (do
        (record-failed-outcome! session-id runtime-task message)
        (record-error-status!
         (:save-schedule-checkpoint! deps)
         request-context
         session-id
         message)
        (throw e)))))

(defn handle-exception!
  [deps {:keys [session-id request-context runtime-task] :as context} e]
  (if ((:session-cancelled? deps) session-id)
    (let [cancel-ex ((:request-cancelled-ex deps)
                     session-id
                     ((:cancellation-reason deps) session-id)
                     e)]
      (record-request-cancelled! deps
                                 context
                                 (:reason (ex-data cancel-ex))
                                 (.getMessage cancel-ex))
      (throw cancel-ex))
    (do
      (record-failed-outcome! session-id runtime-task (.getMessage e))
      (record-error-status!
       (:save-schedule-checkpoint! deps)
       request-context
       session-id
       (.getMessage e))
      (throw e))))

(defn clear-runtime-task-run!
  [deps session-id runtime-task]
  (when-let [{:keys [task-id task-turn-id task-run-id]} runtime-task]
    ((:clear-task-run! deps) session-id task-id task-turn-id task-run-id)))
