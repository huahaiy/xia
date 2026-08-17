(ns xia.scheduler
  "Background scheduler — executes due schedules on a timer.

   Runs a background thread that wakes every 60 seconds, finds
   schedules whose next-run <= now, and executes them.

   Due schedules enter the task-spec runner. Individual task steps dispatch to
   executors by `:kind`.

  Lifecycle: start! → (tick every 60s) → stop!"
  (:require [taoensso.timbre :as log]
            [xia.backup :as backup]
            [xia.bridge :as bridge]
            [xia.limits :as limits]
            [xia.oauth :as oauth]
            [xia.plugin :as plugin]
            [xia.runtime-context :as runtime-context]
            [xia.runtime-state :as runtime-state]
            [xia.schedule :as schedule]
            [xia.policy :as task-policy]
            [xia.task-spec :as task-spec])
  (:import [java.util.concurrent ExecutorService Executors RejectedExecutionException
            ScheduledExecutorService ThreadFactory TimeUnit]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(def ^:private runtime-context-key :xia/scheduler)

(defn make-runtime
  []
  {:tick-executor-atom (atom nil)
   :work-executor-atom (atom nil)
   :running-schedules-atom (atom #{})
   :maintenance-running?-atom (atom false)
   :last-maintenance-at-atom (atom nil)
   :thread-counter-atom (atom 0)
   :runtime-lock (Object.)})

(defn- maybe-current-runtime
  []
  (runtime-context/runtime runtime-context-key))

(defn- current-runtime
  []
  (or (maybe-current-runtime)
      (throw (ex-info "Scheduler runtime is not installed"
                      {:component :xia/scheduler}))))

(defn- tick-executor-atom
  []
  (:tick-executor-atom (current-runtime)))

(defn- work-executor-atom
  []
  (:work-executor-atom (current-runtime)))

(defn- running-schedules-atom
  []
  (:running-schedules-atom (current-runtime)))

(defn- maintenance-running?-atom
  []
  (:maintenance-running?-atom (current-runtime)))

(defn- last-maintenance-at-atom
  []
  (:last-maintenance-at-atom (current-runtime)))

(defn- thread-counter-atom
  []
  (:thread-counter-atom (current-runtime)))

(defn- runtime-lock
  []
  (:runtime-lock (current-runtime)))

(def ^:private maintenance-interval-ms (* 24 60 60 1000))
(defn- max-concurrent-runs
  []
  (task-policy/scheduler-max-concurrent-runs))

(defn- daemon-thread-factory
  [prefix]
  (reify ThreadFactory
    (newThread [_ runnable]
      (doto (Thread. ^Runnable runnable
                     ^String (str prefix "-" (swap! (thread-counter-atom) inc)))
        (.setDaemon true)))))

(defn- ensure-work-executor!
  []
  (locking (runtime-lock)
    (let [work-executor* (work-executor-atom)
          exec @work-executor*]
      (if (and exec (not (.isShutdown ^ExecutorService exec)))
        exec
        (let [new-exec (Executors/newFixedThreadPool (int (max-concurrent-runs))
                                                     (daemon-thread-factory "xia-scheduler-work"))]
          (reset! work-executor* new-exec)
          new-exec)))))

(defn- submit-work!
  [kind f]
  (let [^ExecutorService exec (ensure-work-executor!)
        task-fn (runtime-context/convey-bindings f)]
    (try
      (.submit exec
               ^Runnable
               (fn []
                 (try
                   (task-fn)
                   (catch Throwable t
                     (log/error t "Scheduler work item failed:" kind)))))
      true
      (catch RejectedExecutionException e
        (log/error e "Scheduler work submission rejected:" kind)
        false))))

(defn- shutdown-executor!
  [^ExecutorService exec]
  (.shutdown exec)
  (try
    (.awaitTermination exec 30 TimeUnit/SECONDS)
    (catch InterruptedException _
      (.shutdownNow exec))))

;; ---------------------------------------------------------------------------
;; Execution
;; ---------------------------------------------------------------------------

(defn- schedule-run-context
  [schedule-id trusted? audit-log & {:keys [task-id task-turn-id]}]
  (cond-> {:channel :scheduler
           :schedule-id schedule-id
           :autonomous-run? true
           :approval-bypass? trusted?
           :audit-log audit-log}
    task-id (assoc :task-id task-id)
    task-turn-id (assoc :task-turn-id task-turn-id)))

(defn- task-operation
  [existing-task-id task-id]
  (if (= existing-task-id task-id) :resume :start))

(defn- run-step-output
  [run-result step-id]
  (get-in run-result [:state :outputs step-id]))

(defn- run-step-summary
  [run-result step-id fallback]
  (or (:summary run-result)
      (get-in run-result [:state :steps step-id :summary])
      fallback))

(defn- run-step-error
  [run-result step-id]
  (or (:error run-result)
      (get-in run-result [:state :steps step-id :error])))

(defn- run-success?
  [run-result]
  (= :completed (:status run-result)))

(defn- finalize-prompt-schedule-session!
  [session-id]
  (bridge/finalize-channel-session! session-id
                                    :scheduler
                                    :reason :schedule-finish
                                    :mark-inactive? false))

(defn- schedule-llm-executor
  [{:keys [session-id prompt runtime-op execution-context budget-state
           provider-id resource-session-id]}]
  (fn [{:keys [task-id step]}]
    (let [prompt* (or prompt (:prompt step) (:goal step))]
      {:status :success
       :summary "Scheduled prompt completed"
       :output (bridge/send-message! session-id
                                     prompt*
                                     :channel :scheduler
                                     :task-id task-id
                                     :runtime-op runtime-op
                                     :tool-context execution-context
                                     :provider-id provider-id
                                     :resource-session-id resource-session-id
                                     :request-budget-guard
                                     (fn [_request]
                                       (limits/throw-if-exhausted! budget-state))
                                     :request-observer
                                     (fn [request]
                                       (limits/record-schedule-run-request!
                                        budget-state
                                        request)))})))

(defn- schedule-run-hook!
  [phase {:keys [hook-context schedule-type]} attrs]
  (plugin/run-hooks! :schedule-run
                     (merge hook-context
                            {:phase phase
                             :schedule-type schedule-type}
                            attrs)))

(defn- schedule-run-meta
  [{:keys [meta-fn]}]
  (if meta-fn
    (meta-fn)
    {}))

(defn- run-step-values
  [{:keys [step-id schedule-type summary-fallback-fn]} run-result]
  (let [output   (run-step-output run-result step-id)
        error    (run-step-error run-result step-id)
        summary  (run-step-summary run-result
                                   step-id
                                   (when summary-fallback-fn
                                     (summary-fallback-fn run-result)))
        success? (and (run-success? run-result)
                      (nil? error))
        failure  (or error
                     summary
                     (str "Scheduled " (name schedule-type)
                          " task did not complete"))]
    {:output output
     :error error
     :summary summary
     :success? success?
     :status (if success? :success :error)
     :failure failure}))

(defn- run-success-summary
  [{:keys [success-summary-fn]} run-result output summary]
  (or (when success-summary-fn
        (success-summary-fn run-result output summary))
      summary
      (str output)))

(defn- record-completed-schedule-run!
  [{:keys [schedule-id run-id started audit-log] :as plan} run-result]
  (let [{:keys [output summary success? status failure]} (run-step-values plan
                                                                          run-result)
        result-text (str (or output summary))]
    (schedule-run-hook! :finish
                        plan
                        {:status status
                         :result output})
    (schedule/record-run! schedule-id
                          (cond-> {:started-at started
                                   :finished-at (java.util.Date.)
                                   :status status
                                   :result result-text
                                   :actions @audit-log
                                   :meta (schedule-run-meta plan)}
                            run-id (assoc :run-id run-id)
                            (not success?) (assoc :error failure)))
    (if success?
      (schedule/record-task-success! schedule-id
                                     (run-success-summary plan
                                                          run-result
                                                          output
                                                          summary))
      (schedule/record-task-failure! schedule-id failure))))

(defn- exception-message
  [^Exception e]
  (or (.getMessage e) (str e)))

(defn- exception-status
  [{:keys [exception-status-fn]} e]
  (if exception-status-fn
    (exception-status-fn e)
    :error))

(defn- record-exception-schedule-run!
  [{:keys [schedule-id run-id started audit-log] :as plan} e]
  (let [status (exception-status plan e)
        error  (exception-message e)]
    (schedule-run-hook! :finish
                        plan
                        {:status status
                         :error error})
    (schedule/record-run! schedule-id
                          (cond-> {:started-at started
                                   :finished-at (java.util.Date.)
                                   :status status
                                   :actions @audit-log
                                   :meta (schedule-run-meta plan)
                                   :error error}
                            run-id (assoc :run-id run-id)))
    (schedule/record-task-failure! schedule-id error)))

(defn- tool-schedule-plan
  [{:keys [id tool-id trusted? started-at] :as sched}]
  (let [started   (or started-at (java.util.Date.))
        task-id   (schedule/ensure-schedule-task! sched
                                                  :started-at started)
        audit-log (atom [])
        context   (schedule-run-context id trusted? audit-log
                                        :task-id task-id)]
    {:schedule-id id
     :schedule-type :tool
     :started started
     :task-id task-id
     :step-id :run-tool
     :audit-log audit-log
     :hook-context context
     :run-context context
     :start-state {:phase :tool
                   :summary (str "Running scheduled tool " (name tool-id) ".")
                   :tool-id tool-id
                   :task-id task-id}
     :summary-fallback-fn (fn [_] (str (name tool-id) " completed"))
     :success-summary-fn (fn [_ _ summary] summary)
     :meta-fn (fn [] {:task-id task-id})}))

(defn- prompt-schedule-plan
  [{:keys [id prompt trusted? started-at] :as sched}]
  (let [started (or started-at (java.util.Date.))
        resumed-session-id (schedule/resumable-session-id id)
        session-id (or resumed-session-id
                       (:session-id (bridge/create-session! :scheduler)))
        existing-task-id (schedule/schedule-task-id id)
        task-id (schedule/ensure-schedule-task! sched
                                                :session-id session-id
                                                :started-at started)
        audit-log (atom [])
        budget-state (atom (limits/new-schedule-run-budget id))
        prompt* (schedule/augment-prompt-with-recovery-context id prompt)
        execution-context (schedule-run-context id trusted? audit-log)
        hook-context (assoc execution-context
                            :session-id session-id
                            :task-id task-id)
        runtime-op (task-operation existing-task-id task-id)]
    {:schedule-id id
     :schedule-type :prompt
     :started started
     :task-id task-id
     :step-id :run-prompt
     :audit-log audit-log
     :hook-context hook-context
     :run-context {:message prompt*}
     :executors {:llm (schedule-llm-executor
                       {:session-id session-id
                        :prompt prompt*
                        :runtime-op runtime-op
                        :execution-context execution-context
                        :budget-state budget-state})}
     :before-run! (fn []
                    (when resumed-session-id
                      (bridge/resume-session! session-id
                                              :expected-channel :scheduler)))
     :start-state {:phase :planning
                   :summary (if resumed-session-id
                              "Resumed a scheduled prompt run from the bound task state."
                              "Started a scheduled prompt run.")
                   :resumed? (boolean resumed-session-id)
                   :session-id session-id
                   :task-id task-id}
     :summary-fallback-fn (fn [run-result] (:summary run-result))
     :success-summary-fn (fn [run-result output _summary]
                           (or output (:summary run-result)))
     :meta-fn (fn [] {:task-id task-id
                      :llm-budget @budget-state})
     :exception-status-fn (fn [e]
                            (if (limits/exhausted-exception? e)
                              :budget-exhausted
                              :error))
     :finally! (fn []
                 (finalize-prompt-schedule-session! session-id))}))

(defn- schedule-run-plan
  [{:keys [type] :as sched}]
  (let [plan (case type
               :tool (tool-schedule-plan sched)
               :prompt (prompt-schedule-plan sched)
               (throw (ex-info "Unsupported schedule type" {:type type
                                                            :schedule-id (:id sched)})))]
    (cond-> plan
      (:run-id sched) (assoc :run-id (:run-id sched)))))

(defn- task-run-args
  [{:keys [run-context executors]}]
  (cond-> [:operation :scheduled-run
           :context run-context]
    executors (conj :executors executors)))

(defn- execute-task-schedule
  "Execute a schedule through its canonical task-spec task."
  [sched]
  (let [plan (schedule-run-plan sched)]
    (try
      (when-let [before-run! (:before-run! plan)]
        (before-run!))
      (when-let [start-state (:start-state plan)]
        (schedule/record-task-start! (:schedule-id plan) start-state))
      (schedule-run-hook! :start plan {:started-at (:started plan)})
      (let [result (apply task-spec/run-task!
                          (:task-id plan)
                          (task-run-args plan))]
        (record-completed-schedule-run! plan result))
      (catch Exception e
        (record-exception-schedule-run! plan e))
      (finally
        (when-let [finally! (:finally! plan)]
          (finally!))))))

(defn- claim-runtime-schedule!
  [schedule-id]
  (let [running-atom (running-schedules-atom)]
    (loop [running @running-atom]
      (cond
        (contains? running schedule-id)
        false

        (compare-and-set! running-atom running (conj running schedule-id))
        true

        :else
        (recur @running-atom)))))

(defn- execute-schedule!
  "Execute a single schedule, preventing concurrent runs of the same schedule."
  [sched]
  (let [id (:id sched)]
    (cond
      (not (runtime-state/accepting-new-work?))
      (log/debug "Skipping schedule execution because runtime is draining"
                 {:schedule-id id
                  :phase (runtime-state/phase)
                  :draining? (runtime-state/draining?)})

      (not (claim-runtime-schedule! id))
      (log/debug "Skipping schedule execution because it is already running"
                 {:schedule-id id})

      :else
      (try
        (let [started-at    (java.util.Date.)
              claimed-sched (schedule/claim-schedule-run! id started-at)]
          (if-not claimed-sched
            (log/debug "Skipping schedule execution because it is no longer due"
                       {:schedule-id id})
            (do
              (try
                (let [{:keys [status refreshed errors]}
                      (oauth/refresh-autonomous-accounts!)]
                  (when (seq refreshed)
                    (log/info "Proactively refreshed" (count refreshed)
                              "OAuth account(s) before autonomous schedule execution"))
                  (when (seq errors)
                    (log/warn "Proactive OAuth refresh completed with"
                              (count errors) "failure(s) before schedule" (name id)))
                  (when (= status :skipped)
                    (log/debug "Skipped proactive OAuth refresh before schedule" (name id)
                               "because a recent sweep already ran")))
                (catch Exception e
                  (log/warn e "Proactive OAuth refresh failed before schedule" (name id))))
              (log/info "Executing schedule:" (name id) "type:" (:type claimed-sched))
              (execute-task-schedule (assoc claimed-sched :started-at started-at))
              ;; Trim old history (keep 50 most recent per schedule)
              (schedule/trim-history! id 50))))
        (catch Exception e
          (log/error e "Schedule execution failed:" (name id)))
        (finally
          (swap! (running-schedules-atom) disj id))))))

(defn ^:no-doc run-prompt-schedule!
  [sched]
  (execute-task-schedule sched))

(defn ^:no-doc run-tool-schedule!
  [sched]
  (execute-task-schedule sched))

(defn ^:no-doc run-schedule!
  [sched]
  (execute-schedule! sched))

;; ---------------------------------------------------------------------------
;; Tick — the heartbeat
;; ---------------------------------------------------------------------------

(defn- tick!
  "Find and execute all due schedules."
  []
  (try
    (if-not (runtime-state/accepting-new-work?)
      (log/debug "Scheduler tick skipped because runtime is not accepting new work"
                 {:phase (runtime-state/phase)
                  :draining? (runtime-state/draining?)})
      (let [now (java.util.Date.)
            due (schedule/due-schedules now)]
        (when (seq due)
          (log/info "Scheduler tick:" (count due) "schedule(s) due")
          (doseq [sched due]
            ;; Dispatch due schedules onto a bounded worker pool.
            (submit-work! (str "schedule " (name (:id sched)))
                          #(execute-schedule! sched))))
        (when (backup/backup-due?)
          (submit-work! "automatic backup"
                        #(backup/run-scheduled-backup!)))
        (when (and (or (nil? @(last-maintenance-at-atom))
                       (>= (- (.getTime now) (.getTime ^java.util.Date @(last-maintenance-at-atom)))
                           (long maintenance-interval-ms)))
                   (compare-and-set! (maintenance-running?-atom) false true))
          (try
            (when-not (submit-work! "background maintenance"
                                    (fn []
                                      (try
                                        (bridge/run-memory-maintenance! now)
                                        (reset! (last-maintenance-at-atom) now)
                                        (catch Exception e
                                          (log/error e "Background maintenance failed"))
                                        (finally
                                          (reset! (maintenance-running?-atom) false)))))
              ;; A rejected submission never reaches the worker's finally block.
              ;; Release the admission claim so the next tick can retry.
              (reset! (maintenance-running?-atom) false))
            (catch Exception e
              (reset! (maintenance-running?-atom) false)
              (throw e))))))
    (catch Exception e
      (log/error e "Scheduler tick failed"))))

(defn ^:no-doc tick-once!
  []
  (tick!))

;; ---------------------------------------------------------------------------
;; Lifecycle
;; ---------------------------------------------------------------------------

(defn start!
  "Start the background scheduler. Ticks every 60 seconds."
  []
  (let [runtime (current-runtime)]
    (runtime-context/with-runtime-context
      (:runtime-context runtime)
      #(do
         (when @(tick-executor-atom)
           (log/warn "Scheduler already running"))
         (when-not @(tick-executor-atom)
           (let [^ScheduledExecutorService exec (Executors/newSingleThreadScheduledExecutor)
                 tick-fn (runtime-context/convey-bindings tick!)]
             (ensure-work-executor!)
             (.scheduleAtFixedRate exec ^Runnable tick-fn 60 60 TimeUnit/SECONDS)
             (reset! (tick-executor-atom) exec)
             (log/info "Scheduler started (60s interval)")))))))

(defn stop!
  "Stop the background scheduler gracefully."
  []
  (let [^ScheduledExecutorService tick-exec @(tick-executor-atom)
        ^ExecutorService work-exec @(work-executor-atom)]
    (when tick-exec
      (shutdown-executor! tick-exec)
      (reset! (tick-executor-atom) nil))
    (when work-exec
      (shutdown-executor! work-exec)
      (reset! (work-executor-atom) nil))
    (reset! (maintenance-running?-atom) false)
    (reset! (last-maintenance-at-atom) nil)
    (when (or tick-exec work-exec)
      (log/info "Scheduler stopped"))))

(defn ^:no-doc reset-runtime!
  []
  (reset! (running-schedules-atom) #{})
  (reset! (maintenance-running?-atom) false)
  (reset! (last-maintenance-at-atom) nil)
  nil)

(defn clear-runtime!
  []
  (when-let [runtime (maybe-current-runtime)]
    (when (or (some-> (:tick-executor-atom runtime) deref some?)
              (some-> (:work-executor-atom runtime) deref some?))
      (stop!))
    (reset! (:tick-executor-atom runtime) nil)
    (reset! (:work-executor-atom runtime) nil)
    (reset! (:running-schedules-atom runtime) #{})
    (reset! (:maintenance-running?-atom runtime) false)
    (reset! (:last-maintenance-at-atom runtime) nil)
    (reset! (:thread-counter-atom runtime) 0))
  nil)

(defn running?
  "Check if the scheduler is currently running."
  []
  (some? @(tick-executor-atom)))

(defn runtime-activity
  "Return coarse scheduler runtime activity for control-plane inspection."
  []
  {:running?               (boolean @(tick-executor-atom))
   :running-schedule-count (count @(running-schedules-atom))
   :maintenance-running?   (boolean @(maintenance-running?-atom))})
