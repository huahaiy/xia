(ns xia.scheduler
  "Background scheduler — executes due schedules on a timer.

   Runs a background thread that wakes every 60 seconds, finds
   schedules whose next-run <= now, and executes them.

   Due schedules enter the task-spec runner. Individual task steps dispatch to
   executors by `:kind`.

  Lifecycle: start! → (tick every 60s) → stop!"
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.backup :as backup]
            [xia.bridge :as bridge]
            [xia.db :as db]
            [xia.limits :as limits]
            [xia.oauth :as oauth]
            [xia.plugin :as plugin]
            [xia.runtime-state :as runtime-state]
            [xia.schedule :as schedule]
            [xia.policy :as task-policy]
            [xia.task-spec :as task-spec])
  (:import [java.util.concurrent ExecutorService Executors ScheduledExecutorService ThreadFactory TimeUnit RejectedExecutionException ThreadPoolExecutor]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private installed-runtime-atom (atom nil))
(declare clear-runtime!)

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
  @installed-runtime-atom)

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
  (let [^ExecutorService exec (ensure-work-executor!)]
    (try
      (.submit exec
               ^Runnable
               (fn []
                 (try
                   (f)
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

(defn- schedule-tool-context
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

(defn- execute-tool-schedule
  "Execute a :tool type schedule."
  [{:keys [id tool-id trusted? started-at] :as sched}]
  (let [started          (or started-at (java.util.Date.))
        task-id          (schedule/ensure-schedule-task! sched
                                                         :started-at started)
        audit-log        (atom [])
        context          (schedule-tool-context id trusted? audit-log
                                                :task-id task-id)]
    (schedule/save-task-checkpoint!
     id
     {:phase :tool
      :summary (str "Running scheduled tool " (name tool-id) ".")
      :tool-id tool-id
      :task-id task-id})
    (try
      (plugin/run-hooks! :schedule-run
                         (assoc context
                                :phase :start
                                :schedule-type :tool
                                :started-at started))
      (let [result      (task-spec/run-task! task-id
                                             :operation :scheduled-run
                                             :context context)
            output      (run-step-output result :run-tool)
            error       (run-step-error result :run-tool)
            success?    (run-success? result)
            status      (if success? :success :error)
            summary     (run-step-summary result
                                          :run-tool
                                          (str (name tool-id) " completed"))]
        (plugin/run-hooks! :schedule-run
                           (assoc context
                                  :phase :finish
                                  :schedule-type :tool
                                  :status status
                                  :result output))
        (schedule/record-run! id
                              {:started-at started
                               :finished-at (java.util.Date.)
                               :status status
                               :result (str (or output summary))
                               :actions @audit-log
                               :meta {:task-id task-id}
                               :error error})
        (if error
          (schedule/record-task-failure! id error)
          (schedule/record-task-success! id
                                         (or summary
                                             (str output)))))
      (catch Exception e
        (plugin/run-hooks! :schedule-run
                           (assoc context
                                  :phase :finish
                                  :schedule-type :tool
                                  :status :error
                                  :error (.getMessage e)))
        (schedule/record-run! id
                              {:started-at started
                               :finished-at (java.util.Date.)
                               :status :error
                               :actions @audit-log
                               :meta {:task-id task-id}
                               :error (.getMessage e)})
        (schedule/record-task-failure! id (.getMessage e))))))

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

(defn- execute-prompt-schedule
  "Execute a :prompt type schedule — runs through the full agent loop."
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
        execution-context (schedule-tool-context id trusted? audit-log)
        runtime-op (task-operation existing-task-id task-id)]
    (try
      (schedule/bind-task! id task-id)
      (when resumed-session-id
        (bridge/resume-session! session-id
                                :expected-channel :scheduler))
      (schedule/save-task-checkpoint!
       id
       {:phase :planning
        :summary (if resumed-session-id
                   "Resumed a scheduled prompt run from the last checkpoint."
                   "Started a scheduled prompt run.")
        :resumed? (boolean resumed-session-id)
        :session-id session-id
        :task-id task-id})
      (plugin/run-hooks! :schedule-run
                         (assoc execution-context
                                :session-id session-id
                                :task-id task-id
                                :phase :start
                                :schedule-type :prompt
                                :started-at started))
      (let [result (task-spec/run-task!
                    task-id
                    :operation :scheduled-run
                    :context {:message prompt*}
                    :executors {:llm
                                (schedule-llm-executor
                                 {:session-id session-id
                                  :prompt prompt*
                                  :runtime-op runtime-op
                                  :execution-context execution-context
                                  :budget-state budget-state})})
            output (run-step-output result :run-prompt)
            error  (run-step-error result :run-prompt)]
        (plugin/run-hooks! :schedule-run
                           (assoc execution-context
                                  :session-id session-id
                                  :task-id task-id
                                  :phase :finish
                                  :schedule-type :prompt
                                  :status (if (run-success? result) :success :error)
                                  :result output))
        (schedule/record-run! id
                              {:started-at started
                               :finished-at (java.util.Date.)
                               :status (if (run-success? result) :success :error)
                               :actions @audit-log
                               :meta {:task-id task-id
                                      :llm-budget @budget-state}
                               :result (str (or output (:summary result)))
                               :error error})
        (if error
          (schedule/record-task-failure! id error)
          (schedule/record-task-success! id
                                         (or output
                                             (:summary result)))))
      (catch Exception e
        (let [schedule-budget? (limits/exhausted-exception? e)]
          (plugin/run-hooks! :schedule-run
                             (assoc execution-context
                                    :session-id session-id
                                    :task-id task-id
                                    :phase :finish
                                    :schedule-type :prompt
                                    :status (if schedule-budget?
                                              :budget-exhausted
                                              :error)
                                    :error (.getMessage e)))
          (schedule/record-run! id
                                {:started-at started
                                 :finished-at (java.util.Date.)
                                 :status (if schedule-budget?
                                           :budget-exhausted
                                           :error)
                                 :actions @audit-log
                                 :meta {:task-id task-id
                                        :llm-budget @budget-state}
                                 :error (.getMessage e)})
          (schedule/record-task-failure! id (.getMessage e))))
      (finally
        (finalize-prompt-schedule-session! session-id)))))

(defn- execute-schedule!
  "Execute a single schedule, preventing concurrent runs of the same schedule."
  [sched]
  (let [id (:id sched)]
    (when (and (runtime-state/accepting-new-work?)
               (not (contains? @(running-schedules-atom) id)))
      (swap! (running-schedules-atom) conj id)
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
              (case (:type claimed-sched)
                :tool (execute-tool-schedule (assoc claimed-sched :started-at started-at))
                :prompt (execute-prompt-schedule (assoc claimed-sched :started-at started-at)))
              ;; Trim old history (keep 50 most recent per schedule)
              (schedule/trim-history! id 50))))
        (catch Exception e
          (log/error e "Schedule execution failed:" (name id)))
        (finally
          (swap! (running-schedules-atom) disj id))))
    (when-not (runtime-state/accepting-new-work?)
      (log/debug "Skipping schedule execution because runtime is draining"
                 {:schedule-id id
                  :phase (runtime-state/phase)
                  :draining? (runtime-state/draining?)}))))

(defn ^:no-doc run-prompt-schedule!
  [sched]
  (execute-prompt-schedule sched))

(defn ^:no-doc run-tool-schedule!
  [sched]
  (execute-tool-schedule sched))

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
          (submit-work! "background maintenance"
                        (fn []
                          (try
                            (bridge/run-memory-maintenance! now)
                            (reset! (last-maintenance-at-atom) now)
                            (catch Exception e
                              (log/error e "Background maintenance failed"))
                            (finally
                              (reset! (maintenance-running?-atom) false))))))))
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
  (when @(tick-executor-atom)
    (log/warn "Scheduler already running"))
  (when-not @(tick-executor-atom)
    (let [^ScheduledExecutorService exec (Executors/newSingleThreadScheduledExecutor)]
      (ensure-work-executor!)
      (.scheduleAtFixedRate exec ^Runnable tick! 60 60 TimeUnit/SECONDS)
      (reset! (tick-executor-atom) exec)
      (log/info "Scheduler started (60s interval)"))))

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

(defn install-runtime!
  [runtime]
  (when-let [current (maybe-current-runtime)]
    (when-not (identical? current runtime)
      (clear-runtime!)))
  (reset! installed-runtime-atom runtime)
  runtime)

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
    (reset! (:thread-counter-atom runtime) 0)
    (reset! installed-runtime-atom nil))
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
