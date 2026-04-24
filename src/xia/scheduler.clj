(ns xia.scheduler
  "Background scheduler — executes due schedules on a timer.

   Runs a background thread that wakes every 60 seconds, finds
   schedules whose next-run <= now, and executes them.

   Two execution modes:
   - :tool   — calls tool/execute-tool directly (fast, deterministic)
   - :prompt — creates a session and runs through the full agent loop
               (LLM decides what tools to use)

  Lifecycle: start! → (tick every 60s) → stop!"
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.backup :as backup]
            [xia.db :as db]
            [xia.agent :as agent]
            [xia.hippocampus :as hippo]
            [xia.llm :as llm]
            [xia.oauth :as oauth]
            [xia.runtime-state :as runtime-state]
            [xia.tool :as tool]
            [xia.schedule :as schedule]
            [xia.task-policy :as task-policy]
            [xia.working-memory :as wm])
  (:import [java.util.concurrent ExecutorService Executors ScheduledExecutorService ThreadFactory TimeUnit RejectedExecutionException ThreadPoolExecutor]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private installed-runtime-atom (atom nil))
(declare clear-runtime!)

(defn- make-runtime
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

(defn- ensure-runtime-installed!
  []
  (or (maybe-current-runtime)
      (let [runtime (make-runtime)]
        (reset! installed-runtime-atom runtime)
        runtime)))

(defn- tick-executor-atom
  []
  (:tick-executor-atom (ensure-runtime-installed!)))

(defn- work-executor-atom
  []
  (:work-executor-atom (ensure-runtime-installed!)))

(defn- running-schedules-atom
  []
  (:running-schedules-atom (ensure-runtime-installed!)))

(defn- maintenance-running?-atom
  []
  (:maintenance-running?-atom (ensure-runtime-installed!)))

(defn- last-maintenance-at-atom
  []
  (:last-maintenance-at-atom (ensure-runtime-installed!)))

(defn- thread-counter-atom
  []
  (:thread-counter-atom (ensure-runtime-installed!)))

(defn- runtime-lock
  []
  (:runtime-lock (ensure-runtime-installed!)))

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

(defn- schedule-tool-turn-input
  [tool-id]
  (str "Run scheduled tool " (name tool-id)))

(defn- schedule-tool-turn-summary
  [tool-id]
  (str "Running scheduled tool " (name tool-id) "."))

(defn- schedule-tool-result-summary
  [tool-id result]
  (or (:summary result)
      (:error result)
      (some-> (:content result) str)
      (str (name tool-id) " completed")))

(defn- task-operation
  [existing-task-id task-id]
  (if (= existing-task-id task-id) :resume :start))

(defn- open-schedule-tool-turn!
  [task-id operation tool-id tool-args]
  (let [turn-id (db/start-task-turn! task-id
                                     {:operation operation
                                      :state :running
                                      :input (schedule-tool-turn-input tool-id)
                                      :summary (schedule-tool-turn-summary tool-id)})]
    (db/add-task-item! turn-id
                       {:type :tool-call
                        :status :requested
                        :summary (schedule-tool-turn-summary tool-id)
                        :tool-id (name tool-id)
                        :data {:tool-name (name tool-id)
                               :arguments (or tool-args {})}})
    turn-id))

(defn- close-schedule-tool-turn!
  [task-id turn-id tool-id result]
  (let [status  (if (:error result) :error :success)
        summary (schedule-tool-result-summary tool-id result)]
    (db/add-task-item! turn-id
                       {:type :tool-result
                        :status status
                        :summary summary
                        :tool-id (name tool-id)
                        :data (cond-> {:tool-name (name tool-id)
                                       :status (name status)}
                                (contains? result :content) (assoc :content (:content result))
                                (:summary result) (assoc :summary (:summary result))
                                (:error result) (assoc :error (:error result))
                                (contains? result :result) (assoc :result (:result result)))})
    (db/update-task-turn! turn-id
                          {:state (if (:error result) :failed :completed)
                           :summary summary
                           :error (:error result)})
    (db/update-task! task-id
                     {:state (if (:error result) :failed :completed)
                      :summary summary
                      :stop-reason (when (:error result) :error)
                      :error (:error result)
                      :finished-at (java.util.Date.)})))

(defn- execute-tool-schedule
  "Execute a :tool type schedule."
  [{:keys [id tool-id tool-args trusted? started-at] :as sched}]
  (let [started          (or started-at (java.util.Date.))
        existing-task-id (schedule/schedule-task-id id)
        task-id          (schedule/ensure-schedule-task! sched
                                                         :started-at started)
        turn-id          (open-schedule-tool-turn! task-id
                                                   (task-operation existing-task-id task-id)
                                                   tool-id
                                                   tool-args)
        audit-log        (atom [])
        context          (schedule-tool-context id trusted? audit-log
                                                :task-id task-id
                                                :task-turn-id turn-id)]
    (schedule/save-task-checkpoint!
     id
     {:phase :tool
      :summary (str "Running scheduled tool " (name tool-id) ".")
      :tool-id tool-id
      :task-id task-id})
    (try
      (let [result (tool/execute-tool tool-id (or tool-args {}) context)]
        (close-schedule-tool-turn! task-id turn-id tool-id result)
        (schedule/record-run! id
                              {:started-at started
                               :finished-at (java.util.Date.)
                               :status (if (:error result) :error :success)
                               :result (str result)
                               :actions @audit-log
                               :meta {:task-id task-id}
                               :error (:error result)})
        (if-let [error-message (:error result)]
          (schedule/record-task-failure! id error-message)
          (schedule/record-task-success! id
                                         (or (:summary result)
                                             (str result)))))
      (catch Exception e
        (close-schedule-tool-turn! task-id turn-id tool-id {:error (.getMessage e)})
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
  (try
    (let [topics (:topics (wm/get-wm session-id))]
      (wm/clear-autonomy-state! session-id)
      (wm/snapshot! session-id)
      (hippo/record-conversation! session-id :scheduler :topics topics))
    (catch Exception e
      (log/error e "Failed to finalize scheduler session" session-id))
    (finally
      (try
        (wm/clear-wm! session-id)
        (catch Exception e
          (log/error e "Failed to clear scheduler working memory" session-id))))))

(defn- execute-prompt-schedule
  "Execute a :prompt type schedule — runs through the full agent loop."
  [{:keys [id name prompt trusted? started-at] :as sched}]
  (let [started (or started-at (java.util.Date.))
        resumed-session-id (schedule/resumable-session-id id)
        session-id (or resumed-session-id
                       (db/create-session! :scheduler))
        existing-task-id (schedule/schedule-task-id id)
        task-id (schedule/ensure-schedule-task! sched
                                                :session-id session-id
                                                :started-at started)
        audit-log (atom [])
        budget-state (atom (task-policy/new-schedule-run-llm-budget id))
        prompt* (schedule/augment-prompt-with-recovery-context id prompt)
        execution-context (schedule-tool-context id trusted? audit-log)]
    (try
      (schedule/bind-task! id task-id)
      (when resumed-session-id
        (db/set-session-active! session-id true))
      (wm/ensure-wm! session-id)
      (schedule/save-task-checkpoint!
       id
       {:phase :planning
        :summary (if resumed-session-id
                   "Resumed a scheduled prompt run from the last checkpoint."
                   "Started a scheduled prompt run.")
        :resumed? (boolean resumed-session-id)
        :session-id session-id
        :task-id task-id})
      (let [result (binding [llm/*request-budget-guard*
                             (fn [_request]
                               (task-policy/throw-if-schedule-run-llm-budget-exhausted!
                                budget-state))
                             llm/*request-observer*
                             (fn [request]
                               (task-policy/record-schedule-run-llm-request!
                                budget-state
                                request))]
                     (agent/process-message session-id
                                            prompt*
                                            :channel :scheduler
                                            :task-id task-id
                                            :runtime-op (if (= task-id existing-task-id)
                                                          :resume
                                                          :start)
                                            :tool-context execution-context))]
        (schedule/record-run! id
                              {:started-at started
                               :finished-at (java.util.Date.)
                               :status :success
                               :actions @audit-log
                               :meta {:task-id task-id
                                      :llm-budget @budget-state}
                               :result (str result)})
        (schedule/record-task-success! id result))
      (catch Exception e
        (let [schedule-budget? (= :schedule-run-budget-exhausted
                                  (:type (ex-data e)))]
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
                            (hippo/consolidate-if-pending!)
                            (hippo/maintain-knowledge! now)
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
  (ensure-runtime-installed!)
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
  ([] (or (maybe-current-runtime)
          (install-runtime! (make-runtime))))
  ([runtime]
   (when-let [current (maybe-current-runtime)]
     (when-not (identical? current runtime)
       (clear-runtime!)))
   (reset! installed-runtime-atom runtime)
   runtime))

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
