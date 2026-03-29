(ns xia.scheduler
  "Background scheduler — executes due schedules on a timer.

   Runs a background thread that wakes every 60 seconds, finds
   schedules whose next-run <= now, and executes them.

   Two execution modes:
   - :tool   — calls tool/execute-tool directly (fast, deterministic)
   - :prompt — creates a session and runs through the full agent loop
               (LLM decides what tools to use)

  Lifecycle: start! → (tick every 60s) → stop!"
  (:require [taoensso.timbre :as log]
            [xia.backup :as backup]
            [xia.db :as db]
            [xia.agent :as agent]
            [xia.config :as cfg]
            [xia.hippocampus :as hippo]
            [xia.oauth :as oauth]
            [xia.tool :as tool]
            [xia.schedule :as schedule]
            [xia.working-memory :as wm])
  (:import [java.util.concurrent ExecutorService Executors ScheduledExecutorService ThreadFactory TimeUnit RejectedExecutionException ThreadPoolExecutor]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private tick-executor (atom nil))
(defonce ^:private work-executor (atom nil))
(defonce ^:private running-schedules (atom #{}))
(defonce ^:private maintenance-running? (atom false))
(defonce ^:private last-maintenance-at (atom nil))
(defonce ^:private thread-counter (atom 0))

(def ^:private maintenance-interval-ms (* 24 60 60 1000))
(def ^:private default-max-concurrent-runs 4)

(defn- max-concurrent-runs
  []
  (cfg/positive-long :scheduler/max-concurrent-runs
                     default-max-concurrent-runs))

(defn- daemon-thread-factory
  [prefix]
  (reify ThreadFactory
    (newThread [_ runnable]
      (doto (Thread. ^Runnable runnable
                     (str prefix "-" (swap! thread-counter inc)))
        (.setDaemon true)))))

(defn- ensure-work-executor!
  []
  (locking work-executor
    (let [exec @work-executor]
      (if (and exec (not (.isShutdown ^ExecutorService exec)))
        exec
        (let [new-exec (Executors/newFixedThreadPool (int (max-concurrent-runs))
                                                     (daemon-thread-factory "xia-scheduler-work"))]
          (reset! work-executor new-exec)
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

;; ---------------------------------------------------------------------------
;; Execution
;; ---------------------------------------------------------------------------

(defn- schedule-tool-context
  [schedule-id trusted? audit-log]
  {:channel :scheduler
   :schedule-id schedule-id
   :autonomous-run? true
   :approval-bypass? trusted?
   :audit-log audit-log})

(defn- execute-tool-schedule
  "Execute a :tool type schedule."
  [{:keys [id tool-id tool-args trusted?]}]
  (let [started (java.util.Date.)
        audit-log (atom [])
        context (schedule-tool-context id trusted? audit-log)]
    (schedule/save-task-checkpoint!
     id
     {:phase :tool
      :summary (str "Running scheduled tool " (name tool-id) ".")
      :tool-id tool-id})
    (try
      (let [result (tool/execute-tool tool-id (or tool-args {}) context)]
        (schedule/record-run! id
                              {:started-at started
                               :finished-at (java.util.Date.)
                               :status (if (:error result) :error :success)
                               :result (str result)
                               :actions @audit-log
                               :error (:error result)})
        (if-let [error-message (:error result)]
          (schedule/record-task-failure! id error-message)
          (schedule/record-task-success! id
                                         (or (:summary result)
                                             (str result)))))
      (catch Exception e
        (schedule/record-run! id
                              {:started-at started
                               :finished-at (java.util.Date.)
                               :status :error
                               :actions @audit-log
                               :error (.getMessage e)})
        (schedule/record-task-failure! id (.getMessage e))))))

(defn- finalize-prompt-schedule-session!
  [session-id]
  (try
    (let [topics (:topics (wm/get-wm session-id))]
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
  [{:keys [id prompt trusted?]}]
  (let [started (java.util.Date.)
        resumed-session-id (schedule/resumable-session-id id)
        session-id (or resumed-session-id
                       (db/create-session! :scheduler))
        audit-log (atom [])
        prompt* (schedule/augment-prompt-with-recovery-context id prompt)
        execution-context (schedule-tool-context id trusted? audit-log)]
    (try
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
        :session-id session-id})
      (let [result (agent/process-message session-id
                                          prompt*
                                          :channel :scheduler
                                          :tool-context execution-context)]
        (schedule/record-run! id
                              {:started-at started
                               :finished-at (java.util.Date.)
                               :status :success
                               :actions @audit-log
                               :result (str result)})
        (schedule/record-task-success! id result))
      (catch Exception e
        (schedule/record-run! id
                              {:started-at started
                               :finished-at (java.util.Date.)
                               :status :error
                               :actions @audit-log
                               :error (.getMessage e)})
        (schedule/record-task-failure! id (.getMessage e)))
      (finally
        (finalize-prompt-schedule-session! session-id)))))

(defn- execute-schedule!
  "Execute a single schedule, preventing concurrent runs of the same schedule."
  [sched]
  (let [id (:id sched)]
    (when-not (contains? @running-schedules id)
      (swap! running-schedules conj id)
      (try
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
        (log/info "Executing schedule:" (name id) "type:" (:type sched))
        (case (:type sched)
          :tool (execute-tool-schedule sched)
          :prompt (execute-prompt-schedule sched))
        ;; Trim old history (keep 50 most recent per schedule)
        (schedule/trim-history! id 50)
        (catch Exception e
          (log/error e "Schedule execution failed:" (name id)))
        (finally
          (swap! running-schedules disj id))))))

;; ---------------------------------------------------------------------------
;; Tick — the heartbeat
;; ---------------------------------------------------------------------------

(defn- tick!
  "Find and execute all due schedules."
  []
  (try
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
      (when (and (or (nil? @last-maintenance-at)
                     (>= (- (.getTime now) (.getTime ^java.util.Date @last-maintenance-at))
                         (long maintenance-interval-ms)))
                 (compare-and-set! maintenance-running? false true))
        (submit-work! "background maintenance"
                      (fn []
                        (try
                          (hippo/consolidate-if-pending!)
                          (hippo/maintain-knowledge! now)
                          (reset! last-maintenance-at now)
                          (catch Exception e
                            (log/error e "Background maintenance failed"))
                          (finally
                            (reset! maintenance-running? false)))))))
    (catch Exception e
      (log/error e "Scheduler tick failed"))))

;; ---------------------------------------------------------------------------
;; Lifecycle
;; ---------------------------------------------------------------------------

(defn start!
  "Start the background scheduler. Ticks every 60 seconds."
  []
  (when @tick-executor
    (log/warn "Scheduler already running"))
  (when-not @tick-executor
    (let [^ScheduledExecutorService exec (Executors/newSingleThreadScheduledExecutor)]
      (ensure-work-executor!)
      (.scheduleAtFixedRate exec ^Runnable tick! 60 60 TimeUnit/SECONDS)
      (reset! tick-executor exec)
      (log/info "Scheduler started (60s interval)"))))

(defn stop!
  "Stop the background scheduler gracefully."
  []
  (when-let [^ScheduledExecutorService exec @tick-executor]
    (.shutdown exec)
    (try
      (.awaitTermination exec 30 TimeUnit/SECONDS)
      (catch InterruptedException _
        (.shutdownNow exec)))
    (reset! tick-executor nil)
    (when-let [^ExecutorService work @work-executor]
      (.shutdown work)
      (try
        (.awaitTermination work 30 TimeUnit/SECONDS)
        (catch InterruptedException _
          (.shutdownNow work)))
      (reset! work-executor nil))
    (reset! maintenance-running? false)
    (reset! last-maintenance-at nil)
    (log/info "Scheduler stopped")))

(defn running?
  "Check if the scheduler is currently running."
  []
  (some? @tick-executor))
