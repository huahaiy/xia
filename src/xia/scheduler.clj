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
            [xia.hippocampus :as hippo]
            [xia.tool :as tool]
            [xia.schedule :as schedule]
            [xia.working-memory :as wm])
  (:import [java.util.concurrent Executors ScheduledExecutorService TimeUnit]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private executor (atom nil))
(defonce ^:private running-schedules (atom #{}))
(defonce ^:private maintenance-running? (atom false))
(defonce ^:private last-maintenance-at (atom nil))

(def ^:private maintenance-interval-ms (* 24 60 60 1000))

;; ---------------------------------------------------------------------------
;; Execution
;; ---------------------------------------------------------------------------

(defn- schedule-tool-context
  [schedule-id trusted? audit-log]
  {:channel          :scheduler
   :schedule-id      schedule-id
   :autonomous-run?  true
   :approval-bypass? trusted?
   :audit-log        audit-log})

(defn- execute-tool-schedule
  "Execute a :tool type schedule."
  [{:keys [id tool-id tool-args trusted?]}]
  (let [started   (java.util.Date.)
        audit-log (atom [])
        context   (schedule-tool-context id trusted? audit-log)]
    (schedule/save-task-checkpoint!
      id
      {:phase :tool
       :summary (str "Running scheduled tool " (name tool-id) ".")
       :tool-id tool-id})
    (try
      (let [result (tool/execute-tool tool-id (or tool-args {}) context)]
        (schedule/record-run! id
          {:started-at  started
           :finished-at (java.util.Date.)
           :status      (if (:error result) :error :success)
           :result      (str result)
           :actions     @audit-log
           :error       (:error result)})
        (if-let [error-message (:error result)]
          (schedule/record-task-failure! id error-message)
          (schedule/record-task-success! id
                                         (or (:summary result)
                                             (str result)))))
      (catch Exception e
        (schedule/record-run! id
          {:started-at  started
           :finished-at (java.util.Date.)
           :status      :error
           :actions     @audit-log
           :error       (.getMessage e)})
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
  (let [started            (java.util.Date.)
        resumed-session-id (schedule/resumable-session-id id)
        session-id         (or resumed-session-id
                               (db/create-session! :scheduler))
        audit-log  (atom [])
        prompt*    (schedule/augment-prompt-with-recovery-context id prompt)]
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
      (let [result (agent/process-message session-id prompt*
                                          :channel :scheduler
                                          :tool-context (schedule-tool-context id
                                                                              trusted?
                                                                              audit-log))]
        (schedule/record-run! id
          {:started-at  started
           :finished-at (java.util.Date.)
           :status      :success
           :actions     @audit-log
           :result      (str result)})
        (schedule/record-task-success! id result))
      (catch Exception e
        (schedule/record-run! id
          {:started-at  started
           :finished-at (java.util.Date.)
           :status      :error
           :actions     @audit-log
           :error       (.getMessage e)})
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
        (log/info "Executing schedule:" (name id) "type:" (:type sched))
        (case (:type sched)
          :tool   (execute-tool-schedule sched)
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
    (let [now  (java.util.Date.)
          due  (schedule/due-schedules now)]
      (when (seq due)
        (log/info "Scheduler tick:" (count due) "schedule(s) due")
        (doseq [sched due]
          ;; Execute each in a future to avoid blocking the tick thread
          (future (execute-schedule! sched))))
      (when (backup/backup-due?)
        (future
          (try
            (backup/run-scheduled-backup!)
            (catch Exception e
              (log/error e "Automatic database backup failed")))))
      (when (and (or (nil? @last-maintenance-at)
                     (>= (- (.getTime now) (.getTime ^java.util.Date @last-maintenance-at))
                         maintenance-interval-ms))
                 (compare-and-set! maintenance-running? false true))
        (future
          (try
            (hippo/consolidate-if-pending!)
            (hippo/maintain-knowledge! now)
            (reset! last-maintenance-at now)
            (catch Exception e
              (log/error e "Background maintenance failed"))
            (finally
              (reset! maintenance-running? false))))))
    (catch Exception e
      (log/error e "Scheduler tick failed"))))

;; ---------------------------------------------------------------------------
;; Lifecycle
;; ---------------------------------------------------------------------------

(defn start!
  "Start the background scheduler. Ticks every 60 seconds."
  []
  (when @executor
    (log/warn "Scheduler already running"))
  (when-not @executor
    (let [^ScheduledExecutorService exec (Executors/newSingleThreadScheduledExecutor)]
      (.scheduleAtFixedRate exec ^Runnable tick! 60 60 TimeUnit/SECONDS)
      (reset! executor exec)
      (log/info "Scheduler started (60s interval)"))))

(defn stop!
  "Stop the background scheduler gracefully."
  []
  (when-let [^ScheduledExecutorService exec @executor]
    (.shutdown exec)
    (try
      (.awaitTermination exec 30 TimeUnit/SECONDS)
      (catch InterruptedException _
        (.shutdownNow exec)))
    (reset! executor nil)
    (reset! maintenance-running? false)
    (reset! last-maintenance-at nil)
    (log/info "Scheduler stopped")))

(defn running?
  "Check if the scheduler is currently running."
  []
  (some? @executor))
