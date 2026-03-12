(ns xia.scheduler
  "Background scheduler — executes due schedules on a timer.

   Runs a background thread that wakes every 60 seconds, finds
   schedules whose next-run <= now, and executes them.

   Two execution modes:
   - :tool   — calls tool/execute-tool directly (fast, deterministic)
   - :prompt — creates a session and runs through the full agent loop
               (LLM decides what tools to use)

   Lifecycle: start! → (tick every 60s) → stop!"
  (:require [clojure.tools.logging :as log]
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

(defn- execute-tool-schedule
  "Execute a :tool type schedule."
  [{:keys [id tool-id tool-args trusted?]}]
  (let [started (java.util.Date.)]
    (try
      (let [result (tool/execute-tool tool-id (or tool-args {})
                                      {:channel :scheduler
                                       :schedule-id id
                                       :approval-bypass? trusted?})]
        (schedule/record-run! id
          {:started-at  started
           :finished-at (java.util.Date.)
           :status      (if (:error result) :error :success)
           :result      (str result)
           :error       (:error result)}))
      (catch Exception e
        (schedule/record-run! id
          {:started-at  started
           :finished-at (java.util.Date.)
           :status      :error
           :error       (.getMessage e)})))))

(defn- execute-prompt-schedule
  "Execute a :prompt type schedule — runs through the full agent loop."
  [{:keys [id prompt trusted?]}]
  (let [started    (java.util.Date.)
        session-id (db/create-session! :scheduler)]
    (try
      (wm/ensure-wm! session-id)
      (let [result (agent/process-message session-id prompt
                                          :channel :scheduler
                                          :tool-context {:schedule-id id
                                                         :approval-bypass? trusted?})]
        (schedule/record-run! id
          {:started-at  started
           :finished-at (java.util.Date.)
           :status      :success
           :result      (str result)}))
      (catch Exception e
        (schedule/record-run! id
          {:started-at  started
           :finished-at (java.util.Date.)
           :status      :error
           :error       (.getMessage e)}))
      (finally
        (wm/snapshot! session-id)
        (wm/clear-wm! session-id)))))

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
