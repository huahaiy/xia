(ns xia.agent.supervisor
  "Supervise agent worker futures, stalls, cancellation, and restarts."
  (:require [xia.autonomous :as autonomous]
            [xia.agent.task-runtime :as task-runtime]
            [xia.prompt :as prompt]
            [xia.policy :as task-policy]))

(defn- current-time-ms
  []
  (long (System/currentTimeMillis)))

(defn emit-worker-event!
  [worker-state event]
  (let [now-ms (current-time-ms)]
    (swap! worker-state
           (fn [state]
             (let [phase (:phase event)
                   previous-phase (:phase state)
                   phase-start-ms (if (= phase previous-phase)
                                    (:phase-start-ms state now-ms)
                                    now-ms)
                   seq-no (inc (long (:seq state 0)))
                   event* (merge {:message nil
                                  :partial-content nil
                                  :round nil
                                  :tool-count nil
                                  :tool-id nil
                                  :tool-name nil
                                  :tool-risk? nil
                                  :tool-risk-mode nil
                                  :tool-risk-reason nil
                                  :parallel nil
                                  :checkpoint nil}
                                 event
                                 {:seq seq-no
                                  :phase phase
                                  :phase-start-ms phase-start-ms
                                  :last-event-ms now-ms
                                  :updated-at-ms now-ms})]
               (-> state
                   (merge (dissoc event* :events))
                   (assoc :phase phase
                          :phase-start-ms phase-start-ms
                          :last-event-ms now-ms
                          :updated-at-ms now-ms
                          :seq seq-no
                          :tool-risk? (or (:tool-risk? state)
                                          (true? (:tool-risk? event*)))
                          :tool-risk-mode (or (:tool-risk-mode state)
                                              (:tool-risk-mode event*))
                          :tool-risk-reason (or (:tool-risk-reason state)
                                                (:tool-risk-reason event*)))
                   (update :events (fnil conj []) event*)))))))

(defn- worker-timeout-ms
  [phase]
  (task-policy/supervisor-worker-timeout-ms phase))

(defn- worker-stalled?
  [{:keys [phase last-event-ms]}]
  (when (and phase last-event-ms)
    (> (- (current-time-ms) (long last-event-ms))
       (long (worker-timeout-ms phase)))))

(defn- worker-stall-ex
  [session-id channel iteration max-iterations autonomy-state worker-state]
  (let [tip (autonomous/current-frame autonomy-state)]
    (ex-info (str "Agent supervisor stopped a stalled worker during "
                  (some-> (:phase worker-state) name)
                  " phase")
             {:type :agent-stalled
              :session-id session-id
              :channel channel
              :phase (:phase worker-state)
              :iteration iteration
              :max-iterations max-iterations
              :current-focus (:title tip)
              :progress-status (:progress-status tip)
              :timeout-ms (worker-timeout-ms (:phase worker-state))
              :last-event-ms (:last-event-ms worker-state)
              :tool-id (:tool-id worker-state)
              :tool-name (:tool-name worker-state)
              :round (:round worker-state)})))

(defn- worker-stop-timeout-ex
  [session-id channel iteration max-iterations autonomy-state worker-state]
  (let [tip (autonomous/current-frame autonomy-state)]
    (ex-info (str "Agent supervisor could not stop a stalled worker during "
                  (some-> (:phase worker-state) name)
                  " phase")
             {:type :agent-stop-timeout
              :session-id session-id
              :channel channel
              :phase (:phase worker-state)
              :iteration iteration
              :max-iterations max-iterations
              :current-focus (:title tip)
              :progress-status (:progress-status tip)
              :grace-ms (task-policy/supervisor-restart-grace-ms)
              :tool-id (:tool-id worker-state)
              :tool-name (:tool-name worker-state)
              :round (:round worker-state)})))

(defn stop-worker!
  ([deps session-id]
   (stop-worker! deps session-id nil))
  ([deps session-id worker]
   (let [entry ((:live-run-entry-for-session deps) session-id)
         worker* (or worker (:worker-future entry))
         parallel-tool-futures (seq (:parallel-tool-futures entry))
         interrupted? (volatile! (Thread/interrupted))]
     (try
       ((:interrupt-worker-thread! deps) session-id)
       (when parallel-tool-futures
         ((:cancel-futures! deps) parallel-tool-futures))
       (when worker*
         (future-cancel worker*))
       (if (nil? worker*)
         true
         (let [deadline-ms (+ (current-time-ms)
                              (long (task-policy/supervisor-restart-grace-ms)))]
           (loop []
             (cond
               (future-done? worker*)
               true

               (>= (current-time-ms) deadline-ms)
               false

               :else
               (do
                 (try
                   (Thread/sleep 10)
                   (catch InterruptedException _
                     (vreset! interrupted? true)))
                 (recur))))))
       (finally
         (when @interrupted?
           (.interrupt (Thread/currentThread))))))))

(defn- worker-cancel-stop-timeout-ex
  [deps session-id channel iteration max-iterations autonomy-state worker-state]
  (let [tip (autonomous/current-frame autonomy-state)]
    (ex-info (str "Agent supervisor could not stop the worker after request cancellation during "
                  (some-> (:phase worker-state) name)
                  " phase")
             {:type :agent-stop-timeout
              :session-id session-id
              :channel channel
              :phase (:phase worker-state)
              :iteration iteration
              :max-iterations max-iterations
              :current-focus (:title tip)
              :progress-status (:progress-status tip)
              :grace-ms (task-policy/supervisor-restart-grace-ms)
              :cancel-reason ((:cancellation-reason deps) session-id)
              :tool-id (:tool-id worker-state)
              :tool-name (:tool-name worker-state)
              :round (:round worker-state)})))

(defn- worker-failure-summary
  [deps t]
  (let [type (some-> t ex-data :type)
        message (or (.getMessage ^Throwable t)
                    (some-> type name)
                    "worker failure")]
    ((:truncate-summary deps) message 240)))

(defn- wait-for-worker!
  [deps execution-context session-id channel iteration max-iterations autonomy-state worker-state worker]
  (let [handled-seq (volatile! 0)
        handle-events! (fn [snapshot]
                         (doseq [event (filter #(> (long (:seq % 0))
                                                   (long @handled-seq))
                                               (:events snapshot))]
                           (vreset! handled-seq (long (:seq event 0)))
                           ((:report-supervisor-status! deps)
                            (:phase event)
                            (:message event)
                            autonomy-state
                            iteration
                            max-iterations
                            :worker-phase (:phase event)
                            :round (:round event)
                            :partial-content (:partial-content event)
                            :tool-count (:tool-count event)
                            :tool-id (:tool-id event)
                            :tool-name (:tool-name event)
                            :parallel (:parallel event)
                            :intent-focus (:intent-focus event)
                            :intent-agenda-item (:intent-agenda-item event)
                            :intent-plan-step (:intent-plan-step event)
                            :intent-why (:intent-why event)
                            :intent-tool-name (:intent-tool-name event)
                            :intent-tool-args-summary (:intent-tool-args-summary event))
                           (when-let [checkpoint (:checkpoint event)]
                             ((:save-schedule-checkpoint! deps) execution-context checkpoint))))
        cancel-run! (fn [snapshot]
                      ((:report-supervisor-status! deps)
                       :cancelling
                       "Stopping current work"
                       autonomy-state
                       iteration
                       max-iterations
                       :worker-phase (:phase snapshot)
                       :round (:round snapshot)
                       :tool-count (:tool-count snapshot)
                       :tool-id (:tool-id snapshot)
                       :tool-name (:tool-name snapshot)
                       :parallel (:parallel snapshot)
                       :cancel-reason ((:cancellation-reason deps) session-id))
                      (throw (if (stop-worker! deps session-id worker)
                               ((:request-cancelled-ex deps)
                                session-id
                                ((:cancellation-reason deps) session-id))
                               (worker-cancel-stop-timeout-ex deps
                                                              session-id
                                                              channel
                                                              iteration
                                                              max-iterations
                                                              autonomy-state
                                                              snapshot))))]
    (loop []
      (let [snapshot @worker-state]
        (handle-events! snapshot)
        (cond
          ((:session-cancelled? deps) session-id)
          (cancel-run! snapshot)

          (future-done? worker)
          (do
            (handle-events! @worker-state)
            (try
              @worker
              (catch java.util.concurrent.ExecutionException e
                (throw (or (.getCause e) e)))))

          (worker-stalled? snapshot)
          (do
            (throw (if (stop-worker! deps session-id worker)
                     (worker-stall-ex session-id
                                      channel
                                      iteration
                                      max-iterations
                                      autonomy-state
                                      snapshot)
                     (worker-stop-timeout-ex session-id
                                             channel
                                             iteration
                                             max-iterations
                                             autonomy-state
                                             snapshot))))

          :else
          (do
            (try
              (Thread/sleep (long (task-policy/supervisor-tick-ms)))
              (catch InterruptedException _
                ((:request-session-cancel! deps)
                 session-id
                 (or ((:cancellation-reason deps) session-id)
                     "request interrupted"))
                (cancel-run! snapshot)))
            (recur)))))))

(defn run-supervised-agent-iteration
  [deps session-id channel resource-session-id local-doc-ids artifact-ids
   execution-context assistant-provider assistant-provider-id transient-messages
   working-memory-message update-working-memory? refresh-working-memory?
   max-tool-rounds autonomy-state max-iterations system-prompt-cache-entry
   turn-budget-state]
  (loop [attempt 0]
    (let [worker-token (Object.)
          worker-state (atom {:phase nil
                              :seq 0
                              :last-event-ms (current-time-ms)
                              :events []})
          _ ((:begin-worker-run! deps) session-id worker-token)
          worker (future
                   ((:register-worker-thread! deps) session-id worker-token)
                   (try
                     ((:run-agent-iteration deps)
                      session-id
                      channel
                      resource-session-id
                      local-doc-ids
                      artifact-ids
                      (assoc execution-context
                             :worker-token worker-token)
                      assistant-provider
                      assistant-provider-id
                      transient-messages
                      working-memory-message
                      update-working-memory?
                      refresh-working-memory?
                      max-tool-rounds
                      worker-state
                      system-prompt-cache-entry
                      turn-budget-state)
                     (finally
                       ((:clear-worker-run! deps) session-id worker-token))))]
      ((:register-worker-future! deps) session-id worker-token worker)
      (let [result (try
                     {:ok (wait-for-worker! deps
                                            execution-context
                                            session-id
                                            channel
                                            (:iteration execution-context)
                                            max-iterations
                                            autonomy-state
                                            worker-state
                                            worker)}
                     (catch Throwable t
                       {:error t}))]
        (if-let [t (:error result)]
          (let [worker-snapshot @worker-state
                task-id (:task-id execution-context)
                restart-window-ms (task-policy/task-restart-loop-window-ms)
                recent-restart-count (if task-id
                                       (task-runtime/recent-task-restart-count task-id
                                                                               restart-window-ms)
                                       0)
                restart-decision (task-policy/restart-policy-decision
                                  t
                                  worker-snapshot
                                  attempt
                                  :session-cancelled? ((:session-cancelled? deps) session-id)
                                  :recent-restart-count recent-restart-count
                                  :recent-restart-limit (task-policy/task-restart-loop-limit)
                                  :restart-window-ms restart-window-ms)
                max-restarts (:max-restarts restart-decision)
                attempt* (:attempt restart-decision)
                _ (prompt/policy-decision! (merge restart-decision
                                                  {:decision-type :restart-policy
                                                   :error (worker-failure-summary deps t)}))]
            (if (:allowed? restart-decision)
              (do
                ((:report-supervisor-status! deps)
                 :restarting
                 (str "Restarting iteration after "
                      (worker-failure-summary deps t)
                      " (attempt "
                      attempt*
                      "/"
                      max-restarts
                      ")")
                 autonomy-state
                 (:iteration execution-context)
                 max-iterations
                 :attempt attempt*
                 :max-restarts max-restarts
                 :failure-phase (some-> t ex-data :phase)
                 :worker-phase (:phase worker-snapshot)
                 :round (:round worker-snapshot)
                 :tool-id (:tool-id worker-snapshot)
                 :tool-name (:tool-name worker-snapshot))
                ((:save-schedule-checkpoint! deps)
                 execution-context
                 {:phase :restarting
                  :iteration (:iteration execution-context)
                  :summary (worker-failure-summary deps t)
                  :attempt attempt*
                  :session-id session-id
                  :failure-phase (some-> t ex-data :phase)})
                (Thread/sleep (long (:backoff-ms restart-decision)))
                (recur attempt*))
              (if (= :restart-loop (:mode restart-decision))
                (let [summary (str "Task restart loop detected after "
                                   (:recent-restart-count restart-decision)
                                   " recent restarts in "
                                   (quot (long (:restart-window-ms restart-decision)) 1000)
                                   "s. Investigate before resuming.")]
                  (when task-id
                    (task-runtime/record-task-restart-loop! task-id restart-decision summary))
                  (throw (ex-info summary
                                  {:type :task-restart-loop
                                   :task-id task-id
                                   :session-id session-id
                                   :channel channel
                                   :recent-restart-count (:recent-restart-count restart-decision)
                                   :recent-restart-limit (:recent-restart-limit restart-decision)
                                   :restart-window-ms (:restart-window-ms restart-decision)
                                   :failure-phase (:failure-phase restart-decision)
                                   :worker-phase (:worker-phase restart-decision)}
                                  t)))
                (throw t))))
          (:ok result))))))
