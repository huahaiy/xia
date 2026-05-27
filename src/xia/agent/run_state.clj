(ns xia.agent.run-state
  "Mutable registry for active agent session/task runs.

   This keeps concurrency, cancellation, and worker bookkeeping out of the
   message-processing loop."
  (:import [java.util.concurrent Future]))

(defn make-runtime
  []
  {:active-session-turns-atom (atom #{})
   :session-turn-reservations-atom (atom {})
   :active-session-runs-atom  (atom {})
   :active-task-runs-atom     (atom {})
   :idle-monitor              (Object.)})

(defn- active-session-turns-atom
  [runtime]
  (:active-session-turns-atom runtime))

(defn- active-session-runs-atom
  [runtime]
  (:active-session-runs-atom runtime))

(defn- session-turn-reservations-atom
  [runtime]
  (:session-turn-reservations-atom runtime))

(defn- active-task-runs-atom
  [runtime]
  (:active-task-runs-atom runtime))

(defn- idle-monitor
  [runtime]
  (:idle-monitor runtime))

(defn current-time-ms
  []
  (long (System/currentTimeMillis)))

(defn clear-runtime-state!
  [runtime]
  (reset! (:active-session-turns-atom runtime) #{})
  (reset! (:session-turn-reservations-atom runtime) {})
  (reset! (:active-session-runs-atom runtime) {})
  (reset! (:active-task-runs-atom runtime) {})
  (locking (:idle-monitor runtime)
    (.notifyAll ^Object (:idle-monitor runtime)))
  nil)

(defn runtime-activity
  [runtime]
  {:active-session-turn-count (count @(active-session-turns-atom runtime))
   :active-session-run-count  (count @(active-session-runs-atom runtime))
   :active-task-run-count     (count @(active-task-runs-atom runtime))})

(defn reserve-next-session-turn!
  [runtime session-id metadata]
  (when session-id
    (let [token (Object.)]
      (locking (idle-monitor runtime)
        (let [reservations* (session-turn-reservations-atom runtime)]
          (when-not (contains? @reservations* session-id)
            (swap! reservations* assoc session-id (assoc metadata :token token))
            (.notifyAll ^Object (idle-monitor runtime))
            token))))))

(defn clear-session-turn-reservation!
  [runtime session-id token]
  (when (and session-id token)
    (locking (idle-monitor runtime)
      (swap! (session-turn-reservations-atom runtime)
             (fn [reservations]
               (if (= token (get-in reservations [session-id :token]))
                 (dissoc reservations session-id)
                 reservations)))
      (.notifyAll ^Object (idle-monitor runtime))))
  nil)

(defn try-acquire-session-turn!
  ([runtime session-id]
   (try-acquire-session-turn! runtime session-id nil))
  ([runtime session-id reservation-token]
   (locking (idle-monitor runtime)
     (let [active-session-turns* (active-session-turns-atom runtime)
           reservations* (session-turn-reservations-atom runtime)
           active @active-session-turns*
           reservation (get @reservations* session-id)]
       (cond
         (contains? active session-id)
         false

         reservation
         (if (= reservation-token (:token reservation))
           (do
             (swap! reservations* dissoc session-id)
             (swap! active-session-turns* conj session-id)
             true)
           false)

         reservation-token
         false

         :else
         (do
           (swap! active-session-turns* conj session-id)
           true))))))

(defn release-session-turn!
  [runtime session-id]
  (when session-id
    (locking (idle-monitor runtime)
      (swap! (active-session-turns-atom runtime) disj session-id)
      (.notifyAll ^Object (idle-monitor runtime))))
  nil)

(defn with-session-turn-lock
  ([runtime session-id f]
   (with-session-turn-lock runtime session-id nil f))
  ([runtime session-id reservation-token f]
   (if session-id
     (if (try-acquire-session-turn! runtime session-id reservation-token)
       (try
         (f)
         (finally
           (release-session-turn! runtime session-id)))
       (throw (ex-info "Session is already processing another request"
                       {:type :session-busy
                        :status 409
                        :error "session is busy"
                        :session-id session-id})))
     (f))))

(defn with-session-run
  [runtime session-id f]
  (if session-id
    (let [run-id (Object.)]
      (swap! (active-session-runs-atom runtime) assoc session-id
             {:run-id run-id
              :supervisor-thread (Thread/currentThread)
              :task-id nil
              :child-session-ids #{}
              :cancelled? false
              :cancel-reason nil})
      (locking (idle-monitor runtime)
        (.notifyAll ^Object (idle-monitor runtime)))
      (try
        (f)
        (finally
          (swap! (active-session-runs-atom runtime)
                 (fn [runs]
                   (if (= run-id (get-in runs [session-id :run-id]))
                     (dissoc runs session-id)
                     runs)))
          (locking (idle-monitor runtime)
            (.notifyAll ^Object (idle-monitor runtime))))))
    (f)))

(defn session-run-entry
  [runtime session-id]
  (when session-id
    (get @(active-session-runs-atom runtime) session-id)))

(defn task-run-entry
  [runtime task-id]
  (when task-id
    (get @(active-task-runs-atom runtime) task-id)))

(defn session-bound-task-id
  [runtime session-id]
  (some-> (session-run-entry runtime session-id) :task-id))

(defn live-run-entry-for-session
  [runtime session-id]
  (or (some->> session-id
               (session-bound-task-id runtime)
               (task-run-entry runtime))
      (session-run-entry runtime session-id)))

(defn- wait-for-idle!
  [runtime entry-fn id timeout-ms]
  (let [timeout-ms* (long (max 0 (long timeout-ms)))
        deadline (+ (current-time-ms) timeout-ms*)
        monitor (idle-monitor runtime)]
    (locking monitor
      (loop []
        (cond
          (nil? (entry-fn runtime id))
          true

          (>= (current-time-ms) deadline)
          false

          :else
          (let [remaining-ms (max 1 (- deadline (current-time-ms)))]
            (.wait ^Object monitor (long remaining-ms))
            (recur)))))))

(defn wait-for-session-idle!
  [runtime session-id timeout-ms]
  (wait-for-idle! runtime session-run-entry session-id timeout-ms))

(defn wait-for-task-idle!
  [runtime task-id timeout-ms]
  (wait-for-idle! runtime task-run-entry task-id timeout-ms))

(defn- update-session-run-entry!
  [runtime session-id f]
  (when session-id
    (swap! (active-session-runs-atom runtime)
           (fn [runs]
             (if-let [entry (get runs session-id)]
               (assoc runs session-id (f entry))
               runs)))))

(defn- update-task-run-entry!
  [runtime task-id f]
  (when task-id
    (swap! (active-task-runs-atom runtime)
           (fn [runs]
             (if-let [entry (get runs task-id)]
               (assoc runs task-id (f entry))
               runs)))))

(defn register-task-run!
  [runtime session-id task-id task-turn-id]
  (when (and session-id task-id task-turn-id)
    (when-let [entry (session-run-entry runtime session-id)]
      (let [session-run-id (:run-id entry)
            task-run-id (Object.)
            task-entry {:task-id task-id
                        :task-turn-id task-turn-id
                        :session-id session-id
                        :task-run-id task-run-id
                        :session-run-id session-run-id
                        :supervisor-thread (:supervisor-thread entry)
                        :child-session-ids (:child-session-ids entry)
                        :cancelled? (:cancelled? entry)
                        :cancel-reason (:cancel-reason entry)}]
        (swap! (active-task-runs-atom runtime) assoc task-id task-entry)
        (update-session-run-entry! runtime
                                   session-id
                                   (fn [run]
                                     (if (= session-run-id (:run-id run))
                                       (assoc run
                                              :task-id task-id
                                              :child-session-ids #{})
                                       run)))
        (locking (idle-monitor runtime)
          (.notifyAll ^Object (idle-monitor runtime)))
        task-entry))))

(defn clear-task-run!
  [runtime session-id task-id task-turn-id task-run-id]
  (when task-id
    (let [expected-task-run-id (or task-run-id
                                   (some-> (task-run-entry runtime task-id) :task-run-id))]
      (swap! (active-task-runs-atom runtime)
             (fn [runs]
               (if-let [entry (get runs task-id)]
                 (if (and (or (nil? expected-task-run-id)
                              (= expected-task-run-id (:task-run-id entry)))
                          (or (nil? session-id)
                              (= session-id (:session-id entry)))
                          (or (nil? task-turn-id)
                              (= task-turn-id (:task-turn-id entry))))
                   (dissoc runs task-id)
                   runs)
                 runs)))
      (when session-id
        (update-session-run-entry! runtime
                                   session-id
                                   (fn [entry]
                                     (if (= task-id (:task-id entry))
                                       (assoc entry
                                              :task-id nil)
                                       entry))))
      (locking (idle-monitor runtime)
        (.notifyAll ^Object (idle-monitor runtime))))))

(defn register-child-session!
  [runtime parent-session-id child-session-id]
  (when (and parent-session-id
             child-session-id
             (not= parent-session-id child-session-id))
    (if-let [task-id (session-bound-task-id runtime parent-session-id)]
      (update-task-run-entry! runtime
                              task-id
                              #(update % :child-session-ids (fnil conj #{}) child-session-id))
      (update-session-run-entry! runtime
                                 parent-session-id
                                 #(update % :child-session-ids (fnil conj #{}) child-session-id)))))

(defn unregister-child-session!
  [runtime parent-session-id child-session-id]
  (when (and parent-session-id
             child-session-id
             (not= parent-session-id child-session-id))
    (if-let [task-id (session-bound-task-id runtime parent-session-id)]
      (update-task-run-entry! runtime
                              task-id
                              #(update % :child-session-ids disj child-session-id))
      (update-session-run-entry! runtime
                                 parent-session-id
                                 #(update % :child-session-ids disj child-session-id)))))

(defn begin-worker-run!
  [runtime session-id worker-token]
  (if-let [task-id (session-bound-task-id runtime session-id)]
    (update-task-run-entry! runtime
                            task-id
                            #(assoc % :worker-token worker-token
                                    :worker-thread nil
                                    :worker-future nil
                                    :parallel-tool-futures []))
    (update-session-run-entry! runtime
                               session-id
                               #(assoc % :worker-token worker-token
                                       :worker-thread nil
                                       :worker-future nil
                                       :parallel-tool-futures []))))

(defn register-worker-thread!
  [runtime session-id worker-token]
  (if-let [task-id (session-bound-task-id runtime session-id)]
    (update-task-run-entry! runtime
                            task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (assoc entry :worker-thread (Thread/currentThread))
                                entry)))
    (update-session-run-entry! runtime
                               session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (assoc entry :worker-thread (Thread/currentThread))
                                   entry)))))

(defn clear-worker-thread!
  [runtime session-id worker-token]
  (if-let [task-id (session-bound-task-id runtime session-id)]
    (update-task-run-entry! runtime
                            task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (assoc entry :worker-thread nil)
                                entry)))
    (update-session-run-entry! runtime
                               session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (assoc entry :worker-thread nil)
                                   entry)))))

(defn register-worker-future!
  [runtime session-id worker-token worker]
  (if-let [task-id (session-bound-task-id runtime session-id)]
    (update-task-run-entry! runtime
                            task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (assoc entry :worker-future worker)
                                entry)))
    (update-session-run-entry! runtime
                               session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (assoc entry :worker-future worker)
                                   entry)))))

(defn clear-worker-run!
  [runtime session-id worker-token]
  (if-let [task-id (session-bound-task-id runtime session-id)]
    (update-task-run-entry! runtime
                            task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (assoc entry
                                       :worker-token nil
                                       :worker-thread nil
                                       :worker-future nil
                                       :parallel-tool-futures [])
                                entry)))
    (update-session-run-entry! runtime
                               session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (assoc entry
                                          :worker-token nil
                                          :worker-thread nil
                                          :worker-future nil
                                          :parallel-tool-futures [])
                                   entry)))))

(defn register-parallel-tool-futures!
  [runtime session-id worker-token futures]
  (if-let [task-id (session-bound-task-id runtime session-id)]
    (update-task-run-entry! runtime
                            task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (update entry
                                        :parallel-tool-futures
                                        (fn [existing]
                                          (->> (concat (or existing []) futures)
                                               distinct
                                               vec)))
                                entry)))
    (update-session-run-entry! runtime
                               session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (update entry
                                           :parallel-tool-futures
                                           (fn [existing]
                                             (->> (concat (or existing []) futures)
                                                  distinct
                                                  vec)))
                                   entry)))))

(defn clear-parallel-tool-futures!
  [runtime session-id worker-token futures]
  (if-let [task-id (session-bound-task-id runtime session-id)]
    (update-task-run-entry! runtime
                            task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (update entry
                                        :parallel-tool-futures
                                        (fn [existing]
                                          (let [to-clear (set futures)]
                                            (->> (or existing [])
                                                 (remove to-clear)
                                                 vec))))
                                entry)))
    (update-session-run-entry! runtime
                               session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (update entry
                                           :parallel-tool-futures
                                           (fn [existing]
                                             (let [to-clear (set futures)]
                                               (->> (or existing [])
                                                    (remove to-clear)
                                                    vec))))
                                   entry)))))

(defn interrupt-worker-thread!
  [runtime session-id]
  (when-let [^Thread worker-thread (:worker-thread (live-run-entry-for-session runtime session-id))]
    (when (not= (Thread/currentThread) worker-thread)
      (.interrupt worker-thread))
    true))

(defn cancel-futures!
  [futures]
  (doseq [f futures]
    (future-cancel f)))

(defn request-session-cancel!
  [runtime session-id reason & {:keys [interrupt-supervisor?]
                                :or {interrupt-supervisor? false}}]
  (let [session-entry* (atom nil)
        task-entry*    (atom nil)]
    (when session-id
      (swap! (active-session-runs-atom runtime)
             (fn [runs]
               (if-let [entry (get runs session-id)]
                 (let [updated (assoc entry
                                      :cancelled? true
                                      :cancel-reason (or (:cancel-reason entry)
                                                         reason))]
                   (reset! session-entry* updated)
                   (assoc runs session-id updated))
                 runs)))
      (when-let [task-id (:task-id @session-entry*)]
        (swap! (active-task-runs-atom runtime)
               (fn [runs]
                 (if-let [entry (get runs task-id)]
                   (let [updated (assoc entry
                                        :cancelled? true
                                        :cancel-reason (or (:cancel-reason entry)
                                                           reason))]
                     (reset! task-entry* updated)
                     (assoc runs task-id updated))
                   runs))))
      (when-let [entry (or @task-entry* @session-entry*)]
        (when (and interrupt-supervisor?
                   (not= (Thread/currentThread) ^Thread (:supervisor-thread entry)))
          (.interrupt ^Thread (:supervisor-thread entry)))
        (when (and (:worker-thread entry)
                   (not= (Thread/currentThread) ^Thread (:worker-thread entry)))
          (.interrupt ^Thread (:worker-thread entry)))
        (when-let [parallel-tool-futures (seq (:parallel-tool-futures entry))]
          (cancel-futures! parallel-tool-futures))
        (when-let [^Future worker-future (:worker-future entry)]
          (future-cancel worker-future))
        (doseq [child-session-id (or (:child-session-ids @task-entry*)
                                     (:child-session-ids @session-entry*))]
          (when (not= child-session-id session-id)
            (request-session-cancel! runtime
                                     child-session-id
                                     reason
                                     :interrupt-supervisor? true)))
        true))))

(defn cancel-all-sessions!
  [runtime reason cancel-session!]
  (let [session-ids (keys @(active-session-runs-atom runtime))]
    (doseq [session-id session-ids]
      (cancel-session! session-id reason))
    (count session-ids)))
