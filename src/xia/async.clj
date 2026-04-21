(ns xia.async
  "Shared bounded executors for internal Xia async work."
  (:require [taoensso.timbre :as log]
            [xia.task-policy :as task-policy])
  (:import [java.util.concurrent Callable ExecutorService LinkedBlockingQueue
            RejectedExecutionException RejectedExecutionHandler ThreadFactory
            ThreadPoolExecutor TimeUnit]))

(defonce ^:private installed-runtime-atom (atom nil))
(def ^:private default-shutdown-await-ms 10000)

(declare clear-runtime!)

(defn- make-runtime
  []
  {:executors-atom    (atom {})
   :accepting-atom    (atom true)
   :thread-counter    (atom 0)
   :runtime-lock      (Object.)})

(defn- maybe-current-runtime
  []
  @installed-runtime-atom)

(defn- current-runtime
  []
  (or (maybe-current-runtime)
      (throw (ex-info "Async runtime is not installed"
                      {:component :xia/async-runtime}))))

(defn- executors-atom
  []
  (:executors-atom (current-runtime)))

(defn- accepting-atom
  []
  (:accepting-atom (current-runtime)))

(defn- thread-counter-atom
  []
  (:thread-counter (current-runtime)))

(defn- runtime-lock
  []
  (:runtime-lock (current-runtime)))

(defn- daemon-thread-factory
  [prefix]
  (reify ThreadFactory
    (newThread [_ runnable]
      (doto (Thread. ^Runnable runnable
                     (str prefix "-" (swap! (thread-counter-atom) inc)))
        (.setDaemon true)))))

(defn- caller-runs-unless-shutdown-handler
  []
  (reify RejectedExecutionHandler
    (rejectedExecution [_ runnable executor]
      (if (.isShutdown ^ThreadPoolExecutor executor)
        (throw (RejectedExecutionException.
                "Async executor is shutting down"))
        (.run ^Runnable runnable)))))

(defn- abort-handler
  []
  (reify RejectedExecutionHandler
    (rejectedExecution [_ _ executor]
      (throw (RejectedExecutionException.
              (if (.isShutdown ^ThreadPoolExecutor executor)
                "Async executor is shutting down"
                "Async executor queue is full"))))))

(defn- executor-config
  [kind]
  (case kind
    :background
    {:max-threads (task-policy/async-background-max-threads)
     :queue-capacity (task-policy/async-background-queue-capacity)
     :thread-prefix "xia-async-bg"}

    :parallel
    {:max-threads (task-policy/async-parallel-max-threads)
     :queue-capacity (task-policy/async-parallel-queue-capacity)
     :thread-prefix "xia-async-par"}

    (throw (ex-info (str "Unknown async executor kind: " kind)
                    {:kind kind}))))

(defn- create-executor
  [kind]
  (let [{:keys [max-threads queue-capacity thread-prefix]}
        (executor-config kind)
        ^LinkedBlockingQueue queue (LinkedBlockingQueue. (int queue-capacity))
        rejection-handler (case kind
                            :background (abort-handler)
                            :parallel (caller-runs-unless-shutdown-handler))
        ^ThreadPoolExecutor exec (ThreadPoolExecutor.
                                  (int max-threads)
                                  (int max-threads)
                                  60 TimeUnit/SECONDS
                                  queue
                                  (daemon-thread-factory thread-prefix)
                                  rejection-handler)]
    (.allowCoreThreadTimeOut exec false)
    exec))

(defn- ensure-executor!
  [kind]
  (locking (runtime-lock)
    (when-not @(accepting-atom)
      (throw (RejectedExecutionException.
              "Async runtime is shutting down")))
    (let [executors* (executors-atom)
          ^ExecutorService exec (get @executors* kind)]
      (if (and exec (not (.isShutdown exec)))
        exec
        (let [new-exec (create-executor kind)]
          (swap! executors* assoc kind new-exec)
          new-exec)))))

(defn- convey-bindings
  [f]
  (let [bindings (get-thread-bindings)]
    (fn []
      (with-bindings* bindings f))))

(defn submit!
  ([kind f]
   (submit! kind nil f))
  ([kind description f]
   (let [task-fn (convey-bindings f)]
     (try
       (let [^ExecutorService exec (ensure-executor! kind)]
         (.submit exec
                  ^Callable
                  (fn []
                    (try
                      (task-fn)
                      (catch Throwable t
                        (log/error t "Async task failed"
                                   {:kind kind
                                    :description description})
                        (throw t))))))
       (catch RejectedExecutionException e
         (when-not (= "Async runtime is shutting down" (.getMessage e))
           (log/warn e "Async task submission rejected"
                     {:kind kind
                      :description description}))
         nil)))))

(defn submit-background!
  ([f]
   (submit-background! nil f))
  ([description f]
   (submit! :background description f)))

(defn submit-parallel!
  ([f]
   (submit-parallel! nil f))
  ([description f]
   (submit! :parallel description f)))

(defn- executor-snapshot
  [runtime]
  (locking (:runtime-lock runtime)
    (vec @(:executors-atom runtime))))

(defn- shutdown-executor-gracefully!
  [^ExecutorService exec]
  (.shutdown exec))

(defn- force-shutdown-executor!
  [^ExecutorService exec]
  (.shutdownNow exec))

(defn- await-executor!
  [kind ^ExecutorService exec timeout-ms]
  (try
    (let [terminated? (.awaitTermination exec (long timeout-ms) TimeUnit/MILLISECONDS)]
      (when-not terminated?
        (log/warn "Timed out waiting for async executor to stop"
                  {:kind kind
                   :timeout-ms timeout-ms
                   :active-count (when (instance? ThreadPoolExecutor exec)
                                   (.getActiveCount ^ThreadPoolExecutor exec))
                   :queued-count (when (instance? ThreadPoolExecutor exec)
                                   (.size (.getQueue ^ThreadPoolExecutor exec)))}))
      terminated?)
    (catch InterruptedException e
      (.interrupt (Thread/currentThread))
      (log/warn e "Interrupted while waiting for async executor shutdown"
                {:kind kind})
      false)))

(defn prepare-shutdown!
  "Stop accepting new async work and let already accepted work drain."
  []
  (when-let [runtime (maybe-current-runtime)]
    (locking (:runtime-lock runtime)
      (reset! (:accepting-atom runtime) false)
      (doseq [[_ ^ExecutorService exec] @(:executors-atom runtime)]
        (shutdown-executor-gracefully! exec))
      (count @(:executors-atom runtime)))))

(defn await-background-tasks!
  "Wait for accepted async tasks to finish after prepare-shutdown!.

   The name is intentionally aligned with subsystem shutdown hooks; this waits
   for both background and parallel executors because either kind may hold DB
   work."
  ([] (await-background-tasks! default-shutdown-await-ms))
  ([timeout-ms]
   (if-let [runtime (maybe-current-runtime)]
     (do
       (prepare-shutdown!)
       (let [deadline-ms (+ (System/currentTimeMillis) (long timeout-ms))
             results     (doall
                           (for [[kind exec] (executor-snapshot runtime)
                                 :let [remaining-ms (max 1 (- deadline-ms
                                                              (System/currentTimeMillis)))]]
                             (await-executor! kind exec remaining-ms)))]
         (every? true? results)))
     true)))

(defn install-runtime!
  ([] (install-runtime! (make-runtime)))
  ([runtime]
   (when-let [current (maybe-current-runtime)]
     (when-not (identical? current runtime)
       (clear-runtime!)))
   (reset! (:accepting-atom runtime) true)
   (reset! installed-runtime-atom runtime)
   runtime))

(defn clear-runtime!
  []
  (when-let [runtime (maybe-current-runtime)]
    (prepare-shutdown!)
    (locking (:runtime-lock runtime)
      (doseq [[kind ^ExecutorService exec] @(:executors-atom runtime)]
        (force-shutdown-executor! exec)
        (await-executor! kind exec 25))
      (reset! (:executors-atom runtime) {})
      (reset! (:accepting-atom runtime) true)
      (reset! (:thread-counter runtime) 0)
      (reset! installed-runtime-atom nil)))
  nil)
