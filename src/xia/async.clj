(ns xia.async
  "Shared bounded executors for internal Xia async work."
  (:require [taoensso.timbre :as log]
            [xia.task-policy :as task-policy])
  (:import [java.util.concurrent Callable ExecutorService LinkedBlockingQueue
            RejectedExecutionException RejectedExecutionHandler ThreadFactory
            ThreadPoolExecutor TimeUnit]))

(defonce ^:private executors (atom {}))
(defonce ^:private thread-counter (atom 0))

(defn- daemon-thread-factory
  [prefix]
  (reify ThreadFactory
    (newThread [_ runnable]
      (doto (Thread. ^Runnable runnable
                     (str prefix "-" (swap! thread-counter inc)))
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
  (locking executors
    (let [^ExecutorService exec (get @executors kind)]
      (if (and exec (not (.isShutdown exec)))
        exec
        (let [new-exec (create-executor kind)]
          (swap! executors assoc kind new-exec)
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
   (let [^ExecutorService exec (ensure-executor! kind)
         task-fn (convey-bindings f)]
     (try
       (.submit exec
                ^Callable
                (fn []
                  (task-fn)))
       (catch RejectedExecutionException e
         (log/warn e "Async task submission rejected"
                   {:kind kind
                    :description description})
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
