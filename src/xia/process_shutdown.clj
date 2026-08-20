(ns xia.process-shutdown
  "Coordinate process signals with Xia's orderly runtime cleanup."
  (:require [taoensso.timbre :as log])
  (:import [sun.misc Signal SignalHandler]))

(def ^:private termination-signals ["TERM" "INT"])

(defn ^:no-doc run-termination!
  "Run cleanup and exit once for the first delivered termination signal."
  [handling? cleanup exit! signal-name signal-number]
  (when (compare-and-set! handling? false true)
    (log/info "Received" signal-name "signal; stopping Xia")
    (try
      (cleanup)
      (catch Throwable e
        (log/error e "Failed to stop Xia cleanly after" signal-name))
      (finally
        (exit! (+ 128 (long signal-number)))))))

(defn- install-signal-handler!
  [signal-name handling? cleanup exit!]
  (try
    (let [signal  (Signal. signal-name)
          handler (reify SignalHandler
                    (handle [_ delivered]
                      (run-termination! handling?
                                        cleanup
                                        exit!
                                        (.getName ^Signal delivered)
                                        (.getNumber ^Signal delivered))))
          previous (Signal/handle signal handler)]
      {:signal signal
       :previous previous})
    (catch Throwable e
      ;; Runtime shutdown hooks remain the portable fallback on platforms that
      ;; do not expose TERM or INT through sun.misc.Signal.
      (log/debug e "Could not install process signal handler" signal-name)
      nil)))

(defn register!
  "Register orderly TERM/INT handling plus a JVM-shutdown fallback hook."
  [cleanup]
  (let [handling? (atom false)
        exit!     #(System/exit (int %))
        hook      (Thread.
                   ^Runnable
                   (reify Runnable
                     (run [_]
                       (cleanup)))
                   "xia-shutdown")]
    (.addShutdownHook (Runtime/getRuntime) hook)
    {:hook hook
     :signals (into []
                    (keep #(install-signal-handler! % handling? cleanup exit!))
                    termination-signals)}))

(defn remove!
  "Remove handlers previously returned by register!."
  [{:keys [hook signals]}]
  (doseq [{:keys [signal previous]} signals]
    (try
      (Signal/handle ^Signal signal ^SignalHandler previous)
      (catch Throwable _)))
  (when hook
    (try
      (.removeShutdownHook (Runtime/getRuntime) ^Thread hook)
      (catch IllegalStateException _)
      (catch IllegalArgumentException _)))
  nil)
