(ns xia.policy.async
  "Async executor sizing policy."
  (:require [xia.config :as cfg]))

(def ^:private default-async-background-max-threads 4)
(def ^:private default-async-background-queue-capacity 256)
(def ^:private default-async-parallel-max-threads
  (max 4 (.availableProcessors (Runtime/getRuntime))))
(def ^:private default-async-parallel-queue-capacity 256)

(defn async-background-max-threads
  []
  (cfg/positive-long :async/background-max-threads
                     default-async-background-max-threads))

(defn async-background-queue-capacity
  []
  (cfg/positive-long :async/background-queue-capacity
                     default-async-background-queue-capacity))

(defn async-parallel-max-threads
  []
  (cfg/positive-long :async/parallel-max-threads
                     default-async-parallel-max-threads))

(defn async-parallel-queue-capacity
  []
  (cfg/positive-long :async/parallel-queue-capacity
                     default-async-parallel-queue-capacity))
