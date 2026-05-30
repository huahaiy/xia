(ns xia.policy.scheduler
  "Schedule and scheduler policy."
  (:require [xia.config :as cfg]))

(def ^:private default-schedule-failure-backoff-minutes 15)
(def ^:private default-schedule-max-failure-backoff-minutes (* 12 60))
(def ^:private default-schedule-pause-after-repeated-failures 3)
(def ^:private default-max-schedules 50)
(def ^:private default-min-schedule-interval-minutes 5)
(def ^:private default-scheduler-max-concurrent-runs 4)

(defn schedule-failure-backoff-minutes
  []
  (cfg/positive-long :schedule/failure-backoff-minutes
                     default-schedule-failure-backoff-minutes))

(defn schedule-max-failure-backoff-minutes
  []
  (cfg/positive-long :schedule/max-failure-backoff-minutes
                     default-schedule-max-failure-backoff-minutes))

(defn schedule-pause-after-repeated-failures
  []
  (cfg/positive-long :schedule/pause-after-repeated-failures
                     default-schedule-pause-after-repeated-failures))

(defn max-schedules
  []
  (cfg/positive-long :schedule/max-schedules
                     default-max-schedules))

(defn min-schedule-interval-minutes
  []
  (cfg/positive-long :schedule/min-interval-minutes
                     default-min-schedule-interval-minutes))

(defn scheduler-max-concurrent-runs
  []
  (cfg/positive-long :scheduler/max-concurrent-runs
                     default-scheduler-max-concurrent-runs))

(defn schedule-frequency-policy
  [{:keys [interval-minutes spec]}]
  (let [minimum (long (min-schedule-interval-minutes))]
    (cond
      (some? interval-minutes)
      {:decision-type :schedule-frequency-policy
       :allowed? false
       :mode :interval-limit
       :interval-minutes (long interval-minutes)
       :min-interval-minutes minimum
       :reason (str "Interval too frequent (minimum " minimum " minutes)")}

      :else
      {:decision-type :schedule-frequency-policy
       :allowed? false
       :mode :calendar-frequency
       :spec spec
       :min-interval-minutes minimum
       :reason (str "Schedule too frequent (minimum " minimum " minutes)")})))

(defn schedule-count-policy
  [current-count]
  (let [current-count (long current-count)
        max-schedules (long (max-schedules))
        allowed? (< current-count max-schedules)]
    {:decision-type :schedule-count-policy
     :allowed? allowed?
     :mode (if allowed? :within-limit :schedule-limit)
     :current-count current-count
     :max-schedules max-schedules
     :reason (when-not allowed?
               (str "Too many schedules (max " max-schedules ")"))}))

(defn schedule-failure-backoff-ms
  ^long
  [consecutive-failures]
  (* 60 1000
     (min (long (schedule-max-failure-backoff-minutes))
          (* (long (schedule-failure-backoff-minutes))
             (long (Math/pow 2.0 (double (max 0 (dec (long consecutive-failures))))))))))

(defn schedule-failure-policy
  [{:keys [same-failure? previous-failures now]}]
  (let [previous-failures (long (or previous-failures 0))
        consecutive-failures (if same-failure?
                               (inc previous-failures)
                               1)
        pause-threshold (long (schedule-pause-after-repeated-failures))
        paused? (and same-failure?
                     (>= consecutive-failures pause-threshold))
        backoff-ms (when-not paused?
                     (long (schedule-failure-backoff-ms consecutive-failures)))
        backoff-until (when backoff-ms
                        (java.util.Date.
                         (long (+ (.getTime ^java.util.Date now) backoff-ms))))]
    {:decision-type :schedule-failure-policy
     :mode (if paused? :pause :backoff)
     :same-failure? (boolean same-failure?)
     :consecutive-failures consecutive-failures
     :pause-threshold pause-threshold
     :backoff-ms backoff-ms
     :backoff-minutes (when backoff-ms
                        (long (/ backoff-ms 60000)))
     :max-backoff-minutes (long (schedule-max-failure-backoff-minutes))
     :backoff-until backoff-until
     :reason (if paused?
               "Paused after repeated identical schedule failures"
               "Applied schedule failure backoff")}))
