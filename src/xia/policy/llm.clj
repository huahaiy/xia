(ns xia.policy.llm
  "LLM retry and provider policy."
  (:require [clojure.string :as str]
            [xia.config :as cfg]))

(def ^:private default-llm-retry-statuses #{408 409 425 429 500 502 503 504})
(def ^:private default-max-provider-retry-rounds 4)
(def ^:private default-max-provider-retry-wait-ms 300000)

(defn llm-max-provider-retry-rounds
  []
  (cfg/positive-long :llm/max-provider-retry-rounds
                     default-max-provider-retry-rounds))

(defn llm-max-provider-retry-wait-ms
  []
  (cfg/positive-long :llm/max-provider-retry-wait-ms
                     default-max-provider-retry-wait-ms))

(defn llm-retry-after-ms
  [headers now-ms-fn]
  (when-let [raw (some-> (or (get headers "retry-after")
                             (get headers "Retry-After"))
                         str)]
    (let [value (str/trim raw)]
      (or (try
            (* 1000 (max 0 (Long/parseLong value)))
            (catch Exception _
              nil))
          (try
            (max 0
                 (- (.toEpochMilli (.toInstant (java.time.ZonedDateTime/parse value
                                                                              java.time.format.DateTimeFormatter/RFC_1123_DATE_TIME)))
                    (long (now-ms-fn))))
            (catch Exception _
              nil))))))

(defn llm-retryable-error?
  [^Throwable e]
  (let [status (some-> e ex-data :status)]
    (or (contains? default-llm-retry-statuses status)
        (boolean
         (some #(instance? Throwable %)
               (filter (fn [cause]
                         (or (instance? java.util.concurrent.TimeoutException cause)
                             (instance? java.net.http.HttpTimeoutException cause)
                             (instance? java.net.http.HttpConnectTimeoutException cause)
                             (instance? java.io.IOException cause)))
                       (take-while some? (iterate ex-cause e))))))))

(defn llm-retry-sleep-ms
  [started-at round max-retry-rounds max-retry-wait-ms requested-delay-ms now-ms-fn]
  (let [remaining-ms (- (long max-retry-wait-ms)
                        (- (long (now-ms-fn)) (long started-at)))]
    (when (and (< (long round) (long max-retry-rounds))
               (pos? remaining-ms)
               (pos? (long (or requested-delay-ms 0))))
      (min remaining-ms (long requested-delay-ms)))))

(defn provider-rate-limit-policy
  [provider-id workload limit]
  {:decision-type :provider-rate-limit-policy
   :allowed? false
   :mode :rate-limit
   :provider-id provider-id
   :workload workload
   :limit (long limit)
   :reason (str "Rate limit exceeded for provider " (name provider-id)
                " (max " (long limit) " requests/minute)")})
