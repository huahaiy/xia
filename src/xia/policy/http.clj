(ns xia.policy.http
  "HTTP retry policy.")

(def ^:private default-http-max-attempts 3)
(def ^:private default-http-initial-backoff-ms 1000)
(def ^:private default-http-max-backoff-ms 8000)
(def ^:private default-http-retry-statuses #{408 409 425 429 500 502 503 504})
(def ^:private default-http-retry-methods #{:delete :get :head :options :put :trace})

(defn http-request-retry-config
  [req]
  {:max-attempts (long (or (:max-attempts req)
                           default-http-max-attempts))
   :initial-backoff-ms (long (or (:initial-backoff-ms req)
                                 default-http-initial-backoff-ms))
   :max-backoff-ms (long (or (:max-backoff-ms req)
                             default-http-max-backoff-ms))
   :retry-statuses (or (:retry-statuses req)
                       default-http-retry-statuses)
   :retry-methods (or (:retry-methods req)
                      default-http-retry-methods)})

(defn http-request-retry-enabled?
  [{:keys [method retry-enabled? retry-methods]}]
  (if (some? retry-enabled?)
    retry-enabled?
    (contains? (or retry-methods default-http-retry-methods)
               (or method :get))))

(defn http-request-backoff-ms
  [attempt initial-backoff-ms max-backoff-ms]
  (min (long max-backoff-ms)
       (* (long initial-backoff-ms)
          (bit-shift-left 1 (dec (long attempt))))))

(defn http-request-retry-decision
  [req attempt {:keys [status transient-exception? reason]}]
  (let [{:keys [max-attempts initial-backoff-ms max-backoff-ms retry-statuses retry-methods]}
        (http-request-retry-config req)
        retry-enabled? (http-request-retry-enabled? {:method (:method req)
                                                     :retry-enabled? (:retry-enabled? req)
                                                     :retry-methods retry-methods})
        allowed? (and retry-enabled?
                      (< (long attempt) max-attempts)
                      (or transient-exception?
                          (contains? retry-statuses status)))
        mode (cond
               allowed? (if transient-exception?
                          :transient-exception
                          :transient-status)
               (not retry-enabled?) :retry-disabled
               (>= (long attempt) max-attempts) :attempt-limit
               status :permanent-status
               :else :not-retryable)]
    {:allowed? allowed?
     :mode mode
     :attempt (long attempt)
     :max-attempts max-attempts
     :status status
     :reason reason
     :delay-ms (when allowed?
                 (long (http-request-backoff-ms attempt
                                                initial-backoff-ms
                                                max-backoff-ms)))}))
