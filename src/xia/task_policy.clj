(ns xia.task-policy
  "Dedicated limits and budget policy helpers shared across task runtimes."
  (:require [clojure.string :as str]
            [xia.config :as cfg]))

(def ^:private default-supervisor-max-identical-iterations 3)
(def ^:private default-supervisor-semantic-loop-threshold 0.88)
(def ^:private default-supervisor-max-restarts 1)
(def ^:private default-supervisor-restart-backoff-ms 100)
(def ^:private default-supervisor-restart-grace-ms 1000)
(def ^:private default-max-tool-rounds 100)
(def ^:private default-max-tool-calls-per-round 12)
(def ^:private default-http-max-attempts 3)
(def ^:private default-http-initial-backoff-ms 1000)
(def ^:private default-http-max-backoff-ms 8000)
(def ^:private default-http-retry-statuses #{408 409 425 429 500 502 503 504})
(def ^:private default-http-retry-methods #{:delete :get :head :options :put :trace})
(def ^:private default-llm-retry-statuses #{408 409 425 429 500 502 503 504})
(def ^:private default-max-provider-retry-rounds 4)
(def ^:private default-max-provider-retry-wait-ms 300000)
(def ^:private default-schedule-failure-backoff-minutes 15)
(def ^:private default-schedule-max-failure-backoff-minutes (* 12 60))
(def ^:private default-schedule-pause-after-repeated-failures 3)
(def ^:private default-max-turn-llm-calls 600)
(def ^:private default-max-turn-total-tokens 2000000)
(def ^:private default-max-turn-wall-clock-ms 21600000)

(defn supervisor-max-identical-iterations
  []
  (cfg/positive-long :agent/supervisor-max-identical-iterations
                     default-supervisor-max-identical-iterations))

(defn supervisor-semantic-loop-threshold
  []
  (cfg/positive-double :agent/supervisor-semantic-loop-threshold
                       default-supervisor-semantic-loop-threshold))

(defn supervisor-max-restarts
  []
  (cfg/positive-long :agent/supervisor-max-restarts
                     default-supervisor-max-restarts))

(defn supervisor-restart-backoff-ms
  []
  (cfg/positive-long :agent/supervisor-restart-backoff-ms
                     default-supervisor-restart-backoff-ms))

(defn supervisor-restart-grace-ms
  []
  (cfg/positive-long :agent/supervisor-restart-grace-ms
                     default-supervisor-restart-grace-ms))

(defn max-tool-rounds
  []
  (cfg/positive-long :agent/max-tool-rounds
                     default-max-tool-rounds))

(defn max-tool-calls-per-round
  []
  (cfg/positive-long :agent/max-tool-calls-per-round
                     default-max-tool-calls-per-round))

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

(defn llm-max-provider-retry-rounds
  []
  (cfg/positive-long :llm/max-provider-retry-rounds
                     default-max-provider-retry-rounds))

(defn llm-max-provider-retry-wait-ms
  []
  (cfg/positive-long :llm/max-provider-retry-wait-ms
                     default-max-provider-retry-wait-ms))

(defn max-turn-llm-calls
  []
  (cfg/positive-long :agent/max-turn-llm-calls
                     default-max-turn-llm-calls))

(defn max-turn-total-tokens
  []
  (cfg/positive-long :agent/max-turn-total-tokens
                     default-max-turn-total-tokens))

(defn max-turn-wall-clock-ms
  []
  (cfg/positive-long :agent/max-turn-wall-clock-ms
                     default-max-turn-wall-clock-ms))

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

(defn- current-time-ms
  []
  (long (System/currentTimeMillis)))

(defn- parse-long-value
  [value]
  (cond
    (integer? value)
    (long value)

    (number? value)
    (long value)

    (string? value)
    (try
      (Long/parseLong (str/trim value))
      (catch Exception _
        nil))

    :else
    nil))

(defn- usage-value
  [usage key-name]
  (when (map? usage)
    (some-> (or (get usage key-name)
                (get usage (name key-name))
                (get usage (keyword (name key-name))))
            parse-long-value)))

(defn- truncate-text
  [value max-chars]
  (let [text (some-> value str str/trim)]
    (when (seq text)
      (if (<= (count text) max-chars)
        text
        (str (subs text 0 (max 0 (- max-chars 3))) "...")))))

(defn new-turn-llm-budget
  [session-id channel]
  {:session-id session-id
   :channel channel
   :started-at-ms (current-time-ms)
   :llm-call-count 0
   :llm-total-duration-ms 0
   :prompt-tokens 0
   :completion-tokens 0
   :total-tokens 0
   :max-llm-calls (long (max-turn-llm-calls))
   :max-total-tokens (long (max-turn-total-tokens))
   :max-wall-clock-ms (long (max-turn-wall-clock-ms))})

(defn record-turn-llm-request!
  [turn-budget-state {:keys [usage duration-ms error]}]
  (when turn-budget-state
    (swap! turn-budget-state
           (fn [budget]
             (let [prompt-tokens (or (usage-value usage "prompt_tokens") 0)
                   completion-tokens (or (usage-value usage "completion_tokens")
                                         (usage-value usage "output_tokens")
                                         0)
                   total-tokens (or (usage-value usage "total_tokens")
                                    (+ prompt-tokens completion-tokens))]
               (cond-> (-> budget
                           (update :llm-call-count (fnil inc 0))
                           (update :llm-total-duration-ms (fnil + 0) (long (or duration-ms 0)))
                           (update :prompt-tokens (fnil + 0) (long prompt-tokens))
                           (update :completion-tokens (fnil + 0) (long completion-tokens))
                           (update :total-tokens (fnil + 0) (long total-tokens))
                           (assoc :last-llm-duration-ms (long (or duration-ms 0))
                                  :last-llm-error (when error
                                                    (truncate-text (.getMessage ^Throwable error)
                                                                   240))
                                  :last-llm-at-ms (current-time-ms)))
                 error
                 (update :llm-error-count (fnil inc 0))))))))

(defn- turn-llm-budget-status*
  [budget]
  (let [elapsed-ms (- (current-time-ms) (long (:started-at-ms budget 0)))
        status {:session-id (:session-id budget)
                :channel (:channel budget)
                :llm-call-count (long (:llm-call-count budget 0))
                :total-tokens (long (:total-tokens budget 0))
                :prompt-tokens (long (:prompt-tokens budget 0))
                :completion-tokens (long (:completion-tokens budget 0))
                :elapsed-ms elapsed-ms
                :max-llm-calls (long (:max-llm-calls budget 0))
                :max-total-tokens (long (:max-total-tokens budget 0))
                :max-wall-clock-ms (long (:max-wall-clock-ms budget 0))}]
    (cond
      (>= (:llm-call-count status) (:max-llm-calls status))
      (assoc status :kind :llm-calls)

      (>= (:total-tokens status) (:max-total-tokens status))
      (assoc status :kind :tokens)

      (>= (:elapsed-ms status) (:max-wall-clock-ms status))
      (assoc status :kind :wall-clock)

      :else
      nil)))

(defn turn-llm-budget-status
  [turn-budget-state]
  (when turn-budget-state
    (turn-llm-budget-status* @turn-budget-state)))

(defn format-duration-ms
  [value]
  (let [ms (long (or value 0))]
    (cond
      (>= ms 60000)
      (format "%.1fm" (/ ms 60000.0))

      (>= ms 1000)
      (format "%.1fs" (/ ms 1000.0))

      :else
      (str ms "ms"))))

(defn turn-llm-budget-summary
  [{:keys [kind llm-call-count max-llm-calls total-tokens max-total-tokens elapsed-ms
           max-wall-clock-ms]}]
  (case kind
    :llm-calls
    (str "cumulative LLM call budget (" llm-call-count "/" max-llm-calls ")")

    :tokens
    (str "cumulative token budget (" total-tokens "/" max-total-tokens ")")

    :wall-clock
    (str "wall-clock budget (" (format-duration-ms elapsed-ms)
         "/" (format-duration-ms max-wall-clock-ms) ")")

    "cumulative turn budget"))

(defn turn-llm-budget-ex
  [turn-budget-state]
  (when-let [status (turn-llm-budget-status turn-budget-state)]
    (ex-info (str "Reached the " (turn-llm-budget-summary status))
             (merge {:type :turn-budget-exhausted}
                    status))))

(defn throw-if-turn-llm-budget-exhausted!
  [turn-budget-state]
  (when-let [budget-ex (turn-llm-budget-ex turn-budget-state)]
    (throw budget-ex)))

(defn normalize-approval-policy
  [approval]
  (case (cond
          (keyword? approval) approval
          (string? approval) (keyword approval)
          :else :auto)
    :session :session
    :always :always
    :auto :auto
    :auto))

(defn tool-approval-policy
  [tool inferred-decision]
  (let [approval (or (:tool/approval tool) (:approval tool))
        explicit-decision (when approval
                            {:policy (normalize-approval-policy approval)})]
    (assoc (merge inferred-decision explicit-decision)
           :policy (normalize-approval-policy
                    (or (:policy explicit-decision)
                        (:policy inferred-decision))))))

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

(defn tool-call-limit-decision
  [tool-count]
  (let [tool-count (long tool-count)
        max-tool-calls-per-round (long (max-tool-calls-per-round))
        allowed? (<= tool-count max-tool-calls-per-round)]
    {:allowed? allowed?
     :mode (if allowed? :within-limit :round-call-limit)
     :tool-count tool-count
     :max-tool-calls-per-round max-tool-calls-per-round
     :reason (when-not allowed?
               (str "Too many tool calls in one round: "
                    tool-count
                    " (max "
                    max-tool-calls-per-round
                    ")"))}))

(defn tool-round-limit-decision
  [round max-tool-rounds]
  (let [rounds (long round)
        max-tool-rounds (long max-tool-rounds)
        allowed? (< rounds max-tool-rounds)]
    {:allowed? allowed?
     :mode (if allowed? :within-limit :round-limit)
     :rounds rounds
     :max-tool-rounds max-tool-rounds
     :reason (when-not allowed?
               "Too many tool-calling rounds")}))

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

(def ^:private non-restartable-worker-error-types
  #{:request-cancelled
    :autonomous-loop-stalled
    :autonomous-protocol-invalid
    :turn-budget-exhausted
    :agent-stop-timeout
    :tool-round-limit-exceeded
    :tool-call-limit-exceeded
    :user-message-too-large
    :session-busy})

(defn restart-policy-decision
  [t worker-state attempt & {:keys [session-cancelled?]}]
  (let [failure-type (some-> t ex-data :type)
        next-attempt (inc (long (or attempt 0)))
        max-restarts (long (supervisor-max-restarts))
        allowed? (cond
                   session-cancelled? false
                   (:tool-risk? worker-state) false
                   (instance? InterruptedException t) false
                   (contains? non-restartable-worker-error-types failure-type) false
                   (> next-attempt max-restarts) false
                   :else true)
        mode (cond
               session-cancelled? :cancelled
               (:tool-risk? worker-state) :tool-risk
               (instance? InterruptedException t) :interrupted
               (contains? non-restartable-worker-error-types failure-type) :non-restartable
               (> next-attempt max-restarts) :restart-limit
               :else :restarting)]
    {:allowed? allowed?
     :mode mode
     :attempt next-attempt
     :max-restarts max-restarts
     :backoff-ms (when allowed?
                   (long (supervisor-restart-backoff-ms)))
     :grace-ms (long (supervisor-restart-grace-ms))
     :failure-type failure-type
     :failure-phase (some-> t ex-data :phase)
     :worker-phase (:phase worker-state)
     :tool-risk? (boolean (:tool-risk? worker-state))
     :tool-id (:tool-id worker-state)
     :tool-name (:tool-name worker-state)
     :round (:round worker-state)}))
