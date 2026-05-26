(ns xia.limits
  "First-class LLM usage limits, budget state, and accounting helpers."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [xia.config :as cfg]
            [xia.db :as db]))

(def ^:private default-max-turn-llm-calls 600)
(def ^:private default-max-turn-total-tokens 2000000)
(def ^:private default-max-turn-wall-clock-ms 21600000)
(def ^:private default-max-task-llm-calls 6000)
(def ^:private default-max-task-total-tokens 20000000)
(def ^:private default-max-task-llm-duration-ms 216000000)
(def ^:private default-max-schedule-run-llm-calls 600)
(def ^:private default-max-schedule-run-total-tokens 2000000)
(def ^:private default-max-schedule-run-wall-clock-ms 21600000)

(defn- current-time-ms
  []
  (long (System/currentTimeMillis)))

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

(defn max-task-llm-calls
  []
  (cfg/positive-long :agent/max-task-llm-calls
                     default-max-task-llm-calls))

(defn max-task-total-tokens
  []
  (cfg/positive-long :agent/max-task-total-tokens
                     default-max-task-total-tokens))

(defn max-task-llm-duration-ms
  []
  (cfg/positive-long :agent/max-task-llm-duration-ms
                     default-max-task-llm-duration-ms))

(defn max-schedule-run-llm-calls
  []
  (cfg/positive-long :schedule/max-run-llm-calls
                     default-max-schedule-run-llm-calls))

(defn max-schedule-run-total-tokens
  []
  (cfg/positive-long :schedule/max-run-total-tokens
                     default-max-schedule-run-total-tokens))

(defn max-schedule-run-wall-clock-ms
  []
  (cfg/positive-long :schedule/max-run-wall-clock-ms
                     default-max-schedule-run-wall-clock-ms))

(defn- parse-positive-long
  [value]
  (let [parsed (cond
                 (integer? value) (long value)
                 (number? value) (long value)
                 (string? value) (try
                                   (Long/parseLong (str/trim value))
                                   (catch Exception _ nil))
                 :else nil)]
    (when (and parsed (pos? parsed))
      parsed)))

(defn- optional-positive-long
  [config-key]
  (cfg/custom-option config-key nil parse-positive-long))

(defn- parse-ratio
  [value]
  (try
    (let [parsed (cond
                   (number? value) (double value)
                   (string? value) (Double/parseDouble (str/trim value))
                   :else nil)]
      (when (and parsed (<= 0.0 parsed 1.0))
        parsed))
    (catch Exception _
      nil)))

(defn- map-value
  [m k]
  (when (map? m)
    (let [n (name k)
          underscored (str/replace n "-" "_")]
      (or (get m k)
          (get m n)
          (get m underscored)
          (get m (keyword underscored))))))

(defn- optional-ratio
  [config-key]
  (cfg/custom-option config-key nil parse-ratio))

(def ^:private policy-actions
  #{:deny :warn :require-approval :pause-schedule :prefer-local :downgrade-model})

(def ^:private routing-policy-actions
  #{:prefer-local :downgrade-model})

(defn- config-keyword
  [value]
  (cond
    (keyword? value) value
    (string? value) (let [text (str/trim value)
                          text* (if (str/starts-with? text ":")
                                  (subs text 1)
                                  text)]
                      (when (seq text*)
                        (keyword text*)))
    :else nil))

(defn- parse-policy-action
  [value]
  (let [action (config-keyword value)]
    (when (contains? policy-actions action)
      action)))

(defn- policy-action-config
  [config-key default-action]
  (or (cfg/custom-option config-key default-action parse-policy-action)
      default-action))

(defn- parse-keyword-value
  [value]
  (config-keyword value))

(defn- optional-keyword
  [config-key]
  (cfg/custom-option config-key nil parse-keyword-value))

(defn- parse-edn-map
  [value]
  (cond
    (nil? value)
    nil

    (map? value)
    value

    (string? value)
    (try
      (let [parsed (edn/read-string value)]
        (when (map? parsed)
          parsed))
      (catch Exception _
        nil))

    :else
    nil))

(defn model-price-catalog
  "Return the optional model price catalog from config.

   Expected shape:
   {[:provider-id \"model\"] {:input-usd-per-1m 0.15
                              :output-usd-per-1m 0.60}}"
  []
  (or (cfg/custom-option :limits/model-prices {} parse-edn-map)
      {}))

(defn routing-target-provider-id
  [action]
  (case action
    :prefer-local (optional-keyword :limits/prefer-local-provider-id)
    :downgrade-model (optional-keyword :limits/downgrade-provider-id)
    nil))

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

(defn- response-meta
  [request key-name]
  (or (some-> request :response meta key-name)
      (get-in request [:opts key-name])))

(defn- price-entry
  [provider-id model]
  (let [catalog (model-price-catalog)
        provider-key (some-> provider-id name)
        provider-kw (some-> provider-key keyword)
        model-key (some-> model str)]
    (or (get catalog [provider-id model])
        (get catalog [provider-kw model-key])
        (get catalog [provider-key model-key])
        (get-in catalog [provider-id model])
        (get-in catalog [provider-kw model-key])
        (get-in catalog [provider-key model-key])
        (get catalog (str provider-key "/" model-key)))))

(defn- usd-per-1m->micros
  [value tokens]
  (when (and value tokens)
    (long (Math/ceil (/ (* (double value) 1000000.0 (double tokens))
                        1000000.0)))))

(defn estimate-cost-micros
  [{:keys [provider-id model prompt-tokens completion-tokens]}]
  (when-let [price (price-entry provider-id model)]
    (let [input-usd (or (:input-usd-per-1m price)
                        (:prompt-usd-per-1m price))
          output-usd (or (:output-usd-per-1m price)
                         (:completion-usd-per-1m price))
          input-cost (usd-per-1m->micros input-usd prompt-tokens)
          output-cost (usd-per-1m->micros output-usd completion-tokens)]
      (when (or input-cost output-cost)
        (+ (long (or input-cost 0))
           (long (or output-cost 0)))))))

(defn usage-totals
  [usage]
  (let [prompt-tokens (or (usage-value usage "prompt_tokens") 0)
        completion-tokens (or (usage-value usage "completion_tokens")
                              (usage-value usage "output_tokens")
                              0)
        total-tokens (or (usage-value usage "total_tokens")
                         (+ prompt-tokens completion-tokens))]
    {:prompt-tokens (long prompt-tokens)
     :completion-tokens (long completion-tokens)
     :total-tokens (long total-tokens)}))

(defn request-usage-entry
  ([request]
   (request-usage-entry nil request))
  ([context {:keys [kind usage duration-ms error] :as request}]
   (let [{:keys [prompt-tokens completion-tokens total-tokens]} (usage-totals usage)
         provider-id (response-meta request :provider-id)
         model (response-meta request :model)
         entry (cond-> {:scope :llm-call
                        :kind kind
                        :session-id (:session-id context)
                        :task-id (:task-id context)
                        :schedule-id (:schedule-id context)
                        :goal-id (:persistent-goal-id context)
                        :provider-id provider-id
                        :model model
                        :workload (response-meta request :workload)
                        :llm-call-id (response-meta request :llm-call-id)
                        :prompt-tokens prompt-tokens
                        :completion-tokens completion-tokens
                        :total-tokens total-tokens
                        :duration-ms (long (or duration-ms 0))
                        :status (if error :error :success)
                        :observed-at-ms (current-time-ms)}
                 error
                 (assoc :error (truncate-text (.getMessage ^Throwable error) 240)))
         cost-micros (estimate-cost-micros entry)]
     (cond-> entry
       cost-micros
       (assoc :cost-micros cost-micros
              :cost-estimated? true)))))

(defn log-usage!
  [context request]
  (when (db/connected?)
    (db/log-limit-usage! (request-usage-entry context request))))

(defn usage-totals-for
  [scope selector]
  (if (db/connected?)
    (db/limit-usage-totals scope selector)
    {:scope scope
     :llm-call-count 0
     :prompt-tokens 0
     :completion-tokens 0
     :total-tokens 0
     :llm-total-duration-ms 0
     :cost-micros 0}))

(defn new-turn-budget
  [session-id channel]
  {:scope :turn
   :session-id session-id
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

(defn new-task-budget
  [task-id channel started-at]
  {:scope :task
   :task-id task-id
   :channel channel
   :started-at-ms (cond
                    (instance? java.util.Date started-at)
                    (.getTime ^java.util.Date started-at)

                    (integer? started-at)
                    (long started-at)

                    :else
                    (current-time-ms))
   :llm-call-count 0
   :llm-total-duration-ms 0
   :prompt-tokens 0
   :completion-tokens 0
   :total-tokens 0
   :max-llm-calls (long (max-task-llm-calls))
   :max-total-tokens (long (max-task-total-tokens))
   :max-llm-duration-ms (long (max-task-llm-duration-ms))})

(defn restore-task-budget
  [task-id channel started-at persisted]
  (merge (new-task-budget task-id channel started-at)
         (select-keys (or persisted {})
                      [:started-at-ms
                       :llm-call-count
                       :llm-total-duration-ms
                       :prompt-tokens
                       :completion-tokens
                       :total-tokens
                       :last-llm-duration-ms
                       :last-llm-error
                       :last-llm-at-ms
                       :llm-error-count
                       :last-provider-id
                       :last-model
                       :last-workload
                       :last-llm-call-id])))

(defn new-schedule-run-budget
  [schedule-id]
  {:scope :schedule-run
   :schedule-id schedule-id
   :started-at-ms (current-time-ms)
   :llm-call-count 0
   :llm-total-duration-ms 0
   :prompt-tokens 0
   :completion-tokens 0
   :total-tokens 0
   :max-llm-calls (long (max-schedule-run-llm-calls))
   :max-total-tokens (long (max-schedule-run-total-tokens))
   :max-wall-clock-ms (long (max-schedule-run-wall-clock-ms))})

(defn record-request!
  [budget-state request]
  (when budget-state
    (let [{:keys [prompt-tokens completion-tokens total-tokens duration-ms error
                  provider-id model workload llm-call-id]} (request-usage-entry request)]
      (swap! budget-state
             (fn [budget]
               (cond-> (-> budget
                           (update :llm-call-count (fnil inc 0))
                           (update :llm-total-duration-ms (fnil + 0) duration-ms)
                           (update :prompt-tokens (fnil + 0) prompt-tokens)
                           (update :completion-tokens (fnil + 0) completion-tokens)
                           (update :total-tokens (fnil + 0) total-tokens)
                           (assoc :last-llm-duration-ms duration-ms
                                  :last-llm-error error
                                  :last-llm-at-ms (current-time-ms)
                                  :last-provider-id provider-id
                                  :last-model model
                                  :last-workload workload
                                  :last-llm-call-id llm-call-id))
                 error
                 (update :llm-error-count (fnil inc 0))))))))

(defn record-turn-request!
  [turn-budget-state request]
  (record-request! turn-budget-state request))

(defn record-task-request!
  [task-budget-state request]
  (record-request! task-budget-state request))

(defn record-schedule-run-request!
  [schedule-run-budget-state request]
  (record-request! schedule-run-budget-state request))

(defn- turn-budget-status
  [budget]
  (let [elapsed-ms (- (current-time-ms) (long (:started-at-ms budget 0)))
        status {:scope :turn
                :session-id (:session-id budget)
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

(defn- task-budget-status
  [budget]
  (let [status {:scope :task
                :task-id (:task-id budget)
                :channel (:channel budget)
                :llm-call-count (long (:llm-call-count budget 0))
                :total-tokens (long (:total-tokens budget 0))
                :prompt-tokens (long (:prompt-tokens budget 0))
                :completion-tokens (long (:completion-tokens budget 0))
                :llm-total-duration-ms (long (:llm-total-duration-ms budget 0))
                :max-llm-calls (long (:max-llm-calls budget 0))
                :max-total-tokens (long (:max-total-tokens budget 0))
                :max-llm-duration-ms (long (:max-llm-duration-ms budget 0))}]
    (cond
      (>= (:llm-call-count status) (:max-llm-calls status))
      (assoc status :kind :llm-calls)

      (>= (:total-tokens status) (:max-total-tokens status))
      (assoc status :kind :tokens)

      (>= (:llm-total-duration-ms status) (:max-llm-duration-ms status))
      (assoc status :kind :llm-duration)

      :else
      nil)))

(defn- schedule-run-budget-status
  [budget]
  (let [elapsed-ms (- (current-time-ms) (long (:started-at-ms budget 0)))
        status {:scope :schedule-run
                :schedule-id (:schedule-id budget)
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

(defn budget-status
  [budget-state]
  (when budget-state
    (let [budget @budget-state]
      (case (:scope budget)
        :turn (turn-budget-status budget)
        :task (task-budget-status budget)
        :schedule-run (schedule-run-budget-status budget)
        nil))))

(def ^:private policy-scope-order
  [:org :goal :session :schedule])

(defn- policy-scope-selector
  [context scope]
  (case scope
    :org {}
    :session (when-let [session-id (:session-id context)]
               {:session-id session-id})
    :goal (when-let [goal-id (:persistent-goal-id context)]
            {:goal-id goal-id})
    :schedule (when-let [schedule-id (:schedule-id context)]
                {:schedule-id schedule-id})
    nil))

(defn- configured-policy-ceilings
  [scope]
  (let [prefix (name scope)
        max-llm-calls (optional-positive-long
                       (keyword "limits" (str prefix "-max-llm-calls")))
        max-total-tokens (optional-positive-long
                          (keyword "limits" (str prefix "-max-total-tokens")))
        max-cost-micros (optional-positive-long
                         (keyword "limits" (str prefix "-max-cost-micros")))
        warn-ratio (or (optional-ratio (keyword "limits" (str prefix "-warn-ratio")))
                       0.9)
        near-action (policy-action-config
                     (keyword "limits" (str prefix "-near-action"))
                     :warn)
        action (policy-action-config
                (keyword "limits" (str prefix "-action"))
                :deny)]
    (cond-> {}
      max-llm-calls
      (assoc :max-llm-calls max-llm-calls)

      max-total-tokens
      (assoc :max-total-tokens max-total-tokens)

      max-cost-micros
      (assoc :max-cost-micros max-cost-micros)

      (or max-llm-calls max-total-tokens max-cost-micros)
      (assoc :warn-ratio warn-ratio
             :near-action near-action
             :action action))))

(defn- budget-policy-ceilings
  [budget]
  (let [max-llm-calls (parse-positive-long (map-value budget :max-llm-calls))
        max-total-tokens (parse-positive-long (map-value budget :max-total-tokens))
        max-cost-micros (parse-positive-long (map-value budget :max-cost-micros))
        warn-ratio (or (parse-ratio (map-value budget :warn-ratio)) 0.9)
        near-action (or (parse-policy-action (map-value budget :near-action)) :warn)
        action (or (parse-policy-action (map-value budget :action)) :deny)]
    (not-empty
     (cond-> {}
       max-llm-calls
       (assoc :max-llm-calls max-llm-calls)

       max-total-tokens
       (assoc :max-total-tokens max-total-tokens)

       max-cost-micros
       (assoc :max-cost-micros max-cost-micros)

       (or max-llm-calls max-total-tokens max-cost-micros)
       (assoc :warn-ratio warn-ratio
              :near-action near-action
              :action action)))))

(defn- goal-contract-policy-ceilings
  [context]
  (or (budget-policy-ceilings
       (get-in context [:operating-envelope :effective :goal :budget]))
      (budget-policy-ceilings
       (get-in context [:operating-envelope :effective :limits :goal]))))

(defn- policy-ceilings
  [context scope]
  (if (= scope :goal)
    (or (goal-contract-policy-ceilings context)
        (configured-policy-ceilings scope))
    (configured-policy-ceilings scope)))

(defn- threshold-reached?
  [used limit ratio]
  (and limit
       (pos? (long limit))
       (>= (double used) (* (double limit) (double ratio)))))

(defn- policy-status-with-state
  [scope selector ceilings state action]
  (let [usage (usage-totals-for scope selector)
        status (merge usage
                      ceilings
                      selector
                      {:scope scope
                       :policy? true
                       :state state
                       :action action})]
    (cond
      (and (:max-llm-calls ceilings)
           (case state
             :exhausted (>= (:llm-call-count status) (:max-llm-calls ceilings))
             :near (threshold-reached? (:llm-call-count status)
                                       (:max-llm-calls ceilings)
                                       (:warn-ratio ceilings))
             false))
      (assoc status :kind :llm-calls
             :used (:llm-call-count status)
             :limit (:max-llm-calls ceilings))

      (and (:max-total-tokens ceilings)
           (case state
             :exhausted (>= (:total-tokens status) (:max-total-tokens ceilings))
             :near (threshold-reached? (:total-tokens status)
                                       (:max-total-tokens ceilings)
                                       (:warn-ratio ceilings))
             false))
      (assoc status :kind :tokens
             :used (:total-tokens status)
             :limit (:max-total-tokens ceilings))

      (and (:max-cost-micros ceilings)
           (case state
             :exhausted (>= (:cost-micros status) (:max-cost-micros ceilings))
             :near (threshold-reached? (:cost-micros status)
                                       (:max-cost-micros ceilings)
                                       (:warn-ratio ceilings))
             false))
      (assoc status :kind :cost
             :used (:cost-micros status)
             :limit (:max-cost-micros ceilings))

      :else
      nil)))

(defn- exhausted-policy-status
  [scope selector ceilings]
  (policy-status-with-state scope selector ceilings :exhausted (:action ceilings)))

(defn- near-policy-status
  [scope selector ceilings]
  (when (pos? (double (:warn-ratio ceilings 0.0)))
    (policy-status-with-state scope selector ceilings :near (:near-action ceilings))))

(defn- policy-decision*
  [context states]
  (some (fn [scope]
          (when-let [selector (policy-scope-selector context scope)]
            (let [ceilings (policy-ceilings context scope)]
              (when (seq ceilings)
                (some (fn [state]
                        (let [status (case state
                                       :exhausted (exhausted-policy-status scope selector ceilings)
                                       :near (near-policy-status scope selector ceilings)
                                       nil)]
                          (when status
                            (cond-> status
                              (contains? routing-policy-actions (:action status))
                              (assoc :target-provider-id
                                     (routing-target-provider-id (:action status)))))))
                      states)))))
        policy-scope-order))

(defn policy-status
  [context]
  (policy-decision* context [:exhausted]))

(defn policy-decision
  [context]
  (policy-decision* context [:exhausted :near]))

(defn routing-decision
  [context]
  (let [decision (policy-decision* context [:near])]
    (when (and decision
               (contains? routing-policy-actions (:action decision))
               (:target-provider-id decision))
      decision)))

(defn apply-routing-decision
  [selection-opts decision]
  (if (and decision
           (:target-provider-id decision)
           (not (:provider-id selection-opts)))
    (assoc selection-opts :provider-id (:target-provider-id decision))
    selection-opts))

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

(defn- format-cost-micros
  [value]
  (format "$%.6f" (/ (double (or value 0)) 1000000.0)))

(defn- scope-label
  [scope]
  (case scope
    :org "organization"
    :session "session"
    :schedule "schedule"
    :goal "goal"
    :schedule-run "scheduled run"
    :task "task"
    :turn "turn"
    (name scope)))

(defn- policy-summary
  [{:keys [scope kind llm-call-count max-llm-calls total-tokens max-total-tokens
           cost-micros max-cost-micros]}]
  (let [label (scope-label scope)]
    (case kind
      :llm-calls
      (str label " LLM call ceiling (" llm-call-count "/" max-llm-calls ")")

      :tokens
      (str label " token ceiling (" total-tokens "/" max-total-tokens ")")

      :cost
      (str label " cost ceiling (" (format-cost-micros cost-micros)
           "/" (format-cost-micros max-cost-micros) ")")

      (str label " usage ceiling"))))

(defn budget-summary
  [{:keys [scope kind llm-call-count max-llm-calls total-tokens max-total-tokens
           elapsed-ms max-wall-clock-ms llm-total-duration-ms max-llm-duration-ms
           policy?] :as status}]
  (if policy?
    (policy-summary status)
    (case scope
      :task
      (case kind
        :llm-calls
        (str "cumulative task LLM call budget (" llm-call-count "/" max-llm-calls ")")

        :tokens
        (str "cumulative task token budget (" total-tokens "/" max-total-tokens ")")

        :llm-duration
        (str "cumulative task LLM runtime budget (" (format-duration-ms llm-total-duration-ms)
             "/" (format-duration-ms max-llm-duration-ms) ")")

        "cumulative task budget")

      :schedule-run
      (case kind
        :llm-calls
        (str "scheduled run LLM call budget (" llm-call-count "/" max-llm-calls ")")

        :tokens
        (str "scheduled run token budget (" total-tokens "/" max-total-tokens ")")

        :wall-clock
        (str "scheduled run wall-clock budget (" (format-duration-ms elapsed-ms)
             "/" (format-duration-ms max-wall-clock-ms) ")")

        "scheduled run budget")

      (case kind
        :llm-calls
        (str "cumulative LLM call budget (" llm-call-count "/" max-llm-calls ")")

        :tokens
        (str "cumulative token budget (" total-tokens "/" max-total-tokens ")")

        :wall-clock
        (str "wall-clock budget (" (format-duration-ms elapsed-ms)
             "/" (format-duration-ms max-wall-clock-ms) ")")

        "cumulative turn budget"))))

(defn exhausted-ex
  [budget-state]
  (when-let [status (budget-status budget-state)]
    (ex-info (str "Reached the " (budget-summary status))
             (merge {:type :limit-exhausted}
                    status))))

(defn policy-exhausted-ex
  [context]
  (when-let [status (policy-status context)]
    (ex-info (str "Reached the " (budget-summary status))
             (merge {:type :limit-exhausted}
                    status))))

(defn policy-decision-ex
  [decision]
  (ex-info (str "Reached the " (budget-summary decision))
           (merge {:type :limit-exhausted}
                  decision)))

(defn policy-decision-event
  [decision]
  (merge {:decision-type :limit-policy
          :allowed? (not (contains? #{:deny :pause-schedule} (:action decision)))
          :mode (:action decision)
          :reason (budget-summary decision)}
         (select-keys decision
                      [:scope :state :kind :action :used :limit :llm-call-count
                       :total-tokens :cost-micros :max-llm-calls
                       :max-total-tokens :max-cost-micros :target-provider-id
                       :goal-id])))

(defn exhausted-exception?
  [e]
  (= :limit-exhausted (:type (ex-data e))))

(defn throw-if-exhausted!
  [budget-state]
  (when-let [budget-ex (exhausted-ex budget-state)]
    (throw budget-ex)))

(defn throw-if-policy-exhausted!
  [context]
  (when-let [budget-ex (policy-exhausted-ex context)]
    (throw budget-ex)))
