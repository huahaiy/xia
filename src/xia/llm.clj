(ns xia.llm
  "Multi-provider LLM client. All providers follow the OpenAI-compatible
   chat completions API shape, so one client handles OpenAI, Anthropic
   (via proxy), Qwen, local Ollama, etc."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.http-client :as http]
            [xia.rate-limit :as rate-limit])
  (:import [java.net URI]
           [java.time ZonedDateTime]
           [java.time.format DateTimeFormatter]
           [java.util.concurrent ConcurrentHashMap TimeoutException]
           [java.util.concurrent.atomic AtomicLong]))

(def ^:private request-timeout-ms 120000)
(def ^:private provider-health-base-cooldown-ms 30000)
(def ^:private provider-health-max-cooldown-ms 300000)
(def ^:private llm-retry-statuses #{408 409 425 429 500 502 503 504})
(def ^:private default-max-provider-retry-rounds 4)
(def ^:private default-max-provider-retry-wait-ms 300000)
(def default-rate-limit-per-minute 60)
(def ^:private workload-options
  [{:id :assistant
    :label "Assistant"
    :description "Main user-facing replies and tool calling."}
   {:id :history-compaction
    :label "History Compaction"
    :description "Summarizing older conversation history to fit the prompt budget."}
   {:id :topic-summary
    :label "Topic Summary"
    :description "Working-memory topic summarization."}
   {:id :memory-summary
    :label "Memory Summary"
    :description "Conversation summarization for episodic memory."}
   {:id :memory-importance
    :label "Memory Importance"
    :description "Batch rating episodic-memory importance for retention."}
   {:id :memory-extraction
    :label "Memory Extraction"
    :description "Knowledge extraction during hippocampus consolidation."}
   {:id :fact-utility
    :label "Fact Utility"
    :description "Post-response rating of which retrieved facts were useful."}])
(defonce ^:private workload-counters (atom {}))
(defonce ^:private provider-health (atom {}))
(defonce ^ConcurrentHashMap ^:private provider-rate-limits (ConcurrentHashMap.))
(defonce ^AtomicLong ^:private provider-rate-limit-cleanup (AtomicLong. 0))
(def ^:private loopback-hosts #{"localhost" "127.0.0.1" "::1" "[::1]"})
(def ^:private rate-limit-window-ms 60000)

(defn- long-max
  ^long [^long a ^long b]
  (if (> a b) a b))

(defn- long-min
  ^long [^long a ^long b]
  (if (< a b) a b))

;; ---------------------------------------------------------------------------
;; Provider resolution
;; ---------------------------------------------------------------------------

(defn workload-routes
  "Return the known LLM workload route definitions."
  []
  workload-options)

(defn- current-workload-id-set
  []
  (set (map :id (workload-routes))))

(defn known-workload?
  [workload]
  (contains? (current-workload-id-set) workload))

(defn- normalize-workload
  [workload]
  (let [normalized (cond
                     (nil? workload) nil
                     (keyword? workload) workload
                     (string? workload) (keyword workload)
                     :else (throw (ex-info "LLM workload must be a keyword or string"
                                           {:workload workload})))]
    (when (and normalized
               (not (known-workload? normalized)))
                      (throw (ex-info (str "Unknown LLM workload: " normalized)
                                      {:workload normalized
                       :known-workloads (mapv :id (workload-routes))})))
    normalized))

(defn- provider-key
  [provider]
  (or (:llm.provider/id provider) (:id provider)))

(defn- provider-workloads
  [provider]
  (set (or (:llm.provider/workloads provider)
           (:workloads provider))))

(defn effective-rate-limit-per-minute
  "Return the effective per-provider request cap for a minute window."
  [provider]
  (long (or (:llm.provider/rate-limit-per-minute provider)
            (:rate-limit-per-minute provider)
            default-rate-limit-per-minute)))

(defn- loopback-base-url?
  [base-url]
  (try
    (contains? loopback-hosts
               (some-> base-url URI. .getHost str/lower-case))
    (catch Exception _
      false)))

(defn- provider-allows-private-network?
  [provider]
  (or (:llm.provider/allow-private-network? provider)
      (:allow-private-network? provider)
      (loopback-base-url? (or (:llm.provider/base-url provider)
                              (:base-url provider)))))

(defn vision-capable?
  "True when a provider is configured to accept image input."
  [provider-or-id]
  (let [provider (cond
                   (nil? provider-or-id) nil
                   (map? provider-or-id) provider-or-id
                   :else (db/get-provider provider-or-id))]
    (boolean (:llm.provider/vision? provider))))

(defn- now-ms []
  (System/currentTimeMillis))

(defn- sleep-ms!
  [delay-ms]
  (Thread/sleep (long delay-ms)))

(defn- max-provider-retry-rounds
  []
  (cfg/positive-long :llm/max-provider-retry-rounds
                     default-max-provider-retry-rounds))

(defn- max-provider-retry-wait-ms
  []
  (cfg/positive-long :llm/max-provider-retry-wait-ms
                     default-max-provider-retry-wait-ms))

(defn- provider-cooldown-ms
  ^long
  [consecutive-failures]
  (loop [cooldown (long provider-health-base-cooldown-ms)
         remaining (long-max 0 (dec (long consecutive-failures)))]
    (if (zero? remaining)
      cooldown
      (recur (long-min (long provider-health-max-cooldown-ms)
                       (* 2 cooldown))
             (dec remaining)))))

(defn- provider-health-entry
  [provider-id]
  (get @provider-health provider-id))

(defn- default-provider-id
  []
  (some-> (db/get-default-provider) provider-key))

(defn provider-health-summary
  "Return the current in-memory health status for a provider."
  [provider-id]
  (let [provider-id          (or provider-id (default-provider-id))
        {:keys [consecutive-failures cooldown-until-ms last-success-ms last-failure-ms last-error]}
        (provider-health-entry provider-id)
        current-ms           (long (now-ms))
        cooldown-remaining-ms (when cooldown-until-ms
                                (long-max 0 (- (long cooldown-until-ms) current-ms)))
        status               (cond
                               (pos? (long (or cooldown-remaining-ms 0))) :cooling-down
                               (pos? (long (or consecutive-failures 0)))   :degraded
                               :else                                       :healthy)]
    {:provider-id           provider-id
     :status                status
     :healthy?              (= :healthy status)
     :available?            (not= :cooling-down status)
     :consecutive-failures  (long (or consecutive-failures 0))
     :cooldown-until-ms     cooldown-until-ms
     :cooldown-remaining-ms cooldown-remaining-ms
     :last-success-ms       last-success-ms
     :last-failure-ms       last-failure-ms
     :last-error            last-error}))

(defn- record-provider-success!
  [provider-id]
  (let [timestamp (long (now-ms))]
    (swap! provider-health assoc provider-id
           {:consecutive-failures 0
            :cooldown-until-ms   nil
            :last-success-ms     timestamp
            :last-failure-ms     nil
            :last-error          nil})))

(defn- record-provider-failure!
  [provider-id error-message & {:keys [cooldown-ms]}]
  (let [timestamp (long (now-ms))]
    (swap! provider-health
           (fn [state]
             (let [previous             (get state provider-id)
                   consecutive-failures (inc (long (or (:consecutive-failures previous) 0)))
                   cooldown-ms          (long-max (long (or cooldown-ms 0))
                                                  (provider-cooldown-ms consecutive-failures))]
               (assoc state provider-id
                      {:consecutive-failures consecutive-failures
                       :cooldown-until-ms   (+ timestamp cooldown-ms)
                       :last-success-ms     (:last-success-ms previous)
                       :last-failure-ms     timestamp
                       :last-error          error-message}))))
    (provider-health-summary provider-id)))

(defn- header-value
  [headers header-name]
  (let [normalized-name (str/lower-case (name header-name))]
    (or (get headers header-name)
        (get headers normalized-name)
        (some (fn [[k v]]
                (when (= normalized-name
                         (str/lower-case (name k)))
                  v))
              headers))))

(defn- retry-after-ms
  [headers]
  (when-let [raw (some-> (header-value headers "retry-after") str)]
    (let [value (str/trim raw)]
      (or (try
            (* 1000 (long-max 0 (Long/parseLong value)))
            (catch Exception _
              nil))
          (try
            (long-max 0
                      (- (.toEpochMilli (.toInstant (ZonedDateTime/parse value DateTimeFormatter/RFC_1123_DATE_TIME)))
                         (long (now-ms))))
            (catch Exception _
              nil))))))

(defn- retryable-llm-error?
  [^Throwable e]
  (let [status (some-> e ex-data :status)]
    (or (contains? llm-retry-statuses status)
        (boolean
          (some #(instance? Throwable %)
                (filter (fn [cause]
                          (or (instance? TimeoutException cause)
                              (instance? java.net.http.HttpTimeoutException cause)
                              (instance? java.net.http.HttpConnectTimeoutException cause)
                              (instance? java.io.IOException cause)))
                        (take-while some? (iterate ex-cause e))))))))

(defn- attempts-cooldown-delay-ms
  [attempts]
  (let [health (mapv #(provider-health-summary (:provider-id %)) attempts)]
    (when (and (seq health)
               (every? (comp not :available?) health))
      (some->> health
               (keep :cooldown-remaining-ms)
               (filter pos?)
               seq
               (apply min)))))

(defn- remaining-retry-wait-ms
  ^long
  [started-at max-retry-wait-ms]
  (- (long max-retry-wait-ms) (- (long (now-ms)) (long started-at))))

(defn- retry-sleep-ms
  [started-at round max-retry-rounds max-retry-wait-ms requested-delay-ms]
  (let [remaining-ms (remaining-retry-wait-ms started-at max-retry-wait-ms)]
    (when (and (< (long round) (long max-retry-rounds))
               (pos? remaining-ms)
               (pos? (long (or requested-delay-ms 0))))
      (long-min remaining-ms (long requested-delay-ms)))))

(defn- provider-group-key
  [{:keys [available? consecutive-failures cooldown-until-ms]}]
  (if available?
    [0 consecutive-failures]
    [1 (or cooldown-until-ms Long/MAX_VALUE) consecutive-failures]))

(defn- provider-sort-key
  [{:keys [available? consecutive-failures cooldown-until-ms provider-id]}]
  (conj (provider-group-key {:available? available?
                             :consecutive-failures consecutive-failures
                             :cooldown-until-ms cooldown-until-ms})
        (name provider-id)))

(defn- rotate-vector
  [items offset]
  (let [items  (vec items)
        length (count items)]
    (cond
      (<= length 1) items
      :else
      (let [offset (mod offset length)]
        (vec (concat (subvec items offset)
                     (subvec items 0 offset)))))))

(defn- default-provider-selection []
  (let [provider (or (db/get-default-provider)
                     (throw (ex-info "No default LLM provider configured. Run first-time setup." {})))]
    {:provider    provider
     :provider-id (provider-key provider)}))

(defn- workload-providers
  [workload]
  (->> (db/list-providers)
       (filter #(contains? (provider-workloads %) workload))
       (sort-by #(some-> % provider-key name))
       vec))

(defn- round-robin-provider
  [workload providers]
  (let [active-workloads (current-workload-id-set)
        state (swap! workload-counters
                     (fn [counters]
                       (let [counters* (select-keys counters active-workloads)]
                         (update counters* workload (fnil inc -1)))))
        index (mod (get state workload 0) (count providers))]
    (nth providers index)))

(defn- ordered-workload-providers
  [workload providers]
  (let [annotated    (->> providers
                          (mapv (fn [provider]
                                  (let [provider-id (provider-key provider)
                                        health      (provider-health-summary provider-id)]
                                    (assoc health
                                           :provider provider
                                           :provider-id provider-id
                                           :group-key (provider-group-key health)
                                           :sort-key (provider-sort-key
                                                       (assoc health :provider-id provider-id))))))
                          (sort-by :sort-key))
        grouped      (mapv vec (partition-by :group-key annotated))
        first-group  (first grouped)
        rotated-head (if (seq first-group)
                       (let [seed-provider (round-robin-provider workload (mapv :provider first-group))
                             seed-id       (provider-key seed-provider)
                             start-index   (or (first (keep-indexed (fn [idx entry]
                                                                      (when (= seed-id (:provider-id entry))
                                                                        idx))
                                                                    first-group))
                                               0)]
                         (rotate-vector first-group start-index))
                       [])
        rest-groups  (mapcat identity (rest grouped))]
    (mapv :provider (concat rotated-head rest-groups))))

(defn- provider-attempts
  [{:keys [provider-id workload]}]
  (let [workload (normalize-workload workload)]
    (cond
      provider-id
      [(let [provider (or (db/get-provider provider-id)
                          (throw (ex-info (str "Unknown provider: " provider-id)
                                          {:provider provider-id})))]
         {:provider    provider
          :provider-id (provider-key provider)})]

      workload
      (if-let [providers (seq (workload-providers workload))]
        (mapv (fn [provider]
                {:provider    provider
                 :provider-id (provider-key provider)
                 :workload    workload})
              (ordered-workload-providers workload (vec providers)))
        [(assoc (default-provider-selection) :workload workload)])

      :else
      [(default-provider-selection)])))

(defn resolve-provider-selection
  "Resolve the provider for an LLM request.

   Selection order:
   1. Explicit `:provider-id`
   2. Providers assigned to `:workload` (round-robin if multiple)
   3. Default provider"
  ([] (resolve-provider-selection nil))
  ([{:keys [provider-id workload]}]
   (first (provider-attempts {:provider-id provider-id
                              :workload workload}))))

;; ---------------------------------------------------------------------------
;; Chat completions
;; ---------------------------------------------------------------------------

(defn- append-text
  [existing delta]
  (if (string? delta)
    (str (or existing "") delta)
    existing))

(defn- ensure-tool-call-index
  [tool-calls index]
  (let [index         (long index)
        current-count (long (count tool-calls))]
    (if (< index current-count)
      tool-calls
      (into tool-calls (repeat (inc (- index current-count)) nil)))))

(defn- merge-tool-call-function-delta
  [existing delta]
  (let [existing (or existing {})]
    (cond-> existing
      (contains? delta "name")
      (assoc "name" (append-text (get existing "name")
                                 (get delta "name")))

      (contains? delta "arguments")
      (assoc "arguments" (append-text (get existing "arguments")
                                      (get delta "arguments"))))))

(defn- merge-tool-call-delta
  [tool-calls tool-call-delta]
  (let [index      (long (or (get tool-call-delta "index") 0))
        tool-calls (ensure-tool-call-index tool-calls index)
        existing   (or (nth tool-calls index)
                       {"type" "function"
                        "function" {"name" ""
                                    "arguments" ""}})
        merged     (cond-> existing
                     (contains? tool-call-delta "id")
                     (assoc "id" (get tool-call-delta "id"))

                     (contains? tool-call-delta "type")
                     (assoc "type" (get tool-call-delta "type"))

                     (map? (get tool-call-delta "function"))
                     (update "function"
                             merge-tool-call-function-delta
                             (get tool-call-delta "function")))]
    (assoc tool-calls index merged)))

(defn- merge-choice-delta
  [state choice]
  (let [delta (get choice "delta")]
    (cond-> state
      (contains? delta "role")
      (assoc :role (get delta "role"))

      (contains? delta "content")
      (update :content append-text (get delta "content"))

      (seq (get delta "tool_calls"))
      (update :tool-calls
              (fn [tool-calls]
                (reduce merge-tool-call-delta
                        (or tool-calls [])
                        (get delta "tool_calls"))))

      (contains? choice "finish_reason")
      (assoc :finish-reason (get choice "finish_reason")))))

(defn- finalized-stream-message
  [{:keys [role content tool-calls finish-reason]}]
  {"choices"
   [{"finish_reason" finish-reason
     "message"
     (cond-> {"role" (or role "assistant")
              "content" (or content "")}
       (seq tool-calls)
       (assoc "tool_calls" (vec (remove nil? tool-calls))))}]})

(defn- parse-response-body
  [body provider-id workload]
  (try
    (json/read-json body)
    (catch Exception e
      (throw (ex-info "Failed to parse LLM response body"
                      {:provider-id provider-id
                       :workload    workload}
                      e)))))

(defn- response-preview
  [response]
  (let [rendered (pr-str response)]
    (if (> (count rendered) 300)
      (str (subs rendered 0 297) "...")
      rendered)))

(defn- malformed-response-ex
  [message {:keys [provider-id workload response]}]
  (ex-info message
           {:type :llm/malformed-response
            :provider-id provider-id
            :workload workload
            :response-preview (response-preview response)}))

(defn- response-message!
  [response request-info]
  (cond
    (not (map? response))
    (throw (malformed-response-ex
             "LLM response must be a map"
             (assoc request-info :response response)))

    (not (sequential? (get response "choices")))
    (throw (malformed-response-ex
             "LLM response missing choices"
             (assoc request-info :response response)))

    (not (map? (first (get response "choices"))))
    (throw (malformed-response-ex
             "LLM response missing choices[0]"
             (assoc request-info :response response)))

    (not (map? (get-in response ["choices" 0 "message"])))
    (throw (malformed-response-ex
             "LLM response missing choices[0].message"
             (assoc request-info :response response)))

    :else
    (get-in response ["choices" 0 "message"])))

(defn- simple-message-content!
  [response request-info]
  (let [message (response-message! response request-info)
        content (get message "content")]
    (if (string? content)
      content
      (throw (malformed-response-ex
               "LLM response missing string choices[0].message.content"
               (assoc request-info :response response))))))

(defn- tool-message!
  [response request-info]
  (let [message    (response-message! response request-info)
        content    (get message "content")
        tool-calls (get message "tool_calls")]
    (when-not (or (string? content) (nil? content))
      (throw (malformed-response-ex
               "LLM response has non-string choices[0].message.content"
               (assoc request-info :response response))))
    (when (and (contains? message "tool_calls")
               (not (sequential? tool-calls)))
      (throw (malformed-response-ex
               "LLM response has invalid choices[0].message.tool_calls"
               (assoc request-info :response response))))
    (when (and (nil? content) (not (seq tool-calls)))
      (throw (malformed-response-ex
               "LLM response message has neither content nor tool_calls"
               (assoc request-info :response response))))
    (cond-> message
      (nil? content) (assoc "content" ""))))

(defn- streaming-request?
  [opts]
  (fn? (:on-delta opts)))

(defn- build-request
  "Build the HTTP request map for a chat completion call."
  [{:keys [base-url api-key model allow-private-network?]} messages {:keys [tools temperature max-tokens] :as opts}]
  (let [body (cond-> {:model    model
                      :messages messages}
               tools       (assoc :tools tools)
               temperature (assoc :temperature temperature)
               max-tokens  (assoc :max_tokens max-tokens)
               (streaming-request? opts) (assoc :stream true))]
    {:url           (str base-url "/chat/completions")
     :method        :post
     :headers       {"Authorization" (str "Bearer " api-key)
                     "Content-Type"  "application/json"}
     :body          (json/write-json-str body)
     :allow-private-network? (boolean allow-private-network?)
     :timeout       request-timeout-ms
     :retry-enabled? true
     :request-label "LLM request"}))

(defn- provider-error-message
  [provider-id ^Throwable e]
  (or (some-> e ex-data :body)
      (.getMessage e)
      (str "provider " provider-id " request failed")))

(defn- local-rate-limit-error?
  [^Throwable e]
  (= :llm/rate-limit (some-> e ex-data :type)))

(defn- check-rate-limit!
  [provider-id provider workload]
  (let [limit (effective-rate-limit-per-minute provider)
        now   (long (now-ms))
        _     (rate-limit/maybe-prune-states! provider-rate-limits
                                              provider-rate-limit-cleanup
                                              now
                                              rate-limit-window-ms)
        state (.computeIfAbsent provider-rate-limits provider-id
                (reify java.util.function.Function
                  (apply [_ _] (atom {:timestamps [] :cleaned now}))))]
    (rate-limit/consume-slot!
      state
      now
      rate-limit-window-ms
      limit
      (fn []
        (ex-info (str "Rate limit exceeded for provider " (name provider-id)
                      " (max " limit " requests/minute)")
                 {:type        :llm/rate-limit
                  :provider-id provider-id
                  :workload    workload
                  :limit       limit})))))

(defn- attempt-chat
  [messages opts {:keys [provider provider-id workload]}]
  (let [base-url  (or (:llm.provider/base-url provider) (:base-url provider))
        api-key   (or (:llm.provider/api-key provider) (:api-key provider))
        model     (or (:llm.provider/model provider) (:model provider))
        _         (check-rate-limit! provider-id provider workload)
        req       (build-request {:base-url base-url
                                  :api-key api-key
                                  :model model
                                  :allow-private-network? (provider-allows-private-network? provider)}
                                 messages opts)
        _         (log/debug "LLM request via provider" provider-id
                             "to" base-url
                             "model" model
                             (when workload (str "workload " workload)))
        resp      (if (streaming-request? opts)
                    (let [stream-state (volatile! {:role "assistant"
                                                  :content ""
                                                  :tool-calls []})
                          stream-resp  (http/request-events
                                         (assoc req
                                                :on-event
                                                (fn [{:keys [data]}]
                                                  (when-not (or (str/blank? data)
                                                                (= "[DONE]" data))
                                                    (let [chunk  (parse-response-body data provider-id workload)
                                                          choice (get-in chunk ["choices" 0])]
                                                      (vswap! stream-state merge-choice-delta choice)
                                                      (when-let [content (get-in choice ["delta" "content"])]
                                                        (when-let [on-delta (:on-delta opts)]
                                                          (on-delta {:provider-id provider-id
                                                                     :workload workload
                                                                     :delta content
                                                                     :content (:content @stream-state)}))))))))]
                      (if (:streamed? stream-resp)
                        (assoc stream-resp
                               :response (finalized-stream-message @stream-state))
                        stream-resp))
                    (http/request req))
        status    (:status resp)]
    (when (not= 200 status)
      (throw (ex-info (str "LLM request failed with status " status)
                      {:status      status
                       :headers     (:headers resp)
                       :body        (:body resp)
                       :provider-id provider-id
                        :workload    workload})))
    (if (:streamed? resp)
      (or (:response resp)
          (parse-response-body (:body resp) provider-id workload))
      (parse-response-body (:body resp) provider-id workload))))

(defn- attempt-provider-round
  [messages opts attempts]
  (loop [[attempt & remaining] attempts
         last-failure nil]
    (if-not attempt
      (if last-failure
        (if (:retryable? last-failure)
          {:status :retry
           :delay-ms (:delay-ms last-failure)
           :error (:error last-failure)
           :provider-id (:provider-id last-failure)
           :workload (:workload last-failure)}
          {:status :error
           :error (:error last-failure)
           :provider-id (:provider-id last-failure)
           :workload (:workload last-failure)})
        {:status :error
         :error (ex-info "No LLM provider available for request" {})})
      (let [result (try
                     {:ok? true
                      :response (attempt-chat messages opts attempt)}
                     (catch Exception e
                       {:ok? false
                        :error e}))]
        (if (:ok? result)
          (let [response  (:response result)
                response* (if (instance? clojure.lang.IObj response)
                            (with-meta response
                              (merge (meta response)
                                     {:provider-id (:provider-id attempt)
                                      :workload (:workload attempt)}))
                            response)]
            (record-provider-success! (:provider-id attempt))
            {:status :ok
             :response response*})
          (let [e           (:error result)
                attempt-id  (:provider-id attempt)
                error-text  (provider-error-message attempt-id e)
                failure     (if (local-rate-limit-error? e)
                              {:error       e
                               :retryable?  false
                               :delay-ms    nil
                               :provider-id attempt-id
                               :workload    (:workload attempt)}
                              (let [cooldown-ms (retry-after-ms (some-> e ex-data :headers))
                                    health      (record-provider-failure! attempt-id
                                                                         error-text
                                                                         :cooldown-ms cooldown-ms)]
                                {:error       e
                                 :retryable?  (retryable-llm-error? e)
                                 :delay-ms    (:cooldown-remaining-ms health)
                                 :provider-id attempt-id
                                 :workload    (:workload attempt)}))]
            (if (seq remaining)
              (do
                (log/info (if (local-rate-limit-error? e)
                            "LLM provider hit configured local rate limit; trying next routed provider"
                            "LLM provider failed; trying next routed provider")
                          {:provider-id attempt-id
                           :workload    (:workload attempt)
                           :remaining   (mapv :provider-id remaining)
                           :error       error-text})
                (recur remaining failure))
              (if (:retryable? failure)
                {:status :retry
                 :delay-ms (:delay-ms failure)
                 :error e
                 :provider-id attempt-id
                 :workload (:workload attempt)}
                {:status :error
                 :error e
                 :provider-id attempt-id
                 :workload (:workload attempt)}))))))))

(defn chat
  "Send a chat completion request.

   Options:
     :provider-id  — keyword id of provider (default: DB default)
     :workload     — keyword workload route (round-robin if multiple providers match)
     :tools        — vector of tool definitions (OpenAI function-calling format)
     :temperature  — float
     :max-tokens   — int

   Returns the parsed response body as a Clojure map."
  [messages & {:keys [provider-id workload tools temperature max-tokens] :as opts}]
  (let [started-at          (now-ms)
        max-retry-rounds*   (max-provider-retry-rounds)
        max-retry-wait-ms*  (max-provider-retry-wait-ms)]
    (loop [round 1]
      (let [attempts (provider-attempts {:provider-id provider-id
                                         :workload workload})]
        (when-not (seq attempts)
          (throw (ex-info "No LLM provider available for request"
                          {:provider-id provider-id
                           :workload workload})))
        (if-let [preflight-delay-ms (attempts-cooldown-delay-ms attempts)]
          (if-let [delay-ms (retry-sleep-ms started-at
                                            round
                                            max-retry-rounds*
                                            max-retry-wait-ms*
                                            preflight-delay-ms)]
            (do
              (log/warn "All routed LLM providers are cooling down; waiting before retry"
                        {:provider-id provider-id
                         :workload workload
                         :round round
                         :delay-ms delay-ms
                         :providers (mapv :provider-id attempts)})
              (sleep-ms! delay-ms)
              (recur (inc round)))
            (throw (ex-info "LLM providers are still cooling down"
                            {:provider-id provider-id
                             :workload workload
                             :round round
                             :cooldown-ms preflight-delay-ms
                             :max-retry-rounds max-retry-rounds*
                             :max-retry-wait-ms max-retry-wait-ms*})))
          (let [round-result (attempt-provider-round messages opts attempts)]
            (case (:status round-result)
              :ok
              (:response round-result)

              :retry
              (if-let [delay-ms (retry-sleep-ms started-at
                                                round
                                                max-retry-rounds*
                                                max-retry-wait-ms*
                                                (:delay-ms round-result))]
                (do
                  (log/warn "LLM request exhausted routed providers; retrying after backoff"
                            {:provider-id (:provider-id round-result)
                             :workload (:workload round-result)
                             :round round
                             :delay-ms delay-ms
                             :error (provider-error-message (:provider-id round-result)
                                                            (:error round-result))})
                  (sleep-ms! delay-ms)
                  (recur (inc round)))
                (do
                  (log/error (:error round-result) "LLM request failed after retries"
                             {:provider-id (:provider-id round-result)
                              :workload (:workload round-result)
                              :round round})
                  (throw (:error round-result))))

              :error
              (do
                (log/error (:error round-result) "LLM request failed"
                           {:provider-id (:provider-id round-result)
                            :workload (:workload round-result)
                            :round round})
                (throw (:error round-result))))))))))

(defn chat-simple
  "Convenience: send messages, return the assistant's text content."
  [messages & opts]
  (let [resp         (apply chat messages opts)
        request-info (merge (apply hash-map opts)
                            (select-keys (meta resp) [:provider-id :workload]))]
    (simple-message-content! resp request-info)))

(defn chat-with-tools
  "Send messages with tools. Returns the full message (may contain tool_calls)."
  [messages tools & opts]
  (let [resp         (apply chat messages (concat [:tools tools] opts))
        request-info (merge (apply hash-map opts)
                            (select-keys (meta resp) [:provider-id :workload]))]
    (tool-message! resp request-info)))
