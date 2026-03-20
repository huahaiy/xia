(ns xia.llm
  "Multi-provider LLM client. All providers follow the OpenAI-compatible
   chat completions API shape, so one client handles OpenAI, Anthropic
   (via proxy), Qwen, local Ollama, etc."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.http-client :as http])
  (:import [java.time ZonedDateTime]
           [java.time.format DateTimeFormatter]
           [java.util.concurrent TimeoutException]))

(def ^:private request-timeout-ms 120000)
(def ^:private provider-health-base-cooldown-ms 30000)
(def ^:private provider-health-max-cooldown-ms 300000)
(def ^:private llm-retry-statuses #{408 409 425 429 500 502 503 504})
(def ^:private default-max-provider-retry-rounds 4)
(def ^:private default-max-provider-retry-wait-ms 300000)
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

(defn- build-request
  "Build the HTTP request map for a chat completion call."
  [{:keys [base-url api-key model]} messages {:keys [tools temperature max-tokens]}]
  (let [body (cond-> {:model    model
                      :messages messages}
               tools       (assoc :tools tools)
               temperature (assoc :temperature temperature)
               max-tokens  (assoc :max_tokens max-tokens))]
    {:url           (str base-url "/chat/completions")
     :method        :post
     :headers       {"Authorization" (str "Bearer " api-key)
                     "Content-Type"  "application/json"}
     :body          (json/write-json-str body)
     :timeout       request-timeout-ms
     :retry-enabled? true
     :request-label "LLM request"}))

(defn- provider-error-message
  [provider-id ^Throwable e]
  (or (some-> e ex-data :body)
      (.getMessage e)
      (str "provider " provider-id " request failed")))

(defn- attempt-chat
  [messages opts {:keys [provider provider-id workload]}]
  (let [base-url  (or (:llm.provider/base-url provider) (:base-url provider))
        api-key   (or (:llm.provider/api-key provider) (:api-key provider))
        model     (or (:llm.provider/model provider) (:model provider))
        req       (build-request {:base-url base-url :api-key api-key :model model}
                                 messages opts)
        _         (log/debug "LLM request via provider" provider-id
                             "to" base-url
                             "model" model
                             (when workload (str "workload " workload)))
        resp      (http/request req)
        status    (:status resp)]
    (when (not= 200 status)
      (throw (ex-info (str "LLM request failed with status " status)
                      {:status      status
                       :headers     (:headers resp)
                       :body        (:body resp)
                       :provider-id provider-id
                        :workload    workload})))
    (try
      (json/read-json (:body resp))
      (catch Exception e
        (throw (ex-info "Failed to parse LLM response body"
                        {:provider-id provider-id
                         :workload    workload}
                        e))))))

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
          (let [response (:response result)]
            (record-provider-success! (:provider-id attempt))
            {:status :ok
             :response response})
          (let [e           (:error result)
                attempt-id  (:provider-id attempt)
                error-text  (provider-error-message attempt-id e)
                cooldown-ms (retry-after-ms (some-> e ex-data :headers))
                health      (record-provider-failure! attempt-id
                                                     error-text
                                                     :cooldown-ms cooldown-ms)
                failure     {:error      e
                             :retryable? (retryable-llm-error? e)
                             :delay-ms   (:cooldown-remaining-ms health)
                             :provider-id attempt-id
                             :workload   (:workload attempt)}]
            (if (seq remaining)
              (do
                (log/info "LLM provider failed; trying next routed provider"
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
  (let [resp (apply chat messages opts)]
    (get-in resp ["choices" 0 "message" "content"])))

(defn chat-with-tools
  "Send messages with tools. Returns the full message (may contain tool_calls)."
  [messages tools & opts]
  (let [resp (apply chat messages (concat [:tools tools] opts))]
    (get-in resp ["choices" 0 "message"])))
