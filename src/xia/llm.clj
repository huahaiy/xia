(ns xia.llm
  "Multi-provider LLM client. All providers follow the OpenAI-compatible
   chat completions API shape, so one client handles OpenAI, Anthropic
   (via proxy), Qwen, local Ollama, etc."
  (:require [charred.api :as json]
            [taoensso.timbre :as log]
            [xia.db :as db]
            [xia.http-client :as http]))

(def ^:private request-timeout-ms 120000)
(def ^:private provider-health-base-cooldown-ms 30000)
(def ^:private provider-health-max-cooldown-ms 300000)
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
(def ^:private workload-id-set (set (map :id workload-options)))
(defonce ^:private workload-counters (atom {}))
(defonce ^:private provider-health (atom {}))

;; ---------------------------------------------------------------------------
;; Provider resolution
;; ---------------------------------------------------------------------------

(defn workload-routes
  "Return the known LLM workload route definitions."
  []
  workload-options)

(defn known-workload?
  [workload]
  (contains? workload-id-set workload))

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
                       :known-workloads (mapv :id workload-options)})))
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

(defn- provider-cooldown-ms
  [consecutive-failures]
  (loop [cooldown provider-health-base-cooldown-ms
         remaining (max 0 (dec (long consecutive-failures)))]
    (if (zero? remaining)
      cooldown
      (recur (min provider-health-max-cooldown-ms
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
        current-ms           (now-ms)
        cooldown-remaining-ms (when cooldown-until-ms
                                (max 0 (- cooldown-until-ms current-ms)))
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
  (let [timestamp (now-ms)]
    (swap! provider-health assoc provider-id
           {:consecutive-failures 0
            :cooldown-until-ms   nil
            :last-success-ms     timestamp
            :last-failure-ms     nil
            :last-error          nil})))

(defn- record-provider-failure!
  [provider-id error-message]
  (let [timestamp (now-ms)]
    (swap! provider-health
           (fn [state]
             (let [previous             (get state provider-id)
                   consecutive-failures (inc (long (or (:consecutive-failures previous) 0)))
                   cooldown-ms          (provider-cooldown-ms consecutive-failures)]
               (assoc state provider-id
                      {:consecutive-failures consecutive-failures
                       :cooldown-until-ms   (+ timestamp cooldown-ms)
                       :last-success-ms     (:last-success-ms previous)
                       :last-failure-ms     timestamp
                       :last-error          error-message}))))))

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
  (let [state (swap! workload-counters update workload (fnil inc -1))
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
  (let [attempts (provider-attempts {:provider-id provider-id
                                     :workload workload})]
    (loop [[attempt & remaining] attempts]
      (when-not attempt
        (throw (ex-info "No LLM provider available for request"
                        {:provider-id provider-id
                         :workload workload})))
      (let [result (try
                     {:ok? true
                      :response (attempt-chat messages opts attempt)}
                     (catch Exception e
                       {:ok? false
                        :error e}))]
        (if (:ok? result)
          (let [response (:response result)]
            (record-provider-success! (:provider-id attempt))
            response)
          (let [e          (:error result)
                provider-id (:provider-id attempt)
                error-text  (provider-error-message provider-id e)]
            (record-provider-failure! provider-id error-text)
            (if (seq remaining)
              (do
                (log/info "LLM provider failed; trying next routed provider"
                          {:provider-id provider-id
                           :workload    (:workload attempt)
                           :remaining   (mapv :provider-id remaining)
                           :error       error-text})
                (recur remaining))
              (do
                (log/error e "LLM request failed"
                           {:provider-id provider-id
                            :workload    (:workload attempt)})
                (throw e)))))))))

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
