(ns xia.llm
  "Multi-provider LLM client. All providers follow the OpenAI-compatible
   chat completions API shape, except Anthropic-native providers which
   are translated at the boundary. One client handles OpenAI, Anthropic,
   Qwen, local Ollama, etc."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.async :as async]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.http-client :as http]
            [xia.llm.routing :as llm-routing]
            [xia.oauth :as oauth]
            [xia.prompt :as prompt]
            [xia.rate-limit :as rate-limit]
            [xia.task-policy :as task-policy])
  (:import [java.net URI URLEncoder]
           [java.nio.charset StandardCharsets]
           [java.time ZonedDateTime]
           [java.time.format DateTimeFormatter]
           [java.util.concurrent ConcurrentHashMap TimeoutException]
           [java.util.concurrent.atomic AtomicLong]))

(def ^:private request-timeout-ms 120000)
(def ^:private provider-health-base-cooldown-ms 30000)
(def ^:private provider-health-max-cooldown-ms 300000)
(def default-rate-limit-per-minute 60)
(def ^:private workload-options
  [{:id :assistant
    :label "Assistant"
    :description "Main user-facing replies and tool calling."
    :async? false}
   {:id :history-compaction
    :label "History Compaction"
    :description "Summarizing older conversation history to fit the prompt budget."
    :async? false}
   {:id :topic-summary
    :label "Topic Summary"
    :description "Working-memory topic summarization."
    :async? true}
   {:id :memory-summary
    :label "Memory Summary"
    :description "Conversation summarization for episodic memory."
    :async? true}
   {:id :memory-importance
    :label "Memory Importance"
    :description "Batch rating episodic-memory importance for retention."
    :async? true}
   {:id :memory-extraction
    :label "Memory Extraction"
    :description "Knowledge extraction during hippocampus consolidation."
    :async? true}
   {:id :fact-utility
    :label "Fact Utility"
    :description "Post-response rating of which retrieved facts were useful."
    :async? true}])
(defonce ^:private workload-counters (atom {}))
(defonce ^:private provider-health (atom {}))
(defonce ^ConcurrentHashMap ^:private provider-rate-limits (ConcurrentHashMap.))
(defonce ^AtomicLong ^:private provider-rate-limit-cleanup (AtomicLong. 0))
(defonce ^:private async-log-state
  (atom {:accepting? true
         :tasks #{}}))
(defonce ^:private async-log-lock
  (Object.))
(def ^:private loopback-hosts #{"localhost" "127.0.0.1" "::1" "[::1]"})
(def ^:private rate-limit-window-ms 60000)
(def ^:private anthropic-api-version "2023-06-01")
(def ^:private default-anthropic-max-tokens 4096)
(def ^:dynamic *request-budget-guard* nil)
(def ^:dynamic *request-observer* nil)

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
  (llm-routing/workload-routes workload-options))

(defn reset-runtime!
  "Re-enable async LLM log writes for a fresh runtime."
  []
  (llm-routing/reset-runtime! async-log-lock async-log-state))

(defn prepare-shutdown!
  "Stop accepting new async LLM log writes and return the pending count."
  []
  (llm-routing/prepare-shutdown! async-log-lock async-log-state))

(defn await-background-tasks!
  "Block until tracked async LLM log writes finish."
  []
  (llm-routing/await-background-tasks! async-log-lock async-log-state))

(defn runtime-activity
  "Return coarse LLM runtime activity for control-plane inspection."
  []
  (locking async-log-lock
    {:accepting? (boolean (:accepting? @async-log-state))
     :pending-log-write-count (count (:tasks @async-log-state))}))

(defn- submit-log-write!
  [log-entry]
  (llm-routing/submit-log-write! {:async-log-lock async-log-lock
                                  :async-log-state async-log-state
                                  :submit-background! async/submit-background!
                                  :log-llm-call! db/log-llm-call!}
                                 log-entry))

(defn known-workload?
  [workload]
  (llm-routing/known-workload? workload-options workload))

(defn- normalize-workload
  [workload]
  (llm-routing/normalize-workload workload-options workload))

(defn effective-rate-limit-per-minute
  "Return the effective per-provider request cap for a minute window."
  [provider]
  (llm-routing/effective-rate-limit-per-minute default-rate-limit-per-minute provider))

(defn- loopback-base-url?
  [base-url]
  (llm-routing/loopback-base-url? loopback-hosts base-url))

(defn- provider-allows-private-network?
  [provider]
  (llm-routing/provider-allows-private-network? loopback-hosts provider))

(defn- normalize-base-url
  [base-url]
  (llm-routing/normalize-base-url base-url))

(defn- base-url-host
  [base-url]
  (llm-routing/base-url-host base-url))

(defn- provider-family-from-base-url
  [base-url]
  (llm-routing/provider-family-from-base-url base-url))

(defn vision-capable?
  "True when a provider is configured to accept image input."
  [provider-or-id]
  (llm-routing/vision-capable? db/get-provider provider-or-id))

(defn- now-ms []
  (System/currentTimeMillis))

(defn- sleep-ms!
  [delay-ms]
  (Thread/sleep (long delay-ms)))

(defn- max-provider-retry-rounds
  []
  (task-policy/llm-max-provider-retry-rounds))

(defn- max-provider-retry-wait-ms
  []
  (task-policy/llm-max-provider-retry-wait-ms))

(defn provider-health-summary
  "Return the current in-memory health status for a provider."
  [provider-id]
  (llm-routing/provider-health-summary {:provider-health provider-health
                                        :get-default-provider db/get-default-provider
                                        :now-ms now-ms}
                                       provider-id))

(defn- record-provider-success!
  [provider-id]
  (llm-routing/record-provider-success! provider-health now-ms provider-id))

(defn- record-provider-failure!
  [provider-id error-message & {:keys [cooldown-ms]}]
  (llm-routing/record-provider-failure!
   {:provider-health provider-health
    :now-ms now-ms
    :base-cooldown-ms provider-health-base-cooldown-ms
    :max-cooldown-ms provider-health-max-cooldown-ms}
   provider-id
   error-message
   :cooldown-ms cooldown-ms))

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
  (task-policy/llm-retry-after-ms
   {"retry-after" (header-value headers "retry-after")}
   now-ms))

(defn- retryable-llm-error?
  [^Throwable e]
  (task-policy/llm-retryable-error? e))

(defn- attempts-cooldown-delay-ms
  [attempts]
  (llm-routing/attempts-cooldown-delay-ms provider-health-summary attempts))

(defn- retry-sleep-ms
  [started-at round max-retry-rounds max-retry-wait-ms requested-delay-ms]
  (task-policy/llm-retry-sleep-ms started-at
                                  round
                                  max-retry-rounds
                                  max-retry-wait-ms
                                  requested-delay-ms
                                  now-ms))

(defn- provider-attempts
  [{:keys [provider-id workload]}]
  (llm-routing/provider-attempts {:workload-options workload-options
                                  :get-provider db/get-provider
                                  :get-default-provider db/get-default-provider
                                  :list-providers db/list-providers
                                  :workload-counters workload-counters
                                  :provider-health-summary-fn provider-health-summary}
                                 {:provider-id provider-id
                                  :workload workload}))

(defn resolve-provider-selection
  "Resolve the provider for an LLM request.

  Selection order:
  1. Explicit `:provider-id`
  2. Providers assigned to `:workload` (round-robin if multiple)
  3. Default provider"
  ([] (resolve-provider-selection nil))
  ([{:keys [provider-id workload]}]
   (llm-routing/resolve-provider-selection
    {:workload-options workload-options
     :get-provider db/get-provider
     :get-default-provider db/get-default-provider
     :list-providers db/list-providers
     :workload-counters workload-counters
     :provider-health-summary-fn provider-health-summary}
    {:provider-id provider-id
     :workload workload})))

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
  [{:keys [role content tool-calls finish-reason usage]}]
  (cond-> {"choices"
           [{"finish_reason" finish-reason
             "message"
             (cond-> {"role" (or role "assistant")
                      "content" (or content "")}
               (seq tool-calls)
               (assoc "tool_calls" (vec (remove nil? tool-calls))))}]}
    (map? usage)
    (assoc "usage" usage)))

(defn- parse-json-body
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

(defn- maybe-report-recovered-stream-content!
  [opts provider-id workload partial-content response]
  (when-let [on-delta (:on-delta opts)]
    (when-let [full-content (get-in response ["choices" 0 "message" "content"])]
      (when (string? full-content)
        (let [partial (or partial-content "")
              delta   (cond
                        (= full-content partial)
                        nil

                        (and (seq partial)
                             (str/starts-with? full-content partial))
                        (subs full-content (count partial))

                        :else
                        full-content)]
          (when (seq delta)
            (on-delta {:provider-id provider-id
                       :workload    workload
                       :delta       delta
                       :content     full-content})))))))

(defn- malformed-response-ex
  [message {:keys [provider-id workload response]}]
  (ex-info message
           {:type :llm/malformed-response
            :provider-id provider-id
            :workload workload
            :response-preview (response-preview response)}))

(defn- content-part-text
  [part]
  (cond
    (string? part)
    part

    (map? part)
    (let [text (get part "text")]
      (cond
        (string? text)
        text

        (map? text)
        (or (some-> (get text "value") str)
            (some-> (get text "text") str))

        :else
        nil))

    :else
    nil))

(defn- normalize-message-content
  [content]
  (cond
    (string? content)
    content

    (sequential? content)
    (let [text (->> content
                    (keep content-part-text)
                    (apply str))]
      (when (seq text)
        text))

    :else
    nil))

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
        content (normalize-message-content (get message "content"))]
    (if (string? content)
      content
      (throw (malformed-response-ex
               "LLM response missing text choices[0].message.content"
               (assoc request-info :response response))))))

(defn- tool-message!
  [response request-info]
  (let [message    (response-message! response request-info)
        content    (normalize-message-content (get message "content"))
        tool-calls (get message "tool_calls")]
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
      (contains? message "content") (assoc "content" (or content ""))
      (nil? content) (assoc "content" ""))))

(declare chat)

(defn- response-metadata
  [response request-info]
  (merge request-info
         (select-keys (meta response) [:provider-id :model :workload :llm-call-id])))

(defn- maybe-call-budget-guard!
  [request]
  (when *request-budget-guard*
    (*request-budget-guard* request)))

(defn- maybe-observe-request!
  [request]
  (when *request-observer*
    (*request-observer* request)))

(defn- budgeted-chat-request
  [kind messages opts chat-fn]
  (maybe-call-budget-guard! {:kind kind
                             :messages messages
                             :opts opts})
  (let [started-at (now-ms)]
    (try
      (let [response (chat-fn)
            duration-ms (- (now-ms) started-at)]
        (maybe-observe-request! {:kind kind
                                 :messages messages
                                 :opts opts
                                 :response response
                                 :duration-ms duration-ms
                                 :usage (when (map? response)
                                          (get response "usage"))})
        response)
      (catch Exception e
        (maybe-observe-request! {:kind kind
                                 :messages messages
                                 :opts opts
                                 :error e
                                 :duration-ms (- (now-ms) started-at)})
        (throw e)))))

(defn chat-message
  "Send messages and return the normalized assistant message map.
   Preserves provider/model/workload/call provenance in metadata."
  [messages & {:keys [tools] :as opts}]
  (let [resp         (budgeted-chat-request :chat-message
                                            messages
                                            opts
                                            #(apply chat messages
                                                    (mapcat identity opts)))
        request-info (response-metadata resp (dissoc opts :on-delta))
        message      (if (seq tools)
                       (tool-message! resp request-info)
                       (let [message (response-message! resp request-info)
                             content (normalize-message-content (get message "content"))]
                         (if (string? content)
                           (cond-> message
                             (contains? message "content")
                             (assoc "content" content))
                           (throw (malformed-response-ex
                                    "LLM response missing text choices[0].message.content"
                                    (assoc request-info :response resp))))))]
    (if (instance? clojure.lang.IObj message)
      (with-meta message (merge (meta message) request-info))
      message)))

(declare streaming-request?)

(defn- provider-api-key
  [provider]
  (some-> (or (:llm.provider/api-key provider)
              (:api-key provider))
          str/trim
          not-empty))

(defn- provider-api-headers
  [provider-family {:keys [api-key auth-header]}]
  (case provider-family
    :anthropic
    (cond-> {"anthropic-version" anthropic-api-version}
      api-key (assoc "x-api-key" api-key)
      (and (nil? api-key) (some? auth-header)) (assoc "Authorization" auth-header))

    :openai
    (cond-> {}
      (some? auth-header) (assoc "Authorization" auth-header)
      (and (nil? auth-header) api-key) (assoc "Authorization" (str "Bearer " api-key)))

    {}))

(defn- message-role
  [message]
  (some-> (or (:role message)
              (get message "role"))
          name))

(defn- message-content
  [message]
  (or (:content message)
      (get message "content")))

(defn- message-tool-calls
  [message]
  (or (:tool_calls message)
      (:tool-calls message)
      (get message "tool_calls")
      (get message "tool-calls")))

(defn- message-tool-call-id
  [message]
  (some-> (or (:tool_call_id message)
              (:tool-call-id message)
              (get message "tool_call_id")
              (get message "tool-call_id"))
          str
          str/trim
          not-empty))

(defn- parse-tool-call-arguments
  [arguments]
  (cond
    (map? arguments)
    arguments

    (nil? arguments)
    {}

    (string? arguments)
    (let [arguments (str/trim arguments)]
      (cond
        (str/blank? arguments)
        {}

        :else
        (try
          (let [parsed (json/read-json arguments)]
            (if (map? parsed)
              parsed
              {"value" parsed}))
          (catch Exception _
            {"raw_arguments" arguments}))))

    :else
    {"value" arguments}))

(defn- content-part-type
  [part]
  (some-> (or (:type part)
              (get part "type"))
          str
          str/lower-case))

(defn- image-url-value
  [part]
  (or (get-in part [:image_url :url])
      (get-in part [:image-url :url])
      (get-in part ["image_url" "url"])
      (get-in part ["image-url" "url"])))

(defn- data-url-image-source
  [url]
  (when-let [[_ media-type data] (and (string? url)
                                      (re-matches #"(?is)^data:([^;,]+);base64,(.+)$" url))]
    {"type" "base64"
     "media_type" (str/lower-case media-type)
     "data" data}))

(defn- anthropic-image-source
  [url]
  (or (data-url-image-source url)
      (when (seq (some-> url str str/trim))
        {"type" "url"
         "url" url})))

(defn- anthropic-content-block
  [part]
  (cond
    (string? part)
    (when (seq part)
      {"type" "text"
       "text" part})

    (map? part)
    (case (content-part-type part)
      "text"
      (when-let [text (content-part-text part)]
        {"type" "text"
         "text" text})

      "image_url"
      (when-let [source (some-> part image-url-value anthropic-image-source)]
        {"type" "image"
         "source" source})

      "image"
      (let [source (or (:source part)
                       (get part "source"))]
        (when (map? source)
          {"type" "image"
           "source" source}))

      nil)

    :else
    nil))

(defn- anthropic-content-blocks
  [content]
  (cond
    (nil? content)
    []

    (string? content)
    (if (seq content)
      [{"type" "text"
        "text" content}]
      [])

    (sequential? content)
    (into [] (keep anthropic-content-block) content)

    :else
    (if-let [text (normalize-message-content content)]
      [{"type" "text"
        "text" text}]
      [])))

(defn- tool-call-id
  [tool-call]
  (some-> (or (:id tool-call)
              (get tool-call "id"))
          str
          str/trim
          not-empty))

(defn- tool-call-name
  [tool-call]
  (some-> (or (:name tool-call)
              (get tool-call "name")
              (:name (:function tool-call))
              (get-in tool-call ["function" "name"]))
          str
          str/trim
          not-empty))

(defn- tool-call-arguments
  [tool-call]
  (or (:arguments tool-call)
      (get tool-call "arguments")
      (:arguments (:function tool-call))
      (get-in tool-call ["function" "arguments"])))

(defn- openai-tool-call->anthropic-block
  [tool-call]
  (when-let [name (tool-call-name tool-call)]
    (cond-> {"type" "tool_use"
             "name" name
             "input" (parse-tool-call-arguments (tool-call-arguments tool-call))}
      (tool-call-id tool-call)
      (assoc "id" (tool-call-id tool-call)))))

(defn- anthropic-tool-result-block
  [message]
  (when-let [tool-use-id (message-tool-call-id message)]
    (cond-> {"type" "tool_result"
             "tool_use_id" tool-use-id}
      (contains? message :content)
      (assoc "content" (or (normalize-message-content (message-content message)) ""))

      (contains? message "content")
      (assoc "content" (or (normalize-message-content (message-content message)) "")))))

(defn- flush-pending-tool-results
  [messages pending-tool-results]
  (if (seq pending-tool-results)
    (conj messages {"role" "user"
                    "content" (vec pending-tool-results)})
    messages))

(defn- build-anthropic-message-payload
  [messages]
  (loop [remaining messages
         system-lines []
         pending-tool-results []
         acc []]
    (if-let [message (first remaining)]
      (let [role       (message-role message)
            content    (message-content message)
            tool-calls (message-tool-calls message)]
        (case role
          "system"
          (recur (next remaining)
                 (cond-> system-lines
                   (seq (normalize-message-content content))
                   (conj (normalize-message-content content)))
                 pending-tool-results
                 acc)

          "tool"
          (if-let [tool-result (anthropic-tool-result-block message)]
            (recur (next remaining)
                   system-lines
                   (conj pending-tool-results tool-result)
                   acc)
            (recur (next remaining)
                   system-lines
                   []
                   (conj (flush-pending-tool-results acc pending-tool-results)
                         {"role" "user"
                          "content" (anthropic-content-blocks content)})))

          "assistant"
          (recur (next remaining)
                 system-lines
                 []
                 (conj (flush-pending-tool-results acc pending-tool-results)
                       {"role" "assistant"
                        "content" (into (anthropic-content-blocks content)
                                        (keep openai-tool-call->anthropic-block)
                                        tool-calls)}))

          (recur (next remaining)
                 system-lines
                 []
                 (conj (flush-pending-tool-results acc pending-tool-results)
                       {"role" "user"
                        "content" (anthropic-content-blocks content)}))))
      (cond-> {:messages (flush-pending-tool-results acc pending-tool-results)}
        (seq system-lines)
        (assoc :system (str/join "\n\n" system-lines))))))

(defn- tool-definition-function
  [tool]
  (or (:function tool)
      (get tool "function")
      tool))

(defn- strip-unsupported-schema-keywords
  "Recursively remove anyOf/oneOf/allOf from a JSON schema map.
   Anthropic's API rejects these keywords anywhere in input_schema."
  [schema]
  (when (map? schema)
    (-> schema
        (dissoc "anyOf" "oneOf" "allOf")
        (update-vals (fn [v]
                       (cond
                         (map? v)        (strip-unsupported-schema-keywords v)
                         (sequential? v) (mapv #(cond-> % (map? %) strip-unsupported-schema-keywords) v)
                         :else           v))))))

(defn- openai-tool->anthropic-tool
  [tool]
  (let [function    (tool-definition-function tool)
        name        (some-> (or (:name function)
                                (get function "name"))
                            str
                            str/trim
                            not-empty)
        description (some-> (or (:description function)
                                (get function "description")
                                (:description tool)
                                (get tool "description"))
                            str
                            str/trim
                            not-empty)
        schema      (or (:parameters function)
                        (get function "parameters")
                        (:input_schema tool)
                        (get tool "input_schema")
                        {"type" "object"
                         "properties" {}})]
    (when name
      (cond-> {"name" name
               "input_schema" (if (map? schema)
                                (strip-unsupported-schema-keywords schema)
                                {"type" "object"
                                 "properties" {}})}
        description
        (assoc "description" description)))))

(defn- build-openai-request-body
  [model messages {:keys [tools temperature max-tokens] :as opts}]
  (cond-> {:model    model
           :messages messages}
    (seq tools) (assoc :tools tools)
    (some? temperature) (assoc :temperature temperature)
    (some? max-tokens) (assoc :max_tokens max-tokens)
    (streaming-request? opts) (assoc :stream true)))

(defn- build-anthropic-request-body
  [model messages {:keys [tools temperature max-tokens] :as opts}]
  (let [{:keys [messages system]} (build-anthropic-message-payload messages)]
    (cond-> {:model      model
             :messages   messages
             :max_tokens (or max-tokens default-anthropic-max-tokens)}
      (seq system) (assoc :system system)
      (seq tools) (assoc :tools (into [] (keep openai-tool->anthropic-tool) tools))
      (some? temperature) (assoc :temperature temperature)
      (streaming-request? opts) (assoc :stream true))))

(defn- anthropic-stop-reason->finish-reason
  [stop-reason]
  (case stop-reason
    "end_turn" "stop"
    "stop_sequence" "stop"
    "tool_use" "tool_calls"
    "max_tokens" "length"
    stop-reason))

(defn- usage->openai-usage
  [usage]
  (let [prompt-tokens     (get usage "input_tokens")
        completion-tokens (get usage "output_tokens")]
    (when (or (some? prompt-tokens)
              (some? completion-tokens))
      {"prompt_tokens" (or prompt-tokens 0)
       "completion_tokens" (or completion-tokens 0)
       "total_tokens" (+ (long (or prompt-tokens 0))
                         (long (or completion-tokens 0)))})))

(defn- anthropic-tool-input->arguments
  [input]
  (if (nil? input)
    "{}"
    (json/write-json-str input)))

(defn- anthropic-tool-block->openai-tool-call
  [block]
  (when (= "tool_use" (get block "type"))
    {"id" (or (get block "id")
              (str "tool_" (System/nanoTime)))
     "type" "function"
     "function" {"name" (or (get block "name") "")
                 "arguments" (anthropic-tool-input->arguments (get block "input"))}}))

(defn- anthropic-response->openai-response
  [response]
  (let [content     (or (get response "content") [])
        text        (->> content
                         (filter #(= "text" (get % "type")))
                         (map #(or (get % "text") ""))
                         (apply str))
        tool-calls  (into [] (keep anthropic-tool-block->openai-tool-call) content)
        message     (cond-> {"role" (or (get response "role") "assistant")}
                      (or (seq text) (seq tool-calls))
                      (assoc "content" (if (seq text) text ""))

                      (seq tool-calls)
                      (assoc "tool_calls" tool-calls))
        usage       (usage->openai-usage (get response "usage"))]
    (cond-> {"choices"
             [{"finish_reason" (anthropic-stop-reason->finish-reason
                                 (get response "stop_reason"))
               "message" message}]}
      usage
      (assoc "usage" usage))))

(defn- normalize-chat-response
  [provider-family response]
  (case provider-family
    :anthropic (anthropic-response->openai-response response)
    response))

(defn- streaming-request?
  [opts]
  (fn? (:on-delta opts)))

(defn- stream-complete?
  [{:keys [done? finish-reason]}]
  (or done?
      (some? finish-reason)))

(defn- incomplete-stream-ex
  [provider-id workload {:keys [content finish-reason tool-calls]}]
  (ex-info "LLM streaming response ended before completion"
           {:type                    :llm/incomplete-stream
            :provider-id             provider-id
            :workload                workload
            :finish-reason           finish-reason
            :partial-content-preview (when (seq content)
                                       (response-preview content))
            :tool-call-count         (count (remove nil? tool-calls))}))

(defn- build-request
  "Build the HTTP request map for a chat completion call."
  [{:keys [base-url auth-header api-key model allow-private-network? provider-family]} messages opts]
  (let [provider-family (or provider-family
                            (provider-family-from-base-url base-url))
        base-url        (normalize-base-url base-url)
        api-key         (some-> api-key str/trim not-empty)
        body            (case provider-family
                          :anthropic (build-anthropic-request-body model messages opts)
                          (build-openai-request-body model messages opts))]
    {:url           (str base-url
                         (case provider-family
                           :anthropic "/messages"
                           "/chat/completions"))
     :method        :post
     :headers       (merge {"Content-Type" "application/json"}
                           (provider-api-headers provider-family
                                                 {:api-key api-key
                                                  :auth-header auth-header}))
     :body          (json/write-json-str body)
     :allow-private-network? (boolean allow-private-network?)
     :timeout       request-timeout-ms
     :retry-enabled? true
     :policy-observer prompt/policy-decision!
     :request-label "LLM request"}))

(defn provider-credential-source
  "Return the normalized credential source for a provider."
  [provider]
  (let [auth-type (or (:llm.provider/credential-source provider)
                      (:credential-source provider)
                      (:llm.provider/auth-type provider)
                      (:auth-type provider))]
    (cond
      (keyword? auth-type) auth-type
      (string? auth-type)  (keyword auth-type)
      (some? (or (:llm.provider/oauth-account provider)
                 (:oauth-account provider))) :oauth-account
      (seq (or (:llm.provider/api-key provider)
               (:api-key provider))) :api-key
      :else :none)))

(defn provider-access-mode
  "Return the normalized access mode for a provider.

   :local   = built-in/local runtime access
   :api     = programmatic API credential access"
  [provider]
  (let [access-mode (or (:llm.provider/access-mode provider)
                        (:access-mode provider))
        credential-source (provider-credential-source provider)
        template-id (or (:llm.provider/template provider)
                        (:template provider))
        base-url    (or (:llm.provider/base-url provider)
                        (:base-url provider))]
    (cond
      (or (= :oauth-account credential-source)
          (= :api-key credential-source))
      :api

      (keyword? access-mode) access-mode
      (string? access-mode)  (keyword access-mode)

      (or (= template-id :ollama)
          (loopback-base-url? base-url))
      :local

      :else
      :api)))

(defn- provider-auth-header
  [provider]
  (case (provider-credential-source provider)
    :none
    nil

    :api-key
    (when-let [api-key (some-> (or (:llm.provider/api-key provider)
                                   (:api-key provider))
                               str/trim
                               not-empty)]
      (str "Bearer " api-key))

    :oauth-account
    (when-let [account-id (or (:llm.provider/oauth-account provider)
                              (:oauth-account provider))]
      (oauth/oauth-header (oauth/ensure-account-ready! account-id)))

    nil))

(defn- provider-error-message
  [provider-id ^Throwable e]
  (or (some-> e ex-data :body)
      (.getMessage e)
      (str "provider " provider-id " request failed")))

(defn- recoverable-stream-error?
  [^Throwable e]
  (or (= :llm/incomplete-stream (some-> e ex-data :type))
      (retryable-llm-error? e)))

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
        (prompt/policy-decision! (task-policy/provider-rate-limit-policy
                                  provider-id
                                  workload
                                  limit))
        (ex-info (str "Rate limit exceeded for provider " (name provider-id)
                      " (max " limit " requests/minute)")
                 {:type        :llm/rate-limit
                  :provider-id provider-id
                  :workload    workload
                  :limit       limit})))))

(defn- anthro-stream-tool-input->arguments
  [input]
  (cond
    (nil? input)
    ""

    (and (map? input) (empty? input))
    ""

    :else
    (json/write-json-str input)))

(defn- finalize-stream-tool-call
  [tool-call]
  (update-in tool-call ["function" "arguments"]
             (fn [arguments]
               (if (seq arguments)
                 arguments
                 "{}"))))

(defn- finalized-anthropic-stream-message
  [state]
  (finalized-stream-message
    (update state :tool-calls
            (fn [tool-calls]
              (mapv finalize-stream-tool-call
                    (remove nil? tool-calls))))))

(defn- handle-openai-stream-event!
  [stream-state {:keys [data]} opts provider-id workload]
  (cond
    (str/blank? data)
    nil

    (= "[DONE]" data)
    (vswap! stream-state assoc :done? true)

    :else
    (let [chunk  (parse-json-body data provider-id workload)
          choice (get-in chunk ["choices" 0])]
      (vswap! stream-state merge-choice-delta choice)
      (when-let [content (get-in choice ["delta" "content"])]
        (when-let [on-delta (:on-delta opts)]
          (on-delta {:provider-id provider-id
                     :workload workload
                     :delta content
                     :content (:content @stream-state)}))))))

(defn- handle-anthropic-stream-event!
  [stream-state {:keys [event data]} opts provider-id workload]
  (when-not (str/blank? data)
    (let [chunk      (parse-json-body data provider-id workload)
          event-type (or event (get chunk "type"))]
      (case event-type
        "ping"
        nil

        "message_start"
        (let [message (or (get chunk "message") {})]
          (vswap! stream-state
                  (fn [state]
                    (cond-> state
                      (get message "role")
                      (assoc :role (get message "role"))

                      (usage->openai-usage (get message "usage"))
                      (assoc :usage (usage->openai-usage (get message "usage")))))))

        "content_block_start"
        (let [index (long (or (get chunk "index") 0))
              block (or (get chunk "content_block") {})]
          (when (= "tool_use" (get block "type"))
            (vswap! stream-state update :tool-calls
                    merge-tool-call-delta
                    {"index" index
                     "id" (get block "id")
                     "type" "function"
                     "function" {"name" (or (get block "name") "")
                                 "arguments" (anthro-stream-tool-input->arguments
                                               (get block "input"))}})))

        "content_block_delta"
        (let [index (long (or (get chunk "index") 0))
              delta (or (get chunk "delta") {})
              delta-type (get delta "type")]
          (case delta-type
            "text_delta"
            (let [text (get delta "text")]
              (vswap! stream-state update :content append-text text)
              (when-let [on-delta (:on-delta opts)]
                (on-delta {:provider-id provider-id
                           :workload workload
                           :delta text
                           :content (:content @stream-state)})))

            "input_json_delta"
            (vswap! stream-state update :tool-calls
                    merge-tool-call-delta
                    {"index" index
                     "function" {"arguments" (or (get delta "partial_json") "")}})

            nil))

        "message_delta"
        (vswap! stream-state
                (fn [state]
                  (cond-> state
                    (get-in chunk ["delta" "stop_reason"])
                    (assoc :finish-reason
                           (anthropic-stop-reason->finish-reason
                             (get-in chunk ["delta" "stop_reason"])))

                    (usage->openai-usage (get chunk "usage"))
                    (assoc :usage (usage->openai-usage (get chunk "usage"))))))

        "message_stop"
        (vswap! stream-state assoc :done? true)

        nil))))

(defn- stream-chat-response
  [provider-family stream-req fallback-req opts provider-id workload]
  (let [stream-state (volatile! {:role "assistant"
                                 :content ""
                                 :tool-calls []
                                 :done? false})]
    (try
      (let [stream-resp (http/request-events
                          (assoc stream-req
                                 :on-event
                                 (fn [event]
                                   (case provider-family
                                     :anthropic
                                     (handle-anthropic-stream-event! stream-state event opts provider-id workload)

                                     (handle-openai-stream-event! stream-state event opts provider-id workload)))))]
        (when (and (:streamed? stream-resp)
                   (not (stream-complete? @stream-state)))
          (throw (incomplete-stream-ex provider-id workload @stream-state)))
        (if (:streamed? stream-resp)
          (assoc stream-resp
                 :response (case provider-family
                             :anthropic (finalized-anthropic-stream-message @stream-state)
                             (finalized-stream-message @stream-state)))
          stream-resp))
      (catch Exception e
        (if (recoverable-stream-error? e)
          (do
            (log/warn e "LLM streaming request interrupted; retrying once as non-streaming request"
                      {:provider-id provider-id
                       :workload workload
                       :partial-content-preview (some-> @stream-state :content response-preview)
                       :tool-call-count (count (remove nil? (:tool-calls @stream-state)))})
            (assoc (http/request fallback-req)
                   :stream-recovered? true
                   :stream-partial-content (:content @stream-state)))
          (throw e))))))

(defn- attempt-chat
  [messages opts {:keys [provider provider-id workload]}]
  (let [base-url  (or (:llm.provider/base-url provider) (:base-url provider))
        model     (or (:llm.provider/model provider) (:model provider))
        provider-family (provider-family-from-base-url base-url)
        api-key   (provider-api-key provider)
        auth-header (provider-auth-header provider)
        request-config {:base-url base-url
                        :provider-family provider-family
                        :api-key api-key
                        :auth-header auth-header
                        :model model
                        :allow-private-network? (provider-allows-private-network? provider)}
        _         (check-rate-limit! provider-id provider workload)
        req       (build-request request-config messages opts)
        fallback-req (when (streaming-request? opts)
                       (build-request request-config
                                      messages
                                      (dissoc opts :on-delta)))
        _         (log/debug "LLM request via provider" provider-id
                             "to" base-url
                             "model" model
                             (when workload (str "workload " workload)))
        resp      (if (streaming-request? opts)
                    (stream-chat-response provider-family
                                          req
                                          fallback-req
                                          opts
                                          provider-id
                                          workload)
                    (http/request req))
        status    (:status resp)]
    (when (not= 200 status)
      (throw (ex-info (str "LLM request failed with status " status)
                      {:status      status
                       :headers     (:headers resp)
                       :body        (:body resp)
                       :provider-id provider-id
                        :workload    workload})))
    (let [response (if (:streamed? resp)
                     (or (:response resp)
                         (normalize-chat-response provider-family
                                                  (parse-json-body (:body resp)
                                                                   provider-id
                                                                   workload)))
                     (normalize-chat-response provider-family
                                              (parse-json-body (:body resp)
                                                               provider-id
                                                               workload)))]
      (when (:stream-recovered? resp)
        (maybe-report-recovered-stream-content! opts
                                                provider-id
                                                workload
                                                (:stream-partial-content resp)
                                                response))
      response)))

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
      (let [provider-id (:provider-id attempt)
            model       (or (:llm.provider/model (:provider attempt))
                            (:model (:provider attempt)))
            t0     (now-ms)
            call-id (random-uuid)
            result (try
                     {:ok? true
                      :response (attempt-chat messages opts attempt)}
                     (catch Exception e
                       {:ok? false
                        :error e}))
            dur-ms (- (now-ms) t0)]
        (try
          (let [usage       (when (:ok? result)
                              (get (:response result) "usage"))
                log-entry   (cond-> {:id          call-id
                                     :session-id  (:session-id opts)
                                     :provider-id provider-id
                                     :model       model
                                     :workload    (:workload attempt)
                                     :duration-ms dur-ms
                                     :messages    (json/write-json-str messages)
                                     :created-at  (java.util.Date.)}
                              (:ok? result)
                              (assoc :status   :ok
                                     :response (json/write-json-str (:response result)))
                              (not (:ok? result))
                              (assoc :status :error
                                     :error  (str (:error result)))
                              (some? (:tools opts))
                              (assoc :tools (json/write-json-str (:tools opts)))
                              (get usage "prompt_tokens")
                              (assoc :prompt-tokens (get usage "prompt_tokens"))
                              (get usage "completion_tokens")
                              (assoc :completion-tokens (get usage "completion_tokens")))]
            (submit-log-write! log-entry))
          (catch Exception e
            (log/debug e "Failed to build LLM call log entry")))
        (if (:ok? result)
          (let [response  (:response result)
                response* (if (instance? clojure.lang.IObj response)
                            (with-meta response
                              (merge (meta response)
                                     {:provider-id provider-id
                                      :model model
                                      :workload (:workload attempt)
                                      :llm-call-id call-id}))
                            response)]
            (record-provider-success! provider-id)
            {:status :ok
             :response response*})
          (let [e           (:error result)
                attempt-id  provider-id
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
              (prompt/policy-decision! {:decision-type :provider-retry-policy
                                        :allowed? true
                                        :mode :preflight-cooldown
                                        :delay-ms delay-ms
                                        :round round
                                        :provider-id provider-id
                                        :workload workload
                                        :max-retry-rounds max-retry-rounds*
                                        :max-retry-wait-ms max-retry-wait-ms*
                                        :reason "All routed providers are cooling down"
                                        :providers (mapv :provider-id attempts)})
              (log/warn "All routed LLM providers are cooling down; waiting before retry"
                        {:provider-id provider-id
                         :workload workload
                         :round round
                         :delay-ms delay-ms
                         :providers (mapv :provider-id attempts)})
              (sleep-ms! delay-ms)
              (recur (inc round)))
            (do
              (prompt/policy-decision! {:decision-type :provider-retry-policy
                                        :allowed? false
                                        :mode :retry-limit
                                        :round round
                                        :provider-id provider-id
                                        :workload workload
                                        :max-retry-rounds max-retry-rounds*
                                        :max-retry-wait-ms max-retry-wait-ms*
                                        :delay-ms preflight-delay-ms
                                        :reason "LLM providers are still cooling down"
                                        :providers (mapv :provider-id attempts)})
              (throw (ex-info "LLM providers are still cooling down"
                              {:provider-id provider-id
                               :workload workload
                               :round round
                               :cooldown-ms preflight-delay-ms
                               :max-retry-rounds max-retry-rounds*
                               :max-retry-wait-ms max-retry-wait-ms*}))))
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
                  (prompt/policy-decision! {:decision-type :provider-retry-policy
                                            :allowed? true
                                            :mode :provider-backoff
                                            :delay-ms delay-ms
                                            :round round
                                            :provider-id (:provider-id round-result)
                                            :workload (:workload round-result)
                                            :max-retry-rounds max-retry-rounds*
                                            :max-retry-wait-ms max-retry-wait-ms*
                                            :error (provider-error-message (:provider-id round-result)
                                                                           (:error round-result))})
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
                  (prompt/policy-decision! {:decision-type :provider-retry-policy
                                            :allowed? false
                                            :mode :retry-limit
                                            :round round
                                            :provider-id (:provider-id round-result)
                                            :workload (:workload round-result)
                                            :max-retry-rounds max-retry-rounds*
                                            :max-retry-wait-ms max-retry-wait-ms*
                                            :delay-ms (:delay-ms round-result)
                                            :error (provider-error-message (:provider-id round-result)
                                                                           (:error round-result))})
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

(defn fetch-provider-models
  "Fetch available models from a provider's /models endpoint.
   Accepts a map with :base-url and optionally :api-key or :auth-header.
   Returns a sorted vector of model ID strings."
  [{:keys [base-url api-key auth-header]}]
  (let [provider-family (provider-family-from-base-url base-url)
        base-url        (normalize-base-url base-url)
        api-key         (some-> api-key str/trim not-empty)
        allow-private?  (loopback-base-url? base-url)
        resp            (http/request {:url     (str base-url "/models")
                                       :method  :get
                                       :headers (provider-api-headers provider-family
                                                                      {:api-key api-key
                                                                       :auth-header auth-header})
                                       :allow-private-network? allow-private?
                                       :timeout 15000
                                       :policy-observer prompt/policy-decision!
                                       :request-label "Fetch provider models"})]
    (when (= 200 (:status resp))
      (let [body (json/read-json (:body resp))
            data (or (get body "data") [])]
        (->> data
             (map #(get % "id"))
             (filter string?)
             sort
             vec)))))

(defn- normalize-model-flag
  [value]
  (cond
    (nil? value) nil
    (true? value) true
    (false? value) false
    (number? value) (not (zero? (long value)))
    (string? value) (let [normalized (some-> value str/trim str/lower-case)]
                      (cond
                        (#{"true" "yes" "1" "vision" "image" "images" "multimodal"} normalized) true
                        (#{"false" "no" "0" "text" "text-only"} normalized) false
                        :else nil))
    :else nil))

(defn- modality-values
  [value]
  (cond
    (nil? value) []
    (string? value) [(str/lower-case value)]
    (keyword? value) [(some-> value name str/lower-case)]
    (sequential? value) (mapcat modality-values value)
    :else []))

(defn- image-capable-modality?
  [value]
  (let [normalized (some-> value str/lower-case)]
    (boolean (and normalized
                  (or (str/includes? normalized "image")
                      (str/includes? normalized "vision")
                      (str/includes? normalized "multimodal")
                      (str/includes? normalized "omni"))))))

(defn- vision-capable-model-id?
  [model-id]
  (let [normalized (some-> model-id str/lower-case)]
    (boolean (and normalized
                  (or (re-find #"(^|[/:-])gpt-4o($|[-/])" normalized)
                      (re-find #"(^|[/:-])gpt-4\.1($|[-/])" normalized)
                      (re-find #"(^|[/:-])gemini($|[-/])" normalized)
                      (re-find #"(^|[/:-])claude($|[-/])" normalized)
                      (str/includes? normalized "vision")
                      (str/includes? normalized "vl")
                      (str/includes? normalized "image")
                      (str/includes? normalized "multimodal")
                      (str/includes? normalized "omni")
                      (str/includes? normalized "llava")
                      (str/includes? normalized "pixtral")
                      (str/includes? normalized "minicpm-v")
                      (str/includes? normalized "qvq"))))))

(defn- infer-model-vision
  [model]
  (let [flags      [(normalize-model-flag (get model "vision"))
                    (normalize-model-flag (get model "image_input"))
                    (normalize-model-flag (get-in model ["capabilities" "vision"]))
                    (normalize-model-flag (get-in model ["capabilities" "image_input"]))
                    (normalize-model-flag (get-in model ["capabilities" "image"]))
                    (normalize-model-flag (get-in model ["capabilities" "multimodal"]))]
        modalities (->> [(get model "input_modalities")
                         (get model "modalities")
                         (get model "supported_input_modalities")
                         (get model "input_modality")
                         (get-in model ["architecture" "modality"])
                         (get-in model ["architecture" "input_modalities"])
                         (get-in model ["architecture" "modalities"])
                         (get-in model ["capabilities" "input_modalities"])]
                        (mapcat modality-values)
                        (remove str/blank?))
        model-id   (or (get model "id")
                       (get model "model")
                       (get-in model ["data" "id"]))]
    (cond
      (some true? flags)
      {:vision? true
       :source :metadata}

      (some image-capable-modality? modalities)
      {:vision? true
       :source :metadata}

      (or (some false? flags)
          (seq modalities))
      {:vision? false
       :source :metadata}

      (vision-capable-model-id? model-id)
      {:vision? true
       :source :model-id}

      :else
      {:vision? false
       :source :model-id})))

(def ^:private model-context-window-paths
  [["context_length"]
   ["context_window"]
   ["context_window_tokens"]
   ["max_context_tokens"]
   ["max_context_length"]
   ["max_input_tokens"]
   ["max_prompt_tokens"]
   ["input_token_limit"]
   ["max_sequence_length"]
   ["max_model_len"]
   ["architecture" "context_length"]
   ["architecture" "max_context_tokens"]
   ["architecture" "max_input_tokens"]
   ["capabilities" "context_length"]
   ["capabilities" "context_window"]
   ["capabilities" "max_input_tokens"]
   ["limits" "context_length"]
   ["limits" "context_window"]
   ["limits" "max_context_tokens"]
   ["limits" "max_input_tokens"]])

(def ^:private model-context-window-max 10000000)
(def ^:private recommended-context-input-max 160000)
(def ^:private recommended-system-budget-max 24000)
(def ^:private recommended-min-system-budget 256)
(def ^:private recommended-min-history-budget 256)

(defn- parse-positive-long
  [value]
  (try
    (let [parsed
          (cond
            (integer? value) (long value)
            (number? value) (long (Math/floor (double value)))
            (string? value)
            (let [normalized (-> value
                                 str/trim
                                 str/lower-case
                                 (str/replace #"[,_\s]" ""))]
              (cond
                (re-matches #"[0-9]+" normalized)
                (Long/parseLong normalized)

                (re-matches #"[0-9]+k" normalized)
                (* 1000 (Long/parseLong (subs normalized 0 (dec (count normalized)))))

                (re-matches #"[0-9]+m" normalized)
                (* 1000000 (Long/parseLong (subs normalized 0 (dec (count normalized)))))

                :else nil))
            :else nil)]
      (when (and parsed
                 (<= 512 parsed model-context-window-max))
        parsed))
    (catch Exception _
      nil)))

(defn- get-map-value
  [m key]
  (when (map? m)
    (let [key-name (some-> key name)]
      (or (get m key)
          (when key-name
            (or (get m key-name)
                (get m (keyword key-name))))))))

(defn- get-in-map-values
  [m path]
  (reduce (fn [acc key]
            (get-map-value acc key))
          m
          path))

(defn- context-window-key?
  [key]
  (when-let [normalized (some-> key
                                name
                                str/lower-case
                                (str/replace "-" "_"))]
    (and (not (or (str/includes? normalized "output")
                  (str/includes? normalized "completion")
                  (str/includes? normalized "response")
                  (str/includes? normalized "generated")
                  (str/includes? normalized "reasoning")))
         (or (= normalized "context_length")
             (= normalized "context_window")
             (= normalized "context_window_tokens")
             (= normalized "max_context_tokens")
             (= normalized "max_context_length")
             (= normalized "max_input_tokens")
             (= normalized "max_prompt_tokens")
             (= normalized "input_token_limit")
             (= normalized "max_sequence_length")
             (= normalized "max_model_len")
             (str/includes? normalized "context_token")
             (str/includes? normalized "context_length")
             (str/includes? normalized "max_input_token")
             (str/includes? normalized "max_prompt_token")))))

(defn- nested-context-window-values
  ([value]
   (nested-context-window-values value 0))
  ([value depth]
   (if (> (long depth) 4)
     []
     (cond
       (map? value)
       (mapcat (fn [[k v]]
                 (concat
                   (when (context-window-key? k)
                     (when-let [parsed (parse-positive-long v)]
                       [parsed]))
                   (nested-context-window-values v (inc depth))))
               value)

       (sequential? value)
       (mapcat #(nested-context-window-values % (inc depth)) value)

       :else
       []))))

(defn- infer-model-context-window
  [model]
  (let [explicit-values (->> model-context-window-paths
                             (map #(get-in-map-values model %))
                             (keep parse-positive-long)
                             vec)
        nested-values   (->> (nested-context-window-values model)
                             (keep parse-positive-long)
                             vec)
        candidates      (if (seq explicit-values)
                          explicit-values
                          nested-values)]
    (when (seq candidates)
      {:context-window (apply min candidates)
       :source :metadata})))

(defn- recommended-model-budgets
  [context-window]
  (let [window           (long context-window)
        input-budget-cap (-> (* 0.75 (double window))
                             Math/floor
                             long
                             (long-max 512)
                             (long-min recommended-context-input-max))
        max-system       (long-max recommended-min-system-budget
                                   (- input-budget-cap recommended-min-history-budget))
        system-target    (long (Math/round (* 0.25 (double input-budget-cap))))
        system-budget    (-> system-target
                             (long-max recommended-min-system-budget)
                             (long-min recommended-system-budget-max)
                             (long-min max-system))
        history-budget   (long-max recommended-min-history-budget
                                   (- input-budget-cap system-budget))]
    {:system-prompt-budget system-budget
     :history-budget history-budget
     :input-budget-cap input-budget-cap}))

(defn- encode-model-path-segment
  [model-id]
  (when-let [value (some-> model-id str)]
    (-> (URLEncoder/encode value (.name StandardCharsets/UTF_8))
        (str/replace "+" "%20"))))

(defn fetch-provider-model-metadata
  "Fetch a single model's metadata from /models/{id} and infer whether it
   accepts image input and context-window hints. Falls back to model-id
   inference when the provider does not expose a usable metadata endpoint."
  [{:keys [base-url api-key auth-header model]}]
  (let [model-id        (some-> model str/trim not-empty)
        provider-family (provider-family-from-base-url base-url)
        base-url        (normalize-base-url base-url)
        api-key         (some-> api-key str/trim not-empty)
        allow-private?  (loopback-base-url? base-url)
        fallback        (let [{:keys [vision? source]} (infer-model-vision {"id" model-id})]
                          {:id model-id
                           :vision? vision?
                           :vision-source source})
        metadata-url    (str base-url
                             "/models/"
                             (encode-model-path-segment model-id))
        resp            (http/request {:url     metadata-url
                                       :method  :get
                                       :headers (provider-api-headers provider-family
                                                                      {:api-key api-key
                                                                       :auth-header auth-header})
                                       :allow-private-network? allow-private?
                                       :timeout 15000
                                       :policy-observer prompt/policy-decision!
                                       :request-label "Fetch provider model metadata"})]
    (if (= 200 (:status resp))
      (let [body                  (json/read-json (:body resp))
            body                  (if (map? body) body {"id" model-id})
            body                  (assoc body "id" (or (get body "id") model-id))
            {:keys [vision? source]} (infer-model-vision body)
            context-meta          (infer-model-context-window body)
            context-window        (:context-window context-meta)
            context-window-source (:source context-meta)
            budgets               (when context-window
                                    (recommended-model-budgets context-window))]
        (cond-> {:id model-id
                 :vision? vision?
                 :vision-source source}
          context-window
          (assoc :context-window context-window
                 :context-window-source context-window-source)

          budgets
          (assoc :recommended-system-prompt-budget (:system-prompt-budget budgets)
                 :recommended-history-budget (:history-budget budgets)
                 :recommended-input-budget-cap (:input-budget-cap budgets))))
      fallback)))

(defn chat-simple
  "Convenience: send messages, return the assistant's text content."
  [messages & opts]
  (let [opts-map      (apply hash-map opts)
        resp          (budgeted-chat-request :chat-simple
                                             messages
                                             opts-map
                                             #(apply chat messages opts))
        request-info (merge (apply hash-map opts)
                            (select-keys (meta resp) [:provider-id :workload]))]
    (simple-message-content! resp request-info)))

(defn chat-with-tools
  "Send messages with tools. Returns the full message (may contain tool_calls)."
  [messages tools & opts]
  (let [opts-map      (apply hash-map (concat [:tools tools] opts))
        resp          (budgeted-chat-request :chat-with-tools
                                             messages
                                             opts-map
                                             #(apply chat messages
                                                     (concat [:tools tools] opts)))
        request-info (merge (apply hash-map opts)
                            (select-keys (meta resp) [:provider-id :workload]))]
    (tool-message! resp request-info)))
