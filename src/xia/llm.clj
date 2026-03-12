(ns xia.llm
  "Multi-provider LLM client. All providers follow the OpenAI-compatible
   chat completions API shape, so one client handles OpenAI, Anthropic
   (via proxy), Qwen, local Ollama, etc."
  (:require [hato.client :as hc]
            [charred.api :as json]
            [clojure.tools.logging :as log]
            [xia.db :as db]))

;; ---------------------------------------------------------------------------
;; HTTP client
;; ---------------------------------------------------------------------------

(defonce ^:private http-client (delay (hc/build-http-client {:connect-timeout 30000})))

;; ---------------------------------------------------------------------------
;; Provider resolution
;; ---------------------------------------------------------------------------

(defn- resolve-provider
  "Return a provider map. Uses the explicitly given provider-id, or falls back
   to the default provider in the DB."
  ([] (resolve-provider nil))
  ([provider-id]
   (if provider-id
     (or (db/get-provider provider-id)
         (throw (ex-info (str "Unknown provider: " provider-id) {:provider provider-id})))
     (or (db/get-default-provider)
         (throw (ex-info "No default LLM provider configured. Run first-time setup." {}))))))

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
    {:uri          (str base-url "/chat/completions")
     :method       :post
     :headers      {"Authorization" (str "Bearer " api-key)
                    "Content-Type"  "application/json"}
     :body         (json/write-json-str body)
     :http-client  @http-client}))

(defn chat
  "Send a chat completion request.

   Options:
     :provider-id  — keyword id of provider (default: DB default)
     :tools        — vector of tool definitions (OpenAI function-calling format)
     :temperature  — float
     :max-tokens   — int

   Returns the parsed response body as a Clojure map."
  [messages & {:keys [provider-id tools temperature max-tokens] :as opts}]
  (let [provider  (resolve-provider provider-id)
        base-url  (or (:llm.provider/base-url provider) (:base-url provider))
        api-key   (or (:llm.provider/api-key provider) (:api-key provider))
        model     (or (:llm.provider/model provider) (:model provider))
        req       (build-request {:base-url base-url :api-key api-key :model model}
                                 messages opts)
        _         (log/debug "LLM request to" base-url "model" model)
        resp      (hc/request req)
        status    (:status resp)]
    (when (not= 200 status)
      (log/error "LLM request failed" status (:body resp))
      (throw (ex-info (str "LLM request failed with status " status)
                      {:status status :body (:body resp)})))
    (json/read-json (:body resp))))

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
