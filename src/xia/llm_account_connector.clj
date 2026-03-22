(ns xia.llm-account-connector
  "Provider-specific browser-backed account connectors for subscription-style
   access that is not exposed as a standard API credential."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.browser :as browser]))

(def ^:private openai-connector-id :openai-browser)
(def ^:private openai-home-url "https://chatgpt.com/")
(def ^:private openai-response-timeout-ms 120000)
(def ^:private poll-interval-ms 1000)
(def ^:private stable-response-polls 2)
(def ^:private browser-session-credential-source :browser-session)

(def ^:private openai-prompt-selectors
  ["#prompt-textarea"
   "textarea"
   "div[contenteditable=\"true\"]#prompt-textarea"
   "div[contenteditable=\"true\"][data-testid=\"composer-input\"]"
   "main textarea"])

(def ^:private openai-send-selectors
  ["button[data-testid=\"send-button\"]"
   "button[aria-label*=\"Send\"]"
   "button[aria-label*=\"send\"]"
   "form button[type=\"submit\"]"])

(def ^:private openai-assistant-selectors
  ["[data-message-author-role=\"assistant\"]"
   "main [data-message-author-role=\"assistant\"]"
   "article [data-message-author-role=\"assistant\"]"])

(defn connector-id
  [provider]
  (let [template-id        (or (:llm.provider/template provider)
                               (:template provider))
        raw-credential-source (or (:llm.provider/credential-source provider)
                                  (:credential-source provider)
                                  (:llm.provider/auth-type provider)
                                  (:auth-type provider))
        credential-source  (cond
                             (keyword? raw-credential-source) raw-credential-source
                             (string? raw-credential-source)  (keyword raw-credential-source)
                             :else raw-credential-source)]
    (when (and (= :openai template-id)
               (= browser-session-credential-source credential-source))
      openai-connector-id)))

(defn browser-session-id
  [provider]
  (or (:llm.provider/browser-session provider)
      (:browser-session provider)))

(defn browser-session-connected?
  [session-id]
  (boolean
   (and (string? session-id)
        (some #(= session-id (:session-id %))
              (browser/list-sessions)))))

(defn requires-browser-session?
  [provider]
  (= browser-session-credential-source
     (or (:llm.provider/credential-source provider)
         (:credential-source provider)
         (:llm.provider/auth-type provider)
         (:auth-type provider))))

(defn supported-connector?
  [connector]
  (= openai-connector-id connector))

(defn- visible-query
  [session-id selector]
  (try
    (get (browser/query-elements session-id
                                 :selector selector
                                 :visible-only true
                                 :limit 50)
         :elements)
    (catch Exception e
      (log/debug e "Browser query failed" {:session-id session-id
                                            :selector selector})
      [])))

(defn- first-visible-selector
  [session-id selectors]
  (some (fn [selector]
          (when (seq (visible-query session-id selector))
            selector))
        selectors))

(defn- assistant-texts
  [session-id]
  (->> openai-assistant-selectors
       (mapcat #(visible-query session-id %))
       (map :text)
       (keep (fn [text]
               (let [trimmed (some-> text str str/trim)]
                 (when (seq trimmed)
                   trimmed))))
       vec))

(defn- normalize-message-role
  [message]
  (let [role (or (:role message)
                 (get message "role")
                 "user")]
    (if (keyword? role)
      (name role)
      (str role))))

(defn- normalize-message-content
  [message]
  (let [content (or (:content message)
                    (get message "content"))]
    (cond
      (string? content) content
      (nil? content)    ""
      :else             (json/write-json-str content))))

(defn- render-tool-call-summary
  [tool-calls]
  (when (seq tool-calls)
    (str "\nTool calls:\n" (json/write-json-str tool-calls))))

(defn- render-transcript-entry
  [message]
  (let [role       (normalize-message-role message)
        content    (normalize-message-content message)
        tool-calls (or (:tool_calls message)
                       (get message "tool_calls"))]
    (str (str/upper-case role) ":\n"
         (if (seq content) content "(no content)")
         (or (render-tool-call-summary tool-calls) ""))))

(defn- render-tool-instructions
  [tools]
  (when (seq tools)
    (str
     "Available Xia tools (OpenAI function-calling schema):\n"
     (json/write-json-str tools)
     "\n\n"
     "If a tool is required, reply with raw JSON only in this exact shape:\n"
     "{\"tool_calls\":[{\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"tool_name\",\"arguments\":\"{\\\"arg\\\":\\\"value\\\"}\"}}],\"content\":\"\"}\n"
     "If no tool is needed, reply normally in plain text without markdown fences.")))

(defn- request-prompt
  [messages {:keys [tools]}]
  (str
   "You are answering inside Xia through a browser-backed OpenAI subscription connector.\n"
   "Treat the transcript below as the authoritative conversation context for this request.\n"
   "Ignore any prior page conversation state from earlier browser sessions.\n\n"
   (when-let [tool-instructions (render-tool-instructions tools)]
     (str tool-instructions "\n\n"))
   "Conversation transcript:\n\n"
   (->> messages
        (map render-transcript-entry)
        (str/join "\n\n"))
   "\n\nRespond now as the assistant."))

(defn- strip-code-fences
  [text]
  (let [trimmed (some-> text str str/trim)]
    (if (and (string? trimmed)
             (str/starts-with? trimmed "```")
             (str/ends-with? trimmed "```"))
      (-> trimmed
          (str/replace-first #"^```[a-zA-Z0-9_-]*\s*" "")
          (str/replace #"```$" "")
          str/trim)
      trimmed)))

(defn- normalize-tool-call
  [tool-call index]
  (let [call-map   (if (map? tool-call) tool-call {})
        function*  (if (map? (or (:function call-map) (get call-map "function")))
                     (or (:function call-map) (get call-map "function"))
                     {})
        name*      (or (:name function*)
                       (get function* "name")
                       (:name call-map)
                       (get call-map "name"))
        args-value (or (:arguments function*)
                       (get function* "arguments")
                       (:arguments call-map)
                       (get call-map "arguments"))
        args-text  (cond
                     (string? args-value) args-value
                     (map? args-value)    (json/write-json-str args-value)
                     (nil? args-value)    "{}"
                     :else                (str args-value))]
    {"id" (or (:id call-map)
              (get call-map "id")
              (str "call_" (inc (long index))))
     "type" "function"
     "function" {"name" (or name* "unknown_tool")
                 "arguments" args-text}}))

(defn- response-from-text
  [text]
  (let [body        (or (strip-code-fences text) "")
        parsed-json (try
                      (when (and (str/starts-with? body "{")
                                 (str/ends-with? body "}"))
                        (json/read-json body))
                      (catch Exception _
                        nil))
        tool-calls  (some->> (or (:tool_calls parsed-json)
                                 (get parsed-json "tool_calls"))
                             (map-indexed normalize-tool-call)
                             vec)
        content     (cond
                      (and parsed-json
                           (contains? parsed-json "content"))
                      (or (get parsed-json "content") "")

                      (and parsed-json
                           (contains? parsed-json :content))
                      (or (:content parsed-json) "")

                      :else
                      body)]
    {"choices"
     [{"finish_reason" (if (seq tool-calls) "tool_calls" "stop")
       "message" (cond-> {"role" "assistant"
                          "content" (if (string? content)
                                      content
                                      (json/write-json-str content))}
                   (seq tool-calls)
                   (assoc "tool_calls" tool-calls))}]}))

(defn start-connection!
  [connector]
  (case connector
    :openai-browser
    (do
      (let [opened (browser/open-session openai-home-url
                                         :backend :playwright
                                         :headless false)]
        {:connector  openai-connector-id
         :session-id (:session-id opened)
         :login-url  openai-home-url
         :message    "A Xia-managed browser opened for OpenAI sign-in. Complete the login there, then return to Xia and finish the connection."}))
    (throw (ex-info "Unsupported account connector"
                    {:connector connector}))))

(defn complete-connection!
  [connector session-id]
  (case connector
    :openai-browser
    (let [_              (browser/navigate session-id openai-home-url)
          prompt-selector (first-visible-selector session-id openai-prompt-selectors)]
      (when-not prompt-selector
        (throw (ex-info "OpenAI sign-in is not complete yet. After the ChatGPT page loads, try Finish Sign-In again."
                        {:connector openai-connector-id
                         :session-id session-id})))
      {:connector  openai-connector-id
       :session-id session-id
       :connected  true
       :message    "OpenAI account session is ready."})
    (throw (ex-info "Unsupported account connector"
                    {:connector connector}))))

(defn request-chat
  [provider messages {:keys [on-delta] :as opts}]
  (let [connector  (connector-id provider)
        seed-id    (browser-session-id provider)]
    (case connector
      :openai-browser
      (do
        (when-not (seq (or seed-id ""))
          (throw (ex-info "OpenAI account connector requires a connected browser session."
                          {:connector connector
                           :provider-id (or (:llm.provider/id provider)
                                            (:id provider))})))
        (let [url       (if-let [model (some-> (or (:llm.provider/model provider)
                                                   (:model provider))
                                               str
                                               str/trim
                                               not-empty)]
                          (str openai-home-url "?model=" (java.net.URLEncoder/encode model "UTF-8"))
                          openai-home-url)
              session   (browser/clone-session seed-id :url url)
              session-id (:session-id session)
              prompt    (request-prompt messages opts)
              baseline  (assistant-texts session-id)]
          (try
            (let [prompt-selector (or (first-visible-selector session-id openai-prompt-selectors)
                                      (throw (ex-info "OpenAI page does not expose the message composer. Reconnect the account session and try again."
                                                      {:connector connector
                                                       :session-id session-id})))
                  send-selector   (or (first-visible-selector session-id openai-send-selectors)
                                      (throw (ex-info "OpenAI page does not expose the send button. Reconnect the account session and try again."
                                                      {:connector connector
                                                       :session-id session-id})))]
              (browser/fill-selector session-id prompt-selector prompt)
              (browser/click session-id send-selector)
              (let [deadline (+ (System/currentTimeMillis) openai-response-timeout-ms)]
                (loop [last-text nil
                       stable-count 0]
                  (Thread/sleep poll-interval-ms)
                  (let [texts        (assistant-texts session-id)
                        candidate    (last texts)
                        baseline-last (last baseline)
                        changed?     (or (> (count texts) (count baseline))
                                         (and (seq candidate)
                                              (not= candidate baseline-last)))]
                    (cond
                      (and changed? (seq candidate))
                      (let [stable-count* (if (= candidate last-text)
                                            (inc (long stable-count))
                                            1)]
                        (if (>= stable-count* stable-response-polls)
                          (do
                            (when on-delta
                              (on-delta {:delta candidate
                                         :content candidate}))
                            (response-from-text candidate))
                          (recur candidate stable-count*)))

                      (>= (System/currentTimeMillis) deadline)
                      (throw (ex-info "Timed out waiting for OpenAI account response."
                                      {:connector connector
                                       :session-id session-id}))

                      :else
                      (recur last-text stable-count))))))
            (finally
              (when session-id
                (browser/close-session session-id))))))
      (throw (ex-info "Unsupported account connector"
                      {:connector connector})))))
