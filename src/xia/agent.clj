(ns xia.agent
  "Agent loop — the core runtime that processes user messages.

   Loop: user message → update working memory → build context
         → LLM call (with tools) → tool calls? → response

   Skills = markdown instructions injected into the system prompt.
   Tools  = executable functions the LLM can call via function-calling."
  (:require [clojure.tools.logging :as log]
            [charred.api :as json]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.tool :as tool]
            [xia.context :as context]
            [xia.prompt :as prompt]
            [xia.working-memory :as wm]))

(def ^:private max-tool-rounds 10)

;; build-messages is now in xia.context

(defn- report-status!
  [message & {:as extra}]
  (prompt/status! (merge {:state :running
                          :message message}
                         extra)))

(defn- execute-tool-calls
  "Execute tool calls from the LLM response, return tool result messages."
  [tool-calls context]
  (mapv (fn [tc]
          (let [func-name (get-in tc ["function" "name"])
                args-str  (get-in tc ["function" "arguments"])
                args      (try (json/read-json args-str) (catch Exception _ {}))
                tool-id   (keyword func-name)
                result    (tool/execute-tool tool-id args context)]
            (log/debug "Tool call completed:" func-name)
            {:role         "tool"
             :tool_call_id (get tc "id")
             :content      (if (string? result) result (json/write-json-str result))}))
        tool-calls))

(defn process-message
  "Process a user message in the given session. Returns the assistant's response.

   1. Updates working memory (retrieval pipeline)
   2. Builds context: system prompt (identity + WM context + skills) + history
   3. Calls the LLM with available tools (function-calling)
   4. If the LLM wants to use tools, executes them and loops
   5. Returns the final text response"
  [session-id user-message & {:keys [channel tool-context]
                              :or   {channel :terminal
                                     tool-context {}}}]
  (binding [prompt/*interaction-context* {:channel    channel
                                          :session-id session-id}]
    (try
      (db/add-message! session-id :user user-message)
      (report-status! "Updating working memory"
                      :phase :working-memory)
      (wm/update-wm! user-message session-id channel)
      (let [tools (tool/tool-definitions)]
        (loop [messages (context/build-messages session-id)
               round    0]
          (when (>= round max-tool-rounds)
            (throw (ex-info "Too many tool-calling rounds" {:rounds round})))
          (report-status! (if (zero? round)
                            "Calling model"
                            "Calling model with tool results")
                          :phase :llm
                          :round round)
          (let [response   (if (seq tools)
                             (llm/chat-with-tools messages tools)
                             (llm/chat-simple messages))
                has-tools? (and (map? response) (seq (get response "tool_calls")))]
            (if has-tools?
              (let [tool-calls    (get response "tool_calls")
                    assistant-msg {:role       "assistant"
                                   :content    (get response "content" "")
                                   :tool_calls tool-calls}
                    tool-count    (count tool-calls)
                    tool-results  (do
                                    (report-status! (str "Model requested "
                                                         tool-count
                                                         " tool"
                                                         (when (not= 1 tool-count) "s"))
                                                    :phase :tool-plan
                                                    :round round
                                                    :tool-count tool-count)
                                    (execute-tool-calls tool-calls
                                                        (merge {:session-id session-id
                                                                :channel    channel}
                                                               tool-context)))]
                ;; Store assistant message with tool calls
                (db/add-message! session-id :assistant
                                 (get response "content" "")
                                 :tool-calls (json/write-json-str tool-calls))
                ;; Store tool results
                (doseq [tr tool-results]
                  (db/add-message! session-id :tool
                                   (:content tr)
                                   :tool-id (:tool_call_id tr)))
                ;; Continue with updated messages
                (recur (-> messages
                           (conj assistant-msg)
                           (into tool-results))
                       (inc round)))
              ;; Final text response
              (let [text (if (string? response) response (get response "content" ""))]
                (report-status! "Preparing response"
                                :phase :finalizing)
                (db/add-message! session-id :assistant text)
                (prompt/status! {:state :done
                                 :phase :complete
                                 :message "Ready"})
                text)))))
      (catch Exception e
        (prompt/status! {:state :error
                         :phase :error
                         :message (str "Request failed: " (.getMessage e))})
        (throw e)))))
