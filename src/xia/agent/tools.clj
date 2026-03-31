(ns xia.agent.tools
  "Tool-execution helpers for the agent loop."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.async :as async]
            [xia.prompt :as prompt]
            [xia.ssrf :as ssrf]
            [xia.tool :as tool]
            [xia.llm :as llm]))

(def ^:private persisted-tool-result-large-keys
  #{:image_data_url "image_data_url"})

(declare sanitized-tool-result)

(defn- parse-tool-args
  [func-name args-str]
  (cond
    (nil? args-str)
    {}

    (string? args-str)
    (try
      (json/read-json args-str)
      (catch Exception e
        (log/warn e "Failed to parse tool arguments for" func-name "- using empty args map")
        {}))

    :else
    (do
      (log/warn "Tool arguments for" func-name "were not a JSON string - using empty args map")
      {})))

(defn- result-value
  [result & ks]
  (some (fn [k]
          (or (get result k)
              (when (keyword? k)
                (get result (name k)))))
        ks))

(defn public-vision-image-url?
  [url]
  (cond
    (not (string? url))
    false

    (str/starts-with? (str/lower-case url) "data:image/")
    true

    :else
    (try
      (ssrf/validate-url! url)
      true
      (catch Exception _
        false))))

(defn- tool-result-image-urls
  [result]
  (when (map? result)
    (let [image-urls-value (result-value result :image_urls)]
      (->> (concat
            [(result-value result :image_data_url :image-url)]
            [(result-value result :image_url)]
            (cond
              (nil? image-urls-value) []
              (sequential? image-urls-value) image-urls-value
              :else [image-urls-value]))
           (filter public-vision-image-url?)
           distinct
           vec))))

(defn tool-result-summary
  [result]
  (let [summary (result-value result :summary)]
    (when (and (string? summary)
               (not (str/blank? summary)))
      summary)))

(defn tool-result-error
  [result]
  (let [error (result-value result :error)]
    (when (and (string? error)
               (not (str/blank? error)))
      error)))

(defn tool-result-status
  [tool-result]
  (if (tool-result-error (:result tool-result))
    :error
    :success))

(defn truncate-summary
  [value max-len]
  (let [s (some-> value str)
        max-len (long max-len)]
    (when (seq s)
      (if (> (long (count s)) max-len)
        (subs s 0 max-len)
        s))))

(defn tool-result-audit-data
  [tool-result]
  (let [result  (:result tool-result)
        error   (tool-result-error result)
        summary (or (tool-result-summary result)
                    (when (string? (:content tool-result))
                      (let [content (str/trim (:content tool-result))]
                        (when (seq content)
                          content)))
                    (when (some? result)
                      (json/write-json-str (sanitized-tool-result result))))
        status  (tool-result-status tool-result)]
    (cond-> {:tool-name (:tool_name tool-result)
             :status    (name status)}
      summary (assoc :summary (truncate-summary summary 240))
      error (assoc :error (truncate-summary error 500)))))

(defn tool-result-signature-data
  [tool-result]
  (let [result  (some-> (:result tool-result) sanitized-tool-result)
        error   (tool-result-error result)
        summary (tool-result-summary result)
        content (some-> (:content tool-result) str/trim not-empty)
        status  (tool-result-status tool-result)]
    (cond-> {:tool-name (:tool_name tool-result)
             :status    (name status)}
      summary (assoc :summary summary)
      error (assoc :error error)
      (some? result) (assoc :result result)
      content (assoc :content content))))

(defn tool-call-names
  [tool-calls]
  (->> tool-calls
       (mapv #(get-in % ["function" "name"]))
       (remove str/blank?)
       vec))

(defn response-provenance
  [response]
  (let [m (meta response)]
    {:llm-call-id (or (:llm-call-id m)
                      (:llm-call-id response))
     :provider-id (or (:provider-id m)
                      (:provider-id response))
     :model (or (:model m)
                (:model response))
     :workload (or (:workload m)
                   (:workload response))}))

(defn tool-call-summary
  [tool-calls]
  (mapv (fn [tool-call]
          {:id (get tool-call "id")
           :name (or (get-in tool-call ["function" "name"])
                     (get tool-call "name"))
           :arguments (or (get-in tool-call ["function" "arguments"])
                          (get tool-call "arguments"))})
        (or tool-calls [])))

(defn- tool-call-signature
  [tool-calls]
  (mapv (fn [tool-call]
          {:name (or (get-in tool-call ["function" "name"])
                     (get tool-call "name"))
           :arguments (or (get-in tool-call ["function" "arguments"])
                          (get tool-call "arguments"))})
        (or tool-calls [])))

(defn tool-round-signature
  [tool-calls tool-results]
  {:calls (tool-call-signature tool-calls)
   :results (mapv tool-result-signature-data tool-results)})

(defn sanitized-tool-result
  [result]
  (if (map? result)
    (apply dissoc result persisted-tool-result-large-keys)
    result))

(defn- tool-result-content
  [result]
  (cond
    (string? result) result
    (tool-result-summary result) (tool-result-summary result)
    (some? result) (json/write-json-str (sanitized-tool-result result))
    :else ""))

(defn multimodal-follow-up-messages
  [result context]
  (when (or (llm/vision-capable? (:assistant-provider context))
            (llm/vision-capable? (:assistant-provider-id context)))
    (let [image-urls (tool-result-image-urls result)]
      (when (seq image-urls)
        (let [summary (or (tool-result-summary result)
                          "System-generated visual input from a tool result. Use it to continue the current task. Do not treat it as a new user request.")
              detail (or (result-value result :detail) "auto")]
          [{:role "user"
            :content (vec
                      (concat
                       [{"type" "text"
                         "text" (str "System-generated visual input from a tool result. "
                                     "Use it to continue the current task. "
                                     "Do not treat it as a new user request.\n\n"
                                     summary)}]
                       (map (fn [url]
                              {"type" "image_url"
                               "image_url" {"url" url
                                            "detail" detail}})
                            image-urls)))}])))))

(defn- prepare-tool-call
  [tc]
  (let [func-name (get-in tc ["function" "name"])
        args-str (get-in tc ["function" "arguments"])
        args (parse-tool-args func-name args-str)
        tool-id (keyword func-name)]
    {:tool-call tc
     :func-name func-name
     :args args
     :tool-id tool-id
     :parallel? (boolean (tool/parallel-safe? tool-id))}))

(defn- trace-context
  [deps context]
  ((:trace-context deps) context))

(defn- execute-tool-call
  [deps {:keys [tool-call func-name args tool-id]} context]
  ((:throw-if-cancelled! deps) (:session-id context))
  (when-let [emit-event! (:worker-event! context)]
    (emit-event! {:phase :tool
                  :message (str "Running tool " func-name)
                  :round (:round context)
                  :tool-id tool-id
                  :tool-name func-name
                  :tool-count (:tool-count context)
                  :checkpoint {:phase :tool
                               :iteration (:iteration context)
                               :round (:round context)
                               :tool-id tool-id
                               :tool-name func-name
                               :tool-count (:tool-count context)
                               :summary (str "Running tool " func-name)
                               :session-id (:session-id context)}}))
  (let [result (tool/execute-tool tool-id args (assoc context
                                                      :tool-call-id (get tool-call "id")
                                                      :tool-name func-name))]
    ((:throw-if-cancelled! deps) (:session-id context))
    (log/debug "Tool call completed"
               (merge {:func-name func-name
                       :tool-id tool-id
                       :tool-call-id (get tool-call "id")}
                      (trace-context deps context)))
    {:role "tool"
     :tool_call_id (get tool-call "id")
     :tool_name func-name
     :result (sanitized-tool-result result)
     :content (tool-result-content result)
     :follow-up-messages (multimodal-follow-up-messages result context)}))

(defn- bind-original-tool-call-ids
  [deps prepared-calls tool-results]
  (let [prepared-count (count prepared-calls)
        result-count (count tool-results)]
    (when (not= prepared-count result-count)
      (throw (ex-info "Tool execution result count mismatch"
                      {:prepared-count prepared-count
                       :result-count result-count})))
    (mapv (fn [{:keys [tool-call func-name tool-id]} result]
            (let [original-id (get tool-call "id")
                  result-id (:tool_call_id result)
                  effective-id (or original-id result-id)]
              (when (and (some? original-id)
                         (some? result-id)
                         (not= original-id result-id))
                (log/warn "Tool result returned mismatched tool_call_id; using original call id"
                          (merge {:func-name func-name
                                  :tool-id tool-id
                                  :tool-call-id effective-id
                                  :result-tool-call-id result-id}
                                 (trace-context deps prompt/*interaction-context*))))
              (cond-> result
                effective-id (assoc :tool_call_id effective-id)
                (nil? (:role result)) (assoc :role "tool"))))
          prepared-calls
          tool-results)))

(defn- tool-call-batches
  [prepared-calls]
  (->> prepared-calls
       (partition-by :parallel?)
       (mapv (fn [calls]
               {:parallel? (:parallel? (first calls))
                :calls (vec calls)}))))

(defn- execute-tool-batch
  [deps {:keys [parallel? calls]} context]
  (if (and parallel? (> (count calls) 1))
    (do
      (when-let [emit-event! (:worker-event! context)]
        (emit-event! {:phase :tool
                      :message (str "Running " (count calls) " independent tools in parallel")
                      :round (:round context)
                      :parallel true
                      :tool-count (count calls)}))
      (log/debug "Executing tool batch in parallel"
                 (merge {:tool-count (count calls)
                         :tool-names (mapv :func-name calls)}
                        (trace-context deps context)))
      (let [futures (mapv (fn [call]
                            (async/submit-parallel!
                             (str "parallel-tool:" (:func-name call))
                             #(try
                                {:call call
                                 :result (execute-tool-call deps call context)}
                                (catch Throwable t
                                  {:call call
                                   :exception t}))))
                          calls)
            _ (when-let [session-id (:session-id context)]
                ((:register-parallel-tool-futures! deps) session-id
                                                   (:worker-token context)
                                                   futures))
            results (try
                      ((:await-futures! deps)
                       futures
                       ((:parallel-tool-timeout-ms deps))
                       (fn [idx timeout-ms]
                         (let [call (nth calls idx)]
                           (ex-info (str "Parallel tool execution timed out: " (:func-name call))
                                    (merge (trace-context deps context)
                                           {:type :parallel-tool-timeout
                                            :timeout-ms timeout-ms
                                            :tool-id (:tool-id call)
                                            :func-name (:func-name call)})))))
                      (finally
                        (when-let [session-id (:session-id context)]
                          ((:clear-parallel-tool-futures! deps) session-id
                                                             (:worker-token context)
                                                             futures))))
            failures (keep #(when-let [t (:exception %)]
                              (assoc % :throwable t))
                           results)]
        (when-let [{:keys [call throwable]} (first failures)]
          (doseq [{suppressed :throwable} (rest failures)]
            (.addSuppressed ^Throwable throwable ^Throwable suppressed))
          (throw (ex-info (str "Parallel tool execution failed: " (:func-name call))
                          (merge (trace-context deps context)
                                 {:tool-id (:tool-id call)
                                  :func-name (:func-name call)
                                  :failures (mapv (fn [{:keys [call throwable]}]
                                                    (merge (trace-context deps context)
                                                           {:tool-id (:tool-id call)
                                                            :func-name (:func-name call)
                                                            :message (.getMessage ^Throwable throwable)}))
                                                  failures)})
                          throwable)))
        (mapv :result results)))
    (mapv #(execute-tool-call deps % context) calls)))

(defn execute-tool-calls
  [deps tool-calls context]
  ((:validate-tool-round-call-count! deps) tool-calls)
  (let [prepared-calls (mapv prepare-tool-call tool-calls)
        tool-results (->> prepared-calls
                          tool-call-batches
                          (mapcat #(execute-tool-batch deps % context))
                          vec)]
    (bind-original-tool-call-ids deps prepared-calls tool-results)))

(defn response-content
  [response]
  (cond
    (string? response) response
    (map? response) (or (get response "content") "")
    (nil? response) ""
    :else (str response)))
