(ns xia.context
  "Token-budgeted context assembly for the system prompt.

   Replaces the naive 'dump everything' approach with budget-aware rendering.
   Each section has a priority and budget; lower-priority sections are cut first
   when the total exceeds the budget.

   Budget allocation (configurable):
     P0 Identity   ~300  — never cut
     P0 Topic      ~100  — never cut
     P1 Entities   ~1500 — cut 3rd
     P2 Episodes   ~500  — cut 2nd
     P3 Skills     ~1500 — cut 1st

   Also handles message history compaction: when the conversation history
   exceeds a token budget, older messages are summarized into a recap."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [datalevin.embedding :as emb]
            [taoensso.timbre :as log]
            [charred.api :as json]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.identity :as identity]
            [xia.skill :as skill]
            [xia.llm :as llm]
            [xia.memory :as memory]
            [xia.working-memory :as wm])
  (:import [java.util LinkedHashMap Map]))

;; ============================================================================
;; Token estimation
;; ============================================================================

(def ^:private cjk-char-pattern
  #"(?:\p{IsHan}|\p{IsHiragana}|\p{IsKatakana}|\p{IsHangul})")

(def ^:private codeish-span-pattern
  #"[A-Za-z0-9_./:-]{12,}")

(def ^:private codeish-separator-pattern
  #"[_./:-]+")

(def ^:private camel-segment-pattern
  #"[A-Z]+(?=[A-Z][a-z]|\d|$)|[A-Z]?[a-z]+|\d+")

(defn- long-max
  ^long [^long a ^long b]
  (if (> a b) a b))

(defn- long-min
  ^long [^long a ^long b]
  (if (< a b) a b))

(defn- budget-value
  ^long [budget k]
  (long (or (get budget k) 0)))

(defn- ceil-div
  ^long [^long n ^long d]
  (quot (+ n (dec d)) d))

(defn- codeish-span?
  [span]
  (boolean
   (or (re-find #"[_./:-]" span)
       (re-find #"[a-z][A-Z]" span)
       (and (re-find #"[A-Za-z]" span)
            (re-find #"\d" span)))))

(defn- codeish-chunk-tokens
  [chunk]
  (let [camel-segments  (long (count (re-seq camel-segment-pattern chunk)))
        length-estimate (ceil-div (long (count chunk)) 8)]
    (long-max 1 (long-max camel-segments length-estimate))))

(defn- codeish-span-tokens
  [span]
  (->> (str/split span codeish-separator-pattern)
       (remove str/blank?)
       (map codeish-chunk-tokens)
       (reduce + 0)))

(defn- codeish-discount
  [text]
  (->> (re-seq codeish-span-pattern text)
       (filter codeish-span?)
       (reduce (fn [^long discount span]
                 (let [baseline (quot (long (count span)) 4)
                       adjusted (long-min baseline
                                          (long (codeish-span-tokens span)))]
                    (+ discount (- baseline adjusted))))
               0)))

(defn- heuristic-estimate-tokens
  "Fallback token estimate.

   Uses ~4 chars/token as the prose baseline, counts CJK characters more
   conservatively, and discounts long code/identifier spans that would
   otherwise look too expensive."
  [s]
  (let [text (str s)]
    (if (str/blank? text)
      0
      (let [baseline       (quot (long (count text)) 4)
            cjk-chars      (long (count (re-seq cjk-char-pattern text)))
            cjk-adjustment (- (ceil-div cjk-chars 2)
                              (quot cjk-chars 4))
            code-discount  (long (codeish-discount text))]
        (long-max 1 (- (+ baseline cjk-adjustment) code-discount))))))

(def ^:private token-estimate-cache-max-size 2048)
(def ^:private token-estimate-cache-max-text-chars 8192)

(defn- new-token-estimate-cache
  ^Map []
  (java.util.Collections/synchronizedMap
    (proxy [LinkedHashMap] [256 0.75 true]
      (removeEldestEntry [^java.util.Map$Entry _eldest]
        (> (.size ^LinkedHashMap this)
           (long token-estimate-cache-max-size))))))

(defonce ^:private token-estimate-cache
  (new-token-estimate-cache))

(defn- clear-token-estimate-cache!
  []
  (locking token-estimate-cache
    (.clear ^Map token-estimate-cache)))

(defn- token-estimate-source-key
  [provider]
  (if provider
    [:provider (.getName (class provider)) (System/identityHashCode provider)]
    [:heuristic]))

(defn- uncached-estimate-tokens
  ^long [provider text]
  (if provider
    (try
      (max 1 (long (emb/token-count provider text)))
      (catch Throwable _
        (heuristic-estimate-tokens text)))
    (heuristic-estimate-tokens text)))

(defn estimate-tokens
  [s]
  (let [text (str s)]
    (cond
      (str/blank? text)
      0

      :else
      (let [provider  (db/current-embedding-provider)
            text-size (long (count text))]
        (if (> text-size (long token-estimate-cache-max-text-chars))
          (uncached-estimate-tokens provider text)
          (let [cache-key [(token-estimate-source-key provider) text]]
            (if-some [cached (.get ^Map token-estimate-cache cache-key)]
              (long cached)
              (let [estimate (long (uncached-estimate-tokens provider text))]
                (locking token-estimate-cache
                  (if-some [cached (.get ^Map token-estimate-cache cache-key)]
                    (long cached)
                    (do
                      (.put ^Map token-estimate-cache cache-key
                            (Long/valueOf estimate))
                      estimate)))))))))))

(defn- section-budget-tokens
  ^long [text]
  ;; Section renderers only need a cheap local estimate for incremental budget
  ;; tracking; they do not need provider-aware token counting per line/item.
  (long (heuristic-estimate-tokens text)))

(def ^:private final-prompt-truncation-note
  "\n\n[Context truncated to fit budget.]")

(def ^:private final-prompt-drop-order
  [:skills :episodes :local-docs :entities :topic])

;; ============================================================================
;; Budget config
;; ============================================================================

(def ^:private default-system-prompt-budget
  {:total      4000
   :identity   300
   :topic      100
   :entities   1500
   :local-docs 400
   :episodes   500
   :skills     1500})

(def ^:private default-history-budget 8000)
(def ^:private default-recent-history-message-limit 24)

(defn- valid-budget
  [value]
  (and (map? value)
       (seq value)))

(defn- parse-system-prompt-budget
  [raw]
  (let [parsed (cond
                 (map? raw) raw
                 (string? raw) (edn/read-string raw)
                 :else nil)]
    (when (valid-budget parsed)
      (merge default-system-prompt-budget parsed))))

(defn- configured-system-prompt-budget []
  (cfg/custom-option :context/budget
                     default-system-prompt-budget
                     (fn [raw]
                       (try
                         (parse-system-prompt-budget raw)
                         (catch Exception _
                           nil)))))

(defn- configured-history-budget []
  (cfg/positive-long :context/history-budget
                     default-history-budget))

(defn- configured-recent-history-message-limit []
  (max 4
       (cfg/positive-long :context/recent-history-message-limit
                          default-recent-history-message-limit)))

(defn recent-history-message-limit-config
  []
  (configured-recent-history-message-limit))

(defn history-budget-config
  []
  (configured-history-budget))

(defn config-resolutions
  []
  {:system-prompt-budget
   (cfg/custom-option-resolution :context/budget
                                 default-system-prompt-budget
                                 (fn [raw]
                                   (try
                                     (parse-system-prompt-budget raw)
                                     (catch Exception _
                                       nil))))
   :recent-history-message-limit
   (cfg/positive-long-resolution :context/recent-history-message-limit
                                 default-recent-history-message-limit)
   :history-budget
   (cfg/positive-long-resolution :context/history-budget
                                 default-history-budget)})

(defn- scale-budget
  [budget total]
  (let [base-total (max 1 (long (:total budget)))
        ratio      (/ (double total) base-total)]
    (reduce-kv (fn [scaled k v]
                 (assoc scaled
                        k
                        (if (= k :total)
                          (long total)
                          (max 1 (long (Math/round (* ratio (double v))))))))
               {}
               budget)))

(defn- resolve-context-provider
  [{:keys [provider provider-id]}]
  (or provider
      (when provider-id
        (db/get-provider provider-id))
      (when-not provider-id
        (db/get-default-provider))))

(defn- provider-system-prompt-budget
  [provider]
  (or (:llm.provider/system-prompt-budget provider)
      (:system-prompt-budget provider)))

(defn- provider-history-budget
  [provider]
  (or (:llm.provider/history-budget provider)
      (:history-budget provider)))

(defn- resolve-system-prompt-budget
  [provider]
  (let [budget         (configured-system-prompt-budget)
        total-override (provider-system-prompt-budget provider)]
    (if (and (number? total-override)
             (pos? (long total-override)))
      (scale-budget budget total-override)
      budget)))

(defn- resolve-history-budget
  [provider]
  (let [override (provider-history-budget provider)]
    (if (and (number? override)
             (pos? (long override)))
      (long override)
      (configured-history-budget))))

(defn- tool-result->content
  [tool-result content]
  (cond
    (string? tool-result) tool-result
    (some? tool-result)   (json/write-json-str tool-result)
    :else                 content))

(def ^:private archived-tool-recap-max-lines 24)
(def ^:private archived-tool-recap-max-chars 3200)
(def ^:private archived-tool-args-max-chars 180)
(def ^:private archived-tool-result-max-chars 260)

(defn- truncate-history-text
  [value max-chars]
  (let [text (some-> value str str/trim)
        max* (long max-chars)]
    (when (seq text)
      (if (<= (long (count text)) max*)
        text
        (str (subs text 0 (long-max 1 (- max* 3))) "...")))))

(defn- history-role-name
  [role]
  (cond
    (keyword? role) (name role)
    (symbol? role)  (name role)
    (string? role)  role
    (some? role)    (str role)
    :else           "unknown"))

(defn- tool-call-id-value
  [call]
  (let [call-id (some-> (or (:id call)
                            (get call "id"))
                        str
                        str/trim)]
    (when (seq call-id)
      call-id)))

(defn- message-tool-call-id
  [message]
  (let [call-id (some-> (or (:tool-id message)
                            (:tool_call_id message)
                            (get message "tool_call_id"))
                        str
                        str/trim)]
    (when (seq call-id)
      call-id)))

(defn- remove-pending-tool-call-id
  [pending tool-call-id]
  (let [[prefix suffix] (split-with #(not= % tool-call-id) pending)]
    (if (seq suffix)
      (into [] (concat prefix (next suffix)))
      pending)))

(defn- restore-history-tool-call-ids
  [messages]
  (loop [remaining messages
         pending   []
         acc       []]
    (if-let [{:keys [tool-calls] :as message} (first remaining)]
      (let [role*      (history-role-name (:role message))
            tool-calls* (or tool-calls
                            (:tool_calls message)
                            (get message "tool_calls"))]
        (cond
          (and (= role* "assistant") (seq tool-calls*))
          (recur (next remaining)
                 (into [] (keep tool-call-id-value) tool-calls*)
                 (conj acc message))

          (= role* "tool")
          (let [existing-id (message-tool-call-id message)
                [tool-call-id pending*]
                (cond
                  existing-id [existing-id (remove-pending-tool-call-id pending existing-id)]
                  (seq pending) [(first pending) (into [] (rest pending))]
                  :else         [nil pending])]
            (recur (next remaining)
                   pending*
                   (conj acc (cond-> message
                               tool-call-id (assoc :tool-id tool-call-id)))))

          :else
          (recur (next remaining) [] (conj acc message))))
      (vec acc))))

(defn- history-summary-fragment
  [value]
  (when (some? value)
    (let [text (try
                 (if (string? value)
                   value
                   (json/write-json-str value))
                 (catch Exception _
                   (str value)))
          compact (some-> text str str/trim (str/replace #"\s+" " "))]
      (when (seq compact)
        compact))))

(defn- render-tool-call-summary
  [call]
  (let [call-id  (history-summary-fragment (or (:id call) (get call "id")))
        function (or (:function call) (get call "function"))
        fn-name  (or (history-summary-fragment (or (:name function) (get function "name")))
                     "unknown-tool")
        args     (history-summary-fragment (or (:arguments function)
                                               (get function "arguments")))]
    (str "assistant requested tool"
         (when call-id
           (str "[" call-id "]"))
         ": "
         fn-name
         (when args
           (str " args=" args)))))

(defn- history-message->summary-lines
  [{:keys [content] :as message}]
  (let [role         (history-role-name (:role message))
        content-text (history-summary-fragment content)
        tool-calls   (or (:tool_calls message)
                         (get message "tool_calls"))
        tool-call-id (history-summary-fragment (or (:tool_call_id message)
                                                   (get message "tool_call_id")))]
    (cond-> []
      (= role "tool")
      (conj (str "tool result"
                 (when tool-call-id
                   (str "[" tool-call-id "]"))
                 ": "
                 (or content-text "[no content]")))

      (and (not= role "tool") content-text)
      (conj (str role ": " content-text))

      (seq tool-calls)
      (into (map render-tool-call-summary tool-calls)))))

(defn- history-summary-text
  [messages]
  (transduce
    (mapcat history-message->summary-lines)
    (completing
      (fn [^StringBuilder sb line]
        (when (pos? (.length sb))
          (.append sb "\n"))
        (.append sb ^String line))
      (fn [^StringBuilder sb]
        (.toString sb)))
    (StringBuilder.)
    messages))

(defn- history-message->llm-message
  [{:keys [role content tool-calls tool-id tool-result]}]
  (cond-> {:role (name role)
           :content (if (= role :tool)
                      (tool-result->content tool-result content)
                      content)}
    tool-calls (assoc :tool_calls tool-calls)
    tool-id    (assoc :tool_call_id tool-id)))

(defn- history-recap-message
  [recap]
  {:role "system"
   :content (str "[Conversation recap: " (str/trim recap) "]")})

(defn- tool-recap-message
  [recap]
  {:role "system"
   :content (str "[Archived tool execution recap:\n"
                 (str/trim recap)
                 "\nUse this to avoid repeating tool calls unless the task has changed or fresher data is required.]")})

(defn- recap-message?
  [{:keys [role content]}]
  (and (= "system" (history-role-name role))
       (string? content)
       (or (str/starts-with? content "[Conversation recap:")
           (str/starts-with? content "[Archived tool execution recap:"))))

(defn- tool-call-info
  [call]
  {:id   (history-summary-fragment (or (:id call) (get call "id")))
   :name (or (history-summary-fragment (or (:name (:function call))
                                           (get-in call ["function" "name"])))
             "unknown-tool")
   :args (truncate-history-text
           (history-summary-fragment (or (:arguments (:function call))
                                         (get-in call ["function" "arguments"])))
           archived-tool-args-max-chars)})

(defn- tool-execution-line
  [{:keys [id name args]} result-text]
  (str "- "
       name
       (when id
         (str "[" id "]"))
       (when args
         (str " args=" args))
       " => "
       (or (truncate-history-text result-text archived-tool-result-max-chars)
           "[no result captured]")))

(defn- tool-recap-lines
  [recap]
  (into []
        (comp (map str/trim)
              (remove str/blank?))
        (str/split-lines (or recap ""))))

(defn- trim-tool-recap-lines
  [lines]
  (let [max-lines (long archived-tool-recap-max-lines)
        max-chars (long archived-tool-recap-max-chars)
        lines* (if (> (long (count lines)) max-lines)
                 (into [] (take-last archived-tool-recap-max-lines) lines)
                 (vec lines))]
    (loop [acc []
           remaining (reverse lines*)
           total 0]
      (if-let [line (first remaining)]
        (let [line-cost (+ (long (count line)) (if (pos? total) 1 0))]
          (if (> (+ total line-cost) max-chars)
            (if (seq acc) acc (vector (truncate-history-text line max-chars)))
            (recur (conj acc line) (next remaining) (+ total line-cost))))
        (into [] (reverse acc))))))

(defn- summarize-archived-tool-usage
  [messages]
  (let [lines
        (loop [remaining messages
               calls-by-id {}
               result-lines []]
          (if-let [{:keys [role tool-calls tool-id] :as message} (first remaining)]
            (let [role* (history-role-name role)]
              (cond
                (and (= role* "assistant") (seq tool-calls))
                (recur (next remaining)
                       (reduce (fn [m call]
                                 (if-let [call-id (:id (tool-call-info call))]
                                   (assoc m call-id (tool-call-info call))
                                   m))
                               calls-by-id
                               tool-calls)
                       result-lines)

                (= role* "tool")
                (let [result-text (history-summary-fragment
                                    (tool-result->content (:tool-result message)
                                                          (:content message)))
                      line        (tool-execution-line
                                    (get calls-by-id tool-id
                                         {:id tool-id
                                          :name "unknown-tool"
                                          :args nil})
                                    result-text)]
                  (recur (next remaining) calls-by-id (conj result-lines line)))

                :else
                (recur (next remaining) calls-by-id result-lines)))
            result-lines))]
    (when (seq lines)
      (str/join "\n" (trim-tool-recap-lines lines)))))

(defn- merge-tool-recap
  [existing-recap archived-messages]
  (let [delta-lines (tool-recap-lines (summarize-archived-tool-usage archived-messages))]
    (when-let [merged (seq (trim-tool-recap-lines
                             (into (tool-recap-lines existing-recap) delta-lines)))]
      (str/join "\n" merged))))

(defn- summarize-history-text
  ([history-text opts]
   (summarize-history-text history-text nil opts))
  ([history-text existing-recap {:keys [provider-id workload]}]
   (let [history-text* (some-> history-text str str/trim)
         existing-recap* (some-> existing-recap str str/trim)]
     (when (seq history-text*)
       (let [summary-messages (if (seq existing-recap*)
                                [{:role "system"
                                  :content (str "Update the existing conversation recap to include the newly archived messages. "
                                                "Keep the recap to 2-4 sentences. Capture key topics, decisions, any personal information shared, "
                                                "and important tool usage, including which tools were called and the important results or errors "
                                                "they returned. Be factual and concise.")}
                                 {:role "user"
                                  :content (str "Existing recap:\n"
                                                existing-recap*
                                                "\n\nNewly archived messages:\n"
                                                history-text*)}]
                                [{:role "system"
                                  :content (str "Summarize this conversation excerpt in 2-4 sentences. "
                                                "Capture key topics, decisions, any personal information shared, "
                                                "and important tool usage, including which tools were called and "
                                                "the important results or errors they returned. Be factual.")}
                                 {:role "user" :content history-text*}])]
         (str/trim
          (cond
            provider-id
            (llm/chat-simple summary-messages :provider-id provider-id)

            workload
            (llm/chat-simple summary-messages :workload workload)

            :else
            (llm/chat-simple summary-messages))))))))

;; ============================================================================
;; Renderers
;; ============================================================================

(defn- render-identity []
  (identity/system-prompt))

(defn- render-topic [wm-context]
  (when-let [topics (:topics wm-context)]
    (str "Topic: " topics "\n\n")))

(defn- append-detail-fragment!
  [^StringBuilder sb fragment]
  (when (seq fragment)
    (when (pos? (.length sb))
      (.append sb "; "))
    (.append sb ^String fragment))
  sb)

(defn- append-props-to-detail!
  [^StringBuilder sb m prefix]
  (reduce-kv
    (fn [^StringBuilder sb k v]
      (let [path (if prefix
                   (str prefix "." (clojure.core/name k))
                   (clojure.core/name k))]
        (if (map? v)
          (append-props-to-detail! sb v path)
          (append-detail-fragment! sb (str path ": " v)))))
    sb
    m))

(defn- flatten-props-into
  [acc m prefix]
  (reduce-kv
    (fn [acc k v]
      (let [path (if prefix
                   (str prefix "." (clojure.core/name k))
                   (clojure.core/name k))]
        (if (map? v)
          (flatten-props-into acc v path)
          (conj! acc (str path ": " v)))))
    acc
    m))

(defn- flatten-props
  "Flatten a nested property map into key=value pairs for compact display.
   {:location \"Seattle\" :work {:title \"Engineer\"}} → [\"location: Seattle\" \"work.title: Engineer\"]"
  ([m] (flatten-props m nil))
  ([m prefix]
   (if (and (map? m) (seq m))
     (persistent! (flatten-props-into (transient []) m prefix))
     [])))

(defn- select-facts-for-entity
  [facts]
  (->> facts
       (filter #(>= (double (memory/normalize-fact-confidence (:confidence %))) 0.1))
       (sort-by memory/fact-prompt-score >)
       (take 5)
       vec))

(defn- fact-ref-label
  [idx]
  (str "F" (long idx)))

(defn- render-entity-data
  ([entity]
   (render-entity-data entity nil))
  ([{:keys [name type facts edges properties]} fact-ref-start]
   (let [type-str        (when type (str " (" (clojure.core/name type) ")"))
         selected-facts  (select-facts-for-entity facts)
         ^StringBuilder detail-builder (StringBuilder.)]
     (when (and properties (map? properties) (seq properties))
       (append-props-to-detail! detail-builder properties nil))
     (let [[detail-builder used-fact-refs next-fact-ref]
           (reduce (fn [[^StringBuilder sb refs next-ref] {:keys [eid content]}]
                     (let [ref-label  (when next-ref (fact-ref-label next-ref))
                           content*   (if ref-label
                                        (str "[" ref-label "] " content)
                                        content)
                           refs*      (cond-> refs
                                        (and ref-label eid)
                                        (conj {:eid eid :ref ref-label}))
                           next-ref*  (when next-ref (inc next-ref))]
                       (append-detail-fragment! sb content*)
                       [sb refs* next-ref*]))
                   [detail-builder [] fact-ref-start]
                   selected-facts)]
       (reduce (fn [^StringBuilder sb {:keys [type target]}]
                 (append-detail-fragment! sb
                                          (str (clojure.core/name type) "→" target)))
               detail-builder
               (take 3 (:outgoing edges)))
       (let [detail (when (pos? (.length detail-builder))
                      (.toString detail-builder))]
         {:content        (str "- " name type-str
                               (when-not (str/blank? detail) (str ": " detail)))
          :used-fact-eids (into [] (keep :eid) selected-facts)
          :used-fact-refs used-fact-refs
          :next-fact-ref  next-fact-ref})))))

(defn- render-entity
  [entity]
  (:content (render-entity-data entity)))

(defn- render-entities-data
  "Render active entities + facts into compact format, within token budget."
  [entities budget]
  (let [budget* (long budget)]
    (when (seq entities)
      (loop [ents (sort-by (fn [entity]
                             (double (or (:relevance entity) 0.0)))
                           >
                           entities)
             lines ["### Known"]
             tokens 8
             used-fact-eids []
             used-fact-refs []
             next-fact-ref 1] ; "### Known\n"
        (if (empty? ents)
          {:content        (str/join "\n" lines)
           :tokens         tokens
           :used-fact-eids used-fact-eids
           :used-fact-refs used-fact-refs}
          (let [{:keys [content next-fact-ref]
                 entity-used-fact-eids :used-fact-eids
                 entity-used-fact-refs :used-fact-refs}
                (render-entity-data (first ents) next-fact-ref)
                line        content
                line-tokens (section-budget-tokens line)]
            (if (> (+ tokens line-tokens) budget*)
              {:content        (str/join "\n" lines)
               :tokens         tokens
               :used-fact-eids used-fact-eids
               :used-fact-refs used-fact-refs}
              (recur (rest ents)
                     (conj lines line)
                     (+ tokens line-tokens)
                     (into used-fact-eids entity-used-fact-eids)
                     (into used-fact-refs entity-used-fact-refs)
                     next-fact-ref))))))))

(defn render-entities
  [entities budget]
  (:content (render-entities-data entities budget)))

(defn- format-date [^java.util.Date d]
  (when d
    (let [cal (doto (java.util.Calendar/getInstance) (.setTime d))]
      (format "%s %d"
              (get ["Jan" "Feb" "Mar" "Apr" "May" "Jun"
                    "Jul" "Aug" "Sep" "Oct" "Nov" "Dec"]
                   (.get cal java.util.Calendar/MONTH))
              (.get cal java.util.Calendar/DAY_OF_MONTH)))))

(defn- render-episodes-data
  "Render relevant episodes into compact format, within token budget."
  [episodes budget]
  (let [budget* (long budget)]
    (when (seq episodes)
      (loop [eps episodes
             lines ["### Recent"]
             tokens 10]
        (if (empty? eps)
          {:content (str/join "\n" lines)
           :tokens  tokens}
          (let [{:keys [summary timestamp]} (first eps)
                date-str (format-date timestamp)
                line (str "- [" (or date-str "?") "] " summary)
                line-tokens (section-budget-tokens line)]
            (if (> (+ tokens line-tokens) budget*)
              {:content (str/join "\n" lines)
               :tokens  tokens}
              (recur (rest eps)
                     (conj lines line)
                     (+ tokens line-tokens)))))))))

(defn render-episodes
  [episodes budget]
  (:content (render-episodes-data episodes budget)))

(defn- local-doc-line
  [{:keys [name media-type summary preview matched-chunks]}]
  (let [summary* (some-> (or summary preview)
                         str
                         str/trim
                         (str/replace #"\s+" " ")
                         not-empty)
        summary* (when summary*
                   (if (> (count summary*) 220)
                     (str (subs summary* 0 219) "…")
                     summary*))
        chunk*   (some->> matched-chunks
                          (into [] (comp (map :summary)
                                         (remove str/blank?)))
                          (str/join " | ")
                          not-empty)
        chunk*   (when chunk*
                   (if (> (count chunk*) 180)
                     (str (subs chunk* 0 179) "…")
                     chunk*))]
    (str "- " (or name "Untitled document")
         (when media-type
           (str " (" media-type ")"))
         (when summary*
           (str ": " summary*))
         (when chunk*
           (str " [matches: " chunk* "]")))))

(defn- render-local-docs-data
  "Render relevant local documents into compact format, within token budget."
  [docs budget]
  (let [budget* (long budget)]
    (when (seq docs)
      (loop [remaining docs
             lines ["### Local Documents"]
             tokens 12]
        (if (empty? remaining)
          {:content (str/join "\n" lines)
           :tokens  tokens}
          (let [line (local-doc-line (first remaining))
                line-tokens (section-budget-tokens line)]
            (if (> (+ tokens line-tokens) budget*)
              {:content (str/join "\n" lines)
               :tokens  tokens}
              (recur (rest remaining)
                     (conj lines line)
                     (+ tokens line-tokens)))))))))

(defn render-local-docs
  [docs budget]
  (:content (render-local-docs-data docs budget)))

(defn- render-skills-data
  "Render skills into prompt format, within token budget."
  [skills budget]
  (let [budget* (long budget)]
    (when (seq skills)
      (loop [sks (sort-by (fn [skill]
                            [(- (double (or (:skill/relevance skill) 0.0)))
                             (str/lower-case (or (:skill/name skill) ""))])
                          skills)
             parts ["## Skills\nFollow these instructions when relevant.\n"]
             tokens 20]
        (if (empty? sks)
          {:content (str/join "\n" parts)
           :tokens  tokens}
          (let [s (first sks)
                section (str "### " (:skill/name s) "\n" (:skill/content s))
                section-tokens (section-budget-tokens section)]
            (if (> (+ tokens section-tokens) budget*)
              {:content (str/join "\n" parts)
               :tokens  tokens}
              (recur (rest sks)
                     (conj parts section)
                     (+ tokens section-tokens)))))))))

(defn render-skills
  [skills budget]
  (:content (render-skills-data skills budget)))

(defn- system-prompt-sections->prompt
  [sections]
  (apply str
         (keep (fn [{:keys [key content]}]
                 (when (seq content)
                   (case key
                     :identity content
                     :topic    (str "## Context\n" content)
                     (str content "\n\n"))))
               sections)))

(defn- drop-system-prompt-section
  [sections drop-key]
  (mapv (fn [section]
          (if (= drop-key (:key section))
            (assoc section :content nil)
            section))
        sections))

(defn- section-present?
  [sections k]
  (boolean
   (some (fn [{:keys [key content]}]
           (and (= key k) (seq content)))
         sections)))

(defn- truncate-text-to-budget
  [text budget]
  (let [text*         (str text)
        total-budget  (long-max 0 (long budget))
        note*         final-prompt-truncation-note
        note-tokens   (long (estimate-tokens note*))
        suffix        (if (< note-tokens total-budget) note* "")
        suffix-tokens (long (estimate-tokens suffix))]
    (cond
      (<= (long (estimate-tokens text*)) total-budget)
      text*

      (zero? (count text*))
      text*

      :else
      (loop [lo 0
             hi (count text*)
             best ""]
        (if (> lo hi)
          (if (seq best)
            best
            (let [candidate (if (seq suffix) suffix (subs text* 0 1))]
              (if (<= (long (estimate-tokens candidate)) total-budget)
                candidate
                "")))
          (let [mid       (quot (+ lo hi) 2)
                prefix    (subs text* 0 mid)
                candidate (if (seq suffix)
                            (str prefix suffix)
                            prefix)
                tokens    (long (estimate-tokens candidate))]
            (if (<= tokens total-budget)
              (recur (inc mid) hi candidate)
              (recur lo (dec mid) best))))))))

(defn- recover-system-prompt
  [sections total-budget]
  (loop [sections* sections
         drop-keys final-prompt-drop-order]
    (let [prompt  (system-prompt-sections->prompt sections*)
          tokens  (long (estimate-tokens prompt))]
      (cond
        (<= tokens (long total-budget))
        {:sections sections*
         :prompt   prompt}

        (seq drop-keys)
        (recur (drop-system-prompt-section sections* (first drop-keys))
               (rest drop-keys))

        :else
        {:sections sections*
         :prompt   (truncate-text-to-budget prompt total-budget)}))))

;; ============================================================================
;; System prompt assembly
;; ============================================================================

(defn assemble-system-prompt-data
  "Build the complete system prompt with budget enforcement and return prompt metadata.
   Priority: identity (P0) > topic (P0) > entities (P1) > local docs (P2)
   > episodes (P3) > skills (P4)."
  ([session-id]
   (assemble-system-prompt-data session-id nil))
  ([session-id opts]
   (let [budget     (resolve-system-prompt-budget (resolve-context-provider opts))
         wm-context (or (wm/wm->context session-id)
                       {:topics nil :entities [] :local-docs [] :episodes [] :turn-count 0})
         skills     (skill/skills-for-context wm-context)

        ;; P0: Identity (always included)
        id-section (render-identity)
        id-tokens  (long (estimate-tokens id-section))

        ;; P0: Topic (always included)
        topic-section (render-topic wm-context)
        topic-tokens  (long (estimate-tokens (or topic-section "")))

        ;; Remaining budget for P1-P3
        ;; Allocate to highest priority first so lower priorities get cut first
        remaining (- (budget-value budget :total) id-tokens topic-tokens)

        ;; P1: Entities (highest priority — cut last)
        ent-budget  (long-min (budget-value budget :entities) (long-max 0 remaining))
        ent-data    (render-entities-data (:entities wm-context) ent-budget)
        ent-section (:content ent-data)
        ent-tokens  (long (or (:tokens ent-data) 0))
        remaining   (- remaining ent-tokens)

        ;; P2: Local documents
        doc-budget  (long-min (budget-value budget :local-docs) (long-max 0 remaining))
        doc-data    (render-local-docs-data (:local-docs wm-context) doc-budget)
        doc-section (:content doc-data)
        doc-tokens  (long (or (:tokens doc-data) 0))
        remaining   (- remaining doc-tokens)

        ;; P3: Episodes
        ep-budget  (long-min (budget-value budget :episodes) (long-max 0 remaining))
        ep-data    (render-episodes-data (:episodes wm-context) ep-budget)
        ep-section (:content ep-data)
        ep-tokens  (long (or (:tokens ep-data) 0))
        remaining  (- remaining ep-tokens)

        ;; P4: Skills (lowest priority — cut first)
        skill-budget  (long-min (budget-value budget :skills) (long-max 0 remaining))
        skill-data    (render-skills-data skills skill-budget)
        skill-section (:content skill-data)
        skill-tokens  (long (or (:tokens skill-data) 0))
        sections      [{:key :identity :content id-section}
                       {:key :topic :content topic-section}
                       {:key :entities :content ent-section}
                       {:key :local-docs :content doc-section}
                       {:key :episodes :content ep-section}
                       {:key :skills :content skill-section}]
        {:keys [sections prompt]} (recover-system-prompt sections (budget-value budget :total))
        entities-retained? (section-present? sections :entities)]
    {:prompt         prompt
     :used-fact-eids (if entities-retained?
                       (:used-fact-eids ent-data)
                       [])
     :used-fact-refs (if entities-retained?
                       (:used-fact-refs ent-data)
                       [])})))

(defn assemble-system-prompt
  ([session-id]
   (:prompt (assemble-system-prompt-data session-id nil)))
  ([session-id opts]
   (:prompt (assemble-system-prompt-data session-id opts))))

(defn- system-prompt-cache-key
  [session-id provider]
  {:session-id session-id
   :budget (resolve-system-prompt-budget provider)
   :wm-version (wm/prompt-cache-version session-id)})

;; ============================================================================
;; Message history compaction
;; ============================================================================

(defn compact-history
  "When message history exceeds budget, summarize older messages.
   Returns compacted message list. Raw messages remain in DB."
  ([messages budget]
   (compact-history messages budget nil))
  ([messages budget {:keys [provider-id workload allow-summary?]
                     :or {allow-summary? true}}]
   (let [messages*    (if (vector? messages) messages (vec messages))
         recap-prefix (into [] (take-while recap-message?) messages*)
         live-history (subvec messages* (count recap-prefix))
         total-tokens (long (transduce (map #(estimate-tokens (:content %))) + 0 messages*))
         msg-count    (long (count live-history))
         budget*      (long budget)]
     (if (or (<= total-tokens budget*) (<= msg-count 4))
       messages*
       (let [keep-count  (long-max 4 (quot msg-count 2))
             old-msgs    (subvec live-history 0 (- msg-count keep-count))
             recent-msgs (subvec live-history (- msg-count keep-count))
             old-text    (history-summary-text old-msgs)]
         (if allow-summary?
           (try
             (let [recap (summarize-history-text old-text
                                                 {:provider-id provider-id
                                                  :workload workload})]
               (into recap-prefix
                     (into [(history-recap-message recap)] recent-msgs)))
             (catch Exception e
               (log/warn "Failed to compact history:" (.getMessage e))
               messages))
           (let [recap-tokens (long (transduce (map #(estimate-tokens (:content %))) + 0 recap-prefix))
                 live-budget  (long-max 0 (- budget* recap-tokens))
                 min-keep     (long-min 4 msg-count)
                 ;; Walk newest->oldest so we can preserve the most recent live
                 ;; history under budget, but accumulate into a list so kept
                 ;; messages remain in chronological order without a final
                 ;; reverse pass.
                 kept         (:kept
                               (reduce (fn [{:keys [tokens kept-count kept] :as acc} msg]
                                         (let [msg-tokens (long (estimate-tokens (:content msg)))
                                               must-keep? (< kept-count min-keep)
                                               keep?      (or must-keep?
                                                              (<= (+ tokens msg-tokens) live-budget))]
                                           (if keep?
                                             {:tokens     (+ tokens msg-tokens)
                                              :kept-count (inc kept-count)
                                              :kept       (conj kept msg)}
                                             acc)))
                                       {:tokens 0
                                        :kept-count 0
                                        :kept '()}
                                       (rseq live-history)))]
             (into recap-prefix kept))))))))

(defn- recent-history-message-limit
  [opts]
  (let [limit (or (:recent-message-limit opts)
                  (configured-recent-history-message-limit))]
    (max 4 (long limit))))

(defn- build-history-with-session-recap
  [session-id opts]
  (let [recent-limit (long (recent-history-message-limit opts))
        total        (db/session-message-count session-id)]
    (if (<= total recent-limit)
      {:messages (into [] (map history-message->llm-message)
                       (restore-history-tool-call-ids
                         (db/session-messages-by-eids
                          (into []
                                (map :eid)
                                (db/session-message-metadata-range session-id 0 total total)))))
       :history-recap-updated? false}
      (let [recent-start       (long-max 0 (- total recent-limit))
            recap-state        (db/session-history-recap session-id)
            recap-count0       (long (or (:message-count recap-state) 0))
            recap-count        (if (<= recap-count0 recent-start) recap-count0 0)
            recap-content      (when (= recap-count recap-count0)
                                 (:content recap-state))
            tool-recap-state   (db/session-tool-recap session-id)
            tool-count0        (long (or (:message-count tool-recap-state) 0))
            tool-count         (if (<= tool-count0 recent-start) tool-count0 0)
            tool-recap-content (when (= tool-count tool-count0)
                                 (:content tool-recap-state))
            new-recap-meta     (db/session-message-metadata-range session-id
                                                                  recap-count
                                                                  recent-start
                                                                  total)
            new-tool-meta      (db/session-message-metadata-range session-id
                                                                  tool-count
                                                                  recent-start
                                                                  total)
            recent-meta        (db/session-message-metadata-range session-id
                                                                  recent-start
                                                                  total
                                                                  total)
            recent-eids        (into [] (map :eid) recent-meta)
            recent-messages    (restore-history-tool-call-ids
                                 (db/session-messages-by-eids recent-eids))
            recent-history     (into [] (map history-message->llm-message) recent-messages)]
        (if (and (not (seq new-recap-meta))
                 (not (seq new-tool-meta)))
          {:messages (cond-> []
                       (seq tool-recap-content) (conj (tool-recap-message tool-recap-content))
                       (seq recap-content)      (conj (history-recap-message recap-content))
                       true                     (into recent-history))
           :history-recap-updated? false}
          (let [history-archived-messages (when (seq new-recap-meta)
                                            (restore-history-tool-call-ids
                                              (db/session-messages-by-eids
                                                (into [] (map :eid) new-recap-meta))))
                tool-archived-messages    (when (seq new-tool-meta)
                                            (restore-history-tool-call-ids
                                              (db/session-messages-by-eids
                                                (into [] (map :eid) new-tool-meta))))
                new-tool-recap            (or (when (seq tool-archived-messages)
                                                (merge-tool-recap tool-recap-content
                                                                  tool-archived-messages))
                                              tool-recap-content)]
            (try
              (let [new-recap (if (seq history-archived-messages)
                                (summarize-history-text
                                  (history-summary-text history-archived-messages)
                                  recap-content
                                  {:provider-id (:compaction-provider-id opts)
                                    :workload    (or (:compaction-workload opts)
                                                     :history-compaction)})
                                recap-content)]
                (when (seq new-tool-recap)
                  (db/save-session-tool-recap! session-id new-tool-recap recent-start))
                (if (seq new-recap)
                  (do
                    (db/save-session-history-recap! session-id new-recap recent-start)
                    {:messages (cond-> []
                                 (seq new-tool-recap) (conj (tool-recap-message new-tool-recap))
                                 true                 (conj (history-recap-message new-recap))
                                 true                 (into recent-history))
                     :history-recap-updated? true})
                  {:messages (into [] (map history-message->llm-message)
                                   (restore-history-tool-call-ids
                                     (db/session-messages session-id)))
                   :history-recap-updated? false}))
              (catch Exception e
                (log/warn "Failed to update session history recap:" (.getMessage e))
                (when (seq new-tool-recap)
                  (db/save-session-tool-recap! session-id new-tool-recap recent-start))
                {:messages (into [] (map history-message->llm-message)
                                 (restore-history-tool-call-ids
                                   (db/session-messages session-id)))
                 :history-recap-updated? false}))))))))

;; ============================================================================
;; Build messages (moved from agent.clj)
;; ============================================================================

(defn build-messages-data
  "Build the full message list for an LLM call:
   system prompt (identity + WM context + skills) + compacted history."
  ([session-id]
   (build-messages-data session-id nil))
  ([session-id opts]
   (let [provider     (resolve-context-provider opts)
         cache-key    (system-prompt-cache-key session-id provider)
         cache-entry  (:system-prompt-cache-entry opts)
         compaction-provider-id (:compaction-provider-id opts)
         compaction-workload    (or (:compaction-workload opts)
                                    :history-compaction)
         {:keys [prompt used-fact-eids used-fact-refs] :as system-prompt-data}
         (if (= cache-key (:key cache-entry))
           (:data cache-entry)
           (assemble-system-prompt-data session-id (assoc opts :provider provider)))
         {:keys [messages history-recap-updated?]}
         (build-history-with-session-recap
          session-id
          {:recent-message-limit (:recent-message-limit opts)
           :compaction-provider-id compaction-provider-id
           :compaction-workload compaction-workload})
         budget       (resolve-history-budget provider)
         compacted    (compact-history messages
                                      budget
                                      (cond-> {}
                                        true
                                        (assoc :allow-summary? (not history-recap-updated?))

                                        compaction-provider-id
                                        (assoc :provider-id compaction-provider-id)

                                        (and (not compaction-provider-id)
                                             compaction-workload)
                                        (assoc :workload compaction-workload)))]
     {:messages      (into [{:role "system" :content prompt}]
                           compacted)
      :used-fact-eids used-fact-eids
      :used-fact-refs used-fact-refs
      :system-prompt-cache-entry {:key cache-key
                                  :data system-prompt-data}})))

(defn build-messages
  ([session-id]
   (:messages (build-messages-data session-id nil)))
  ([session-id opts]
   (:messages (build-messages-data session-id opts))))
