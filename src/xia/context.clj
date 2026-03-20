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
            [xia.db :as db]
            [xia.identity :as identity]
            [xia.skill :as skill]
            [xia.llm :as llm]
            [xia.memory :as memory]
            [xia.working-memory :as wm]))

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

(defn estimate-tokens
  [s]
  (let [text (str s)]
    (if (str/blank? text)
      0
      (if-let [provider (db/current-embedding-provider)]
        (try
          (max 1 (long (emb/token-count provider text)))
          (catch Throwable _
            (heuristic-estimate-tokens text)))
        (heuristic-estimate-tokens text)))))

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

(defn- configured-system-prompt-budget []
  (if-let [custom (db/get-config :context/budget)]
    (try
      (let [parsed (edn/read-string custom)]
        (if (valid-budget parsed)
          (merge default-system-prompt-budget parsed)
          default-system-prompt-budget))
      (catch Exception _
        default-system-prompt-budget))
    default-system-prompt-budget))

(defn- configured-history-budget []
  (if-let [custom (db/get-config :context/history-budget)]
    (try
      (long (edn/read-string custom))
      (catch Exception _
        default-history-budget))
    default-history-budget))

(defn- configured-recent-history-message-limit []
  (if-let [custom (db/get-config :context/recent-history-message-limit)]
    (try
      (let [parsed (long (edn/read-string custom))]
        (max 4 parsed))
      (catch Exception _
        default-recent-history-message-limit))
    default-recent-history-message-limit))

(defn recent-history-message-limit-config
  []
  (configured-recent-history-message-limit))

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

(defn- render-entity-data [{:keys [name type facts edges properties]}]
  (let [type-str        (when type (str " (" (clojure.core/name type) ")"))
        selected-facts  (select-facts-for-entity facts)
        ^StringBuilder detail-builder (StringBuilder.)]
    (when (and properties (map? properties) (seq properties))
      (append-props-to-detail! detail-builder properties nil))
    (reduce (fn [^StringBuilder sb {:keys [content]}]
              (append-detail-fragment! sb content))
            detail-builder
            selected-facts)
    (reduce (fn [^StringBuilder sb {:keys [type target]}]
              (append-detail-fragment! sb
                                       (str (clojure.core/name type) "→" target)))
            detail-builder
            (take 3 (:outgoing edges)))
    (let [detail (when (pos? (.length detail-builder))
                   (.toString detail-builder))]
      {:content        (str "- " name type-str
                            (when-not (str/blank? detail) (str ": " detail)))
       :used-fact-eids (into [] (keep :eid) selected-facts)})))

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
           used-fact-eids []] ; "### Known\n"
      (if (empty? ents)
        {:content        (str/join "\n" lines)
         :used-fact-eids used-fact-eids}
        (let [{:keys [content]
               entity-used-fact-eids :used-fact-eids}
              (render-entity-data (first ents))
              line       content
              line-tokens (long (estimate-tokens line))]
          (if (> (+ tokens line-tokens) budget*)
            {:content        (str/join "\n" lines)
             :used-fact-eids used-fact-eids}
            (recur (rest ents)
                   (conj lines line)
                   (+ tokens line-tokens)
                   (into used-fact-eids entity-used-fact-eids)))))))))

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

(defn render-episodes
  "Render relevant episodes into compact format, within token budget."
  [episodes budget]
  (let [budget* (long budget)]
    (when (seq episodes)
    (loop [eps episodes
           lines ["### Recent"]
           tokens 10]
      (if (empty? eps)
        (str/join "\n" lines)
        (let [{:keys [summary timestamp]} (first eps)
              date-str (format-date timestamp)
              line (str "- [" (or date-str "?") "] " summary)
              line-tokens (long (estimate-tokens line))]
          (if (> (+ tokens line-tokens) budget*)
            (str/join "\n" lines)
            (recur (rest eps)
                   (conj lines line)
                   (+ tokens line-tokens)))))))))

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

(defn render-local-docs
  "Render relevant local documents into compact format, within token budget."
  [docs budget]
  (let [budget* (long budget)]
    (when (seq docs)
    (loop [remaining docs
           lines ["### Local Documents"]
           tokens 12]
      (if (empty? remaining)
        (str/join "\n" lines)
        (let [line (local-doc-line (first remaining))
              line-tokens (long (estimate-tokens line))]
          (if (> (+ tokens line-tokens) budget*)
            (str/join "\n" lines)
            (recur (rest remaining)
                   (conj lines line)
                   (+ tokens line-tokens)))))))))

(defn render-skills
  "Render skills into prompt format, within token budget."
  [skills budget]
  (let [budget* (long budget)]
    (when (seq skills)
    (loop [sks skills
           parts ["## Skills\nFollow these instructions when relevant.\n"]
           tokens 20]
      (if (empty? sks)
        (str/join "\n" parts)
        (let [s (first sks)
              section (str "### " (:skill/name s) "\n" (:skill/content s))
              section-tokens (long (estimate-tokens section))]
          (if (> (+ tokens section-tokens) budget*)
            (str/join "\n" parts) ; budget exceeded
            (recur (rest sks)
                   (conj parts section)
                   (+ tokens section-tokens)))))))))

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
        ent-tokens  (long (estimate-tokens (or ent-section "")))
        remaining   (- remaining ent-tokens)

        ;; P2: Local documents
        doc-budget  (long-min (budget-value budget :local-docs) (long-max 0 remaining))
        doc-section (render-local-docs (:local-docs wm-context) doc-budget)
        doc-tokens  (long (estimate-tokens (or doc-section "")))
        remaining   (- remaining doc-tokens)

        ;; P3: Episodes
        ep-budget  (long-min (budget-value budget :episodes) (long-max 0 remaining))
        ep-section (render-episodes (:episodes wm-context) ep-budget)
        ep-tokens  (long (estimate-tokens (or ep-section "")))
        remaining  (- remaining ep-tokens)

        ;; P4: Skills (lowest priority — cut first)
        skill-budget  (long-min (budget-value budget :skills) (long-max 0 remaining))
        skill-section (render-skills skills skill-budget)
        skill-tokens  (long (estimate-tokens (or skill-section "")))]
    {:prompt         (str id-section
                          (when topic-section (str "## Context\n" topic-section))
                          (when ent-section (str ent-section "\n\n"))
                          (when doc-section (str doc-section "\n\n"))
                          (when ep-section (str ep-section "\n\n"))
                          (when skill-section (str skill-section "\n\n")))
     :used-fact-eids (:used-fact-eids ent-data)})))

(defn assemble-system-prompt
  ([session-id]
   (:prompt (assemble-system-prompt-data session-id nil)))
  ([session-id opts]
   (:prompt (assemble-system-prompt-data session-id opts))))

;; ============================================================================
;; Message history compaction
;; ============================================================================

(defn compact-history
  "When message history exceeds budget, summarize older messages.
   Returns compacted message list. Raw messages remain in DB."
  ([messages budget]
   (compact-history messages budget nil))
  ([messages budget {:keys [provider-id workload]}]
   (let [messages*     (if (vector? messages) messages (vec messages))
         recap-prefix  (into [] (take-while recap-message?) messages*)
         live-history  (subvec messages* (count recap-prefix))
         total-tokens  (long (transduce (map #(estimate-tokens (:content %))) + 0 messages*))
         msg-count     (long (count live-history))
         budget*       (long budget)]
     (if (or (<= total-tokens budget*) (<= msg-count 4))
       messages* ; fits in budget or too few to compact
       ;; Summarize the older half
       (let [keep-count  (long-max 4 (quot msg-count 2))
             old-msgs    (subvec live-history 0 (- msg-count keep-count))
             recent-msgs (subvec live-history (- msg-count keep-count))
             old-text    (history-summary-text old-msgs)]
         (try
           (let [recap (summarize-history-text old-text {:provider-id provider-id
                                                         :workload workload})]
             (into recap-prefix
                   (into [(history-recap-message recap)] recent-msgs)))
           (catch Exception e
             (log/warn "Failed to compact history:" (.getMessage e))
             messages)))))))

(defn- recent-history-message-limit
  [opts]
  (let [limit (or (:recent-message-limit opts)
                  (configured-recent-history-message-limit))]
    (max 4 (long limit))))

  (defn- build-history-with-session-recap
  [session-id opts]
  (let [recent-limit (long (recent-history-message-limit opts))
        metadata     (db/session-message-metadata session-id)
        total        (long (count metadata))]
    (if (<= total recent-limit)
      (into [] (map history-message->llm-message)
            (db/session-messages-by-eids (into [] (map :eid) metadata)))
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
            new-recap-meta     (subvec metadata recap-count recent-start)
            new-tool-meta      (subvec metadata tool-count recent-start)
            recent-meta        (subvec metadata recent-start total)
            recent-eids        (into [] (map :eid) recent-meta)
            recent-messages    (db/session-messages-by-eids recent-eids)
            recent-history     (into [] (map history-message->llm-message) recent-messages)]
        (if (and (not (seq new-recap-meta))
                 (not (seq new-tool-meta)))
          (cond-> []
            (seq tool-recap-content) (conj (tool-recap-message tool-recap-content))
            (seq recap-content)      (conj (history-recap-message recap-content))
            true                     (into recent-history))
          (let [history-archived-messages (when (seq new-recap-meta)
                                            (db/session-messages-by-eids
                                              (into [] (map :eid) new-recap-meta)))
                tool-archived-messages    (when (seq new-tool-meta)
                                            (db/session-messages-by-eids
                                              (into [] (map :eid) new-tool-meta)))
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
                                   :workload (or (:compaction-workload opts)
                                                 :history-compaction)})
                                recap-content)]
                (when (seq new-tool-recap)
                  (db/save-session-tool-recap! session-id new-tool-recap recent-start))
                (if (seq new-recap)
                  (do
                    (db/save-session-history-recap! session-id new-recap recent-start)
                    (cond-> []
                      (seq new-tool-recap) (conj (tool-recap-message new-tool-recap))
                      true                 (conj (history-recap-message new-recap))
                      true                 (into recent-history)))
                  (into [] (map history-message->llm-message)
                        (db/session-messages session-id))))
              (catch Exception e
                (log/warn "Failed to update session history recap:" (.getMessage e))
                (when (seq new-tool-recap)
                  (db/save-session-tool-recap! session-id new-tool-recap recent-start))
                (into [] (map history-message->llm-message)
                      (db/session-messages session-id))))))))))

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
         compaction-provider-id (:compaction-provider-id opts)
         compaction-workload    (or (:compaction-workload opts)
                                    :history-compaction)
         {:keys [prompt used-fact-eids]}
         (assemble-system-prompt-data session-id (assoc opts :provider provider))
         history-msgs (build-history-with-session-recap
                       session-id
                       {:recent-message-limit (:recent-message-limit opts)
                        :compaction-provider-id compaction-provider-id
                        :compaction-workload compaction-workload})
         budget       (resolve-history-budget provider)
         compacted    (compact-history history-msgs
                                      budget
                                      (cond-> {}
                                        compaction-provider-id
                                        (assoc :provider-id compaction-provider-id)

                                        (and (not compaction-provider-id)
                                             compaction-workload)
                                        (assoc :workload compaction-workload)))]
     {:messages      (into [{:role "system" :content prompt}]
                           compacted)
      :used-fact-eids used-fact-eids})))

(defn build-messages
  ([session-id]
   (:messages (build-messages-data session-id nil)))
  ([session-id opts]
   (:messages (build-messages-data session-id opts))))
