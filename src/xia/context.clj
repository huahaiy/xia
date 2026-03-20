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

(defn- ceil-div
  [n d]
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
  (let [camel-segments (count (re-seq camel-segment-pattern chunk))
        length-estimate (ceil-div (count chunk) 8)]
    (max 1 camel-segments length-estimate)))

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
       (reduce (fn [discount span]
                 (let [baseline (quot (count span) 4)
                       adjusted (min baseline (codeish-span-tokens span))]
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
      (let [baseline       (quot (count text) 4)
            cjk-chars      (count (re-seq cjk-char-pattern text))
            cjk-adjustment (- (ceil-div cjk-chars 2)
                              (quot cjk-chars 4))
            code-discount  (codeish-discount text)]
        (max 1 (- (+ baseline cjk-adjustment) code-discount))))))

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

(defn- flatten-props
  "Flatten a nested property map into key=value pairs for compact display.
   {:location \"Seattle\" :work {:title \"Engineer\"}} → [\"location: Seattle\" \"work.title: Engineer\"]"
  ([m] (flatten-props m nil))
  ([m prefix]
   (reduce-kv
     (fn [acc k v]
       (let [path (if prefix (str prefix "." (clojure.core/name k)) (clojure.core/name k))]
         (if (map? v)
           (into acc (flatten-props v path))
           (conj acc (str path ": " v)))))
     []
     m)))

(defn- select-facts-for-entity
  [facts]
  (->> facts
       (filter #(>= (memory/normalize-fact-confidence (:confidence %)) 0.1))
       (sort-by memory/fact-prompt-score >)
       (take 5)
       vec))

(defn- render-entity-data [{:keys [name type facts edges properties]}]
  (let [type-str (when type (str " (" (clojure.core/name type) ")"))
        ;; Properties as compact key: value pairs
        prop-strs (when (and properties (map? properties) (seq properties))
                    (flatten-props properties))
        selected-facts (select-facts-for-entity facts)
        fact-strs (map :content selected-facts)
        ;; Outgoing edges
        edge-strs (->> (:outgoing edges)
                       (take 3)
                       (map (fn [{:keys [type target]}]
                              (str (clojure.core/name type) "→" target))))
        detail (str/join "; " (concat prop-strs fact-strs edge-strs))]
    {:content       (str "- " name type-str
                         (when-not (str/blank? detail) (str ": " detail)))
     :used-fact-eids (->> selected-facts
                          (keep :eid)
                          vec)}))

(defn- render-entity
  [entity]
  (:content (render-entity-data entity)))

(defn- render-entities-data
  "Render active entities + facts into compact format, within token budget."
  [entities budget]
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
              line-tokens (estimate-tokens line)]
          (if (> (+ tokens line-tokens) budget)
            {:content        (str/join "\n" lines)
             :used-fact-eids used-fact-eids}
            (recur (rest ents)
                   (conj lines line)
                   (+ tokens line-tokens)
                   (into used-fact-eids entity-used-fact-eids))))))))

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
  (when (seq episodes)
    (loop [eps episodes
           lines ["### Recent"]
           tokens 10]
      (if (empty? eps)
        (str/join "\n" lines)
        (let [{:keys [summary timestamp]} (first eps)
              date-str (format-date timestamp)
              line (str "- [" (or date-str "?") "] " summary)
              line-tokens (estimate-tokens line)]
          (if (> (+ tokens line-tokens) budget)
            (str/join "\n" lines)
            (recur (rest eps)
                   (conj lines line)
                   (+ tokens line-tokens))))))))

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
                          (map :summary)
                          (remove str/blank?)
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
  (when (seq docs)
    (loop [remaining docs
           lines ["### Local Documents"]
           tokens 12]
      (if (empty? remaining)
        (str/join "\n" lines)
        (let [line (local-doc-line (first remaining))
              line-tokens (estimate-tokens line)]
          (if (> (+ tokens line-tokens) budget)
            (str/join "\n" lines)
            (recur (rest remaining)
                   (conj lines line)
                   (+ tokens line-tokens))))))))

(defn render-skills
  "Render skills into prompt format, within token budget."
  [skills budget]
  (when (seq skills)
    (loop [sks skills
           parts ["## Skills\nFollow these instructions when relevant.\n"]
           tokens 20]
      (if (empty? sks)
        (str/join "\n" parts)
        (let [s (first sks)
              section (str "### " (:skill/name s) "\n" (:skill/content s))
              section-tokens (estimate-tokens section)]
          (if (> (+ tokens section-tokens) budget)
            (str/join "\n" parts) ; budget exceeded
            (recur (rest sks)
                   (conj parts section)
                   (+ tokens section-tokens))))))))

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
        id-tokens  (estimate-tokens id-section)

        ;; P0: Topic (always included)
        topic-section (render-topic wm-context)
        topic-tokens  (estimate-tokens (or topic-section ""))

        ;; Remaining budget for P1-P3
        ;; Allocate to highest priority first so lower priorities get cut first
        remaining (- (:total budget) id-tokens topic-tokens)

        ;; P1: Entities (highest priority — cut last)
        ent-budget  (min (:entities budget) (max 0 remaining))
        ent-data    (render-entities-data (:entities wm-context) ent-budget)
        ent-section (:content ent-data)
        ent-tokens  (estimate-tokens (or ent-section ""))
        remaining   (- remaining ent-tokens)

        ;; P2: Local documents
        doc-budget  (min (:local-docs budget) (max 0 remaining))
        doc-section (render-local-docs (:local-docs wm-context) doc-budget)
        doc-tokens  (estimate-tokens (or doc-section ""))
        remaining   (- remaining doc-tokens)

        ;; P3: Episodes
        ep-budget  (min (:episodes budget) (max 0 remaining))
        ep-section (render-episodes (:episodes wm-context) ep-budget)
        ep-tokens  (estimate-tokens (or ep-section ""))
        remaining  (- remaining ep-tokens)

        ;; P4: Skills (lowest priority — cut first)
        skill-budget  (min (:skills budget) (max 0 remaining))
        skill-section (render-skills skills skill-budget)
        skill-tokens  (estimate-tokens (or skill-section ""))]
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
   (let [total-tokens (->> messages
                           (map #(estimate-tokens (:content %)))
                           (reduce +))
         msg-count    (count messages)]
     (if (or (<= total-tokens budget) (<= msg-count 4))
       messages ; fits in budget or too few to compact
       ;; Summarize the older half
       (let [keep-count  (max 4 (quot msg-count 2))
             old-msgs    (subvec (vec messages) 0 (- msg-count keep-count))
             recent-msgs (subvec (vec messages) (- msg-count keep-count))
             old-text    (->> old-msgs
                              (mapcat history-message->summary-lines)
                              (str/join "\n"))]
         (try
           (let [recap (summarize-history-text old-text {:provider-id provider-id
                                                         :workload workload})]
             (into [(history-recap-message recap)] recent-msgs))
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
  (let [recent-limit (recent-history-message-limit opts)
        metadata     (db/session-message-metadata session-id)
        total        (count metadata)]
    (if (<= total recent-limit)
      (mapv history-message->llm-message
            (db/session-messages-by-eids (mapv :eid metadata)))
      (let [recent-start      (max 0 (- total recent-limit))
            recap-state       (db/session-history-recap session-id)
            summarized-count0 (long (or (:message-count recap-state) 0))
            summarized-count  (if (<= summarized-count0 recent-start)
                                summarized-count0
                                0)
            recap-content     (when (= summarized-count summarized-count0)
                                (:content recap-state))
            newly-archived    (subvec metadata summarized-count recent-start)
            recent-meta       (subvec metadata recent-start total)
            recent-eids       (mapv :eid recent-meta)
            recent-messages   (db/session-messages-by-eids recent-eids)]
        (if-not (seq newly-archived)
          (let [recent-history (mapv history-message->llm-message recent-messages)]
            (if (seq recap-content)
              (into [(history-recap-message recap-content)] recent-history)
              recent-history))
          (let [archived-text (->> (db/session-messages-by-eids (mapv :eid newly-archived))
                                   (mapcat history-message->summary-lines)
                                   (str/join "\n"))]
            (try
              (let [new-recap (summarize-history-text archived-text
                                                      recap-content
                                                      {:provider-id (:compaction-provider-id opts)
                                                       :workload (or (:compaction-workload opts)
                                                                     :history-compaction)})]
                (if (seq new-recap)
                  (do
                    (db/save-session-history-recap! session-id new-recap recent-start)
                    (into [(history-recap-message new-recap)]
                          (map history-message->llm-message recent-messages)))
                  (mapv history-message->llm-message
                        (db/session-messages session-id))))
              (catch Exception e
                (log/warn "Failed to update session history recap:" (.getMessage e))
                (mapv history-message->llm-message
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
