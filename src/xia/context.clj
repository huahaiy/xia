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
            [clojure.tools.logging :as log]
            [charred.api :as json]
            [xia.db :as db]
            [xia.identity :as identity]
            [xia.skill :as skill]
            [xia.llm :as llm]
            [xia.working-memory :as wm]))

;; ============================================================================
;; Token estimation
;; ============================================================================

(defn estimate-tokens
  "Rough token estimate: ~4 chars per token."
  [s]
  (if (str/blank? s) 0 (quot (count s) 4)))

;; ============================================================================
;; Budget config
;; ============================================================================

(def ^:private default-system-prompt-budget
  {:total      4000
   :identity   300
   :topic      100
   :entities   1500
   :episodes   500
   :skills     1500})

(def ^:private default-history-budget 8000)

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

(defn- render-entity [{:keys [name type facts edges properties]}]
  (let [type-str (when type (str " (" (clojure.core/name type) ")"))
        ;; Properties as compact key: value pairs
        prop-strs (when (and properties (map? properties) (seq properties))
                    (flatten-props properties))
        ;; Top facts by confidence
        fact-strs (->> facts
                       (filter #(> (:confidence % 0) 0.3))
                       (take 5)
                       (map :content))
        ;; Outgoing edges
        edge-strs (->> (:outgoing edges)
                       (take 3)
                       (map (fn [{:keys [type target]}]
                              (str (clojure.core/name type) "→" target))))
        detail (str/join "; " (concat prop-strs fact-strs edge-strs))]
    (str "- " name type-str
         (when-not (str/blank? detail) (str ": " detail)))))

(defn render-entities
  "Render active entities + facts into compact format, within token budget."
  [entities budget]
  (when (seq entities)
    (loop [ents entities
           lines ["### Known"]
           tokens 8] ; "### Known\n"
      (if (empty? ents)
        (str/join "\n" lines)
        (let [line (render-entity (first ents))
              line-tokens (estimate-tokens line)]
          (if (> (+ tokens line-tokens) budget)
            (str/join "\n" lines) ; budget exceeded, stop
            (recur (rest ents)
                   (conj lines line)
                   (+ tokens line-tokens))))))))

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

(defn assemble-system-prompt
  "Build the complete system prompt with budget enforcement.
   Priority: identity (P0) > topic (P0) > entities (P1) > episodes (P2) > skills (P3)."
  ([session-id]
   (assemble-system-prompt session-id nil))
  ([session-id opts]
   (let [budget     (resolve-system-prompt-budget (resolve-context-provider opts))
        wm-context (or (wm/wm->context session-id)
                       {:topics nil :entities [] :episodes [] :turn-count 0})
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
        ent-section (render-entities (:entities wm-context) ent-budget)
        ent-tokens  (estimate-tokens (or ent-section ""))
        remaining   (- remaining ent-tokens)

        ;; P2: Episodes (cut second)
        ep-budget  (min (:episodes budget) (max 0 remaining))
        ep-section (render-episodes (:episodes wm-context) ep-budget)
        ep-tokens  (estimate-tokens (or ep-section ""))
        remaining  (- remaining ep-tokens)

        ;; P3: Skills (lowest priority — cut first)
        skill-budget  (min (:skills budget) (max 0 remaining))
        skill-section (render-skills skills skill-budget)
        skill-tokens  (estimate-tokens (or skill-section ""))]
    (str id-section
         (when topic-section (str "## Context\n" topic-section))
         (when ent-section (str ent-section "\n\n"))
         (when ep-section (str ep-section "\n\n"))
         (when skill-section (str skill-section "\n\n"))))))

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
                              (map (fn [{:keys [role content]}]
                                     (str (name role) ": " content)))
                              (str/join "\n"))]
         (try
           (let [messages [{:role "system"
                            :content "Summarize this conversation excerpt in 2-4 sentences. Capture key topics, decisions, and any personal information shared. Be factual."}
                           {:role "user" :content old-text}]
                 recap    (cond
                            provider-id
                            (llm/chat-simple messages :provider-id provider-id)

                            workload
                            (llm/chat-simple messages :workload workload)

                            :else
                            (llm/chat-simple messages))]
             (into [{:role "system"
                     :content (str "[Conversation recap: " (str/trim recap) "]")}]
                   recent-msgs))
           (catch Exception e
             (log/warn "Failed to compact history:" (.getMessage e))
             messages)))))))

;; ============================================================================
;; Build messages (moved from agent.clj)
;; ============================================================================

(defn build-messages
  "Build the full message list for an LLM call:
   system prompt (identity + WM context + skills) + compacted history."
  ([session-id]
   (build-messages session-id nil))
  ([session-id opts]
   (let [provider     (resolve-context-provider opts)
         compaction-provider-id (:compaction-provider-id opts)
         compaction-workload    (or (:compaction-workload opts)
                                    :history-compaction)
        sys-prompt   (assemble-system-prompt session-id (assoc opts :provider provider))
        history      (db/session-messages session-id)
        history-msgs (mapv (fn [{:keys [role content tool-calls tool-id tool-result]}]
                             (cond-> {:role (name role)
                                      :content (if (= role :tool)
                                                 (tool-result->content tool-result content)
                                                 content)}
                               tool-calls (assoc :tool_calls tool-calls)
                               tool-id    (assoc :tool_call_id tool-id)))
                           history)
        budget       (resolve-history-budget provider)
        compacted    (compact-history history-msgs
                                      budget
                                      (cond-> {}
                                        compaction-provider-id
                                        (assoc :provider-id compaction-provider-id)

                                        (and (not compaction-provider-id)
                                             compaction-workload)
                                        (assoc :workload compaction-workload)))]
    (into [{:role "system" :content sys-prompt}]
          compacted))))
