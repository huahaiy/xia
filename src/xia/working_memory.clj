(ns xia.working-memory
  "Working memory — the active, curated context for the current conversation.

   Analogous to the prefrontal cortex: holds a relevance-filtered subset of
   the entire memory system. Can pull in facts from months ago if relevant now.
   Tracks what the agent is 'thinking about.'

   Lifecycle:
   1. Created on session start (warm start from previous session's topics)
   2. Updated each turn before the LLM call (retrieval pipeline)
   3. Cleared on session end (topic summary feeds into episodic recording)

   Runtime: in-memory atom. Snapshot to DB only on session events."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.memory :as memory]))

;; ============================================================================
;; State
;; ============================================================================

(defonce ^:private wm-atom (atom nil))

(def ^:private default-config
  {:max-slots             15
   :max-episode-refs      5
   :decay-factor          0.85
   :eviction-threshold    0.1
   :topic-update-interval 5})   ; update topic summary every N turns

;; ============================================================================
;; Stopwords for keyword extraction
;; ============================================================================

(def ^:private stopwords
  #{"a" "an" "the" "is" "are" "was" "were" "be" "been" "being"
    "have" "has" "had" "do" "does" "did" "will" "would" "could"
    "should" "may" "might" "shall" "can" "need" "to" "of" "in"
    "for" "on" "with" "at" "by" "from" "as" "into" "through"
    "during" "before" "after" "above" "below" "between" "out"
    "off" "over" "under" "again" "then" "once" "here" "there"
    "when" "where" "why" "how" "all" "both" "each" "few" "more"
    "most" "other" "some" "such" "no" "nor" "not" "only" "own"
    "same" "so" "than" "too" "very" "just" "because" "but" "and"
    "or" "if" "while" "about" "up" "it" "its" "i" "me" "my"
    "we" "our" "you" "your" "he" "him" "his" "she" "her" "they"
    "them" "their" "what" "which" "who" "whom" "this" "that"
    "these" "those" "am" "also" "like" "get" "got" "going" "go"
    "make" "know" "think" "want" "see" "look" "tell" "say" "said"
    "one" "two" "yeah" "yes" "ok" "okay" "hi" "hello" "hey"
    "please" "thanks" "thank" "don't" "doesn't" "didn't" "isn't"
    "aren't" "wasn't" "weren't" "won't" "wouldn't" "couldn't"
    "shouldn't" "can't" "let" "let's" "i'm" "i've" "i'd" "i'll"
    "you're" "you've" "you'd" "you'll" "he's" "she's" "it's"
    "we're" "we've" "they're" "they've" "that's" "there's"})

;; ============================================================================
;; Stage 1: Keyword Extraction (pure Clojure, zero LLM cost)
;; ============================================================================

(defn extract-search-terms
  "Extract search terms from a user message.
   Splits words, filters stopwords, extracts proper nouns,
   includes entity names already in working memory."
  [message]
  (let [;; Tokenize and clean
        words (->> (str/split (str/lower-case message) #"[^\w'-]+")
                   (remove str/blank?)
                   (remove #(< (count %) 2)))
        ;; Filter stopwords
        meaningful (remove stopwords words)
        ;; Extract capitalized words from original message (proper nouns)
        proper-nouns (->> (re-seq #"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*" message)
                         (map str/lower-case))
        ;; Include active entity names from WM
        wm-names (when-let [wm @wm-atom]
                   (->> (:slots wm)
                        vals
                        (map :name)
                        (remove nil?)
                        (map str/lower-case)))
        ;; Combine, deduplicate
        all-terms (distinct (concat proper-nouns meaningful wm-names))]
    (vec (take 20 all-terms))))

;; ============================================================================
;; Stage 2: Hybrid Search
;; ============================================================================

(defn search-knowledge
  "Search across nodes, facts, and episodes using FTS.
   Returns {:nodes [...] :facts [...] :episodes [...]}."
  [terms]
  (let [query (str/join " " terms)]
    (when-not (str/blank? query)
      {:nodes    (memory/search-nodes query :top 10)
       :facts    (memory/search-facts query :top 15)
       :episodes (memory/search-episodes query :top 5)})))

;; ============================================================================
;; Stage 3: Graph Expansion (spreading activation)
;; ============================================================================

(defn expand-graph
  "For matched nodes, pull facts and one-hop edges.
   Returns additional nodes that are connected to matched nodes."
  [node-eids]
  (reduce
    (fn [acc eid]
      (let [edges  (memory/node-edges eid)
            ;; Get connected node eids from edges
            connected (->> (db/q '[:find ?to
                                   :in $ ?from
                                   :where [?e :kg.edge/from ?from]
                                          [?e :kg.edge/to ?to]]
                                 eid)
                           (map first))
            incoming  (->> (db/q '[:find ?from
                                   :in $ ?to
                                   :where [?e :kg.edge/to ?to]
                                          [?e :kg.edge/from ?from]]
                                 eid)
                           (map first))
            all-connected (distinct (concat connected incoming))]
        (reduce (fn [a c-eid]
                  (if (contains? a c-eid)
                    a ; already known
                    (let [node (memory/get-node c-eid)]
                      (assoc a c-eid {:node-eid c-eid
                                      :name     (:kg.node/name node)
                                      :type     (:kg.node/type node)
                                      :expanded? true}))))
                acc
                all-connected)))
    {}
    node-eids))

;; ============================================================================
;; Stage 4: Working Memory Merge, Decay, Eviction
;; ============================================================================

(defn- merge-node-into-slot
  "Merge a search result node into WM, refreshing or creating a slot."
  [slots node-eid name type relevance turn-count]
  (if-let [existing (get slots node-eid)]
    ;; Refresh: boost relevance
    (assoc slots node-eid
           (assoc existing :relevance (min 1.0 (+ (:relevance existing) relevance))))
    ;; New slot
    (assoc slots node-eid
           {:node-eid   node-eid
            :name       name
            :type       type
            :facts      (memory/node-facts node-eid)
            :edges      (memory/node-edges node-eid)
            :properties (memory/node-properties node-eid)
            :relevance  relevance
            :pinned?    false
            :added-turn turn-count})))

(defn- merge-results!
  "Merge search + expansion results into working memory."
  [search-results expanded-nodes]
  (swap! wm-atom
    (fn [wm]
      (let [turn (:turn-count wm)
            ;; Merge direct search hits at high relevance
            slots-with-nodes
            (reduce (fn [slots {:keys [eid name type]}]
                      (merge-node-into-slot slots eid name type 0.8 turn))
                    (:slots wm)
                    (:nodes search-results))
            ;; Merge nodes found via fact search
            slots-with-fact-nodes
            (reduce (fn [slots {:keys [node-eid]}]
                      (if (contains? slots node-eid)
                        ;; Boost existing slot
                        (update-in slots [node-eid :relevance]
                                   #(min 1.0 (+ % 0.3)))
                        ;; Add the node
                        (let [node (memory/get-node node-eid)]
                          (merge-node-into-slot
                            slots node-eid
                            (:kg.node/name node) (:kg.node/type node)
                            0.6 turn))))
                    slots-with-nodes
                    (:facts search-results))
            ;; Merge expanded (one-hop) nodes at lower relevance
            slots-with-expanded
            (reduce (fn [slots [eid {:keys [name type]}]]
                      (if (contains? slots eid)
                        slots ; already in WM from direct search
                        (merge-node-into-slot slots eid name type 0.3 turn)))
                    slots-with-fact-nodes
                    expanded-nodes)
            ;; Merge episode refs
            max-refs (get-in wm [:config :max-episode-refs])
            new-refs (->> (:episodes search-results)
                          (mapv (fn [{:keys [eid summary timestamp]}]
                                  {:episode-eid eid
                                   :summary     summary
                                   :timestamp   timestamp
                                   :relevance   0.7})))
            merged-refs (->> (concat (:episode-refs wm) new-refs)
                             (group-by :episode-eid)
                             vals
                             (map (fn [dups]
                                    (apply max-key :relevance dups)))
                             (sort-by :relevance >)
                             (take max-refs)
                             vec)]
        (assoc wm
               :slots        slots-with-expanded
               :episode-refs merged-refs)))))

(defn decay-slots!
  "Apply decay to all non-pinned, non-refreshed slots."
  []
  (swap! wm-atom
    (fn [wm]
      (let [factor (get-in wm [:config :decay-factor])]
        (update wm :slots
                (fn [slots]
                  (reduce-kv
                    (fn [acc eid slot]
                      (assoc acc eid
                             (if (:pinned? slot)
                               slot
                               (update slot :relevance * factor))))
                    {}
                    slots)))))))

(defn evict-slots!
  "Remove slots below the eviction threshold. Enforce max capacity."
  []
  (swap! wm-atom
    (fn [wm]
      (let [threshold (get-in wm [:config :eviction-threshold])
            max-slots (get-in wm [:config :max-slots])
            filtered  (->> (:slots wm)
                           (filter (fn [[_ slot]]
                                     (or (:pinned? slot)
                                         (>= (:relevance slot) threshold))))
                           (sort-by (fn [[_ slot]] (:relevance slot)) >)
                           (take max-slots)
                           (into {}))]
        (assoc wm :slots filtered)))))

;; ============================================================================
;; Topic Tracking
;; ============================================================================

(defn update-topics!
  "Update the topic summary using the LLM. Called every N turns."
  []
  (let [wm @wm-atom
        entities (->> (:slots wm) vals (map :name) (remove nil?) (str/join ", "))
        episodes (->> (:episode-refs wm) (map :summary) (str/join "; "))
        prompt   (str "Current entities in focus: " entities
                      "\nRecent relevant episodes: " episodes
                      "\n\nSummarize the current conversation focus in ONE sentence.")]
    (try
      (let [summary (llm/chat-simple
                      [{:role "system"
                        :content "You are a topic summarizer. Return exactly ONE sentence describing the current conversation focus. Be specific and concise."}
                       {:role "user" :content prompt}])]
        (swap! wm-atom
          (fn [wm]
            (assoc wm
                   :prev-topics (:topics wm)
                   :topics      (str/trim summary)
                   :topic-turn  (:turn-count wm))))
        (log/debug "Updated topic summary:" summary))
      (catch Exception e
        (log/warn "Failed to update topic summary:" (.getMessage e))))))

(defn detect-topic-shift?
  "Compare current search terms with previous topic summary.
   Returns true if topics have shifted significantly."
  [search-terms]
  (let [wm @wm-atom
        prev-topics (:topics wm)]
    (when (and prev-topics (seq search-terms))
      (let [prev-words (->> (str/split (str/lower-case prev-topics) #"[^\w]+")
                            (remove str/blank?)
                            set)
            current-words (set (map str/lower-case search-terms))
            overlap (count (clojure.set/intersection prev-words current-words))
            max-possible (max 1 (min (count prev-words) (count current-words)))
            similarity (/ (double overlap) max-possible)]
        ;; Shift if less than 20% overlap
        (< similarity 0.2)))))

;; ============================================================================
;; Auto-segmentation
;; ============================================================================

(defn auto-segment!
  "Close the current episode and start a new one within the same session.
   Called when a significant topic shift is detected."
  [session-id channel]
  (let [wm @wm-atom]
    (log/info "Topic shift detected — auto-segmenting episode")
    ;; Record current episode with WM topics as context
    (memory/record-episode!
      {:type       :conversation
       :summary    (or (:topics wm) "Conversation segment")
       :context    (str "Topic: " (:topics wm))
       :channel    channel
       :session-id session-id})
    ;; Reset topic tracking for new segment
    (swap! wm-atom assoc
           :prev-topics (:topics wm)
           :topics      nil
           :topic-turn  (:turn-count wm))))

;; ============================================================================
;; Per-turn Update (orchestrator)
;; ============================================================================

(defn update-wm!
  "Per-turn working memory update. Runs the full retrieval pipeline:
   1. Extract search terms from user message
   2. Hybrid search across KG nodes, facts, episodes
   3. Graph expansion (spreading activation)
   4. Merge results, decay, evict
   5. Check for topic shift → auto-segment if needed
   6. Periodically update topic summary"
  [user-message session-id channel]
  (when @wm-atom
    (swap! wm-atom update :turn-count inc)
    (let [;; Stage 1: keyword extraction
          terms (extract-search-terms user-message)]
      (when (seq terms)
        ;; Stage 2: hybrid search
        (let [results (search-knowledge terms)]
          (when results
            ;; Stage 3: graph expansion
            (let [matched-eids (mapv :eid (:nodes results))
                  expanded     (when (seq matched-eids)
                                 (expand-graph matched-eids))]
              ;; Stage 4: merge + decay + evict
              (merge-results! results expanded)
              (decay-slots!)
              (evict-slots!)))))
      ;; Check for topic shift → auto-segment
      (when (and (> (:turn-count @wm-atom) 3)
                 (detect-topic-shift? terms))
        (auto-segment! session-id channel))
      ;; Periodically update topic summary
      (let [wm @wm-atom
            interval (get-in wm [:config :topic-update-interval])
            turns-since (- (:turn-count wm) (:topic-turn wm))]
        (when (>= turns-since interval)
          (future (update-topics!)))))
    @wm-atom))

;; ============================================================================
;; Lifecycle
;; ============================================================================

(defn create-wm!
  "Create a new working memory for a session."
  [session-id]
  (reset! wm-atom
    {:session-id   session-id
     :topics       nil
     :prev-topics  nil
     :turn-count   0
     :topic-turn   0
     :slots        {}
     :episode-refs []
     :config       default-config})
  @wm-atom)

(defn warm-start!
  "Pre-populate WM from the previous session's topic summary.
   Avoids the 'xia forgot everything' feel on quick restarts."
  []
  (when @wm-atom
    (let [last-episode (db/latest-session-episode)]
      (when-let [prev-context (or (:context last-episode)
                                  (:summary last-episode))]
        (log/info "Warm start from previous session:" prev-context)
        (swap! wm-atom assoc :prev-topics prev-context)
        (let [terms (extract-search-terms prev-context)
              results (when (seq terms) (search-knowledge terms))]
          (when results
            (let [matched-eids (mapv :eid (:nodes results))
                  expanded (when (seq matched-eids) (expand-graph matched-eids))]
              (merge-results! results expanded)))))))
  @wm-atom)

(defn get-wm
  "Return current working memory state."
  []
  @wm-atom)

(defn clear-wm!
  "Clear working memory (on session end)."
  []
  (reset! wm-atom nil))

;; ============================================================================
;; Pinning
;; ============================================================================

(defn pin! [node-eid]
  (swap! wm-atom update-in [:slots node-eid :pinned?] (constantly true)))

(defn unpin! [node-eid]
  (swap! wm-atom update-in [:slots node-eid :pinned?] (constantly false)))

;; ============================================================================
;; Export for context assembly
;; ============================================================================

(defn wm->context
  "Export working memory as context data for prompt assembly."
  []
  (when-let [wm @wm-atom]
    {:topics       (:topics wm)
     :entities     (->> (:slots wm)
                        vals
                        (sort-by :relevance >)
                        (mapv (fn [{:keys [name type facts edges properties relevance pinned?]}]
                                {:name       name
                                 :type       type
                                 :facts      facts
                                 :edges      edges
                                 :properties properties
                                 :relevance  relevance
                                 :pinned?    pinned?})))
     :episodes     (->> (:episode-refs wm)
                        (sort-by :relevance >)
                        (mapv (fn [{:keys [summary timestamp relevance]}]
                                {:summary   summary
                                 :timestamp timestamp
                                 :relevance relevance})))
     :turn-count   (:turn-count wm)}))

;; ============================================================================
;; Snapshot (crash-recovery)
;; ============================================================================

(defn snapshot!
  "Persist current WM state to DB."
  []
  (when-let [wm @wm-atom]
    (try
      (db/save-wm-snapshot! wm)
      (catch Exception e
        (log/warn "Failed to save WM snapshot:" (.getMessage e))))))
