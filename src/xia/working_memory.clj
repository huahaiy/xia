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
  (:require [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.memory :as memory]))

;; ============================================================================
;; State
;; ============================================================================

(def ^:dynamic *session-id*
  "Current working-memory session for APIs that omit an explicit session id."
  nil)

(def ^:dynamic *session-op-session-id* nil)
(def ^:dynamic *session-op-thread* nil)

(defonce ^:private wm-atom (atom {}))
(def ^:private session-op-lock-count 512)
(defonce ^:private session-op-locks
  (vec (repeatedly session-op-lock-count #(Object.))))

(declare get-wm warm-start!)

(defn- default-session-id []
  (when (= 1 (count @wm-atom))
    (first (keys @wm-atom))))

(defn- resolve-session-id
  ([] (resolve-session-id nil))
  ([session-id]
   (or session-id *session-id* (default-session-id))))

(defn- session-wm
  ([]
   (session-wm nil))
  ([session-id]
   (when-let [sid (resolve-session-id session-id)]
     (get @wm-atom sid))))

(defn- session-op-lock
  [session-id]
  (when session-id
    (nth session-op-locks
         (mod (bit-and Integer/MAX_VALUE (int (hash session-id)))
              session-op-lock-count))))

(defn- in-session-op?
  [session-id]
  (and session-id
       (= session-id *session-op-session-id*)
       (identical? (Thread/currentThread) *session-op-thread*)))

(defn- run-session-op!
  [session-id f]
  (if-let [sid (resolve-session-id session-id)]
    (if (in-session-op? sid)
      (f sid)
      (locking (session-op-lock sid)
        (binding [*session-op-session-id* sid
                  *session-op-thread* (Thread/currentThread)]
          (f sid))))
    (f nil)))

(defn- update-session-wm!
  [session-id f]
  (let [sid (resolve-session-id session-id)
        result (atom nil)]
    (when sid
      (swap! wm-atom
             (fn [states]
               (if-let [wm (get states sid)]
                 (let [updated (f wm)]
                   (reset! result updated)
                   (assoc states sid updated))
                 states))))
    @result))

(def ^:private default-config
  {:max-slots             15
   :max-episode-refs      5
   :max-local-doc-refs    4
   :decay-factor          0.85
   :eviction-threshold    0.1
   :topic-update-interval 5})   ; update topic summary every N turns

(def ^:private fact-utility-batch-size 20)
(def ^:private fact-utility-prompt
  "You are rating how useful retrieved memory facts were for answering the user.

Return a JSON object with this exact shape:
{
  \"facts\": [
    {\"index\": 0, \"utility\": 0.0},
    {\"index\": 1, \"utility\": 0.0}
  ]
}

Rules:
- utility must be a number between 0.0 and 1.0
- higher = the fact clearly helped answer the user, guide tool use, disambiguate context, or personalize the response
- lower = the fact was irrelevant, redundant, or not meaningfully used
- rate every fact in the batch
- return ONLY valid JSON, no markdown fencing")

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
;; Stage 1: Keyword Extraction (cheap lexical hints)
;; ============================================================================

(defn extract-search-terms
  "Extract search terms from a user message.
   Splits words, filters stopwords, extracts proper nouns,
   includes entity names already in working memory.
   These terms are used as a lexical hint for FTS, not as the only recall path."
  ([message]
   (extract-search-terms nil message))
  ([session-id message]
   (let [;; Tokenize and clean
        words (into []
                    (comp (remove str/blank?)
                          (remove #(< (count %) 2)))
                    (str/split (str/lower-case message) #"[^\w'-]+"))
        ;; Filter stopwords
        meaningful (into [] (remove stopwords) words)
        ;; Extract capitalized words from original message (proper nouns)
        proper-nouns (into [] (map str/lower-case)
                           (or (re-seq #"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*" message)
                               []))
        ;; Include active entity names from WM
        wm-names (when-let [wm (session-wm session-id)]
                   (let [slot-names (into []
                                          (comp (map :name)
                                                (remove nil?)
                                                (map str/lower-case))
                                          (vals (:slots wm)))
                         doc-names  (into []
                                          (comp (map :name)
                                                (remove nil?)
                                                (map str/lower-case))
                                          (:local-doc-refs wm))]
                     (into slot-names doc-names)))
        ;; Combine, deduplicate
        all-terms (into []
                        (comp cat (distinct) (take 20))
                        [proper-nouns meaningful wm-names])]
     all-terms)))

;; ============================================================================
;; Stage 2: Hybrid Search
;; ============================================================================

(defn search-knowledge
  "Search across nodes, facts, and episodes using hybrid lexical + semantic retrieval.
   Returns {:nodes [...] :facts [...] :episodes [...] :local-docs [...]}."
  ([terms]
   (search-knowledge terms nil))
  ([terms semantic-query]
   (search-knowledge nil terms semantic-query))
  ([session-id terms semantic-query]
   (search-knowledge session-id terms semantic-query nil))
  ([session-id terms semantic-query resource-session-id]
   (let [fts-query      (str/join " " terms)
         semantic-query (or semantic-query fts-query)
         local-doc-session-id (or resource-session-id session-id)
         safe-search    (fn [step f]
                          (try
                            (f)
                            (catch Exception e
                              (log/warn e "Working-memory search step failed; continuing with empty results"
                                        {:step step
                                         :session-id session-id})
                              [])))]
    (when (or (not (str/blank? fts-query))
              (not (str/blank? semantic-query)))
      {:nodes      (safe-search :nodes
                                #(memory/search-nodes semantic-query :fts-query fts-query :top 10))
       :facts      (safe-search :facts
                                #(memory/search-facts semantic-query :fts-query fts-query :top 15))
       :episodes   (safe-search :episodes
                                #(memory/search-episodes semantic-query :fts-query fts-query :top 5))
       :local-docs (safe-search :local-docs
                                #(memory/search-local-docs local-doc-session-id
                                                           semantic-query
                                                           :fts-query fts-query
                                                           :top 4))}))))

;; ============================================================================
;; Stage 3: Graph Expansion (spreading activation)
;; ============================================================================

(defn expand-graph
  "For matched nodes, pull facts and one-hop edges.
   Returns additional nodes that are connected to matched nodes."
  [node-eids]
  (memory/connected-node-summaries node-eids))

;; ============================================================================
;; Stage 4: Working Memory Merge, Decay, Eviction
;; ============================================================================

(defn- boost-relevance
  ^double
  [existing boost]
  (let [existing* (-> (double (or existing 0.0))
                      (clojure.core/max 0.0)
                      (clojure.core/min 1.0))
        boost*    (-> (double (or boost 0.0))
                      (clojure.core/max 0.0)
                      (clojure.core/min 1.0))]
    ;; Refreshes should strengthen active slots without letting them get pinned
    ;; at 1.0 under repeated mention-plus-decay cycles.
    (+ existing* (* (- 1.0 existing*) boost*))))

(defn- merge-node-into-slot
  "Merge a search result node into WM, refreshing or creating a slot."
  [slots node-eid name type relevance turn-count]
  (if-let [existing (get slots node-eid)]
    ;; Refresh: boost relevance
    (assoc slots node-eid
           (assoc existing :relevance (boost-relevance (:relevance existing)
                                                       relevance)))
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

(defn- ref-relevance
  ^double
  [ref]
  (double (or (:relevance ref) 0.0)))

(defn- better-ref?
  [candidate existing]
  (> (double (ref-relevance candidate))
     (double (ref-relevance existing))))

(defn- lowest-ranked-ref-entry
  [selected]
  (reduce-kv
    (fn [lowest ref-id ref]
      (if (or (nil? lowest)
              (< (double (ref-relevance ref))
                 (double (ref-relevance (second lowest)))))
        [ref-id ref]
        lowest))
    nil
    selected))

(defn- merge-bounded-refs
  [existing-refs new-refs ref-id-key max-count]
  (if (pos? (long max-count))
    (let [select-ref (fn [selected ref]
                       (let [ref-id (ref-id-key ref)]
                         (if-let [existing (get selected ref-id)]
                           (if (better-ref? ref existing)
                             (assoc selected ref-id ref)
                             selected)
                           (if (< (long (count selected)) (long max-count))
                             (assoc selected ref-id ref)
                             (let [[lowest-id lowest-ref] (lowest-ranked-ref-entry selected)]
                               (if (better-ref? ref lowest-ref)
                                 (-> selected
                                     (dissoc lowest-id)
                                     (assoc ref-id ref))
                                 selected))))))
          selected   (reduce select-ref {} existing-refs)
          selected   (reduce select-ref selected new-refs)]
      (->> selected
           vals
           (sort-by :relevance >)
           vec))
    []))

(defn- merge-results!
  "Merge search + expansion results into working memory."
  [session-id search-results expanded-nodes]
  (update-session-wm! session-id
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
	                                   #(boost-relevance % 0.3))
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
            new-refs (into [] (map (fn [{:keys [eid summary timestamp]}]
                                     {:episode-eid eid
                                      :summary     summary
                                      :timestamp   timestamp
                                      :relevance   0.7}))
                           (:episodes search-results))
            merged-refs (merge-bounded-refs (:episode-refs wm)
                                            new-refs
                                            :episode-eid
                                            max-refs)
            max-doc-refs (get-in wm [:config :max-local-doc-refs])
            new-doc-refs (into [] (map-indexed
	                                    (fn [idx {:keys [id name media-type summary preview matched-chunks]}]
	                                      {:doc-id         id
	                                       :name           name
	                                       :media-type     media-type
	                                       :summary        summary
	                                       :preview        preview
	                                       :matched-chunks matched-chunks
	                                       :relevance      (clojure.core/max 0.45
	                                                                         (- 0.8 (* (double idx) 0.1)))}))
                               (:local-docs search-results))
            merged-doc-refs (merge-bounded-refs (:local-doc-refs wm)
                                                new-doc-refs
                                                :doc-id
                                                max-doc-refs)]
        (assoc wm
               :slots        slots-with-expanded
               :episode-refs merged-refs
               :local-doc-refs merged-doc-refs)))))

(defn- decay-slot-map
  [slots factor]
  (reduce-kv
    (fn [acc eid slot]
	      (assoc acc eid
	             (if (:pinned? slot)
	               slot
	               (update slot :relevance #(* (double %) (double factor))))))
    {}
    slots))

(defn- prune-slot-map
  [slots threshold max-slots]
  (->> slots
	       (filter (fn [[_ slot]]
	                 (or (:pinned? slot)
	                     (>= (double (:relevance slot)) (double threshold)))))
       (sort-by (fn [[_ slot]] (:relevance slot)) >)
       (take max-slots)
       (into {})))

(defn decay-slots!
  "Apply decay to all non-pinned, non-refreshed slots."
  ([]
   (decay-slots! nil))
  ([session-id]
   (run-session-op! session-id
     (fn [sid]
       (update-session-wm! sid
        (fn [wm]
          (let [factor (get-in wm [:config :decay-factor])]
            (update wm :slots
                    #(decay-slot-map % factor)))))))))

(defn evict-slots!
  "Remove slots below the eviction threshold. Enforce max capacity."
  ([]
   (evict-slots! nil))
  ([session-id]
   (run-session-op! session-id
     (fn [sid]
       (update-session-wm! sid
        (fn [wm]
          (let [threshold (get-in wm [:config :eviction-threshold])
                max-slots (get-in wm [:config :max-slots])
                filtered  (prune-slot-map (:slots wm) threshold max-slots)]
            (assoc wm :slots filtered))))))))

;; ============================================================================
;; Topic Tracking
;; ============================================================================

(defn update-topics!
  "Update the topic summary using the LLM. Called every N turns."
  ([]
   (update-topics! nil))
  ([session-id]
   (run-session-op! session-id
     (fn [sid]
       (let [wm       (session-wm sid)
             entities (str/join ", " (into [] (comp (map :name) (remove nil?))
                                           (vals (:slots wm))))
             local-docs (str/join ", " (into [] (comp (map :name) (remove nil?))
                                             (:local-doc-refs wm)))
             episodes (str/join "; " (into [] (comp (map :summary) (remove nil?))
                                            (:episode-refs wm)))
             prompt   (str "Current entities in focus: " entities
                           "\nRelevant local documents: " local-docs
                           "\nRecent relevant episodes: " episodes
                           "\n\nSummarize the current conversation focus in ONE sentence.")]
         (when wm
           (try
             (let [summary (llm/chat-simple
                             [{:role "system"
                               :content "You are a topic summarizer. Return exactly ONE sentence describing the current conversation focus. Be specific and concise."}
                              {:role "user" :content prompt}]
                             :workload :topic-summary)]
               (update-session-wm! sid
                 (fn [current-wm]
                   (assoc current-wm
                          :prev-topics (:topics current-wm)
                          :topics      (str/trim summary)
                          :topic-turn  (:turn-count current-wm))))
                (log/debug "Updated topic summary:" summary))
             (catch Exception e
               (log/warn "Failed to update topic summary:" (.getMessage e))))))))))

(defn- schedule-topic-update!
  [session-id]
  (try
    (future
      (try
        (update-topics! session-id)
        (catch Exception e
          (log/warn e "Background topic update failed" {:session-id session-id}))))
    (catch Exception e
      (log/warn e "Failed to schedule background topic update" {:session-id session-id})
      nil)))

(defn- parse-fact-utility
  [value]
  (cond
    (number? value) (double value)
    (string? value) (try
                      (Double/parseDouble (str/trim value))
                      (catch Exception _ 0.5))
    :else 0.5))

(defn- normalize-fact-utility
  [value]
  (memory/normalize-fact-utility (parse-fact-utility value)))

(defn- fact-utility-defaults
  [facts]
  (into {}
        (map (fn [{:keys [eid utility]}]
               [eid (memory/normalize-fact-utility utility)]))
        facts))

(defn- rate-fact-utility-batch
  [facts user-message assistant-response]
  (let [user-msg  (str "User message:\n" (or user-message "")
                       "\n\nAssistant response:\n" (or assistant-response "")
                       "\n\nFacts:\n"
                       (transduce
                         (map-indexed
                           (fn [idx {:keys [content confidence utility]}]
                             (str "Fact " idx
                                  "\nContent: " content
                                  "\nConfidence: " (format "%.3f" (double (or confidence 0.0)))
                                  "\nUtility: " (format "%.3f" (double (or utility 0.5))))))
                         (completing
                           (fn [^StringBuilder sb fact-text]
                             (when (pos? (.length sb))
                               (.append sb "\n\n---\n\n"))
                             (.append sb ^String fact-text))
                           (fn [^StringBuilder sb]
                             (.toString sb)))
                         (StringBuilder.)
                         facts))
        defaults  (fact-utility-defaults facts)]
    (try
      (let [response (llm/chat-simple
                       [{:role "system" :content fact-utility-prompt}
                        {:role "user" :content user-msg}]
                       :workload :fact-utility)
            parsed   (json/read-json response)]
        (reduce
          (fn [scores entry]
            (let [idx (get entry "index")]
              (if (and (number? idx)
                       (<= 0 (long idx))
                       (< (long idx) (count facts)))
                (assoc scores
                       (:eid (nth facts (long idx)))
                       (normalize-fact-utility (get entry "utility")))
                scores)))
          defaults
          (get parsed "facts" [])))
      (catch Exception e
        (log/warn "Failed to rate fact utility batch:" (.getMessage e))
        defaults))))

(defn review-fact-utility!
  [fact-eids user-message assistant-response]
  (let [facts (memory/facts-by-eids fact-eids)]
    (when (and (seq facts)
               (seq (str/trim (or assistant-response ""))))
      (doseq [batch (partition-all fact-utility-batch-size facts)]
        (let [ratings (rate-fact-utility-batch (vec batch) user-message assistant-response)]
          (doseq [[fact-eid utility] ratings]
            (memory/update-fact-utility! fact-eid utility)))))
    (count facts)))

(defn detect-topic-shift?
  "Compare current search terms with previous topic summary.
   Returns true if topics have shifted significantly."
  ([search-terms]
   (detect-topic-shift? nil search-terms))
  ([session-id search-terms]
   (let [wm (session-wm session-id)
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
         (< similarity 0.2))))))

;; ============================================================================
;; Auto-segmentation
;; ============================================================================

(defn auto-segment!
  "Close the current episode and start a new one within the same session.
   Called when a significant topic shift is detected."
  [session-id channel]
  (run-session-op! session-id
    (fn [sid]
      (let [wm (session-wm sid)]
        (log/info "Topic shift detected — auto-segmenting episode")
        ;; Record current episode with WM topics as context
        (memory/record-episode!
          {:type       :conversation
           :summary    (or (:topics wm) "Conversation segment")
           :context    (str "Topic: " (:topics wm))
           :channel    channel
           :session-id sid})
        ;; Reset topic tracking for new segment
        (update-session-wm! sid
          (fn [wm]
            (assoc wm
                   :prev-topics (:topics wm)
                   :topics      nil
                   :topic-turn  (:turn-count wm))))))))

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
  ([user-message session-id channel]
   (update-wm! user-message session-id channel nil))
  ([user-message session-id channel {:keys [resource-session-id]}]
   (run-session-op! session-id
     (fn [sid]
       (when (session-wm sid)
         (update-session-wm! sid #(update % :turn-count inc))
         (let [terms   (extract-search-terms sid user-message)
               results (search-knowledge sid terms user-message resource-session-id)]
           (when results
             (let [matched-eids (mapv :eid (:nodes results))
                   expanded     (when (seq matched-eids)
                                  (expand-graph matched-eids))]
               (merge-results! sid results expanded)
               (update-session-wm! sid
                 (fn [wm]
                   (let [factor    (get-in wm [:config :decay-factor])
                         threshold (get-in wm [:config :eviction-threshold])
                         max-slots (get-in wm [:config :max-slots])
                         decayed   (decay-slot-map (:slots wm) factor)
                         filtered  (prune-slot-map decayed threshold max-slots)]
                     (assoc wm :slots filtered))))))
	           (let [wm (session-wm sid)]
	             (when (and wm
	                        (> (long (:turn-count wm)) 3)
	                        (detect-topic-shift? sid terms))
	               (auto-segment! sid channel))
	             (when wm
	               (let [interval    (get-in wm [:config :topic-update-interval])
	                     turns-since (- (long (:turn-count wm)) (long (:topic-turn wm)))]
	                 (when (and interval (>= turns-since (long interval)))
	                   (schedule-topic-update! sid))))))
         (get-wm sid))))))

;; ============================================================================
;; Lifecycle
;; ============================================================================

(defn create-wm!
  "Create a new working memory for a session."
  [session-id]
  (run-session-op! session-id
    (fn [sid]
      (let [wm {:session-id      sid
                :topics          nil
                :prev-topics     nil
                :autonomy-state  nil
                :turn-count      0
                :topic-turn      0
                :slots           {}
                :episode-refs    []
                :local-doc-refs  []
                :config          default-config}]
        (swap! wm-atom assoc sid wm)
        wm))))

(defn ensure-wm!
  "Return working memory for the session, creating and warm-starting it if needed."
  [session-id]
  (run-session-op! session-id
    (fn [sid]
      (or (get-wm sid)
          (do
            (create-wm! sid)
            (warm-start! sid)
            (get-wm sid))))))

(defn warm-start!
  "Pre-populate WM from the previous session's topic summary.
   Avoids the 'xia forgot everything' feel on quick restarts."
  ([]
   (warm-start! nil))
  ([session-id]
   (run-session-op! session-id
     (fn [sid]
       (when sid
         (when (session-wm sid)
           (let [last-episode (db/latest-session-episode)]
             (when-let [prev-context (or (:context last-episode)
                                         (:summary last-episode))]
               (log/info "Warm start from previous session:" prev-context)
               (update-session-wm! sid #(assoc % :prev-topics prev-context))
               (let [terms (extract-search-terms sid prev-context)
                     results (search-knowledge sid terms prev-context)]
                 (when results
                   (let [matched-eids (mapv :eid (:nodes results))
                         expanded (when (seq matched-eids) (expand-graph matched-eids))]
                     (merge-results! sid results expanded)))))))
         (get-wm sid))))))

(defn get-wm
  "Return current working memory state."
  ([]
   (session-wm nil))
  ([session-id]
   (session-wm session-id)))

(defn autonomy-state
  ([]
   (autonomy-state nil))
  ([session-id]
   (get-in (session-wm session-id) [:autonomy-state])))

(defn set-autonomy-state!
  [session-id autonomy-state]
  (run-session-op! session-id
    (fn [sid]
      (update-session-wm! sid
        #(assoc % :autonomy-state autonomy-state)))))

(defn clear-autonomy-state!
  [session-id]
  (run-session-op! session-id
    (fn [sid]
      (update-session-wm! sid
        #(assoc % :autonomy-state nil)))))

(defn clear-wm!
  "Clear working memory (on session end)."
  ([]
   (reset! wm-atom {}))
  ([session-id]
   (run-session-op! session-id
     (fn [sid]
       (when sid
         (swap! wm-atom dissoc sid))))))

;; ============================================================================
;; Pinning
;; ============================================================================

(defn pin!
  ([node-eid]
   (pin! nil node-eid))
  ([session-id node-eid]
   (run-session-op! session-id
     (fn [sid]
       (update-session-wm! sid
         #(assoc-in % [:slots node-eid :pinned?] true))))))

(defn unpin!
  ([node-eid]
   (unpin! nil node-eid))
  ([session-id node-eid]
   (run-session-op! session-id
     (fn [sid]
       (update-session-wm! sid
         #(assoc-in % [:slots node-eid :pinned?] false))))))

;; ============================================================================
;; Export for context assembly
;; ============================================================================

(defn wm->context
  "Export working memory as context data for prompt assembly."
  ([]
   (wm->context nil))
  ([session-id]
   (when-let [wm (session-wm session-id)]
    {:topics       (:topics wm)
     :autonomy     (:autonomy-state wm)
     :entities     (into []
                         (map (fn [{:keys [name type facts edges properties relevance pinned?]}]
                                {:name       name
                                 :type       type
                                 :facts      facts
                                 :edges      edges
                                 :properties properties
                                 :relevance  relevance
                                 :pinned?    pinned?}))
                         (sort-by :relevance > (vals (:slots wm))))
     :episodes     (into []
                         (map (fn [{:keys [summary timestamp relevance]}]
                                {:summary   summary
                                 :timestamp timestamp
                                 :relevance relevance}))
                         (sort-by :relevance > (:episode-refs wm)))
     :local-docs   (into []
                         (map (fn [{:keys [doc-id name media-type summary preview matched-chunks relevance]}]
                                {:id             doc-id
                                 :name           name
                                 :media-type     media-type
                                 :summary        summary
                                 :preview        preview
                                 :matched-chunks matched-chunks
                                 :relevance      relevance}))
                         (sort-by :relevance > (:local-doc-refs wm)))
     :turn-count   (:turn-count wm)})))

;; ============================================================================
;; Snapshot (crash-recovery)
;; ============================================================================

(defn snapshot!
  "Persist current WM state to DB."
  ([]
   (snapshot! nil))
  ([session-id]
   (run-session-op! session-id
     (fn [sid]
       (when-let [wm (session-wm sid)]
         (try
           (db/save-wm-snapshot! wm)
           (catch Exception e
             (log/warn "Failed to save WM snapshot:" (.getMessage e)))))))))
