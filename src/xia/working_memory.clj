(ns xia.working-memory
  "Working memory — the active, curated context for the current conversation.

   Analogous to the prefrontal cortex: holds a relevance-filtered subset of
   the entire memory system. Can pull in facts from months ago if relevant now.
   Tracks what the agent is 'thinking about.'

  Lifecycle:
   1. Created on session start (warm start from previous session's topics)
   2. Updated each turn before the LLM call (retrieval pipeline)
   3. Debounced snapshots persist active-session state during long-running work
   4. Cleared on session end (topic summary feeds into episodic recording)

  Runtime: in-memory atom with debounced persistence for crash recovery."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.async :as async]
            [xia.autonomous :as autonomous]
            [xia.config :as cfg]
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
(def ^:dynamic *suppress-snapshot-scheduling?* false)

(defonce ^:private wm-atom (atom {}))
(defonce ^:private runtime-state-atom (atom {:generation 0
                                             :reaper-future nil}))
(defonce ^:private runtime-lock (Object.))
(def ^:private session-op-lock-count 512)
(defonce ^:private session-op-locks
  (vec (repeatedly session-op-lock-count #(Object.))))

(declare get-wm warm-start! snapshot! snapshot-interval-ms snapshot-debounce-ms)

(def ^:private default-snapshot-interval-ms 30000)
(def ^:private default-snapshot-debounce-ms 2000)
(def ^:private default-reaper-interval-ms (* 5 60 1000))
(def ^:private default-reap-idle-after-ms (* 2 60 60 1000))
(def ^:private live-task-states
  #{:running :waiting_input :waiting_approval :paused :resumable})

(defn- current-time-ms []
  (System/currentTimeMillis))

(defn- sleep-ms!
  [delay-ms]
  (Thread/sleep (long delay-ms)))

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
  (run-session-op! session-id
    (fn [sid]
      (when sid
        (let [states @wm-atom]
          (when-let [wm (get states sid)]
            (let [now        (current-time-ms)
                  schedule?  (atom false)
                  updated    (cond-> (-> (f wm)
                                         (assoc :last-active-at-ms now)
                                         (assoc :prompt-cache-version
                                                (inc (long (or (:prompt-cache-version wm) 0)))))
                               (not *suppress-snapshot-scheduling?*)
                               (-> (assoc :last-dirty-at-ms now)
                                   ((fn [next-wm]
                                      (if (:snapshot-task-scheduled? next-wm)
                                        next-wm
                                        (do
                                          (reset! schedule? true)
                                          (assoc next-wm :snapshot-task-scheduled? true)))))))]
              (reset! wm-atom (assoc states sid updated))
              (when @schedule?
                (when-not (async/submit-background!
                           "wm-snapshot"
                           #(loop []
                              (let [wm-state (get-wm sid)]
                                (when wm-state
                                  (let [dirty-at         (long (or (:last-dirty-at-ms wm-state) 0))
                                        last-snapshot-at (long (or (:last-snapshot-at-ms wm-state) 0))
                                        debounce-at      (+ dirty-at (snapshot-debounce-ms))
                                        interval-at      (if (pos? last-snapshot-at)
                                                           (+ last-snapshot-at (snapshot-interval-ms))
                                                           Long/MAX_VALUE)
                                        due-at           (long (min debounce-at interval-at))
                                        now*             (current-time-ms)
                                        delay-ms         (max 0 (- due-at now*))]
                                    (if (or (zero? dirty-at) (zero? delay-ms))
                                      (snapshot! sid)
                                      (do
                                        (sleep-ms! delay-ms)
                                        (recur))))))))
                  (run-session-op! sid
                    (fn [sid*]
                      (swap! wm-atom update sid*
                             #(when %
                                (assoc % :snapshot-task-scheduled? false)))))))
              updated)))))))

(defn- update-session-wm-bookkeeping!
  [session-id f]
  (run-session-op! session-id
    (fn [sid]
      (when sid
        (let [states @wm-atom]
          (when-let [wm (get states sid)]
            (let [updated (f wm)]
              (reset! wm-atom (assoc states sid updated))
              updated)))))))

(def ^:private default-config
  {:max-slots             15
   :max-episode-refs      5
   :max-local-doc-refs    4
   :decay-factor          0.85
   :eviction-threshold    0.1
   :topic-update-interval 5})   ; update topic summary every N turns

(defn- snapshot-interval-ms
  []
  (cfg/positive-long :wm/snapshot-interval-ms
                     default-snapshot-interval-ms))

(defn- snapshot-debounce-ms
  []
  (cfg/positive-long :wm/snapshot-debounce-ms
                     default-snapshot-debounce-ms))

(defn- reaper-interval-ms
  []
  (cfg/positive-long :wm/reaper-interval-ms
                     default-reaper-interval-ms))

(defn- reap-idle-after-ms
  []
  (cfg/positive-long :wm/reap-idle-after-ms
                     default-reap-idle-after-ms))

(def ^:private fact-utility-batch-size 20)
(def ^:private fact-utility-heuristic-utility 1.0)
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
                              [])))
         search-steps   [[:nodes
                          #(memory/search-nodes semantic-query :fts-query fts-query :top 10)]
                         [:facts
                          #(memory/search-facts semantic-query :fts-query fts-query :top 15)]
                         [:episodes
                          #(memory/search-episodes semantic-query :fts-query fts-query :top 5)]
                         [:local-docs
                          #(memory/search-local-docs local-doc-session-id
                                                     semantic-query
                                                     :fts-query fts-query
                                                     :top 4)]]]
     (when (or (not (str/blank? fts-query))
               (not (str/blank? semantic-query)))
       (let [search-futures (mapv (fn [[step f]]
                                    [step (async/submit-parallel!
                                           (str "wm-search:" (name step))
                                           #(safe-search step f))])
                                  search-steps)]
         (into (array-map)
               (map (fn [[step result-future]]
                      [step @result-future]))
               search-futures))))))

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
  ([slots node-eid name type relevance turn-count]
   (merge-node-into-slot slots node-eid name type relevance turn-count nil))
  ([slots node-eid name type relevance turn-count node-data]
   (merge-node-into-slot slots node-eid name type relevance turn-count node-data nil))
  ([slots node-eid name type relevance turn-count node-data
   {:keys [boost-existing? refresh-existing?]
     :or {boost-existing? true
          refresh-existing? false}}]
   (if-let [existing (get slots node-eid)]
     (let [{:keys [facts edges properties]} node-data
           refreshed (if refresh-existing?
                       (cond-> (assoc existing
                                      :name name
                                      :type type)
                         (some? node-data)
                         (assoc :facts facts
                                :edges edges
                                :properties properties))
                       existing)
           updated   (if boost-existing?
                       (assoc refreshed
                              :relevance (boost-relevance (:relevance refreshed)
                                                          relevance))
                       refreshed)]
       (assoc slots node-eid updated))
     ;; New slot
     (let [{:keys [facts edges properties]} (or node-data
                                                {:facts      (memory/node-facts node-eid)
                                                 :edges      (memory/node-edges node-eid)
                                                 :properties (memory/node-properties node-eid)})]
       (assoc slots node-eid
              {:node-eid   node-eid
               :name       name
               :type       type
               :facts      facts
               :edges      edges
               :properties properties
               :relevance  relevance
               :pinned?    false
               :added-turn turn-count})))))

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

(defn- preload-merge-node-data
  [current-slots search-results expanded-nodes & {:keys [refresh-existing?]
                                                  :or {refresh-existing? false}}]
  (let [existing-eids     (set (keys (or current-slots {})))
        include-node?     (fn [eid]
                            (or refresh-existing?
                                (not (contains? existing-eids eid))))
        direct-nodes      (into {}
                                (keep (fn [{:keys [eid name type]}]
                                        (when (include-node? eid)
                                          [eid {:name name
                                                :type type}])))
                                (:nodes search-results))
        fact-node-eids    (->> (:facts search-results)
                               (map :node-eid)
                               distinct
                               (remove #(or (and (not refresh-existing?)
                                                 (contains? existing-eids %))
                                            (contains? direct-nodes %))))
        fact-nodes        (into {}
                                (map (fn [node-eid]
                                       [node-eid {}]))
                                fact-node-eids)
        expanded-node-map (into {}
                                (keep (fn [[eid {:keys [name type]}]]
                                        (when-not (or (and (not refresh-existing?)
                                                           (contains? existing-eids eid))
                                                      (contains? direct-nodes eid)
                                                      (contains? fact-nodes eid))
                                          [eid {:name name
                                                :type type}])))
                                expanded-nodes)
        node-metadata     (merge direct-nodes fact-nodes expanded-node-map)
        hydrated-by-eid   (memory/node-data-by-eids (keys node-metadata))]
    (into {}
          (map (fn [[node-eid {:keys [name type]}]]
                 (let [{hydrated-name :name
                        hydrated-type :type
                        facts :facts
                        edges :edges
                        properties :properties} (get hydrated-by-eid node-eid)]
                   [node-eid {:name (or name hydrated-name)
                              :type (or type hydrated-type)
                              :slot-data {:facts facts
                                          :edges edges
                                          :properties properties}}])))
          node-metadata)))

(defn- merge-results!
  "Merge search + expansion results into working memory."
  ([session-id search-results expanded-nodes]
   (merge-results! session-id search-results expanded-nodes nil))
  ([session-id search-results expanded-nodes {:keys [boost-existing? refresh-existing?]
                                              :or {boost-existing? true
                                                   refresh-existing? false}}]
  (let [merge-opts          {:boost-existing? boost-existing?
                             :refresh-existing? refresh-existing?}
        preloaded-node-data (preload-merge-node-data (some-> (session-wm session-id) :slots)
                                                     search-results
                                                     expanded-nodes
                                                     :refresh-existing? refresh-existing?)]
    (update-session-wm! session-id
      (fn [wm]
        (let [turn (:turn-count wm)
            ;; Merge direct search hits at high relevance
            slots-with-nodes
            (reduce (fn [slots {:keys [eid name type]}]
                      (merge-node-into-slot slots
                                            eid
                                            name
                                            type
                                            0.8
                                            turn
                                            (some-> (get preloaded-node-data eid) :slot-data)
                                            merge-opts))
                    (:slots wm)
                    (:nodes search-results))
            ;; Merge nodes found via fact search
            slots-with-fact-nodes
            (reduce (fn [slots {:keys [node-eid]}]
	                      (let [{:keys [name type slot-data]} (get preloaded-node-data node-eid)
                                in-slots? (contains? slots node-eid)]
                            (if (and in-slots?
                                     (not refresh-existing?)
                                     boost-existing?)
	                          ;; Boost existing slot
	                          (update-in slots [node-eid :relevance]
	                                     #(boost-relevance % 0.3))
                              (merge-node-into-slot
                               slots node-eid
                               (or name (get-in slots [node-eid :name]))
                               (or type (get-in slots [node-eid :type]))
                               0.6 turn
                               slot-data
                               merge-opts))))
                    slots-with-nodes
                    (:facts search-results))
            ;; Merge expanded (one-hop) nodes at lower relevance
            slots-with-expanded
            (reduce (fn [slots [eid {:keys [name type]}]]
                      (if (and (contains? slots eid)
                               (not refresh-existing?))
                        slots ; already in WM from direct search
                        (merge-node-into-slot slots
                                              eid
                                              name
                                              type
                                              0.3
                                              turn
                                              (some-> (get preloaded-node-data eid) :slot-data)
                                              merge-opts)))
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
               :local-doc-refs merged-doc-refs)))))))

(defn- decay-slot-map
  ([slots factor]
   (decay-slot-map slots factor nil))
  ([slots factor current-turn]
   (reduce-kv
     (fn [acc eid slot]
       (assoc acc eid
              (if (or (:pinned? slot)
                      (and (some? current-turn)
                           (= (long current-turn)
                              (long (or (:added-turn slot) -1)))))
                slot
                (update slot :relevance #(* (double %) (double factor))))))
     {}
     slots)))

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
           (let [factor       (get-in wm [:config :decay-factor])
                 current-turn (:turn-count wm)]
             (update wm :slots
                     #(decay-slot-map % factor current-turn)))))))))

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
    (async/submit-background!
     "wm-topic-update"
     #(try
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

(defn- fact-utility-text
  [text]
  (-> (or text "")
      str/lower-case
      (str/replace #"[^\w'-]+" " ")
      str/trim))

(defn- fact-utility-terms
  [text]
  (->> (str/split (fact-utility-text text) #"\s+")
       (remove str/blank?)
       (remove #(< (count %) 3))
       (remove stopwords)
       distinct
       vec))

(defn- fact-clearly-used?
  [fact assistant-response]
  (let [fact-text      (fact-utility-text (:content fact))
        response-text  (fact-utility-text assistant-response)
        exact-match?   (and (>= (count fact-text) 8)
                            (str/includes? response-text fact-text))
        fact-terms     (fact-utility-terms (:content fact))
        response-terms (set (fact-utility-terms assistant-response))
        overlap        (count (filter response-terms fact-terms))
        term-count     (count fact-terms)
        overlap-ratio  (if (pos? term-count)
                         (/ (double overlap) (double term-count))
                         0.0)]
    (or exact-match?
        (and (= 1 term-count)
             (= 1 overlap))
        (and (>= term-count 2)
             (>= overlap 2)
             (>= overlap-ratio 0.75)))))

(defn apply-fact-utility-heuristic!
  [fact-eids assistant-response]
  (let [facts     (memory/facts-by-eids fact-eids)
        response* (some-> assistant-response str str/trim)]
    (if-not (seq response*)
      0
      (reduce (fn [updated {:keys [eid] :as fact}]
                (if (fact-clearly-used? fact response*)
                  (do
                    (memory/update-fact-utility! eid fact-utility-heuristic-utility)
                    (inc updated))
                  updated))
              0
              facts))))

(defn apply-explicit-fact-utility!
  [fact-eids]
  (reduce (fn [updated fact-eid]
            (if fact-eid
              (do
                (memory/update-fact-utility! fact-eid fact-utility-heuristic-utility)
                (inc updated))
              updated))
          0
          (distinct fact-eids)))

(defn- rate-fact-utility-batch
  [entries]
  (let [user-msg  (str "Fact-use review items:\n\n"
                       (transduce
                         (map-indexed
                           (fn [idx {:keys [fact user-message assistant-response explicitly-used?]}]
                             (let [{:keys [content confidence utility]} fact]
                               (str "Fact " idx
                                    "\nUser message: " (or user-message "")
                                    "\nAssistant response: " (or assistant-response "")
                                    "\nContent: " content
                                    "\nExplicitly used: " (if explicitly-used? "yes" "no")
                                    "\nConfidence: " (format "%.3f" (double (or confidence 0.0)))
                                    "\nUtility: " (format "%.3f" (double (or utility 0.5)))))))
                         (completing
                           (fn [^StringBuilder sb fact-text]
                             (when (pos? (.length sb))
                               (.append sb "\n\n---\n\n"))
                             (.append sb ^String fact-text))
                           (fn [^StringBuilder sb]
                             (.toString sb)))
                         (StringBuilder.)
                         entries))
        defaults  (fact-utility-defaults (map :fact entries))
        explicit  (reduce (fn [scores {:keys [fact explicitly-used?]}]
                            (if explicitly-used?
                              (update scores
                                      (:eid fact)
                                      (fn [existing]
                                        (if (nil? existing)
                                          fact-utility-heuristic-utility
                                          (max (double existing)
                                               fact-utility-heuristic-utility))))
                              scores))
                          {}
                          entries)]
    (if-not (seq entries)
      {}
      (try
        (let [response (llm/chat-simple
                         [{:role "system" :content fact-utility-prompt}
                          {:role "user" :content user-msg}]
                         :workload :fact-utility)
              parsed   (json/read-json response)]
          (let [observed
                (reduce
                  (fn [scores entry]
                    (let [idx (get entry "index")]
                      (if (and (number? idx)
                               (<= 0 (long idx))
                               (< (long idx) (count entries)))
                        (let [fact-eid (:eid (:fact (nth entries (long idx))))
                              utility  (normalize-fact-utility (get entry "utility"))]
                          (update scores fact-eid
                                  (fn [existing]
                                    (if (nil? existing)
                                      utility
                                      (max (double existing) utility)))))
                        scores)))
                  {}
                  (get parsed "facts" []))]
            (merge defaults observed explicit)))
        (catch Exception e
          (log/warn "Failed to rate fact utility batch:" (.getMessage e))
          (merge defaults explicit))))))

(defn review-fact-utility-observations!
  [observations]
  (let [facts-by-eid (into {}
                           (map (juxt :eid identity))
                           (memory/facts-by-eids (keep :fact-eid observations)))
        entries      (into []
                           (keep (fn [{:keys [fact-eid user-message assistant-response explicitly-used?]}]
                                   (when-let [fact (get facts-by-eid fact-eid)]
                                     {:fact fact
                                      :user-message user-message
                                      :assistant-response assistant-response
                                      :explicitly-used? explicitly-used?})))
                           observations)]
    (doseq [batch (partition-all fact-utility-batch-size entries)]
      (let [ratings (rate-fact-utility-batch (vec batch))]
        (doseq [[fact-eid utility] ratings]
          (memory/update-fact-utility! fact-eid utility))))
    (count entries)))

(defn review-fact-utility!
  [fact-eids user-message assistant-response]
  (review-fact-utility-observations!
   (into []
         (map (fn [fact-eid]
                {:fact-eid fact-eid
                 :user-message user-message
                 :assistant-response assistant-response}))
         fact-eids)))

(defn- topic-shift-terms
  [value]
  (cond
    (string? value)
    (set (extract-search-terms value))

    (sequential? value)
    (->> value
         (keep #(some-> % str str/lower-case str/trim))
         (remove str/blank?)
         (remove #(< (count %) 2))
         (remove stopwords)
         set)

    :else
    #{}))

(defn- normalized-topic-shift-baseline
  [search-terms]
  (some->> (topic-shift-terms search-terms)
           seq
           sort
           vec))

(defn- topic-shift-baseline-terms
  [wm]
  (let [baseline-terms (some-> (:topic-shift-baseline wm) topic-shift-terms)
        topic-terms    (some-> (:topics wm) topic-shift-terms)]
    (cond
      (seq baseline-terms) baseline-terms
      (seq topic-terms) topic-terms
      :else nil)))

(defn detect-topic-shift?
  "Compare current search terms with the latest lexical topic baseline.
   Returns true if topics have shifted significantly."
  ([search-terms]
   (detect-topic-shift? nil search-terms))
  ([session-id search-terms]
   (let [wm (session-wm session-id)
         prev-words (topic-shift-baseline-terms wm)]
     (when (and (seq prev-words) (seq search-terms))
       (let [prev-words       prev-words
             current-words    (topic-shift-terms search-terms)
             overlap          (count (clojure.set/intersection prev-words current-words))
             prev-count       (max 1 (count prev-words))
             current-count    (max 1 (count current-words))
             max-count        (max prev-count current-count)
             union-count      (max 1 (count (clojure.set/union prev-words current-words)))
             max-coverage     (/ (double overlap) (double max-count))
             jaccard          (/ (double overlap) (double union-count))]
         (and (seq prev-words)
              (seq current-words)
              (or (zero? overlap)
                  (and (< jaccard 0.15)
                       (< max-coverage 0.34)))))))))

(defn- should-auto-segment?
  [session-id search-terms]
  (let [wm       (session-wm session-id)
        baseline (normalized-topic-shift-baseline search-terms)
        shift?   (detect-topic-shift? session-id baseline)
        pending? (some? (:pending-topic-shift wm))]
    (cond
      (not shift?)
      (do
        (update-session-wm! session-id
                            #(cond-> (assoc % :pending-topic-shift nil)
                               baseline
                               (assoc :topic-shift-baseline baseline)))
        false)

      pending?
      (do
        (update-session-wm! session-id #(assoc % :pending-topic-shift nil))
        true)

      :else
      (do
        (update-session-wm! session-id
          #(assoc % :pending-topic-shift {:search-terms (vec search-terms)
                                          :turn-count (:turn-count %)}))
        false))))

;; ============================================================================
;; Auto-segmentation
;; ============================================================================

(defn auto-segment!
  "Close the current episode and start a new one within the same session.
   Called when a significant topic shift is detected."
  [session-id channel search-terms]
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
                   :prev-topics         (:topics wm)
                   :topics              nil
                   :topic-shift-baseline (normalized-topic-shift-baseline search-terms)
                   :pending-topic-shift nil
                   :topic-turn          (:turn-count wm))))))))

;; ============================================================================
;; Per-turn Update (orchestrator)
;; ============================================================================

(defn- run-wm-retrieval!
  [user-message session-id channel {:keys [resource-session-id
                                           increment-turn?
                                           decay-slots?
                                           topic-maintenance?
                                           boost-existing?
                                           refresh-existing?]
                                    :or {increment-turn? true
                                         decay-slots? true
                                         topic-maintenance? true
                                         boost-existing? true
                                         refresh-existing? false}}]
  (run-session-op! session-id
    (fn [sid]
      (when (session-wm sid)
        (when increment-turn?
          (update-session-wm! sid #(update % :turn-count inc)))
        (let [terms   (extract-search-terms sid user-message)
              results (search-knowledge sid terms user-message resource-session-id)]
          (when results
            (let [matched-eids (mapv :eid (:nodes results))
                  expanded     (when (seq matched-eids)
                                 (expand-graph matched-eids))]
              (merge-results! sid
                              results
                              expanded
                              {:boost-existing? boost-existing?
                               :refresh-existing? refresh-existing?})
              (when decay-slots?
                (update-session-wm! sid
                  (fn [wm]
                    (let [factor       (get-in wm [:config :decay-factor])
                          threshold    (get-in wm [:config :eviction-threshold])
                          max-slots    (get-in wm [:config :max-slots])
                          current-turn (:turn-count wm)
                          decayed      (decay-slot-map (:slots wm) factor current-turn)
                          filtered     (prune-slot-map decayed threshold max-slots)]
                      (assoc wm :slots filtered)))))))
          (when topic-maintenance?
            (let [wm (session-wm sid)]
              (when (and wm
                         (seq terms))
                (if (> (long (:turn-count wm)) 3)
                  (when (should-auto-segment? sid terms)
                    (auto-segment! sid channel terms))
                  (update-session-wm! sid
                                      #(cond-> %
                                         (normalized-topic-shift-baseline terms)
                                         (assoc :topic-shift-baseline
                                                (normalized-topic-shift-baseline terms))))))
              (when wm
                (let [interval    (get-in wm [:config :topic-update-interval])
                      turns-since (- (long (:turn-count wm)) (long (:topic-turn wm)))]
                  (when (and interval (>= turns-since (long interval)))
                    (schedule-topic-update! sid))))))
          (get-wm sid))))))

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
  ([user-message session-id channel opts]
   (run-wm-retrieval! user-message session-id channel opts)))

(defn refresh-wm!
  "Refresh retrieval-backed WM state during a running turn without creating a
   new conversational turn. This re-runs retrieval against the current query
   so memory writes from earlier tool activity become visible in later
   iterations, while avoiding turn-count inflation, decay, and repeated
   relevance boosting for already-present slots."
  ([user-message session-id channel]
   (refresh-wm! user-message session-id channel nil))
  ([user-message session-id channel opts]
   (run-wm-retrieval! user-message
                      session-id
                      channel
                      (merge {:increment-turn? false
                              :decay-slots? false
                              :topic-maintenance? false
                              :boost-existing? false
                              :refresh-existing? true}
                             opts))))

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
                :prompt-cache-version 0
                :last-active-at-ms (current-time-ms)
                :last-snapshot-at-ms 0
                :last-dirty-at-ms nil
                :snapshot-task-scheduled? false
                :topic-shift-baseline nil
                :pending-topic-shift nil
                :autonomy-state  nil
                :turn-count      0
                :topic-turn      0
                :slots           {}
                :episode-refs    []
                :local-doc-refs  []
                :config          default-config}]
        (swap! wm-atom assoc sid wm)
        wm))))

(defn- restore-snapshot!
  [session-id]
  (run-session-op! session-id
    (fn [sid]
      (when-let [snapshot (db/load-wm-snapshot sid)]
        (binding [*suppress-snapshot-scheduling?* true]
          (update-session-wm! sid
            (fn [wm]
              (cond-> wm
                (contains? snapshot :topics)
                (assoc :topics (:topics snapshot))

                (contains? snapshot :autonomy-state)
                (assoc :autonomy-state (some-> (:autonomy-state snapshot)
                                               autonomous/normalize-state))

                (contains? snapshot :slots)
                (assoc :slots (or (:slots snapshot) {}))

                (contains? snapshot :episode-refs)
                (assoc :episode-refs (vec (or (:episode-refs snapshot) [])))

                (contains? snapshot :local-doc-refs)
                (assoc :local-doc-refs (vec (or (:local-doc-refs snapshot) [])))

                :always
                (assoc :last-snapshot-at-ms (some-> (:updated-at snapshot)
                                                    ^java.util.Date
                                                    .getTime
                                                    long
                                                    (or 0))
                       :last-dirty-at-ms nil
                       :snapshot-task-scheduled? false)))))
        (get-wm sid)))))

(defn ensure-wm!
  "Return working memory for the session, creating and warm-starting it if needed."
  [session-id]
  (run-session-op! session-id
    (fn [sid]
      (or (get-wm sid)
          (do
            (create-wm! sid)
            (when-not (restore-snapshot! sid)
              (warm-start! sid))
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
        #(assoc % :autonomy-state (some-> autonomy-state
                                          autonomous/normalize-state))))))

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

(defn prompt-cache-version
  ([]
   (prompt-cache-version nil))
  ([session-id]
   (if-let [wm (session-wm session-id)]
     (long (or (:prompt-cache-version wm) 0))
     ::no-working-memory)))

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
         (let [saved-at-ms (current-time-ms)]
         (try
           (db/save-wm-snapshot! wm)
           (update-session-wm-bookkeeping! sid
             #(assoc %
                     :last-snapshot-at-ms saved-at-ms
                     :last-dirty-at-ms nil
                     :snapshot-task-scheduled? false))
           (catch Exception e
             (update-session-wm-bookkeeping! sid
               #(assoc % :snapshot-task-scheduled? false))
             (log/warn "Failed to save WM snapshot:" (.getMessage e))))))))))

(defn snapshot-all!
  "Persist every currently live working-memory session to DB."
  []
  (let [session-ids (vec (keys @wm-atom))]
    (doseq [session-id session-ids]
      (snapshot! session-id))
    (count session-ids)))

(defn- task-live?
  [task]
  (contains? live-task-states
             (or (get-in task [:meta :runtime :state])
                 (:state task))))

(defn- session-protected-from-reap?
  [session-id]
  (try
    (when-let [task (db/current-session-task session-id)]
      (task-live? task))
    (catch Exception e
      (log/debug e "Failed to inspect session task during WM reap" session-id)
      false)))

(defn- stale-session-wm?
  [wm now-ms]
  (let [last-active-at-ms (long (or (:last-active-at-ms wm) 0))]
    (and (pos? last-active-at-ms)
         (>= (- now-ms last-active-at-ms)
             (reap-idle-after-ms)))))

(defn- maybe-reap-stale-session-wm!
  [session-id now-ms]
  (run-session-op! session-id
    (fn [sid]
      (when-let [wm (session-wm sid)]
        (when (and (stale-session-wm? wm now-ms)
                   (not (session-protected-from-reap? sid)))
          (let [dirty? (some? (:last-dirty-at-ms wm))]
            (when dirty?
              (snapshot! sid))
            (let [wm* (session-wm sid)]
              (cond
                (nil? wm*)
                false

                (some? (:last-dirty-at-ms wm*))
                (do
                  (log/warn "Skipping stale WM reap because snapshot did not complete"
                            {:session-id sid})
                  false)

                :else
                (let [idle-ms (- now-ms (long (or (:last-active-at-ms wm*) 0)))]
                  (clear-wm! sid)
                  (log/info "Reaped stale working memory"
                            {:session-id sid
                             :idle-ms idle-ms
                             :dirty? dirty?})
                  true)))))))))

(defn- reap-stale-working-memories!
  []
  (let [now-ms      (current-time-ms)
        session-ids (vec (keys @wm-atom))]
    (reduce (fn [count* session-id]
              (if (true? (maybe-reap-stale-session-wm! session-id now-ms))
                (inc count*)
                count*))
            0
            session-ids)))

(defn- run-reaper-loop!
  [generation]
  (loop []
    (when (= generation (:generation @runtime-state-atom))
      (let [continue?
            (try
              (sleep-ms! (reaper-interval-ms))
              (when (= generation (:generation @runtime-state-atom))
                (let [reaped (reap-stale-working-memories!)]
                  (when (pos? reaped)
                    (log/info "Reaped" reaped "stale working-memory session(s)"))))
              true
              (catch InterruptedException _
                false)
              (catch Exception e
                (log/error e "Working-memory reaper failed")
                true))]
        (when continue?
          (recur))))))

(defn reset-runtime!
  []
  (locking runtime-lock
    (when-let [future (:reaper-future @runtime-state-atom)]
      (.cancel future true))
    (let [generation (inc (long (:generation @runtime-state-atom)))
          future     (async/submit-background! "wm-reaper"
                                               #(run-reaper-loop! generation))]
      (reset! wm-atom {})
      (reset! runtime-state-atom {:generation generation
                                  :reaper-future future})))
  nil)

(defn prepare-shutdown!
  []
  (locking runtime-lock
    (when-let [future (:reaper-future @runtime-state-atom)]
      (.cancel future true))
    (swap! runtime-state-atom
           (fn [state]
             {:generation (inc (long (:generation state)))
              :reaper-future nil})))
  nil)
