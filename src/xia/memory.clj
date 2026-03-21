(ns xia.memory
  "Memory system — modeled after human memory.

   Components:
   - Episodic memory: records events, conversations, context (like hippocampal recording)
   - Knowledge graph: long-term structured memory — entities, relations, facts
   - Hippocampus: process that consolidates episodes into the knowledge graph

   Flow:
   conversation → episodic recording → hippocampus consolidation → knowledge graph
                                                                  ↓
                                                        recalled into context"
  (:require [clojure.string :as str]
            [datalevin.built-ins :as builtins]
            [datalevin.core :as d]
            [xia.config :as cfg]
            [xia.db :as db]))

;; ============================================================================
;; Episodic Memory
;; ============================================================================
;; Records what happened — conversation summaries, events, context.
;; Each episode captures a bounded interaction with enough context
;; to reconstruct what occurred and why.

(defn record-episode!
  "Record an episode — a bounded event or interaction."
  [{:keys [type summary context participants channel session-id importance]}]
  (db/transact!
    [(cond-> {:episode/id         (random-uuid)
              :episode/type       (or type :conversation)
              :episode/summary    (or summary "")
              :episode/timestamp  (java.util.Date.)
              :episode/processed? false}
       context      (assoc :episode/context context)
       participants (assoc :episode/participants participants)
       channel      (assoc :episode/channel (name channel))
       importance   (assoc :episode/importance (float importance))
       session-id   (assoc :episode/session-id (str session-id)))]))

(defn- empty->nil [s] (when-not (= "" s) s))

(def ^:private ms-per-day (* 24 60 60 1000))

(def ^:private default-episode-retention-config
  {:full-resolution-ms    (* 182 24 60 60 1000)
   :default-importance    0.5
   :retained-decayed-count 8
   :decay-half-life-ms    (* 365 24 60 60 1000)})

(def ^:private default-fact-utility 0.5)
(def ^:private fact-confidence-weight 0.8)
(def ^:private fact-utility-weight 0.2)

(defn- long-max
  ^long [^long a ^long b]
  (if (> a b) a b))

(defn- long-min
  ^long [^long a ^long b]
  (if (< a b) a b))

(defn- double-max
  ^double [^double a ^double b]
  (if (> a b) a b))

(defn- double-min
  ^double [^double a ^double b]
  (if (< a b) a b))

(defn- indexed-rank
  ^long [idx]
  (inc (long idx)))

(def ^:private episode-retention-config-keys
  {:full-resolution-ms    :memory/episode-full-resolution-ms
   :decay-half-life-ms    :memory/episode-decay-half-life-ms
   :retained-decayed-count :memory/episode-retained-decayed-count})

(defn episode-retention-settings
  "Return the effective episode retention settings."
  []
  (assoc default-episode-retention-config
         :full-resolution-ms
         (cfg/positive-long (:full-resolution-ms episode-retention-config-keys)
                            (:full-resolution-ms default-episode-retention-config))
         :decay-half-life-ms
         (cfg/positive-long (:decay-half-life-ms episode-retention-config-keys)
                            (:decay-half-life-ms default-episode-retention-config))
         :retained-decayed-count
         (cfg/positive-long (:retained-decayed-count episode-retention-config-keys)
                            (:retained-decayed-count default-episode-retention-config))))

(defn- normalize-importance
  ^double
  [importance]
  (let [value (double (or importance
                          (:default-importance default-episode-retention-config)))]
    (-> value
        (double-max 0.0)
        (double-min 1.0))))

(defn normalize-fact-confidence
  ^double
  [confidence]
  (-> (double (or confidence 0.0))
      (double-max 0.0)
      (double-min 1.0)))

(defn normalize-fact-utility
  ^double
  [utility]
  (-> (double (or utility default-fact-utility))
      (double-max 0.0)
      (double-min 1.0)))

(defn fact-prompt-score
  ^double
  [{:keys [confidence utility]}]
  (+ (* (double fact-confidence-weight)
        (normalize-fact-confidence confidence))
     (* (double fact-utility-weight)
        (normalize-fact-utility utility))))

(defn fact-utility-half-life-factor
  ^double
  [utility]
  (+ 0.5 (normalize-fact-utility utility)))

(defn unprocessed-episodes
  "Get episodes not yet consolidated by the hippocampus."
  []
  (->> (db/q '[:find ?e ?summary ?ctx ?ts ?type ?importance
               :where
               [?e :episode/summary ?summary]
               [?e :episode/timestamp ?ts]
               [?e :episode/type ?type]
               [(get-else $ ?e :episode/context "") ?ctx]
               [(get-else $ ?e :episode/importance 0.5) ?importance]
               [?e :episode/processed? false]])
       (map (fn [[eid summary ctx ts type importance]]
              {:eid        eid
               :summary    summary
               :context    (empty->nil ctx)
               :timestamp  ts
               :type       type
               :importance (double importance)}))
       (sort-by :timestamp)))

(defn mark-episode-processed! [eid]
  (db/transact! [[:db/add eid :episode/processed? true]]))

(defn mark-episode-consolidation-failed!
  [eid error-message]
  (db/transact! [[:db/add eid :episode/processed? true]
                 [:db/add eid :episode/consolidation-error (str error-message)]
                 [:db/add eid :episode/consolidation-failed-at (java.util.Date.)]]))

(defn recent-episodes
  "Get the N most recent episodes."
  [n]
  (->> (db/q '[:find ?e ?summary ?ctx ?ts ?type ?importance
               :where
               [?e :episode/summary ?summary]
               [?e :episode/timestamp ?ts]
               [?e :episode/type ?type]
               [(get-else $ ?e :episode/context "") ?ctx]
               [(get-else $ ?e :episode/importance 0.5) ?importance]])
       (sort-by #(nth % 3) #(compare %2 %1))
       (take n)
       (mapv (fn [[eid summary ctx ts type importance]]
               {:eid        eid
                :summary    summary
                :context    (empty->nil ctx)
                :timestamp  ts
                :type       type
                :importance (double importance)}))))

(defn- processed-episodes-query
  []
  '[:find ?e ?summary ?ctx ?ts ?type ?channel ?session-id ?importance
    :in $ ?cutoff
    :where
    [?e :episode/summary ?summary]
    [?e :episode/timestamp ?ts]
    [(< ?ts ?cutoff)]
    [?e :episode/type ?type]
    [(get-else $ ?e :episode/context "") ?ctx]
    [(get-else $ ?e :episode/channel "") ?channel]
    [(get-else $ ?e :episode/session-id "") ?session-id]
    [(get-else $ ?e :episode/importance 0.5) ?importance]
    [?e :episode/processed? true]])

(defn- processed-episodes
  [^java.util.Date cutoff]
  (->> (db/q (processed-episodes-query) cutoff)
       (mapv (fn [[eid summary ctx ts type channel session-id importance]]
               {:eid        eid
                :summary    summary
                :context    (empty->nil ctx)
                :timestamp  ts
                :type       type
                :channel    (empty->nil channel)
                :session-id (empty->nil session-id)
                :importance (double importance)}))))

(defn- retention-group-key
  [{:keys [session-id channel type]}]
  [(or session-id :global) (or channel :unknown) type])

(defn- episode-age-ms
  ^long
  [^java.util.Date timestamp ^java.util.Date as-of]
  (long-max 0 (- (.getTime as-of) (.getTime timestamp))))

(defn- decayed-episode?
  [episode ^java.util.Date as-of retention-config]
  (> (episode-age-ms (:timestamp episode) as-of)
     (long (or (:full-resolution-ms retention-config) 0))))

(defn- episode-decay-score
  ^double
  [episode ^java.util.Date as-of retention-config]
  (let [importance   (normalize-importance (:importance episode))
        half-life-ms (double (or (:decay-half-life-ms retention-config) 1))
        age-ms       (episode-age-ms (:timestamp episode) as-of)]
    (* importance
       (Math/pow 0.5
                 (/ age-ms
                    (double half-life-ms))))))

(defn- sort-episodes-by-decay-score
  [episodes ^java.util.Date as-of retention-config]
  (sort
    (fn [a b]
      (let [score-a (episode-decay-score a as-of retention-config)
            score-b (episode-decay-score b as-of retention-config)
            importance-a (normalize-importance (:importance a))
            importance-b (normalize-importance (:importance b))
            ts-a (.getTime ^java.util.Date (:timestamp a))
            ts-b (.getTime ^java.util.Date (:timestamp b))]
        (cond
          (not= score-a score-b)
          (compare score-b score-a)

          (not= importance-a importance-b)
          (compare importance-b importance-a)

          :else
          (compare ts-b ts-a))))
    episodes))

(defn- retained-episodes-for-group
  [episodes ^java.util.Date as-of retention-config]
  (let [fresh (remove #(decayed-episode? % as-of retention-config) episodes)
        older (filter #(decayed-episode? % as-of retention-config) episodes)]
    (vec
      (concat
        fresh
        (->> (sort-episodes-by-decay-score older as-of retention-config)
             (take (:retained-decayed-count retention-config)))))))

(defn- prunable-processed-episodes
  [episodes ^java.util.Date as-of retention-config]
  (->> episodes
       (group-by retention-group-key)
       vals
       (mapcat (fn [group]
                 (let [keep-eids (set (map :eid (retained-episodes-for-group group
                                                                             as-of
                                                                             retention-config)))]
                   (remove #(contains? keep-eids (:eid %)) group))))
       vec))

(defn- detach-episode-provenance-tx
  [episode-eid]
  ;; Pruning keeps extracted facts/edges but removes their episode provenance
  ;; before deleting the old episode entity.
  (let [fact-eids (map first (db/q '[:find ?f
                                     :in $ ?episode
                                     :where [?f :kg.fact/source ?episode]]
                                   episode-eid))
        edge-eids (map first (db/q '[:find ?e
                                     :in $ ?episode
                                     :where [?e :kg.edge/source ?episode]]
                                   episode-eid))]
    (vec
      (concat
        (map (fn [fact-eid]
               [:db/retract fact-eid :kg.fact/source episode-eid])
             fact-eids)
        (map (fn [edge-eid]
               [:db/retract edge-eid :kg.edge/source episode-eid])
             edge-eids)
        [[:db/retractEntity episode-eid]]))))

(defn processed-episode-prune-plan
  "Return the prune transaction and affected episodes for processed episodes.

   `exclude-eids` can be used to defer pruning a just-processed episode until the
   next pass, while still pruning older processed episodes atomically with the
   caller's transaction."
  ([] (processed-episode-prune-plan (java.util.Date.) nil))
  ([^java.util.Date as-of] (processed-episode-prune-plan as-of nil))
  ([^java.util.Date as-of {:keys [exclude-eids]}]
   (let [retention-config (episode-retention-settings)
         cutoff-ms        (long-max 0 (- (.getTime as-of)
                                         (long (or (:full-resolution-ms retention-config)
                                                   0))))
         cutoff           (java.util.Date. cutoff-ms)
         excluded         (set exclude-eids)
         to-remove        (->> (prunable-processed-episodes (processed-episodes cutoff)
                                                            as-of
                                                            retention-config)
                               (remove #(contains? excluded (:eid %)))
                               vec)]
     {:to-remove to-remove
      :tx-data   (into [] (mapcat #(detach-episode-provenance-tx (:eid %))
                                  to-remove))})))

(defn prune-processed-episodes!
  "Prune processed episodes with exponential-decay downsampling.

   Keeps all processed episodes for the full-resolution window. After that,
   episodes compete by a decayed importance score and only the top-ranked
   older items are retained per session/channel/type."
  ([] (prune-processed-episodes! (java.util.Date.)))
  ([^java.util.Date as-of]
   (let [{:keys [to-remove tx-data]} (processed-episode-prune-plan as-of)]
     (when (seq tx-data)
       (db/transact! tx-data))
     (count to-remove))))

;; ============================================================================
;; Knowledge Graph (Long-term Memory)
;; ============================================================================
;; Entities and their relationships — the things xia knows about the world.
;; Datalevin's [entity, attribute, value] datoms are already a graph;
;; we model nodes and edges explicitly so the LLM can create/query them.

;; --- Nodes ---

(defn add-node!
  "Add or update a node in the knowledge graph.
   `properties` is a Clojure map stored as idoc for structured queries."
  [{:keys [name type properties]}]
  (let [id (random-uuid)]
    (db/transact!
      [(cond-> {:kg.node/id         id
                :kg.node/name       name
                :kg.node/type       (or type :concept)
                :kg.node/created-at (java.util.Date.)
                :kg.node/updated-at (java.util.Date.)}
         properties (assoc :kg.node/properties properties))])
    id))

(defn find-node
  "Find a node by name (case-insensitive substring match)."
  [name-pattern]
  (let [pattern (some-> name-pattern str/lower-case)]
    (if (str/blank? pattern)
      []
      (->> (db/q '[:find ?e ?name ?type
                   :in $ ?pattern
                   :where
                   [?e :kg.node/name ?name]
                   [(clojure.string/lower-case ?name) ?lower-name]
                   [(clojure.string/includes? ?lower-name ?pattern)]
                   [?e :kg.node/type ?type]]
                 pattern)
           (mapv (fn [[eid name type]]
                   {:eid eid :name name :type type}))))))

(defn get-node [node-eid]
  (into {} (db/entity node-eid)))

;; --- Node properties (idoc) ---

(defn node-properties
  "Get a node's properties map (idoc). Returns nil if no properties set."
  [node-eid]
  (:kg.node/properties (db/entity node-eid)))

(defn set-node-property!
  "Set a property on a node. `path` is a vector of keys, `value` is the new value.
   Creates the properties map if it doesn't exist.

   Examples:
     (set-node-property! eid [:location] \"Seattle\")
     (set-node-property! eid [:work :title] \"Engineer\")"
  [node-eid path value]
  (let [existing (node-properties node-eid)]
    (if existing
      ;; Patch existing properties
      (db/transact! [[:db.fn/patchIdoc node-eid :kg.node/properties
                      [[:set path value]]]])
      ;; Create new properties map
      (db/transact! [[:db/add node-eid :kg.node/properties
                      (assoc-in {} path value)]]))
    (db/transact! [[:db/add node-eid :kg.node/updated-at (java.util.Date.)]])))

(defn remove-node-property!
  "Remove a property from a node.

   Example: (remove-node-property! eid [:location])"
  [node-eid path]
  (when (node-properties node-eid)
    (db/transact! [[:db.fn/patchIdoc node-eid :kg.node/properties
                    [[:unset path]]]])
    (db/transact! [[:db/add node-eid :kg.node/updated-at (java.util.Date.)]])))

(defn query-nodes-by-property
  "Find nodes whose properties match a pattern via idoc-match.

   Examples:
     (query-nodes-by-property {:location \"Seattle\"})
     (query-nodes-by-property {:work {:title \"Engineer\"}})
     (query-nodes-by-property {:* \"Seattle\"})  — value at any depth"
  [pattern]
  (try
    (->> (db/q '[:find ?e ?name ?type
                 :in $ ?q
                 :where
                 [(idoc-match $ :kg.node/properties ?q) [[?e ?a ?v]]]
                 [?e :kg.node/name ?name]
                 [?e :kg.node/type ?type]]
               pattern)
         (mapv (fn [[eid name type]]
                 {:eid eid :name name :type type})))
    (catch Exception _ [])))

;; --- Edges (Relations) ---

(defn add-edge!
  "Add a relationship between two nodes."
  [{:keys [from-eid to-eid type label weight source-eid]}]
  (let [id (random-uuid)]
    (db/transact!
      [(cond-> {:kg.edge/id         id
                :kg.edge/from       from-eid
                :kg.edge/to         to-eid
                :kg.edge/type       (or type :related-to)
                :kg.edge/created-at (java.util.Date.)}
         label      (assoc :kg.edge/label label)
         weight     (assoc :kg.edge/weight weight)
         source-eid (assoc :kg.edge/source source-eid))])
    id))

(defn node-edges
  "Get all edges connected to a node (outgoing and incoming)."
  [node-eid]
  (let [outgoing (db/q '[:find ?edge ?to-name ?type ?label
                          :in $ ?from
                          :where
                          [?edge :kg.edge/from ?from]
                          [?edge :kg.edge/to ?to]
                          [?to :kg.node/name ?to-name]
                          [?edge :kg.edge/type ?type]
                          [(get-else $ ?edge :kg.edge/label "") ?label]]
                       node-eid)
        incoming (db/q '[:find ?edge ?from-name ?type ?label
                          :in $ ?to
                          :where
                          [?edge :kg.edge/to ?to]
                          [?edge :kg.edge/from ?from]
                          [?from :kg.node/name ?from-name]
                          [?edge :kg.edge/type ?type]
                          [(get-else $ ?edge :kg.edge/label "") ?label]]
                       node-eid)]
    {:outgoing (mapv (fn [[_ name type label]]
                       {:target name :type type :label (empty->nil label)})
                     outgoing)
     :incoming (mapv (fn [[_ name type label]]
                       {:source name :type type :label (empty->nil label)})
                     incoming)}))

(defn connected-node-summaries
  "Return one-hop neighboring nodes for the given seed node eids.
   Uses bulk queries so graph expansion does not issue per-node edge lookups
   and per-neighbor entity fetches."
  [node-eids]
  (if-not (seq node-eids)
    {}
    (let [seed-eids (vec (distinct node-eids))
          seed-set  (set seed-eids)
          outgoing  (db/q '[:find ?neighbor ?name ?type
                            :in $ [?seed ...]
                            :where
                            [?edge :kg.edge/from ?seed]
                            [?edge :kg.edge/to ?neighbor]
                            [?neighbor :kg.node/name ?name]
                            [?neighbor :kg.node/type ?type]]
                          seed-eids)
          incoming  (db/q '[:find ?neighbor ?name ?type
                            :in $ [?seed ...]
                            :where
                            [?edge :kg.edge/to ?seed]
                            [?edge :kg.edge/from ?neighbor]
                            [?neighbor :kg.node/name ?name]
                            [?neighbor :kg.node/type ?type]]
                          seed-eids)]
      (reduce (fn [acc [neighbor-eid name type]]
                (if (contains? seed-set neighbor-eid)
                  acc
                  (assoc acc neighbor-eid
                         {:node-eid  neighbor-eid
                          :name      name
                          :type      type
                          :expanded? true})))
              {}
              (concat outgoing incoming)))))

;; --- Facts (atomic knowledge attached to nodes) ---

(defn add-fact!
  "Add a fact about an entity."
  [{:keys [node-eid content confidence utility source-eid]}]
  (let [now (java.util.Date.)]
    (db/transact!
      [(cond-> {:kg.fact/id         (random-uuid)
                :kg.fact/node       node-eid
                :kg.fact/content    content
                :kg.fact/confidence (or confidence 1.0)
                :kg.fact/utility    (float (normalize-fact-utility utility))
                :kg.fact/created-at now
                :kg.fact/updated-at now
                :kg.fact/decayed-at now}
         source-eid (assoc :kg.fact/source source-eid))])))

(defn node-facts
  "Get all facts about a node."
  [node-eid]
  (->> (db/q '[:find ?f ?content ?confidence ?utility ?updated
               :in $ ?node
               :where
               [?f :kg.fact/node ?node]
               [?f :kg.fact/content ?content]
               [?f :kg.fact/confidence ?confidence]
               [(get-else $ ?f :kg.fact/utility 0.5) ?utility]
               [?f :kg.fact/updated-at ?updated]]
             node-eid)
       (sort-by #(nth % 4) #(compare %2 %1))
       (mapv (fn [[eid content confidence utility _]]
               {:eid        eid
                :content    content
                :confidence confidence
                :utility    (double utility)}))))

;; ============================================================================
;; Hybrid Search (FTS + Embedding via Datalevin)
;; ============================================================================
;; FTS keeps exact lexical recall strong, while embedding search improves
;; synonym and intent-level matching over the same source datoms.

(def ^:private rrf-k 60.0)
(def ^:private lexical-rrf-weight 1.0)
(def ^:private semantic-rrf-weight 1.0)
(def ^:private default-candidate-pool-multiplier 4)
(def ^:private minimum-candidate-pool-size 20)

(def ^:private candidate-pool-config-keys
  {:nodes    :memory/search-node-candidate-pool-size
   :facts    :memory/search-fact-candidate-pool-size
   :episodes :memory/search-episode-candidate-pool-size
   :edges    :memory/search-edge-candidate-pool-size
   :local-docs :memory/search-local-doc-candidate-pool-size
   :artifacts :memory/search-artifact-candidate-pool-size})

(defn- default-candidate-pool-size
  ^long [top]
  (long-max (long minimum-candidate-pool-size)
            (* (long default-candidate-pool-multiplier)
               (long top))))

(defn- candidate-pool-size
  [kind top]
  (let [default-size (default-candidate-pool-size top)
        configured   (long (cfg/positive-long (get candidate-pool-config-keys kind)
                                              default-size))]
    (-> configured
        (long-max (long top))
        int)))

(defn- fulltext-domain-hits
  [domain query & {:keys [top] :or {top 10}}]
  (if (str/blank? query)
    []
    (let [dbv  (d/db (db/conn))
          opts {:domains [domain]
                :top top
                :display :refs+scores}]
      (try
        (->> (builtins/fulltext dbv query opts)
             (map-indexed
	               (fn [idx tuple]
	                 (let [[eid attr value score] (vec tuple)]
	                   {:eid       eid
	                    :attr      attr
	                    :value     value
	                    :lex-score (double score)
	                    :lex-rank  (indexed-rank idx)})))
             vec)
        (catch Exception _
          [])))))

(defn- embedding-domain-hits
  [domain query & {:keys [top] :or {top 10}}]
  (if (str/blank? query)
    []
    (let [dbv  (d/db (db/conn))
          opts {:domains [domain]
                :top top
                :display :refs+dists}]
      (try
        (->> (builtins/embedding-neighbors dbv query opts)
             (map-indexed
	               (fn [idx tuple]
	                 (let [[eid attr value distance] (vec tuple)]
	                   {:eid          eid
	                    :attr         attr
	                    :value        value
	                    :sem-distance (double distance)
	                    :sem-rank     (indexed-rank idx)})))
             vec)
        (catch Exception _
          [])))))

(defn- update-best-rank
  ^long
  [current rank]
  (cond
    (nil? rank) (long (or current Long/MAX_VALUE))
    current (long-min (long current) (long rank))
    :else (long rank)))

(defn- update-best-score
  ^double
  [current score]
  (cond
    (nil? score) (double (or current 0.0))
    current (double-max (double current) (double score))
    :else (double score)))

(defn- update-best-distance
  ^double
  [current distance]
  (cond
    (nil? distance) (double (or current Double/POSITIVE_INFINITY))
    current (double-min (double current) (double distance))
    :else (double distance)))

(defn- merge-domain-hit
  [acc {:keys [eid attr lex-score lex-rank sem-distance sem-rank]}]
  (update acc eid
          (fn [candidate]
            (cond-> (or candidate {:eid eid})
              attr         (update :matched-attrs (fnil conj #{}) attr)
              lex-score    (assoc :lex-score (update-best-score (:lex-score candidate)
                                                                lex-score))
              lex-rank     (assoc :lex-rank (update-best-rank (:lex-rank candidate)
                                                              lex-rank))
              sem-distance (assoc :sem-distance
                                  (update-best-distance (:sem-distance candidate)
                                                        sem-distance))
              sem-rank     (assoc :sem-rank (update-best-rank (:sem-rank candidate)
                                                              sem-rank))))))

(defn- rrf-component
  ^double
  [weight rank]
  (if rank
    (/ (double weight) (+ (double rrf-k) (double rank)))
    0.0))

(defn- hybrid-rrf-score
  ^double
  [{:keys [lex-rank sem-rank]}]
  (+ (rrf-component lexical-rrf-weight lex-rank)
     (rrf-component semantic-rrf-weight sem-rank)))

(defn- rank-domain-candidates
  [domain query & {:keys [top fts-query pool-size] :or {top 10}}]
  (let [pool-size       (long (or pool-size
                                  (default-candidate-pool-size top)))
        lexical-hits    (fulltext-domain-hits domain (or fts-query query) :top pool-size)
        semantic-hits   (embedding-domain-hits domain query :top pool-size)
        merged-hits     (vals (reduce merge-domain-hit {} (concat lexical-hits semantic-hits)))]
    (->> merged-hits
         (map #(assoc % :rrf-score (hybrid-rrf-score %)))
         (sort-by (fn [{:keys [rrf-score lex-score sem-distance eid]}]
                    [(- (double rrf-score))
                     (- (double (or lex-score 0.0)))
                     (double (or sem-distance Double/POSITIVE_INFINITY))
                     eid]))
         (take top)
         vec)))

(defn- node-result
  [eid]
  (let [e (into {} (d/entity (d/db (db/conn)) eid))]
    {:eid  eid
     :name (:kg.node/name e)
     :type (:kg.node/type e)}))

(defn- ref-eid
  [value]
  (or (:db/id value) value))

(defn- fact-result
  [eid]
  (let [e (into {} (d/entity (d/db (db/conn)) eid))]
    {:eid        eid
     :node-eid   (ref-eid (:kg.fact/node e))
     :content    (:kg.fact/content e)
     :confidence (:kg.fact/confidence e)
     :utility    (double (or (:kg.fact/utility e)
                             default-fact-utility))}))

(defn- episode-result
  [eid]
  (let [e (into {} (d/entity (d/db (db/conn)) eid))]
     {:eid        eid
      :summary    (:episode/summary e)
      :context    (:episode/context e)
      :timestamp  (:episode/timestamp e)
      :importance (double (or (:episode/importance e)
                              (:default-importance
                                default-episode-retention-config)))}))

(defn- edge-result
  [eid]
  (let [e (into {} (d/entity (d/db (db/conn)) eid))]
    {:eid      eid
     :from-eid (ref-eid (:kg.edge/from e))
     :to-eid   (ref-eid (:kg.edge/to e))
     :type     (:kg.edge/type e)
     :label    (:kg.edge/label e)}))

(defn- local-doc-doc-fulltext-hits
  [session-id query & {:keys [top] :or {top 10}}]
  (if (or (nil? session-id) (str/blank? query))
    []
    (let [opts {:domains [db/local-doc-domain]
                :top top
                :display :refs+scores}]
      (try
        (->> (db/q '[:find ?e ?a ?v ?score
                     :in $ ?sid ?query ?opts
                     :where
                     [?session :session/id ?sid]
                     [?e :local.doc/session ?session]
                     [?e :local.doc/status :ready]
                     [(fulltext $ ?query ?opts) [[?e ?a ?v ?score]]]]
                   session-id query opts)
             (sort-by #(double (nth % 3)) >)
             (map-indexed
	               (fn [idx [eid attr value score]]
	                 {:eid       eid
	                  :attr      attr
	                  :value     value
	                  :lex-score (double score)
	                  :lex-rank  (indexed-rank idx)}))
             vec)
        (catch Exception _
          [])))))

(defn- local-doc-doc-embedding-hits
  [session-id query & {:keys [top] :or {top 10}}]
  (if (or (nil? session-id) (str/blank? query))
    []
    (let [opts {:domains [db/local-doc-domain]
                :top top
                :display :refs+dists}]
      (try
        (->> (db/q '[:find ?e ?a ?v ?dist
                     :in $ ?sid ?query ?opts
                     :where
                     [?session :session/id ?sid]
                     [?e :local.doc/session ?session]
                     [?e :local.doc/status :ready]
                     [(embedding-neighbors $ ?query ?opts) [[?e ?a ?v ?dist]]]]
                   session-id query opts)
             (sort-by #(double (nth % 3)))
             (map-indexed
	               (fn [idx [eid attr value distance]]
	                 {:eid          eid
	                  :attr         attr
	                  :value        value
	                  :sem-distance (double distance)
	                  :sem-rank     (indexed-rank idx)}))
             vec)
        (catch Exception _
          [])))))

(defn- local-doc-chunk-fulltext-hits
  [session-id query & {:keys [top] :or {top 10}}]
  (if (or (nil? session-id) (str/blank? query))
    []
    (let [opts {:domains [db/local-doc-chunk-domain]
                :top top
                :display :refs+scores}]
      (try
        (->> (db/q '[:find ?chunk ?a ?v ?score
                     :in $ ?sid ?query ?opts
                     :where
                     [?session :session/id ?sid]
                     [?chunk :local.doc.chunk/session ?session]
                     [?chunk :local.doc.chunk/doc ?doc]
                     [?doc :local.doc/status :ready]
                     [(fulltext $ ?query ?opts) [[?chunk ?a ?v ?score]]]]
                   session-id query opts)
             (sort-by #(double (nth % 3)) >)
             (map-indexed
	               (fn [idx [eid attr value score]]
	                 {:eid       eid
	                  :attr      attr
	                  :value     value
	                  :lex-score (double score)
	                  :lex-rank  (indexed-rank idx)}))
             vec)
        (catch Exception _
          [])))))

(defn- local-doc-chunk-embedding-hits
  [session-id query & {:keys [top] :or {top 10}}]
  (if (or (nil? session-id) (str/blank? query))
    []
    (let [opts {:domains [db/local-doc-chunk-domain]
                :top top
                :display :refs+dists}]
      (try
        (->> (db/q '[:find ?chunk ?a ?v ?dist
                     :in $ ?sid ?query ?opts
                     :where
                     [?session :session/id ?sid]
                     [?chunk :local.doc.chunk/session ?session]
                     [?chunk :local.doc.chunk/doc ?doc]
                     [?doc :local.doc/status :ready]
                     [(embedding-neighbors $ ?query ?opts) [[?chunk ?a ?v ?dist]]]]
                   session-id query opts)
             (sort-by #(double (nth % 3)))
             (map-indexed
	               (fn [idx [eid attr value distance]]
	                 {:eid          eid
	                  :attr         attr
	                  :value        value
	                  :sem-distance (double distance)
	                  :sem-rank     (indexed-rank idx)}))
             vec)
        (catch Exception _
          [])))))

(defn- local-doc-doc-result
  [eid]
  (let [e (db/entity eid)]
    {:doc-eid     eid
     :id          (:local.doc/id e)
     :name        (:local.doc/name e)
     :media-type  (:local.doc/media-type e)
     :status      (:local.doc/status e)
     :size-bytes  (:local.doc/size-bytes e)
     :summary     (:local.doc/summary e)
     :preview     (:local.doc/preview e)
     :chunk-count (:local.doc/chunk-count e)}))

(defn- local-doc-chunk-result
  [eid]
  (let [e       (db/entity eid)
        doc-eid (ref-eid (:local.doc.chunk/doc e))]
    (assoc (local-doc-doc-result doc-eid)
           :chunk-id      (:local.doc.chunk/id e)
           :chunk-index   (:local.doc.chunk/index e)
           :chunk-summary (:local.doc.chunk/summary e)
           :chunk-preview (:local.doc.chunk/preview e))))

(defn- local-doc-chunk-sort-key
  [{:keys [rrf-score lex-score sem-distance index]}]
  [(- (double (or rrf-score 0.0)))
   (- (double (or lex-score 0.0)))
   (double (or sem-distance Double/POSITIVE_INFINITY))
   (long (or index Long/MAX_VALUE))])

(defn- merge-local-doc-doc-hit
  [acc doc-hit]
  (let [doc (local-doc-doc-result (:eid doc-hit))]
    (update acc (:doc-eid doc)
            (fn [candidate]
              (let [current (or candidate doc)]
	                (-> current
	                    (merge doc)
	                    (assoc :doc-rrf-score (double-max (double (or (:doc-rrf-score current) 0.0))
	                                                      (double (:rrf-score doc-hit)))
	                           :doc-lex-score (update-best-score (:doc-lex-score current)
	                                                             (:lex-score doc-hit))
                           :doc-sem-distance (update-best-distance (:doc-sem-distance current)
                                                                   (:sem-distance doc-hit))
                           :direct-doc-hit? true)))))))

(defn- merge-local-doc-chunk-hit
  [acc chunk-hit]
  (let [{:keys [doc-eid] :as chunk} (local-doc-chunk-result (:eid chunk-hit))
        chunk-evidence {:id           (:chunk-id chunk)
                        :index        (:chunk-index chunk)
                        :summary      (:chunk-summary chunk)
                        :preview      (:chunk-preview chunk)
                        :rrf-score    (:rrf-score chunk-hit)
                        :lex-score    (:lex-score chunk-hit)
                        :sem-distance (:sem-distance chunk-hit)}]
    (update acc doc-eid
            (fn [candidate]
              (let [current    (or candidate (local-doc-doc-result doc-eid))
                    chunk-hits (->> (conj (vec (or (:chunk-hits current) []))
                                          chunk-evidence)
                                    (sort-by local-doc-chunk-sort-key)
                                    (take 3)
                                    vec)]
                (-> current
                    (merge (select-keys chunk [:id :name :media-type :status :size-bytes :summary :preview :chunk-count]))
                    (assoc :chunk-hits chunk-hits
                           :chunk-rrf-score (transduce (map #(double (or (:rrf-score %) 0.0)))
                                                       +
                                                       0.0
                                                       chunk-hits)
                           :chunk-lex-score (reduce (fn [best hit]
                                                      (update-best-score best (:lex-score hit)))
                                                    nil
                                                    chunk-hits)
                           :chunk-sem-distance (reduce (fn [best hit]
                                                         (update-best-distance best (:sem-distance hit)))
                                                       nil
                                                       chunk-hits))))))))

(defn- finalize-local-doc-candidate
  [{:keys [chunk-hits doc-rrf-score chunk-rrf-score summary preview] :as candidate}]
  (let [matched-chunks (into [] (map #(select-keys % [:id :index :summary :preview]))
                            (or chunk-hits []))
        total-score    (+ (* 0.8 (double (or doc-rrf-score 0.0)))
                          (* 1.2 (double (or chunk-rrf-score 0.0))))]
    (-> candidate
        (assoc :matched-chunks matched-chunks
               :summary (or summary preview)
               :total-score total-score
               :has-chunk-hit? (seq matched-chunks)))))

(defn- rank-local-doc-candidates
  [session-id query & {:keys [top fts-query pool-size] :or {top 10}}]
  (let [pool-size        (long (or pool-size
                                   (default-candidate-pool-size top)))
        doc-lexical      (local-doc-doc-fulltext-hits session-id (or fts-query query) :top pool-size)
        doc-semantic     (local-doc-doc-embedding-hits session-id query :top pool-size)
        doc-merged       (reduce merge-domain-hit {} doc-lexical)
        doc-merged       (reduce merge-domain-hit doc-merged doc-semantic)
        doc-hits         (into [] (map #(assoc % :rrf-score (hybrid-rrf-score %)))
                               (vals doc-merged))
        chunk-lexical    (local-doc-chunk-fulltext-hits session-id (or fts-query query) :top pool-size)
        chunk-semantic   (local-doc-chunk-embedding-hits session-id query :top pool-size)
        chunk-merged     (reduce merge-domain-hit {} chunk-lexical)
        chunk-merged     (reduce merge-domain-hit chunk-merged chunk-semantic)
        chunk-hits       (into [] (map #(assoc % :rrf-score (hybrid-rrf-score %)))
                               (vals chunk-merged))
        merged-candidates (reduce merge-local-doc-chunk-hit
                                  (reduce merge-local-doc-doc-hit {} doc-hits)
                                  chunk-hits)]
    (->> merged-candidates
         vals
         (map finalize-local-doc-candidate)
         (sort-by (fn [{:keys [has-chunk-hit? total-score chunk-lex-score doc-lex-score chunk-sem-distance doc-sem-distance doc-eid]}]
                    [(if has-chunk-hit? 0 1)
                     (- (double (or total-score 0.0)))
                     (- (double (or chunk-lex-score doc-lex-score 0.0)))
                     (double (or chunk-sem-distance doc-sem-distance Double/POSITIVE_INFINITY))
                     doc-eid]))
         (take top)
         vec)))

(defn- local-doc-result
  [{:keys [doc-eid id name media-type status size-bytes summary preview chunk-count matched-chunks]}]
  {:eid            doc-eid
   :id             id
   :name           name
   :media-type     media-type
   :status         status
   :size-bytes     size-bytes
   :summary        summary
   :preview        preview
   :chunk-count    chunk-count
   :matched-chunks matched-chunks})

(defn- artifact-fulltext-hits
  [session-id query & {:keys [top] :or {top 10}}]
  (if (or (nil? session-id) (str/blank? query))
    []
    (let [opts {:domains [db/artifact-domain]
                :top top
                :display :refs+scores}]
      (try
        (->> (db/q '[:find ?e ?a ?v ?score
                     :in $ ?sid ?query ?opts
                     :where
                     [?session :session/id ?sid]
                     [?e :artifact/session ?session]
                     [?e :artifact/status :ready]
                     [(fulltext $ ?query ?opts) [[?e ?a ?v ?score]]]]
                   session-id query opts)
             (sort-by #(double (nth % 3)) >)
             (map-indexed
	               (fn [idx [eid attr value score]]
	                 {:eid       eid
	                  :attr      attr
	                  :value     value
	                  :lex-score (double score)
	                  :lex-rank  (indexed-rank idx)}))
             vec)
        (catch Exception _
          [])))))

(defn- artifact-embedding-hits
  [session-id query & {:keys [top] :or {top 10}}]
  (if (or (nil? session-id) (str/blank? query))
    []
    (let [opts {:domains [db/artifact-domain]
                :top top
                :display :refs+dists}]
      (try
        (->> (db/q '[:find ?e ?a ?v ?dist
                     :in $ ?sid ?query ?opts
                     :where
                     [?session :session/id ?sid]
                     [?e :artifact/session ?session]
                     [?e :artifact/status :ready]
                     [(embedding-neighbors $ ?query ?opts) [[?e ?a ?v ?dist]]]]
                   session-id query opts)
             (sort-by #(double (nth % 3)))
             (map-indexed
	               (fn [idx [eid attr value distance]]
	                 {:eid          eid
	                  :attr         attr
	                  :value        value
	                  :sem-distance (double distance)
	                  :sem-rank     (indexed-rank idx)}))
             vec)
        (catch Exception _
          [])))))

(defn- artifact-result
  [eid]
  (let [e (db/entity eid)]
    {:eid          eid
     :id           (:artifact/id e)
     :name         (:artifact/name e)
     :title        (:artifact/title e)
     :kind         (:artifact/kind e)
     :media-type   (:artifact/media-type e)
     :status       (:artifact/status e)
     :size-bytes   (:artifact/size-bytes e)
     :preview      (:artifact/preview e)
     :has-blob?    (some? (:artifact/blob-id e))
     :text-available? (some? (:artifact/text e))}))

(defn- rank-artifact-candidates
  [session-id query & {:keys [top fts-query pool-size] :or {top 10}}]
  (let [pool-size     (long (or pool-size
                                (default-candidate-pool-size top)))
        lexical-hits  (artifact-fulltext-hits session-id (or fts-query query) :top pool-size)
        semantic-hits (artifact-embedding-hits session-id query :top pool-size)
        merged-hits   (vals (reduce merge-domain-hit {} (concat lexical-hits semantic-hits)))]
    (->> merged-hits
         (map #(assoc % :rrf-score (hybrid-rrf-score %)))
         (sort-by (fn [{:keys [rrf-score lex-score sem-distance eid]}]
                    [(- (double rrf-score))
                     (- (double (or lex-score 0.0)))
                     (double (or sem-distance Double/POSITIVE_INFINITY))
                     eid]))
         (take top)
         vec)))

(defn search-nodes
  "Search KG nodes by name using lexical FTS plus semantic recall.
   Returns [{:eid :name :type}]."
  [query & {:keys [top fts-query] :or {top 10}}]
  (->> (rank-domain-candidates db/kg-node-domain
                               query
                               :top top
                               :fts-query fts-query
                               :pool-size (candidate-pool-size :nodes top))
       (mapv (comp node-result :eid))))

(defn search-facts
  "Search KG facts by content using lexical FTS plus semantic recall.
   Returns [{:eid :node-eid :content :confidence :utility}]."
  [query & {:keys [top fts-query] :or {top 15}}]
  (->> (rank-domain-candidates db/kg-fact-domain
                               query
                               :top top
                               :fts-query fts-query
                               :pool-size (candidate-pool-size :facts top))
       (mapv (comp fact-result :eid))))

(defn search-episodes
  "Search episodes by summary/context using lexical FTS plus semantic recall.
   Returns [{:eid :summary :context :timestamp :importance}]."
  [query & {:keys [top fts-query] :or {top 5}}]
  (->> (rank-domain-candidates db/episode-text-domain
                               query
                               :top top
                               :fts-query fts-query
                               :pool-size (candidate-pool-size :episodes top))
       (mapv (comp episode-result :eid))))

(defn search-edges
  "Search KG edges by label via FTS. Returns [{:eid :from-eid :to-eid :type :label}]."
  [query & {:keys [top] :or {top 10}}]
  (->> (fulltext-domain-hits db/kg-edge-domain
                             query
                             :top (candidate-pool-size :edges top))
       (mapv (comp edge-result :eid))))

(defn search-local-docs
  "Search session-scoped local documents by chunk-preferred hybrid retrieval.
   Returns [{:id :name :media-type :status :size-bytes :summary :preview :chunk-count :matched-chunks}]."
  [session-id query & {:keys [top fts-query] :or {top 5}}]
  (->> (rank-local-doc-candidates session-id
                                  query
                                  :top top
                                  :fts-query fts-query
                                  :pool-size (candidate-pool-size :local-docs top))
       (mapv local-doc-result)))

(defn search-artifacts
  "Search session-scoped artifacts by name/title/text using hybrid retrieval.
   Returns [{:id :name :title :kind :media-type :status :size-bytes :preview}]."
  [session-id query & {:keys [top fts-query] :or {top 5}}]
  (->> (rank-artifact-candidates session-id
                                 query
                                 :top top
                                 :fts-query fts-query
                                 :pool-size (candidate-pool-size :artifacts top))
       (mapv (comp artifact-result :eid))))

(defn node-facts-with-eids
  "Get all facts about a node, including fact eids (for dedup)."
  [node-eid]
  (->> (db/q '[:find ?f ?content ?confidence ?utility ?updated
               :in $ ?node
               :where
               [?f :kg.fact/node ?node]
               [?f :kg.fact/content ?content]
               [?f :kg.fact/confidence ?confidence]
               [(get-else $ ?f :kg.fact/utility 0.5) ?utility]
               [?f :kg.fact/updated-at ?updated]]
             node-eid)
       (sort-by #(nth % 4) #(compare %2 %1))
       (mapv (fn [[eid content confidence utility updated]]
               {:eid        eid
                :content    content
                :confidence confidence
                :utility    (double utility)
                :updated-at updated}))))

(defn facts-by-eids
  [fact-eids]
  (->> fact-eids
       distinct
       (keep (fn [fact-eid]
               (when-let [entity (some-> fact-eid db/entity not-empty)]
                 {:eid        fact-eid
                  :node-eid   (ref-eid (:kg.fact/node entity))
                  :content    (:kg.fact/content entity)
                  :confidence (:kg.fact/confidence entity)
                  :utility    (double (or (:kg.fact/utility entity)
                                          default-fact-utility))
                  :updated-at (:kg.fact/updated-at entity)})))
       vec))

(defn forget-fact!
  [fact-eid]
  (when-let [fact (first (facts-by-eids [fact-eid]))]
    (let [now (java.util.Date.)]
      (db/transact! [[:db/retractEntity fact-eid]
                     [:db/add (:node-eid fact) :kg.node/updated-at now]])
      fact)))

(defn update-fact-utility!
  [fact-eid observed-utility]
  (when-let [entity (some-> fact-eid db/entity not-empty)]
    (let [current    (normalize-fact-utility (:kg.fact/utility entity))
          observed   (normalize-fact-utility observed-utility)
          adjusted   (+ (* 0.6 (double current)) (* 0.4 (double observed)))]
      (db/transact! [[:db/add fact-eid :kg.fact/utility (float adjusted)]])
      adjusted)))

;; ============================================================================
;; Context Assembly — what the agent actually sees
;; DEPRECATED: Use xia.context/assemble-system-prompt instead.
;; ============================================================================

(defn recall-knowledge
  "Assemble knowledge context for the system prompt.
   Returns a structured summary of what xia knows."
  []
  (let [;; All nodes with their facts
        nodes (db/q '[:find ?name ?type
                      :where
                      [?e :kg.node/name ?name]
                      [?e :kg.node/type ?type]])
        ;; Group facts by node for key entities
        node-summaries
        (->> nodes
             (group-by second) ; group by type
             (map (fn [[type entries]]
                    [type (mapv first entries)]))
             (into {}))]
    node-summaries))

(defn knowledge->prompt
  "Format knowledge graph contents into a prompt section."
  []
  (let [knowledge (recall-knowledge)
        episodes  (recent-episodes 5)]
    (str
      (when (seq knowledge)
        (str "## What you know\n"
             (clojure.string/join
               "\n"
               (for [[type names] knowledge]
                 (str "- " (name type) ": "
                      (clojure.string/join ", " names))))
             "\n\n"))
      (when (seq episodes)
        (str "## Recent interactions\n"
             (clojure.string/join
               "\n"
               (map #(str "- " (:summary %)) episodes))
             "\n\n")))))
