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
  (:require [datalevin.core :as d]
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
  [importance]
  (let [value (double (or importance
                          (:default-importance default-episode-retention-config)))]
    (-> value
        (max 0.0)
        (min 1.0))))

(defn normalize-fact-confidence
  [confidence]
  (-> (double (or confidence 0.0))
      (max 0.0)
      (min 1.0)))

(defn normalize-fact-utility
  [utility]
  (-> (double (or utility default-fact-utility))
      (max 0.0)
      (min 1.0)))

(defn fact-prompt-score
  [{:keys [confidence utility]}]
  (+ (* fact-confidence-weight
        (normalize-fact-confidence confidence))
     (* fact-utility-weight
        (normalize-fact-utility utility))))

(defn fact-utility-half-life-factor
  [utility]
  (+ 0.5 (normalize-fact-utility utility)))

(defn unprocessed-episodes
  "Get episodes not yet consolidated by the hippocampus.
   Uses entity lookup for :processed? check (Datalevin 0.10.x boolean index
   doesn't reliably reflect updates)."
  []
  (->> (db/q '[:find ?e ?summary ?ctx ?ts ?type ?importance
               :where
               [?e :episode/summary ?summary]
               [?e :episode/timestamp ?ts]
               [?e :episode/type ?type]
               [(get-else $ ?e :episode/context "") ?ctx]
               [(get-else $ ?e :episode/importance 0.5) ?importance]])
       (filter (fn [[eid]]
                 (not (:episode/processed? (db/entity eid)))))
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

(defn- processed-episodes
  []
  (->> (db/q '[:find ?e ?summary ?ctx ?ts ?type ?channel ?session-id ?importance
               :where
               [?e :episode/summary ?summary]
               [?e :episode/timestamp ?ts]
               [?e :episode/type ?type]
               [(get-else $ ?e :episode/context "") ?ctx]
               [(get-else $ ?e :episode/channel "") ?channel]
               [(get-else $ ?e :episode/session-id "") ?session-id]
               [(get-else $ ?e :episode/importance 0.5) ?importance]])
       (filter (fn [[eid]]
                 (:episode/processed? (db/entity eid))))
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
  [^java.util.Date timestamp ^java.util.Date as-of]
  (max 0 (- (.getTime as-of) (.getTime timestamp))))

(defn- decayed-episode?
  [episode ^java.util.Date as-of retention-config]
  (> (episode-age-ms (:timestamp episode) as-of)
     (:full-resolution-ms retention-config)))

(defn- episode-decay-score
  [episode ^java.util.Date as-of retention-config]
  (let [importance   (normalize-importance (:importance episode))
        half-life-ms (:decay-half-life-ms retention-config)
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

(defn prune-processed-episodes!
  "Prune processed episodes with exponential-decay downsampling.

   Keeps all processed episodes for the full-resolution window. After that,
   episodes compete by a decayed importance score and only the top-ranked
   older items are retained per session/channel/type."
  ([] (prune-processed-episodes! (java.util.Date.)))
  ([^java.util.Date as-of]
   (let [retention-config (episode-retention-settings)
         to-remove        (prunable-processed-episodes (processed-episodes)
                                                       as-of
                                                       retention-config)]
     (when (seq to-remove)
       (db/transact! (vec (mapcat #(detach-episode-provenance-tx (:eid %))
                                  to-remove))))
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
  (let [pattern (clojure.string/lower-case name-pattern)]
    (->> (db/q '[:find ?e ?name ?type
                 :where
                 [?e :kg.node/name ?name]
                 [?e :kg.node/type ?type]])
         (filter (fn [[_ name _]]
                   (clojure.string/includes?
                     (clojure.string/lower-case name) pattern)))
         (mapv (fn [[eid name type]]
                 {:eid eid :name name :type type})))))

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
;; Full-text Search (BM25 via Datalevin)
;; ============================================================================
;; These functions provide hybrid-ready search: FTS works today; vector search
;; activates when Datalevin's embedding service becomes available.

(defn- fts-datoms
  "Run fulltext search, return results as maps {:eid :attr :value}.
   Datalevin fulltext-datoms returns [eid attr value] vectors."
  [query]
  (when (and query (not (clojure.string/blank? query)))
    (let [db (d/db (db/conn))]
      (try
        (mapv (fn [d] {:eid (nth d 0) :attr (nth d 1) :value (nth d 2)})
              (d/fulltext-datoms db query))
        (catch Exception _
          [])))))

(defn search-nodes
  "Search KG nodes by name via FTS. Returns [{:eid :name :type}]."
  [query & {:keys [top] :or {top 10}}]
  (->> (fts-datoms query)
       (filter #(= :kg.node/name (:attr %)))
       (take top)
       (mapv (fn [{:keys [eid]}]
               (let [e (into {} (d/entity (d/db (db/conn)) eid))]
                 {:eid  eid
                  :name (:kg.node/name e)
                  :type (:kg.node/type e)})))))

(defn search-facts
  "Search KG facts by content via FTS. Returns [{:eid :node-eid :content :confidence :utility}]."
  [query & {:keys [top] :or {top 15}}]
  (->> (fts-datoms query)
       (filter #(= :kg.fact/content (:attr %)))
       (take top)
       (mapv (fn [{:keys [eid]}]
               (let [e (into {} (d/entity (d/db (db/conn)) eid))]
                 {:eid        eid
                  :node-eid   (:kg.fact/node e)
                  :content    (:kg.fact/content e)
                  :confidence (:kg.fact/confidence e)
                  :utility    (double (or (:kg.fact/utility e)
                                          default-fact-utility))})))))

(defn search-episodes
  "Search episodes by summary/context via FTS. Returns [{:eid :summary :context :timestamp :importance}]."
  [query & {:keys [top] :or {top 5}}]
  (->> (fts-datoms query)
       (filter #(#{:episode/summary :episode/context} (:attr %)))
       (take top)
       (mapv (fn [{:keys [eid]}]
               (let [e (into {} (d/entity (d/db (db/conn)) eid))]
                  {:eid       eid
                   :summary   (:episode/summary e)
                   :context   (:episode/context e)
                   :timestamp (:episode/timestamp e)
                    :importance (double (or (:episode/importance e)
                                          (:default-importance default-episode-retention-config)))})))))

(defn search-edges
  "Search KG edges by label via FTS. Returns [{:eid :from-eid :to-eid :type :label}]."
  [query & {:keys [top] :or {top 10}}]
  (->> (fts-datoms query)
       (filter #(= :kg.edge/label (:attr %)))
       (take top)
       (mapv (fn [{:keys [eid]}]
               (let [e (into {} (d/entity (d/db (db/conn)) eid))]
                 {:eid      eid
                  :from-eid (:kg.edge/from e)
                  :to-eid   (:kg.edge/to e)
                  :type     (:kg.edge/type e)
                  :label    (:kg.edge/label e)})))))

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
               (when-let [entity (some-> fact-eid db/entity seq (into {}))]
                 {:eid        fact-eid
                  :node-eid   (:kg.fact/node entity)
                  :content    (:kg.fact/content entity)
                  :confidence (:kg.fact/confidence entity)
                  :utility    (double (or (:kg.fact/utility entity)
                                          default-fact-utility))
                  :updated-at (:kg.fact/updated-at entity)})))
       vec))

(defn update-fact-utility!
  [fact-eid observed-utility]
  (when-let [entity (some-> fact-eid db/entity seq (into {}))]
    (let [current    (normalize-fact-utility (:kg.fact/utility entity))
          observed   (normalize-fact-utility observed-utility)
          adjusted   (+ (* 0.6 current) (* 0.4 observed))]
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
