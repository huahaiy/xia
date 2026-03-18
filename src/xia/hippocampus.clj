(ns xia.hippocampus
  "Hippocampus — consolidates episodic memory into the knowledge graph.

   Like the biological hippocampus, this process:
   1. Reviews recent episodes (conversations, events)
   2. Extracts entities, relationships, and facts via the LLM
   3. Merges them into the knowledge graph (creating or updating nodes/edges)
   4. Marks episodes as processed

   Runs after conversations end, or periodically as a background process."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [charred.api :as json]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.memory :as memory]))

;; ---------------------------------------------------------------------------
;; Extraction prompt
;; ---------------------------------------------------------------------------

(def ^:private extraction-prompt
  "You are a memory consolidation system. Given a conversation summary and context,
extract structured knowledge.

Return a JSON object with:
{
  \"entities\": [
    {
      \"name\": \"...\",
      \"type\": \"person|place|thing|concept|preference|event\",
      \"facts\": [\"fact1\", \"fact2\"],
      \"properties\": {\"key\": \"value\"}
    }
  ],
  \"relations\": [
    {\"from\": \"entity name\", \"to\": \"entity name\", \"type\": \"knows|likes|dislikes|works-at|uses|part-of|wants|prefers\", \"label\": \"description\"}
  ]
}

Rules:
- Extract ONLY what is clearly stated or strongly implied
- Use the user's actual name if known, not \"the user\"
- Entity types: person, place, thing, concept, preference, event
- Relation types: knows, likes, dislikes, works-at, uses, part-of, wants, prefers, related-to
- Facts should be atomic, one piece of information each
- Properties are structured key-value attributes of an entity (location, age, role, company, language, etc.)
  Use properties for concrete attributes; use facts for observations or context-dependent statements.
  Example: {\"location\": \"Seattle\", \"role\": \"engineer\"} as properties,
           [\"prefers functional programming\"] as facts.
  Omit \"properties\" if none apply. Use flat keys when possible; nest only when natural (e.g. {\"work\": {\"company\": \"Acme\"}}).
- If nothing meaningful to extract, return {\"entities\": [], \"relations\": []}
- Return ONLY valid JSON, no markdown fencing")

(def ^:private importance-batch-size 20)
(def ^:private default-episode-importance 0.5)

(def ^:private importance-prompt
  "You are rating which episodic memories deserve longer retention.

Return a JSON object with this exact shape:
{
  \"episodes\": [
    {\"index\": 0, \"importance\": 0.0},
    {\"index\": 1, \"importance\": 0.0}
  ]
}

Rules:
- importance must be a number between 0.0 and 1.0
- higher = retain longer because the episode contains durable personal facts, major decisions, commitments, recurring responsibilities, emotionally significant events, or unusually informative context
- lower = routine chatter, transient status, or disposable operational detail
- rate every episode in the batch
- return ONLY valid JSON, no markdown fencing")

;; ---------------------------------------------------------------------------
;; LLM extraction
;; ---------------------------------------------------------------------------

(defn- extract-knowledge
  "Call the LLM to extract entities and relations from an episode."
  [episode]
  (let [user-msg (str "Episode type: " (name (:type episode))
                      "\nSummary: " (:summary episode)
                      (when (:context episode)
                        (str "\nContext: " (:context episode))))
        response (llm/chat-simple
                   [{:role "system" :content extraction-prompt}
                    {:role "user"   :content user-msg}]
                   :workload :memory-extraction)]
    (try
      (json/read-json response)
      (catch Exception e
        (log/warn "Failed to parse extraction response:" (.getMessage e))
        nil))))

(defn- parse-importance
  [value]
  (cond
    (number? value) (double value)
    (string? value) (try
                      (Double/parseDouble (str/trim value))
                      (catch Exception _ default-episode-importance))
    :else default-episode-importance))

(defn- normalize-importance
  [value]
  (-> value
      parse-importance
      (max 0.0)
      (min 1.0)))

(defn- episode-importance-defaults
  [episodes]
  (into {}
        (map (fn [{:keys [eid]}]
               [eid default-episode-importance]))
        episodes))

(defn- rate-importance-batch
  [episodes]
  (let [user-msg (->> episodes
                      (map-indexed
                        (fn [idx {:keys [timestamp type summary context]}]
                          (str "Episode " idx
                               "\nTimestamp: " (some-> timestamp .toInstant str)
                               "\nType: " (name type)
                               "\nSummary: " summary
                               (when context
                                 (str "\nContext: " context)))))
                      (str/join "\n\n---\n\n"))
        defaults (episode-importance-defaults episodes)
        response (llm/chat-simple
                   [{:role "system" :content importance-prompt}
                    {:role "user"   :content user-msg}]
                   :workload :memory-importance)]
    (try
      (let [parsed (json/read-json response)]
        (reduce
          (fn [scores entry]
            (let [idx (get entry "index")]
              (if (and (number? idx)
                       (<= 0 (long idx))
                       (< (long idx) (count episodes)))
                (assoc scores
                       (:eid (nth episodes (long idx)))
                       (normalize-importance (get entry "importance")))
                scores)))
          defaults
          (get parsed "episodes" [])))
      (catch Exception e
        (log/warn "Failed to parse episode importance batch:" (.getMessage e))
        defaults))))

(defn- rate-episode-importance
  [episodes]
  (->> episodes
       (sort-by :timestamp)
       (partition-all importance-batch-size)
       (map #(rate-importance-batch (vec %)))
       (reduce merge {})))

;; ---------------------------------------------------------------------------
;; Knowledge graph merging
;; ---------------------------------------------------------------------------

(defn- existing-node-match
  [name]
  (let [existing (memory/find-node name)]
    (when (seq existing)
      (or (first (filter #(= (str/lower-case (:name %))
                             (str/lower-case name))
                         existing))
          (first existing)))))

(defn- find-or-create-node!
  "Find an existing node by name, or create a new one."
  [name type]
  (if-let [match (existing-node-match name)]
    (:eid match)
    ;; Create new
    (let [id (memory/add-node! {:name name :type (keyword type)})]
      ;; Get the eid of the just-created node
      (ffirst (db/q '[:find ?e :in $ ?id
                      :where [?e :kg.node/id ?id]] id)))))

(defn- canonical-fact
  [content]
  (-> (or content "")
      str/lower-case
      str/trim
      (str/replace #"[^a-z0-9]+" " ")
      (str/replace #"\s+" " ")
      str/trim))

(defn- fact-tokens
  [content]
  (let [canon (canonical-fact content)]
    (if (str/blank? canon)
      #{}
      (->> (str/split canon #" ")
           (remove #(<= (count %) 2))
           set))))

(defn- fact-similar?
  "Check if two fact strings are conservatively similar enough to dedup.

   Prefer false negatives over false positives here: duplicate facts are less
   damaging than silently collapsing a correction into an older fact."
  [existing-content new-content]
  (let [a  (canonical-fact existing-content)
        b  (canonical-fact new-content)
        ta (fact-tokens existing-content)
        tb (fact-tokens new-content)]
    (or (= a b)
        (str/includes? a b)
        (str/includes? b a)
        (and (seq ta) (seq tb) (set/subset? ta tb))
        (and (seq ta) (seq tb) (set/subset? tb ta)))))

(defn- dedup-fact!
  "Add a fact with deduplication. If a similar fact exists on the node,
   update it instead of creating a duplicate. If the new fact contradicts
   an existing one, lower the old fact's confidence and add the new one."
  [node-eid content source-eid]
  (let [existing (memory/node-facts-with-eids node-eid)
        similar  (first (filter #(fact-similar? (:content %) content) existing))
        now      (java.util.Date.)]
    (if similar
      ;; Update existing fact (refresh timestamp, keep higher confidence)
      (let [bottomed-at (:kg.fact/bottomed-at (db/entity (:eid similar)))]
        (db/transact! (cond-> [[:db/add (:eid similar) :kg.fact/updated-at now]
                               [:db/add (:eid similar) :kg.fact/decayed-at now]]
                        bottomed-at
                        (conj [:db/retract (:eid similar) :kg.fact/bottomed-at bottomed-at]))))
      ;; No similar fact — add new
      (memory/add-fact! {:node-eid   node-eid
                         :content    content
                         :source-eid source-eid}))))

(defn- node-facts-for-dedup
  [node-eid episode-eid]
  (->> (memory/node-facts-with-eids node-eid)
       (remove (fn [{:keys [eid]}]
                 (= episode-eid (:kg.fact/source (db/entity eid)))))
       vec))

(defn- retract-episode-extractions-tx
  [episode-eid]
  ;; Consolidation retries replace prior fact/edge extractions from the same
  ;; episode before writing the fresh extraction result.
  (let [fact-eids (map first (db/q '[:find ?f
                                     :in $ ?episode
                                     :where [?f :kg.fact/source ?episode]]
                                   episode-eid))
        edge-eids (map first (db/q '[:find ?e
                                     :in $ ?episode
                                     :where [?e :kg.edge/source ?episode]]
                                   episode-eid))]
    (mapv (fn [eid] [:db/retractEntity eid])
          (concat fact-eids edge-eids))))

(defn- deep-merge-props
  [left right]
  (merge-with (fn [a b]
                (if (and (map? a) (map? b))
                  (deep-merge-props a b)
                  b))
              left
              right))

(defn- merge-node-properties
  [existing-props new-props]
  (if (seq new-props)
    (deep-merge-props (or existing-props {}) new-props)
    existing-props))

(defn- ensure-node-state!
  [nodes* facts* name type episode-eid]
  (let [key (str/lower-case name)]
    (or (get @nodes* key)
        (let [existing (existing-node-match name)
              node     (if existing
                         (let [entity (db/entity (:eid existing))]
                           {:id         (:kg.node/id entity)
                            :ref        [:kg.node/id (:kg.node/id entity)]
                            :name       (:kg.node/name entity)
                            :type       (:kg.node/type entity)
                            :properties (:kg.node/properties entity)
                            :new?       false
                            :persist?   false
                            :eid        (:eid existing)})
                         (let [node-id (random-uuid)]
                           {:id         node-id
                            :ref        [:kg.node/id node-id]
                            :name       name
                            :type       (keyword type)
                            :properties nil
                            :new?       true
                            :persist?   true
                            :eid        nil}))]
          (swap! nodes* assoc key node)
          (swap! facts* assoc (:id node)
                 (if-let [eid (:eid node)]
                   (node-facts-for-dedup eid episode-eid)
                   []))
          node))))

(defn- queue-fact-tx!
  [fact-tx* facts* refreshed-facts node content episode-eid now]
  (let [normalized (some-> content str str/trim)]
    (when (seq normalized)
      (let [similar (first (filter #(fact-similar? (:content %) normalized)
                                   (get @facts* (:id node) [])))]
        (cond
          (and similar (:eid similar))
          (when-not (contains? @refreshed-facts (:eid similar))
            (let [bottomed-at (:kg.fact/bottomed-at (db/entity (:eid similar)))]
              (swap! refreshed-facts conj (:eid similar))
              (swap! fact-tx* into
                     (cond-> [[:db/add (:eid similar) :kg.fact/updated-at now]
                              [:db/add (:eid similar) :kg.fact/decayed-at now]]
                       bottomed-at
                       (conj [:db/retract (:eid similar) :kg.fact/bottomed-at bottomed-at])))))

          similar
          nil

          :else
          (do
            (swap! fact-tx* conj
                   {:kg.fact/id         (random-uuid)
                    :kg.fact/node       (:ref node)
                    :kg.fact/content    normalized
                    :kg.fact/confidence 1.0
                    :kg.fact/utility    0.5
                    :kg.fact/source     episode-eid
                    :kg.fact/created-at now
                    :kg.fact/updated-at now
                    :kg.fact/decayed-at now})
            (swap! facts* update (:id node) conj {:content normalized})))))))

(declare keywordize-props)

(defn- build-merge-tx
  [extraction episode-eid & {:keys [mark-processed? importance]}]
  (when extraction
    (let [entities        (get extraction "entities" [])
          relations       (get extraction "relations" [])
          now             (java.util.Date.)
          nodes*          (atom {})
          facts*          (atom {})
          refreshed-facts (atom #{})
          edge-keys*      (atom #{})
          fact-tx*        (atom [])
          edge-tx*        (atom [])]
      (doseq [entity entities]
        (let [name  (some-> (get entity "name") str str/trim)
              type  (get entity "type" "concept")
              facts (get entity "facts" [])
              props (keywordize-props (get entity "properties"))]
          (when (seq name)
            (let [node (ensure-node-state! nodes* facts* name type episode-eid)]
              (when (seq props)
                (swap! nodes* update (str/lower-case name)
                       (fn [current]
                         (-> current
                             (assoc :persist? true)
                             (assoc :properties
                                    (merge-node-properties
                                      (:properties current)
                                      props))))))
              (doseq [fact facts]
                (queue-fact-tx! fact-tx* facts* refreshed-facts
                                node fact episode-eid now))))))

      (doseq [rel relations]
        (let [from-name (some-> (get rel "from") str str/trim)
              to-name   (some-> (get rel "to") str str/trim)
              from-node (get @nodes* (some-> from-name str/lower-case))
              to-node   (get @nodes* (some-> to-name str/lower-case))
              edge-key  [(:id from-node)
                         (:id to-node)
                         (keyword (get rel "type" "related-to"))
                         (or (get rel "label") "")]]
          (when (and from-node to-node (not (contains? @edge-keys* edge-key)))
            (swap! edge-keys* conj edge-key)
            (swap! edge-tx* conj
                   (cond-> {:kg.edge/id         (random-uuid)
                            :kg.edge/from       (:ref from-node)
                            :kg.edge/to         (:ref to-node)
                            :kg.edge/type       (keyword (get rel "type" "related-to"))
                            :kg.edge/created-at now
                            :kg.edge/source     episode-eid}
                     (seq (get rel "label"))
                     (assoc :kg.edge/label (get rel "label")))))))

      (vec
        (concat
          (retract-episode-extractions-tx episode-eid)
          (->> (vals @nodes*)
               (filter :persist?)
               (map (fn [{:keys [id name type properties new?]}]
                      (cond-> {:kg.node/id         id
                               :kg.node/name       name
                               :kg.node/type       type
                               :kg.node/updated-at now}
                        new?       (assoc :kg.node/created-at now)
                        properties (assoc :kg.node/properties properties)))))
          @fact-tx*
          @edge-tx*
          (when mark-processed?
            [[:db/add episode-eid :episode/processed? true]
             [:db/add episode-eid :episode/importance
              (float (normalize-importance importance))]]))))))

(defn- keywordize-props
  "Convert string-keyed JSON map from LLM to keyword-keyed map for idoc storage.
   Handles nested maps recursively."
  [m]
  (when (map? m)
    (reduce-kv
      (fn [acc k v]
        (assoc acc (keyword k) (if (map? v) (keywordize-props v) v)))
      {}
      m)))

(defn- merge-extraction!
  "Merge extracted knowledge into the knowledge graph.
   Includes fact deduplication: similar facts are updated instead of duplicated."
  [extraction episode-eid]
  (when-let [tx-data (seq (build-merge-tx extraction episode-eid))]
    (db/transact! tx-data)))

;; ---------------------------------------------------------------------------
;; Consolidation
;; ---------------------------------------------------------------------------

(defn consolidate-episode!
  "Process a single episode: extract knowledge and merge into the graph."
  [{:keys [eid summary importance] :as episode}]
  (log/info "Consolidating episode:" summary)
  (let [extraction (extract-knowledge episode)]
    (if-not extraction
      (let [error-message "knowledge extraction returned invalid JSON"]
        (memory/mark-episode-consolidation-failed! eid error-message)
        (log/warn "Skipping episode after invalid extraction:" summary)
        {:status      :invalid-extraction
         :episode-eid eid
         :summary     summary
         :error       error-message})
      (do
        (db/transact! (build-merge-tx extraction
                                      eid
                                      :mark-processed? true
                                      :importance (or importance
                                                      default-episode-importance)))
        (memory/prune-processed-episodes!)
        (log/info "Consolidated episode, extracted"
                  (count (get extraction "entities" []))
                  "entities and"
                  (count (get extraction "relations" []))
                  "relations")
        {:status      :ok
         :episode-eid eid
         :summary     summary
         :entities    (count (get extraction "entities" []))
         :relations   (count (get extraction "relations" []))}))))

(defn consolidate-pending!
  "Process all unprocessed episodes."
  []
  (let [episodes (memory/unprocessed-episodes)]
    (when (seq episodes)
      (let [importance-by-eid (rate-episode-importance episodes)]
        (log/info "Consolidating" (count episodes) "pending episodes")
        (doseq [ep episodes]
          (try
            (consolidate-episode! (assoc ep :importance
                                         (get importance-by-eid
                                              (:eid ep)
                                              default-episode-importance)))
            (catch Exception e
              (log/error e "Failed to consolidate episode:" (:summary ep)))))))))

;; ---------------------------------------------------------------------------
;; Episode creation from conversation
;; ---------------------------------------------------------------------------

(defn summarize-conversation
  "Use the LLM to create a summary of a conversation for episodic storage."
  [messages]
  (let [convo-text (->> messages
                        (map (fn [{:keys [role content local-docs]}]
                               (str (name role) ": " content
                                    (when (seq local-docs)
                                      (str "\n[local documents: "
                                           (str/join ", "
                                                     (keep :name local-docs))
                                           "]")))))
                        (clojure.string/join "\n"))
        response   (llm/chat-simple
                     [{:role "system"
                       :content "Summarize this conversation in 1-3 sentences.
                                Focus on: what was discussed, what was decided,
                                what the user wanted, and any personal information
                                shared. Be factual and concise."}
                      {:role "user" :content convo-text}]
                     :workload :memory-summary)]
    response))

(defn- referenced-local-doc-names
  [messages]
  (->> messages
       (mapcat :local-docs)
       (keep :name)
       (remove str/blank?)
       distinct
       vec))

(defn record-conversation!
  "Record a conversation as an episodic memory and trigger consolidation.
   Includes WM topic summary as episode context for richer consolidation."
  [session-id channel & {:keys [topics]}]
  (let [messages (db/session-messages session-id)]
    (when (> (count messages) 1) ; at least one exchange
      (let [summary    (summarize-conversation messages)
            doc-names  (referenced-local-doc-names messages)
            context    (not-empty
                         (str/join "\n"
                                   (cond-> []
                                     topics
                                     (conj (str "Topic: " topics))
                                     (seq doc-names)
                                     (conj (str "Local documents referenced: "
                                                (str/join ", " doc-names))))))]
        (memory/record-episode!
          {:type         :conversation
           :summary      summary
           :context      context
           :channel      channel
           :session-id   session-id
           :participants (db/get-config :user/name)})
        ;; Consolidate in the background
        (future
          (try
            (consolidate-pending!)
            (catch Exception e
              (log/error e "Background consolidation failed for session" session-id))))))))

;; ---------------------------------------------------------------------------
;; Knowledge maintenance
;; ---------------------------------------------------------------------------

(def ^:private default-knowledge-decay-config
  {:grace-period-ms      (* 182 24 60 60 1000)
   :half-life-ms         (* 730 24 60 60 1000)
   :min-confidence       0.1
   :maintenance-step-ms  (* 24 60 60 1000)
   :archive-after-bottom-ms (* 365 24 60 60 1000)})

(def ^:private knowledge-decay-config-keys
  {:grace-period-ms     :memory/knowledge-decay-grace-period-ms
   :half-life-ms        :memory/knowledge-decay-half-life-ms
   :min-confidence      :memory/knowledge-decay-min-confidence
   :maintenance-step-ms :memory/knowledge-decay-maintenance-step-ms
   :archive-after-bottom-ms :memory/knowledge-decay-archive-after-bottom-ms})

(defn- configured-bounded-double
  [config-key default-value]
  (if-let [raw (db/get-config config-key)]
    (try
      (let [parsed (Double/parseDouble (str raw))]
        (if (<= 0.0 parsed 1.0)
          parsed
          default-value))
      (catch Exception _
        default-value))
    default-value))

(defn knowledge-decay-settings
  "Return the effective fact-confidence decay settings."
  []
  (assoc default-knowledge-decay-config
         :grace-period-ms
         (cfg/positive-long (:grace-period-ms knowledge-decay-config-keys)
                            (:grace-period-ms default-knowledge-decay-config))
         :half-life-ms
         (cfg/positive-long (:half-life-ms knowledge-decay-config-keys)
                            (:half-life-ms default-knowledge-decay-config))
         :min-confidence
         (configured-bounded-double (:min-confidence knowledge-decay-config-keys)
                                    (:min-confidence default-knowledge-decay-config))
         :maintenance-step-ms
         (cfg/positive-long (:maintenance-step-ms knowledge-decay-config-keys)
                            (:maintenance-step-ms default-knowledge-decay-config))
         :archive-after-bottom-ms
         (cfg/positive-long (:archive-after-bottom-ms knowledge-decay-config-keys)
                            (:archive-after-bottom-ms default-knowledge-decay-config))))

(defn- decay-window-ms
  [^java.util.Date updated-at ^java.util.Date as-of decay-config]
  (max 0
       (- (.getTime as-of)
          (.getTime updated-at)
          (:grace-period-ms decay-config))))

(defn- decayed-at
  [fact-entity ^java.util.Date updated-at]
  (let [last-decayed (:kg.fact/decayed-at fact-entity)]
    (if (and last-decayed (.before ^java.util.Date last-decayed updated-at))
      updated-at
      (or last-decayed updated-at))))

(defn- bottomed-out-confidence?
  [confidence decay-config]
  (<= (memory/normalize-fact-confidence confidence)
      (+ (double (:min-confidence decay-config)) 1.0e-9)))

(defn- inferred-bottomed-at
  [fact-entity previous-confidence effective-confidence ^java.util.Date updated-at ^java.util.Date last-decayed ^java.util.Date as-of decay-config]
  (when (bottomed-out-confidence? effective-confidence decay-config)
    (or (:kg.fact/bottomed-at fact-entity)
        (when-not (bottomed-out-confidence? previous-confidence decay-config)
          as-of)
        last-decayed
        updated-at
        as-of)))

(defn- archive-bottomed-fact?
  [confidence ^java.util.Date bottomed-at ^java.util.Date as-of decay-config]
  (and bottomed-at
       (bottomed-out-confidence? confidence decay-config)
       (>= (- (.getTime as-of) (.getTime bottomed-at))
           (:archive-after-bottom-ms decay-config))))

(defn- next-confidence
  [confidence utility ^java.util.Date updated-at ^java.util.Date last-decayed ^java.util.Date as-of decay-config]
  (let [previous-window (decay-window-ms updated-at last-decayed decay-config)
        current-window  (decay-window-ms updated-at as-of decay-config)
        delta-window    (- current-window previous-window)
        utility-factor  (memory/fact-utility-half-life-factor utility)]
    (when (pos? delta-window)
      (max (:min-confidence decay-config)
           (* confidence
              (Math/pow 0.5
                        (/ delta-window
                           (double (* (:half-life-ms decay-config)
                                      utility-factor)))))))))

(defn maintain-knowledge!
  "Periodic maintenance: apply time-based exponential decay to stale facts.
   Facts get a grace period, then decay toward a floor using the configured half-life.
   Facts that remain at the confidence floor long enough are archived out of the live DB."
  ([] (maintain-knowledge! (java.util.Date.)))
  ([^java.util.Date as-of]
   (let [decay-config (knowledge-decay-settings)
         facts (db/q '[:find ?f ?confidence ?utility ?updated
                       :where
                       [?f :kg.fact/confidence ?confidence]
                       [(get-else $ ?f :kg.fact/utility 0.5) ?utility]
                       [?f :kg.fact/updated-at ?updated]])
         step-ms (:maintenance-step-ms decay-config)
         {:keys [decay-tx archive-eids]}
         (reduce
           (fn [{:keys [decay-tx archive-eids] :as acc} [eid confidence utility updated]]
             (let [fact-entity    (db/entity eid)
                   last-decayed   (decayed-at fact-entity updated)
                   due?           (>= (- (.getTime as-of) (.getTime last-decayed))
                                      step-ms)
                   decayed-conf   (when due?
                                    (next-confidence confidence utility updated last-decayed as-of decay-config))
                   effective-conf (double (or decayed-conf confidence))
                   bottomed-at    (inferred-bottomed-at fact-entity
                                                       confidence
                                                       effective-conf
                                                       updated
                                                       last-decayed
                                                       as-of
                                                       decay-config)]
               (cond
                 (archive-bottomed-fact? effective-conf bottomed-at as-of decay-config)
                 (update acc :archive-eids conj eid)

                 :else
                 (let [new-tx (cond-> []
                                (and decayed-conf
                                     (> (Math/abs (- effective-conf (double confidence)))
                                        1.0e-9))
                                (into [[:db/add eid :kg.fact/confidence (float effective-conf)]
                                       [:db/add eid :kg.fact/decayed-at as-of]])

                                (and bottomed-at
                                     (not= bottomed-at (:kg.fact/bottomed-at fact-entity)))
                                (conj [:db/add eid :kg.fact/bottomed-at bottomed-at]))]
                   (if (seq new-tx)
                     (update acc :decay-tx into new-tx)
                     acc)))))
           {:decay-tx [] :archive-eids []}
           facts)]
     (when (seq decay-tx)
       (log/info "Updated confidence of" (count (filter #(= :kg.fact/confidence (nth % 2 nil)) decay-tx)) "stale fact fields")
       (db/transact! decay-tx))
     (when (seq archive-eids)
       (log/info "Archiving" (count archive-eids) "bottomed-out facts")
       (db/transact! (mapv (fn [eid] [:db/retractEntity eid]) archive-eids)))
     (+ (count decay-tx) (count archive-eids)))))

(defn consolidate-if-pending!
  "Idle consolidation: if pending episodes exceed threshold, consolidate."
  [& {:keys [threshold] :or {threshold 3}}]
  (let [pending (memory/unprocessed-episodes)]
    (when (>= (count pending) threshold)
      (log/info "Idle consolidation triggered:" (count pending) "pending episodes")
      (consolidate-pending!))))
