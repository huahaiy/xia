(ns xia.hippocampus
  "Hippocampus — consolidates episodic memory into the knowledge graph.

   Like the biological hippocampus, this process:
   1. Reviews recent episodes (conversations, events)
   2. Extracts entities, relationships, and facts via the LLM
   3. Merges them into the knowledge graph (creating or updating nodes/edges)
   4. Marks episodes as processed

   Runs after conversations end, or periodically as a background process."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [charred.api :as json]
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
                    {:role "user"   :content user-msg}])]
    (try
      (json/read-json response)
      (catch Exception e
        (log/warn "Failed to parse extraction response:" (.getMessage e))
        nil))))

;; ---------------------------------------------------------------------------
;; Knowledge graph merging
;; ---------------------------------------------------------------------------

(defn- find-or-create-node!
  "Find an existing node by name, or create a new one."
  [name type]
  (let [existing (memory/find-node name)]
    (if (seq existing)
      ;; Return the best match (exact name match preferred)
      (:eid (or (first (filter #(= (clojure.string/lower-case (:name %))
                                    (clojure.string/lower-case name))
                                existing))
                (first existing)))
      ;; Create new
      (let [id (memory/add-node! {:name name :type (keyword type)})]
        ;; Get the eid of the just-created node
        (ffirst (db/q '[:find ?e :in $ ?id
                        :where [?e :kg.node/id ?id]] id))))))

(defn- fact-similar?
  "Check if two fact strings are substantially similar (for dedup)."
  [existing-content new-content]
  (let [a (str/lower-case (str/trim existing-content))
        b (str/lower-case (str/trim new-content))]
    (or (= a b)
        (str/includes? a b)
        (str/includes? b a))))

(defn- dedup-fact!
  "Add a fact with deduplication. If a similar fact exists on the node,
   update it instead of creating a duplicate. If the new fact contradicts
   an existing one, lower the old fact's confidence and add the new one."
  [node-eid content source-eid]
  (let [existing (memory/node-facts-with-eids node-eid)
        similar  (first (filter #(fact-similar? (:content %) content) existing))]
    (if similar
      ;; Update existing fact (refresh timestamp, keep higher confidence)
      (db/transact! [[:db/add (:eid similar) :kg.fact/updated-at (java.util.Date.)]])
      ;; No similar fact — add new
      (memory/add-fact! {:node-eid   node-eid
                         :content    content
                         :source-eid source-eid}))))

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
  (when extraction
    (let [entities (get extraction "entities")
          relations (get extraction "relations")
          ;; Build name→eid lookup as we create nodes
          node-eids (atom {})]

      ;; Create/find nodes, add facts (with dedup), merge properties
      (doseq [entity entities]
        (let [name  (get entity "name")
              type  (get entity "type" "concept")
              facts (get entity "facts" [])
              props (get entity "properties")]
          (when name
            (let [eid (find-or-create-node! name type)]
              (swap! node-eids assoc (str/lower-case name) eid)
              ;; Add facts with dedup
              (doseq [fact facts]
                (dedup-fact! eid fact episode-eid))
              ;; Merge properties (set each key, preserving existing)
              (when (and props (map? props))
                (doseq [[k v] (keywordize-props props)]
                  (memory/set-node-property! eid [k] v)))))))

      ;; Create edges
      (doseq [rel relations]
        (let [from-name (get rel "from")
              to-name   (get rel "to")
              from-eid  (get @node-eids (some-> from-name str/lower-case))
              to-eid    (get @node-eids (some-> to-name str/lower-case))]
          (when (and from-eid to-eid)
            (memory/add-edge! {:from-eid   from-eid
                               :to-eid     to-eid
                               :type       (keyword (get rel "type" "related-to"))
                               :label      (get rel "label")
                               :source-eid episode-eid})))))))

;; ---------------------------------------------------------------------------
;; Consolidation
;; ---------------------------------------------------------------------------

(defn consolidate-episode!
  "Process a single episode: extract knowledge and merge into the graph."
  [{:keys [eid summary] :as episode}]
  (log/info "Consolidating episode:" summary)
  (let [extraction (extract-knowledge episode)]
    (merge-extraction! extraction eid)
    (memory/mark-episode-processed! eid)
    (log/info "Consolidated episode, extracted"
              (count (get extraction "entities" []))
              "entities and"
              (count (get extraction "relations" []))
              "relations")))

(defn consolidate-pending!
  "Process all unprocessed episodes."
  []
  (let [episodes (memory/unprocessed-episodes)]
    (when (seq episodes)
      (log/info "Consolidating" (count episodes) "pending episodes")
      (doseq [ep episodes]
        (try
          (consolidate-episode! ep)
          (catch Exception e
            (log/error e "Failed to consolidate episode:" (:summary ep))))))))

;; ---------------------------------------------------------------------------
;; Episode creation from conversation
;; ---------------------------------------------------------------------------

(defn summarize-conversation
  "Use the LLM to create a summary of a conversation for episodic storage."
  [messages]
  (let [convo-text (->> messages
                        (map (fn [{:keys [role content]}]
                               (str (name role) ": " content)))
                        (clojure.string/join "\n"))
        response   (llm/chat-simple
                     [{:role "system"
                       :content "Summarize this conversation in 1-3 sentences.
                                Focus on: what was discussed, what was decided,
                                what the user wanted, and any personal information
                                shared. Be factual and concise."}
                      {:role "user" :content convo-text}])]
    response))

(defn record-conversation!
  "Record a conversation as an episodic memory and trigger consolidation.
   Includes WM topic summary as episode context for richer consolidation."
  [session-id channel & {:keys [topics]}]
  (let [messages (db/session-messages session-id)]
    (when (> (count messages) 1) ; at least one exchange
      (let [summary (summarize-conversation messages)]
        (memory/record-episode!
          {:type         :conversation
           :summary      summary
           :context      (when topics (str "Topic: " topics))
           :channel      channel
           :session-id   session-id
           :participants (db/get-config :user/name)})
        ;; Consolidate in the background
        (future (consolidate-pending!))))))

;; ---------------------------------------------------------------------------
;; Knowledge maintenance
;; ---------------------------------------------------------------------------

(defn maintain-knowledge!
  "Periodic maintenance: decay confidence of unreinforced facts.
   Facts that haven't been updated recently get slightly lower confidence."
  []
  (let [one-week-ago (java.util.Date. (- (System/currentTimeMillis) (* 7 24 60 60 1000)))
        old-facts (db/q '[:find ?f ?confidence ?updated
                          :where
                          [?f :kg.fact/confidence ?confidence]
                          [?f :kg.fact/updated-at ?updated]])
        stale     (filter (fn [[_ _ updated]]
                            (.before ^java.util.Date updated one-week-ago))
                          old-facts)]
    (when (seq stale)
      (log/info "Decaying confidence of" (count stale) "stale facts")
      (doseq [[eid confidence _] stale]
        (let [new-conf (max 0.1 (* confidence 0.95))]
          (db/transact! [[:db/add eid :kg.fact/confidence (float new-conf)]]))))))

(defn consolidate-if-pending!
  "Idle consolidation: if pending episodes exceed threshold, consolidate."
  [& {:keys [threshold] :or {threshold 3}}]
  (let [pending (memory/unprocessed-episodes)]
    (when (>= (count pending) threshold)
      (log/info "Idle consolidation triggered:" (count pending) "pending episodes")
      (consolidate-pending!))))
