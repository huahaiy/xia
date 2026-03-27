(ns xia.memory-edit
  "Narrow long-term-memory mutation helpers exposed to privileged tools."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xia.db :as db]
            [xia.memory :as memory]
            [xia.prompt :as prompt]))

(def ^:private correction-tool-id :memory-correct-fact)
(def ^:private correction-tool-name "memory-correct-fact")
(def ^:private correction-tool-description
  "Correct a stored long-term memory fact by retracting the wrong fact and storing the replacement.")
(def ^:private correction-approval-reason
  "changes long-term memory")
(def ^:private max-suggestions 5)

(defn- normalize-text
  [value]
  (-> (or value "")
      str
      str/trim
      str/lower-case
      (str/replace #"\s+" " ")))

(defn- token-set
  [value]
  (->> (re-seq #"[[:alnum:]]+" (normalize-text value))
       (remove #(<= (count %) 1))
       set))

(defn- ref-eid
  [value]
  (or (:db/id value) value))

(defn- node-name-match?
  [candidate entity-name]
  (let [wanted (normalize-text entity-name)
        actual (normalize-text (:node-name candidate))]
    (or (= actual wanted)
        (str/includes? actual wanted)
        (str/includes? wanted actual))))

(defn- exact-fact-match?
  [candidate old-fact]
  (= (normalize-text (:content candidate))
     (normalize-text old-fact)))

(defn- candidate->summary
  [candidate]
  {:fact_id     (:fact-id candidate)
   :entity_name (:node-name candidate)
   :fact        (:content candidate)})

(defn- fact-row->candidate
  [[fact-id node-eid node-name content confidence utility]]
  {:fact-id     fact-id
   :node-eid    node-eid
   :node-name   node-name
   :content     content
   :confidence  confidence
   :utility     (double utility)})

(defn- all-fact-candidates
  []
  (->> (db/q '[:find ?fact ?node ?node-name ?content ?confidence ?utility
               :where
               [?fact :kg.fact/node ?node]
               [?node :kg.node/name ?node-name]
               [?fact :kg.fact/content ?content]
               [?fact :kg.fact/confidence ?confidence]
               [(get-else $ ?fact :kg.fact/utility 0.5) ?utility]])
       (mapv fact-row->candidate)))

(defn- filter-candidates
  [candidates entity-name]
  (if (str/blank? entity-name)
    candidates
    (filterv #(node-name-match? % entity-name) candidates)))

(defn- candidate-score
  [candidate old-fact]
  (let [candidate-text (normalize-text (:content candidate))
        wanted-text    (normalize-text old-fact)
        wanted-tokens  (token-set old-fact)]
    (cond
      (= candidate-text wanted-text)
      100

      (or (str/includes? candidate-text wanted-text)
          (str/includes? wanted-text candidate-text))
      50

      :else
      (count (set/intersection wanted-tokens (token-set candidate-text))))))

(defn- candidate-suggestions
  [old-fact entity-name]
  (let [candidates  (filter-candidates (all-fact-candidates) entity-name)
        suggestions (->> candidates
                         (map (fn [candidate]
                                (assoc candidate :score (candidate-score candidate old-fact))))
                         (filter #(pos? (long (:score %))))
                         (sort-by (fn [{:keys [score node-name content fact-id]}]
                                    [(- (long score))
                                     (str/lower-case (or node-name ""))
                                     (str/lower-case (or content ""))
                                     (long fact-id)]))
                         (take max-suggestions)
                         (mapv candidate->summary))]
    (if (or (seq suggestions)
            (str/blank? entity-name))
      suggestions
      (->> candidates
           (take max-suggestions)
           (mapv candidate->summary)))))

(defn- fact-entity
  [fact-id]
  (let [fact-id* (cond
                   (integer? fact-id) (long fact-id)
                   (number? fact-id)  (long fact-id)
                   (string? fact-id)  (try
                                        (Long/parseLong (str/trim fact-id))
                                        (catch Exception _ nil))
                   :else nil)]
    (when fact-id*
      {:fact-id fact-id*
       :entity  (some-> fact-id* db/entity not-empty)})))

(defn- fact-entity->candidate
  [fact-id entity]
  (when-let [node-eid (some-> (:kg.fact/node entity) ref-eid)]
    (let [node (db/entity node-eid)]
      {:fact-id     fact-id
       :node-eid    node-eid
       :node-name   (:kg.node/name node)
       :content     (:kg.fact/content entity)
       :confidence  (:kg.fact/confidence entity)
       :utility     (double (or (:kg.fact/utility entity) 0.5))
       :source-eid  (some-> (:kg.fact/source entity) ref-eid)})))

(defn- resolve-target-facts
  [{:keys [fact-id old-fact entity-name]}]
  (if-let [{:keys [fact-id entity]} (and fact-id (fact-entity fact-id))]
    (let [candidate (when entity
                      (fact-entity->candidate fact-id entity))]
      (cond
        (nil? candidate)
        {:status  :not_found
         :message "The requested fact no longer exists."}

        (and (seq entity-name)
             (not (node-name-match? candidate entity-name)))
        {:status          :mismatch
         :message         "The requested fact_id does not belong to the requested entity."
         :candidate_facts [(candidate->summary candidate)]}

        (not (exact-fact-match? candidate old-fact))
        {:status          :mismatch
         :message         "The requested fact_id does not match old_fact."
         :candidate_facts [(candidate->summary candidate)]}

        :else
        {:status     :resolved
         :candidates [candidate]}))
    (let [candidates (->> (all-fact-candidates)
                          (#(filter-candidates % entity-name))
                          (filterv #(exact-fact-match? % old-fact)))]
      (cond
        (= 1 (count candidates))
        {:status     :resolved
         :candidates candidates}

        (empty? candidates)
        {:status          :not_found
         :message         "No exact stored fact matched old_fact."
         :candidate_facts (candidate-suggestions old-fact entity-name)}

        :else
        {:status          :ambiguous
         :message         "Matched multiple stored facts. Retry with fact_id or entity_name."
         :candidate_facts (mapv candidate->summary candidates)}))))

(defn- correction-approved?
  []
  (= correction-tool-id (:tool-id prompt/*interaction-context*)))

(defn- ensure-correction-approved!
  [arguments]
  (when-not (correction-approved?)
    (prompt/status! {:state    :waiting
                     :phase    :approval
                     :message  (str "Waiting for approval for " correction-tool-name)
                     :tool-id  correction-tool-id
                     :tool-name correction-tool-name})
    (when-not (prompt/approve! {:tool-id     correction-tool-id
                                :tool-name   correction-tool-name
                                :description correction-tool-description
                                :arguments   arguments
                                :policy      :always
                                :reason      correction-approval-reason})
      (throw (ex-info (str "user denied approval for privileged tool "
                           (name correction-tool-id))
                      {:tool-id correction-tool-id}))))
  true)

(defn- identical-correction?
  [candidate corrected-fact]
  (= (normalize-text (:content candidate))
     (normalize-text corrected-fact)))

(defn- existing-replacement
  [candidate corrected-fact]
  (->> (memory/node-facts-with-eids (:node-eid candidate))
       (remove #(= (:eid %) (:fact-id candidate)))
       (filter #(= (normalize-text (:content %))
                   (normalize-text corrected-fact)))
       first))

(defn correct-fact!
  "Retract an exact stored fact and add the corrected replacement."
  [{:keys [fact-id old-fact corrected-fact entity-name] :as args}]
  (let [old-fact*       (some-> old-fact str str/trim)
        corrected-fact* (some-> corrected-fact str str/trim)]
    (when (str/blank? old-fact*)
      (throw (ex-info "old_fact is required" {:type :memory-edit/missing-old-fact})))
    (when (str/blank? corrected-fact*)
      (throw (ex-info "corrected_fact is required"
                      {:type :memory-edit/missing-corrected-fact})))
    (let [{:keys [status candidates message candidate_facts]}
          (resolve-target-facts {:fact-id fact-id
                                 :old-fact old-fact*
                                 :entity-name entity-name})]
      (case status
        :resolved
        (let [candidate (first candidates)]
          (if (identical-correction? candidate corrected-fact*)
            {:status "unchanged"
             :message "The stored fact already matches corrected_fact."
             :fact (candidate->summary candidate)}
            (do
              (ensure-correction-approved! {"fact_id" fact-id
                                            "old_fact" old-fact*
                                            "corrected_fact" corrected-fact*
                                            "entity_name" entity-name})
              (memory/forget-fact! (:fact-id candidate))
              (if-let [replacement (existing-replacement candidate corrected-fact*)]
                {:status         "merged"
                 :message        "Removed the wrong fact. The corrected fact was already stored."
                 :entity_name    (:node-name candidate)
                 :retracted_fact (candidate->summary candidate)
                 :new_fact       {:fact_id     (:eid replacement)
                                  :entity_name (:node-name candidate)
                                  :fact        (:content replacement)}}
                (do
                  (memory/add-fact! {:node-eid    (:node-eid candidate)
                                     :content     corrected-fact*
                                     :confidence  (:confidence candidate)
                                     :utility     (:utility candidate)
                                     :source-eid  (:source-eid candidate)})
                  (let [new-fact (first (filter #(= (normalize-text corrected-fact*)
                                                    (normalize-text (:content %)))
                                               (memory/node-facts-with-eids (:node-eid candidate))))]
                    {:status         "corrected"
                     :entity_name    (:node-name candidate)
                     :retracted_fact (candidate->summary candidate)
                     :new_fact       {:fact_id     (:eid new-fact)
                                      :entity_name (:node-name candidate)
                                      :fact        (:content new-fact)}}))))))

        :not_found
        {:status          "not_found"
         :message         message
         :candidate_facts candidate_facts}

        :ambiguous
        {:status          "ambiguous"
         :message         message
         :candidate_facts candidate_facts}

        :mismatch
        {:status          "mismatch"
         :message         message
         :candidate_facts candidate_facts}))))
