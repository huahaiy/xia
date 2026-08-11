(ns xia.skill.proposal
  "Durable, review-first proposals for creating or improving prompt skills."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.skill :as skill])
  (:import [java.util Date UUID]))

(def proposal-ops #{:create :patch :archive})
(def proposal-statuses #{:pending :rejected :applied})
(def proposal-risks #{:low :medium :high})
(def review-decisions #{:approve :reject})

(defn- now [] (Date.))

(defn- normalize-keyword
  [value field allowed]
  (let [value* (cond
                 (keyword? value) value
                 (string? value) (some-> value str/trim not-empty keyword)
                 :else nil)]
    (if (contains? allowed value*)
      value*
      (throw (ex-info (str "Invalid skill proposal " field)
                      {:type :skill-proposal/invalid-field
                       :field field
                       :value value
                       :allowed (sort (map name allowed))})))))

(defn- normalize-uuid
  [value field]
  (cond
    (nil? value) nil
    (instance? UUID value) value
    (string? value) (try
                      (UUID/fromString (str/trim value))
                      (catch IllegalArgumentException _
                        (throw (ex-info (str "Invalid " field)
                                        {:type :skill-proposal/invalid-id
                                         :field field
                                         :value value}))))
    :else (throw (ex-info (str "Invalid " field)
                          {:type :skill-proposal/invalid-id
                           :field field
                           :value value}))))

(defn- normalize-skill-id
  [value]
  (cond
    (keyword? value) value
    (string? value) (some-> value str/trim not-empty keyword)
    :else nil))

(defn- value-of
  [m k]
  (or (get m k)
      (get m (name k))
      (get m (str/replace (name k) "-" "_"))))

(defn- nonblank
  [value]
  (some-> value str str/trim not-empty))

(defn- reviewer-text
  [value]
  (cond
    (keyword? value) (name value)
    (some? value) (nonblank value)
    :else nil))

(defn- write-doc
  [value]
  (when (some? value)
    (json/write-json-str value)))

(defn- keywordize-json
  [value]
  (cond
    (map? value) (into {}
                       (map (fn [[k v]]
                              [(if (string? k) (keyword k) k)
                               (keywordize-json v)]))
                       value)
    (vector? value) (mapv keywordize-json value)
    (sequential? value) (map keywordize-json value)
    :else value))

(defn- read-doc
  [value]
  (cond
    (nil? value) nil
    (string? value) (try
                      (keywordize-json (json/read-json value))
                      (catch Exception _
                        value))
    :else value))

(defn- decode-proposal-docs
  [proposal]
  (some-> proposal
          (update :skill.proposal/source read-doc)
          (update :skill.proposal/evidence read-doc)))

(defn- proposal-entity
  [eid]
  (decode-proposal-docs (db/entity eid)))

(defn- proposal-eid
  [proposal-id]
  (ffirst (db/q '[:find ?e :in $ ?id
                  :where [?e :skill.proposal/id ?id]]
                (normalize-uuid proposal-id "proposal_id"))))

(defn get-proposal
  [proposal-id]
  (some-> proposal-id
          proposal-eid
          proposal-entity))

(defn proposal-summary
  [proposal]
  (cond-> {:id (some-> (:skill.proposal/id proposal) str)
           :op (some-> (:skill.proposal/op proposal) name)
           :status (some-> (:skill.proposal/status proposal) name)
           :skill_id (some-> (:skill.proposal/skill-id proposal) name)
           :skill_name (:skill.proposal/skill-name proposal)
           :title (:skill.proposal/title proposal)
           :rationale (:skill.proposal/rationale proposal)
           :risk (some-> (:skill.proposal/risk proposal) name)}
    (:skill.proposal/task-id proposal)
    (assoc :task_id (str (:skill.proposal/task-id proposal)))
    (:skill.proposal/source proposal)
    (assoc :source (:skill.proposal/source proposal))
    (:skill.proposal/evidence proposal)
    (assoc :evidence (:skill.proposal/evidence proposal))
    (:skill.proposal/review-note proposal)
    (assoc :review_note (:skill.proposal/review-note proposal))
    (:skill.proposal/reviewer proposal)
    (assoc :reviewer (:skill.proposal/reviewer proposal))))

(defn proposal-detail
  [proposal]
  (assoc (proposal-summary proposal)
         :content (:skill.proposal/content proposal)))

(defn list-proposals
  [& {:keys [status task-id]}]
  (let [status* (when status
                  (normalize-keyword status "status" proposal-statuses))
        task-id* (normalize-uuid task-id "task_id")
        eids    (db/q '[:find ?e
                        :where [?e :skill.proposal/id _]])]
    (->> eids
         (map (comp proposal-entity first))
         (filter (fn [proposal]
                   (and (or (nil? status*)
                            (= status* (:skill.proposal/status proposal)))
                        (or (nil? task-id*)
                            (= task-id* (:skill.proposal/task-id proposal))))))
         (sort-by (juxt (comp - #(.getTime ^Date (or % (Date. 0))) :skill.proposal/created-at)
                        :skill.proposal/title))
         vec)))

(defn proposals-for-task
  [task-id]
  (list-proposals :task-id task-id))

(defn- validate-proposal!
  [{:keys [op skill-id content]}]
  (when-not skill-id
    (throw (ex-info "Skill proposal requires a skill id"
                    {:type :skill-proposal/invalid
                     :field :skill-id})))
  (when (and (contains? #{:create :patch} op)
             (str/blank? (or content "")))
    (throw (ex-info "Skill proposal requires content"
                    {:type :skill-proposal/invalid
                     :field :content
                     :op op}))))

(defn create-proposal!
  "Store a pending skill proposal. Proposal application is always a separate
   review operation."
  [{:keys [id task-id op skill-id skill-name title rationale content risk source evidence]}]
  (let [op*       (normalize-keyword op "op" proposal-ops)
        skill-id* (normalize-skill-id skill-id)
        risk*     (if (some? risk)
                    (normalize-keyword risk "risk" proposal-risks)
                    :medium)
        created   (now)
        proposal  {:id (or (normalize-uuid id "proposal_id")
                           (UUID/randomUUID))
                   :task-id (normalize-uuid task-id "task_id")
                   :op op*
                   :skill-id skill-id*
                   :skill-name (nonblank skill-name)
                   :title (or (nonblank title)
                              (case op*
                                :create "Create reusable skill"
                                :patch "Update reusable skill"
                                :archive "Archive reusable skill"))
                   :rationale (or (nonblank rationale) "")
                   :content content
                   :risk risk*
                   :source source
                   :evidence evidence}]
    (validate-proposal! proposal)
    (db/transact!
     [(cond-> {:skill.proposal/id (:id proposal)
               :skill.proposal/op (:op proposal)
               :skill.proposal/status :pending
               :skill.proposal/skill-id (:skill-id proposal)
               :skill.proposal/title (:title proposal)
               :skill.proposal/rationale (:rationale proposal)
               :skill.proposal/risk (:risk proposal)
               :skill.proposal/created-at created
               :skill.proposal/updated-at created}
        (:task-id proposal) (assoc :skill.proposal/task-id (:task-id proposal))
        (:skill-name proposal) (assoc :skill.proposal/skill-name (:skill-name proposal))
        (:content proposal) (assoc :skill.proposal/content (:content proposal))
        (:source proposal) (assoc :skill.proposal/source (write-doc (:source proposal)))
        (:evidence proposal) (assoc :skill.proposal/evidence (write-doc (:evidence proposal))))])
    (get-proposal (:id proposal))))

(defn reject-proposal!
  [proposal-id & {:keys [reviewer note]}]
  (let [proposal-id* (normalize-uuid proposal-id "proposal_id")
        eid          (proposal-eid proposal-id*)]
    (when-not eid
      (throw (ex-info "Skill proposal not found"
                      {:type :skill-proposal/not-found
                       :proposal-id proposal-id*})))
    (db/transact! [(cond-> {:db/id eid
                            :skill.proposal/status :rejected
                            :skill.proposal/updated-at (now)
                            :skill.proposal/reviewed-at (now)}
                     (reviewer-text reviewer) (assoc :skill.proposal/reviewer (reviewer-text reviewer))
                     note (assoc :skill.proposal/review-note (str note)))])
    (get-proposal proposal-id*)))

(defn- proposal-skill-source
  [proposal proposal-id]
  {:origin :skill-proposal
   :created-by :agent
   :proposal-id (str proposal-id)
   :source-task-id (some-> (:skill.proposal/task-id proposal) str)
   :risk (:skill.proposal/risk proposal)})

(defn- assert-pending!
  [proposal]
  (when-not (= :pending (:skill.proposal/status proposal))
    (throw (ex-info "Skill proposal is not pending"
                    {:type :skill-proposal/not-pending
                     :proposal-id (:skill.proposal/id proposal)
                     :status (:skill.proposal/status proposal)}))))

(defn- assert-existing-skill!
  [skill-id]
  (or (db/get-skill skill-id)
      (throw (ex-info "Skill proposal target skill not found"
                      {:type :skill-proposal/skill-not-found
                       :skill-id skill-id}))))

(defn- assert-source-current!
  [proposal existing]
  (let [expected (or (get-in proposal [:skill.proposal/source :skill :content-sha256])
                     (get-in proposal [:skill.proposal/source "skill" "content_sha256"])
                     (get-in proposal [:skill.proposal/source "skill" "content-sha256"]))]
    (when (and expected
               (not= expected (:skill/content-sha256 existing)))
      (throw (ex-info "Skill changed after proposal was generated"
                      {:type :skill-proposal/source-skill-changed
                       :skill-id (:skill/id existing)
                       :expected-content-sha256 expected
                       :actual-content-sha256 (:skill/content-sha256 existing)})))))

(defn apply-proposal!
  "Apply an approved proposal. New skills are saved as disabled drafts by default;
   pass `:enable? true` to enable them immediately."
  [proposal-id & {:keys [reviewer note enable?]}]
  (let [proposal-id* (normalize-uuid proposal-id "proposal_id")
        proposal     (or (get-proposal proposal-id*)
                         (throw (ex-info "Skill proposal not found"
                                         {:type :skill-proposal/not-found
                                          :proposal-id proposal-id*})))
        _            (assert-pending! proposal)
        op           (:skill.proposal/op proposal)
        skill-id     (:skill.proposal/skill-id proposal)
        content      (:skill.proposal/content proposal)
        applied-at   (now)
        result       (case op
                       :create
                       (skill/save-skill!
                        {:id skill-id
                         :name (or (:skill.proposal/skill-name proposal)
                                   (name skill-id))
                         :description (:skill.proposal/rationale proposal)
                         :content content
                         :enabled? (true? enable?)
                         :trust-level :agent-authored
                         :source-format :skill-proposal
                         :provenance (proposal-skill-source proposal proposal-id*)})

                       :patch
                       (let [existing (assert-existing-skill! skill-id)]
                         (assert-source-current! proposal existing)
                         (let [saved (skill/save-skill!
                                      {:id skill-id
                                       :name (:skill/name existing)
                                       :description (:skill/description existing)
                                       :content content
                                       :version (:skill/version existing)
                                       :enabled? (:skill/enabled? existing)
                                       :trust-level (:skill/trust-level existing)
                                       :trust-note (:skill/trust-note existing)
                                       :lifecycle (:skill/lifecycle existing)
                                       :lifecycle-reason (:skill/lifecycle-reason existing)
                                       :provenance (:skill/provenance existing)})]
                           (skill/record-usage! skill-id :patched)
                           saved))

                       :archive
                       (do
                         (assert-existing-skill! skill-id)
                         (db/update-skill-lifecycle! skill-id
                                                     {:lifecycle :archived
                                                      :reason (:skill.proposal/rationale proposal)
                                                      :enabled? false
                                                      :archived-at applied-at})
                         (db/get-skill skill-id)))]
    (db/transact! [(cond-> {:skill.proposal/id proposal-id*
                            :skill.proposal/status :applied
                            :skill.proposal/updated-at applied-at
                            :skill.proposal/reviewed-at applied-at
                            :skill.proposal/applied-at applied-at}
                     (reviewer-text reviewer) (assoc :skill.proposal/reviewer (reviewer-text reviewer))
                     note (assoc :skill.proposal/review-note (str note)))])
    {:proposal (get-proposal proposal-id*)
     :skill result}))

(defn- assistant-content
  [message]
  (cond
    (string? message) message
    (map? message) (or (:content message)
                       (get message "content")
                       (some-> message :message :content)
                       (some-> message (get "message") (get "content")))
    :else nil))

(defn- agent-authored-skill?
  [saved]
  (or (= :agent-authored (:skill/trust-level saved))
      (= :agent (get-in saved [:skill/provenance :created-by]))))

(defn- target-skill
  [proposal]
  (when (contains? #{:patch :archive} (:skill.proposal/op proposal))
    (db/get-skill (:skill.proposal/skill-id proposal))))

(defn- llm-review-blocker
  [proposal target]
  (cond
    (nil? proposal)
    {:reason :not-found
     :message "Skill proposal not found"}

    (not= :pending (:skill.proposal/status proposal))
    {:reason :not-pending
     :message "LLM review requires a pending proposal"
     :status (:skill.proposal/status proposal)}

    (= :high (:skill.proposal/risk proposal))
    {:reason :high-risk
     :message "High-risk skill proposals require human review"}

    (= :create (:skill.proposal/op proposal))
    nil

    (contains? #{:patch :archive} (:skill.proposal/op proposal))
    (cond
      (nil? target)
      {:reason :skill-not-found
       :message "LLM review target skill was not found"
       :skill-id (:skill.proposal/skill-id proposal)}

      (not (agent-authored-skill? target))
      {:reason :non-agent-authored-skill
       :message "LLM review can only modify agent-authored skills"
       :skill-id (:skill/id target)
       :trust-level (:skill/trust-level target)}

      :else nil)

    :else
    {:reason :unsupported-op
     :message "Unsupported skill proposal operation"
     :op (:skill.proposal/op proposal)}))

(defn llm-review-eligible?
  "True when an LLM may review and directly approve/reject this proposal.

   LLM review is intentionally narrower than human review: it can create
   disabled agent-authored drafts, or patch/archive existing agent-authored
   skills. User-authored, imported, system, and high-risk proposals require a
   human reviewer."
  [proposal-or-id]
  (let [proposal (if (map? proposal-or-id)
                   proposal-or-id
                   (get-proposal proposal-or-id))]
    (nil? (llm-review-blocker proposal (target-skill proposal)))))

(defn- assert-llm-review-eligible!
  [proposal target]
  (when-let [blocker (llm-review-blocker proposal target)]
    (throw (ex-info (:message blocker)
                    (assoc blocker
                           :type :skill-proposal/llm-review-not-eligible
                           :proposal-id (:skill.proposal/id proposal))))))

(defn- review-target-body
  [target]
  (when target
    {:id (some-> (:skill/id target) name)
     :name (:skill/name target)
     :version (:skill/version target)
     :trust_level (some-> (:skill/trust-level target) name)
     :lifecycle (some-> (:skill/lifecycle target) name)
     :enabled (:skill/enabled? target)
     :content_sha256 (:skill/content-sha256 target)
     :content (:skill/content target)}))

(defn- review-request
  [proposal target]
  {:kind "skill-proposal-review-request"
   :version 1
   :proposal (proposal-detail proposal)
   :target_skill (review-target-body target)
   :rules ["Return JSON only: {\"decision\":\"approve\"|\"reject\",\"reason\":\"...\",\"enable\":false}."
           "Approve only if the proposed skill change is reusable beyond the completed task."
           "Reject if the proposal includes secrets, credentials, raw private messages, private URLs, personal identifiers, or one-off task details."
           "Reject patches or archives unless the target_skill trust_level is agent-authored."
           "Reject high-risk proposals. Human review is required for those."
           "For create proposals, approval creates a disabled draft unless enable is explicitly allowed by the caller."]})

(def ^:private review-system-prompt
  (str "You review Xia skill proposals before they are applied. "
       "Return only valid JSON. Be conservative: reject unless the proposal is "
       "safe, reusable, and within the stated rules."))

(defn- parse-review-response!
  [message]
  (let [content (or (assistant-content message) "{}")
        parsed  (try
                  (json/read-json content)
                  (catch Exception e
                    (throw (ex-info "LLM skill proposal review returned invalid JSON"
                                    {:type :skill-proposal/invalid-llm-review
                                     :content content}
                                    e))))
        decision (normalize-keyword (or (value-of parsed :decision)
                                        (value-of parsed :status))
                                    "review decision"
                                    review-decisions)
        reason   (or (nonblank (value-of parsed :reason))
                     (nonblank (value-of parsed :rationale))
                     "No review reason supplied.")]
    {:decision decision
     :reason reason
     :enable? (true? (or (value-of parsed :enable)
                         (value-of parsed :enable?)))}))

(defn review-proposal-with-llm!
  "Ask an LLM to review a pending proposal and immediately apply or reject it
   when eligible. Create approvals remain disabled drafts by default; pass
   `:allow-enable? true` to honor an LLM `enable: true` response."
  [proposal-id & {:keys [allow-enable?] :as opts}]
  (let [proposal-id* (normalize-uuid proposal-id "proposal_id")
        proposal     (or (get-proposal proposal-id*)
                         (throw (ex-info "Skill proposal not found"
                                         {:type :skill-proposal/not-found
                                          :proposal-id proposal-id*})))
        _            (assert-pending! proposal)
        target       (target-skill proposal)
        _            (assert-llm-review-eligible! proposal target)
        llm-opts     (cond-> {}
                       (:provider-id opts) (assoc :provider-id (:provider-id opts))
                       (:workload opts) (assoc :workload (:workload opts))
                       (contains? opts :temperature) (assoc :temperature (:temperature opts))
                       (contains? opts :max-tokens) (assoc :max-tokens (:max-tokens opts)))
        message      (apply llm/chat-message
                            [{"role" "system" "content" review-system-prompt}
                             {"role" "user" "content" (json/write-json-str
                                                       (review-request proposal target))}]
                            (mapcat identity llm-opts))
        {:keys [decision reason enable?]} (parse-review-response! message)]
    (case decision
      :approve
      (let [result (apply-proposal! proposal-id*
                                    :reviewer :llm
                                    :note reason
                                    :enable? (and allow-enable? enable?))]
        {:decision :approved
         :reason reason
         :proposal (:proposal result)
         :skill (:skill result)})

      :reject
      (let [reviewed (reject-proposal! proposal-id*
                                       :reviewer :llm
                                       :note reason)]
        {:decision :rejected
         :reason reason
         :proposal reviewed}))))

(defn- review-or-defer-proposal!
  [proposal opts]
  (let [target  (target-skill proposal)
        blocker (llm-review-blocker proposal target)]
    (if blocker
      {:decision :needs-human-review
       :reason (:reason blocker)
       :message (:message blocker)
       :proposal proposal}
      (apply review-proposal-with-llm!
             (:skill.proposal/id proposal)
             (mapcat identity opts)))))

(defn- task-skill-snapshots
  [task]
  (->> (get-in task [:contract :skills])
       (keep (fn [ref]
               (let [skill-id (normalize-skill-id (or (:id ref)
                                                      (:skill-id ref)
                                                      (get ref "id")
                                                      (get ref "skill_id")))]
                 (when-let [saved (and skill-id (db/get-skill skill-id))]
                   {:id skill-id
                    :name (:skill/name saved)
                    :version (:skill/version saved)
                    :content-sha256 (:skill/content-sha256 saved)
                    :content (:skill/content saved)}))))
       vec))

(defn- generation-request
  [task skills max-proposals]
  {:kind "skill-improvement-reflection-request"
   :version 1
   :task {:id (some-> (:id task) str)
          :title (:title task)
          :summary (:summary task)
          :state (some-> (:state task) name)
          :goal (or (get-in task [:contract :goal])
                    (:title task))}
   :skills (mapv (fn [{:keys [id name version content-sha256 content]}]
                   {:id (clojure.core/name id)
                    :name name
                    :version version
                    :content_sha256 content-sha256
                    :content content})
                 skills)
   :max_proposals max-proposals
   :rules ["Return JSON only: {\"proposals\": [...]}."
           "Use op=create for new reusable behavior, op=patch for a full replacement of an existing skill, or op=archive for obsolete agent-authored skills."
           "Do not include secrets, personal identifiers, OAuth data, raw email bodies, private URLs, or one-off task details."
           "Prefer no proposal when the task produced no reusable process knowledge."
           "Every create or patch proposal must include content."]})

(def ^:private generation-system-prompt
  (str "You identify reusable prompt-skill improvements after a Xia task. "
       "Return only valid JSON. Proposals are review drafts, not direct writes. "
       "Be conservative: propose nothing unless the improvement is reusable."))

(defn- normalize-generated-proposal
  [task-id skill-snapshots proposal]
  (let [op       (normalize-keyword (value-of proposal :op) "op" proposal-ops)
        skill-id (normalize-skill-id (or (value-of proposal :skill-id)
                                         (value-of proposal :skill_id)))
        existing (some #(when (= skill-id (:id %)) %) skill-snapshots)]
    {:task-id task-id
     :op op
     :skill-id skill-id
     :skill-name (or (value-of proposal :name)
                     (value-of proposal :skill-name)
                     (some-> skill-id name))
     :title (value-of proposal :title)
     :rationale (value-of proposal :rationale)
     :content (value-of proposal :content)
     :risk (or (value-of proposal :risk) :medium)
     :source (cond-> {:generator :llm-skill-reflection}
               existing (assoc :skill {:id (name (:id existing))
                                       :version (:version existing)
                                       :content-sha256 (:content-sha256 existing)}))
     :evidence {:task-id (str task-id)}}))

(defn generate-proposals-for-task!
  "Reflect on a task and store pending skill-improvement proposals.

   This is intentionally opt-in. Callers decide when post-task reflection should
   spend an LLM call; applying a proposal remains a separate review operation."
  [task-id & {:keys [skills max-proposals] :or {max-proposals 3} :as opts}]
  (let [task-id* (normalize-uuid task-id "task_id")
        task     (or (db/get-task task-id*)
                     (throw (ex-info "Task not found"
                                     {:type :skill-proposal/task-not-found
                                      :task-id task-id*})))
        snapshots (vec (or skills (task-skill-snapshots task)))
        request   (generation-request task snapshots max-proposals)
        llm-opts  (cond-> {}
                    (:provider-id opts) (assoc :provider-id (:provider-id opts))
                    (:workload opts) (assoc :workload (:workload opts))
                    (contains? opts :temperature) (assoc :temperature (:temperature opts))
                    (contains? opts :max-tokens) (assoc :max-tokens (:max-tokens opts)))
        message   (apply llm/chat-message
                         [{"role" "system" "content" generation-system-prompt}
                          {"role" "user" "content" (json/write-json-str request)}]
                         (mapcat identity llm-opts))
        parsed    (json/read-json (or (assistant-content message) "{}"))
        proposals (or (get parsed "proposals") [])]
    (mapv #(create-proposal!
            (normalize-generated-proposal task-id* snapshots %))
          proposals)))

(defn generate-and-review-proposals-for-task!
  "Run the post-task learning loop: generate pending proposals, then ask an LLM
   reviewer to approve/reject proposals that are eligible for automated review.

   Proposals that are high-risk or target user/imported/system skills are left
   pending with a `:needs-human-review` review result."
  [task-id & {:keys [allow-enable?] :as opts}]
  (let [proposals   (apply generate-proposals-for-task!
                           task-id
                           (mapcat identity (dissoc opts :allow-enable?)))
        review-opts (cond-> {}
                      (:provider-id opts) (assoc :provider-id (:provider-id opts))
                      (:workload opts) (assoc :workload (:workload opts))
                      (contains? opts :temperature) (assoc :temperature (:temperature opts))
                      (contains? opts :max-tokens) (assoc :max-tokens (:max-tokens opts))
                      (contains? opts :allow-enable?) (assoc :allow-enable? allow-enable?))]
    {:proposals proposals
     :reviews (mapv #(review-or-defer-proposal! % review-opts)
                    proposals)}))
