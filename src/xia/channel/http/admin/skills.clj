(ns xia.channel.http.admin.skills
  "Skill admin HTTP handlers."
  (:require [clojure.string :as str]
            [xia.channel.http.admin.common :as common]
            [xia.db :as db]
            [xia.skill :as skill]
            [xia.skill.openclaw :as openclaw-skill]
            [xia.skill.proposal :as skill-proposal]))

(defn- infer-skill-id
  [data]
  (let [name-text (common/nonblank-str (get data "name"))
        base      (or (common/normalize-id-segment name-text)
                      "skill")
        used-ids  (map :skill/id (db/list-skills))]
    (common/next-available-id base used-ids)))

(defn- parse-skill-tags
  [value]
  (let [parts (cond
                (string? value) (str/split value #"[,\n]")
                (sequential? value) value
                :else nil)]
    (->> parts
         (map common/nonblank-str)
         (keep common/normalize-id-segment)
         (map keyword)
         set)))

(defn skill->body
  [deps skill]
  {:id                    (some-> (:skill/id skill) name)
   :name                  (:skill/name skill)
   :description           (:skill/description skill)
   :version               (:skill/version skill)
   :content_sha256        (:skill/content-sha256 skill)
   :source_sha256         (:skill/source-sha256 skill)
   :tags                  (->> (or (:skill/tags skill) [])
                               (map name)
                               sort
                               vec)
   :enabled               (boolean (:skill/enabled? skill))
   :source_format         (some-> (:skill/source-format skill) name)
   :source_path           (:skill/source-path skill)
   :source_url            (:skill/source-url skill)
   :source_name           (:skill/source-name skill)
   :provenance            (:skill/provenance skill)
   :trust_level           (some-> (:skill/trust-level skill) name)
   :trust_note            (:skill/trust-note skill)
   :lifecycle             (some-> (:skill/lifecycle skill) name)
   :lifecycle_reason      (:skill/lifecycle-reason skill)
   :installed_at          (common/instant->str deps (:skill/installed-at skill))
   :updated_at            (common/instant->str deps (:skill/updated-at skill))
   :archived_at           (common/instant->str deps (:skill/archived-at skill))
   :selected_count        (long (or (:skill/selected-count skill) 0))
   :injected_count        (long (or (:skill/injected-count skill) 0))
   :viewed_count          (long (or (:skill/viewed-count skill) 0))
   :patched_count         (long (or (:skill/patched-count skill) 0))
   :last_used_at          (common/instant->str deps (:skill/last-used-at skill))
   :last_update_check_at  (common/instant->str deps (:skill/last-update-check-at skill))
   :last_update_status    (some-> (:skill/last-update-status skill) name)
   :last_update_source_sha256 (:skill/last-update-source-sha256 skill)
   :import_warnings       (->> (or (:skill/import-warnings skill) [])
                               sort
                               vec)
   :imported_from_openclaw (boolean (:skill/imported-from-openclaw? skill))})

(defn- skill->detail-body
  [deps skill]
  (assoc (skill->body deps skill)
         :content (:skill/content skill)))

(defn handle-save-skill
  [deps req]
  (try
    (let [data        (or (common/read-body deps req) {})
          skill-id    (if-let [id-text (common/nonblank-str (get data "id"))]
                        (common/parse-keyword-id id-text "id")
                        (infer-skill-id data))
          existing    (db/get-skill skill-id)
          skill-name  (or (common/nonblank-str (get data "name"))
                          (:skill/name existing)
                          (name skill-id))
          description (if (contains? data "description")
                        (or (common/nonblank-str (get data "description")) "")
                        (:skill/description existing))
          content     (if (contains? data "content")
                        (str (or (get data "content") ""))
                        (or (:skill/content existing) ""))
          version     (if (contains? data "version")
                        (common/nonblank-str (get data "version"))
                        (:skill/version existing))
          enabled?    (if (contains? data "enabled")
                        (true? (get data "enabled"))
                        (when (contains? existing :skill/enabled?)
                          (:skill/enabled? existing)))
          tags        (if (contains? data "tags")
                        (parse-skill-tags (get data "tags"))
                        nil)
          saved       (skill/save-skill! {:id          skill-id
                                          :name        skill-name
                                          :description description
                                          :content     content
                                          :version     version
                                          :tags        tags
                                          :enabled?    enabled?})]
      (common/json-response deps 200 {:skill (skill->detail-body deps saved)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-get-skill
  [deps skill-id]
  (try
    (let [skill-key (common/parse-keyword-id skill-id "skill_id")
          saved     (db/get-skill skill-key)]
      (if saved
        (do
          (skill/record-usage! skill-key :viewed)
          (common/json-response deps 200 {:skill (skill->detail-body deps (db/get-skill skill-key))}))
        (common/json-response deps 404 {:error "skill not found"})))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-delete-skill
  [deps skill-id]
  (try
    (let [skill-key (common/parse-keyword-id skill-id "skill_id")]
      (if (db/get-skill skill-key)
        (do
          (db/remove-skill! skill-key)
          (common/json-response deps 200 {:status "deleted"
                                          :skill_id (name skill-key)}))
        (common/json-response deps 404 {:error "skill not found"})))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn- skill-update-check->body
  [result]
  (cond-> {:status (some-> (:status result) name)
           :skill_id (some-> (:skill-id result) name)
           :source (:source result)
           :current_sha256 (:current_sha256 result)
           :imported_sha256 (:imported_sha256 result)
           :source_sha256 (:source_sha256 result)
           :safe_to_apply (boolean (:safe_to_apply? result))}
    (seq (:warnings result)) (assoc :warnings (vec (:warnings result)))
    (seq (:errors result)) (assoc :errors (vec (:errors result)))))

(defn handle-check-skill-update
  [deps skill-id]
  (try
    (let [skill-key (common/parse-keyword-id skill-id "skill_id")
          saved     (db/get-skill skill-key)]
      (if-not saved
        (common/json-response deps 404 {:error "skill not found"})
        (let [result (if (:skill/imported-from-openclaw? saved)
                       (openclaw-skill/check-openclaw-update! saved)
                       (skill/check-import-update! skill-key))]
          (common/json-response deps 200 {:update (skill-update-check->body result)
                                          :skill (skill->body deps (db/get-skill skill-key))}))))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn- curator-skill-summary->body
  [deps summary]
  (update summary :last_used_at #(common/instant->str deps %)))

(defn handle-curate-skills
  [deps req]
  (try
    (let [data (or (common/read-body deps req) {})
          stale-days (if-let [value (get data "stale_days")]
                       (Long/parseLong (str value))
                       skill/default-stale-days)
          archive-agent-authored? (if (contains? data "archive_agent_authored")
                                    (true? (get data "archive_agent_authored"))
                                    true)
          report (skill/curate-skills! {:stale-days stale-days
                                        :archive-agent-authored? archive-agent-authored?})]
      (common/json-response deps 200
                            {:curator {:stale (mapv #(curator-skill-summary->body deps %) (:stale report))
                                       :archived (mapv #(curator-skill-summary->body deps %) (:archived report))
                                       :suggestions (:suggestions report)}
                             :skills (mapv #(skill->body deps %) (db/list-skills))}))
    (catch NumberFormatException _
      (common/json-response deps 400 {:error "stale_days must be an integer"}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-import-openclaw-skill
  [deps req]
  (try
    (let [data    (or (common/read-body deps req) {})
          source  (common/nonblank-str (get data "source"))
          strict? (if (contains? data "strict")
                    (true? (get data "strict"))
                    true)]
      (when-not source
        (throw (ex-info "missing 'source' field" {:field "source"})))
      (let [report (openclaw-skill/import-openclaw-source! source :strict? strict?)
            skill  (db/get-skill (:skill-id report))]
        (common/json-response
         deps
         200
         {:import {:status         (some-> (:status report) name)
                   :skill_id       (some-> (:skill-id report) name)
                   :name           (:name report)
                   :warnings       (vec (:warnings report))
                   :ignored_fields (vec (:ignored-fields report))
                   :resources      (mapv (fn [{:keys [path size-bytes]}]
                                           {:path path
                                            :size_bytes size-bytes})
                                         (:resources report))
                   :tool_aliases   (mapv (fn [{:keys [id from to]}]
                                           {:id   (some-> id name)
                                            :from from
                                            :to   to})
                                         (:tool-aliases report))
                   :source         {:format (some-> (get-in report [:source :format]) name)
                                    :path   (get-in report [:source :path])
                                    :url    (get-in report [:source :url])
                                    :name   (get-in report [:source :name])}}
          :skill  (skill->body deps skill)})))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-skills
  [deps _req]
  (common/json-response deps 200 {:skills (mapv #(skill->body deps %) (db/list-skills))}))

(defn- proposal->body
  ([deps proposal]
   (proposal->body deps proposal false))
  ([deps proposal include-content?]
   (cond-> (skill-proposal/proposal-summary proposal)
     include-content? (assoc :content (:skill.proposal/content proposal))
     (:skill.proposal/created-at proposal)
     (assoc :created_at (common/instant->str deps (:skill.proposal/created-at proposal)))
     (:skill.proposal/updated-at proposal)
     (assoc :updated_at (common/instant->str deps (:skill.proposal/updated-at proposal)))
     (:skill.proposal/reviewed-at proposal)
     (assoc :reviewed_at (common/instant->str deps (:skill.proposal/reviewed-at proposal)))
     (:skill.proposal/applied-at proposal)
     (assoc :applied_at (common/instant->str deps (:skill.proposal/applied-at proposal))))))

(defn- proposal-data->attrs
  [data]
  {:id (get data "id")
   :task-id (get data "task_id")
   :op (get data "op")
   :skill-id (or (get data "skill_id")
                 (get data "skill-id"))
   :skill-name (or (get data "skill_name")
                   (get data "skill-name")
                   (get data "name"))
   :title (get data "title")
   :rationale (get data "rationale")
   :content (get data "content")
   :risk (get data "risk")
   :source (get data "source")
   :evidence (get data "evidence")})

(defn handle-skill-proposals
  [deps _req]
  (common/json-response
   deps
   200
   {:skill_proposals (mapv #(proposal->body deps %)
                            (skill-proposal/list-proposals))}))

(defn handle-create-skill-proposal
  [deps req]
  (try
    (let [data     (or (common/read-body deps req) {})
          proposal (skill-proposal/create-proposal! (proposal-data->attrs data))]
      (common/json-response deps 201 {:skill_proposal (proposal->body deps proposal true)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-approve-skill-proposal
  [deps proposal-id req]
  (try
    (let [data   (or (common/read-body deps req) {})
          result (skill-proposal/apply-proposal! proposal-id
                                                 :reviewer (get data "reviewer")
                                                 :note (get data "note")
                                                 :enable? (true? (get data "enable")))]
      (common/json-response
       deps
       200
       {:skill_proposal (proposal->body deps (:proposal result) true)
        :skill (skill->body deps (:skill result))}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-llm-review-skill-proposal
  [deps proposal-id req]
  (try
    (let [data   (or (common/read-body deps req) {})
          result (skill-proposal/review-proposal-with-llm!
                  proposal-id
                  :provider-id (common/nonblank-str (get data "provider_id"))
                  :workload (common/nonblank-str (get data "workload"))
                  :allow-enable? (true? (get data "allow_enable")))]
      (common/json-response
       deps
       200
       (cond-> {:decision (some-> (:decision result) name)
                :reason (:reason result)
                :skill_proposal (proposal->body deps (:proposal result) true)}
         (:skill result) (assoc :skill (skill->body deps (:skill result))))))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-reject-skill-proposal
  [deps proposal-id req]
  (try
    (let [data     (or (common/read-body deps req) {})
          proposal (skill-proposal/reject-proposal! proposal-id
                                                    :reviewer (get data "reviewer")
                                                    :note (get data "note"))]
      (common/json-response deps 200 {:skill_proposal (proposal->body deps proposal true)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))
