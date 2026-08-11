(ns xia.skill-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.skill :as skill]
            [xia.skill.proposal :as skill-proposal]
            [xia.test-helpers :as th])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(use-fixtures :each th/with-test-db)

(defn- temp-skill-file
  [filename content]
  (let [dir  (Files/createTempDirectory "xia-skill-test"
                                        (into-array FileAttribute []))
        path (.resolve dir filename)]
    (spit (str path) content)
    (str path)))

(deftest save-skill-records-provenance-hash-and-usage
  (let [content "# Curator Note\n\nKeep library metadata current."
        saved   (skill/save-skill! {:id :curator-note
                                    :name "Curator Note"
                                    :content content
                                    :trust-level :agent-authored})]
    (is (= :agent-authored (:skill/trust-level saved)))
    (is (= :active (:skill/lifecycle saved)))
    (is (= :xia-local (:skill/source-format saved)))
    (is (= (skill/content-sha256 content)
           (:skill/content-sha256 saved)))
    (skill/record-usage! :curator-note :selected)
    (skill/record-usage! :curator-note :injected)
    (skill/record-usage! :curator-note :viewed)
    (skill/record-usage! :curator-note :patched)
    (let [updated (db/get-skill :curator-note)]
      (is (= 1 (:skill/selected-count updated)))
      (is (= 1 (:skill/injected-count updated)))
      (is (= 1 (:skill/viewed-count updated)))
      (is (= 1 (:skill/patched-count updated)))
      (is (some? (:skill/last-used-at updated))))))

(deftest imported-file-update-check-is-safe
  (let [path "# Maintenance Skill\n\nUse old process."
        file (temp-skill-file "maintenance-skill.md" path)
        _    (skill/import-skill-file! file)]
    (is (= :current (:status (skill/check-import-update! :maintenance-skill))))
    (spit file "# Maintenance Skill\n\nUse new process.")
    (is (= :update-available (:status (skill/check-import-update! :maintenance-skill))))
    (skill/save-skill! {:id :maintenance-skill
                        :name "Maintenance Skill"
                        :content "# Maintenance Skill\n\nLocal edits."})
    (is (= :local-edits (:status (skill/check-import-update! :maintenance-skill))))))

(deftest curator-marks-stale-and-only-archives-agent-authored-skills
  (skill/save-skill! {:id :agent-draft
                      :name "Agent Draft"
                      :content "# Agent Draft\n\nTemporary instructions."
                      :tags #{:drafting :email}
                      :trust-level :agent-authored})
  (skill/save-skill! {:id :user-draft
                      :name "User Draft"
                      :content "# User Draft\n\nOwned instructions."
                      :tags #{:drafting :email}
                      :trust-level :user-authored})
  (let [report (skill/curate-skills! {:stale-days 0})
        stale-ids (set (map :id (:stale report)))
        archived-ids (set (map :id (:archived report)))
        agent-skill (db/get-skill :agent-draft)
        user-skill (db/get-skill :user-draft)]
    (is (contains? stale-ids "agent-draft"))
    (is (contains? stale-ids "user-draft"))
    (is (contains? archived-ids "agent-draft"))
    (is (not (contains? archived-ids "user-draft")))
    (is (= :archived (:skill/lifecycle agent-skill)))
    (is (false? (:skill/enabled? agent-skill)))
    (is (= :stale (:skill/lifecycle user-skill)))
    (is (true? (:skill/enabled? user-skill)))
    (is (seq (:suggestions report)))))

(deftest approved-create-proposal-saves-disabled-agent-authored-draft
  (let [session-id (db/create-session! :terminal)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :task
                                     :state :completed
                                     :title "Reply to billing dispute"
                                     :summary "Used a reusable billing follow-up structure."})
        proposal   (skill-proposal/create-proposal!
                    {:task-id task-id
                     :op :create
                     :skill-id :billing-follow-up
                     :skill-name "Billing Follow-up"
                     :title "Create billing follow-up skill"
                     :rationale "The task produced a reusable follow-up structure."
                     :content "# Billing Follow-up\n\nUse a concise dispute summary."
                     :risk :low})
        result     (skill-proposal/apply-proposal! (:skill.proposal/id proposal)
                                                   :reviewer :user
                                                   :note "Looks reusable.")
        saved      (db/get-skill :billing-follow-up)
        reviewed   (:proposal result)]
    (is (= :applied (:skill.proposal/status reviewed)))
    (is (= false (:skill/enabled? saved)))
    (is (= :agent-authored (:skill/trust-level saved)))
    (is (= :skill-proposal (:skill/source-format saved)))
    (is (= :skill-proposal (get-in saved [:skill/provenance :origin])))
    (is (= "Looks reusable." (:skill.proposal/review-note reviewed)))
    (is (= 1 (count (skill-proposal/proposals-for-task task-id))))))

(deftest patch-proposal-refuses-to-apply-after-source-skill-changed
  (let [original "# Email Drafting\n\nUse a concise tone."
        saved    (skill/save-skill! {:id :email-drafting
                                     :name "Email Drafting"
                                     :content original})
        proposal (skill-proposal/create-proposal!
                  {:op :patch
                   :skill-id :email-drafting
                   :title "Patch email drafting skill"
                   :rationale "Add escalation wording."
                   :content "# Email Drafting\n\nUse a concise tone and clear escalation wording."
                   :source {:skill {:id "email-drafting"
                                    :content-sha256 (:skill/content-sha256 saved)}}})]
    (skill/save-skill! {:id :email-drafting
                        :name "Email Drafting"
                        :content "# Email Drafting\n\nUse a warm tone."})
    (let [ex (try
               (skill-proposal/apply-proposal! (:skill.proposal/id proposal))
               nil
               (catch clojure.lang.ExceptionInfo e
                 e))]
      (is (= :skill-proposal/source-skill-changed (:type (ex-data ex))))
      (is (= :pending (:skill.proposal/status
                       (skill-proposal/get-proposal (:skill.proposal/id proposal))))))))

(deftest task-reflection-generates-pending-skill-proposals
  (let [skill*    (skill/save-skill! {:id :email-drafting
                                      :name "Email Drafting"
                                      :content "# Email Drafting\n\nUse a concise tone."})
        task-id   (db/create-task! {:channel :terminal
                                    :type :task
                                    :state :completed
                                    :title "Reply to billing dispute"
                                    :summary "Used clearer escalation language."
                                    :contract {:kind :task
                                               :version 1
                                               :goal "Reply to billing dispute"
                                               :skills [{:id :email-drafting
                                                         :version (:skill/version skill*)
                                                         :content-sha256 (:skill/content-sha256 skill*)}]}})]
    (with-redefs [llm/chat-message
                  (fn [& _]
                    {"content" "{\"proposals\":[{\"op\":\"patch\",\"skill_id\":\"email-drafting\",\"title\":\"Add escalation wording\",\"rationale\":\"The task found reusable escalation phrasing.\",\"content\":\"# Email Drafting\\n\\nUse concise tone and clear escalation wording.\",\"risk\":\"medium\"}]}"})]
      (let [proposals (skill-proposal/generate-proposals-for-task! task-id)
            proposal  (first proposals)]
        (is (= 1 (count proposals)))
        (is (= :pending (:skill.proposal/status proposal)))
        (is (= :patch (:skill.proposal/op proposal)))
        (is (= :email-drafting (:skill.proposal/skill-id proposal)))
        (is (= (:skill/content-sha256 skill*)
               (get-in proposal [:skill.proposal/source :skill :content-sha256])))
        (is (= "Use a concise tone." (second (clojure.string/split (:skill/content (db/get-skill :email-drafting))
                                                                   #"\n\n"))))))))

(deftest llm-review-applies-agent-authored-patch
  (let [saved    (skill/save-skill! {:id :agent-email-drafting
                                     :name "Agent Email Drafting"
                                     :content "# Agent Email Drafting\n\nUse a concise tone."
                                     :trust-level :agent-authored})
        proposal (skill-proposal/create-proposal!
                  {:op :patch
                   :skill-id :agent-email-drafting
                   :title "Patch agent email drafting"
                   :rationale "Add reusable escalation phrasing."
                   :content "# Agent Email Drafting\n\nUse a concise tone and clear escalation phrasing."
                   :risk :low
                   :source {:skill {:id "agent-email-drafting"
                                    :content-sha256 (:skill/content-sha256 saved)}}})]
    (with-redefs [llm/chat-message
                  (fn [& _]
                    {"content" "{\"decision\":\"approve\",\"reason\":\"Reusable and safe.\"}"})]
      (let [result  (skill-proposal/review-proposal-with-llm! (:skill.proposal/id proposal))
            updated (db/get-skill :agent-email-drafting)]
        (is (= :approved (:decision result)))
        (is (= :applied (:skill.proposal/status (:proposal result))))
        (is (= "llm" (:skill.proposal/reviewer (:proposal result))))
        (is (= "Reusable and safe." (:skill.proposal/review-note (:proposal result))))
        (is (= "# Agent Email Drafting\n\nUse a concise tone and clear escalation phrasing."
               (:skill/content updated)))
        (is (= 1 (:skill/patched-count updated)))))))

(deftest llm-review-rejects-pending-agent-authored-proposal
  (let [saved    (skill/save-skill! {:id :agent-email-drafting
                                     :name "Agent Email Drafting"
                                     :content "# Agent Email Drafting\n\nUse a concise tone."
                                     :trust-level :agent-authored})
        proposal (skill-proposal/create-proposal!
                  {:op :patch
                   :skill-id :agent-email-drafting
                   :title "Patch agent email drafting"
                   :rationale "Add wording that might be too task-specific."
                   :content "# Agent Email Drafting\n\nMention the Acme invoice exception."
                   :risk :medium
                   :source {:skill {:id "agent-email-drafting"
                                    :content-sha256 (:skill/content-sha256 saved)}}})]
    (with-redefs [llm/chat-message
                  (fn [& _]
                    {"content" "{\"decision\":\"reject\",\"reason\":\"Contains one-off task details.\"}"})]
      (let [result  (skill-proposal/review-proposal-with-llm! (:skill.proposal/id proposal))
            updated (db/get-skill :agent-email-drafting)]
        (is (= :rejected (:decision result)))
        (is (= :rejected (:skill.proposal/status (:proposal result))))
        (is (= "llm" (:skill.proposal/reviewer (:proposal result))))
        (is (= "Contains one-off task details." (:skill.proposal/review-note (:proposal result))))
        (is (= "# Agent Email Drafting\n\nUse a concise tone."
               (:skill/content updated)))))))

(deftest llm-review-refuses-user-authored-patch
  (let [called?  (atom false)
        saved    (skill/save-skill! {:id :user-email-drafting
                                     :name "User Email Drafting"
                                     :content "# User Email Drafting\n\nUse my preferred tone."})
        proposal (skill-proposal/create-proposal!
                  {:op :patch
                   :skill-id :user-email-drafting
                   :title "Patch user email drafting"
                   :rationale "Add reusable escalation phrasing."
                   :content "# User Email Drafting\n\nUse my preferred tone and escalation phrasing."
                   :risk :low
                   :source {:skill {:id "user-email-drafting"
                                    :content-sha256 (:skill/content-sha256 saved)}}})
        ex       (with-redefs [llm/chat-message
                               (fn [& _]
                                 (reset! called? true)
                                 {"content" "{\"decision\":\"approve\",\"reason\":\"safe\"}"})]
                   (try
                     (skill-proposal/review-proposal-with-llm! (:skill.proposal/id proposal))
                     nil
                     (catch clojure.lang.ExceptionInfo e
                       e)))]
    (is (= :skill-proposal/llm-review-not-eligible (:type (ex-data ex))))
    (is (= :non-agent-authored-skill (:reason (ex-data ex))))
    (is (= false @called?))
    (is (= :pending (:skill.proposal/status
                     (skill-proposal/get-proposal (:skill.proposal/id proposal)))))
    (is (= "# User Email Drafting\n\nUse my preferred tone."
           (:skill/content (db/get-skill :user-email-drafting))))))

(deftest task-reflection-can-llm-review-generated-agent-authored-patches
  (let [skill*  (skill/save-skill! {:id :agent-email-drafting
                                    :name "Agent Email Drafting"
                                    :content "# Agent Email Drafting\n\nUse a concise tone."
                                    :trust-level :agent-authored})
        task-id (db/create-task! {:channel :terminal
                                  :type :task
                                  :state :completed
                                  :title "Reply to billing dispute"
                                  :summary "Used clearer escalation language."
                                  :contract {:kind :task
                                             :version 1
                                             :goal "Reply to billing dispute"
                                             :skills [{:id :agent-email-drafting
                                                       :version (:skill/version skill*)
                                                       :content-sha256 (:skill/content-sha256 skill*)}]}})]
    (with-redefs [llm/chat-message
                  (fn [messages & _]
                    (let [payload (get (second messages) "content")]
                      (if (clojure.string/includes? payload "skill-improvement-reflection-request")
                        {"content" "{\"proposals\":[{\"op\":\"patch\",\"skill_id\":\"agent-email-drafting\",\"title\":\"Add escalation wording\",\"rationale\":\"The task found reusable escalation phrasing.\",\"content\":\"# Agent Email Drafting\\n\\nUse concise tone and clear escalation wording.\",\"risk\":\"low\"}]}"}
                        {"content" "{\"decision\":\"approve\",\"reason\":\"Reusable and safe.\"}"})))]
      (let [result  (skill-proposal/generate-and-review-proposals-for-task! task-id)
            review  (first (:reviews result))
            saved*  (db/get-skill :agent-email-drafting)]
        (is (= 1 (count (:proposals result))))
        (is (= :approved (:decision review)))
        (is (= :applied (:skill.proposal/status (:proposal review))))
        (is (= "llm" (:skill.proposal/reviewer (:proposal review))))
        (is (= "# Agent Email Drafting\n\nUse concise tone and clear escalation wording."
               (:skill/content saved*)))))))
