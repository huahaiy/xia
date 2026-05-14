(ns xia.skill-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.db :as db]
            [xia.skill :as skill]
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
