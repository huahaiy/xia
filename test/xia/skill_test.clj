(ns xia.skill-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.string :as str]
            [xia.test-helpers :as th]
            [xia.db :as db]
            [xia.skill :as skill]))

(use-fixtures :each th/with-test-db)

;; ---------------------------------------------------------------------------
;; import-skill-edn!
;; ---------------------------------------------------------------------------

(deftest test-import-skill-edn
  (testing "imports basic skill"
    (skill/import-skill-edn!
      {:id      :test-skill
       :name    "Test Skill"
       :content "## Steps\nDo the thing."
       :tags    #{:test}})
    (let [s (db/get-skill :test-skill)]
      (is (some? s))
      (is (= "Test Skill" (:skill/name s)))
      (is (true? (:skill/enabled? s)))
      (is (str/includes? (:skill/content s) "Do the thing."))))

  (testing "requires :id"
    (is (thrown? Exception
          (skill/import-skill-edn! {:name "No ID" :content "test"}))))

  (testing "requires :content"
    (is (thrown? Exception
          (skill/import-skill-edn! {:id :no-content :name "No Content"})))))

(deftest test-import-skill-with-idoc
  (testing "skill with valid markdown gets idoc index"
    (skill/import-skill-edn!
      {:id      :email-drafting
       :name    "Email Drafting"
       :content "# Email Drafting\n\n## Tone\n\nBe professional and warm.\n\n## Structure\n\nStart with greeting."})
    (let [s (db/get-skill :email-drafting)]
      (is (some? (:skill/doc s)) "Should have idoc document"))))

(deftest test-import-skill-file-rejects-reader-eval
  (let [path (doto (java.io.File/createTempFile "xia-skill" ".edn")
               (.deleteOnExit))]
    (spit path "#=(+ 1 2)")
    (is (thrown-with-msg?
          RuntimeException
          #"No dispatch macro for: ="
          (skill/import-skill-file! (.getAbsolutePath path))))))

(deftest test-import-skill-mixed-content-fallback
  (testing "skill with mixed content/subheaders falls back gracefully"
    ;; This would have both content AND subheaders under one heading
    ;; which idoc doesn't allow — should still import without idoc
    (skill/import-skill-edn!
      {:id      :mixed-skill
       :name    "Mixed Skill"
       :content "# Mixed Skill\nSome content here.\n## Sub Heading\nMore content."})
    (let [s (db/get-skill :mixed-skill)]
      (is (some? s) "Skill should still be imported")
      (is (= "Mixed Skill" (:skill/name s))))))

;; ---------------------------------------------------------------------------
;; search-skills (FTS)
;; ---------------------------------------------------------------------------

(deftest test-search-skills
  (skill/import-skill-edn!
    {:id :cooking :name "Cooking" :content "# Cooking\n\n## Recipes\n\nPasta carbonara instructions."})
  (skill/import-skill-edn!
    {:id :coding :name "Coding" :content "# Coding\n\n## Guidelines\n\nWrite clean code."})

  (Thread/sleep 100)

  (testing "finds skill by content keyword"
    (let [results (skill/search-skills "pasta")]
      (is (pos? (count results)))
      (is (= :cooking (:skill/id (first results))))))

  (testing "blank query returns nil"
    (is (nil? (skill/search-skills ""))))

  (testing "nil query returns nil"
    (is (nil? (skill/search-skills nil)))))

;; ---------------------------------------------------------------------------
;; skill-section (idoc get-in)
;; ---------------------------------------------------------------------------

(deftest test-skill-section
  (skill/import-skill-edn!
    {:id      :email
     :name    "Email"
     :content "# Email\n\n## Tone\n\nBe professional.\n\n## Structure\n\nStart with greeting."})

  (testing "extracts section content"
    (let [tone (skill/skill-section :email :email :tone)]
      (is (some? tone))
      (is (str/includes? (str tone) "professional"))))

  (testing "returns nil for nonexistent section"
    (is (nil? (skill/skill-section :email :email :nonexistent))))

  (testing "returns nil for nonexistent skill"
    (is (nil? (skill/skill-section :nonexistent :anything)))))

;; ---------------------------------------------------------------------------
;; skill-headings
;; ---------------------------------------------------------------------------

(deftest test-skill-headings
  (skill/import-skill-edn!
    {:id      :guide
     :name    "Guide"
     :content "# Guide\n\n## Introduction\n\nHello.\n\n## Steps\n\nDo things."})

  (testing "returns heading structure"
    (let [headings (skill/skill-headings :guide)]
      (is (map? headings))
      (is (contains? headings :guide))))

  (testing "returns nil for skill without idoc"
    (is (nil? (skill/skill-headings :nonexistent)))))

;; ---------------------------------------------------------------------------
;; skills-for-context
;; ---------------------------------------------------------------------------

(deftest test-skills-for-context
  (skill/import-skill-edn!
    {:id :email-skill :name "Email" :content "# Email\n\n## Rules\n\nDraft professional emails." :tags #{:email}})
  (skill/import-skill-edn!
    {:id :code-skill :name "Code" :content "# Code\n\n## Rules\n\nWrite clean code." :tags #{:code}})

  (Thread/sleep 100)

  (testing "returns all enabled skills when no WM context"
    (let [skills (skill/skills-for-context)]
      (is (>= (count skills) 2))))

  (testing "returns relevant skills when WM context has topics"
    (let [skills (skill/skills-for-context {:topics "drafting an email"})]
      (is (pos? (count skills)))
      (is (= :email-skill (:skill/id (first skills))))
      (is (> (:skill/relevance (first skills))
             (or (:skill/relevance (second skills)) 0.0))))))

;; ---------------------------------------------------------------------------
;; skills->prompt
;; ---------------------------------------------------------------------------

(deftest test-skills-to-prompt
  (testing "formats skills into prompt"
    (let [skills [{:skill/name "Email" :skill/content "Draft emails."}
                  {:skill/name "Code" :skill/content "Write code."}]
          result (skill/skills->prompt skills)]
      (is (str/includes? result "## Skills"))
      (is (str/includes? result "### Email"))
      (is (str/includes? result "Draft emails."))
      (is (str/includes? result "### Code"))))

  (testing "nil for empty skills"
    (is (nil? (skill/skills->prompt []))))

  (testing "nil for nil skills"
    (is (nil? (skill/skills->prompt nil)))))
