(ns xia.context-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.string :as str]
            [xia.test-helpers :as th]
            [xia.db :as db]
            [xia.context :as ctx]))

(use-fixtures :each th/with-test-db)

;; ---------------------------------------------------------------------------
;; Token estimation
;; ---------------------------------------------------------------------------

(deftest test-estimate-tokens
  (is (= 0 (ctx/estimate-tokens "")))
  (is (= 0 (ctx/estimate-tokens nil)))
  (is (= 3 (ctx/estimate-tokens "hello world!"))) ; 12 chars / 4
  (is (pos? (ctx/estimate-tokens "some text"))))

;; ---------------------------------------------------------------------------
;; flatten-props
;; ---------------------------------------------------------------------------

(deftest test-flatten-props
  (let [fp #'xia.context/flatten-props]
    (testing "flat properties"
      (is (= ["location: Seattle" "role: engineer"]
             (sort (fp {:location "Seattle" :role "engineer"})))))

    (testing "nested properties"
      (let [result (fp {:work {:company "Acme" :title "CTO"}})]
        (is (some #(= "work.company: Acme" %) result))
        (is (some #(= "work.title: CTO" %) result))))

    (testing "mixed flat and nested"
      (let [result (fp {:location "Seattle" :work {:company "Acme"}})]
        (is (some #(= "location: Seattle" %) result))
        (is (some #(= "work.company: Acme" %) result))))

    (testing "empty map"
      (is (= [] (fp {}))))

    (testing "deeply nested"
      (let [result (fp {:a {:b {:c "deep"}}})]
        (is (= ["a.b.c: deep"] result))))))

;; ---------------------------------------------------------------------------
;; render-entity
;; ---------------------------------------------------------------------------

(deftest test-render-entity
  (let [re #'xia.context/render-entity]
    (testing "entity with properties, facts, and edges"
      (let [line (re {:name       "Hong"
                      :type       :person
                      :properties {:location "Seattle" :role "engineer"}
                      :facts      [{:content "prefers vim" :confidence 1.0}]
                      :edges      {:outgoing [{:type :works-at :target "Acme"}]
                                   :incoming []}})]
        (is (str/starts-with? line "- Hong (person): "))
        (is (str/includes? line "location: Seattle"))
        (is (str/includes? line "role: engineer"))
        (is (str/includes? line "prefers vim"))
        (is (str/includes? line "works-at→Acme"))))

    (testing "entity with only properties"
      (let [line (re {:name       "Seattle"
                      :type       :place
                      :properties {:state "Washington"}
                      :facts      []
                      :edges      {:outgoing [] :incoming []}})]
        (is (str/includes? line "state: Washington"))))

    (testing "entity with no properties"
      (let [line (re {:name  "Clojure"
                      :type  :concept
                      :facts [{:content "functional language" :confidence 1.0}]
                      :edges {:outgoing [] :incoming []}})]
        (is (str/includes? line "functional language"))
        (is (not (str/includes? line "nil")))))

    (testing "entity with nil properties"
      (let [line (re {:name       "Bob"
                      :type       :person
                      :properties nil
                      :facts      [{:content "likes pizza" :confidence 0.8}]
                      :edges      {:outgoing [] :incoming []}})]
        (is (str/includes? line "likes pizza"))))

    (testing "entity with empty properties"
      (let [line (re {:name       "Carol"
                      :type       :person
                      :properties {}
                      :facts      [{:content "works remotely" :confidence 0.9}]
                      :edges      {:outgoing [] :incoming []}})]
        (is (str/includes? line "works remotely"))))

    (testing "low-confidence facts are filtered"
      (let [line (re {:name  "Test"
                      :type  :concept
                      :facts [{:content "high conf" :confidence 0.8}
                              {:content "low conf"  :confidence 0.2}]
                      :edges {:outgoing [] :incoming []}})]
        (is (str/includes? line "high conf"))
        (is (not (str/includes? line "low conf")))))

    (testing "entity with no detail"
      (let [line (re {:name "Empty"
                      :type :concept
                      :facts []
                      :edges {:outgoing [] :incoming []}})]
        (is (= "- Empty (concept)" line))))))

;; ---------------------------------------------------------------------------
;; render-entities (budget-aware)
;; ---------------------------------------------------------------------------

(deftest test-render-entities-budget
  (testing "renders within budget"
    (let [entities [{:name "A" :type :concept :facts [] :edges {:outgoing [] :incoming []}}
                    {:name "B" :type :concept :facts [] :edges {:outgoing [] :incoming []}}]
          result (ctx/render-entities entities 1000)]
      (is (str/includes? result "### Known"))
      (is (str/includes? result "- A"))
      (is (str/includes? result "- B"))))

  (testing "truncates when budget exceeded"
    (let [entities (mapv (fn [i]
                           {:name  (str "Entity" i)
                            :type  :concept
                            :facts [{:content (apply str (repeat 100 "x")) :confidence 1.0}]
                            :edges {:outgoing [] :incoming []}})
                         (range 20))
          result (ctx/render-entities entities 50)]
      ;; Should have some entities but not all 20
      (is (str/includes? result "### Known"))
      (is (< (count (re-seq #"- Entity" result)) 20))))

  (testing "nil entities"
    (is (nil? (ctx/render-entities nil 1000))))

  (testing "empty entities"
    (is (nil? (ctx/render-entities [] 1000)))))

;; ---------------------------------------------------------------------------
;; render-episodes
;; ---------------------------------------------------------------------------

(deftest test-render-episodes
  (let [re #'xia.context/render-episodes]
    (testing "renders episodes with dates"
      (let [episodes [{:summary   "Discussed Clojure"
                       :timestamp (java.util.Date.)
                       :relevance 0.8}]
            result (re episodes 500)]
        (is (str/includes? result "### Recent"))
        (is (str/includes? result "Discussed Clojure"))))

    (testing "empty episodes"
      (is (nil? (re [] 500))))

    (testing "nil episodes"
      (is (nil? (re nil 500))))))

;; ---------------------------------------------------------------------------
;; render-skills
;; ---------------------------------------------------------------------------

(deftest test-render-skills
  (let [rs #'xia.context/render-skills]
    (testing "renders skills"
      (let [skills [{:skill/name "email-drafting" :skill/content "Write emails professionally."}]
            result (rs skills 1000)]
        (is (str/includes? result "## Skills"))
        (is (str/includes? result "### email-drafting"))
        (is (str/includes? result "Write emails professionally."))))

    (testing "budget-aware truncation"
      (let [skills (mapv (fn [i]
                           {:skill/name    (str "skill-" i)
                            :skill/content (apply str (repeat 500 "x"))})
                         (range 10))
            result (rs skills 200)]
        ;; Should not include all 10 skills
        (is (< (count (re-seq #"### skill-" result)) 10))))

    (testing "empty skills"
      (is (nil? (rs [] 1000))))))

;; ---------------------------------------------------------------------------
;; assemble-system-prompt (integration)
;; ---------------------------------------------------------------------------

(deftest test-assemble-system-prompt
  ;; Set up identity
  (db/set-identity! :name "TestXia")
  (db/set-identity! :personality "Helpful")
  (db/set-identity! :guidelines "Be nice")
  (db/set-identity! :description "A test assistant")

  ;; No WM active, no skills
  (let [prompt (ctx/assemble-system-prompt)]
    (testing "includes identity"
      (is (str/includes? prompt "TestXia")))

    (testing "is a string"
      (is (string? prompt)))))

;; ---------------------------------------------------------------------------
;; compact-history
;; ---------------------------------------------------------------------------

(deftest test-compact-history
  (testing "short history passes through"
    (let [msgs [{:role :user :content "hi"}
                {:role :assistant :content "hello"}]
          result (ctx/compact-history msgs 10000)]
      (is (= msgs result))))

  (testing "few messages pass through regardless of budget"
    (let [msgs [{:role :user :content (apply str (repeat 5000 "x"))}
                {:role :assistant :content (apply str (repeat 5000 "x"))}]
          result (ctx/compact-history msgs 10)]
      (is (= msgs result) "Should not compact when <= 4 messages")))

  (testing "compacts long history via LLM"
    (let [msgs (vec (for [i (range 10)]
                      {:role    (if (even? i) :user :assistant)
                       :content (str "message " i " " (apply str (repeat 200 "x")))}))
          called? (atom false)]
      (with-redefs [xia.llm/chat-simple (fn [_]
                                          (reset! called? true)
                                          "Recap of earlier conversation.")]
        (let [result (ctx/compact-history msgs 100)]
          (is @called? "LLM should have been called for compaction")
          (is (< (count result) (count msgs)) "Should have fewer messages"))))))

;; ---------------------------------------------------------------------------
;; build-messages
;; ---------------------------------------------------------------------------

(deftest test-build-messages
  ;; Set up identity
  (db/set-identity! :name "TestXia")
  (db/set-identity! :description "Test")

  (let [sid (db/create-session! :terminal)]
    (db/add-message! sid :user "hello")
    (db/add-message! sid :assistant "hi there")

    (let [msgs (ctx/build-messages sid "what's up?")]
      (testing "starts with system message"
        (is (= "system" (:role (first msgs)))))

      (testing "ends with user message"
        (is (= "user" (:role (last msgs))))
        (is (= "what's up?" (:content (last msgs)))))

      (testing "includes history"
        (is (>= (count msgs) 3))))))
