(ns xia.hippocampus-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [xia.test-helpers :as th]
            [xia.db :as db]
            [xia.memory :as memory]
            [xia.hippocampus :as hippo]))

(use-fixtures :each th/with-test-db)

;; ---------------------------------------------------------------------------
;; keywordize-props (private, test via merge-extraction! behavior)
;; ---------------------------------------------------------------------------

(deftest test-keywordize-props
  (let [kp #'xia.hippocampus/keywordize-props]
    (testing "flat string-keyed map"
      (is (= {:location "Seattle" :role "engineer"}
             (kp {"location" "Seattle" "role" "engineer"}))))

    (testing "nested string-keyed map"
      (is (= {:work {:company "Acme" :title "Engineer"}}
             (kp {"work" {"company" "Acme" "title" "Engineer"}}))))

    (testing "mixed nesting"
      (is (= {:name "Hong" :work {:company "Acme"} :age 30}
             (kp {"name" "Hong" "work" {"company" "Acme"} "age" 30}))))

    (testing "nil input"
      (is (nil? (kp nil))))

    (testing "non-map input"
      (is (nil? (kp "not a map"))))))

;; ---------------------------------------------------------------------------
;; fact-similar?
;; ---------------------------------------------------------------------------

(deftest test-fact-similar
  (let [fs? #'xia.hippocampus/fact-similar?]
    (testing "exact match"
      (is (true? (fs? "likes Clojure" "likes Clojure"))))

    (testing "case insensitive"
      (is (true? (fs? "Likes Clojure" "likes clojure"))))

    (testing "substring match"
      (is (true? (fs? "prefers functional programming" "functional programming"))))

    (testing "reverse substring"
      (is (true? (fs? "Clojure" "prefers Clojure for everything"))))

    (testing "no match"
      (is (false? (fs? "likes Python" "prefers Rust"))))

    (testing "whitespace trimming"
      (is (true? (fs? "  likes Clojure  " "likes Clojure"))))))

;; ---------------------------------------------------------------------------
;; dedup-fact!
;; ---------------------------------------------------------------------------

(deftest test-dedup-fact
  (let [node-eid (th/seed-node! "Hong" "person")
        ep-eid   (th/seed-episode! "test episode")]

    (testing "adds new fact when no similar exists"
      (#'xia.hippocampus/dedup-fact! node-eid "likes Clojure" ep-eid)
      (let [facts (memory/node-facts node-eid)]
        (is (= 1 (count facts)))
        (is (= "likes Clojure" (:content (first facts))))))

    (testing "updates timestamp instead of duplicating similar fact"
      (#'xia.hippocampus/dedup-fact! node-eid "likes Clojure" ep-eid)
      (let [facts (memory/node-facts node-eid)]
        (is (= 1 (count facts)) "Should still be 1 fact, not 2")))

    (testing "adds genuinely different fact"
      (#'xia.hippocampus/dedup-fact! node-eid "lives in Seattle" ep-eid)
      (let [facts (memory/node-facts node-eid)]
        (is (= 2 (count facts)))))))

;; ---------------------------------------------------------------------------
;; find-or-create-node!
;; ---------------------------------------------------------------------------

(deftest test-find-or-create-node
  (let [focn! #'xia.hippocampus/find-or-create-node!]
    (testing "creates new node when none exists"
      (let [eid (focn! "Alice" "person")]
        (is (some? eid))
        (is (= "Alice" (:kg.node/name (db/entity eid))))))

    (testing "finds existing node by exact name"
      (let [original-eid (focn! "Alice" "person")
            found-eid    (focn! "Alice" "person")]
        (is (= original-eid found-eid))))

    (testing "finds existing node case-insensitively"
      (let [original-eid (focn! "Alice" "person")
            found-eid    (focn! "alice" "person")]
        (is (= original-eid found-eid))))))

;; ---------------------------------------------------------------------------
;; merge-extraction! — full integration with properties
;; ---------------------------------------------------------------------------

(deftest test-merge-extraction-with-properties
  (let [ep-eid (th/seed-episode! "test episode")]
    (testing "merges entities with properties into KG"
      (#'xia.hippocampus/merge-extraction!
        {"entities" [{"name"       "Hong"
                      "type"       "person"
                      "facts"      ["software engineer" "prefers vim"]
                      "properties" {"location" "Seattle"
                                    "role"     "engineer"}}]
         "relations" []}
        ep-eid)
      (let [nodes (memory/find-node "Hong")]
        (is (= 1 (count nodes)))
        (let [node-eid (:eid (first nodes))
              props    (memory/node-properties node-eid)
              facts    (memory/node-facts node-eid)]
          (is (= "Seattle" (:location props)))
          (is (= "engineer" (:role props)))
          (is (= 2 (count facts))))))

    (testing "merges entities with nested properties"
      (#'xia.hippocampus/merge-extraction!
        {"entities" [{"name"       "Bob"
                      "type"       "person"
                      "facts"      []
                      "properties" {"work" {"company" "Acme" "title" "CTO"}}}]
         "relations" []}
        ep-eid)
      (let [node-eid (:eid (first (memory/find-node "Bob")))
            props    (memory/node-properties node-eid)]
        (is (= {:company "Acme" :title "CTO"} (:work props)))))

    (testing "creates edges between entities"
      (#'xia.hippocampus/merge-extraction!
        {"entities"  [{"name" "Carol" "type" "person"}
                      {"name" "Acme"  "type" "thing"}]
         "relations" [{"from" "Carol" "to" "Acme"
                        "type" "works-at" "label" "employed at Acme"}]}
        ep-eid)
      (let [carol-eid (:eid (first (memory/find-node "Carol")))
            edges     (memory/node-edges carol-eid)]
        (is (= 1 (count (:outgoing edges))))
        (is (= "Acme" (:target (first (:outgoing edges)))))
        (is (= :works-at (:type (first (:outgoing edges)))))))

    (testing "nil extraction is a no-op"
      (#'xia.hippocampus/merge-extraction! nil ep-eid))))

;; ---------------------------------------------------------------------------
;; consolidate-episode! (requires LLM mock)
;; ---------------------------------------------------------------------------

(deftest test-consolidate-episode-with-mock-llm
  (let [ep-eid (th/seed-episode! "Discussed Clojure web frameworks")]
    (with-redefs [xia.llm/chat-simple
                  (fn [_messages]
                    "{\"entities\": [{\"name\": \"Clojure\", \"type\": \"concept\", \"facts\": [\"functional JVM language\"], \"properties\": {\"paradigm\": \"functional\"}}], \"relations\": []}")]
      (hippo/consolidate-episode!
        {:eid     ep-eid
         :summary "Discussed Clojure web frameworks"
         :type    :conversation})
      ;; Verify the node was created with properties
      (let [nodes (memory/find-node "Clojure")]
        (is (= 1 (count nodes)))
        (let [node-eid (:eid (first nodes))
              props    (memory/node-properties node-eid)]
          (is (= "functional" (:paradigm props))))))))

;; ---------------------------------------------------------------------------
;; consolidate-pending!
;; ---------------------------------------------------------------------------

(deftest test-consolidate-pending
  (th/seed-episode! "Episode 1")
  (th/seed-episode! "Episode 2")
  (is (= 2 (count (memory/unprocessed-episodes))))
  (with-redefs [xia.llm/chat-simple
                (fn [_] "{\"entities\": [], \"relations\": []}")]
    ;; Consolidate each directly to surface any errors
    (doseq [ep (memory/unprocessed-episodes)]
      (hippo/consolidate-episode! ep))
    ;; All episodes should be marked processed
    (is (empty? (memory/unprocessed-episodes)))))

;; ---------------------------------------------------------------------------
;; maintain-knowledge! — confidence decay
;; ---------------------------------------------------------------------------

(deftest test-maintain-knowledge
  (let [node-eid      (th/seed-node! "TestEntity" "concept")
        fact-eid      (th/seed-fact! node-eid "some fact" :confidence 1.0)
        now-ms        (System/currentTimeMillis)
        ninety-days   (* 90 24 60 60 1000)
        now           (java.util.Date. now-ms)
        old-updated   (java.util.Date. (- now-ms ninety-days))
        expected-conf (max 0.1
                           (Math/pow 0.5 (/ (- ninety-days (* 7 24 60 60 1000))
                                            (double (* 60 24 60 60 1000)))))]
    (db/transact! [[:db/add fact-eid :kg.fact/updated-at old-updated]])
    (hippo/maintain-knowledge! now)
    (let [updated-conf (:kg.fact/confidence (db/entity fact-eid))
          decayed-at   (:kg.fact/decayed-at (db/entity fact-eid))]
      (is (< updated-conf 1.0) "Confidence should have decayed")
      (is (< (Math/abs (- updated-conf expected-conf)) 1.0e-3)
          "Confidence should follow the documented exponential half-life")
      (is (= now decayed-at) "Maintenance should record when decay was applied"))))

(deftest test-maintain-knowledge-is-idempotent-for-same-time
  (let [node-eid    (th/seed-node! "StableEntity" "concept")
        fact-eid    (th/seed-fact! node-eid "stable fact" :confidence 1.0)
        now-ms      (System/currentTimeMillis)
        now         (java.util.Date. now-ms)
        thirty-days (java.util.Date. (- now-ms (* 30 24 60 60 1000)))]
    (db/transact! [[:db/add fact-eid :kg.fact/updated-at thirty-days]])
    (hippo/maintain-knowledge! now)
    (let [once-conf (:kg.fact/confidence (db/entity fact-eid))]
      (hippo/maintain-knowledge! now)
      (let [twice-conf (:kg.fact/confidence (db/entity fact-eid))]
        (is (= once-conf twice-conf)
            "The same elapsed time should not be charged twice")))))

;; ---------------------------------------------------------------------------
;; consolidate-if-pending!
;; ---------------------------------------------------------------------------

(deftest test-consolidate-if-pending-below-threshold
  (testing "does nothing below threshold"
    (th/seed-episode! "Only one")
    (let [called? (atom false)]
      (with-redefs [xia.llm/chat-simple (fn [_] (reset! called? true) "{\"entities\": [], \"relations\": []}")]
        (hippo/consolidate-if-pending! :threshold 3)
        (is (false? @called?) "Should not trigger consolidation below threshold")))))

(deftest test-consolidate-if-pending-at-threshold
  (testing "triggers consolidation at threshold"
    (th/seed-episode! "One")
    (th/seed-episode! "Two")
    (th/seed-episode! "Three")
    (with-redefs [xia.llm/chat-simple (fn [_] "{\"entities\": [], \"relations\": []}")]
      (hippo/consolidate-if-pending! :threshold 3)
      (is (empty? (memory/unprocessed-episodes))))))
