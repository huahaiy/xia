(ns xia.memory-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [xia.test-helpers :as th]
            [xia.db :as db]
            [xia.memory :as memory]))

(use-fixtures :each th/with-test-db)

;; ---------------------------------------------------------------------------
;; Episodic Memory
;; ---------------------------------------------------------------------------

(deftest test-record-episode
  (testing "records and retrieves an episode"
    (memory/record-episode! {:type    :conversation
                             :summary "Discussed Clojure"
                             :context "Topic: web frameworks"})
    (let [episodes (memory/recent-episodes 10)]
      (is (= 1 (count episodes)))
      (is (= "Discussed Clojure" (:summary (first episodes))))
      (is (= "Topic: web frameworks" (:context (first episodes)))))))

(deftest test-unprocessed-episodes
  (testing "returns only unprocessed episodes"
    (memory/record-episode! {:summary "Episode 1"})
    (memory/record-episode! {:summary "Episode 2"})
    (let [unprocessed (memory/unprocessed-episodes)]
      (is (= 2 (count unprocessed))))
    ;; Mark one as processed
    (let [ep (first (memory/unprocessed-episodes))
          eid (:eid ep)]
      (memory/mark-episode-processed! eid)
      ;; Verify the entity was actually updated
      (let [entity (into {} (db/entity eid))]
        (is (true? (:episode/processed? entity))
            "Entity should be marked as processed")))
    (let [remaining (memory/unprocessed-episodes)]
      (is (= 1 (count remaining))))))

(deftest test-recent-episodes-ordering
  (testing "returns most recent first"
    (memory/record-episode! {:summary "First"})
    (Thread/sleep 10)
    (memory/record-episode! {:summary "Second"})
    (Thread/sleep 10)
    (memory/record-episode! {:summary "Third"})
    (let [episodes (memory/recent-episodes 2)]
      (is (= 2 (count episodes)))
      (is (= "Third" (:summary (first episodes)))))))

;; ---------------------------------------------------------------------------
;; Knowledge Graph — Nodes
;; ---------------------------------------------------------------------------

(deftest test-add-and-find-node
  (testing "creates a node and finds it by name"
    (memory/add-node! {:name "Clojure" :type :concept})
    (let [results (memory/find-node "Clojure")]
      (is (= 1 (count results)))
      (is (= "Clojure" (:name (first results))))
      (is (= :concept (:type (first results))))))

  (testing "case-insensitive search"
    (let [results (memory/find-node "clojure")]
      (is (= 1 (count results)))))

  (testing "substring match"
    (let [results (memory/find-node "Cloj")]
      (is (= 1 (count results))))))

(deftest test-get-node
  (let [node-eid (th/seed-node! "TestNode" "concept")]
    (let [node (memory/get-node node-eid)]
      (is (= "TestNode" (:kg.node/name node)))
      (is (= :concept (:kg.node/type node))))))

;; ---------------------------------------------------------------------------
;; Node Properties (idoc)
;; ---------------------------------------------------------------------------

(deftest test-node-properties
  (let [node-eid (th/seed-node! "Hong" "person")]
    (testing "initially nil"
      (is (nil? (memory/node-properties node-eid))))

    (testing "set first property creates map"
      (memory/set-node-property! node-eid [:location] "Seattle")
      (is (= "Seattle" (:location (memory/node-properties node-eid)))))

    (testing "set additional property patches map"
      (memory/set-node-property! node-eid [:role] "engineer")
      (let [props (memory/node-properties node-eid)]
        (is (= "Seattle" (:location props)))
        (is (= "engineer" (:role props)))))

    (testing "set nested property"
      (memory/set-node-property! node-eid [:work :company] "Acme")
      (is (= "Acme" (get-in (memory/node-properties node-eid) [:work :company]))))

    (testing "remove property"
      (memory/remove-node-property! node-eid [:role])
      (let [props (memory/node-properties node-eid)]
        (is (nil? (:role props)))
        (is (= "Seattle" (:location props)))))))

(deftest test-query-nodes-by-property
  (let [n1 (th/seed-node! "Alice" "person")
        n2 (th/seed-node! "Bob" "person")]
    (memory/set-node-property! n1 [:location] "Seattle")
    (memory/set-node-property! n2 [:location] "Portland")

    (testing "exact property match"
      (let [results (memory/query-nodes-by-property {:location "Seattle"})]
        (is (= 1 (count results)))
        (is (= "Alice" (:name (first results))))))

    (testing "no match returns empty"
      (is (empty? (memory/query-nodes-by-property {:location "Tokyo"}))))))

;; ---------------------------------------------------------------------------
;; Edges
;; ---------------------------------------------------------------------------

(deftest test-edges
  (let [n1 (th/seed-node! "Alice" "person")
        n2 (th/seed-node! "Acme" "thing")]
    (memory/add-edge! {:from-eid n1 :to-eid n2
                       :type :works-at :label "employed at Acme"})
    (testing "outgoing edges"
      (let [edges (memory/node-edges n1)]
        (is (= 1 (count (:outgoing edges))))
        (is (= "Acme" (:target (first (:outgoing edges)))))
        (is (= :works-at (:type (first (:outgoing edges)))))))

    (testing "incoming edges"
      (let [edges (memory/node-edges n2)]
        (is (= 1 (count (:incoming edges))))
        (is (= "Alice" (:source (first (:incoming edges)))))))))

;; ---------------------------------------------------------------------------
;; Facts
;; ---------------------------------------------------------------------------

(deftest test-facts
  (let [node-eid (th/seed-node! "Alice" "person")]
    (testing "add and retrieve facts"
      (memory/add-fact! {:node-eid node-eid :content "likes Clojure"})
      (memory/add-fact! {:node-eid node-eid :content "lives in Seattle" :confidence 0.8})
      (let [facts (memory/node-facts node-eid)]
        (is (= 2 (count facts)))
        ;; All facts should have content and confidence
        (is (every? :content facts))
        (is (every? :confidence facts))))))

(deftest test-node-facts-with-eids
  (let [node-eid (th/seed-node! "Bob" "person")]
    (memory/add-fact! {:node-eid node-eid :content "likes Python"})
    (let [facts (memory/node-facts-with-eids node-eid)]
      (is (= 1 (count facts)))
      (is (some? (:eid (first facts))))
      (is (= "likes Python" (:content (first facts)))))))

;; ---------------------------------------------------------------------------
;; Full-text Search
;; ---------------------------------------------------------------------------

(deftest test-search-nodes
  (th/seed-node! "Clojure" "concept")
  (th/seed-node! "ClojureScript" "concept")
  (th/seed-node! "Python" "concept")
  ;; Give FTS time to index
  (Thread/sleep 100)

  (testing "finds matching nodes"
    (let [results (memory/search-nodes "Clojure")]
      (is (pos? (count results)))
      (is (every? :name results)))))

(deftest test-search-facts
  (let [node-eid (th/seed-node! "TestNode" "concept")]
    (th/seed-fact! node-eid "runs on the JVM")
    (th/seed-fact! node-eid "has immutable data structures")
    (Thread/sleep 100)

    (testing "finds matching facts"
      (let [results (memory/search-facts "JVM")]
        (is (pos? (count results)))
        (is (= "runs on the JVM" (:content (first results))))))))

(deftest test-search-episodes
  (memory/record-episode! {:summary "Talked about functional programming"})
  (memory/record-episode! {:summary "Discussed cooking recipes"})
  (Thread/sleep 100)

  (testing "finds matching episodes"
    (let [results (memory/search-episodes "functional programming")]
      (is (pos? (count results)))
      (is (= "Talked about functional programming" (:summary (first results)))))))

(deftest test-search-edges
  (let [n1 (th/seed-node! "Alice" "person")
        n2 (th/seed-node! "Acme" "thing")]
    (memory/add-edge! {:from-eid n1 :to-eid n2
                       :type :works-at :label "employed at Acme Corp"})
    (Thread/sleep 100)

    (testing "finds matching edges by label"
      (let [results (memory/search-edges "Acme Corp")]
        (is (pos? (count results)))
        (is (= :works-at (:type (first results))))))))

;; ---------------------------------------------------------------------------
;; recall-knowledge
;; ---------------------------------------------------------------------------

(deftest test-recall-knowledge
  (th/seed-node! "Alice" "person")
  (th/seed-node! "Clojure" "concept")
  (th/seed-node! "Seattle" "place")

  (testing "returns nodes grouped by type"
    (let [knowledge (memory/recall-knowledge)]
      (is (map? knowledge))
      (is (contains? knowledge :person))
      (is (contains? knowledge :concept))
      (is (contains? knowledge :place))
      (is (some #{"Alice"} (:person knowledge))))))
