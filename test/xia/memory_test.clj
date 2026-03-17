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
    (let [remaining (with-redefs [xia.db/entity
                                  (fn [_]
                                    (throw (ex-info "unprocessed-episodes should not call db/entity"
                                                    {})))]
                      (memory/unprocessed-episodes))]
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

(deftest test-episode-retention-settings-use-config-overrides
  (db/set-config! :memory/episode-full-resolution-ms (* 730 24 60 60 1000))
  (db/set-config! :memory/episode-decay-half-life-ms (* 180 24 60 60 1000))
  (db/set-config! :memory/episode-retained-decayed-count 12)
  (let [settings (memory/episode-retention-settings)]
    (is (= (* 730 24 60 60 1000) (:full-resolution-ms settings)))
    (is (= (* 180 24 60 60 1000) (:decay-half-life-ms settings)))
    (is (= 12 (:retained-decayed-count settings)))))

(deftest test-prune-processed-episodes-does-timestamp-downsampling
  (let [config     (memory/episode-retention-settings)
        window-ms  (:full-resolution-ms config)
        day-ms     (* 24 60 60 1000)
        now        (java.util.Date. (* 5 window-ms))
        sid-a      (random-uuid)
        sid-b      (random-uuid)]
    (th/seed-episode! "recent-1"
                      :processed? true
                      :session-id sid-a
                      :channel :terminal
                      :timestamp (java.util.Date. (- (.getTime now) (* 90 day-ms))))
    (th/seed-episode! "recent-2"
                      :processed? true
                      :session-id sid-a
                      :channel :terminal
                      :timestamp (java.util.Date. (- (.getTime now) (* 180 day-ms))))
    (doseq [idx (range 9)]
      (th/seed-episode! (str "older-" idx)
                        :processed? true
                        :session-id sid-a
                        :channel :terminal
                        :timestamp (java.util.Date. (- (.getTime now)
                                                       (+ (* 2 window-ms)
                                                          (* idx day-ms))))))
    (th/seed-episode! "other-session-keep"
                      :processed? true
                      :session-id sid-b
                      :channel :terminal
                      :timestamp (java.util.Date. (- (.getTime now) (+ (* 2 window-ms)
                                                                       (* 3 day-ms)))))
    (th/seed-episode! "pending-old"
                      :processed? false
                      :session-id sid-a
                      :channel :terminal
                      :timestamp (java.util.Date. (- (.getTime now) (+ (* 2 window-ms)
                                                                       (* 120 day-ms)))))
    (is (= 1 (memory/prune-processed-episodes! now)))
    (is (= #{"recent-1"
             "recent-2"
             "older-0"
             "older-1"
             "older-2"
             "older-3"
             "older-4"
             "older-5"
             "older-6"
             "older-7"
             "other-session-keep"
             "pending-old"}
           (->> (db/q '[:find ?summary :where [?e :episode/summary ?summary]])
                (map first)
                set)))))

(deftest test-prune-processed-episodes-clears-provenance-before-deletion
  (let [window-ms (:full-resolution-ms (memory/episode-retention-settings))
        now       (java.util.Date. (* 10 window-ms))
        sid       (random-uuid)
        day-ms    (* 24 60 60 1000)
        old-ep    (th/seed-episode! "old-processed"
                                    :processed? true
                                    :session-id sid
                                    :channel :terminal
                                    :timestamp (java.util.Date. (- (.getTime now) (+ (* 2 window-ms)
                                                                                     (* 20 day-ms)))))
        alice     (th/seed-node! "Alice" "person")
        acme      (th/seed-node! "Acme" "thing")]
    (doseq [idx (range 8)]
      (th/seed-episode! (str "keep-processed-" idx)
                        :processed? true
                        :session-id sid
                        :channel :terminal
                        :timestamp (java.util.Date. (- (.getTime now)
                                                       (+ (* 2 window-ms)
                                                          (* idx day-ms))))))
    (memory/add-fact! {:node-eid alice
                       :content "worked on legacy system"
                       :source-eid old-ep})
    (memory/add-edge! {:from-eid alice
                       :to-eid acme
                       :type :works-at
                       :label "worked at Acme"
                       :source-eid old-ep})
    (is (= 1 (memory/prune-processed-episodes! now)))
    (is (seq (db/q '[:find ?e :where [?e :episode/summary "keep-processed-0"]])))
    (is (empty? (db/q '[:find ?e :where [?e :episode/summary "old-processed"]])))
    (is (nil? (:kg.fact/source (db/entity (ffirst (db/q '[:find ?f
                                                          :where
                                                          [?f :kg.fact/content "worked on legacy system"]]))))))
    (is (nil? (:kg.edge/source (db/entity (ffirst (db/q '[:find ?e
                                                          :where
                                                          [?e :kg.edge/label "worked at Acme"]]))))))))

(deftest test-prune-processed-episodes-slows-decay-for-important-episodes
  (let [window-ms (:full-resolution-ms (memory/episode-retention-settings))
        now       (java.util.Date. (* 12 window-ms))
        day-ms    (* 24 60 60 1000)
        sid       (random-uuid)]
    (th/seed-episode! "important-1"
                      :processed? true
                      :importance 1.0
                      :session-id sid
                      :channel :terminal
                      :timestamp (java.util.Date. (- (.getTime now) (+ (* 2 window-ms)
                                                                       (* 10 day-ms)))))
    (th/seed-episode! "important-2"
                      :processed? true
                      :importance 0.95
                      :session-id sid
                      :channel :terminal
                      :timestamp (java.util.Date. (- (.getTime now) (+ (* 2 window-ms)
                                                                       (* 11 day-ms)))))
    (doseq [idx (range 8)]
      (th/seed-episode! (str "low-" idx)
                        :processed? true
                        :importance 0.1
                        :session-id sid
                        :channel :terminal
                        :timestamp (java.util.Date. (- (.getTime now)
                                                       (+ (* 2 window-ms)
                                                          (* (+ 12 idx) day-ms))))))
    (is (= 2 (memory/prune-processed-episodes! now)))
    (is (= #{"important-1"
             "important-2"
             "low-0"
             "low-1"
             "low-2"
             "low-3"
             "low-4"
             "low-5"}
           (->> (db/q '[:find ?summary :where [?e :episode/summary ?summary]])
                (map first)
                set)))))

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
      (is (= 1 (count results)))))

  (testing "non-prefix substring match"
    (let [results (memory/find-node "loju")]
      (is (= 1 (count results)))
      (is (= "Clojure" (:name (first results)))))))

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
        ;; All facts should have content, confidence, and utility
        (is (every? :content facts))
        (is (every? :confidence facts))
        (is (every? :utility facts))))))

(deftest test-node-facts-with-eids
  (let [node-eid (th/seed-node! "Bob" "person")]
    (memory/add-fact! {:node-eid node-eid :content "likes Python"})
    (let [facts (memory/node-facts-with-eids node-eid)]
      (is (= 1 (count facts)))
      (is (some? (:eid (first facts))))
      (is (= "likes Python" (:content (first facts))))
      (is (= 0.5 (:utility (first facts)))))))

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

(deftest test-search-nodes-semantic
  (th/seed-node! "Car" "concept")

  (testing "finds semantically similar nodes without lexical overlap"
    (let [results (memory/search-nodes "automobile")]
      (is (pos? (count results)))
      (is (= "Car" (:name (first results)))))))

(deftest test-search-facts-semantic
  (let [node-eid (th/seed-node! "Garage" "place")]
    (th/seed-fact! node-eid "stores a car indoors")

    (testing "finds semantically similar facts without lexical overlap"
      (let [results (memory/search-facts "automobile")]
        (is (pos? (count results)))
        (is (= "stores a car indoors" (:content (first results))))))))

(deftest test-search-episodes-semantic
  (memory/record-episode! {:summary "Fixed the car before the trip"})

  (testing "finds semantically similar episodes without lexical overlap"
    (let [results (memory/search-episodes "automobile")]
      (is (pos? (count results)))
      (is (= "Fixed the car before the trip" (:summary (first results)))))))

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
