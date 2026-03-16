(ns xia.hippocampus-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.string :as str]
            [taoensso.timbre :as log]
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

    (testing "high-overlap corrections do not dedup"
      (is (false? (fs? "She prefers Python for data science work"
                       "She prefers Ruby for data science work"))))

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

(deftest test-dedup-fact-keeps-high-overlap-corrections
  (let [node-eid (th/seed-node! "Avery" "person")
        ep-eid   (th/seed-episode! "correction episode")]
    (#'xia.hippocampus/dedup-fact! node-eid
                                   "She prefers Python for data science work"
                                   ep-eid)
    (#'xia.hippocampus/dedup-fact! node-eid
                                   "She prefers Ruby for data science work"
                                   ep-eid)
    (let [facts (memory/node-facts node-eid)
          contents (set (map :content facts))]
      (is (= 2 (count facts)))
      (is (= #{"She prefers Python for data science work"
               "She prefers Ruby for data science work"}
             contents)))))

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

    (testing "deep-merges nested properties on later extractions"
      (#'xia.hippocampus/merge-extraction!
        {"entities" [{"name"       "Dana"
                      "type"       "person"
                      "facts"      []
                      "properties" {"work" {"company" "Acme"
                                            "title"   "Engineer"}}}]
         "relations" []}
        ep-eid)
      (#'xia.hippocampus/merge-extraction!
        {"entities" [{"name"       "Dana"
                      "type"       "person"
                      "facts"      []
                      "properties" {"work" {"salary" 100000}}}]
         "relations" []}
        ep-eid)
      (let [node-eid (:eid (first (memory/find-node "Dana")))
            props    (memory/node-properties node-eid)]
        (is (= {:company "Acme"
                :title   "Engineer"
                :salary  100000}
               (:work props)))))

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

(deftest test-merge-extraction-replaces-same-episode-partials
  (let [ep-eid    (th/seed-episode! "retry episode")
        alice-eid (th/seed-node! "Alice" "person")
        acme-eid  (th/seed-node! "Acme" "thing")
        now       (java.util.Date.)]
    (db/transact!
      [{:kg.fact/id         (random-uuid)
        :kg.fact/node       alice-eid
        :kg.fact/content    "old retry fact"
        :kg.fact/confidence 1.0
        :kg.fact/source     ep-eid
        :kg.fact/created-at now
        :kg.fact/updated-at now
        :kg.fact/decayed-at now}
       {:kg.edge/id         (random-uuid)
        :kg.edge/from       alice-eid
        :kg.edge/to         acme-eid
        :kg.edge/type       :works-at
        :kg.edge/label      "old retry edge"
        :kg.edge/source     ep-eid
        :kg.edge/created-at now}])
    (#'xia.hippocampus/merge-extraction!
      {"entities"  [{"name" "Alice" "type" "person" "facts" ["new retry fact"]}
                    {"name" "Acme" "type" "thing" "facts" []}]
       "relations" [{"from" "Alice" "to" "Acme"
                      "type" "works-at" "label" "new retry edge"}]}
      ep-eid)
    (let [facts (memory/node-facts alice-eid)
          edges (memory/node-edges alice-eid)]
      (is (= ["new retry fact"] (mapv :content facts)))
      (is (= 1 (count (:outgoing edges))))
      (is (= "Acme" (:target (first (:outgoing edges)))))
      (is (= "new retry edge" (:label (first (:outgoing edges))))))))

;; ---------------------------------------------------------------------------
;; consolidate-episode! (requires LLM mock)
;; ---------------------------------------------------------------------------

(deftest test-consolidate-episode-with-mock-llm
  (let [ep-eid (th/seed-episode! "Discussed Clojure web frameworks")]
    (with-redefs [xia.llm/chat-simple
                  (fn [_messages & _opts]
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

(deftest test-consolidate-episode-triggers-episode-pruning
  (let [ep-eid  (th/seed-episode! "Prunable episode")
        called? (atom false)]
    (with-redefs [xia.llm/chat-simple
                  (fn [_messages & _opts]
                    "{\"entities\": [], \"relations\": []}")
                  xia.memory/prune-processed-episodes!
                  (fn []
                    (reset! called? true)
                    0)]
      (hippo/consolidate-episode!
        {:eid     ep-eid
         :summary "Prunable episode"
         :type    :conversation}))
    (is @called?)))

(deftest test-consolidate-episode-rolls-back-on-write-failure
  (let [ep-eid  (th/seed-episode! "Atomic rollback")
        seen-tx (atom nil)]
    (with-redefs [xia.llm/chat-simple
                  (fn [_messages & _opts]
                    "{\"entities\": [{\"name\": \"RollbackNode\", \"type\": \"concept\", \"facts\": [\"should not persist\"]}], \"relations\": []}")
                  xia.db/transact!
                  (fn [tx-data]
                    (reset! seen-tx tx-data)
                    (throw (ex-info "synthetic failure" {:tx-count (count tx-data)})))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"synthetic failure"
            (hippo/consolidate-episode!
              {:eid     ep-eid
               :summary "Atomic rollback"
               :type    :conversation}))))
    (is (some? @seen-tx))
    (is (some #(= [:db/add ep-eid :episode/processed? true] %) @seen-tx))
    (is (false? (:episode/processed? (db/entity ep-eid))))
    (is (empty? (memory/find-node "RollbackNode")))))

(deftest test-consolidate-episode-quarantines-invalid-extractions
  (let [ep-eid (th/seed-episode! "Bad extraction")]
    (with-redefs [xia.hippocampus/extract-knowledge
                  (fn [_episode] nil)]
      (is (= {:status      :invalid-extraction
              :episode-eid ep-eid
              :summary     "Bad extraction"
              :error       "knowledge extraction returned invalid JSON"}
             (hippo/consolidate-episode!
               {:eid     ep-eid
                :summary "Bad extraction"
                :type    :conversation}))))
    (let [episode (db/entity ep-eid)]
      (is (true? (:episode/processed? episode)))
      (is (= "knowledge extraction returned invalid JSON"
             (:episode/consolidation-error episode)))
      (is (some? (:episode/consolidation-failed-at episode))))
    (is (empty? (memory/unprocessed-episodes)))
    (is (empty? (memory/find-node "Bad extraction")))))

;; ---------------------------------------------------------------------------
;; consolidate-pending!
;; ---------------------------------------------------------------------------

(deftest test-consolidate-pending
  (th/seed-episode! "Episode 1")
  (th/seed-episode! "Episode 2")
  (is (= 2 (count (memory/unprocessed-episodes))))
  (with-redefs [xia.llm/chat-simple
                (fn [_messages & _opts] "{\"entities\": [], \"relations\": []}")]
    ;; Consolidate each directly to surface any errors
    (doseq [ep (memory/unprocessed-episodes)]
      (hippo/consolidate-episode! ep))
    ;; All episodes should be marked processed
    (is (empty? (memory/unprocessed-episodes)))))

(deftest test-consolidate-pending-rates-importance-in-one-batched-call
  (let [now               (System/currentTimeMillis)
        ep-1              (th/seed-episode! "Episode 1" :timestamp (java.util.Date. (- now 2000)))
        ep-2              (th/seed-episode! "Episode 2" :timestamp (java.util.Date. (- now 1000)))
        importance-calls   (atom 0)
        extraction-calls   (atom 0)]
    (with-redefs [xia.llm/chat-simple
                  (fn [_messages & opts]
                    (case (first (drop 1 opts))
                      :memory-importance (do
                                           (swap! importance-calls inc)
                                           "{\"episodes\":[{\"index\":0,\"importance\":0.9},{\"index\":1,\"importance\":0.2}]}")
                      :memory-extraction (do
                                           (swap! extraction-calls inc)
                                           "{\"entities\": [], \"relations\": []}")
                      "{\"entities\": [], \"relations\": []}"))
                  xia.memory/prune-processed-episodes!
                  (fn []
                    0)]
      (hippo/consolidate-pending!))
    (is (= 1 @importance-calls))
    (is (= 2 @extraction-calls))
    (is (< (Math/abs (- 0.9
                        (double (:episode/importance (db/entity ep-1)))))
           1.0e-6))
    (is (< (Math/abs (- 0.2
                        (double (:episode/importance (db/entity ep-2)))))
           1.0e-6))))

(deftest test-consolidate-pending-quarantines-invalid-episodes-and-processes-rest
  (let [bad-eid  (th/seed-episode! "Bad extraction")
        good-eid (th/seed-episode! "Good extraction")]
    (with-redefs [xia.hippocampus/rate-episode-importance
                  (fn [episodes]
                    (into {}
                          (map (fn [{:keys [eid]}] [eid 0.5]))
                          episodes))
                  xia.hippocampus/extract-knowledge
                  (fn [{:keys [summary]}]
                    (if (= "Bad extraction" summary)
                      nil
                      {"entities" [] "relations" []}))
                  xia.memory/prune-processed-episodes!
                  (fn [] 0)]
      (hippo/consolidate-pending!))
    (let [bad  (db/entity bad-eid)
          good (db/entity good-eid)]
      (is (true? (:episode/processed? bad)))
      (is (= "knowledge extraction returned invalid JSON"
             (:episode/consolidation-error bad)))
      (is (true? (:episode/processed? good))))
    (is (empty? (memory/unprocessed-episodes)))))

(deftest test-record-conversation-logs-background-consolidation-failures
  (let [session-id (db/create-session! :terminal)
        logged     (promise)]
    (db/add-message! session-id :user "hello")
    (db/add-message! session-id :assistant "hi there")
    (with-redefs [xia.hippocampus/summarize-conversation (constantly "summary")
                  xia.hippocampus/consolidate-pending!   (fn []
                                                           (throw (ex-info "boom" {:type :test})))
                  log/-log!
                  (fn [_config level _ns-str _file _line _column _msg-type _auto-err vargs_ _base-data _callsite-id _spying?]
                    (let [vargs     @vargs_
                          throwable (when (instance? Throwable (first vargs))
                                      (first vargs))
                          msg-args   (if throwable (rest vargs) vargs)]
                      (when (= :error level)
                        (deliver logged {:level level
                                         :throwable throwable
                                         :message (str/join " " msg-args)}))))]
      (hippo/record-conversation! session-id :terminal)
      (let [entry (deref logged 1000 ::timeout)]
        (is (not= ::timeout entry))
        (is (= :error (:level entry)))
        (is (instance? Exception (:throwable entry)))
        (is (re-find #"Background consolidation failed for session"
                     (:message entry)))))))

;; ---------------------------------------------------------------------------
;; maintain-knowledge! — confidence decay
;; ---------------------------------------------------------------------------

(deftest test-knowledge-decay-settings-use-config-overrides
  (db/set-config! :memory/knowledge-decay-grace-period-ms (* 14 24 60 60 1000))
  (db/set-config! :memory/knowledge-decay-half-life-ms (* 120 24 60 60 1000))
  (db/set-config! :memory/knowledge-decay-min-confidence 0.25)
  (db/set-config! :memory/knowledge-decay-maintenance-step-ms (* 3 24 60 60 1000))
  (db/set-config! :memory/knowledge-decay-archive-after-bottom-ms (* 45 24 60 60 1000))
  (let [settings (hippo/knowledge-decay-settings)]
    (is (= (* 14 24 60 60 1000) (:grace-period-ms settings)))
    (is (= (* 120 24 60 60 1000) (:half-life-ms settings)))
    (is (= 0.25 (:min-confidence settings)))
    (is (= (* 3 24 60 60 1000) (:maintenance-step-ms settings)))
    (is (= (* 45 24 60 60 1000) (:archive-after-bottom-ms settings)))))

(deftest test-maintain-knowledge
  (let [settings      (hippo/knowledge-decay-settings)
        node-eid      (th/seed-node! "TestEntity" "concept")
        fact-eid      (th/seed-fact! node-eid "some fact" :confidence 1.0)
        now-ms        (System/currentTimeMillis)
        elapsed-ms    (+ (:grace-period-ms settings)
                         (* 2 (:half-life-ms settings)))
        now           (java.util.Date. now-ms)
        old-updated   (java.util.Date. (- now-ms elapsed-ms))
        expected-conf (max (:min-confidence settings)
                           (Math/pow 0.5 (/ (- elapsed-ms (:grace-period-ms settings))
                                            (double (:half-life-ms settings)))))]
    (db/transact! [[:db/add fact-eid :kg.fact/updated-at old-updated]])
    (hippo/maintain-knowledge! now)
    (let [updated-conf (:kg.fact/confidence (db/entity fact-eid))
          decayed-at   (:kg.fact/decayed-at (db/entity fact-eid))]
      (is (< updated-conf 1.0) "Confidence should have decayed")
      (is (< (Math/abs (- updated-conf expected-conf)) 1.0e-3)
          "Confidence should follow the documented exponential half-life")
      (is (= now decayed-at) "Maintenance should record when decay was applied"))))

(deftest test-maintain-knowledge-is-idempotent-for-same-time
  (let [settings    (hippo/knowledge-decay-settings)
        node-eid    (th/seed-node! "StableEntity" "concept")
        fact-eid    (th/seed-fact! node-eid "stable fact" :confidence 1.0)
        now-ms      (System/currentTimeMillis)
        now         (java.util.Date. now-ms)
        old-updated (java.util.Date. (- now-ms
                                        (+ (:grace-period-ms settings)
                                           (* 30 24 60 60 1000))))]
    (db/transact! [[:db/add fact-eid :kg.fact/updated-at old-updated]])
    (hippo/maintain-knowledge! now)
    (let [once-conf (:kg.fact/confidence (db/entity fact-eid))]
      (hippo/maintain-knowledge! now)
      (let [twice-conf (:kg.fact/confidence (db/entity fact-eid))]
        (is (= once-conf twice-conf)
            "The same elapsed time should not be charged twice")))))

(deftest test-maintain-knowledge-slows-decay-for-high-utility-facts
  (let [settings      (hippo/knowledge-decay-settings)
        node-eid      (th/seed-node! "UsefulEntity" "concept")
        low-fact-eid  (th/seed-fact! node-eid "low utility fact" :confidence 1.0 :utility 0.0)
        high-fact-eid (th/seed-fact! node-eid "high utility fact" :confidence 1.0 :utility 1.0)
        now-ms        (System/currentTimeMillis)
        now           (java.util.Date. now-ms)
        old-updated   (java.util.Date. (- now-ms
                                          (+ (:grace-period-ms settings)
                                             (* 2 (:half-life-ms settings)))))]
    (db/transact! [[:db/add low-fact-eid :kg.fact/updated-at old-updated]
                   [:db/add high-fact-eid :kg.fact/updated-at old-updated]])
    (hippo/maintain-knowledge! now)
    (is (> (:kg.fact/confidence (db/entity high-fact-eid))
           (:kg.fact/confidence (db/entity low-fact-eid)))
        "High-utility facts should decay more slowly than low-utility facts")))

(deftest test-maintain-knowledge-archives-bottomed-out-facts
  (let [settings   (hippo/knowledge-decay-settings)
        node-eid   (th/seed-node! "ArchiveEntity" "concept")
        fact-eid   (th/seed-fact! node-eid "stale archived fact"
                                  :confidence (:min-confidence settings)
                                  :utility 0.5)
        now-ms     (System/currentTimeMillis)
        now        (java.util.Date. now-ms)
        bottomed   (java.util.Date. (- now-ms
                                       (:archive-after-bottom-ms settings)
                                       (* 2 24 60 60 1000)))
        updated-at (java.util.Date. (- (.getTime bottomed)
                                       (* 30 24 60 60 1000)))]
    (db/transact! [[:db/add fact-eid :kg.fact/updated-at updated-at]
                   [:db/add fact-eid :kg.fact/decayed-at bottomed]
                   [:db/add fact-eid :kg.fact/bottomed-at bottomed]])
    (hippo/maintain-knowledge! now)
    (is (empty? (into {} (db/entity fact-eid)))
        "Facts that have stayed at the confidence floor past the archive window should be removed")))

(deftest test-dedup-refresh-clears-bottomed-at
  (let [settings   (hippo/knowledge-decay-settings)
        node-eid   (th/seed-node! "RefreshEntity" "concept")
        fact-eid   (th/seed-fact! node-eid "prefers tea"
                                  :confidence (:min-confidence settings)
                                  :utility 0.5)
        bottomed   (java.util.Date. (- (System/currentTimeMillis)
                                       (:archive-after-bottom-ms settings)
                                       (* 2 24 60 60 1000)))]
    (db/transact! [[:db/add fact-eid :kg.fact/bottomed-at bottomed]])
    (#'xia.hippocampus/dedup-fact! node-eid "prefers tea" nil)
    (is (nil? (:kg.fact/bottomed-at (db/entity fact-eid)))
        "Refreshing a similar fact should clear the archival timer")))

;; ---------------------------------------------------------------------------
;; consolidate-if-pending!
;; ---------------------------------------------------------------------------

(deftest test-consolidate-if-pending-below-threshold
  (testing "does nothing below threshold"
    (th/seed-episode! "Only one")
    (let [called? (atom false)]
      (with-redefs [xia.llm/chat-simple (fn [_messages & _opts]
                                          (reset! called? true)
                                          "{\"entities\": [], \"relations\": []}")]
        (hippo/consolidate-if-pending! :threshold 3)
        (is (false? @called?) "Should not trigger consolidation below threshold")))))

(deftest test-consolidate-if-pending-at-threshold
  (testing "triggers consolidation at threshold"
    (th/seed-episode! "One")
    (th/seed-episode! "Two")
    (th/seed-episode! "Three")
    (with-redefs [xia.llm/chat-simple (fn [_messages & _opts]
                                        "{\"entities\": [], \"relations\": []}")]
      (hippo/consolidate-if-pending! :threshold 3)
      (is (empty? (memory/unprocessed-episodes))))))
