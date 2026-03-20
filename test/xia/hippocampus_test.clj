(ns xia.hippocampus-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.artifact :as artifact]
            [xia.test-helpers :as th]
            [xia.db :as db]
            [xia.local-doc :as local-doc]
            [xia.memory :as memory]
            [xia.hippocampus :as hippo]))

(use-fixtures :each th/with-test-db)

(defn- abs-double
  [value]
  (let [value* (double value)]
    (if (neg? value*)
      (- value*)
      value*)))

(defn- date-at
  ^java.util.Date [millis]
  (java.util.Date. (long millis)))

(defn- run-concurrently!
  [n f]
  (let [start   (promise)
        futures (mapv (fn [idx]
                        (future
                          @start
                          (f idx)))
                      (range n))]
    (deliver start true)
    (doseq [fut futures]
      (is (not= ::timeout (deref fut 5000 ::timeout))
          "Concurrent helper work should finish without hanging"))))

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

(deftest test-dedup-fact-serializes-concurrent-identical-writes
  (let [node-eid   (th/seed-node! "ConcurrentHong" "person")
        episode-id (th/seed-episode! "concurrent direct dedup")
        original   xia.memory/node-facts-with-eids]
    (with-redefs [xia.memory/node-facts-with-eids
                  (fn [eid]
                    (let [facts (original eid)]
                      (Thread/sleep 40)
                      facts))]
      (run-concurrently! 8
                         (fn [_]
                           (#'xia.hippocampus/dedup-fact!
                             node-eid
                             "likes Clojure"
                             episode-id))))
    (let [facts (memory/node-facts node-eid)]
      (is (= 1 (count facts)))
      (is (= ["likes Clojure"] (mapv :content facts))))))

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

(deftest test-merge-extraction-serializes-concurrent-fact-dedup
  (let [node-eid    (th/seed-node! "ConcurrentMergeNode" "concept")
        extraction  {"entities"  [{"name" "ConcurrentMergeNode"
                                   "type" "concept"
                                   "facts" ["prefers Clojure"]}]
                     "relations" []}
        episode-eids (mapv #(th/seed-episode! (str "concurrent merge " %))
                           (range 8))
        original    xia.memory/node-facts-with-eids]
    (with-redefs [xia.memory/node-facts-with-eids
                  (fn [eid]
                    (let [facts (original eid)]
                      (Thread/sleep 40)
                      facts))]
      (run-concurrently! (count episode-eids)
                         (fn [idx]
                           (#'xia.hippocampus/merge-extraction!
                             extraction
                             (nth episode-eids idx)))))
    (let [facts (memory/node-facts node-eid)]
      (is (= 1 (count facts)))
      (is (= ["prefers Clojure"] (mapv :content facts))))))

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
                  xia.memory/processed-episode-prune-plan
                  (fn [& _]
                    (reset! called? true)
                    {:to-remove []
                     :tx-data []})]
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

(deftest test-consolidate-episode-combines-pruning-with-merge-transaction
  (let [ep-eid      (th/seed-episode! "Atomic prune")
        stale-eid   (th/seed-episode! "Older processed episode")
        seen-tx     (atom nil)]
    (with-redefs [xia.llm/chat-simple
                  (fn [_messages & _opts]
                    "{\"entities\": [], \"relations\": []}")
                  xia.memory/processed-episode-prune-plan
                  (fn [& _]
                    {:to-remove [{:eid stale-eid}]
                     :tx-data   [[:db/retractEntity stale-eid]]})
                  xia.db/transact!
                  (fn [tx-data]
                    (reset! seen-tx tx-data)
                    (throw (ex-info "synthetic failure" {:tx-count (count tx-data)})))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"synthetic failure"
            (hippo/consolidate-episode!
              {:eid     ep-eid
               :summary "Atomic prune"
               :type    :conversation}))))
    (is (some? @seen-tx))
    (is (some #(= [:db/add ep-eid :episode/processed? true] %) @seen-tx))
    (is (some #(= [:db/retractEntity stale-eid] %) @seen-tx))))

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
        ep-1              (th/seed-episode! "Episode 1" :timestamp (date-at (- now 2000)))
        ep-2              (th/seed-episode! "Episode 2" :timestamp (date-at (- now 1000)))
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
                  xia.memory/processed-episode-prune-plan
                  (fn [& _]
                    {:to-remove []
                     :tx-data []})]
      (hippo/consolidate-pending!))
    (is (= 1 @importance-calls))
    (is (= 2 @extraction-calls))
    (is (< (abs-double (- 0.9
                          (double (:episode/importance (db/entity ep-1)))))
           1.0e-6))
    (is (< (abs-double (- 0.2
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
                  xia.memory/processed-episode-prune-plan
                  (fn [& _]
                    {:to-remove []
                     :tx-data []})]
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

(deftest test-record-conversation-persists-explicit-local-document-references
  (let [session-id (db/create-session! :terminal)
        doc        (local-doc/save-upload! {:session-id session-id
                                            :name "paper.md"
                                            :media-type "text/markdown"
                                            :text "# Research"})
        report     (artifact/create-artifact! {:session-id session-id
                                               :name "findings.json"
                                               :title "Findings"
                                               :kind :json
                                               :data {"topic" "research"}})]
    (db/add-message! session-id :user "summarize this"
                     :local-doc-ids [(:id doc)]
                     :artifact-ids [(:id report)])
    (db/add-message! session-id :assistant "summary"
                     :local-doc-ids [(:id doc)]
                     :artifact-ids [(:id report)])
    (with-redefs [xia.hippocampus/summarize-conversation (constantly "summary")
                  xia.hippocampus/consolidate-pending!   (fn [] nil)]
      (hippo/record-conversation! session-id :terminal :topics "research")
      (let [episode (first (memory/recent-episodes 5))]
        (is (= "summary" (:summary episode)))
        (is (.contains ^String (:context episode) "Topic: research"))
        (is (.contains ^String (:context episode) "Local documents referenced: paper.md"))
        (is (.contains ^String (:context episode) "Artifacts referenced: Findings"))))))

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
        grace-period-ms (long (:grace-period-ms settings))
        half-life-ms  (long (:half-life-ms settings))
        elapsed-ms    (+ grace-period-ms
                         (* 2 half-life-ms))
        now           (date-at now-ms)
        old-updated   (date-at (- now-ms elapsed-ms))
        expected-conf (clojure.core/max (double (:min-confidence settings))
                                        (Math/pow 0.5 (/ (- elapsed-ms grace-period-ms)
                                                         (double half-life-ms))))]
    (db/transact! [[:db/add fact-eid :kg.fact/updated-at old-updated]])
    (hippo/maintain-knowledge! now)
    (let [updated-conf (:kg.fact/confidence (db/entity fact-eid))
          decayed-at   (:kg.fact/decayed-at (db/entity fact-eid))]
      (is (< updated-conf 1.0) "Confidence should have decayed")
      (is (< (abs-double (- (double updated-conf) expected-conf)) 1.0e-3)
          "Confidence should follow the documented exponential half-life")
      (is (= now decayed-at) "Maintenance should record when decay was applied"))))

(deftest test-maintain-knowledge-is-idempotent-for-same-time
  (let [settings    (hippo/knowledge-decay-settings)
        node-eid    (th/seed-node! "StableEntity" "concept")
        fact-eid    (th/seed-fact! node-eid "stable fact" :confidence 1.0)
        now-ms      (System/currentTimeMillis)
        now         (date-at now-ms)
        grace-period-ms (long (:grace-period-ms settings))
        old-updated (date-at (- now-ms
                                 (+ grace-period-ms
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
        now           (date-at now-ms)
        grace-period-ms (long (:grace-period-ms settings))
        half-life-ms  (long (:half-life-ms settings))
        old-updated   (date-at (- now-ms
                                  (+ grace-period-ms
                                     (* 2 half-life-ms))))]
    (db/transact! [[:db/add low-fact-eid :kg.fact/updated-at old-updated]
                   [:db/add high-fact-eid :kg.fact/updated-at old-updated]])
    (hippo/maintain-knowledge! now)
    (is (> (:kg.fact/confidence (db/entity high-fact-eid))
           (:kg.fact/confidence (db/entity low-fact-eid)))
        "High-utility facts should decay more slowly than low-utility facts")))

(deftest test-due-for-decay-facts-selects-only_due_rows
  (let [settings        (hippo/knowledge-decay-settings)
        node-eid        (th/seed-node! "DecayQueryEntity" "concept")
        now-ms          (System/currentTimeMillis)
        as-of           (date-at now-ms)
        maintenance-step-ms (long (:maintenance-step-ms settings))
        stale-updated   (date-at (- now-ms
                                     maintenance-step-ms
                                     (* 2 24 60 60 1000)))
        old-maintained-updated (date-at (- now-ms
                                           maintenance-step-ms
                                           (* 6 24 60 60 1000)))
        fresh-updated   (date-at (- now-ms
                                     (quot maintenance-step-ms 2)))
        recent-decayed  (date-at (- now-ms
                                     (quot maintenance-step-ms 2)))
        old-decayed     (date-at (- now-ms
                                     maintenance-step-ms
                                     (* 3 24 60 60 1000)))
        stale-eid       (th/seed-fact! node-eid "stale fact" :confidence 1.0)
        fresh-eid       (th/seed-fact! node-eid "fresh fact" :confidence 1.0)
        recent-eid      (th/seed-fact! node-eid "recently maintained fact" :confidence 1.0)
        old-eid         (th/seed-fact! node-eid "old maintained fact" :confidence 1.0)]
    (db/transact! [[:db/add stale-eid :kg.fact/updated-at stale-updated]
                   [:db/add fresh-eid :kg.fact/updated-at fresh-updated]
                   [:db/add recent-eid :kg.fact/updated-at stale-updated]
                   [:db/add recent-eid :kg.fact/decayed-at recent-decayed]
                   [:db/add old-eid :kg.fact/updated-at old-maintained-updated]
                   [:db/add old-eid :kg.fact/decayed-at old-decayed]])
    (let [rows (#'xia.hippocampus/due-for-decay-facts as-of settings)
          by-eid (into {} (map (fn [[eid confidence utility updated last-decayed]]
                                 [eid {:confidence confidence
                                       :utility utility
                                       :updated updated
                                       :last-decayed last-decayed}]))
                       rows)]
      (is (contains? by-eid stale-eid))
      (is (contains? by-eid old-eid))
      (is (not (contains? by-eid fresh-eid)))
      (is (not (contains? by-eid recent-eid)))
      (is (= stale-updated (:last-decayed (get by-eid stale-eid))))
      (is (= old-decayed (:last-decayed (get by-eid old-eid)))))))

(deftest test-maintain-knowledge-archives-bottomed-out-facts
  (let [settings   (hippo/knowledge-decay-settings)
        node-eid   (th/seed-node! "ArchiveEntity" "concept")
        fact-eid   (th/seed-fact! node-eid "stale archived fact"
                                  :confidence (:min-confidence settings)
                                  :utility 0.5)
        now-ms     (System/currentTimeMillis)
        now        (date-at now-ms)
        archive-after-bottom-ms (long (:archive-after-bottom-ms settings))
        bottomed   (date-at (- now-ms
                                 archive-after-bottom-ms
                                 (* 2 24 60 60 1000)))
        updated-at (date-at (- (.getTime bottomed)
                                 (* 30 24 60 60 1000)))]
    (db/transact! [[:db/add fact-eid :kg.fact/updated-at updated-at]
                   [:db/add fact-eid :kg.fact/decayed-at bottomed]
                   [:db/add fact-eid :kg.fact/bottomed-at bottomed]])
    (hippo/maintain-knowledge! now)
    (is (empty? (into {} (db/entity fact-eid)))
        "Facts that have stayed at the confidence floor past the archive window should be removed")))

(deftest test-due-for-archive-eids-selects_old_bottomed_facts
  (let [settings       (hippo/knowledge-decay-settings)
        node-eid       (th/seed-node! "ArchiveQueryEntity" "concept")
        now-ms         (System/currentTimeMillis)
        as-of          (date-at now-ms)
        archive-after-bottom-ms (long (:archive-after-bottom-ms settings))
        old-bottomed   (date-at (- now-ms
                                   archive-after-bottom-ms
                                   (* 2 24 60 60 1000)))
        recent-bottomed (date-at (- now-ms
                                    (quot archive-after-bottom-ms 2)))
        stale-eid      (th/seed-fact! node-eid "stale archive fact"
                                      :confidence (:min-confidence settings)
                                      :utility 0.5)
        recent-eid     (th/seed-fact! node-eid "recent bottomed fact"
                                      :confidence (:min-confidence settings)
                                      :utility 0.5)
        active-eid     (th/seed-fact! node-eid "still above floor"
                                      :confidence 0.9
                                      :utility 0.5)]
    (db/transact! [[:db/add stale-eid :kg.fact/bottomed-at old-bottomed]
                   [:db/add recent-eid :kg.fact/bottomed-at recent-bottomed]
                   [:db/add active-eid :kg.fact/bottomed-at old-bottomed]])
    (is (= #{stale-eid}
           (set (#'xia.hippocampus/due-for-archive-eids as-of settings))))))

(deftest test-dedup-refresh-clears-bottomed-at
  (let [settings   (hippo/knowledge-decay-settings)
        node-eid   (th/seed-node! "RefreshEntity" "concept")
        fact-eid   (th/seed-fact! node-eid "prefers tea"
                                  :confidence (:min-confidence settings)
                                  :utility 0.5)
        archive-after-bottom-ms (long (:archive-after-bottom-ms settings))
        bottomed   (date-at (- (System/currentTimeMillis)
                                 archive-after-bottom-ms
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
