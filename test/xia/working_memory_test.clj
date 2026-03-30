(ns xia.working-memory-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [xia.autonomous :as autonomous]
            [xia.test-helpers :as th]
            [xia.db :as db]
            [xia.local-doc :as local-doc]
            [xia.memory :as memory]
            [xia.working-memory :as wm]))

(use-fixtures :each th/with-test-db)

(defn- abs-double
  [value]
  (let [value* (double value)]
    (if (neg? value*)
      (- value*)
      value*)))

;; ---------------------------------------------------------------------------
;; Lifecycle
;; ---------------------------------------------------------------------------

(deftest test-create-and-clear-wm
  (testing "create-wm! initializes state"
    (let [sid (random-uuid)
          state (wm/create-wm! sid)]
      (is (= sid (:session-id state)))
      (is (= 0 (:turn-count state)))
      (is (= {} (:slots state)))
      (is (= [] (:episode-refs state)))
      (is (= [] (:local-doc-refs state)))
      (is (some? (:config state)))))

  (testing "get-wm returns current state"
    (is (some? (wm/get-wm))))

  (testing "clear-wm! resets to nil"
    (wm/clear-wm!)
    (is (nil? (wm/get-wm)))))

(deftest test-autonomy-state-round-trips-through-working-memory
  (let [sid   (db/create-session! :terminal)
        state (autonomous/normalize-state
               {:goal "Handle billing emails"
                :stack [{:title "Handle billing emails"
                         :progress-status :in-progress
                         :agenda [{:item "Check inbox" :status :completed}]}]})]
    (wm/ensure-wm! sid)
    (wm/set-autonomy-state! sid state)
    (is (= state (wm/autonomy-state sid)))
    (is (= state (:autonomy (wm/wm->context sid))))
    (wm/snapshot! sid)
    (wm/clear-wm! sid)
    (is (nil? (wm/get-wm sid)))
    (wm/ensure-wm! sid)
    (is (= state (wm/autonomy-state sid)))
    (is (= state (:autonomy (wm/wm->context sid))))
    (wm/clear-autonomy-state! sid)
    (is (nil? (wm/autonomy-state sid)))
    (wm/clear-wm! sid)))

;; ---------------------------------------------------------------------------
;; Keyword extraction
;; ---------------------------------------------------------------------------

(deftest test-extract-search-terms
  (let [sid (random-uuid)]
    (wm/create-wm! sid)

    (testing "filters stopwords"
      (let [terms (wm/extract-search-terms "I want to use Clojure for my project")]
        (is (not (some #{"i" "to" "for" "my"} terms)))
        (is (some #{"clojure"} terms))
        (is (some #{"project"} terms))))

    (testing "extracts proper nouns"
      (let [terms (wm/extract-search-terms "I talked to Alice about Seattle")]
        (is (some #{"alice"} terms))
        (is (some #{"seattle"} terms))))

    (testing "includes WM entity names"
      (let [node-eid (th/seed-node! "Hong" "person")]
        ;; Manually add to WM slots
        (swap! @#'xia.working-memory/wm-atom
               assoc-in [sid :slots node-eid]
               {:node-eid node-eid :name "Hong" :type :person :relevance 0.8})
        (let [terms (wm/extract-search-terms "hello there")]
          (is (some #{"hong"} terms)))))

    (testing "limits to 20 terms"
      (let [long-msg (apply str (interpose " " (map #(str "word" %) (range 50))))
            terms (wm/extract-search-terms long-msg)]
        (is (<= (count terms) 20))))

    ;; Clear WM slots before testing empty input (earlier test added "Hong" to WM)
    (wm/clear-wm! sid)
    (wm/create-wm! sid)

    (testing "handles empty input (no WM entities)"
      (let [terms (wm/extract-search-terms "")]
        (is (empty? terms))))

    (wm/clear-wm! sid)))

;; ---------------------------------------------------------------------------
;; Search knowledge
;; ---------------------------------------------------------------------------

(deftest test-search-knowledge
  (testing "returns nodes, facts, episodes from FTS"
    ;; Seed data
    (th/seed-node! "Clojure" "concept")
    (let [node-eid (th/seed-node! "Ring" "concept")]
      (th/seed-fact! node-eid "Clojure web framework"))
    (th/seed-episode! "Discussed Clojure web development")

    ;; Give FTS time to index
    (Thread/sleep 100)

    (let [results (wm/search-knowledge ["clojure"])]
      (is (map? results))
      (is (contains? results :nodes))
      (is (contains? results :facts))
      (is (contains? results :episodes))
      (is (contains? results :local-docs))))

  (testing "returns semantic matches for synonym queries"
    (let [sid      (db/create-session! :http)
          node-eid (th/seed-node! "Car" "concept")]
      (th/seed-fact! node-eid "parks the car in the garage")
      (memory/record-episode! {:summary "Cleaned the car yesterday"})
      (local-doc/save-upload! {:session-id sid
                               :name "garage.txt"
                               :media-type "text/plain"
                               :text "The car stays inside the garage."})

      (let [results (wm/search-knowledge sid ["automobile"] "automobile")]
        (is (some #(= "Car" (:name %)) (:nodes results)))
        (is (some #(= "parks the car in the garage" (:content %))
                  (:facts results)))
        (is (some #(= "Cleaned the car yesterday" (:summary %))
                  (:episodes results)))
        (is (some #(= "garage.txt" (:name %))
                  (:local-docs results)))))))

(deftest search-knowledge-degrades-when-search-step-fails
  (with-redefs [xia.memory/search-nodes      (fn [& _]
                                               (throw (ex-info "embedding unavailable"
                                                               {:type :embedding-service-failure})))
                xia.memory/search-facts      (constantly [])
                xia.memory/search-episodes   (constantly [])
                xia.memory/search-local-docs (constantly [])]
    (let [results (wm/search-knowledge (random-uuid) ["atlas"] "atlas")]
      (is (= {:nodes []
              :facts []
              :episodes []
              :local-docs []}
             results)))))

(deftest expand-graph-bulk-loads-neighbors
  (let [seed-a   (th/seed-node! "SeedA" "concept")
        seed-b   (th/seed-node! "SeedB" "concept")
        friend   (th/seed-node! "Friend" "concept")
        parent   (th/seed-node! "Parent" "concept")
        q-calls  (atom 0)
        node-loads (atom 0)
        orig-q   xia.db/q]
    (memory/add-edge! {:from-eid seed-a :to-eid friend :type :related-to})
    (memory/add-edge! {:from-eid parent :to-eid seed-a :type :related-to})
    (memory/add-edge! {:from-eid seed-a :to-eid seed-b :type :related-to})
    (with-redefs [xia.db/q (fn [query & inputs]
                             (swap! q-calls inc)
                             (apply orig-q query inputs))
                  xia.memory/get-node (fn [& _]
                                        (swap! node-loads inc)
                                        nil)]
      (let [expanded (wm/expand-graph [seed-a seed-b])]
        (is (= #{friend parent}
               (set (keys expanded))))
        (is (= "Friend" (:name (get expanded friend))))
        (is (= "Parent" (:name (get expanded parent))))
        (is (every? :expanded? (vals expanded)))
        (is (= 2 @q-calls))
        (is (= 0 @node-loads))))))

;; ---------------------------------------------------------------------------
;; Slot merge with properties
;; ---------------------------------------------------------------------------

(deftest test-merge-node-into-slot-includes-properties
  (let [sid (random-uuid)]
    (wm/create-wm! sid)
    (let [node-eid (th/seed-node! "Hong" "person")]
    ;; Set properties on the node
      (memory/set-node-property! node-eid [:location] "Seattle")
      (memory/set-node-property! node-eid [:role] "engineer")
      ;; Add a fact
      (th/seed-fact! node-eid "prefers vim")

      ;; Merge into WM via private fn
      (let [merge-fn #'xia.working-memory/merge-node-into-slot
            slots    (merge-fn {} node-eid "Hong" :person 0.8 1)]
        (testing "slot has properties"
          (let [slot (get slots node-eid)]
            (is (some? (:properties slot)))
            (is (= "Seattle" (:location (:properties slot))))
            (is (= "engineer" (:role (:properties slot))))))

        (testing "slot has facts"
          (let [slot (get slots node-eid)]
            (is (= 1 (count (:facts slot))))
            (is (= "prefers vim" (:content (first (:facts slot)))))))

        (testing "slot has edges"
          (let [slot (get slots node-eid)]
            (is (map? (:edges slot)))))))

    (wm/clear-wm! sid)))

;; ---------------------------------------------------------------------------
;; Decay & eviction
;; ---------------------------------------------------------------------------

(deftest test-decay-and-eviction
  (let [sid (random-uuid)]
    (wm/create-wm! sid)
    (let [n1 (th/seed-node! "Entity1" "concept")
          n2 (th/seed-node! "Entity2" "concept")]
    ;; Manually populate slots
      (swap! @#'xia.working-memory/wm-atom
             update-in [sid :slots] merge
             {n1 {:node-eid n1 :name "Entity1" :relevance 0.5 :pinned? false}
              n2 {:node-eid n2 :name "Entity2" :relevance 0.05 :pinned? false}})

      (testing "decay reduces relevance of unpinned slots"
        (wm/decay-slots! sid)
        (let [slots (:slots (wm/get-wm sid))]
          (is (< (:relevance (get slots n1)) 0.5))))

      (testing "eviction removes below-threshold slots"
        (wm/evict-slots! sid)
        (let [slots (:slots (wm/get-wm sid))]
          (is (contains? slots n1) "Entity1 should survive (0.5 * 0.85 > 0.1)")
          (is (not (contains? slots n2)) "Entity2 should be evicted (0.05 * 0.85 < 0.1)"))))

    (wm/clear-wm! sid)))

(deftest test-pinned-slots-survive-decay
  (let [sid (random-uuid)]
    (wm/create-wm! sid)
    (let [n1 (th/seed-node! "Pinned" "concept")]
      (swap! @#'xia.working-memory/wm-atom
             assoc-in [sid :slots n1]
             {:node-eid n1 :name "Pinned" :relevance 0.5 :pinned? true})
      (wm/decay-slots! sid)
      (is (= 0.5 (:relevance (get-in (wm/get-wm sid) [:slots n1])))
          "Pinned slot should not decay"))

    (wm/clear-wm! sid)))

(deftest test-fresh-slots-do-not-decay-on-the-turn-they-were-added
  (let [node-eid        (th/seed-node! "Fresh" "concept")
        decay-slot-map  #'xia.working-memory/decay-slot-map
        slots           {node-eid {:node-eid node-eid
                                   :name "Fresh"
                                   :relevance 0.8
                                   :pinned? false
                                   :added-turn 3}}
        same-turn       (decay-slot-map slots 0.85 3)
        later-turn      (decay-slot-map slots 0.85 4)]
    (is (= 0.8
           (double (get-in same-turn [node-eid :relevance]))))
    (is (= (* 0.8 0.85)
           (double (get-in later-turn [node-eid :relevance]))))))

(deftest test-refresh-boost-does-not-pin-slot
  (let [node-eid        (th/seed-node! "Sticky" "concept")
        merge-slot      #'xia.working-memory/merge-node-into-slot
        decay-slot-map  #'xia.working-memory/decay-slot-map
        step            (fn [slots]
                          (-> slots
                              (merge-slot node-eid "Sticky" :concept 0.8 1)
                              (decay-slot-map 0.85)))
        relevances      (->> {}
                             (iterate step)
                             (drop 1)
                             (take 12)
                             (map #(double (get-in % [node-eid :relevance])))
                             vec)]
    (is (every? #(< % 1.0) relevances))
    (is (< (last relevances) 0.85))
    (is (> (last relevances) 0.75))
    (is (> (last relevances) (first relevances)))))

(deftest merge-bounded-refs-dedupes-and-stays-capped
  (let [merge-refs #'xia.working-memory/merge-bounded-refs]
    (testing "episode refs keep the highest-relevance copy per episode"
      (let [merged (merge-refs [{:episode-eid :old-low
                                 :summary "old-low"
                                 :relevance 0.2}
                                {:episode-eid :keep
                                 :summary "keep-old"
                                 :relevance 0.75}]
                               [{:episode-eid :old-low
                                 :summary "old-high"
                                 :relevance 0.9}
                                {:episode-eid :new-mid
                                 :summary "new-mid"
                                 :relevance 0.6}
                                {:episode-eid :drop
                                 :summary "drop"
                                 :relevance 0.1}]
                               :episode-eid
                               2)]
        (is (= [:old-low :keep]
               (mapv :episode-eid merged)))
        (is (= [0.9 0.75]
               (mapv :relevance merged)))
        (is (= "old-high"
               (:summary (first merged))))))

    (testing "local doc refs replace weaker duplicates without exceeding the cap"
      (let [merged (merge-refs [{:doc-id "a"
                                 :name "A-old"
                                 :relevance 0.55}
                                {:doc-id "b"
                                 :name "B"
                                 :relevance 0.7}]
                               [{:doc-id "a"
                                 :name "A-new"
                                 :matched-chunks [{:index 0}]
                                 :relevance 0.8}
                                {:doc-id "c"
                                 :name "C"
                                 :relevance 0.6}
                                {:doc-id "d"
                                 :name "D"
                                 :relevance 0.4}]
                               :doc-id
                               3)
            by-id  (into {} (map (juxt :doc-id identity) merged))]
        (is (= ["a" "b" "c"]
               (mapv :doc-id merged)))
        (is (= "A-new"
               (get-in by-id ["a" :name])))
        (is (= [{:index 0}]
               (get-in by-id ["a" :matched-chunks])))
        (is (= 3 (count merged)))))))

;; ---------------------------------------------------------------------------
;; Pin / Unpin
;; ---------------------------------------------------------------------------

(deftest test-pin-unpin
  (let [sid (random-uuid)]
    (wm/create-wm! sid)
    (let [n1 (th/seed-node! "PinTest" "concept")]
      (swap! @#'xia.working-memory/wm-atom
             assoc-in [sid :slots n1]
             {:node-eid n1 :name "PinTest" :relevance 0.5 :pinned? false})

      (wm/pin! sid n1)
      (is (true? (get-in (wm/get-wm sid) [:slots n1 :pinned?])))

      (wm/unpin! sid n1)
      (is (false? (get-in (wm/get-wm sid) [:slots n1 :pinned?]))))

    (wm/clear-wm! sid)))

;; ---------------------------------------------------------------------------
;; wm->context export
;; ---------------------------------------------------------------------------

(deftest test-wm-context-export
  (let [sid (db/create-session! :terminal)]
    (wm/create-wm! sid)
    (let [node-eid (th/seed-node! "Hong" "person")
          doc      (local-doc/save-upload! {:session-id sid
                                            :name "research-notes.md"
                                            :media-type "text/markdown"
                                            :text "Clojure notes for the Seattle project"})]
    (memory/set-node-property! node-eid [:location] "Seattle")
    (th/seed-fact! node-eid "likes Clojure")

    ;; Populate WM slot with the full data
    (let [merge-fn #'xia.working-memory/merge-node-into-slot]
      (swap! @#'xia.working-memory/wm-atom
             update-in [sid :slots]
             (fn [slots] (merge-fn slots node-eid "Hong" :person 0.9 1))))

    (swap! @#'xia.working-memory/wm-atom assoc-in [sid :topics] "discussing Clojure")
    (swap! @#'xia.working-memory/wm-atom assoc-in [sid :local-doc-refs]
           [{:doc-id (:id doc)
             :name (:name doc)
             :media-type (:media-type doc)
             :preview (:preview doc)
             :relevance 0.75}])

    (let [ctx (wm/wm->context sid)]
      (testing "exports topics"
        (is (= "discussing Clojure" (:topics ctx))))

      (testing "exports entities with properties"
        (let [entity (first (:entities ctx))]
          (is (= "Hong" (:name entity)))
          (is (= :person (:type entity)))
          (is (some? (:properties entity)))
          (is (= "Seattle" (:location (:properties entity))))))

      (testing "exports facts"
        (let [entity (first (:entities ctx))]
          (is (= 1 (count (:facts entity))))
          (is (= "likes Clojure" (:content (first (:facts entity)))))))

      (testing "exports local documents"
        (let [doc (first (:local-docs ctx))]
          (is (= "research-notes.md" (:name doc)))
          (is (= "text/markdown" (:media-type doc)))))

      (testing "exports turn count"
        (is (= 0 (:turn-count ctx))))))

    (wm/clear-wm! sid)))

(deftest test-review-fact-utility
  (let [node-eid (th/seed-node! "UtilityTest" "concept")
        fact-eid (th/seed-fact! node-eid "likes Clojure" :utility 0.5)]
    (with-redefs [xia.llm/chat-simple
                  (fn [_messages & opts]
                    (is (= [:workload :fact-utility] opts))
                    "{\"facts\":[{\"index\":0,\"utility\":1.0}]}")]
      (is (= 1 (wm/review-fact-utility! [fact-eid] "What does Hong like?" "Hong likes Clojure."))))
    (is (< (abs-double (- 0.7
                          (double (:kg.fact/utility (db/entity fact-eid)))))
           1.0e-6))))

;; ---------------------------------------------------------------------------
;; Topic shift detection
;; ---------------------------------------------------------------------------

(deftest test-detect-topic-shift
  (let [sid (random-uuid)]
    (wm/create-wm! sid)

    (testing "no shift when topics overlap"
      (swap! @#'xia.working-memory/wm-atom assoc-in [sid :topics] "discussing Clojure web frameworks")
      (is (not (wm/detect-topic-shift? sid ["clojure" "frameworks" "ring"]))))

    (testing "shift when topics completely different"
      (swap! @#'xia.working-memory/wm-atom assoc-in [sid :topics] "discussing Clojure web frameworks")
      (is (true? (wm/detect-topic-shift? sid ["recipe" "pasta" "cooking"]))))

    (testing "no shift when no previous topics"
      (swap! @#'xia.working-memory/wm-atom assoc-in [sid :topics] nil)
      (is (nil? (wm/detect-topic-shift? sid ["anything"]))))

    (wm/clear-wm! sid)))

;; ---------------------------------------------------------------------------
;; update-wm! integration
;; ---------------------------------------------------------------------------

(deftest test-update-wm-integration
  (let [sid (random-uuid)]
    (db/transact! [{:session/id sid :session/channel :terminal :session/active? true}])
    (wm/create-wm! sid)

    ;; Seed some knowledge
    (let [node-eid (th/seed-node! "Clojure" "concept")]
      (memory/set-node-property! node-eid [:paradigm] "functional")
      (th/seed-fact! node-eid "runs on the JVM"))
    (local-doc/save-upload! {:session-id sid
                             :name "functional-notes.md"
                             :media-type "text/markdown"
                             :text "Clojure is a functional language that runs on the JVM."})

    (Thread/sleep 100) ; FTS indexing

    (testing "update-wm! populates slots from search"
      (wm/update-wm! "Tell me about Clojure" sid :terminal)
      (let [state (wm/get-wm sid)]
        (is (= 1 (:turn-count state)))
        ;; Should have found Clojure node
        (let [slot-names (->> (:slots state) vals (map :name) set)]
          (is (contains? slot-names "Clojure")))))

    (testing "update-wm! populates local document refs from search"
      (let [state (wm/get-wm sid)
            doc-names (->> (:local-doc-refs state) (map :name) set)]
        (is (contains? doc-names "functional-notes.md"))))

    (testing "slot includes properties"
      (let [state (wm/get-wm sid)
            clojure-slot (->> (:slots state) vals (filter #(= "Clojure" (:name %))) first)]
        (when clojure-slot
          (is (= "functional" (:paradigm (:properties clojure-slot)))))))

    (wm/clear-wm! sid)))

(deftest update-wm-continues-when-search-step-fails
  (let [sid (random-uuid)]
    (db/transact! [{:session/id sid :session/channel :terminal :session/active? true}])
    (wm/create-wm! sid)
    (with-redefs [xia.memory/search-nodes      (fn [& _]
                                                 (throw (ex-info "embedding unavailable"
                                                                 {:type :embedding-service-failure})))
                  xia.memory/search-facts      (constantly [])
                  xia.memory/search-episodes   (constantly [])
                  xia.memory/search-local-docs (constantly [])]
      (let [state (wm/update-wm! "Tell me about Atlas" sid :terminal)]
        (is (= 1 (:turn-count state)))
        (is (= {} (:slots state)))
        (is (= [] (:local-doc-refs state)))))
    (wm/clear-wm! sid)))

(deftest update-wm-does-not-immediately-decay-fresh-slots
  (let [sid      (random-uuid)
        node-eid (th/seed-node! "Fresh slot" "concept")]
    (db/transact! [{:session/id sid :session/channel :terminal :session/active? true}])
    (wm/create-wm! sid)
    (try
      (with-redefs [xia.memory/search-nodes      (fn [_semantic-query & _opts]
                                                   [{:eid node-eid
                                                     :name "Fresh slot"
                                                     :type :concept}])
                    xia.memory/search-facts      (constantly [])
                    xia.memory/search-episodes   (constantly [])
                    xia.memory/search-local-docs (constantly [])
                    xia.memory/connected-node-summaries (fn [_matched-eids] nil)]
        (let [state (wm/update-wm! "fresh" sid :terminal)
              slot  (get-in state [:slots node-eid])]
          (is (= 1 (:turn-count state)))
          (is (= 1 (:added-turn slot)))
          (is (= 0.8 (double (:relevance slot))))))
      (finally
        (wm/clear-wm! sid)))))

;; ---------------------------------------------------------------------------
;; Session serialization
;; ---------------------------------------------------------------------------

(deftest session-ops-serialize-per-session
  (let [sid        (random-uuid)
        gate       (promise)
        active     (atom 0)
        max-active (atom 0)
        op         (fn []
                     (#'xia.working-memory/run-session-op! sid
                      (fn [_]
                        @gate
                        (let [n (swap! active inc)]
                          (swap! max-active max n)
                          (Thread/sleep 75)
                          (swap! active dec)))))]
    (wm/create-wm! sid)
    (let [f1 (future (op))
          f2 (future (op))]
      (deliver gate true)
      @f1
      @f2
      (is (= 1 @max-active)))
    (wm/clear-wm! sid)))

(deftest session-ops-allow-different-sessions-in-parallel
  (let [sid-a      (random-uuid)
        session-op-lock #'xia.working-memory/session-op-lock
        sid-b      (loop [candidate (random-uuid)]
                     (if (identical? (@session-op-lock sid-a)
                                     (@session-op-lock candidate))
                       (recur (random-uuid))
                       candidate))
        gate       (promise)
        active     (atom 0)
        max-active (atom 0)
        op         (fn [sid]
                     (#'xia.working-memory/run-session-op! sid
                      (fn [_]
                        @gate
                        (let [n (swap! active inc)]
                          (swap! max-active max n)
                          (Thread/sleep 75)
                          (swap! active dec)))))]
    (wm/create-wm! sid-a)
    (wm/create-wm! sid-b)
    (let [f1 (future (op sid-a))
          f2 (future (op sid-b))]
      (deliver gate true)
      @f1
      @f2
      (is (= 2 @max-active)))
    (wm/clear-wm! sid-a)
    (wm/clear-wm! sid-b)))

(deftest session-op-locks-are-fixed-and-non-session-specific
  (let [locks @#'xia.working-memory/session-op-locks
        sid-a (random-uuid)
        sid-b (random-uuid)]
    (is (= 512 (count locks)))
    (is (some? (@#'xia.working-memory/session-op-lock sid-a)))
    (is (some? (@#'xia.working-memory/session-op-lock sid-b)))
    (wm/create-wm! sid-a)
    (wm/create-wm! sid-b)
    (wm/clear-wm! sid-a)
    (wm/clear-wm! sid-b)
    (is (= 512 (count @#'xia.working-memory/session-op-locks)))))
