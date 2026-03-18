(ns xia.working-memory-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [xia.test-helpers :as th]
            [xia.db :as db]
            [xia.local-doc :as local-doc]
            [xia.memory :as memory]
            [xia.working-memory :as wm]))

(use-fixtures :each th/with-test-db)

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
    (is (< (Math/abs (- 0.7
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
        sid-b      (random-uuid)
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

(deftest clear-wm-cleans-up-session-op-agents
  (let [sid-a   (random-uuid)
        sid-b   (random-uuid)
        agents  @#'xia.working-memory/session-op-agents]
    (wm/create-wm! sid-a)
    (wm/create-wm! sid-b)
    (is (.containsKey agents sid-a))
    (is (.containsKey agents sid-b))

    (wm/clear-wm! sid-a)
    (is (not (.containsKey agents sid-a)))
    (is (.containsKey agents sid-b))

    (wm/clear-wm!)
    (is (= 0 (.size agents)))))
