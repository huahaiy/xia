(ns xia.working-memory-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [xia.test-helpers :as th]
            [xia.db :as db]
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
  (wm/create-wm! (random-uuid))

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
             assoc-in [:slots node-eid]
             {:node-eid node-eid :name "Hong" :type :person :relevance 0.8})
      (let [terms (wm/extract-search-terms "hello there")]
        (is (some #{"hong"} terms)))))

  (testing "limits to 20 terms"
    (let [long-msg (apply str (interpose " " (map #(str "word" %) (range 50))))
          terms (wm/extract-search-terms long-msg)]
      (is (<= (count terms) 20))))

  ;; Clear WM slots before testing empty input (earlier test added "Hong" to WM)
  (wm/clear-wm!)
  (wm/create-wm! (random-uuid))

  (testing "handles empty input (no WM entities)"
    (let [terms (wm/extract-search-terms "")]
      (is (empty? terms))))

  (wm/clear-wm!))

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
      (is (contains? results :episodes)))))

;; ---------------------------------------------------------------------------
;; Slot merge with properties
;; ---------------------------------------------------------------------------

(deftest test-merge-node-into-slot-includes-properties
  (wm/create-wm! (random-uuid))

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

  (wm/clear-wm!))

;; ---------------------------------------------------------------------------
;; Decay & eviction
;; ---------------------------------------------------------------------------

(deftest test-decay-and-eviction
  (wm/create-wm! (random-uuid))

  (let [n1 (th/seed-node! "Entity1" "concept")
        n2 (th/seed-node! "Entity2" "concept")]
    ;; Manually populate slots
    (swap! @#'xia.working-memory/wm-atom
           update :slots merge
           {n1 {:node-eid n1 :name "Entity1" :relevance 0.5 :pinned? false}
            n2 {:node-eid n2 :name "Entity2" :relevance 0.05 :pinned? false}})

    (testing "decay reduces relevance of unpinned slots"
      (wm/decay-slots!)
      (let [slots (:slots (wm/get-wm))]
        (is (< (:relevance (get slots n1)) 0.5))))

    (testing "eviction removes below-threshold slots"
      (wm/evict-slots!)
      (let [slots (:slots (wm/get-wm))]
        (is (contains? slots n1) "Entity1 should survive (0.5 * 0.85 > 0.1)")
        (is (not (contains? slots n2)) "Entity2 should be evicted (0.05 * 0.85 < 0.1)"))))

  (wm/clear-wm!))

(deftest test-pinned-slots-survive-decay
  (wm/create-wm! (random-uuid))

  (let [n1 (th/seed-node! "Pinned" "concept")]
    (swap! @#'xia.working-memory/wm-atom
           assoc-in [:slots n1]
           {:node-eid n1 :name "Pinned" :relevance 0.5 :pinned? true})
    (wm/decay-slots!)
    (is (= 0.5 (:relevance (get-in (wm/get-wm) [:slots n1])))
        "Pinned slot should not decay"))

  (wm/clear-wm!))

;; ---------------------------------------------------------------------------
;; Pin / Unpin
;; ---------------------------------------------------------------------------

(deftest test-pin-unpin
  (wm/create-wm! (random-uuid))
  (let [n1 (th/seed-node! "PinTest" "concept")]
    (swap! @#'xia.working-memory/wm-atom
           assoc-in [:slots n1]
           {:node-eid n1 :name "PinTest" :relevance 0.5 :pinned? false})

    (wm/pin! n1)
    (is (true? (get-in (wm/get-wm) [:slots n1 :pinned?])))

    (wm/unpin! n1)
    (is (false? (get-in (wm/get-wm) [:slots n1 :pinned?]))))

  (wm/clear-wm!))

;; ---------------------------------------------------------------------------
;; wm->context export
;; ---------------------------------------------------------------------------

(deftest test-wm-context-export
  (wm/create-wm! (random-uuid))

  (let [node-eid (th/seed-node! "Hong" "person")]
    (memory/set-node-property! node-eid [:location] "Seattle")
    (th/seed-fact! node-eid "likes Clojure")

    ;; Populate WM slot with the full data
    (let [merge-fn #'xia.working-memory/merge-node-into-slot]
      (swap! @#'xia.working-memory/wm-atom
             update :slots
             (fn [slots] (merge-fn slots node-eid "Hong" :person 0.9 1))))

    (swap! @#'xia.working-memory/wm-atom assoc :topics "discussing Clojure")

    (let [ctx (wm/wm->context)]
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

      (testing "exports turn count"
        (is (= 0 (:turn-count ctx))))))

  (wm/clear-wm!))

;; ---------------------------------------------------------------------------
;; Topic shift detection
;; ---------------------------------------------------------------------------

(deftest test-detect-topic-shift
  (wm/create-wm! (random-uuid))

  (testing "no shift when topics overlap"
    (swap! @#'xia.working-memory/wm-atom assoc :topics "discussing Clojure web frameworks")
    (is (not (wm/detect-topic-shift? ["clojure" "frameworks" "ring"]))))

  (testing "shift when topics completely different"
    (swap! @#'xia.working-memory/wm-atom assoc :topics "discussing Clojure web frameworks")
    (is (true? (wm/detect-topic-shift? ["recipe" "pasta" "cooking"]))))

  (testing "no shift when no previous topics"
    (swap! @#'xia.working-memory/wm-atom assoc :topics nil)
    (is (nil? (wm/detect-topic-shift? ["anything"]))))

  (wm/clear-wm!))

;; ---------------------------------------------------------------------------
;; update-wm! integration
;; ---------------------------------------------------------------------------

(deftest test-update-wm-integration
  (let [sid (random-uuid)]
    (wm/create-wm! sid)

    ;; Seed some knowledge
    (let [node-eid (th/seed-node! "Clojure" "concept")]
      (memory/set-node-property! node-eid [:paradigm] "functional")
      (th/seed-fact! node-eid "runs on the JVM"))

    (Thread/sleep 100) ; FTS indexing

    (testing "update-wm! populates slots from search"
      (wm/update-wm! "Tell me about Clojure" sid :terminal)
      (let [state (wm/get-wm)]
        (is (= 1 (:turn-count state)))
        ;; Should have found Clojure node
        (let [slot-names (->> (:slots state) vals (map :name) set)]
          (is (contains? slot-names "Clojure")))))

    (testing "slot includes properties"
      (let [state (wm/get-wm)
            clojure-slot (->> (:slots state) vals (filter #(= "Clojure" (:name %))) first)]
        (when clojure-slot
          (is (= "functional" (:paradigm (:properties clojure-slot)))))))

    (wm/clear-wm!)))
