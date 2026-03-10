(ns xia.schedule-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.schedule :as schedule]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

;; ---------------------------------------------------------------------------
;; Create
;; ---------------------------------------------------------------------------

(deftest create-tool-schedule
  (let [result (schedule/create-schedule!
                 {:id      :test-tool
                  :name    "Test Tool Schedule"
                  :spec    {:minute #{0} :hour #{9}}
                  :type    :tool
                  :tool-id :web-fetch})]
    (is (= :test-tool (:id result)))
    (is (some? (:next-run result)))))

(deftest create-prompt-schedule
  (let [result (schedule/create-schedule!
                 {:id     :test-prompt
                  :name   "Morning Check"
                  :spec   {:minute #{0} :hour #{8}}
                  :type   :prompt
                  :prompt "Check my email and summarize"})]
    (is (= :test-prompt (:id result)))
    (is (some? (:next-run result)))))

(deftest create-interval-schedule
  (let [result (schedule/create-schedule!
                 {:id      :every-30
                  :spec    {:interval-minutes 30}
                  :type    :tool
                  :tool-id :web-fetch})]
    (is (= :every-30 (:id result)))
    (is (some? (:next-run result)))))

(deftest create-requires-id
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"must have an :id"
        (schedule/create-schedule! {:spec {:minute #{0}} :type :tool :tool-id :x}))))

(deftest create-requires-spec
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"must have a :spec"
        (schedule/create-schedule! {:id :x :type :tool :tool-id :x}))))

(deftest create-requires-valid-type
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"must be :tool or :prompt"
        (schedule/create-schedule! {:id :x :spec {:minute #{0}} :type :bad}))))

(deftest create-tool-requires-tool-id
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"must specify :tool-id"
        (schedule/create-schedule! {:id :x :spec {:minute #{0}} :type :tool}))))

(deftest create-prompt-requires-prompt
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"must specify :prompt"
        (schedule/create-schedule! {:id :x :spec {:minute #{0}} :type :prompt}))))

(deftest create-rejects-too-frequent
  ;; Empty spec = every minute
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"too frequent"
        (schedule/create-schedule!
          {:id :x :spec {} :type :tool :tool-id :x}))))

(deftest create-rejects-short-interval
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"too frequent"
        (schedule/create-schedule!
          {:id :x :spec {:interval-minutes 2} :type :tool :tool-id :x}))))

(deftest create-allows-5-min-interval
  (let [result (schedule/create-schedule!
                 {:id :every-5 :spec {:interval-minutes 5} :type :tool :tool-id :x})]
    (is (= :every-5 (:id result)))))

(deftest create-coerces-vectors-to-sets
  ;; Tool params arrive as JSON arrays → vectors
  (let [result (schedule/create-schedule!
                 {:id      :vec-test
                  :spec    {:minute [0 30] :hour [9 17]}
                  :type    :tool
                  :tool-id :x})]
    (is (= :vec-test (:id result)))
    (let [sched (schedule/get-schedule :vec-test)]
      (is (= #{0 30} (:minute (:spec sched))))
      (is (= #{9 17} (:hour (:spec sched)))))))

;; ---------------------------------------------------------------------------
;; Get
;; ---------------------------------------------------------------------------

(deftest get-schedule-returns-details
  (schedule/create-schedule!
    {:id          :detail-test
     :name        "Detail Test"
     :description "Testing details"
     :spec        {:minute #{0} :hour #{9}}
     :type        :tool
     :tool-id     :web-fetch
     :tool-args   {"url" "https://example.com"}})
  (let [sched (schedule/get-schedule :detail-test)]
    (is (= :detail-test (:id sched)))
    (is (= "Detail Test" (:name sched)))
    (is (= "Testing details" (:description sched)))
    (is (= #{0} (:minute (:spec sched))))
    (is (= #{9} (:hour (:spec sched))))
    (is (= :tool (:type sched)))
    (is (= :web-fetch (:tool-id sched)))
    (is (= {"url" "https://example.com"} (:tool-args sched)))
    (is (true? (:enabled? sched)))
    (is (some? (:next-run sched)))))

(deftest get-nonexistent-returns-nil
  (is (nil? (schedule/get-schedule :nonexistent))))

;; ---------------------------------------------------------------------------
;; List
;; ---------------------------------------------------------------------------

(deftest list-schedules-test
  (schedule/create-schedule!
    {:id :sched-a :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (schedule/create-schedule!
    {:id :sched-b :spec {:minute #{0} :hour #{18}} :type :prompt :prompt "check stuff"})
  (let [scheds (schedule/list-schedules)]
    (is (= 2 (count scheds)))
    (is (every? :id scheds))
    (is (every? :spec scheds))))

;; ---------------------------------------------------------------------------
;; Update
;; ---------------------------------------------------------------------------

(deftest update-schedule-name
  (schedule/create-schedule!
    {:id :upd-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (let [updated (schedule/update-schedule! :upd-test {:name "New Name"})]
    (is (= "New Name" (:name updated)))))

(deftest update-schedule-spec-recalculates-next-run
  (schedule/create-schedule!
    {:id :upd-spec :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (let [before (:next-run (schedule/get-schedule :upd-spec))
        _      (schedule/update-schedule! :upd-spec {:spec {:minute #{0} :hour #{18}}})
        after  (:next-run (schedule/get-schedule :upd-spec))]
    (is (some? after))
    (is (not= before after))))

(deftest update-nonexistent-throws
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"not found"
        (schedule/update-schedule! :nope {:name "x"}))))

;; ---------------------------------------------------------------------------
;; Pause / Resume
;; ---------------------------------------------------------------------------

(deftest pause-and-resume
  (schedule/create-schedule!
    {:id :pausable :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (is (true? (:enabled? (schedule/get-schedule :pausable))))
  (schedule/pause-schedule! :pausable)
  (is (false? (:enabled? (schedule/get-schedule :pausable))))
  (schedule/resume-schedule! :pausable)
  (is (true? (:enabled? (schedule/get-schedule :pausable)))))

;; ---------------------------------------------------------------------------
;; Remove
;; ---------------------------------------------------------------------------

(deftest remove-schedule-test
  (schedule/create-schedule!
    {:id :removable :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (is (some? (schedule/get-schedule :removable)))
  (schedule/remove-schedule! :removable)
  (is (nil? (schedule/get-schedule :removable))))

;; ---------------------------------------------------------------------------
;; Run history
;; ---------------------------------------------------------------------------

(deftest record-and-query-history
  (schedule/create-schedule!
    {:id :hist-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (let [now (java.util.Date.)]
    (schedule/record-run! :hist-test
      {:started-at  now
       :finished-at (java.util.Date.)
       :status      :success
       :result      "all good"})
    (schedule/record-run! :hist-test
      {:started-at  (java.util.Date.)
       :finished-at (java.util.Date.)
       :status      :error
       :error       "something broke"}))
  (let [history (schedule/schedule-history :hist-test)]
    (is (= 2 (count history)))
    ;; Most recent first
    (is (= :error (:status (first history))))
    (is (= :success (:status (second history))))))

(deftest record-run-updates-last-run
  (schedule/create-schedule!
    {:id :last-run-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (is (nil? (:last-run (schedule/get-schedule :last-run-test))))
  (schedule/record-run! :last-run-test
    {:started-at (java.util.Date.) :status :success})
  (is (some? (:last-run (schedule/get-schedule :last-run-test)))))

(deftest trim-history-keeps-recent
  (schedule/create-schedule!
    {:id :trim-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (dotimes [_ 5]
    (schedule/record-run! :trim-test
      {:started-at (java.util.Date.) :status :success :result "ok"})
    (Thread/sleep 10))
  (is (= 5 (count (schedule/schedule-history :trim-test 100))))
  (schedule/trim-history! :trim-test 2)
  (is (= 2 (count (schedule/schedule-history :trim-test 100)))))

;; ---------------------------------------------------------------------------
;; Due schedules
;; ---------------------------------------------------------------------------

(deftest due-schedules-query
  (schedule/create-schedule!
    {:id :due-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  ;; Set next-run to the past
  (db/transact! [{:schedule/id :due-test
                  :schedule/next-run (java.util.Date. 0)}])
  (let [due (schedule/due-schedules (java.util.Date.))]
    (is (= 1 (count due)))
    (is (= :due-test (:id (first due))))))

(deftest disabled-schedules-not-due
  (schedule/create-schedule!
    {:id :disabled-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (db/transact! [{:schedule/id :disabled-test
                  :schedule/next-run (java.util.Date. 0)}])
  (schedule/pause-schedule! :disabled-test)
  (is (empty? (schedule/due-schedules (java.util.Date.)))))

;; ---------------------------------------------------------------------------
;; Schedule limit
;; ---------------------------------------------------------------------------

(deftest schedule-limit-enforced
  (dotimes [i 50]
    (schedule/create-schedule!
      {:id (keyword (str "sched-" i))
       :spec {:minute #{0} :hour #{9}}
       :type :tool
       :tool-id :x}))
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"Too many schedules"
        (schedule/create-schedule!
          {:id :one-too-many :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x}))))
