(ns xia.schedule-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.schedule :as schedule]
            [xia.scheduler :as scheduler]
            [xia.test-helpers :refer [with-test-db]]
            [xia.tool :as tool]))

(use-fixtures :each with-test-db)

(deftest create-tool-schedule
  (let [result (schedule/create-schedule!
                {:id      :test-tool
                 :name    "Test Tool Schedule"
                 :spec    {:minute #{0} :hour #{9}}
                 :type    :tool
                 :tool-id :web-fetch})]
    (is (= :test-tool (:id result)))
    (is (some? (:next-run result)))))

(deftest create-rejects-too-frequent
  (let [decisions (atom [])]
    (with-redefs [prompt/policy-decision! (fn [decision]
                                            (swap! decisions conj decision)
                                            nil)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"too frequent"
           (schedule/create-schedule!
            {:id :x :spec {} :type :tool :tool-id :x})))
      (is (some #(= {:decision-type :schedule-frequency-policy
                     :allowed? false
                     :mode :calendar-frequency
                     :min-interval-minutes 5}
                    (select-keys %
                                 [:decision-type
                                  :allowed?
                                  :mode
                                  :min-interval-minutes]))
                @decisions)))))

(deftest safe-schedule-history-redacts-audit-actions
  (schedule/create-schedule!
   {:id :audit-safe :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (schedule/record-run! :audit-safe
    {:started-at  (java.util.Date.)
     :finished-at (java.util.Date.)
     :status      :error
     :actions     [{:tool-id "browser-login" :status "blocked"}]
     :error       "sensitive failure"})
  (let [run (first (schedule/safe-schedule-history :audit-safe 1))]
    (is (= :error (:status run)))
    (is (not (contains? run :actions)))
    (is (not (contains? run :error)))))

(deftest tool-schedule-runs-through-task-spec-runner
  (db/install-tool! {:id          :scheduled-safe
                     :name        "scheduled-safe"
                     :description "Scheduled safe tool"
                     :approval    :auto
                     :handler     "(fn [_] {\"summary\" \"scheduled ok\" \"content\" \"ok\"})"})
  (tool/load-tool! :scheduled-safe)
  (schedule/create-schedule!
   {:id :runner-schedule
    :spec {:minute #{0} :hour #{9}}
    :type :tool
    :tool-id :scheduled-safe})
  (let [sched   (schedule/get-schedule :runner-schedule)
        _       (scheduler/run-tool-schedule! sched)
        task-id (schedule/schedule-task-id :runner-schedule)
        task    (db/get-task task-id)
        items   (mapcat #(db/turn-items (:id %))
                        (db/task-turns task-id))
        run     (first (schedule/schedule-history :runner-schedule))]
    (is (= :completed (:state task)))
    (is (= :completed (get-in task [:meta :task-spec :status])))
    (is (= :success (:status run)))
    (is (some #(= :task-step (:type %)) items))
    (is (some #(= :tool-call (:type %)) items))
    (is (some #(= :tool-result (:type %)) items))))

(deftest schedule-limit-enforced
  (let [decisions (atom [])]
    (dotimes [i 50]
      (schedule/create-schedule!
       {:id (keyword (str "sched-" i))
        :spec {:minute #{0} :hour #{9}}
        :type :tool
        :tool-id :x}))
    (with-redefs [prompt/policy-decision! (fn [decision]
                                            (swap! decisions conj decision)
                                            nil)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Too many schedules"
           (schedule/create-schedule!
            {:id :one-too-many
             :spec {:minute #{0} :hour #{9}}
             :type :tool
             :tool-id :x})))
      (is (some #(= {:decision-type :schedule-count-policy
                     :allowed? false
                     :mode :schedule-limit
                     :current-count 50
                     :max-schedules 50}
                    (select-keys %
                                 [:decision-type
                                  :allowed?
                                  :mode
                                  :current-count
                                  :max-schedules]))
                @decisions)))))
