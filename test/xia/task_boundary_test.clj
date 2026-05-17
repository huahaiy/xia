(ns xia.task-boundary-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.agent.task-runtime :as task-runtime]
            [xia.db :as db]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest boundary-finalizer-persists-explicit-boundary-fields
  (let [session-id (db/create-session! :terminal)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :schedule
                                     :state :running
                                     :title "Daily report"})
        checkpoint {:summary "Gathered source data"
                    :current-focus "Prepare the daily report"
                    :next-step "Draft the report"
                    :stack [{:title "Daily report"}
                            {:title "Drafting"
                             :next-step "Draft the report"
                             :open-questions ["Confirm final numbers?"]}]
                    :open-questions ["Who should review the final report?"]}]
    (task-runtime/save-task-checkpoint! task-id checkpoint)
    (task-runtime/sync-runtime-task! task-id {:state :paused
                                              :stop-reason :budget})
    (let [stored   (:boundary (db/get-task task-id))
          rendered (task-runtime/task-boundary-summary task-id)]
      (is (= :pause (:boundary/kind stored)))
      (is (= :paused (:boundary/state stored)))
      (is (= "Gathered source data" (:boundary/summary stored)))
      (is (= "Draft the report" (:boundary/next-step stored)))
      (is (= "Draft the report" (:boundary/stack-tip stored)))
      (is (= ["Who should review the final report?"
              "Confirm final numbers?"]
             (:boundary/open-questions stored)))
      (is (re-find #"next scheduled run" (:boundary/resume-hint stored)))
      (is (re-find #"next scheduled run" (:boundary/schedule-run-hint stored)))
      (is (= (:boundary/summary stored) (:summary rendered)))
      (is (= (:boundary/resume-hint stored) (:resume-hint rendered)))
      (is (= (:boundary/next-step stored) (:next-step rendered))))))

(deftest boundary-finalizer-keeps-required-shape-without-schedule
  (let [session-id (db/create-session! :terminal)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :interactive
                                     :state :running
                                     :title "One-shot task"})]
    (task-runtime/sync-runtime-task! task-id {:state :completed})
    (let [stored (:boundary (db/get-task task-id))]
      (is (= :completion (:boundary/kind stored)))
      (is (contains? stored :boundary/summary))
      (is (contains? stored :boundary/resume-hint))
      (is (contains? stored :boundary/next-step))
      (is (contains? stored :boundary/stack-tip))
      (is (contains? stored :boundary/open-questions))
      (is (contains? stored :boundary/schedule-run-hint))
      (is (= [] (:boundary/open-questions stored))))))
