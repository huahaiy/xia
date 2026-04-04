(ns xia.runtime-health-test
  (:require [clojure.test :refer [deftest is testing]]
            [xia.runtime-health :as runtime-health]))

(deftest idle-status-requires-a-running-phase-and-no-active-work
  (testing "running with no blockers is idle and shutdown-safe"
    (with-redefs [xia.runtime-state/phase (constantly :running)
                  xia.agent/runtime-activity (constantly {:active-session-turn-count 0
                                                         :active-session-run-count 0
                                                         :active-task-run-count 0})
                  xia.scheduler/runtime-activity (constantly {:running? true
                                                              :running-schedule-count 0
                                                              :maintenance-running? false})
                  xia.hippocampus/runtime-activity (constantly {:accepting? true
                                                                :pending-background-task-count 0})
                  xia.llm/runtime-activity (constantly {:accepting? true
                                                       :pending-log-write-count 0})]
      (let [status (runtime-health/idle-status)]
        (is (= :running (:phase status)))
        (is (true? (:idle? status)))
        (is (true? (:shutdown-allowed? status)))
        (is (empty? (:blockers status))))))

  (testing "active work or a non-running phase blocks idle shutdown"
    (with-redefs [xia.runtime-state/phase (constantly :stopping)
                  xia.agent/runtime-activity (constantly {:active-session-turn-count 0
                                                         :active-session-run-count 1
                                                         :active-task-run-count 0})
                  xia.scheduler/runtime-activity (constantly {:running? true
                                                              :running-schedule-count 1
                                                              :maintenance-running? false})
                  xia.hippocampus/runtime-activity (constantly {:accepting? false
                                                                :pending-background-task-count 1})
                  xia.llm/runtime-activity (constantly {:accepting? false
                                                       :pending-log-write-count 1})]
      (let [status (runtime-health/idle-status)
            blockers (set (map (juxt :component :kind) (:blockers status)))]
        (is (= :stopping (:phase status)))
        (is (false? (:idle? status)))
        (is (false? (:shutdown-allowed? status)))
        (is (contains? blockers [:agent :session-runs]))
        (is (contains? blockers [:scheduler :schedule-runs]))
        (is (contains? blockers [:hippocampus :background-consolidation]))
        (is (contains? blockers [:llm :background-log-writes]))))))
