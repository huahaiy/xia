(ns xia.wake-projection-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.db :as db]
            [xia.schedule :as schedule]
            [xia.test-helpers :refer [with-test-db]]
            [xia.wake-projection :as wake-projection])
  (:import [java.time ZoneId]
           [java.util Date]))

(use-fixtures :each with-test-db)

(deftest current-snapshot-derives-operational-wake-state
  (let [scheduled-at  (Date. 5000)
        running-at    (Date. 0)
        backoff-at    (Date. 15000)
        generated-at  (Date. 20000)]
    (doseq [schedule-id [:scheduled :running :retry :disabled]]
      (schedule/create-schedule! {:id schedule-id
                                  :spec {:interval-minutes 30}
                                  :type :tool
                                  :tool-id :noop}))
    (db/transact! [{:schedule/id :scheduled
                    :schedule/next-run scheduled-at}
                   {:schedule/id :running
                    :schedule/next-run running-at}
                   {:schedule/id :retry
                    :schedule/next-run backoff-at}
                   {:schedule/id :disabled
                    :schedule/enabled? false}
                   {:schedule.state/schedule-id :running
                    :schedule.state/status :running}
                   {:schedule.state/schedule-id :retry
                    :schedule.state/status :backoff
                    :schedule.state/backoff-until backoff-at}])
    (schedule/claim-schedule-run! :running running-at)
    (let [snapshot-a (wake-projection/current-snapshot {:generated-at generated-at})
          snapshot-b (wake-projection/current-snapshot {:generated-at (Date. 25000)})
          schedules  (into {} (map (juxt :schedule_id identity)) (:schedules snapshot-a))]
      (is (= (or (db/current-instance-id) "default")
             (:tenant_id snapshot-a)))
      (is (= 1 (:projection_schema_version snapshot-a)))
      (is (= (str (ZoneId/systemDefault))
             (:effective_timezone snapshot-a)))
      (is (= "1970-01-01T00:00:20Z"
             (:generated_at snapshot-a)))
      (is (= "1970-01-01T00:00:05Z"
             (:next_global_wake_at snapshot-a)))
      (is (= (:projection_seq snapshot-a)
             (:projection_seq snapshot-b)))
      (is (string? (:projection_seq snapshot-a)))
      (is (= {:schedule_id "scheduled"
              :enabled true
              :status "scheduled"
              :next_wake_at "1970-01-01T00:00:05Z"
              :wake_reason "user_schedule"}
             (get schedules "scheduled")))
      (is (= {:schedule_id "running"
              :enabled true
              :status "running"
              :next_wake_at "1970-01-01T00:30:00Z"
              :wake_reason "user_schedule"}
             (get schedules "running")))
      (is (= {:schedule_id "retry"
              :enabled true
              :status "backoff"
              :next_wake_at "1970-01-01T00:00:15Z"
              :wake_reason "retry_backoff"}
             (get schedules "retry")))
      (is (= {:schedule_id "disabled"
              :enabled false
              :status "disabled"
              :next_wake_at nil
              :wake_reason nil}
             (get schedules "disabled"))))))
