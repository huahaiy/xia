(ns xia.task-event-test
  (:require [clojure.test :refer :all]
            [xia.task-event :as task-event]))

(deftest runtime-status-normalization-accepts-transport-keys
  (let [status (task-event/normalize-runtime-status
                {:state "running"
                 :phase "tool"
                 :partial_content "partial"
                 :tool_id "web-search"
                 :tool_count 2
                 :updated_at :now})]
    (is (= {:state :running
            :phase :tool
            :partial-content "partial"
            :tool-id "web-search"
            :tool-count 2
            :updated-at :now}
           status))))

(deftest task-status-event-projects-to-runtime-status
  (let [received-at (java.util.Date.)
        event {:type :task.status
               :task-id (random-uuid)
               :summary "Searching"
               :received-at received-at
               :data {:state "running"
                      :phase "tool"
                      :partial_content "Searching..."}}]
    (is (= {:state :running
            :phase :tool
            :partial-content "Searching..."
            :updated-at received-at
            :message "Searching"}
           (task-event/event-runtime-status event)))))

(deftest event-wire-body-is-channel-neutral
  (let [task-id (random-uuid)
        turn-id (random-uuid)
        created-at (java.util.Date. 0)
        body (task-event/event->wire-body
              {:id "event-1"
               :index 3
               :type :task.status
               :task-id task-id
               :turn-id turn-id
               :created-at created-at
               :summary "Running"
               :data {:state :running}}
              :instant->str #(.toString ^java.util.Date %))]
    (is (= {:id "event-1"
            :index 3
            :type "task.status"
            :task_id (str task-id)
            :turn_id (str turn-id)
            :created_at (.toString created-at)
            :summary "Running"
            :data {:state :running}}
           body))))

(deftest terminal-projection-uses-canonical-status
  (let [session-id (random-uuid)]
    (is (= {:session-id session-id
            :state :running
            :phase :tool
            :message "Tool running"
            :partial-content "Tool running"}
           (task-event/terminal-status-projection
            {:type :task.status
             :summary "Tool running"
             :data {:state "running"
                    :phase "tool"
                    :partial_content "Tool running"}}
            session-id)))))
