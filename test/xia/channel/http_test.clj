(ns xia.channel.http-test
  (:require [charred.api :as json]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.channel.http :as http]
            [xia.db :as db]
            [xia.schedule :as schedule]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(defn- response-json
  [response]
  (json/read-json (:body response)))

(deftest command-wake-projection-requires-command-token
  (with-redefs-fn {#'xia.channel.http/command-channel-token (constantly "secret-token")}
    (fn []
      (let [response (#'http/router* {:request-method :get
                                      :uri "/command/managed/wake-projection"
                                      :headers {}})
            body     (response-json response)]
        (is (= 401 (:status response)))
        (is (= "missing or invalid command token"
               (get body "error")))))))

(deftest command-wake-projection-returns-full-snapshot-and-etag
  (schedule/create-schedule! {:id :digest
                              :spec {:interval-minutes 30}
                              :type :tool
                              :tool-id :noop})
  (with-redefs-fn {#'xia.channel.http/command-channel-token (constantly "secret-token")}
    (fn []
        (let [response (#'http/router* {:request-method :get
                                        :uri "/command/managed/wake-projection"
                                        :headers {"authorization" "Bearer secret-token"}})
              body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= "application/json"
               (get-in response [:headers "Content-Type"])))
        (is (= (str "\"" (get body "projection_seq") "\"")
               (get-in response [:headers "ETag"])))
        (is (= (or (db/current-instance-id) "default")
               (get body "tenant_id")))
        (is (= 1 (get body "projection_schema_version")))
        (is (string? (get body "projection_seq")))
        (is (string? (get body "generated_at")))
        (is (string? (get body "effective_timezone")))
        (is (vector? (get body "schedules")))
        (is (= {"schedule_id" "digest"
                "enabled" true
                "status" "scheduled"
                "wake_reason" "user_schedule"}
               (select-keys (first (get body "schedules"))
                            ["schedule_id" "enabled" "status" "wake_reason"])))
        (is (string? (get (first (get body "schedules")) "next_wake_at")))))))

(deftest command-runtime-status-includes-memory-consolidation-summary
  (with-redefs-fn
    {#'xia.channel.http/command-channel-token (constantly "secret-token")
     #'xia.runtime-health/idle-status
     (constantly {:phase :running
                  :draining? false
                  :drain-requested-at nil
                  :accepting-new-work? true
                  :idle? true
                  :shutdown-allowed? false
                  :blockers []
                  :activity {:agent {:active-session-turn-count 0
                                     :active-session-run-count 0
                                     :active-task-run-count 0}
                             :scheduler {:running? true
                                         :running-schedule-count 0
                                         :maintenance-running? false}
                             :hippocampus {:accepting? true
                                           :pending-background-task-count 1}
                             :llm {:accepting? true
                                   :pending-log-write-count 0}}})
     #'xia.hippocampus/consolidation-summary
     (constantly {:accepting true
                  :pending_background_task_count 1
                  :backlog {:pending_episode_count 3
                            :failed_episode_count 1
                            :last_failed_episode_at "2026-04-05T10:00:00Z"}
                  :stats {:started_at "2026-04-05T09:00:00Z"
                          :attempted_episode_count 12
                          :successful_episode_count 9
                          :failed_attempt_count 3
                          :invalid_extraction_count 2
                          :exception_count 1
                          :success_rate 0.75
                          :extracted_entity_count 21
                          :extracted_relation_count 8
                          :extracted_fact_count 34
                          :avg_extracted_entities_per_success 2.3333333333333335
                          :avg_extracted_relations_per_success 0.8888888888888888
                          :avg_extracted_facts_per_success 3.7777777777777777
                          :last_attempt_at "2026-04-05T10:15:00Z"
                          :last_success_at "2026-04-05T10:10:00Z"
                          :last_failure_at "2026-04-05T10:15:00Z"
                          :last_error "knowledge extraction returned invalid JSON"
                          :last_error_kind "invalid-extraction"}})}
    (fn []
      (let [response (#'http/router* {:request-method :get
                                      :uri "/command/runtime/status"
                                      :headers {"authorization" "Bearer secret-token"}})
            body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= 3 (get-in body ["memory_consolidation" "backlog" "pending_episode_count"])))
        (is (= 1 (get-in body ["memory_consolidation" "backlog" "failed_episode_count"])))
        (is (= 12 (get-in body ["memory_consolidation" "stats" "attempted_episode_count"])))
        (is (= 0.75 (get-in body ["memory_consolidation" "stats" "success_rate"])))
        (is (= "invalid-extraction"
               (get-in body ["memory_consolidation" "stats" "last_error_kind"])))
        (is (= 1 (get-in body ["activity" "hippocampus" "pending_background_task_count"])))))))
