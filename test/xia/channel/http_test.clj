(ns xia.channel.http-test
  (:require [charred.api :as json]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.channel.http :as http]
            [xia.checkpoint :as checkpoint]
            [xia.db :as db]
            [xia.schedule :as schedule]
            [xia.test-helpers :refer [wait-until with-test-db]])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(use-fixtures :each
  (fn [f]
    (with-test-db
      (fn []
        (checkpoint/reset-runtime!)
        (try
          (f)
          (finally
            (checkpoint/prepare-shutdown!)
            (checkpoint/await-background-tasks!)))))))

(defn- response-json
  [response]
  (json/read-json (:body response)))

(defn- temp-dir
  []
  (str (Files/createTempDirectory "xia-http-test"
                                  (into-array FileAttribute []))))

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
        (is (= (get body "workspace_tx")
               (get body "projection_seq")))
        (is (integer? (get body "projection_seq")))
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

(deftest command-managed-checkpoint-requires-command-token
  (with-redefs-fn {#'xia.channel.http/command-channel-token (constantly "secret-token")}
    (fn []
      (let [response (#'http/router* {:request-method :post
                                      :uri "/command/managed/checkpoints"
                                      :headers {}})
            body     (response-json response)]
        (is (= 401 (:status response)))
        (is (= "missing or invalid command token"
               (get body "error")))))))

(deftest command-managed-checkpoint-creates-staged-copy-asynchronously
  (let [checkpoint-root (temp-dir)
        support-dir     (str (db/current-db-path) "/.xia")]
    (spit (File. ^String support-dir "master.key") "do-not-copy")
    (spit (File. ^String support-dir "master.passphrase") "do-not-copy")
    (with-redefs-fn {#'xia.channel.http/command-channel-token (constantly "secret-token")}
      (fn []
        (let [response (#'http/router* {:request-method :post
                                        :uri "/command/managed/checkpoints"
                                        :headers {"authorization" "Bearer secret-token"}
                                        :body (json/write-json-str {"staging_root" checkpoint-root})})
              body         (response-json response)
              checkpoint-id (get body "checkpoint_id")
              final-body   (wait-until
                             #(let [status-response (#'http/router* {:request-method :get
                                                                     :uri (str "/command/managed/checkpoints/" checkpoint-id)
                                                                     :headers {"authorization" "Bearer secret-token"}})
                                    status-body     (response-json status-response)]
                                (when (= "ready" (get status-body "status"))
                                  status-body))
                             {:timeout-ms 5000
                              :interval-ms 10})
              staged-db    (get final-body "staged_db_path")]
          (is (= 202 (:status response)))
          (is (= "pending" (get body "status")))
          (is (string? checkpoint-id))
          (is (some? final-body))
          (is (= "ready" (get final-body "status")))
          (is (= "prompt-passphrase" (get final-body "key_source")))
          (is (= false (get final-body "includes_secret_material")))
          (is (integer? (get final-body "workspace_tx")))
          (is (= checkpoint-root
                 (.getAbsolutePath (.getParentFile (File. ^String (get final-body "staged_path"))))))
          (is (.exists (File. ^String (get final-body "manifest_path"))))
          (is (.exists (File. ^String (get final-body "ready_marker_path"))))
          (is (.exists (File. ^String staged-db)))
          (is (.exists (File. ^String (str staged-db "/data.mdb"))))
          (is (.exists (File. ^String (str staged-db "/.xia/master.salt"))))
          (is (false? (.exists (File. ^String (str staged-db "/.xia/master.key")))))
          (is (false? (.exists (File. ^String (str staged-db "/.xia/master.passphrase"))))))))))
