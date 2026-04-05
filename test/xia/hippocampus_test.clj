(ns xia.hippocampus-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.hippocampus :as hippo]
            [xia.test-helpers :as th]))

(use-fixtures :each
  (fn [f]
    (th/with-test-db
      (fn []
        (hippo/reset-runtime!)
        (f)))))

(deftest consolidation-summary-tracks-successful-extraction-yield
  (th/seed-episode! "User likes Clojure and works at Acme.")
  (with-redefs [xia.llm/chat-simple
                (fn [_ & {:keys [workload]}]
                  (case workload
                    :memory-importance "{\"episodes\":[{\"index\":0,\"importance\":0.8}]}"
                    :memory-extraction "{\"entities\":[{\"name\":\"User\",\"type\":\"person\",\"facts\":[\"likes Clojure\",\"works at Acme\"]},{\"name\":\"Acme\",\"type\":\"thing\",\"facts\":[\"is the user's employer\"]}],\"relations\":[{\"from\":\"User\",\"to\":\"Acme\",\"type\":\"works-at\",\"label\":\"works at\"}]}"
                    (throw (ex-info "unexpected workload" {:workload workload}))))]
    (hippo/consolidate-pending!))
  (let [summary (hippo/consolidation-summary)]
    (is (= 0 (get-in summary [:backlog :pending_episode_count])))
    (is (= 0 (get-in summary [:backlog :failed_episode_count])))
    (is (= 1 (get-in summary [:stats :attempted_episode_count])))
    (is (= 1 (get-in summary [:stats :successful_episode_count])))
    (is (= 0 (get-in summary [:stats :failed_attempt_count])))
    (is (= 0 (get-in summary [:stats :invalid_extraction_count])))
    (is (= 2 (get-in summary [:stats :extracted_entity_count])))
    (is (= 1 (get-in summary [:stats :extracted_relation_count])))
    (is (= 3 (get-in summary [:stats :extracted_fact_count])))
    (is (= 1.0 (get-in summary [:stats :success_rate])))
    (is (string? (get-in summary [:stats :started_at])))
    (is (string? (get-in summary [:stats :last_success_at])))
    (is (nil? (get-in summary [:stats :last_error])))))

(deftest consolidation-summary-tracks-invalid-extractions-and-failed-episodes
  (th/seed-episode! "Episode with invalid extraction response.")
  (with-redefs [xia.llm/chat-simple
                (fn [_ & {:keys [workload]}]
                  (case workload
                    :memory-importance "{\"episodes\":[{\"index\":0,\"importance\":0.4}]}"
                    :memory-extraction "not-json"
                    (throw (ex-info "unexpected workload" {:workload workload}))))]
    (hippo/consolidate-pending!))
  (let [summary (hippo/consolidation-summary)]
    (is (= 0 (get-in summary [:backlog :pending_episode_count])))
    (is (= 1 (get-in summary [:backlog :failed_episode_count])))
    (is (string? (get-in summary [:backlog :last_failed_episode_at])))
    (is (= 1 (get-in summary [:stats :attempted_episode_count])))
    (is (= 0 (get-in summary [:stats :successful_episode_count])))
    (is (= 1 (get-in summary [:stats :failed_attempt_count])))
    (is (= 1 (get-in summary [:stats :invalid_extraction_count])))
    (is (= 0 (get-in summary [:stats :exception_count])))
    (is (= 0.0 (get-in summary [:stats :success_rate])))
    (is (= "knowledge extraction returned invalid JSON"
           (get-in summary [:stats :last_error])))
    (is (= "invalid-extraction"
           (get-in summary [:stats :last_error_kind])))
    (is (string? (get-in summary [:stats :last_failure_at])))))
