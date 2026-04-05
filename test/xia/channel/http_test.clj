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
