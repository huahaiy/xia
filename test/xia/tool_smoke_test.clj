(ns xia.tool-smoke-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.test-helpers :refer [with-test-db]]
            [xia.tool :as tool]))

(use-fixtures :each with-test-db)

(deftest safe-tool-runs-without-approval
  (db/install-tool! {:id          :safe-tool
                     :name        "safe-tool"
                     :description "Safe tool"
                     :approval    :auto
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :safe-tool)
  (is (= {"status" "ok"}
         (tool/execute-tool :safe-tool {} {:channel :scheduler}))))

(deftest privileged-tool-blocks-without-approval-handler
  (db/install-tool! {:id          :privileged-tool
                     :name        "privileged-tool"
                     :description "Privileged tool"
                     :approval    :session
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :privileged-tool)
  (let [result (tool/execute-tool :privileged-tool {} {:channel :scheduler})]
    (is (= "Tool privileged-tool blocked: No approval handler available for current channel"
           (:error result)))))

(deftest privileged-tool-approval-is-cached-per-session
  (db/install-tool! {:id          :session-tool
                     :name        "session-tool"
                     :description "Session tool"
                     :approval    :session
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :session-tool)
  (let [calls      (atom 0)
        session-id (random-uuid)]
    (prompt/register-approval! :terminal
                               (fn [_]
                                 (swap! calls inc)
                                 true))
    (try
      (is (= {"status" "ok"}
             (tool/execute-tool :session-tool {} {:channel :terminal
                                                  :session-id session-id})))
      (is (= {"status" "ok"}
             (tool/execute-tool :session-tool {} {:channel :terminal
                                                  :session-id session-id})))
      (is (= 1 @calls))
      (finally
        (prompt/register-approval! :terminal nil)
        (tool/clear-session-approvals! session-id)))))

(deftest ensure-bundled-tools-installs-core-default-set
  (let [count (tool/ensure-bundled-tools!)]
    (is (pos? count))
    (is (= :branch-tasks (:tool/id (db/get-tool :branch-tasks))))
    (is (= :peer-list (:tool/id (db/get-tool :peer-list))))
    (is (= :artifact-create (:tool/id (db/get-tool :artifact-create))))
    (is (= :browser-open (:tool/id (db/get-tool :browser-open))))
    (is (= :calendar-event-create (:tool/id (db/get-tool :calendar-event-create))))
    (is (= :local-doc-search (:tool/id (db/get-tool :local-doc-search))))))

(deftest ensure-bundled-tools-refreshes-bundled-approval-policy
  (db/install-tool! {:id          :email-list
                     :name        "email-list"
                     :description "Old email list"
                     :approval    :session
                     :handler     "(fn [_] {})"})
  (tool/ensure-bundled-tools!)
  (is (= :auto (:tool/approval (db/get-tool :email-list)))))
