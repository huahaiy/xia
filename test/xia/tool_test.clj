(ns xia.tool-test
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

(deftest privileged-tool-allows-preapproved-context
  (db/install-tool! {:id          :scheduled-tool
                     :name        "scheduled-tool"
                     :description "Scheduled privileged tool"
                     :approval    :session
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :scheduled-tool)
  (is (= {"status" "ok"}
         (tool/execute-tool :scheduled-tool {}
                            {:channel :scheduler
                             :approval-bypass? true}))))

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

(deftest privileged-tool-reports-status-updates
  (db/install-tool! {:id          :status-tool
                     :name        "status-tool"
                     :description "Status tool"
                     :approval    :session
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :status-tool)
  (let [events     (atom [])
        session-id (random-uuid)]
    (prompt/register-status! :terminal
                             (fn [status]
                               (swap! events conj (select-keys status
                                                               [:state :phase :message :tool-id]))))
    (prompt/register-approval! :terminal (fn [_] true))
    (try
      (is (= {"status" "ok"}
             (tool/execute-tool :status-tool {}
                                {:channel :terminal
                                 :session-id session-id})))
      (is (= [{:state :waiting
               :phase :approval
               :message "Waiting for approval for status-tool"
               :tool-id :status-tool}
              {:state :running
               :phase :tool
               :message "Running tool status-tool"
               :tool-id :status-tool}
              {:state :running
               :phase :tool
               :message "Finished tool status-tool"
               :tool-id :status-tool}]
             @events))
      (finally
        (prompt/register-status! :terminal nil)
        (prompt/register-approval! :terminal nil)
        (tool/clear-session-approvals! session-id)))))

(deftest tool-definitions-annotate-approval-requirement
  (db/install-tool! {:id          :annotated-tool
                     :name        "annotated-tool"
                     :description "Annotated tool"
                     :approval    :session
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :annotated-tool)
  (let [defs      (tool/tool-definitions)
        tool-def  (some #(when (= "annotated-tool"
                                  (get-in % [:function :name]))
                           %)
                        defs)]
    (is (= "Annotated tool Requires user approval before execution."
           (get-in tool-def [:function :description])))))

(deftest execution-mode-controls-parallel-safety
  (db/install-tool! {:id             :parallel-tool
                     :name           "parallel-tool"
                     :description    "Parallel safe tool"
                     :approval       :auto
                     :execution-mode :parallel-safe
                     :handler        "(fn [_] {\"status\" \"ok\"})"})
  (db/install-tool! {:id             :sequential-tool
                     :name           "sequential-tool"
                     :description    "Sequential tool"
                     :approval       :auto
                     :execution-mode :sequential
                     :handler        "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :parallel-tool)
  (tool/load-tool! :sequential-tool)
  (is (true? (tool/parallel-safe? :parallel-tool)))
  (is (false? (tool/parallel-safe? :sequential-tool))))

(deftest ensure-bundled-tools-installs-default-set
  (let [count (tool/ensure-bundled-tools!)]
    (is (pos? count))
    (is (= :web-search (:tool/id (db/get-tool :web-search))))
    (is (= :browser-open (:tool/id (db/get-tool :browser-open))))
    (is (= :browser-navigate (:tool/id (db/get-tool :browser-navigate))))
    (is (= :browser-read-page (:tool/id (db/get-tool :browser-read-page))))
    (is (= :browser-wait (:tool/id (db/get-tool :browser-wait))))
    (is (= :browser-list-sessions (:tool/id (db/get-tool :browser-list-sessions))))
    (is (= :browser-list-sites (:tool/id (db/get-tool :browser-list-sites))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :web-search))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :browser-list-sessions))))
    (is (nil? (:tool/execution-mode (db/get-tool :browser-open))))))
