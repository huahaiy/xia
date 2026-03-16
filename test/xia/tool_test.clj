(ns xia.tool-test
  (:require [clojure.test :refer :all]
            [xia.browser :as browser]
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

(deftest autonomous-service-tools-require-approved-services
  (db/register-service! {:id                   :gmail
                         :name                 "Gmail"
                         :base-url             "https://gmail.googleapis.com"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :autonomous-approved? false})
  (db/install-tool! {:id          :service-tool
                     :name        "service-tool"
                     :description "Service tool"
                     :approval    :session
                     :handler     "(fn [_] (xia.service/request :gmail :get \"/messages\"))"})
  (tool/load-tool! :service-tool)
  (is (= "Tool service-tool blocked: no approved services are available for autonomous execution"
         (:error (tool/execute-tool :service-tool {}
                                    {:channel          :scheduler
                                     :autonomous-run?  true
                                     :approval-bypass? true})))))

(deftest autonomous-service-tools-run-through-approved-service
  (db/register-service! {:id                   :github
                         :name                 "GitHub"
                         :base-url             "https://api.github.com"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :autonomous-approved? true})
  (db/install-tool! {:id          :service-tool
                     :name        "service-tool"
                     :description "Service tool"
                     :approval    :session
                     :handler     "(fn [_] (xia.service/request :github :get \"/user\"))"})
  (tool/load-tool! :service-tool)
  (with-redefs [xia.http-client/request (fn [_]
                                          {:status 200
                                           :headers {"content-type" "application/json"}
                                           :body "{\"login\":\"hyang\"}"})]
    (is (= 200
           (:status (tool/execute-tool :service-tool {}
                                       {:channel          :scheduler
                                        :autonomous-run?  true
                                        :approval-bypass? true}))))))

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

(deftest tool-definitions-are-scoped-by-context
  (db/install-tool! {:id          :web-search
                     :name        "web-search"
                     :description "Search the web for information"
                     :tags        #{:web :research :search}
                     :approval    :auto
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (db/install-tool! {:id          :schedule-manage
                     :name        "schedule-manage"
                     :description "Manage a scheduled task"
                     :tags        #{:schedule :automation :task}
                     :approval    :auto
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :web-search)
  (tool/load-tool! :schedule-manage)
  (with-redefs [xia.working-memory/wm->context (fn [_]
                                                 {:topics   "web research"
                                                  :entities [{:name "OpenAI"}]})]
    (let [defs  (tool/tool-definitions {:session-id   (random-uuid)
                                        :user-message "search the web for OpenAI"})
          names (set (map #(get-in % [:function :name]) defs))]
      (is (contains? names "web-search"))
      (is (not (contains? names "schedule-manage"))))))

(deftest tool-definitions-fall-back-to-all-visible-tools-without-match
  (db/install-tool! {:id          :web-search
                     :name        "web-search"
                     :description "Search the web for information"
                     :tags        #{:web :research :search}
                     :approval    :auto
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (db/install-tool! {:id          :schedule-manage
                     :name        "schedule-manage"
                     :description "Manage a scheduled task"
                     :tags        #{:schedule :automation :task}
                     :approval    :auto
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :web-search)
  (tool/load-tool! :schedule-manage)
  (with-redefs [xia.working-memory/wm->context (fn [_]
                                                 {:topics   "completely unrelated"
                                                  :entities []})]
    (let [defs  (tool/tool-definitions {:session-id   (random-uuid)
                                        :user-message "hello there"})
          names (set (map #(get-in % [:function :name]) defs))]
      (is (= #{"web-search" "schedule-manage"} names)))))

(deftest autonomous-tool-definitions-hide-unavailable-privileged-tools
  (db/register-service! {:id                   :github
                         :name                 "GitHub"
                         :base-url             "https://api.github.com"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :autonomous-approved? true})
  (db/register-site-cred! {:id                     :portal
                           :name                   "Portal"
                           :login-url              "https://portal.example/login"
                           :username               "hyang"
                           :password               "pw"
                           :autonomous-approved?   false})
  (db/install-tool! {:id          :auto-tool
                     :name        "auto-tool"
                     :description "Automatic tool"
                     :approval    :auto
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (db/install-tool! {:id          :manual-tool
                     :name        "manual-tool"
                     :description "Manual tool"
                     :approval    :session
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (db/install-tool! {:id          :service-tool
                     :name        "service-tool"
                     :description "Service tool"
                     :approval    :session
                     :handler     "(fn [_] (xia.service/request :github :get \"/user\"))"})
  (db/install-tool! {:id          :site-tool
                     :name        "site-tool"
                     :description "Site tool"
                     :approval    :session
                     :handler     "(fn [_] (xia.browser/login :portal))"})
  (tool/load-tool! :auto-tool)
  (tool/load-tool! :manual-tool)
  (tool/load-tool! :service-tool)
  (tool/load-tool! :site-tool)
  (let [trusted-defs   (tool/tool-definitions {:channel          :scheduler
                                               :autonomous-run?  true
                                               :approval-bypass? true})
        untrusted-defs (tool/tool-definitions {:channel         :scheduler
                                               :autonomous-run? true})
        trusted-names  (set (map #(get-in % [:function :name]) trusted-defs))
        untrusted-names (set (map #(get-in % [:function :name]) untrusted-defs))]
    (is (contains? trusted-names "auto-tool"))
    (is (contains? trusted-names "service-tool"))
    (is (not (contains? trusted-names "manual-tool")))
    (is (not (contains? trusted-names "site-tool")))
    (is (contains? untrusted-names "auto-tool"))
    (is (not (contains? untrusted-names "service-tool")))
    (is (not (contains? untrusted-names "site-tool")))
    (is (not (contains? untrusted-names "manual-tool"))))
  (db/register-site-cred! {:id                     :portal
                           :name                   "Portal"
                           :login-url              "https://portal.example/login"
                           :username               "hyang"
                           :password               "pw"
                           :autonomous-approved?   true})
  (let [trusted-names (set (map #(get-in % [:function :name])
                                (tool/tool-definitions {:channel          :scheduler
                                                        :autonomous-run?  true
                                                        :approval-bypass? true})))]
    (is (contains? trusted-names "site-tool"))))

(deftest tool-execution-audit-is-recorded
  (db/register-service! {:id                   :github
                         :name                 "GitHub"
                         :base-url             "https://api.github.com"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :autonomous-approved? true})
  (db/install-tool! {:id          :audited-tool
                     :name        "audited-tool"
                     :description "Audited tool"
                     :approval    :session
                     :handler     "(fn [_] (xia.service/request :github :get \"/user\"))"})
  (tool/load-tool! :audited-tool)
  (let [audit-log (atom [])]
    (with-redefs [xia.http-client/request (fn [_]
                                            {:status 200
                                             :headers {"content-type" "application/json"}
                                             :body "{\"login\":\"hyang\"}"})]
      (is (= 200
             (:status (tool/execute-tool :audited-tool {}
                              {:channel          :scheduler
                               :autonomous-run?  true
                               :approval-bypass? true
                               :audit-log        audit-log})))))
    (is (= [{:type "service-request"
             :service-id "github"
             :method "get"
             :path "/user"
             :status "success"
             :http-status 200}
            {:type "tool"
             :tool-id "audited-tool"
             :tool-name "audited-tool"
             :arguments {}
             :status "success"
             :approval-policy "session"
             :approval-mode "autonomous-bypass"}]
           (mapv #(dissoc % :at) @audit-log)))))

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
    (is (= :browser-runtime-status (:tool/id (db/get-tool :browser-runtime-status))))
    (is (= :browser-bootstrap-runtime (:tool/id (db/get-tool :browser-bootstrap-runtime))))
    (is (= :browser-open (:tool/id (db/get-tool :browser-open))))
    (is (= :browser-navigate (:tool/id (db/get-tool :browser-navigate))))
    (is (= :browser-read-page (:tool/id (db/get-tool :browser-read-page))))
    (is (= :browser-wait (:tool/id (db/get-tool :browser-wait))))
    (is (= :browser-list-sessions (:tool/id (db/get-tool :browser-list-sessions))))
    (is (= :browser-list-sites (:tool/id (db/get-tool :browser-list-sites))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :web-search))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :browser-runtime-status))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :browser-list-sessions))))
    (is (nil? (:tool/execution-mode (db/get-tool :browser-open))))
    (is (contains? (get-in (db/get-tool :browser-open) [:tool/parameters "properties"])
                   "backend"))
    (is (contains? (get-in (db/get-tool :browser-login) [:tool/parameters "properties"])
                   "backend"))
    (is (contains? (get-in (db/get-tool :browser-login-interactive) [:tool/parameters "properties"])
                   "backend"))))

(deftest browser-runtime-tools-execute-through-sci
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :browser-runtime-status)
  (tool/load-tool! :browser-bootstrap-runtime)
  (let [status (tool/execute-tool :browser-runtime-status {} {:channel :scheduler})
        session-id (random-uuid)]
    (is (= #{:htmlunit :playwright}
           (set (map :backend (:backends status)))))
    (prompt/register-approval! :terminal (fn [_] true))
    (try
      (let [bootstrap (tool/execute-tool :browser-bootstrap-runtime {"backend" "playwright"}
                                         {:channel :terminal
                                          :session-id session-id})]
        (is (= :playwright (:backend bootstrap)))
        (is (= :running (:status bootstrap))))
      (finally
        (prompt/register-approval! :terminal nil)
        (tool/clear-session-approvals! session-id)
        (browser/close-all-sessions!)))))

(deftest browser-open-tool-accepts-playwright-backend
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :browser-open)
  (db/set-config! :browser/playwright-enabled? "true")
  (let [result (tool/execute-tool :browser-open {"url" "https://example.com"
                                                 "backend" "playwright"}
                                  {:channel :scheduler})]
    (try
      (is (= :playwright (:backend result)))
      (is (= "Example Domain" (:title result)))
      (finally
        (browser/close-session (:session-id result))))))
