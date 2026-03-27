(ns xia.tool-test
  (:require [charred.api :as json]
            [clojure.test :refer :all]
            [xia.artifact :as artifact]
            [xia.browser :as browser]
            [xia.db :as db]
            [xia.http-client :as http-client]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.local-doc :as local-doc]
            [xia.memory :as memory]
            [xia.paths :as paths]
            [xia.prompt :as prompt]
            [xia.test-helpers :as th :refer [with-test-db]]
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

(deftest import-tool-file-rejects-reader-eval
  (let [path (doto (java.io.File/createTempFile "xia-tool" ".edn")
               (.deleteOnExit))]
    (spit path "#=(+ 1 2)")
    (is (thrown-with-msg?
          RuntimeException
          #"No dispatch macro for: ="
          (tool/import-tool-file! (.getAbsolutePath path))))))

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

(deftest memory-correct-fact-tool-corrects-memory-with-one-approval
  (tool/reset-runtime!)
  (tool/import-tool-file! "resources/tools/memory-correct-fact.edn")
  (let [node-eid   (th/seed-node! "Gmail" "service")
        _          (th/seed-fact! node-eid "service_id: hyang@juji-inc.com")
        approvals* (atom 0)
        session-id (random-uuid)]
    (prompt/register-approval! :terminal
                               (fn [_]
                                 (swap! approvals* inc)
                                 true))
    (try
      (let [result (tool/execute-tool :memory-correct-fact
                                      {"old_fact" "service_id: hyang@juji-inc.com"
                                       "corrected_fact" "Gmail service id is gmail"
                                       "entity_name" "Gmail"}
                                      {:channel :terminal
                                       :session-id session-id})
            facts  (memory/node-facts-with-eids node-eid)]
        (is (= "corrected" (:status result)))
        (is (= 1 @approvals*))
        (is (= ["Gmail service id is gmail"]
               (mapv :content facts))))
      (finally
        (prompt/register-approval! :terminal nil)
        (tool/clear-session-approvals! session-id)))))

(deftest tool-definitions-are-scoped-by-context
  (tool/reset-runtime!)
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
  (tool/reset-runtime!)
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

(deftest vision-tagged-tools-require-vision-capable-provider
  (db/install-tool! {:id          :vision-tool
                     :name        "vision-tool"
                     :description "Interpret a screenshot"
                     :tags        #{:vision :image}
                     :approval    :auto
                     :handler     "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! :vision-tool)
  (let [without-vision (set (map #(get-in % [:function :name])
                                 (tool/tool-definitions {:assistant-provider {:llm.provider/id :text-only
                                                                               :llm.provider/vision? false}})))
        with-vision    (set (map #(get-in % [:function :name])
                                 (tool/tool-definitions {:assistant-provider {:llm.provider/id :vision
                                                                               :llm.provider/vision? true}})))]
    (is (not (contains? without-vision "vision-tool")))
    (is (contains? with-vision "vision-tool"))))

(deftest bundled-recent-work-tool-summarizes-cross-session-state
  (let [sid-a (db/create-session! :terminal)
        sid-b (db/create-session! :terminal)]
    (artifact/create-artifact! {:session-id sid-a
                                :name "older.md"
                                :kind :markdown
                                :content "# Older\n\nAlpha"})
    (local-doc/save-upload! {:session-id sid-a
                             :name "notes.md"
                             :media-type "text/markdown"
                             :text "# Notes\n\nAlpha"})
    (artifact/create-artifact! {:session-id sid-b
                                :name "current.md"
                                :kind :markdown
                                :content "# Current\n\nBeta"})
    (tool/ensure-bundled-tools!)
    (let [result (tool/execute-tool :recent-work {}
                                    {:channel :terminal
                                     :session-id sid-b})]
      (is (seq (get result :recent_episodes)))
      (is (= "current.md" (get-in result [:artifacts 0 :name])))
      (is (= "notes.md" (get-in result [:local_documents 0 :name]))))))

(deftest bundled-workspace-tools-share-items-across-sessions
  (let [root   (str (java.nio.file.Files/createTempDirectory "xia-workspace-tool-test"
                                                             (into-array java.nio.file.attribute.FileAttribute [])))
        sid-a  (db/create-session! :terminal)
        sid-b  (db/create-session! :terminal)
        report (artifact/create-artifact! {:session-id sid-a
                                           :name "report.md"
                                           :kind :markdown
                                           :content "# Shared\n\nAlpha"})
        doc    (local-doc/save-upload! {:session-id sid-a
                                        :name "notes.md"
                                        :media-type "text/markdown"
                                        :text "# Notes\n\nBeta"})]
    (with-redefs [paths/shared-workspace-root (constantly root)]
      (tool/ensure-bundled-tools!)
      (prompt/register-approval! :terminal (fn [_] true))
      (try
        (let [shared-note     (tool/execute-tool :workspace-write-note
                                                 {"content" "handoff note"
                                                  "title" "Handoff"}
                                                 {:channel :terminal
                                                  :session-id sid-a})
              shared-report   (tool/execute-tool :workspace-publish-artifact
                                                 {"artifact_id" (str (:id report))}
                                                 {:channel :terminal
                                                  :session-id sid-a})
              shared-doc      (tool/execute-tool :workspace-publish-doc
                                                 {"doc_id" (str (:id doc))}
                                                 {:channel :terminal
                                                  :session-id sid-a})
              listing         (tool/execute-tool :workspace-list {}
                                                 {:channel :terminal
                                                  :session-id sid-b})
              note-slice      (tool/execute-tool :workspace-read
                                                 {"item_id" (str (:id shared-note))
                                                  "max_chars" 7}
                                                 {:channel :terminal
                                                  :session-id sid-b})
              imported-report (tool/execute-tool :workspace-import-artifact
                                                 {"item_id" (str (:id shared-report))}
                                                 {:channel :terminal
                                                  :session-id sid-b})
              imported-doc    (tool/execute-tool :workspace-import-doc
                                                 {"item_id" (str (:id shared-doc))}
                                                 {:channel :terminal
                                                  :session-id sid-b})]
          (is (= "default" (:workspace_id listing)))
          (is (= #{(str (:id shared-note))
                   (str (:id shared-report))
                   (str (:id shared-doc))}
                 (set (map #(str (:id %)) (:items listing)))))
          (is (= "handoff" (:text note-slice)))
          (is (= "report.md" (:name imported-report)))
          (is (= "# Shared\n\nAlpha"
                 (:text (artifact/get-session-artifact sid-b (:id imported-report)))))
          (is (= "notes.md" (:name imported-doc)))
          (is (= "# Notes\n\nBeta" (:text imported-doc))))
        (finally
          (prompt/register-approval! :terminal nil)
          (tool/clear-session-approvals! sid-a)
          (tool/clear-session-approvals! sid-b))))))

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
    (is (= :branch-tasks (:tool/id (db/get-tool :branch-tasks))))
    (is (= :peer-list (:tool/id (db/get-tool :peer-list))))
    (is (= :peer-chat (:tool/id (db/get-tool :peer-chat))))
    (is (= :peer-instance-list (:tool/id (db/get-tool :peer-instance-list))))
    (is (= :peer-instance-start (:tool/id (db/get-tool :peer-instance-start))))
    (is (= :peer-instance-status (:tool/id (db/get-tool :peer-instance-status))))
    (is (= :peer-instance-stop (:tool/id (db/get-tool :peer-instance-stop))))
    (is (= :artifact-create (:tool/id (db/get-tool :artifact-create))))
    (is (= [["content"] ["rows"] ["data"]]
           (mapv #(get % "required")
                 (get-in (db/get-tool :artifact-create) [:tool/parameters "anyOf"]))))
    (is (= :artifact-list (:tool/id (db/get-tool :artifact-list))))
    (is (= :artifact-search (:tool/id (db/get-tool :artifact-search))))
    (is (= :artifact-read (:tool/id (db/get-tool :artifact-read))))
    (is (= :artifact-delete (:tool/id (db/get-tool :artifact-delete))))
    (is (= :email-list (:tool/id (db/get-tool :email-list))))
    (is (= :email-read (:tool/id (db/get-tool :email-read))))
    (is (= :email-send (:tool/id (db/get-tool :email-send))))
    (is (= :email-delete (:tool/id (db/get-tool :email-delete))))
    (is (= :session (:tool/approval (db/get-tool :email-delete))))
    (is (= :web-search (:tool/id (db/get-tool :web-search))))
    (is (= :browser-runtime-status (:tool/id (db/get-tool :browser-runtime-status))))
    (is (= :browser-bootstrap-runtime (:tool/id (db/get-tool :browser-bootstrap-runtime))))
    (is (= :browser-install-deps (:tool/id (db/get-tool :browser-install-deps))))
    (is (= :browser-open (:tool/id (db/get-tool :browser-open))))
    (is (= :browser-screenshot (:tool/id (db/get-tool :browser-screenshot))))
    (is (= :browser-navigate (:tool/id (db/get-tool :browser-navigate))))
    (is (= :browser-read-page (:tool/id (db/get-tool :browser-read-page))))
    (is (= :browser-query-elements (:tool/id (db/get-tool :browser-query-elements))))
    (is (= :browser-wait (:tool/id (db/get-tool :browser-wait))))
    (is (= :local-doc-search (:tool/id (db/get-tool :local-doc-search))))
    (is (= :local-doc-read (:tool/id (db/get-tool :local-doc-read))))
    (is (= :browser-list-sessions (:tool/id (db/get-tool :browser-list-sessions))))
    (is (= :browser-list-sites (:tool/id (db/get-tool :browser-list-sites))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :web-search))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :browser-runtime-status))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :browser-screenshot))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :browser-query-elements))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :peer-list))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :artifact-list))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :artifact-search))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :artifact-read))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :local-doc-search))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :local-doc-read))))
    (is (= :parallel-safe (:tool/execution-mode (db/get-tool :browser-list-sessions))))
    (is (nil? (:tool/execution-mode (db/get-tool :browser-open))))
    (is (contains? (get-in (db/get-tool :browser-open) [:tool/parameters "properties"])
                   "backend"))
    (is (contains? (get-in (db/get-tool :branch-tasks) [:tool/parameters "properties"])
                   "tasks"))
    (is (contains? (get-in (db/get-tool :email-send) [:tool/parameters "properties"])
                   "to"))
    (is (contains? (get-in (db/get-tool :email-delete) [:tool/parameters "properties"])
                   "message_id"))
    (is (contains? (get-in (db/get-tool :peer-chat) [:tool/parameters "properties"])
                   "service_id"))
    (is (contains? (get-in (db/get-tool :peer-instance-start) [:tool/parameters "properties"])
                   "instance_id"))
    (is (contains? (get-in (db/get-tool :browser-login) [:tool/parameters "properties"])
                   "backend"))
    (is (contains? (get-in (db/get-tool :browser-login-interactive) [:tool/parameters "properties"])
                   "backend"))))

(deftest bundled-email-tools-run-through-approved-service
  (tool/ensure-bundled-tools!)
  (doseq [tool-id [:email-list :email-read :email-send :email-delete]]
    (tool/load-tool! tool-id))
  (db/register-service! {:id                   :gmail
                         :name                 "Gmail"
                         :base-url             "https://gmail.googleapis.com"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :autonomous-approved? true})
  (let [requests (atom [])]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (swap! requests conj req)
                    (cond
                      (and (= :get (:method req))
                           (= "https://gmail.googleapis.com/gmail/v1/users/me/messages" (:url req)))
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body "{\"messages\":[{\"id\":\"m1\"}],\"resultSizeEstimate\":1}"}

                      (and (= :get (:method req))
                           (= "https://gmail.googleapis.com/gmail/v1/users/me/messages/m1" (:url req))
                           (= "metadata" (get-in req [:query-params "format"])))
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body "{\"id\":\"m1\",\"threadId\":\"t1\",\"snippet\":\"Need a reply\",\"labelIds\":[\"INBOX\"],\"internalDate\":\"1710000000000\",\"payload\":{\"headers\":[{\"name\":\"Subject\",\"value\":\"Budget\"},{\"name\":\"From\",\"value\":\"boss@example.com\"}]}}"}

                      (and (= :get (:method req))
                           (= "https://gmail.googleapis.com/gmail/v1/users/me/messages/m1" (:url req))
                           (= "full" (get-in req [:query-params "format"])))
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body "{\"id\":\"m1\",\"threadId\":\"t1\",\"snippet\":\"Need a reply\",\"labelIds\":[\"INBOX\"],\"payload\":{\"headers\":[{\"name\":\"Subject\",\"value\":\"Budget\"}],\"parts\":[{\"mimeType\":\"text/plain\",\"body\":{\"data\":\"SGVsbG8\"}}]}}"}

                      (and (= :post (:method req))
                           (= "https://gmail.googleapis.com/gmail/v1/users/me/messages/send" (:url req)))
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body "{\"id\":\"sent-1\",\"threadId\":\"t1\",\"labelIds\":[\"SENT\"]}"}

                      (and (= :post (:method req))
                           (= "https://gmail.googleapis.com/gmail/v1/users/me/messages/m1/modify" (:url req))
                           (= ["TRASH"] (get (json/read-json (:body req)) "addLabelIds"))
                           (= ["INBOX" "UNREAD"] (get (json/read-json (:body req)) "removeLabelIds")))
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body "{\"id\":\"m1\",\"threadId\":\"t1\",\"labelIds\":[\"TRASH\"]}"}

                      :else
                      (throw (ex-info "Unexpected Gmail request" {:request req}))))]
      (let [context {:channel          :scheduler
                     :autonomous-run?  true
                     :approval-bypass? true}
            listed  (tool/execute-tool :email-list {"max_results" 1} context)
            read    (tool/execute-tool :email-read {"message_id" "m1"} context)
            sent    (tool/execute-tool :email-send {"to" "boss@example.com"
                                                    "subject" "Re: Budget"
                                                    "body" "Done"} context)
            deleted (tool/execute-tool :email-delete {"message_id" "m1"} context)]
        (is (= "gmail" (:service-id listed)))
        (is (= 1 (:returned-count listed)))
        (is (= "Budget" (get-in listed [:messages 0 :subject])))
        (is (= "Hello" (:body read)))
        (is (= :plain (:body-kind read)))
        (is (= "sent" (:status sent)))
        (is (= "sent-1" (:id sent)))
        (is (= "trashed" (:status deleted)))
        (is (= "m1" (:id deleted)))
        (is (= "Bearer tok" (get-in (first @requests) [:headers "Authorization"])))))))

(deftest bundled-email-delete-approval-is-cached-per-session
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :email-delete)
  (db/register-service! {:id        :gmail
                         :name      "Gmail"
                         :base-url  "https://gmail.googleapis.com"
                         :auth-type :bearer
                         :auth-key  "tok"})
  (let [approvals* (atom 0)
        requests*  (atom [])
        session-id (random-uuid)]
    (prompt/register-approval! :terminal
                               (fn [_]
                                 (swap! approvals* inc)
                                 true))
    (try
      (with-redefs [http-client/request
                    (fn [req]
                      (swap! requests* conj req)
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body "{\"id\":\"m1\",\"threadId\":\"t1\",\"labelIds\":[\"TRASH\"]}"})]
        (is (= "trashed"
               (:status (tool/execute-tool :email-delete
                                           {"message_id" "m1"}
                                           {:channel :terminal
                                            :session-id session-id}))))
        (is (= "trashed"
               (:status (tool/execute-tool :email-delete
                                           {"message_id" "m2"}
                                           {:channel :terminal
                                            :session-id session-id}))))
        (is (= 1 @approvals*))
        (is (= 2 (count @requests*))))
      (finally
        (prompt/register-approval! :terminal nil)
        (tool/clear-session-approvals! session-id)))))

(deftest bundled-peer-tools-run-through-approved-service
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :peer-list)
  (tool/load-tool! :peer-chat)
  (db/register-service! {:id                   :ops-peer
                         :name                 "Ops Xia"
                         :base-url             "http://127.0.0.1:4011"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :allow-private-network? true
                         :autonomous-approved? true})
  (let [requests (atom [])]
    (with-redefs [xia.service/request
                  (fn [service-id method path & opts]
                    (swap! requests conj {:service-id service-id
                                          :method method
                                          :path path
                                          :opts (apply hash-map opts)})
                    {:status 200
                     :body {"session_id" "peer-session"
                            "role" "assistant"
                            "content" "Peer reply"}})]
      (let [context {:channel          :scheduler
                     :autonomous-run?  true
                     :approval-bypass? true}
            peers   (tool/execute-tool :peer-list {} context)
            reply   (tool/execute-tool :peer-chat {"service_id" "ops-peer"
                                                   "message" "Summarize the rollout"
                                                   "timeout_ms" 45000}
                                       context)]
        (is (= [{:service_id "ops-peer"
                 :name "Ops Xia"
                 :base_url "http://127.0.0.1:4011"
                 :allow_private_network true
                 :local true
                 :autonomous_approved true}]
               (:peers peers)))
        (is (= {:service_id "ops-peer"
                :session_id "peer-session"
                :role "assistant"
                :content "Peer reply"}
               reply))
        (is (= [{:service-id :ops-peer
                 :method :post
                 :path "/command/chat"
                 :opts {:body {"message" "Summarize the rollout"}
                        :as :json
                        :timeout 45000}}]
               @requests))))))

(deftest bundled-peer-instance-tools-execute-through-sci
  (tool/ensure-bundled-tools!)
  (doseq [tool-id [:peer-instance-list
                   :peer-instance-start
                   :peer-instance-status
                   :peer-instance-stop]]
    (tool/load-tool! tool-id))
  (let [session-id (random-uuid)
        process    (let [alive? (atom true)
                         self   (atom nil)
                         p      (proxy [Process] []
                                  (getOutputStream [] nil)
                                  (getInputStream [] nil)
                                  (getErrorStream [] nil)
                                  (waitFor [] (reset! alive? false) 0)
                                  (exitValue []
                                    (if @alive?
                                      (throw (IllegalThreadStateException. "process still running"))
                                      0))
                                  (destroy []
                                    (reset! alive? false))
                                  (destroyForcibly []
                                    (reset! alive? false)
                                    @self)
                                  (isAlive [] @alive?)
                                  (pid [] 4242))]
                     (reset! self p)
                     p)]
    (prompt/register-approval! :terminal (fn [_] true))
    (try
      (instance-supervisor/shutdown!)
      (instance-supervisor/configure! {:enabled? true
                                       :command "/opt/xia/bin/xia"})
      (instance-supervisor/set-instance-management-enabled! true)
      (with-redefs [xia.instance-supervisor/spawn-process!
                    (fn [_command _args _env _log-path]
                      process)
                    xia.instance-supervisor/wait-until-ready!
                    (fn [_base-url _process _wait-for-ready-ms _log-path]
                      true)
                    xia.instance-supervisor/start-exit-watcher!
                    (fn [_instance-id _process]
                      nil)
                    xia.instance-supervisor/wait-for-exit
                    (fn [_process _timeout-ms]
                      true)]
        (let [context {:channel :terminal
                       :session-id session-id}
              started (tool/execute-tool :peer-instance-start {"instance_id" "ops-child"
                                                               "template_instance" "base"
                                                               "port" 4115}
                                         context)
              listed  (tool/execute-tool :peer-instance-list {} context)
              status  (tool/execute-tool :peer-instance-status {"instance_id" "ops-child"}
                                         context)
              stopped (tool/execute-tool :peer-instance-stop {"instance_id" "ops-child"
                                                              "timeout_ms" 1500}
                                         context)]
          (is (= "ops-child" (:instance_id started)))
          (is (= "base" (:template_instance started)))
          (is (= 4115 (:port started)))
          (is (= "running" (:state started)))
          (is (= [{:instance_id "ops-child"
                   :service_id "xia-managed-instance-ops-child"
                   :service_name "Managed Xia ops-child"
                   :base_url "http://127.0.0.1:4115"
                   :port 4115
                   :pid 4242
                   :state "running"
                   :alive true
                   :template_instance "base"
                   :log_path (str (paths/default-instance-root "ops-child") "/xia.log")
                   :started_at (:started_at started)
                   :exited_at nil
                   :exit_code nil}]
                 (:instances listed)))
          (is (= "ops-child" (:instance_id status)))
          (is (= "running" (:state status)))
          (is (= "exited" (:state stopped)))
          (is (= 0 (:exit_code stopped)))))
      (finally
        (instance-supervisor/shutdown!)
        (instance-supervisor/set-instance-management-enabled! false)
        (instance-supervisor/configure! {:enabled? false})
        (prompt/register-approval! :terminal nil)
        (tool/clear-session-approvals! session-id)))))

(deftest branch-workers-cannot-run-branch-tasks-recursively
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :branch-tasks)
  (let [session-id (db/create-session! :terminal)
        result     (tool/execute-tool :branch-tasks
                                      {"tasks" [{"task" "nested"
                                                  "prompt" "should not run"}]}
                                      {:channel :terminal
                                       :session-id session-id
                                       :branch-worker? true})]
    (is (= "Tool branch-tasks blocked: tool branch-tasks is not available to branch workers"
           (:error result)))))

(deftest browser-runtime-tools-execute-through-sci
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :browser-runtime-status)
  (tool/load-tool! :browser-bootstrap-runtime)
  (tool/load-tool! :browser-install-deps)
  (let [status (tool/execute-tool :browser-runtime-status {} {:channel :scheduler})
        session-id (random-uuid)]
    (is (= #{:playwright}
           (set (map :backend (:backends status)))))
    (prompt/register-approval! :terminal (fn [_] true))
    (try
      (let [bootstrap (tool/execute-tool :browser-bootstrap-runtime {"backend" "playwright"}
                                         {:channel :terminal
                                          :session-id session-id})]
        (is (= :playwright (:backend bootstrap)))
        (is (= :running (:status bootstrap))))
      (let [deps (tool/execute-tool :browser-install-deps {}
                                    {:channel :terminal
                                     :session-id session-id})]
        (is (= :playwright (:backend deps)))
        (is (contains? #{:unsupported-platform :dry-run} (:status deps))))
      (finally
        (prompt/register-approval! :terminal nil)
        (tool/clear-session-approvals! session-id)
        (browser/close-all-sessions!)))))

(deftest browser-screenshot-tool-executes-through-sci
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :browser-screenshot)
  (db/set-config! :browser/playwright-enabled? "true")
  (let [opened (browser/open-session "https://example.com" :backend :playwright)
        session-id (:session-id opened)]
    (try
      (let [result (tool/execute-tool :browser-screenshot {"session_id" session-id
                                                           "full_page" true
                                                           "detail" "high"}
                                      {:channel :scheduler})]
        (is (= session-id (:session-id result)))
        (is (= :playwright (:backend result)))
        (is (= true (:full_page result)))
        (is (= "high" (:detail result)))
        (is (string? (:image_data_url result))))
      (finally
        (browser/close-session session-id)))))

(deftest browser-query-elements-tool-executes-through-sci
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :browser-query-elements)
  (let [opened (browser/open-session "https://example.com" :backend :playwright)
        session-id (:session-id opened)]
    (try
      (let [result (tool/execute-tool :browser-query-elements {"session_id" session-id
                                                               "kind" "links"
                                                               "limit" 1}
                                      {:channel :scheduler})]
        (is (= session-id (:session-id result)))
        (is (= :playwright (:backend result)))
        (is (= :links (:kind result)))
        (is (= 1 (:returned_count result)))
        (is (pos? (:total_count result)))
        (is (string? (get-in result [:elements 0 :selector]))))
      (finally
        (browser/close-session session-id)))))

(deftest local-doc-tools-execute-through-sci
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :local-doc-search)
  (tool/load-tool! :local-doc-read)
  (let [sid   (db/create-session! :http)
        saved (local-doc/save-upload! {:session-id sid
                                       :name "garage-notes.md"
                                       :media-type "text/markdown"
                                       :text "The car stays inside the garage overnight."})
        search (tool/execute-tool :local-doc-search {"query" "automobile"}
                                  {:channel :terminal
                                   :session-id sid})]
    (is (= "garage-notes.md" (:name (first search))))
    (let [read (tool/execute-tool :local-doc-read {"doc_id" (str (:id saved))
                                                   "max_chars" 12}
                                  {:channel :terminal
                                   :session-id sid})]
      (is (= (:id saved) (:id read)))
      (is (= "garage-notes.md" (:name read)))
      (is (= 0 (:offset read)))
      (is (= 12 (:end-offset read)))
      (is (= "The car stay" (:text read)))
      (is (true? (:truncated? read))))))

(deftest artifact-tools-execute-through-sci
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :artifact-create)
  (tool/load-tool! :artifact-list)
  (tool/load-tool! :artifact-search)
  (tool/load-tool! :artifact-read)
  (tool/load-tool! :artifact-delete)
  (let [sid     (db/create-session! :http)
        created (tool/execute-tool :artifact-create {"name" "report.json"
                                                     "kind" "json"
                                                     "data" {"status" "ok"}}
                                   {:channel :terminal
                                    :session-id sid})]
    (is (= "report.json" (:name created)))
    (is (= :json (:kind created)))
    (is (= "application/json" (:media-type created)))
    (let [listed (tool/execute-tool :artifact-list {}
                                    {:channel :terminal
                                     :session-id sid})]
      (is (= "report.json" (:name (first listed))))
      (is (nil? (:text (first listed)))))
    (let [search (tool/execute-tool :artifact-search {"query" "status"}
                                    {:channel :terminal
                                     :session-id sid})]
      (is (= "report.json" (:name (first search)))))
    (let [read (tool/execute-tool :artifact-read {"artifact_id" (str (:id created))
                                                  "max_chars" 8}
                                  {:channel :terminal
                                   :session-id sid})]
      (is (= (:id created) (:id read)))
      (is (= 8 (:end-offset read)))
      (is (string? (:text read)))
      (is (true? (:truncated? read))))
    (let [deleted (tool/execute-tool :artifact-delete {"artifact_id" (str (:id created))}
                                     {:channel :terminal
                                      :session-id sid})]
      (is (= "deleted" (:status deleted)))
      (is (= [] (tool/execute-tool :artifact-list {}
                                   {:channel :terminal
                                    :session-id sid}))))))

(deftest artifact-create-tool-rejects-empty-payload
  (tool/ensure-bundled-tools!)
  (tool/load-tool! :artifact-create)
  (let [sid    (db/create-session! :http)
        result (tool/execute-tool :artifact-create {}
                                  {:channel :terminal
                                   :session-id sid})]
    (is (= "Tool execution failed: missing artifact content"
           (:error result)))))

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
        (when-let [session-id (:session-id result)]
          (browser/close-session session-id))))))
