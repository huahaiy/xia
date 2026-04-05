(ns xia.channel.http-admin-test
  (:require [charred.api :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.browser :as browser]
            [xia.channel.http.admin :as http-admin]
            [xia.db :as db]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(defn- response-json
  [response]
  (json/read-json (:body response)))

(defn- temp-overlay-file
  [payload]
  (let [path (str (java.nio.file.Files/createTempFile
                    "xia-admin-overlay"
                    ".edn"
                    (make-array java.nio.file.attribute.FileAttribute 0)))]
    (spit (io/file path) (pr-str payload))
    path))

(defn- admin-deps
  ([] (admin-deps nil))
  ([body]
   {:exception-response (fn [^Throwable throwable]
                          (let [data (ex-data throwable)]
                            {:status (or (:status data) 400)
                             :headers {"Content-Type" "application/json; charset=utf-8"}
                             :body (json/write-json-str {:error (or (:error data)
                                                                    (.getMessage throwable))
                                                         :details (dissoc data :status :error)})}))
    :instant->str       (fn [value] (some-> value str))
    :json-response      (fn [status body*]
                          {:status status
                           :headers {"Content-Type" "application/json; charset=utf-8"}
                           :body (json/write-json-str body*)})
    :read-body          (constantly body)
    :request-base-url   (constantly "http://localhost:3008")
    :truncate-text      (fn [value limit]
                          (let [text (str (or value ""))
                                limit* (long limit)]
                            (subs text 0 (min limit* (count text)))))}))

(deftest admin-config-includes-redacted-runtime-overlay-summary
  (instance-supervisor/configure! {:enabled? true
                                   :command "/opt/xia/bin/xia"})
  (runtime-overlay/activate!
    {:overlay/version 1
     :snapshot/id "overlay-debug-v1"
     :config-overrides {:browser/backend-default :remote
                        :browser/remote-enabled? true
                        :context/recent-history-message-limit 55
                        :web/search-backend "searxng"}
     :forced-keys #{:browser/backend-default}
     :tx-data [{:llm.provider/id :platform-openai
                :llm.provider/name "OpenAI (Platform)"
                :llm.provider/default? true}
               {:service/id :platform-search
                :service/name "Platform Search"}
               {:oauth.account/id :platform-oauth
                :oauth.account/name "Platform OAuth"}
               {:site-cred/id :platform-site
                :site-cred/name "Platform Site"}]})
  (try
    (db/set-config! :context/history-budget 777)
    (db/set-config! :web/search-brave-api-key "tenant-brave-key")
    (db/set-config! :browser/remote-base-url "http://browser-runtime.internal")
    (db/set-config! :browser/playwright-headless? "false")
    (db/set-config! :instance/management-enabled? "true")
    (db/upsert-provider! {:id :tenant-openai
                          :name "Tenant OpenAI"
                          :base-url "https://tenant-llm.example"
                          :model "tenant-model"})
    (db/register-service! {:id :tenant-search
                           :name "Tenant Search"
                           :base-url "https://tenant-search.example"
                           :auth-type :bearer
                           :auth-key "tenant-token"})
    (db/register-oauth-account! {:id :tenant-oauth
                                 :name "Tenant OAuth"})
    (db/register-site-cred! {:id :tenant-site
                             :name "Tenant Site"
                             :login-url "https://tenant.example/login"})
    (with-redefs [browser/browser-runtime-status
                  (constantly {:configured-default-backend :remote
                               :selected-auto-backend :remote
                               :backends [{:backend :playwright
                                           :status :disabled
                                           :available? false
                                           :ready? false
                                           :running? false}
                                          {:backend :remote
                                           :status :unconfigured
                                           :available? false
                                           :ready? false
                                           :running? false}]})]
      (let [response       (#'http-admin/handle-admin-config (admin-deps) {})
            body           (response-json response)
            db-schema      (get body "db_schema")
            overlay        (get body "runtime_overlay")
            web-search     (get body "web_search")
            browser-runtime (get body "browser_runtime")
            instance-mgmt  (get body "instance_management")
            conversation   (get body "conversation_context")
            backup         (get body "database_backup")
            provider-by-id (into {} (map (juxt #(get % "id") identity)) (get body "providers"))
            service-by-id  (into {} (map (juxt #(get % "id") identity)) (get body "services"))
            oauth-by-id    (into {} (map (juxt #(get % "id") identity)) (get body "oauth_accounts"))
            site-by-id     (into {} (map (juxt #(get % "id") identity)) (get body "sites"))]
      (is (= 200 (:status response)))
      (is (= db/current-schema-version
             (get db-schema "schema_version")))
      (is (= db/current-schema-version
             (get db-schema "supported_schema_version")))
      (is (= db/current-schema-version
             (get db-schema "released_schema_version")))
      (is (= [db/current-schema-version]
             (get db-schema "frozen_schema_versions")))
      (is (= (str "xia/schema/" db/current-schema-version ".edn")
             (get db-schema "schema_resource_path")))
      (is (= (str "xia/schema/" db/current-schema-version ".edn")
             (get db-schema "supported_schema_resource_path")))
      (is (string? (get db-schema "schema_applied_at")))
      (is (= []
             (get db-schema "available_migrations")))
      (is (= []
             (get db-schema "migration_history")))
      (is (= true (get overlay "active")))
      (is (= "overlay-debug-v1" (get overlay "snapshot_id")))
      (is (= 1 (get overlay "overlay_version")))
      (is (= 1 (get overlay "source_overlay_version")))
      (is (= "platform-openai" (get overlay "provider_default_id")))
      (is (= #{"browser/backend-default"
               "browser/remote-enabled?"
               "context/recent-history-message-limit"
               "web/search-backend"}
             (set (get overlay "config_override_keys"))))
      (is (= ["browser/backend-default"]
             (get overlay "forced_keys")))
      (is (= 1 (get-in overlay ["entity_counts" "providers"])))
      (is (= 1 (get-in overlay ["entity_counts" "services"])))
      (is (= 1 (get-in overlay ["entity_counts" "oauth_accounts"])))
      (is (= 1 (get-in overlay ["entity_counts" "site_creds"])))
      (is (= "runtime-overlay" (get-in web-search ["sources" "backend"])))
      (is (= "tenant-db" (get-in web-search ["sources" "brave_api_key"])))
      (is (= "default" (get-in web-search ["sources" "searxng_url"])))
      (is (= "" (get web-search "backend")))
      (is (= "tenant-brave-key" (get web-search "brave_api_key")))
      (is (= "" (get web-search "searxng_url")))
      (is (= "searxng-json" (get-in web-search ["config_resolution" "backend" "effective_value"])))
      (is (= "replace" (get-in web-search ["config_resolution" "backend" "overlay" "mode"])))
      (is (= "duckduckgo-html" (get-in web-search ["config_resolution" "backend" "default_value"])))
      (is (= "tenant-brave-key" (get-in web-search ["config_resolution" "brave_api_key" "tenant_value"])))
      (is (= "tenant-db" (get-in web-search ["config_resolution" "brave_api_key" "source"])))
      (is (= "default" (get-in web-search ["config_resolution" "searxng_url" "source"])))
      (is (= "runtime-overlay" (get-in conversation ["sources" "recent_history_message_limit"])))
      (is (= "tenant-db" (get-in conversation ["sources" "history_budget"])))
      (is (= "replace" (get-in conversation ["config_resolution" "recent_history_message_limit" "overlay" "mode"])))
      (is (= 55 (get-in conversation ["config_resolution" "recent_history_message_limit" "effective_value"])))
      (is (= 24 (get-in conversation ["config_resolution" "recent_history_message_limit" "default_value"])))
      (is (= "runtime-overlay" (get-in conversation ["config_resolution" "recent_history_message_limit" "source"])))
      (is (= 777 (get-in conversation ["config_resolution" "history_budget" "tenant_value"])))
      (is (= "tenant-db" (get-in conversation ["config_resolution" "history_budget" "source"])))
      (is (= "default" (get-in backup ["sources" "retain_count"])))
      (is (= 7 (get-in backup ["config_resolution" "retain_count" "default_value"])))
      (is (= "default" (get-in backup ["config_resolution" "retain_count" "source"])))
      (is (= "remote" (get browser-runtime "configured_default_backend")))
      (is (= "remote" (get browser-runtime "selected_auto_backend")))
      (is (= "runtime-overlay" (get-in browser-runtime ["sources" "configured_default_backend"])))
      (is (= "runtime-overlay" (get-in browser-runtime ["sources" "remote_enabled"])))
      (is (= "tenant-db" (get-in browser-runtime ["sources" "remote_base_url"])))
      (is (= "tenant-db" (get-in browser-runtime ["sources" "playwright_headless"])))
      (is (= "http://browser-runtime.internal" (get-in browser-runtime ["config_resolution" "remote" "base_url" "tenant_value"])))
      (is (= false (get-in browser-runtime ["config_resolution" "playwright" "headless" "tenant_value"])))
      (is (= "runtime-overlay" (get-in browser-runtime ["config_resolution" "remote" "enabled" "source"])))
      (is (= true (get instance-mgmt "configured")))
      (is (= true (get instance-mgmt "enabled")))
      (is (= true (get instance-mgmt "host_capability_enabled")))
      (is (= "/opt/xia/bin/xia" (get instance-mgmt "command")))
      (is (= "tenant-db" (get-in instance-mgmt ["sources" "enabled"])))
      (is (= true (get-in instance-mgmt ["config_resolution" "enabled" "tenant_value"])))
      (is (= "runtime-overlay" (get-in provider-by-id ["platform-openai" "runtime_source"])))
      (is (= "tenant-db" (get-in provider-by-id ["tenant-openai" "runtime_source"])))
      (is (= "runtime-overlay" (get-in service-by-id ["platform-search" "runtime_source"])))
      (is (= "tenant-db" (get-in service-by-id ["tenant-search" "runtime_source"])))
      (is (= "runtime-overlay" (get-in oauth-by-id ["platform-oauth" "runtime_source"])))
      (is (= "tenant-db" (get-in oauth-by-id ["tenant-oauth" "runtime_source"])))
      (is (= "runtime-overlay" (get-in site-by-id ["platform-site" "runtime_source"])))
      (is (= "tenant-db" (get-in site-by-id ["tenant-site" "runtime_source"])))))
    (finally
      (runtime-overlay/clear!)
      (instance-supervisor/configure! {:enabled? false}))))

(deftest admin-config-shows-web-search-default-resolution
  (let [response   (#'http-admin/handle-admin-config (admin-deps) {})
        body       (response-json response)
        web-search (get body "web_search")]
    (is (= 200 (:status response)))
    (is (= "default" (get-in web-search ["sources" "backend"])))
    (is (= "default" (get-in web-search ["config_resolution" "backend" "source"])))
    (is (= "duckduckgo-html" (get-in web-search ["config_resolution" "backend" "effective_value"])))
    (is (= "duckduckgo-html" (get-in web-search ["config_resolution" "backend" "default_value"])))
    (is (nil? (get-in web-search ["config_resolution" "backend" "overlay"])))))

(deftest admin-config-includes-memory-consolidation-summary
  (with-redefs [xia.hippocampus/consolidation-summary
                (constantly {:accepting true
                             :pending_background_task_count 2
                             :backlog {:pending_episode_count 4
                                       :failed_episode_count 1
                                       :last_failed_episode_at "2026-04-05T10:00:00Z"}
                             :stats {:started_at "2026-04-05T09:00:00Z"
                                     :attempted_episode_count 7
                                     :successful_episode_count 5
                                     :failed_attempt_count 2
                                     :invalid_extraction_count 1
                                     :exception_count 1
                                     :success_rate 0.7142857142857143
                                     :extracted_entity_count 11
                                     :extracted_relation_count 6
                                     :extracted_fact_count 17
                                     :avg_extracted_entities_per_success 2.2
                                     :avg_extracted_relations_per_success 1.2
                                     :avg_extracted_facts_per_success 3.4
                                     :last_attempt_at "2026-04-05T10:05:00Z"
                                     :last_success_at "2026-04-05T10:00:00Z"
                                     :last_failure_at "2026-04-05T10:05:00Z"
                                     :last_error "background consolidation failed"
                                     :last_error_kind "exception"}})]
    (let [response             (#'http-admin/handle-admin-config (admin-deps) {})
          body                 (response-json response)
          memory-consolidation (get body "memory_consolidation")]
      (is (= 200 (:status response)))
      (is (= 4 (get-in memory-consolidation ["backlog" "pending_episode_count"])))
      (is (= 1 (get-in memory-consolidation ["backlog" "failed_episode_count"])))
      (is (= 7 (get-in memory-consolidation ["stats" "attempted_episode_count"])))
      (is (= 5 (get-in memory-consolidation ["stats" "successful_episode_count"])))
      (is (= 0.7142857142857143
             (get-in memory-consolidation ["stats" "success_rate"])))
      (is (= "exception"
             (get-in memory-consolidation ["stats" "last_error_kind"])))
      (is (= "2026-04-05T09:00:00Z"
             (get-in memory-consolidation ["stats" "started_at"]))))))

(deftest admin-config-shows-redacted-messaging-config-resolution
  (runtime-overlay/activate!
    {:overlay/version 1
     :snapshot/id "overlay-messaging-v1"
     :config-overrides {:messaging/imessage-enabled? true}})
  (try
    (db/set-config! :messaging/slack-enabled? "true")
    (db/set-config! :secret/messaging-slack-bot-token "slack-secret-token")
    (db/set-config! :messaging/imessage-poll-interval-ms 9000)
    (let [response   (#'http-admin/handle-admin-config (admin-deps) {})
          body       (response-json response)
          messaging  (get body "messaging_channels")]
      (is (= 200 (:status response)))
      (is (= "tenant-db" (get-in messaging ["slack" "sources" "enabled"])))
      (is (= "tenant-db" (get-in messaging ["slack" "sources" "bot_token"])))
      (is (= true (get-in messaging ["slack" "config_resolution" "bot_token" "effective_value"])))
      (is (= false (get-in messaging ["slack" "config_resolution" "bot_token" "default_value"])))
      (is (= true (get-in messaging ["slack" "config_resolution" "bot_token" "tenant_value"])))
      (is (nil? (get-in messaging ["slack" "config_resolution" "bot_token" "overlay"])))
      (is (= "runtime-overlay" (get-in messaging ["imessage" "sources" "enabled"])))
      (is (= true (get-in messaging ["imessage" "config_resolution" "enabled" "effective_value"])))
      (is (= "replace" (get-in messaging ["imessage" "config_resolution" "enabled" "overlay" "mode"])))
      (is (= true (get-in messaging ["imessage" "config_resolution" "enabled" "overlay" "value"])))
      (is (= "tenant-db" (get-in messaging ["imessage" "sources" "poll_interval_ms"])))
      (is (= 9000 (get-in messaging ["imessage" "config_resolution" "poll_interval_ms" "tenant_value"]))))
    (finally
      (runtime-overlay/clear!))))

(deftest admin-config-shows-effective-tenant-winner-under-overlay-cap
  (runtime-overlay/activate!
    {:overlay/version 1
     :snapshot/id "overlay-cap-debug-v1"
     :config-overrides {:context/history-budget {:merge :cap
                                                 :value 6000}}})
  (try
    (db/set-config! :context/history-budget 5000)
    (let [response     (#'http-admin/handle-admin-config (admin-deps) {})
          body         (response-json response)
          conversation (get body "conversation_context")]
      (is (= 200 (:status response)))
      (is (= "tenant-db" (get-in conversation ["sources" "history_budget"])))
      (is (= "tenant-db" (get-in conversation ["config_resolution" "history_budget" "source"])))
      (is (= 5000 (get-in conversation ["config_resolution" "history_budget" "tenant_value"])))
      (is (= "cap" (get-in conversation ["config_resolution" "history_budget" "overlay" "mode"])))
      (is (= 6000 (get-in conversation ["config_resolution" "history_budget" "overlay" "value"])))
      (is (= 5000 (get-in conversation ["config_resolution" "history_budget" "effective_value"]))))
    (finally
      (runtime-overlay/clear!))))

(deftest admin-save-provider-rejects-overlay-managed-provider
  (runtime-overlay/activate!
    {:overlay/version 1
     :snapshot/id "overlay-locked-provider"
     :tx-data [{:llm.provider/id :platform-openai
                :llm.provider/name "OpenAI (Platform)"}]})
  (try
    (let [response (#'http-admin/handle-save-provider
                     (admin-deps {"id" "platform-openai"
                                  "name" "Mutated Provider"
                                  "base_url" "https://platform.example"
                                  "model" "gpt-5"})
                     {})
          body     (response-json response)]
      (is (= 409 (:status response)))
      (is (= "entity is managed by the active runtime overlay"
             (get body "error")))
      (is (= "provider"
             (get-in body ["details" "entity-kind"])))
      (is (= "platform-openai"
             (get-in body ["details" "entity-id"]))))
    (finally
      (runtime-overlay/clear!))))

(deftest admin-save-provider-persists-model-budget-hints
  (let [response (#'http-admin/handle-save-provider
                   (admin-deps {"id" "openai"
                                "name" "OpenAI"
                                "base_url" "https://api.example.com/v1"
                                "model" "gpt-test"
                                "context_window" "128000"
                                "context_window_source" "metadata"
                                "recommended_system_prompt_budget" "24000"
                                "recommended_history_budget" "72000"
                                "recommended_input_budget_cap" "96000"})
                   {})
        body     (response-json response)
        provider (db/get-provider :openai)]
    (is (= 200 (:status response)))
    (is (= 128000 (:llm.provider/context-window provider)))
    (is (= :metadata (:llm.provider/context-window-source provider)))
    (is (= 24000 (:llm.provider/recommended-system-prompt-budget provider)))
    (is (= 72000 (:llm.provider/recommended-history-budget provider)))
    (is (= 96000 (:llm.provider/recommended-input-budget-cap provider)))
    (is (= 128000 (get-in body ["provider" "context_window"])))
    (is (= "metadata" (get-in body ["provider" "context_window_source"])))
    (is (= 24000 (get-in body ["provider" "recommended_system_prompt_budget"])))
    (is (= 72000 (get-in body ["provider" "recommended_history_budget"])))
    (is (= 96000 (get-in body ["provider" "recommended_input_budget_cap"])))))

(deftest admin-runtime-overlay-reload-updates-current-file
  (let [overlay-file (temp-overlay-file
                       {:overlay/version 1
                        :snapshot/id "overlay-reload-v1"
                        :config-overrides {:browser/backend-default :playwright}})]
    (try
      (runtime-overlay/load-file! overlay-file)
      (spit (io/file overlay-file)
            (pr-str {:overlay/version 1
                     :snapshot/id "overlay-reload-v2"
                     :config-overrides {:browser/backend-default :remote}}))
      (let [response (#'http-admin/handle-reload-runtime-overlay (admin-deps) {})
            body     (response-json response)
            overlay  (get body "runtime_overlay")]
        (is (= 200 (:status response)))
        (is (= "reloaded" (get body "status")))
        (is (= "overlay-reload-v2" (get overlay "snapshot_id")))
        (is (= overlay-file (get overlay "source_path")))
        (is (= true (get overlay "reloadable")))
        (is (= 2 (get overlay "reload_count"))))
      (finally
        (runtime-overlay/clear!)))))

(deftest admin-runtime-overlay-reload-preserves-current-overlay-on-invalid-update
  (let [overlay-file (temp-overlay-file
                       {:overlay/version 1
                        :snapshot/id "overlay-stable-v1"
                        :config-overrides {:browser/backend-default :remote}})]
    (try
      (runtime-overlay/load-file! overlay-file)
      (spit (io/file overlay-file)
            (pr-str {:overlay/version 99
                     :snapshot/id "overlay-invalid"}))
      (let [response (#'http-admin/handle-reload-runtime-overlay (admin-deps) {})
            body     (response-json response)]
        (is (= 400 (:status response)))
        (is (= "Unsupported runtime overlay version." (get body "error")))
        (is (= "overlay-stable-v1" (runtime-overlay/snapshot-id)))
        (is (= "remote" (runtime-overlay/config-db-value :browser/backend-default))))
      (finally
        (runtime-overlay/clear!)))))
