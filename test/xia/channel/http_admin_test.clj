(ns xia.channel.http-admin-test
  (:require [charred.api :as json]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.channel.http.admin :as http-admin]
            [xia.db :as db]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(defn- response-json
  [response]
  (json/read-json (:body response)))

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
    (let [response       (#'http-admin/handle-admin-config (admin-deps) {})
          body           (response-json response)
          overlay        (get body "runtime_overlay")
          web-search     (get body "web_search")
          conversation   (get body "conversation_context")
          backup         (get body "database_backup")
          provider-by-id (into {} (map (juxt #(get % "id") identity)) (get body "providers"))
          service-by-id  (into {} (map (juxt #(get % "id") identity)) (get body "services"))
          oauth-by-id    (into {} (map (juxt #(get % "id") identity)) (get body "oauth_accounts"))
          site-by-id     (into {} (map (juxt #(get % "id") identity)) (get body "sites"))]
      (is (= 200 (:status response)))
      (is (= true (get overlay "active")))
      (is (= "overlay-debug-v1" (get overlay "snapshot_id")))
      (is (= 1 (get overlay "overlay_version")))
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
      (is (= "runtime-overlay" (get-in provider-by-id ["platform-openai" "runtime_source"])))
      (is (= "tenant-db" (get-in provider-by-id ["tenant-openai" "runtime_source"])))
      (is (= "runtime-overlay" (get-in service-by-id ["platform-search" "runtime_source"])))
      (is (= "tenant-db" (get-in service-by-id ["tenant-search" "runtime_source"])))
      (is (= "runtime-overlay" (get-in oauth-by-id ["platform-oauth" "runtime_source"])))
      (is (= "tenant-db" (get-in oauth-by-id ["tenant-oauth" "runtime_source"])))
      (is (= "runtime-overlay" (get-in site-by-id ["platform-site" "runtime_source"])))
      (is (= "tenant-db" (get-in site-by-id ["tenant-site" "runtime_source"]))))
    (finally
      (runtime-overlay/clear!))))

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
