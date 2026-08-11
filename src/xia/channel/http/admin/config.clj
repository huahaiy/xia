(ns xia.channel.http.admin.config
  "Admin configuration and runtime-summary HTTP handlers."
  (:require [clojure.string :as str]
            [xia.backup :as backup]
            [xia.browser :as browser]
            [xia.bridge :as bridge]
            [xia.channel.http.admin.common :as common]
            [xia.channel.http.admin.instances :as admin-instances]
            [xia.channel.http.admin.oauth :as admin-oauth]
            [xia.channel.http.admin.plugins :as admin-plugins]
            [xia.channel.http.admin.providers :as admin-providers]
            [xia.channel.http.admin.schedules :as admin-schedules]
            [xia.channel.http.admin.services :as admin-services]
            [xia.channel.http.admin.sites :as admin-sites]
            [xia.channel.http.admin.skills :as admin-skills]
            [xia.channel.messaging :as messaging]
            [xia.context :as context]
            [xia.db :as db]
            [xia.db-schema :as db-schema]
            [xia.identity :as identity]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.llm :as llm]
            [xia.llm-log :as llm-log]
            [xia.llm-provider-template :as llm-provider-template]
            [xia.local-ocr :as local-ocr]
            [xia.memory :as memory]
            [xia.paths :as paths]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.schedule :as schedule]
            [xia.summarizer :as summarizer]
            [xia.web :as web]))

(def ^:private ms-per-day (* 24 60 60 1000))

(defn- db-migration->admin-body
  [{:keys [from-version to-version description applied-at]}]
  (cond-> {:from_version from-version
           :to_version to-version
           :description description}
    applied-at
    (assoc :applied_at applied-at)))

(defn- days->ms
  [days]
  (when-some [days* (some-> days long)]
    (* (long days*) (long ms-per-day))))

(defn- sort-by-name
  [entries]
  (->> entries
       (sort-by (fn [entry]
                  (str/lower-case (or (:name entry) (:id entry) ""))))
       vec))

(defn- admin-config-value
  [value]
  (cond
    (keyword? value) (name value)
    :else value))

(defn- tenant-admin-config-value
  [config-key]
  (or (some-> (db/tenant-config-value config-key) admin-config-value)
      ""))

(defn- days-value
  [value]
  (when (some? value)
    (long (/ (long value) (long ms-per-day)))))

(defn- config-resolution->admin-body
  ([resolution]
   (config-resolution->admin-body resolution admin-config-value))
  ([resolution transform]
   (let [transform* (or transform identity)]
     (cond-> {:source (some-> (:source resolution) name)
              :effective_value (transform* (:value resolution))
              :default_value (transform* (:default-value resolution))}
       (:tenant-present? resolution)
       (assoc :tenant_value (transform* (:tenant-value resolution)))

       (:overlay-present? resolution)
       (assoc :overlay {:mode (some-> (:overlay-mode resolution) name)
                        :value (transform* (:overlay-value resolution))})))))

(defn- secret-resolution->admin-body
  [resolution]
  (config-resolution->admin-body resolution #(boolean (common/nonblank-str %))))

(defn- provider->admin-body
  [provider]
  (admin-providers/provider->admin-body provider))

(defn- llm-provider-template->admin-body
  [template]
  (admin-providers/template->admin-body template))

(defn- memory-retention->admin-body
  [deps]
  (let [{:keys [full-resolution-ms decay-half-life-ms retained-decayed-count]}
        (memory/episode-retention-settings)
        resolutions (memory/episode-retention-config-resolutions)]
    {:full_resolution_days (long (/ (long full-resolution-ms) (long ms-per-day)))
     :decay_half_life_days (long (/ (long decay-half-life-ms) (long ms-per-day)))
     :retained_count       (long retained-decayed-count)
     :sources              {:full_resolution_days (some-> (get-in resolutions [:full-resolution-ms :source]) name)
                            :decay_half_life_days (some-> (get-in resolutions [:decay-half-life-ms :source]) name)
                            :retained_count (some-> (get-in resolutions [:retained-decayed-count :source]) name)}
     :config_resolution    {:full_resolution_days (config-resolution->admin-body
                                                   (:full-resolution-ms resolutions)
                                                   days-value)
                            :decay_half_life_days (config-resolution->admin-body
                                                   (:decay-half-life-ms resolutions)
                                                   days-value)
                            :retained_count (config-resolution->admin-body
                                             (:retained-decayed-count resolutions))}}))

(defn- knowledge-decay->admin-body
  []
  (let [{:keys [grace-period-ms half-life-ms min-confidence maintenance-step-ms archive-after-bottom-ms]}
        (bridge/knowledge-decay-settings)
        resolutions (bridge/knowledge-decay-config-resolutions)]
    {:grace_period_days         (long (/ (long grace-period-ms) (long ms-per-day)))
     :half_life_days            (long (/ (long half-life-ms) (long ms-per-day)))
     :min_confidence            min-confidence
     :maintenance_interval_days (long (/ (long maintenance-step-ms) (long ms-per-day)))
     :archive_after_bottom_days (long (/ (long archive-after-bottom-ms) (long ms-per-day)))
     :sources                   {:grace_period_days (some-> (get-in resolutions [:grace-period-ms :source]) name)
                                 :half_life_days (some-> (get-in resolutions [:half-life-ms :source]) name)
                                 :min_confidence (some-> (get-in resolutions [:min-confidence :source]) name)
                                 :maintenance_interval_days (some-> (get-in resolutions [:maintenance-step-ms :source]) name)
                                 :archive_after_bottom_days (some-> (get-in resolutions [:archive-after-bottom-ms :source]) name)}
     :config_resolution         {:grace_period_days (config-resolution->admin-body
                                                     (:grace-period-ms resolutions)
                                                     days-value)
                                 :half_life_days (config-resolution->admin-body
                                                  (:half-life-ms resolutions)
                                                  days-value)
                                 :min_confidence (config-resolution->admin-body
                                                  (:min-confidence resolutions))
                                 :maintenance_interval_days (config-resolution->admin-body
                                                             (:maintenance-step-ms resolutions)
                                                             days-value)
                                 :archive_after_bottom_days (config-resolution->admin-body
                                                             (:archive-after-bottom-ms resolutions)
                                                             days-value)}}))

(defn- memory-consolidation->admin-body
  []
  (bridge/memory-consolidation-summary))

(defn- conversation-context->admin-body
  []
  (let [resolutions (context/config-resolutions)]
    {:recent_history_message_limit (context/recent-history-message-limit-config)
     :history_budget               (context/history-budget-config)
     :sources                      {:recent_history_message_limit (some-> (get-in resolutions [:recent-history-message-limit :source]) name)
                                    :history_budget (some-> (get-in resolutions [:history-budget :source]) name)}
     :config_resolution            {:recent_history_message_limit (config-resolution->admin-body
                                                                   (:recent-history-message-limit resolutions))
                                    :history_budget (config-resolution->admin-body
                                                     (:history-budget resolutions))}}))

(defn- llm-logging->admin-body
  []
  (let [{:keys [full-payloads? retention-days]} (llm/diagnostic-log-settings)
        resolutions (llm/diagnostic-log-config-resolutions)]
    {:full_payloads_enabled (boolean full-payloads?)
     :retention_days       (long retention-days)
     :sources              {:full_payloads_enabled (some-> (get-in resolutions [:full-payloads? :source]) name)
                            :retention_days (some-> (get-in resolutions [:retention-days :source]) name)}
     :config_resolution    {:full_payloads_enabled (config-resolution->admin-body
                                                    (:full-payloads? resolutions))
                            :retention_days (config-resolution->admin-body
                                             (:retention-days resolutions))}}))

(defn- local-doc-summarization->admin-body
  []
  (let [resolutions (summarizer/config-resolutions)]
    {:model_summaries_enabled   (boolean (summarizer/enabled?))
     :model_summary_backend     (some-> (summarizer/summary-backend) name)
     :model_summary_provider_id (some-> (summarizer/external-provider-id) name)
     :chunk_summary_max_tokens  (summarizer/chunk-summary-max-tokens)
     :doc_summary_max_tokens    (summarizer/document-summary-max-tokens)
     :sources                   {:model_summaries_enabled (some-> (get-in resolutions [:enabled :source]) name)
                                 :model_summary_backend (some-> (get-in resolutions [:backend :source]) name)
                                 :model_summary_provider_id (some-> (get-in resolutions [:provider-id :source]) name)
                                 :chunk_summary_max_tokens (some-> (get-in resolutions [:chunk-summary-max-tokens :source]) name)
                                 :doc_summary_max_tokens (some-> (get-in resolutions [:document-summary-max-tokens :source]) name)}
     :config_resolution         {:model_summaries_enabled (config-resolution->admin-body
                                                           (:enabled resolutions))
                                 :model_summary_backend (config-resolution->admin-body
                                                         (:backend resolutions))
                                 :model_summary_provider_id (config-resolution->admin-body
                                                             (:provider-id resolutions))
                                 :chunk_summary_max_tokens (config-resolution->admin-body
                                                            (:chunk-summary-max-tokens resolutions))
                                 :doc_summary_max_tokens (config-resolution->admin-body
                                                          (:document-summary-max-tokens resolutions))}}))

(defn- local-doc-ocr->admin-body
  []
  (let [resolutions (local-ocr/config-resolutions)]
    (assoc (local-ocr/admin-body)
           :sources {:enabled (some-> (get-in resolutions [:enabled :source]) name)
                     :model_backend (some-> (get-in resolutions [:backend :source]) name)
                     :external_provider_id (some-> (get-in resolutions [:provider-id :source]) name)
                     :timeout_ms (some-> (get-in resolutions [:timeout-ms :source]) name)
                     :max_tokens (some-> (get-in resolutions [:max-tokens :source]) name)}
           :config_resolution {:enabled (config-resolution->admin-body
                                         (:enabled resolutions))
                               :model_backend (config-resolution->admin-body
                                               (:backend resolutions))
                               :external_provider_id (config-resolution->admin-body
                                                      (:provider-id resolutions))
                               :timeout_ms (config-resolution->admin-body
                                            (:timeout-ms resolutions))
                               :max_tokens (config-resolution->admin-body
                                            (:max-tokens resolutions))})))

(defn- database-backup->admin-body
  [deps]
  (let [settings (backup/admin-body)
        resolutions (backup/config-resolutions)]
    {:enabled           (boolean (:enabled settings))
     :directory         (:directory settings)
     :interval_hours    (:interval_hours settings)
     :retain_count      (:retain_count settings)
     :running           (boolean (:running settings))
     :started_at        (common/instant->str deps (:started_at settings))
     :last_attempt_at   (common/instant->str deps (:last_attempt_at settings))
     :last_success_at   (common/instant->str deps (:last_success_at settings))
     :last_archive_path (:last_archive_path settings)
     :last_error        (:last_error settings)
     :next_due_at       (common/instant->str deps (:next_due_at settings))
     :sources           {:enabled (some-> (get-in resolutions [:enabled :source]) name)
                         :directory (some-> (get-in resolutions [:directory :source]) name)
                         :interval_hours (some-> (get-in resolutions [:interval-hours :source]) name)
                         :retain_count (some-> (get-in resolutions [:retain-count :source]) name)}
     :config_resolution {:enabled (config-resolution->admin-body
                                   (:enabled resolutions))
                         :directory (config-resolution->admin-body
                                     (:directory resolutions))
                         :interval_hours (config-resolution->admin-body
                                          (:interval-hours resolutions))
                         :retain_count (config-resolution->admin-body
                                        (:retain-count resolutions))}}))

(defn- web-search->admin-body
  []
  (let [resolutions (web/search-config-resolutions)]
    {:backend       (tenant-admin-config-value :web/search-backend)
     :brave_api_key (tenant-admin-config-value :web/search-brave-api-key)
     :searxng_url   (tenant-admin-config-value :web/search-searxng-url)
     :sources       {:backend (some-> (get-in resolutions [:backend :source]) name)
                     :brave_api_key (some-> (get-in resolutions [:brave-api-key :source]) name)
                     :searxng_url (some-> (get-in resolutions [:searxng-url :source]) name)}
     :config_resolution
     {:backend (config-resolution->admin-body (:backend resolutions) admin-config-value)
      :brave_api_key (config-resolution->admin-body (:brave-api-key resolutions))
      :searxng_url (config-resolution->admin-body (:searxng-url resolutions))}}))

(defn- browser-runtime->admin-body
  []
  (let [runtime-status (browser/browser-runtime-status)
        resolutions    (browser/config-resolutions)]
    {:configured_default_backend (some-> (:configured-default-backend runtime-status) name)
     :selected_auto_backend      (some-> (:selected-auto-backend runtime-status) name)
     :backends                   (mapv (fn [backend-status]
                                         (-> backend-status
                                             (update :backend #(some-> % name))
                                             (update :status #(some-> % name))))
                                       (:backends runtime-status))
     :sources                    {:configured_default_backend (some-> (get-in resolutions [:backend-default :source]) name)
                                  :remote_enabled (some-> (get-in resolutions [:remote :enabled :source]) name)
                                  :remote_base_url (some-> (get-in resolutions [:remote :base-url :source]) name)
                                  :remote_token_file (some-> (get-in resolutions [:remote :token-file :source]) name)
                                  :remote_timeout_ms (some-> (get-in resolutions [:remote :timeout-ms :source]) name)
                                  :playwright_enabled (some-> (get-in resolutions [:playwright :enabled :source]) name)
                                  :playwright_headless (some-> (get-in resolutions [:playwright :headless :source]) name)
                                  :playwright_auto_install (some-> (get-in resolutions [:playwright :auto-install :source]) name)
                                  :playwright_browsers_path (some-> (get-in resolutions [:playwright :browsers-path :source]) name)
                                  :playwright_channel (some-> (get-in resolutions [:playwright :channel :source]) name)}
     :config_resolution          {:configured_default_backend (config-resolution->admin-body
                                                               (:backend-default resolutions)
                                                               admin-config-value)
                                  :remote {:enabled (config-resolution->admin-body (get-in resolutions [:remote :enabled]))
                                           :base_url (config-resolution->admin-body (get-in resolutions [:remote :base-url]))
                                           :token_file (secret-resolution->admin-body (get-in resolutions [:remote :token-file]))
                                           :timeout_ms (config-resolution->admin-body (get-in resolutions [:remote :timeout-ms]))}
                                  :playwright {:enabled (config-resolution->admin-body (get-in resolutions [:playwright :enabled]))
                                               :headless (config-resolution->admin-body (get-in resolutions [:playwright :headless]))
                                               :auto_install (config-resolution->admin-body (get-in resolutions [:playwright :auto-install]))
                                               :browsers_path (config-resolution->admin-body (get-in resolutions [:playwright :browsers-path]))
                                               :channel (config-resolution->admin-body (get-in resolutions [:playwright :channel]))}}}))

(defn- instance-management->admin-body
  []
  (let [state        (instance-supervisor/admin-body)
        resolutions  (instance-supervisor/config-resolutions)]
    {:configured              (:configured state)
     :enabled                 (:enabled state)
     :host_capability_enabled (:host_capability_enabled state)
     :command                 (:command state)
     :parent_instance_id      (:parent_instance_id state)
     :sources                 {:enabled (some-> (get-in resolutions [:enabled :source]) name)}
     :config_resolution       {:enabled (config-resolution->admin-body (:enabled resolutions))}}))

(defn- save-config-override!
  [config-key value]
  (if (some? value)
    (db/set-config! config-key value)
    (db/delete-config! config-key)))

(defn- managed-instance->admin-body
  [deps instance]
  (admin-instances/instance->admin-body deps instance))

(defn- schedule->admin-body
  [deps sched]
  (admin-schedules/schedule->admin-body deps sched))

(defn- tool->admin-body
  [tool]
  {:id          (some-> (:tool/id tool) name)
   :name        (:tool/name tool)
   :description (:tool/description tool)
   :approval    (some-> (:tool/approval tool) name)
   :enabled     (boolean (:tool/enabled? tool))})

(defn- parse-optional-bounded-double
  [value field-name]
  (let [text (common/nonblank-str value)]
    (when text
      (try
        (let [parsed (Double/parseDouble text)]
          (when-not (<= 0.0 parsed 1.0)
            (throw (ex-info (str "'" field-name "' must be between 0.0 and 1.0")
                            {:field field-name
                             :value value})))
          parsed)
        (catch NumberFormatException _
          (throw (ex-info (str "'" field-name "' must be between 0.0 and 1.0")
                          {:field field-name
                           :value value})))))))

(defn- parse-summary-backend
  [value field-name]
  (let [backend (some-> value common/nonblank-str keyword)]
    (when backend
      (when-not (contains? #{:local :external} backend)
        (throw (ex-info (str "'" field-name "' must be one of: local, external")
                        {:field field-name
                         :value value})))
      backend)))

(defn- parse-optional-provider-id
  [value field-name]
  (when-let [provider-id-str (common/nonblank-str value)]
    (let [provider-id (keyword provider-id-str)]
      (when-not (db/get-provider provider-id)
        (throw (ex-info (str "'" field-name "' must reference an existing provider")
                        {:field field-name
                         :value value})))
      provider-id)))

(defn handle-reload-runtime-overlay
  [deps req]
  (try
    (let [data         (or (common/read-body deps req) {})
          overlay-path (or (common/nonblank-str (get data "overlay_path"))
                           (common/nonblank-str (get data :overlay_path)))]
      (if overlay-path
        (runtime-overlay/reload! overlay-path)
        (runtime-overlay/reload!))
      (common/json-response deps 200
                            {:status "reloaded"
                             :runtime_overlay (runtime-overlay/admin-summary)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-admin-config
  [deps _req]
  (let [providers       (db/list-providers)
        setup-required? (or (empty? providers)
                            (nil? (db/get-default-provider)))
        storage-layout  (paths/storage-layout (db/current-db-path))]
    (common/json-response
     deps
     200
     {:setup_required setup-required?
      :identity (let [soul (identity/get-soul)]
                  {:name        (:name soul "Xia")
                   :role        (:role soul "")
                   :description (:description soul "")
                   :personality (:personality soul "")
                   :guidelines  (:guidelines soul "")})
      :instance {:id (or (db/current-instance-id)
                         paths/default-instance-id)
                 :parent_instance_id (instance-supervisor/parent-instance-id)}
      :db_schema {:schema_version (db/schema-version)
                  :supported_schema_version db/current-schema-version
                  :released_schema_version (db-schema/released-schema-version)
                  :frozen_schema_versions (db-schema/frozen-schema-versions)
                  :schema_resource_path (db/schema-resource-path)
                  :supported_schema_resource_path (db-schema/schema-resource-path db/current-schema-version)
                  :schema_applied_at (db/schema-applied-at)
                  :migration_history (mapv db-migration->admin-body
                                           (or (db/schema-migration-history) []))
                  :available_migrations (mapv db-migration->admin-body
                                              (db-schema/migration-registry-summary))}
      :capabilities (instance-supervisor/capabilities)
      :instance_management (instance-management->admin-body)
      :browser_runtime (browser-runtime->admin-body)
      :runtime_overlay (runtime-overlay/admin-summary)
      :managed_instances (mapv #(managed-instance->admin-body deps %)
                               (instance-supervisor/list-managed-instances))
      :storage {:db_path        (:db-path storage-layout)
                :support_dir    (:support-dir storage-layout)
                :workspace_root (:workspace-root storage-layout)
                :embed_dir      (:embed-dir storage-layout)
                :llm_dir        (:llm-dir storage-layout)
                :ocr_dir        (:ocr-dir storage-layout)}
      :providers (->> providers
                      (into [] (map provider->admin-body))
                      sort-by-name)
      :llm_provider_templates (->> (llm-provider-template/list-templates)
                                   (into [] (map llm-provider-template->admin-body))
                                   sort-by-name)
      :web_search (web-search->admin-body)
      :conversation_context (conversation-context->admin-body)
      :llm_logging (llm-logging->admin-body)
      :memory_retention (memory-retention->admin-body deps)
      :knowledge_decay (knowledge-decay->admin-body)
      :memory_consolidation (memory-consolidation->admin-body)
      :local_doc_summarization (local-doc-summarization->admin-body)
      :local_doc_ocr (local-doc-ocr->admin-body)
      :database_backup (database-backup->admin-body deps)
      :messaging_channels (messaging/admin-body)
      :llm_workloads (into [] (map (fn [{:keys [id label description async?]}]
                                     {:id          (name id)
                                      :label       label
                                      :description description
                                      :async       (boolean async?)}))
                           (llm/workload-routes))
      :oauth_provider_templates (->> (admin-oauth/list-templates)
                                     (into [] (map admin-oauth/oauth-template->admin-body))
                                     sort-by-name)
      :oauth_accounts (->> (db/list-oauth-accounts)
                           (into [] (map #(admin-oauth/oauth-account->admin-body deps %)))
                           sort-by-name)
      :services  (->> (db/list-services)
                      (into [] (map admin-services/service->admin-body))
                      sort-by-name)
      :sites     (->> (db/list-site-creds)
                      (into [] (map admin-sites/site->admin-body))
                      sort-by-name)
      :schedules (->> (schedule/list-schedules)
                      (into [] (map #(schedule->admin-body deps %)))
                      sort-by-name)
      :tools     (->> (db/list-tools)
                      (into [] (map tool->admin-body))
                      sort-by-name)
      :plugins   (->> (db/list-plugins)
                      (into [] (map #(admin-plugins/plugin->admin-body deps %)))
                      sort-by-name)
      :skills    (->> (db/list-skills)
                      (into [] (map #(admin-skills/skill->body deps %)))
                      sort-by-name)})))

(defn handle-save-memory-retention
  [deps req]
  (try
    (let [data                 (or (common/read-body deps req) {})
          full-resolution-days (when (contains? data "full_resolution_days")
                                 (common/parse-optional-positive-long (get data "full_resolution_days")
                                                                      "full_resolution_days"))
          decay-half-life-days (when (contains? data "decay_half_life_days")
                                 (common/parse-optional-positive-long (get data "decay_half_life_days")
                                                                      "decay_half_life_days"))
          retained-count       (when (contains? data "retained_count")
                                 (common/parse-optional-positive-long (get data "retained_count")
                                                                      "retained_count"))]
      (when (contains? data "full_resolution_days")
        (save-config-override! :memory/episode-full-resolution-ms
                               (days->ms full-resolution-days)))
      (when (contains? data "decay_half_life_days")
        (save-config-override! :memory/episode-decay-half-life-ms
                               (days->ms decay-half-life-days)))
      (when (contains? data "retained_count")
        (save-config-override! :memory/episode-retained-decayed-count
                               retained-count))
      (common/json-response deps 200 {:memory_retention (memory-retention->admin-body deps)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-save-llm-logging
  [deps req]
  (try
    (let [data          (or (common/read-body deps req) {})
          full-present? (contains? data "full_payloads_enabled")
          full-value    (get data "full_payloads_enabled")
          retention-present? (contains? data "retention_days")
          retention-raw (get data "retention_days")
          retention-days (when retention-present?
                           (let [text (common/nonblank-str retention-raw)]
                             (when text
                               (or (llm-log/parse-retention-days text)
                                   (throw (ex-info
                                           (str "'retention_days' must be an integer between 1 and "
                                                llm-log/max-retention-days)
                                           {:field "retention_days"
                                            :value retention-raw}))))))]
      (when (and full-present?
                 (some? full-value)
                 (not (instance? Boolean full-value)))
        (throw (ex-info "'full_payloads_enabled' must be a boolean"
                        {:field "full_payloads_enabled"
                         :value full-value})))
      (when full-present?
        (save-config-override! llm-log/full-payloads-config-key full-value))
      (when retention-present?
        (save-config-override! llm-log/retention-days-config-key retention-days))
      (db/prune-llm-log!)
      (common/json-response deps 200 {:llm_logging (llm-logging->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-save-web-search
  [deps req]
  (try
    (let [data (or (common/read-body deps req) {})]
      (save-config-override! :web/search-backend
                             (common/nonblank-str (get data "backend")))
      (save-config-override! :web/search-brave-api-key
                             (common/nonblank-str (get data "brave_api_key")))
      (save-config-override! :web/search-searxng-url
                             (common/nonblank-str (get data "searxng_url")))
      (common/json-response deps 200
                            {:web_search (web-search->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-save-identity
  [deps req]
  (try
    (let [data (or (common/read-body deps req) {})]
      (doseq [[json-key soul-key] [["name" :name]
                                   ["role" :role]
                                   ["description" :description]
                                   ["personality" :personality]
                                   ["guidelines" :guidelines]]]
        (when (contains? data json-key)
          (identity/set-soul! soul-key (str (get data json-key "")))))
      (let [soul (identity/get-soul)]
        (when (contains? data "controller_enabled")
          (instance-supervisor/set-instance-management-enabled!
           (true? (get data "controller_enabled"))))
        (common/json-response
         deps
         200
         {:identity {:name        (:name soul "Xia")
                     :role        (:role soul "")
                     :description (:description soul "")
                     :personality (:personality soul "")
                     :guidelines  (:guidelines soul "")}
          :capabilities (instance-supervisor/capabilities)})))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-save-conversation-context
  [deps req]
  (try
    (let [data                         (or (common/read-body deps req) {})
          recent-history-message-limit (when (contains? data "recent_history_message_limit")
                                         (common/parse-optional-positive-long
                                          (get data "recent_history_message_limit")
                                          "recent_history_message_limit"))
          history-budget               (when (contains? data "history_budget")
                                         (common/parse-optional-positive-long
                                          (get data "history_budget")
                                          "history_budget"))]
      (when (contains? data "recent_history_message_limit")
        (save-config-override! :context/recent-history-message-limit
                               recent-history-message-limit))
      (when (contains? data "history_budget")
        (save-config-override! :context/history-budget
                               history-budget))
      (common/json-response deps 200 {:conversation_context (conversation-context->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-save-knowledge-decay
  [deps req]
  (try
    (let [data                      (or (common/read-body deps req) {})
          grace-period-days         (when (contains? data "grace_period_days")
                                      (common/parse-optional-positive-long (get data "grace_period_days")
                                                                           "grace_period_days"))
          half-life-days            (when (contains? data "half_life_days")
                                      (common/parse-optional-positive-long (get data "half_life_days")
                                                                           "half_life_days"))
          min-confidence            (when (contains? data "min_confidence")
                                      (parse-optional-bounded-double (get data "min_confidence")
                                                                     "min_confidence"))
          maintenance-interval-days (when (contains? data "maintenance_interval_days")
                                      (common/parse-optional-positive-long
                                       (get data "maintenance_interval_days")
                                       "maintenance_interval_days"))
          archive-after-bottom-days (when (contains? data "archive_after_bottom_days")
                                      (common/parse-optional-positive-long
                                       (get data "archive_after_bottom_days")
                                       "archive_after_bottom_days"))]
      (when (contains? data "grace_period_days")
        (save-config-override! :memory/knowledge-decay-grace-period-ms
                               (days->ms grace-period-days)))
      (when (contains? data "half_life_days")
        (save-config-override! :memory/knowledge-decay-half-life-ms
                               (days->ms half-life-days)))
      (when (contains? data "min_confidence")
        (save-config-override! :memory/knowledge-decay-min-confidence
                               min-confidence))
      (when (contains? data "maintenance_interval_days")
        (save-config-override! :memory/knowledge-decay-maintenance-step-ms
                               (days->ms maintenance-interval-days)))
      (when (contains? data "archive_after_bottom_days")
        (save-config-override! :memory/knowledge-decay-archive-after-bottom-ms
                               (days->ms archive-after-bottom-days)))
      (common/json-response deps 200 {:knowledge_decay (knowledge-decay->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-save-local-doc-summarization
  [deps req]
  (try
    (let [data                     (or (common/read-body deps req) {})
          enabled?                 (when (contains? data "model_summaries_enabled")
                                     (true? (get data "model_summaries_enabled")))
          backend                  (when (contains? data "model_summary_backend")
                                     (parse-summary-backend (get data "model_summary_backend")
                                                            "model_summary_backend"))
          provider-id              (when (contains? data "model_summary_provider_id")
                                     (parse-optional-provider-id (get data "model_summary_provider_id")
                                                                 "model_summary_provider_id"))
          chunk-summary-max-tokens (when (contains? data "chunk_summary_max_tokens")
                                     (common/parse-optional-positive-long (get data "chunk_summary_max_tokens")
                                                                          "chunk_summary_max_tokens"))
          doc-summary-max-tokens   (when (contains? data "doc_summary_max_tokens")
                                     (common/parse-optional-positive-long (get data "doc_summary_max_tokens")
                                                                          "doc_summary_max_tokens"))
          effective-provider-id    (when (= backend :external) provider-id)]
      (when (contains? data "model_summaries_enabled")
        (save-config-override! :local-doc/model-summaries-enabled? enabled?))
      (when (contains? data "model_summary_backend")
        (save-config-override! :local-doc/model-summary-backend
                               (some-> backend name)))
      (when (contains? data "model_summary_provider_id")
        (save-config-override! :local-doc/model-summary-provider-id
                               (some-> effective-provider-id name)))
      (when (contains? data "chunk_summary_max_tokens")
        (save-config-override! :local-doc/chunk-summary-max-tokens
                               chunk-summary-max-tokens))
      (when (contains? data "doc_summary_max_tokens")
        (save-config-override! :local-doc/doc-summary-max-tokens
                               doc-summary-max-tokens))
      (common/json-response deps 200 {:local_doc_summarization (local-doc-summarization->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-save-local-doc-ocr
  [deps req]
  (try
    (let [data          (or (common/read-body deps req) {})
          enabled?      (when (contains? data "enabled")
                          (true? (get data "enabled")))
          model-backend (when (contains? data "model_backend")
                          (parse-summary-backend (get data "model_backend")
                                                 "model_backend"))
          provider-id   (when (contains? data "external_provider_id")
                          (parse-optional-provider-id (get data "external_provider_id")
                                                      "external_provider_id"))
          timeout-ms    (when (contains? data "timeout_ms")
                          (common/parse-optional-positive-long (get data "timeout_ms")
                                                               "timeout_ms"))
          max-tokens    (when (contains? data "max_tokens")
                          (common/parse-optional-positive-long (get data "max_tokens")
                                                               "max_tokens"))
          _             (when (and provider-id
                                   (not (llm/vision-capable? provider-id)))
                          (throw (ex-info "'external_provider_id' must reference a vision-capable provider"
                                          {:field "external_provider_id"
                                           :value (name provider-id)})))]
      (when (contains? data "enabled")
        (save-config-override! :local-doc/ocr-enabled? enabled?))
      (when (contains? data "model_backend")
        (save-config-override! :local-doc/ocr-backend
                               (some-> model-backend name)))
      (when (contains? data "external_provider_id")
        (save-config-override! :local-doc/ocr-provider-id
                               (some-> provider-id name)))
      (when (contains? data "timeout_ms")
        (save-config-override! :local-doc/ocr-timeout-ms timeout-ms))
      (when (contains? data "max_tokens")
        (save-config-override! :local-doc/ocr-max-tokens max-tokens))
      (common/json-response deps 200 {:local_doc_ocr (local-doc-ocr->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-save-database-backup
  [deps req]
  (try
    (let [data           (or (common/read-body deps req) {})
          enabled?       (when (contains? data "enabled")
                           (true? (get data "enabled")))
          directory      (when (contains? data "directory")
                           (common/nonblank-str (get data "directory")))
          interval-hours (when (contains? data "interval_hours")
                           (common/parse-optional-positive-long (get data "interval_hours")
                                                                "interval_hours"))
          retain-count   (when (contains? data "retain_count")
                           (common/parse-optional-positive-long (get data "retain_count")
                                                                "retain_count"))]
      (when (contains? data "enabled")
        (save-config-override! :backup/enabled? enabled?))
      (when (contains? data "directory")
        (save-config-override! :backup/directory directory))
      (when (contains? data "interval_hours")
        (save-config-override! :backup/interval-hours interval-hours))
      (when (contains? data "retain_count")
        (save-config-override! :backup/retain-count retain-count))
      (common/json-response deps 200 {:database_backup (database-backup->admin-body deps)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-save-messaging
  [deps req]
  (try
    (let [data  (or (common/read-body deps req) {})
          saved (messaging/save-admin-config!
                 {:slack (when (contains? data "slack")
                           {:enabled (get-in data ["slack" "enabled"])
                            :bot-token (get-in data ["slack" "bot_token"])
                            :signing-secret (get-in data ["slack" "signing_secret"])})
                  :telegram (when (contains? data "telegram")
                              {:enabled (get-in data ["telegram" "enabled"])
                               :bot-token (get-in data ["telegram" "bot_token"])
                               :webhook-secret (get-in data ["telegram" "webhook_secret"])})
                  :imessage (when (contains? data "imessage")
                              {:enabled (get-in data ["imessage" "enabled"])
                               :poll-interval-ms (get-in data ["imessage" "poll_interval_ms"])})})]
      (common/json-response deps 200 {:messaging_channels saved}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))
    (catch Exception e
      (common/json-response deps 500 {:error (or (.getMessage e)
                                                 "failed to save messaging settings")}))))
