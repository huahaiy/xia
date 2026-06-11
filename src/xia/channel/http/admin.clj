(ns xia.channel.http.admin
  "Admin/config/provider/oauth/service/schedule/skill HTTP handlers."
  (:require [clojure.string :as str]
            [charred.api :as json]
            [xia.autonomous :as autonomous]
            [xia.backup :as backup]
            [xia.browser :as browser]
            [xia.bridge :as bridge]
            [xia.channel.http.admin.instances :as admin-instances]
            [xia.channel.http.admin.providers :as admin-providers]
            [xia.channel.http.admin.schedules :as admin-schedules]
            [xia.channel.http.common :as http-common]
            [xia.channel.http.request :as http-request]
            [xia.channel.http.value :as http-value]
            [xia.channel.messaging :as messaging]
            [xia.config :as config]
            [xia.context :as context]
            [xia.db :as db]
            [xia.db-schema :as db-schema]
            [xia.identity :as identity]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.llm :as llm]
            [xia.llm-provider-template :as llm-provider-template]
            [xia.local-ocr :as local-ocr]
            [xia.oauth :as oauth]
            [xia.oauth-template :as oauth-template]
            [xia.paths :as paths]
            [xia.plugin :as plugin]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.schedule :as schedule]
            [xia.service :as service-proxy]
            [xia.setup :as setup]
            [xia.skill :as skill]
            [xia.skill.openclaw :as openclaw-skill]
            [xia.summarizer :as summarizer]
            [xia.memory :as memory]
            [xia.web :as web])
  (:import [java.net URI]
           [java.util Date]))

(def ^:private service-auth-types #{:bearer :basic :api-key-header :query-param :oauth-account})
(def ^:private service-email-backends #{:imap-smtp})
(def ^:private mail-security-modes #{:ssl :starttls :none})
(def ^:private oauth-account-connection-modes #{:oauth-flow :manual-token})
(def ^:private ms-per-day (* 24 60 60 1000))

(defn- db-migration->admin-body
  [{:keys [from-version to-version description applied-at]}]
  (cond-> {:from_version from-version
           :to_version to-version
           :description description}
    applied-at
    (assoc :applied_at applied-at)))

(defn- html-response
  [body]
  {:status  200
   :headers {"Content-Type" "text/html; charset=utf-8"}
   :body    body})

(defn- days->ms
  [days]
  (when-some [days* (some-> days long)]
    (* (long days*) (long ms-per-day))))

(defn- escape-html
  [value]
  (-> (str (or value ""))
      (str/replace "&" "&amp;")
      (str/replace "<" "&lt;")
      (str/replace ">" "&gt;")
      (str/replace "\"" "&quot;")
      (str/replace "'" "&#39;")))

(defn- parse-iso-instant
  [value field]
  (when-let [text (some-> value str str/trim not-empty)]
    (try
      (Date/from (java.time.Instant/parse text))
      (catch Exception _
        (throw (ex-info (str "invalid '" field "' field")
                        {:field field})))))) 

(defn- oauth-account-connection-mode
  [account]
  (or (:oauth.account/connection-mode account)
      (if (or (some-> (:oauth.account/authorize-url account) str str/trim not-empty)
              (some-> (:oauth.account/token-url account) str str/trim not-empty)
              (some-> (:oauth.account/client-id account) str str/trim not-empty))
        :oauth-flow
        :manual-token)))

(defn- normalize-base-url
  [value]
  (some-> value http-value/nonblank-str (str/replace #"/+$" "")))

(defn- normalize-id-segment
  [value]
  (some-> value
          str
          str/trim
          str/lower-case
          (str/replace #"[^a-z0-9]+" "-")
          (str/replace #"^-+|-+$" "")
          not-empty))

(defn- next-available-id
  [base used-ids]
  (let [base*    (or (normalize-id-segment base) "item")
        used-set (set (keep #(when % (name %)) used-ids))]
    (loop [candidate base*
           suffix    2]
      (if (contains? used-set candidate)
        (recur (str base* "-" suffix) (inc suffix))
        (keyword candidate)))))

(defn- infer-site-id
  [data]
  (let [name-text  (http-value/nonblank-str (get data "name"))
        login-url  (http-value/nonblank-str (get data "login_url"))
        url-base   (when login-url
                     (try
                       (let [uri  (URI. login-url)
                             host (some-> (.getHost uri)
                                          (str/replace #"^www\." ""))
                             path (some-> (.getPath uri) http-value/nonblank-str)]
                         (normalize-id-segment
                           (str/join "-" (filter some? [host path]))))
                       (catch Exception _
                         (normalize-id-segment login-url))))
        base       (or (normalize-id-segment name-text)
                       url-base
                       "site")
        used-ids   (map :site-cred/id (db/list-site-creds))]
    (next-available-id base used-ids)))

(defn- infer-skill-id
  [data]
  (let [name-text (http-value/nonblank-str (get data "name"))
        base      (or (normalize-id-segment name-text)
                      "skill")
        used-ids  (map :skill/id (db/list-skills))]
    (next-available-id base used-ids)))

(defn- plugin->admin-body
  [deps plugin]
  (let [manifest (:plugin/manifest plugin)]
    {:id           (some-> (:plugin/id plugin) name)
     :name         (:plugin/name plugin)
     :description  (:plugin/description plugin)
     :version      (:plugin/version plugin)
     :enabled      (boolean (:plugin/enabled? plugin))
     :capabilities (mapv str (sort-by str (:plugin/capabilities plugin)))
     :hooks        (mapv (fn [{:keys [id event]}]
                           {:id (some-> id name)
                            :event (some-> event name)})
                         (:hooks manifest))
     :installed_at (http-common/instant->str deps (:plugin/installed-at plugin))
     :updated_at   (http-common/instant->str deps (:plugin/updated-at plugin))}))

(defn- parse-skill-tags
  [value]
  (let [parts (cond
                (string? value) (str/split value #"[,\n]")
                (sequential? value) value
                :else nil)]
    (->> parts
         (map http-value/nonblank-str)
         (keep normalize-id-segment)
         (map keyword)
         set)))

(defn- parse-extra-fields
  [value]
  (let [text (http-value/nonblank-str value)]
    (when text
      (try
        (json/write-json-str (json/read-json text))
        (catch Exception _
          (throw (ex-info "extra_fields must be valid JSON"
                          {:field "extra_fields"})))))))

(defn- parse-json-object-string
  [value field-name]
  (let [text (http-value/nonblank-str value)]
    (when text
      (try
        (let [parsed (json/read-json text)]
          (when-not (map? parsed)
            (throw (ex-info (str field-name " must be a JSON object")
                            {:field field-name})))
          (json/write-json-str parsed))
        (catch clojure.lang.ExceptionInfo e
          (throw e))
        (catch Exception _
          (throw (ex-info (str field-name " must be valid JSON")
                          {:field field-name})))))))

(defn- parse-json-object-value
  [value field-name]
  (cond
    (nil? value)
    nil

    (map? value)
    value

    :else
    (some-> (parse-json-object-string value field-name)
            json/read-json)))

(defn- parse-auth-type
  [value]
  (let [auth-type (some-> value http-value/nonblank-str keyword)]
    (when-not (contains? service-auth-types auth-type)
      (throw (ex-info "invalid auth_type"
                      {:field "auth_type"
                       :value value})))
    auth-type))

(defn- parse-optional-service-email-backend
  [value]
  (when-let [backend (some-> value http-value/nonblank-str keyword)]
    (when-not (contains? service-email-backends backend)
      (throw (ex-info "invalid email_backend"
                      {:field "email_backend"
                       :value value})))
    backend))

(defn- parse-optional-mail-security
  [value field]
  (when-let [security (some-> value http-value/nonblank-str keyword)]
    (when-not (contains? mail-security-modes security)
      (throw (ex-info (str "invalid " field)
                      {:field field
                       :value value})))
    security))

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
  (config-resolution->admin-body resolution #(boolean (http-value/nonblank-str %))))

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
     :started_at        (http-common/instant->str deps (:started_at settings))
     :last_attempt_at   (http-common/instant->str deps (:last_attempt_at settings))
     :last_success_at   (http-common/instant->str deps (:last_success_at settings))
     :last_archive_path (:last_archive_path settings)
     :last_error        (:last_error settings)
     :next_due_at       (http-common/instant->str deps (:next_due_at settings))
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

(defn- service->admin-body
  [service]
  (let [oauth-account  (some-> (:service/oauth-account service) db/get-oauth-account)
        runtime-source (when-let [service-id (:service/id service)]
                         (name (runtime-overlay/entity-source :service service-id)))]
    {:id                               (some-> (:service/id service) name)
     :runtime_source                   runtime-source
     :name                             (:service/name service)
     :base_url                         (:service/base-url service)
     :smtp_url                         (:service/smtp-url service)
     :auth_type                        (some-> (:service/auth-type service) name)
     :email_backend                    (some-> (:service/email-backend service) name)
     :auth_header                      (:service/auth-header service)
     :auth_username                    (:service/auth-username service)
     :email_address                    (:service/email-address service)
     :imap_security                    (some-> (:service/imap-security service) name)
     :smtp_security                    (some-> (:service/smtp-security service) name)
     :inbox_folder                     (:service/inbox-folder service)
     :drafts_folder                    (:service/drafts-folder service)
     :sent_folder                      (:service/sent-folder service)
     :archive_folder                   (:service/archive-folder service)
     :trash_folder                     (:service/trash-folder service)
     :oauth_account                    (some-> (:service/oauth-account service) name)
     :oauth_account_name               (:oauth.account/name oauth-account)
     :oauth_account_connected          (boolean (http-value/nonblank-str (:oauth.account/access-token oauth-account)))
     :oauth_account_autonomous_approved (boolean (and oauth-account
                                                     (autonomous/oauth-account-autonomous-approved? oauth-account)))
     :rate_limit_per_minute            (:service/rate-limit-per-minute service)
     :allow_private_network            (boolean (:service/allow-private-network? service))
     :effective_rate_limit_per_minute  (service-proxy/effective-rate-limit-per-minute service)
     :autonomous_approved              (boolean (autonomous/service-autonomous-approved? service))
     :enabled                          (boolean (:service/enabled? service))
     :auth_key_configured              (boolean (http-value/nonblank-str (:service/auth-key service)))}))

(defn- managed-instance->admin-body
  [deps instance]
  (admin-instances/instance->admin-body deps instance))

(defn- oauth-account->admin-body
  [deps account]
  {:id                       (some-> (:oauth.account/id account) name)
   :runtime_source           (when-let [account-id (:oauth.account/id account)]
                               (name (runtime-overlay/entity-source :oauth-account account-id)))
   :name                     (:oauth.account/name account)
   :connection_mode          (some-> (oauth-account-connection-mode account) name)
   :authorize_url            (:oauth.account/authorize-url account)
   :token_url                (:oauth.account/token-url account)
   :client_id                (:oauth.account/client-id account)
   :provider_template        (some-> (:oauth.account/provider-template account) name)
   :scopes                   (:oauth.account/scopes account)
   :redirect_uri             (:oauth.account/redirect-uri account)
   :auth_params              (:oauth.account/auth-params account)
   :token_params             (:oauth.account/token-params account)
   :client_secret_configured (boolean (http-value/nonblank-str (:oauth.account/client-secret account)))
   :access_token_configured  (boolean (http-value/nonblank-str (:oauth.account/access-token account)))
   :refresh_token_configured (boolean (http-value/nonblank-str (:oauth.account/refresh-token account)))
   :token_type               (:oauth.account/token-type account)
   :autonomous_approved      (boolean (autonomous/oauth-account-autonomous-approved? account))
   :connected                (boolean (http-value/nonblank-str (:oauth.account/access-token account)))
   :expires_at               (http-common/instant->str deps (:oauth.account/expires-at account))
   :connected_at             (http-common/instant->str deps (:oauth.account/connected-at account))})

(defn- oauth-template->admin-body
  [template]
  {:id            (some-> (:id template) name)
   :name          (:name template)
   :description   (:description template)
   :authorize_url (:authorize-url template)
   :token_url     (:token-url template)
   :api_base_url  (:api-base-url template)
   :service_id    (:service-id template)
   :service_name  (:service-name template)
   :scopes        (:scopes template)
   :auth_params   (json/write-json-str (or (:auth-params template) {}))
   :token_params  (json/write-json-str (or (:token-params template) {}))
   :notes         (:notes template)})

(defn- oauth-account-template-service-spec
  [account]
  (when-let [template-id (:oauth.account/provider-template account)]
    (when-let [template (oauth-template/get-template template-id)]
      (let [service-id   (some-> (:service-id template) http-value/nonblank-str keyword)
            service-name (or (http-value/nonblank-str (:service-name template))
                             (http-value/nonblank-str (:name template)))
            api-base-url (http-value/nonblank-str (:api-base-url template))]
        (when (and service-id api-base-url)
          {:id       service-id
           :name     (or service-name (name service-id))
           :base-url api-base-url})))))

(defn- approve-template-oauth-account!
  [account]
  (if (and (oauth-account-template-service-spec account)
           (not (autonomous/oauth-account-autonomous-approved? account)))
    (let [account-id (:oauth.account/id account)]
      (db/save-oauth-account!
       (cond-> {:id account-id
                :name (:oauth.account/name account)
                :scopes (:oauth.account/scopes account)
                :autonomous-approved? true}
         (contains? account :oauth.account/connection-mode)
         (assoc :connection-mode (:oauth.account/connection-mode account))
         (contains? account :oauth.account/authorize-url)
         (assoc :authorize-url (:oauth.account/authorize-url account))
         (contains? account :oauth.account/token-url)
         (assoc :token-url (:oauth.account/token-url account))
         (contains? account :oauth.account/client-id)
         (assoc :client-id (:oauth.account/client-id account))
         (contains? account :oauth.account/client-secret)
         (assoc :client-secret (:oauth.account/client-secret account))
         (contains? account :oauth.account/provider-template)
         (assoc :provider-template (:oauth.account/provider-template account))
         (contains? account :oauth.account/redirect-uri)
         (assoc :redirect-uri (:oauth.account/redirect-uri account))
         (contains? account :oauth.account/auth-params)
         (assoc :auth-params (:oauth.account/auth-params account))
         (contains? account :oauth.account/token-params)
         (assoc :token-params (:oauth.account/token-params account))
         (contains? account :oauth.account/access-token)
         (assoc :access-token (:oauth.account/access-token account))
         (contains? account :oauth.account/refresh-token)
         (assoc :refresh-token (:oauth.account/refresh-token account))
         (contains? account :oauth.account/token-type)
         (assoc :token-type (:oauth.account/token-type account))
         (contains? account :oauth.account/expires-at)
         (assoc :expires-at (:oauth.account/expires-at account))
         (contains? account :oauth.account/connected-at)
         (assoc :connected-at (:oauth.account/connected-at account))))
      (db/get-oauth-account account-id))
    account))

(defn- sync-template-service-for-oauth-account!
  [account]
  (let [account (approve-template-oauth-account! account)]
    (when-let [{:keys [id name base-url]} (oauth-account-template-service-spec account)]
    (let [existing (db/get-service id)]
      (db/save-service! {:id            id
                         :name          (or (some-> (:service/name existing) http-value/nonblank-str)
                                            name)
                         :base-url      base-url
                         :auth-type     :oauth-account
                         :oauth-account (:oauth.account/id account)
                         :autonomous-approved? true})
      (db/get-service id)))))

(defn- auto-managed-template-service-for-oauth-account
  [account]
  (when-let [{:keys [id base-url]} (oauth-account-template-service-spec account)]
    (let [service (db/get-service id)]
      (when (and service
                 (= :oauth-account (:service/auth-type service))
                 (= (:oauth.account/id account) (:service/oauth-account service))
                 (= base-url (http-value/nonblank-str (:service/base-url service))))
        service))))

(defn- site->admin-body
  [site]
  {:id                  (some-> (:site-cred/id site) name)
   :runtime_source      (when-let [site-id (:site-cred/id site)]
                          (name (runtime-overlay/entity-source :site-cred site-id)))
   :name                (:site-cred/name site)
   :login_url           (:site-cred/login-url site)
   :username_field      (:site-cred/username-field site)
   :password_field      (:site-cred/password-field site)
   :form_selector       (:site-cred/form-selector site)
   :extra_fields        (:site-cred/extra-fields site)
   :autonomous_approved (boolean (autonomous/site-autonomous-approved? site))
   :username_configured (boolean (http-value/nonblank-str (:site-cred/username site)))
   :password_configured (boolean (http-value/nonblank-str (:site-cred/password site)))})

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

(defn- skill->body
  [deps skill]
  {:id                    (some-> (:skill/id skill) name)
   :name                  (:skill/name skill)
   :description           (:skill/description skill)
   :version               (:skill/version skill)
   :content_sha256        (:skill/content-sha256 skill)
   :source_sha256         (:skill/source-sha256 skill)
   :tags                  (->> (or (:skill/tags skill) [])
                               (map name)
                               sort
                               vec)
   :enabled               (boolean (:skill/enabled? skill))
   :source_format         (some-> (:skill/source-format skill) name)
   :source_path           (:skill/source-path skill)
   :source_url            (:skill/source-url skill)
   :source_name           (:skill/source-name skill)
   :provenance            (:skill/provenance skill)
   :trust_level           (some-> (:skill/trust-level skill) name)
   :trust_note            (:skill/trust-note skill)
   :lifecycle             (some-> (:skill/lifecycle skill) name)
   :lifecycle_reason      (:skill/lifecycle-reason skill)
   :installed_at          (http-common/instant->str deps (:skill/installed-at skill))
   :updated_at            (http-common/instant->str deps (:skill/updated-at skill))
   :archived_at           (http-common/instant->str deps (:skill/archived-at skill))
   :selected_count        (long (or (:skill/selected-count skill) 0))
   :injected_count        (long (or (:skill/injected-count skill) 0))
   :viewed_count          (long (or (:skill/viewed-count skill) 0))
   :patched_count         (long (or (:skill/patched-count skill) 0))
   :last_used_at          (http-common/instant->str deps (:skill/last-used-at skill))
   :last_update_check_at  (http-common/instant->str deps (:skill/last-update-check-at skill))
   :last_update_status    (some-> (:skill/last-update-status skill) name)
   :last_update_source_sha256 (:skill/last-update-source-sha256 skill)
   :import_warnings       (->> (or (:skill/import-warnings skill) [])
                               sort
                               vec)
   :imported_from_openclaw (boolean (:skill/imported-from-openclaw? skill))})

(defn- skill->detail-body
  [deps skill]
  (assoc (skill->body deps skill)
         :content (:skill/content skill)))

(defn- parse-optional-bounded-double
  [value field-name]
  (let [text (http-value/nonblank-str value)]
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
  (let [backend (some-> value http-value/nonblank-str keyword)]
    (when backend
      (when-not (contains? #{:local :external} backend)
        (throw (ex-info (str "'" field-name "' must be one of: local, external")
                        {:field field-name
                         :value value})))
      backend)))

(defn- parse-optional-provider-id
  [value field-name]
  (when-let [provider-id-str (http-value/nonblank-str value)]
    (let [provider-id (keyword provider-id-str)]
      (when-not (db/get-provider provider-id)
        (throw (ex-info (str "'" field-name "' must reference an existing provider")
                        {:field field-name
                         :value value})))
      provider-id)))

(defn- oauth-callback-page
  [status title message account-id]
  (let [title*   (escape-html title)
        message* (escape-html message)
        account* (some-> account-id name escape-html)]
    (str "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">"
         "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
         "<title>Xia OAuth</title>"
         "<style>body{margin:0;font-family:\"Avenir Next\",\"Segoe UI\",sans-serif;background:#f5efe3;color:#172119;display:grid;place-items:center;min-height:100vh;padding:24px;}main{max-width:36rem;background:rgba(255,252,246,.96);border:1px solid rgba(23,33,25,.12);border-radius:24px;padding:28px;box-shadow:0 20px 50px rgba(23,33,25,.12);}h1{margin:0 0 12px;font-size:2rem;}p{line-height:1.6;margin:0 0 10px;}code{font-family:\"SFMono-Regular\",Consolas,monospace;background:rgba(23,33,25,.06);padding:2px 6px;border-radius:8px;}</style>"
         "</head><body><main><h1>" title* "</h1><p>" message* "</p>"
         (when account*
           (str "<p>OAuth account: <code>" account* "</code></p>"))
         "<p>You can close this window and return to Xia.</p>"
         "<script>"
         "try {"
         "  if (window.opener && window.opener !== window) {"
         "    window.opener.postMessage({type:'xia-oauth-complete', status:" (json/write-json-str (name status)) ", account_id:" (json/write-json-str (some-> account-id name)) "}, window.location.origin);"
         "  }"
         "} catch (_err) {}"
         "setTimeout(() => { try { window.close(); } catch (_err) {} }, 1200);"
         "</script></main></body></html>")))

(def handle-list-managed-instances admin-instances/handle-list-managed-instances)

(def handle-stop-managed-instance admin-instances/handle-stop-managed-instance)

(defn handle-reload-runtime-overlay
  [deps req]
  (try
    (let [data         (or (http-common/read-body deps req) {})
          overlay-path (or (http-value/nonblank-str (get data "overlay_path"))
                           (http-value/nonblank-str (get data :overlay_path)))]
      (if overlay-path
        (runtime-overlay/reload! overlay-path)
        (runtime-overlay/reload!))
      (http-common/json-response deps 200
                      {:status "reloaded"
                       :runtime_overlay (runtime-overlay/admin-summary)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-admin-config
  [deps _req]
  (let [providers       (db/list-providers)
        setup-required? (or (empty? providers)
                            (nil? (db/get-default-provider)))
        storage-layout  (paths/storage-layout (db/current-db-path))]
    (http-common/json-response
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
       :oauth_provider_templates (->> (oauth-template/list-templates)
                                      (into [] (map oauth-template->admin-body))
                                      sort-by-name)
       :oauth_accounts (->> (db/list-oauth-accounts)
                            (into [] (map #(oauth-account->admin-body deps %)))
                            sort-by-name)
       :services  (->> (db/list-services)
                       (into [] (map service->admin-body))
                       sort-by-name)
       :sites     (->> (db/list-site-creds)
                       (into [] (map site->admin-body))
                       sort-by-name)
       :schedules (->> (schedule/list-schedules)
                       (into [] (map #(schedule->admin-body deps %)))
                       sort-by-name)
       :tools     (->> (db/list-tools)
                       (into [] (map tool->admin-body))
                       sort-by-name)
       :plugins   (->> (db/list-plugins)
                       (into [] (map #(plugin->admin-body deps %)))
                       sort-by-name)
       :skills    (->> (db/list-skills)
                       (into [] (map #(skill->body deps %)))
                       sort-by-name)})))

(def handle-fetch-provider-models admin-providers/handle-fetch-provider-models)

(def handle-fetch-provider-model-metadata admin-providers/handle-fetch-provider-model-metadata)

(def handle-save-provider admin-providers/handle-save-provider)

(def handle-delete-provider admin-providers/handle-delete-provider)

(defn handle-save-memory-retention
  [deps req]
  (try
    (let [data                 (or (http-common/read-body deps req) {})
          full-resolution-days (when (contains? data "full_resolution_days")
                                 (http-value/parse-optional-positive-long (get data "full_resolution_days")
                                                               "full_resolution_days"))
          decay-half-life-days (when (contains? data "decay_half_life_days")
                                 (http-value/parse-optional-positive-long (get data "decay_half_life_days")
                                                               "decay_half_life_days"))
          retained-count       (when (contains? data "retained_count")
                                 (http-value/parse-optional-positive-long (get data "retained_count")
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
      (http-common/json-response deps 200 {:memory_retention (memory-retention->admin-body deps)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-save-web-search
  [deps req]
  (try
    (let [data (or (http-common/read-body deps req) {})]
      (save-config-override! :web/search-backend
                             (http-value/nonblank-str (get data "backend")))
      (save-config-override! :web/search-brave-api-key
                             (http-value/nonblank-str (get data "brave_api_key")))
      (save-config-override! :web/search-searxng-url
                             (http-value/nonblank-str (get data "searxng_url")))
      (http-common/json-response deps 200
                      {:web_search (web-search->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-save-identity
  [deps req]
  (try
    (let [data (or (http-common/read-body deps req) {})]
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
        (http-common/json-response
          deps
          200
          {:identity {:name        (:name soul "Xia")
                      :role        (:role soul "")
                      :description (:description soul "")
                      :personality (:personality soul "")
                      :guidelines  (:guidelines soul "")}
           :capabilities (instance-supervisor/capabilities)})))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-save-conversation-context
  [deps req]
  (try
    (let [data                         (or (http-common/read-body deps req) {})
          recent-history-message-limit (when (contains? data "recent_history_message_limit")
                                         (http-value/parse-optional-positive-long (get data "recent_history_message_limit")
                                                                       "recent_history_message_limit"))
          history-budget               (when (contains? data "history_budget")
                                         (http-value/parse-optional-positive-long (get data "history_budget")
                                                                       "history_budget"))]
      (when (contains? data "recent_history_message_limit")
        (save-config-override! :context/recent-history-message-limit
                               recent-history-message-limit))
      (when (contains? data "history_budget")
        (save-config-override! :context/history-budget
                               history-budget))
      (http-common/json-response deps 200 {:conversation_context (conversation-context->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-save-knowledge-decay
  [deps req]
  (try
    (let [data                      (or (http-common/read-body deps req) {})
          grace-period-days         (when (contains? data "grace_period_days")
                                      (http-value/parse-optional-positive-long (get data "grace_period_days")
                                                                    "grace_period_days"))
          half-life-days            (when (contains? data "half_life_days")
                                      (http-value/parse-optional-positive-long (get data "half_life_days")
                                                                    "half_life_days"))
          min-confidence            (when (contains? data "min_confidence")
                                      (parse-optional-bounded-double (get data "min_confidence")
                                                                     "min_confidence"))
          maintenance-interval-days (when (contains? data "maintenance_interval_days")
                                      (http-value/parse-optional-positive-long (get data "maintenance_interval_days")
                                                                    "maintenance_interval_days"))
          archive-after-bottom-days (when (contains? data "archive_after_bottom_days")
                                      (http-value/parse-optional-positive-long (get data "archive_after_bottom_days")
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
      (http-common/json-response deps 200 {:knowledge_decay (knowledge-decay->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-save-local-doc-summarization
  [deps req]
  (try
    (let [data                     (or (http-common/read-body deps req) {})
          enabled?                 (when (contains? data "model_summaries_enabled")
                                     (true? (get data "model_summaries_enabled")))
          backend                  (when (contains? data "model_summary_backend")
                                     (parse-summary-backend (get data "model_summary_backend")
                                                            "model_summary_backend"))
          provider-id              (when (contains? data "model_summary_provider_id")
                                     (parse-optional-provider-id (get data "model_summary_provider_id")
                                                                 "model_summary_provider_id"))
          chunk-summary-max-tokens (when (contains? data "chunk_summary_max_tokens")
                                     (http-value/parse-optional-positive-long (get data "chunk_summary_max_tokens")
                                                                   "chunk_summary_max_tokens"))
          doc-summary-max-tokens   (when (contains? data "doc_summary_max_tokens")
                                     (http-value/parse-optional-positive-long (get data "doc_summary_max_tokens")
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
      (http-common/json-response deps 200 {:local_doc_summarization (local-doc-summarization->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-save-local-doc-ocr
  [deps req]
  (try
    (let [data          (or (http-common/read-body deps req) {})
          enabled?      (when (contains? data "enabled")
                          (true? (get data "enabled")))
          model-backend (when (contains? data "model_backend")
                          (parse-summary-backend (get data "model_backend")
                                                 "model_backend"))
          provider-id   (when (contains? data "external_provider_id")
                          (parse-optional-provider-id (get data "external_provider_id")
                                                      "external_provider_id"))
          timeout-ms    (when (contains? data "timeout_ms")
                          (http-value/parse-optional-positive-long (get data "timeout_ms")
                                                        "timeout_ms"))
          max-tokens    (when (contains? data "max_tokens")
                          (http-value/parse-optional-positive-long (get data "max_tokens")
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
      (http-common/json-response deps 200 {:local_doc_ocr (local-doc-ocr->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-save-database-backup
  [deps req]
  (try
    (let [data           (or (http-common/read-body deps req) {})
          enabled?       (when (contains? data "enabled")
                           (true? (get data "enabled")))
          directory      (when (contains? data "directory")
                           (http-value/nonblank-str (get data "directory")))
          interval-hours (when (contains? data "interval_hours")
                           (http-value/parse-optional-positive-long (get data "interval_hours")
                                                         "interval_hours"))
          retain-count   (when (contains? data "retain_count")
                           (http-value/parse-optional-positive-long (get data "retain_count")
                                                         "retain_count"))]
      (when (contains? data "enabled")
        (save-config-override! :backup/enabled? enabled?))
      (when (contains? data "directory")
        (save-config-override! :backup/directory directory))
      (when (contains? data "interval_hours")
        (save-config-override! :backup/interval-hours interval-hours))
      (when (contains? data "retain_count")
        (save-config-override! :backup/retain-count retain-count))
      (http-common/json-response deps 200 {:database_backup (database-backup->admin-body deps)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-save-messaging
  [deps req]
  (try
    (let [data  (or (http-common/read-body deps req) {})
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
      (http-common/json-response deps 200 {:messaging_channels saved}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))
    (catch Exception e
      (http-common/json-response deps 500 {:error (or (.getMessage e)
                                           "failed to save messaging settings")}))))

(defn handle-save-service
  [deps req]
  (try
    (let [data                   (or (http-common/read-body deps req) {})
          service-id             (http-value/parse-keyword-id (get data "id") "id")
          existing               (db/get-service service-id)
          base-url               (http-value/nonblank-str (get data "base_url"))
          smtp-url               (if (contains? data "smtp_url")
                                   (http-value/nonblank-str (get data "smtp_url"))
                                   (:service/smtp-url existing))
          name                   (or (http-value/nonblank-str (get data "name"))
                                     (name service-id))
          auth-type              (parse-auth-type (get data "auth_type"))
          email-backend          (if (contains? data "email_backend")
                                   (parse-optional-service-email-backend (get data "email_backend"))
                                   (:service/email-backend existing))
          entered-auth-key       (http-value/nonblank-str (get data "auth_key"))
          auth-username          (or (http-value/nonblank-str (get data "auth_username"))
                                     (:service/auth-username existing))
          email-address          (or (http-value/nonblank-str (get data "email_address"))
                                     (:service/email-address existing))
          imap-security          (or (parse-optional-mail-security (get data "imap_security") "imap_security")
                                     (:service/imap-security existing))
          smtp-security          (or (parse-optional-mail-security (get data "smtp_security") "smtp_security")
                                     (:service/smtp-security existing))
          inbox-folder           (or (http-value/nonblank-str (get data "inbox_folder"))
                                     (:service/inbox-folder existing))
          drafts-folder          (or (http-value/nonblank-str (get data "drafts_folder"))
                                     (:service/drafts-folder existing))
          sent-folder            (or (http-value/nonblank-str (get data "sent_folder"))
                                     (:service/sent-folder existing))
          archive-folder         (or (http-value/nonblank-str (get data "archive_folder"))
                                     (:service/archive-folder existing))
          trash-folder           (or (http-value/nonblank-str (get data "trash_folder"))
                                     (:service/trash-folder existing))
          rate-limit-per-minute  (http-value/parse-optional-positive-long (get data "rate_limit_per_minute")
                                                               "rate_limit_per_minute")
          allow-private-network? (when (contains? data "allow_private_network")
                                   (true? (get data "allow_private_network")))
          autonomous-approved?   (when (contains? data "autonomous_approved")
                                   (true? (get data "autonomous_approved")))
          enabled?               (if (contains? data "enabled")
                                   (true? (get data "enabled"))
                                   true)
          oauth-account-id       (when (= :oauth-account auth-type)
                                   (let [value (or (http-value/nonblank-str (get data "oauth_account"))
                                                   (some-> (:service/oauth-account existing) name))]
                                     (when-not value
                                       (throw (ex-info "oauth_account is required for oauth-account auth_type"
                                                       {:field "oauth_account"})))
                                     (let [account-id (keyword value)]
                                       (when-not (db/get-oauth-account account-id)
                                         (throw (ex-info "unknown oauth_account"
                                                         {:field "oauth_account"
                                                          :value value})))
                                       account-id)))
          entered-header         (http-value/nonblank-str (get data "auth_header"))
          auth-header            (when (#{:api-key-header :query-param} auth-type)
                                   (or entered-header
                                       (:service/auth-header existing)))
          auth-key               (when-not (= :oauth-account auth-type)
                                   (or entered-auth-key
                                       (:service/auth-key existing)
                                       ""))]
      (when-not base-url
        (throw (ex-info "missing 'base_url' field" {:field "base_url"})))
      (when (and (#{:api-key-header :query-param} auth-type)
                 (nil? auth-header))
        (throw (ex-info "auth_header is required for the selected auth_type"
                        {:field "auth_header"})))
      (db/save-service! {:id                     service-id
                         :name                   name
                         :base-url               base-url
                         :smtp-url               smtp-url
                         :auth-type              auth-type
                         :email-backend          email-backend
                         :auth-key               (or auth-key "")
                         :auth-username          auth-username
                         :auth-header            auth-header
                         :email-address          email-address
                         :imap-security          imap-security
                         :smtp-security          smtp-security
                         :inbox-folder           inbox-folder
                         :drafts-folder          drafts-folder
                         :sent-folder            sent-folder
                         :archive-folder         archive-folder
                         :trash-folder           trash-folder
                         :oauth-account          oauth-account-id
                         :rate-limit-per-minute  rate-limit-per-minute
                         :allow-private-network? allow-private-network?
                         :autonomous-approved?   autonomous-approved?
                         :enabled?               enabled?})
      (http-common/json-response deps 200 {:service (service->admin-body (db/get-service service-id))}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-save-oauth-account
  [deps req]
  (try
    (let [data                  (or (http-common/read-body deps req) {})
          account-id            (http-value/parse-keyword-id (get data "id") "id")
          existing              (db/get-oauth-account account-id)
          name                  (or (http-value/nonblank-str (get data "name"))
                                    (name account-id))
          connection-mode       (let [parsed (if (contains? data "connection_mode")
                                               (some-> (get data "connection_mode") http-value/nonblank-str keyword)
                                               (when existing
                                                 (oauth-account-connection-mode existing)))]
                                  (when (and parsed
                                             (not (oauth-account-connection-modes parsed)))
                                    (throw (ex-info "unknown connection_mode"
                                                    {:field "connection_mode"
                                                     :value (name parsed)})))
                                  (or parsed :oauth-flow))
          authorize-url         (http-value/nonblank-str (get data "authorize_url"))
          token-url             (http-value/nonblank-str (get data "token_url"))
          client-id             (http-value/nonblank-str (get data "client_id"))
          client-secret         (or (http-value/nonblank-str (get data "client_secret"))
                                    (:oauth.account/client-secret existing)
                                    "")
          access-token          (or (http-value/nonblank-str (get data "access_token"))
                                    (:oauth.account/access-token existing))
          refresh-token         (or (http-value/nonblank-str (get data "refresh_token"))
                                    (:oauth.account/refresh-token existing))
          token-type            (or (http-value/nonblank-str (get data "token_type"))
                                    (:oauth.account/token-type existing)
                                    "Bearer")
          expires-at            (if (contains? data "expires_at")
                                  (parse-iso-instant (get data "expires_at") "expires_at")
                                  (:oauth.account/expires-at existing))
          connected-at          (cond
                                  (http-value/nonblank-str (get data "access_token")) (Date.)
                                  access-token (:oauth.account/connected-at existing)
                                  :else nil)
          provider-template-id  (if (contains? data "provider_template")
                                  (some-> (get data "provider_template") http-value/nonblank-str keyword)
                                  (:oauth.account/provider-template existing))
          scopes                (or (http-value/nonblank-str (get data "scopes")) "")
          redirect-uri          (http-value/nonblank-str (get data "redirect_uri"))
          auth-params           (parse-json-object-string (get data "auth_params") "auth_params")
          token-params          (parse-json-object-string (get data "token_params") "token_params")
          autonomous-approved?  (let [requested (when (contains? data "autonomous_approved")
                                                  (true? (get data "autonomous_approved")))
                                      template-account (cond-> {:oauth.account/provider-template provider-template-id}
                                                         provider-template-id
                                                         (assoc :oauth.account/id account-id))]
                                  (if (oauth-account-template-service-spec template-account)
                                    true
                                    requested))]
      (when (= connection-mode :oauth-flow)
        (when-not authorize-url
          (throw (ex-info "missing 'authorize_url' field" {:field "authorize_url"})))
        (when-not token-url
          (throw (ex-info "missing 'token_url' field" {:field "token_url"})))
        (when-not client-id
          (throw (ex-info "missing 'client_id' field" {:field "client_id"}))))
      (when (= connection-mode :manual-token)
        (when-not access-token
          (throw (ex-info "missing 'access_token' field"
                          {:field "access_token"}))))
      (when (and provider-template-id
                 (nil? (oauth-template/get-template provider-template-id)))
        (throw (ex-info "unknown provider_template"
                        {:field "provider_template"
                         :value (name provider-template-id)})))
      (db/save-oauth-account! {:id                    account-id
                               :name                  name
                               :connection-mode       connection-mode
                               :authorize-url         (when (= connection-mode :oauth-flow) authorize-url)
                               :token-url             (when (= connection-mode :oauth-flow) token-url)
                               :client-id             (when (= connection-mode :oauth-flow) client-id)
                               :client-secret         client-secret
                               :provider-template     provider-template-id
                               :scopes                scopes
                               :redirect-uri          redirect-uri
                               :auth-params           auth-params
                               :token-params          token-params
                               :autonomous-approved?  autonomous-approved?
                               :access-token          access-token
                               :refresh-token         refresh-token
                               :token-type            token-type
                               :expires-at            expires-at
                               :connected-at          connected-at})
      (let [saved-account (db/get-oauth-account account-id)]
        (sync-template-service-for-oauth-account! saved-account)
        (http-common/json-response deps 200 {:oauth_account (oauth-account->admin-body deps saved-account)})))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-delete-oauth-account
  [deps account-id]
  (try
    (let [oauth-id             (http-value/parse-keyword-id account-id "oauth_account_id")
          account              (db/get-oauth-account oauth-id)
          linked-providers     (into []
                                     (filter #(= oauth-id (:llm.provider/oauth-account %)))
                                     (db/list-providers))
          linked-services      (into []
                                     (filter #(= oauth-id (:service/oauth-account %)))
                                     (db/list-services))
          auto-managed-service (some-> account auto-managed-template-service-for-oauth-account)
          auto-managed-only?   (and (empty? linked-providers)
                                    (= 1 (count linked-services))
                                    auto-managed-service
                                    (= (:service/id auto-managed-service)
                                       (:service/id (first linked-services))))]
      (cond
        (nil? account)
        (http-common/json-response deps 404 {:error "oauth account not found"})

        auto-managed-only?
        (do
          (db/remove-service! (:service/id auto-managed-service))
          (db/remove-oauth-account! oauth-id)
          (http-common/json-response deps 200 {:status "deleted"
                                    :oauth_account_id (name oauth-id)}))

        (or (seq linked-providers) (seq linked-services))
        (http-common/json-response deps 409 {:error "oauth account is still referenced by a provider or service"})

        :else
        (do
          (db/remove-oauth-account! oauth-id)
          (http-common/json-response deps 200 {:status "deleted"
                                    :oauth_account_id (name oauth-id)}))))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-start-oauth-connect
  [deps account-id req]
  (try
    (let [oauth-id     (http-value/parse-keyword-id account-id "oauth_account_id")
          account      (or (db/get-oauth-account oauth-id)
                           (throw (ex-info "unknown oauth_account"
                                           {:field "oauth_account_id"
                                            :value (name oauth-id)})))
          callback-url (str (or (http-common/request-base-url deps req)
                                (throw (ex-info "cannot determine callback base URL"
                                                {:field "host"})))
                            "/oauth/callback")
          _            (when (= :manual-token (oauth-account-connection-mode account))
                         (throw (ex-info "manual-token connections do not support Connect Now"
                                         {:field "connection_mode"})))
          started      (oauth/start-authorization! oauth-id callback-url)]
      (http-common/json-response deps 200 {:oauth_account_id   (name oauth-id)
                                :authorization_url (:authorization-url started)
                                :redirect_uri      (:redirect-uri started)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-refresh-oauth-account
  [deps account-id]
  (try
    (let [oauth-id          (http-value/parse-keyword-id account-id "oauth_account_id")
          current-account   (or (db/get-oauth-account oauth-id)
                                (throw (ex-info "unknown oauth_account"
                                                {:field "oauth_account_id"
                                                 :value (name oauth-id)})))
          _                 (when-not (http-value/nonblank-str (:oauth.account/refresh-token current-account))
                              (throw (ex-info "refresh token is not configured for this connection"
                                              {:field "refresh_token"})))
          _                 (when (= :manual-token (oauth-account-connection-mode current-account))
                              (throw (ex-info "manual-token connections do not support Refresh"
                                              {:field "connection_mode"})))
          refreshed-account (oauth/refresh-account! oauth-id)]
      (http-common/json-response deps 200 {:oauth_account (oauth-account->admin-body deps refreshed-account)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-oauth-callback
  [deps req]
  (let [params             (http-request/parse-query-string (:query-string req))
        state              (get params "state")
        pending-account-id (some-> (and (seq state) (oauth/callback-account-id state)) name)
        code               (get params "code")
        error-code         (get params "error")
        error-description  (or (get params "error_description") error-code)]
    (cond
      (not (seq state))
      (html-response (oauth-callback-page "error"
                                          "OAuth failed"
                                          "Missing authorization state."
                                          nil))

      (seq error-code)
      (html-response (oauth-callback-page "error"
                                          "OAuth was not completed"
                                          (str "Provider returned: " error-description)
                                          pending-account-id))

      (not (seq code))
      (html-response (oauth-callback-page "error"
                                          "OAuth failed"
                                          "Missing authorization code."
                                          pending-account-id))

      :else
      (try
        (let [account (oauth/complete-authorization! state code)]
          (sync-template-service-for-oauth-account! account)
          (html-response (oauth-callback-page "ok"
                                              "OAuth connected"
                                              "Xia stored the new access token and can now use this account for online work."
                                              (some-> (:oauth.account/id account) name))))
        (catch clojure.lang.ExceptionInfo e
          (html-response (oauth-callback-page "error"
                                              "OAuth failed"
                                              (.getMessage e)
                                              pending-account-id)))))))

(defn handle-save-site
  [deps req]
  (try
    (let [data                 (or (http-common/read-body deps req) {})
          site-id              (if-let [id-text (http-value/nonblank-str (get data "id"))]
                                 (http-value/parse-keyword-id id-text "id")
                                 (infer-site-id data))
          existing             (db/get-site-cred site-id)
          login-url            (http-value/nonblank-str (get data "login_url"))
          name                 (or (http-value/nonblank-str (get data "name"))
                                   (name site-id))
          username-field       (or (http-value/nonblank-str (get data "username_field"))
                                   "username")
          password-field       (or (http-value/nonblank-str (get data "password_field"))
                                   "password")
          username             (or (http-value/nonblank-str (get data "username"))
                                   (:site-cred/username existing)
                                   "")
          password             (or (http-value/nonblank-str (get data "password"))
                                   (:site-cred/password existing)
                                   "")
          form-selector        (http-value/nonblank-str (get data "form_selector"))
          extra-fields         (parse-extra-fields (get data "extra_fields"))
          autonomous-approved? (when (contains? data "autonomous_approved")
                                 (true? (get data "autonomous_approved")))]
      (when-not login-url
        (throw (ex-info "missing 'login_url' field" {:field "login_url"})))
      (db/save-site-cred! {:id                     site-id
                           :name                   name
                           :login-url              login-url
                           :username-field         username-field
                           :password-field         password-field
                           :username               username
                           :password               password
                           :form-selector          form-selector
                           :extra-fields           extra-fields
                           :autonomous-approved?   autonomous-approved?})
      (http-common/json-response deps 200 {:site (site->admin-body (db/get-site-cred site-id))}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-delete-site
  [deps site-id]
  (try
    (let [site-key (http-value/parse-keyword-id site-id "site_id")]
      (if (db/get-site-cred site-key)
        (do
          (db/remove-site-cred! site-key)
          (http-common/json-response deps 200 {:status "deleted"
                                    :site_id (name site-key)}))
        (http-common/json-response deps 404 {:error "site credential not found"})))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(def handle-save-schedule admin-schedules/handle-save-schedule)

(def handle-delete-schedule admin-schedules/handle-delete-schedule)

(def handle-pause-schedule admin-schedules/handle-pause-schedule)

(def handle-resume-schedule admin-schedules/handle-resume-schedule)

(defn handle-save-skill
  [deps req]
  (try
    (let [data        (or (http-common/read-body deps req) {})
          skill-id    (if-let [id-text (http-value/nonblank-str (get data "id"))]
                        (http-value/parse-keyword-id id-text "id")
                        (infer-skill-id data))
          existing    (db/get-skill skill-id)
          skill-name  (or (http-value/nonblank-str (get data "name"))
                          (:skill/name existing)
                          (name skill-id))
          description (if (contains? data "description")
                        (or (http-value/nonblank-str (get data "description")) "")
                        (:skill/description existing))
          content     (if (contains? data "content")
                        (str (or (get data "content") ""))
                        (or (:skill/content existing) ""))
          version     (if (contains? data "version")
                        (http-value/nonblank-str (get data "version"))
                        (:skill/version existing))
          enabled?    (if (contains? data "enabled")
                        (true? (get data "enabled"))
                        (when (contains? existing :skill/enabled?)
                          (:skill/enabled? existing)))
          tags        (if (contains? data "tags")
                        (parse-skill-tags (get data "tags"))
                        nil)
          saved       (skill/save-skill! {:id          skill-id
                                          :name        skill-name
                                          :description description
                                          :content     content
                                          :version     version
                                          :tags        tags
                                          :enabled?    enabled?})]
      (http-common/json-response deps 200 {:skill (skill->detail-body deps saved)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-get-skill
  [deps skill-id]
  (try
    (let [skill-key (http-value/parse-keyword-id skill-id "skill_id")
          saved     (db/get-skill skill-key)]
      (if saved
        (do
          (skill/record-usage! skill-key :viewed)
          (http-common/json-response deps 200 {:skill (skill->detail-body deps (db/get-skill skill-key))}))
        (http-common/json-response deps 404 {:error "skill not found"})))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-delete-skill
  [deps skill-id]
  (try
    (let [skill-key (http-value/parse-keyword-id skill-id "skill_id")]
      (if (db/get-skill skill-key)
        (do
          (db/remove-skill! skill-key)
          (http-common/json-response deps 200 {:status "deleted"
                                    :skill_id (name skill-key)}))
        (http-common/json-response deps 404 {:error "skill not found"})))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn- skill-update-check->body
  [result]
  (cond-> {:status (some-> (:status result) name)
           :skill_id (some-> (:skill-id result) name)
           :source (:source result)
           :current_sha256 (:current_sha256 result)
           :imported_sha256 (:imported_sha256 result)
           :source_sha256 (:source_sha256 result)
           :safe_to_apply (boolean (:safe_to_apply? result))}
    (seq (:warnings result)) (assoc :warnings (vec (:warnings result)))
    (seq (:errors result)) (assoc :errors (vec (:errors result)))))

(defn handle-check-skill-update
  [deps skill-id]
  (try
    (let [skill-key (http-value/parse-keyword-id skill-id "skill_id")
          saved     (db/get-skill skill-key)]
      (if-not saved
        (http-common/json-response deps 404 {:error "skill not found"})
        (let [result (if (:skill/imported-from-openclaw? saved)
                       (openclaw-skill/check-openclaw-update! saved)
                       (skill/check-import-update! skill-key))]
          (http-common/json-response deps 200 {:update (skill-update-check->body result)
                                    :skill (skill->body deps (db/get-skill skill-key))}))))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn- curator-skill-summary->body
  [deps summary]
  (update summary :last_used_at #(http-common/instant->str deps %)))

(defn handle-curate-skills
  [deps req]
  (try
    (let [data (or (http-common/read-body deps req) {})
          stale-days (if-let [value (get data "stale_days")]
                       (Long/parseLong (str value))
                       skill/default-stale-days)
          archive-agent-authored? (if (contains? data "archive_agent_authored")
                                    (true? (get data "archive_agent_authored"))
                                    true)
          report (skill/curate-skills! {:stale-days stale-days
                                        :archive-agent-authored? archive-agent-authored?})]
      (http-common/json-response deps 200
                      {:curator {:stale (mapv #(curator-skill-summary->body deps %) (:stale report))
                                 :archived (mapv #(curator-skill-summary->body deps %) (:archived report))
                                 :suggestions (:suggestions report)}
                       :skills (mapv #(skill->body deps %) (db/list-skills))}))
    (catch NumberFormatException _
      (http-common/json-response deps 400 {:error "stale_days must be an integer"}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-import-openclaw-skill
  [deps req]
  (try
    (let [data    (or (http-common/read-body deps req) {})
          source  (http-value/nonblank-str (get data "source"))
          strict? (if (contains? data "strict")
                    (true? (get data "strict"))
                    true)]
      (when-not source
        (throw (ex-info "missing 'source' field" {:field "source"})))
      (let [report (openclaw-skill/import-openclaw-source! source :strict? strict?)
            skill  (db/get-skill (:skill-id report))]
        (http-common/json-response
          deps
          200
          {:import {:status         (some-> (:status report) name)
                    :skill_id       (some-> (:skill-id report) name)
                    :name           (:name report)
                    :warnings       (vec (:warnings report))
                    :ignored_fields (vec (:ignored-fields report))
                    :resources      (mapv (fn [{:keys [path size-bytes]}]
                                            {:path path
                                             :size_bytes size-bytes})
                                          (:resources report))
                    :tool_aliases   (mapv (fn [{:keys [id from to]}]
                                            {:id   (some-> id name)
                                             :from from
                                             :to   to})
                                          (:tool-aliases report))
                    :source         {:format (some-> (get-in report [:source :format]) name)
                                     :path   (get-in report [:source :path])
                                     :url    (get-in report [:source :url])
                                     :name   (get-in report [:source :name])}}
           :skill  (skill->body deps skill)})))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-save-plugin
  [deps req]
  (try
    (let [data     (or (http-common/read-body deps req) {})
          manifest (or (get data "manifest")
                       (get data :manifest)
                       data)
          saved    (plugin/install-plugin! manifest)]
      (http-common/json-response deps 200
                      {:status "saved"
                       :plugin (plugin->admin-body deps saved)
                       :plugins (->> (db/list-plugins)
                                     (into [] (map #(plugin->admin-body deps %)))
                                     sort-by-name)}))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-enable-plugin
  [deps plugin-id enabled?]
  (try
    (let [plugin-id* (http-value/parse-keyword-id plugin-id "plugin_id")
          existing   (db/get-plugin plugin-id*)]
      (if existing
        (let [saved (plugin/enable-plugin! plugin-id* enabled?)]
        (http-common/json-response deps 200
                        {:status (if enabled? "enabled" "disabled")
                         :plugin (plugin->admin-body deps saved)
                         :plugins (->> (db/list-plugins)
                                       (into [] (map #(plugin->admin-body deps %)))
                                       sort-by-name)}))
        (http-common/json-response deps 404 {:error "plugin not found"})))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-skills
  [deps _req]
  (http-common/json-response deps 200 {:skills (mapv #(skill->body deps %) (db/list-skills))}))
