(ns xia.channel.http.admin.providers
  "LLM provider admin HTTP handlers."
  (:require [clojure.string :as str]
            [xia.channel.http.admin.common :as common]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.llm-provider-template :as llm-provider-template]
            [xia.oauth :as oauth]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.setup :as setup]))

(def ^:private provider-access-modes #{:local :api :account})
(def ^:private provider-credential-sources #{:none :api-key :oauth-account})

(defn- unique-provider-api-key
  [providers]
  (let [api-keys (->> providers
                      (keep (comp common/nonblank-str :llm.provider/api-key))
                      distinct
                      vec)]
    (when (= 1 (count api-keys))
      (first api-keys))))

(defn- infer-reusable-provider-api-key
  [{:keys [provider-id template-id base-url]}]
  (let [normalized-base-url (common/normalize-base-url base-url)
        providers           (->> (db/list-providers)
                                 (remove #(= provider-id (:llm.provider/id %)))
                                 (filter #(= :api-key (llm/provider-credential-source %))))]
    (or
      (when (and template-id normalized-base-url)
        (unique-provider-api-key
          (filter #(and (= template-id (:llm.provider/template %))
                        (= normalized-base-url
                           (common/normalize-base-url (:llm.provider/base-url %))))
                  providers)))
      (when template-id
        (unique-provider-api-key
          (filter #(= template-id (:llm.provider/template %))
                  providers)))
      (when normalized-base-url
        (unique-provider-api-key
          (filter #(= normalized-base-url
                      (common/normalize-base-url (:llm.provider/base-url %)))
                  providers))))))

(defn- parse-provider-workloads
  [value]
  (let [entries (cond
                  (nil? value) []
                  (sequential? value) value
                  :else (str/split (str value) #","))]
    (->> entries
         (map common/nonblank-str)
         (remove nil?)
         distinct
         (mapv (fn [entry]
                 (let [workload (keyword entry)]
                   (when-not (llm/known-workload? workload)
                     (throw (ex-info "invalid workload"
                                     {:field "workloads"
                                      :value entry})))
                   workload))))))

(defn- parse-provider-access-mode
  [value]
  (let [access-mode (some-> value common/nonblank-str keyword)]
    (when-not (contains? provider-access-modes access-mode)
      (throw (ex-info "invalid access_mode"
                      {:field "access_mode"
                       :value value})))
    access-mode))

(defn- parse-provider-credential-source
  [value]
  (let [auth-type (some-> value common/nonblank-str keyword)]
    (when-not (contains? provider-credential-sources auth-type)
      (throw (ex-info "invalid credential_source"
                      {:field "credential_source"
                       :value value})))
    auth-type))

(defn provider->admin-body
  [provider]
  (let [provider-id       (some-> (:llm.provider/id provider) name)
        runtime-source    (when-let [provider-key (:llm.provider/id provider)]
                            (name (runtime-overlay/entity-source :provider provider-key)))
        access-mode       (llm/provider-access-mode provider)
        credential-source (llm/provider-credential-source provider)
        oauth-account     (some-> (:llm.provider/oauth-account provider) db/get-oauth-account)
        health            (llm/provider-health-summary (:llm.provider/id provider))]
    {:id                          provider-id
     :runtime_source              runtime-source
     :name                        (:llm.provider/name provider)
     :template                    (some-> (:llm.provider/template provider) name)
     :access_mode                 (some-> access-mode name)
     :credential_source           (some-> credential-source name)
     :auth_type                   (some-> credential-source name)
     :oauth_account               (some-> (:llm.provider/oauth-account provider) name)
     :oauth_account_name          (:oauth.account/name oauth-account)
     :oauth_account_connected     (boolean (common/nonblank-str (:oauth.account/access-token oauth-account)))
     :base_url                    (:llm.provider/base-url provider)
     :model                       (:llm.provider/model provider)
     :workloads                   (->> (:llm.provider/workloads provider)
                                       (map name)
                                       sort
                                       vec)
     :vision                      (boolean (:llm.provider/vision? provider))
     :allow_private_network       (boolean (:llm.provider/allow-private-network? provider))
     :context_window              (:llm.provider/context-window provider)
     :context_window_source       (some-> (:llm.provider/context-window-source provider) name)
     :system_prompt_budget        (:llm.provider/system-prompt-budget provider)
     :history_budget              (:llm.provider/history-budget provider)
     :recommended_system_prompt_budget
     (:llm.provider/recommended-system-prompt-budget provider)
     :recommended_history_budget
     (:llm.provider/recommended-history-budget provider)
     :recommended_input_budget_cap
     (:llm.provider/recommended-input-budget-cap provider)
     :rate_limit_per_minute       (:llm.provider/rate-limit-per-minute provider)
     :effective_rate_limit_per_minute (llm/effective-rate-limit-per-minute provider)
     :health_status               (name (:status health))
     :health_failures             (:consecutive-failures health)
     :health_cooldown_ms          (:cooldown-remaining-ms health)
     :health_last_error           (:last-error health)
     :default                     (boolean (:llm.provider/default? provider))
     :api_key_configured          (boolean (common/nonblank-str (:llm.provider/api-key provider)))}))

(defn template->admin-body
  [template]
  (let [access-modes (->> (or (:access-modes template) [])
                          (mapv (fn [mode]
                                  {:id                 (some-> (:id mode) name)
                                   :label              (:label mode)
                                   :description        (:description mode)
                                   :credential_sources (->> (or (:credential-sources mode) [])
                                                            (map name)
                                                            vec)
                                   :default            (boolean (:default? mode))})))
        auth-types   (->> access-modes
                          (mapcat :credential_sources)
                          distinct
                          vec)]
    {:id                       (some-> (:id template) name)
     :name                     (:name template)
     :description              (:description template)
     :category                 (some-> (:category template) name)
     :base_url                 (:base-url template)
     :model_suggestion         (:model-suggestion template)
     :account_url              (:account-url template)
     :api_key_url              (:api-key-url template)
     :docs_url                 (:docs-url template)
     :install_url              (:install-url template)
     :access_modes             access-modes
     :auth_types               auth-types
     :oauth_provider_templates (->> (or (:oauth-provider-templates template) [])
                                    (map name)
                                    vec)
     :oauth_setup_note         (:oauth-setup-note template)
     :sign_in_options          (->> (or (:sign-in-options template) [])
                                    (map name)
                                    vec)
     :notes                    (:notes template)}))

(defn- provider-request-context
  [body]
  (let [provider-id (when (contains? body "provider_id")
                      (some-> (get body "provider_id") common/nonblank-str keyword))
        provider    (when provider-id
                      (or (db/get-provider provider-id)
                          (throw (ex-info "unknown provider_id"
                                          {:field "provider_id"
                                           :value (name provider-id)}))))
        base-url    (or (get body "base_url")
                        (:llm.provider/base-url provider))
        api-key     (or (common/nonblank-str (get body "api_key"))
                        (common/nonblank-str (:llm.provider/api-key provider)))
        auth-header (when (and provider
                               (nil? api-key)
                               (= :oauth-account (llm/provider-credential-source provider)))
                      (when-let [account-id (:llm.provider/oauth-account provider)]
                        (oauth/oauth-header (oauth/ensure-account-ready! account-id))))]
    {:provider provider
     :base-url base-url
     :api-key api-key
     :auth-header auth-header}))

(defn handle-fetch-provider-models
  [deps req]
  (try
    (let [{:keys [base-url api-key auth-header]} (provider-request-context
                                                   (common/read-body deps req))]
      (when-not (common/nonblank-str base-url)
        (throw (ex-info "base_url is required" {:type :http/bad-request})))
      (let [models (llm/fetch-provider-models {:base-url    base-url
                                               :api-key     api-key
                                               :auth-header auth-header})]
        (common/json-response deps 200 {:models (or models [])})))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))
    (catch Exception e
      (common/json-response deps 502 {:error (str "Failed to fetch models: " (.getMessage e))}))))

(defn handle-fetch-provider-model-metadata
  [deps req]
  (try
    (let [body                              (common/read-body deps req)
          {:keys [base-url api-key auth-header]} (provider-request-context body)
          model-id                          (get body "model")]
      (when-not (common/nonblank-str base-url)
        (throw (ex-info "base_url is required" {:type :http/bad-request})))
      (when-not (common/nonblank-str model-id)
        (throw (ex-info "model is required" {:type :http/bad-request})))
      (let [{:keys [id vision? vision-source
                    context-window context-window-source
                    recommended-system-prompt-budget
                    recommended-history-budget
                    recommended-input-budget-cap]}
            (llm/fetch-provider-model-metadata {:base-url    base-url
                                                :api-key     api-key
                                                :auth-header auth-header
                                                :model       model-id})]
        (common/json-response
          deps
          200
          {:model (cond-> {:id            id
                           :vision        (boolean vision?)
                           :vision_source (some-> vision-source name)}
                    context-window
                    (assoc :context_window context-window
                           :context_window_source (some-> context-window-source name))

                    recommended-system-prompt-budget
                    (assoc :recommended_system_prompt_budget recommended-system-prompt-budget)

                    recommended-history-budget
                    (assoc :recommended_history_budget recommended-history-budget)

                    recommended-input-budget-cap
                    (assoc :recommended_input_budget_cap recommended-input-budget-cap))})))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))
    (catch Exception e
      (common/json-response deps 502 {:error (str "Failed to fetch model metadata: " (.getMessage e))}))))

(defn handle-save-provider
  [deps req]
  (try
    (let [data                       (or (common/read-body deps req) {})
          provider-id                (common/parse-keyword-id (get data "id") "id")
          existing-provider          (db/get-provider provider-id)
          base-url                   (common/nonblank-str (get data "base_url"))
          model                      (common/nonblank-str (get data "model"))
          name                       (or (common/nonblank-str (get data "name"))
                                         (name provider-id))
          api-key                    (common/nonblank-str (get data "api_key"))
          reuse-api-key-provider-id  (if (contains? data "reuse_api_key_provider_id")
                                       (some-> (get data "reuse_api_key_provider_id") common/nonblank-str keyword)
                                       nil)
          template-id                (if (contains? data "template")
                                       (some-> (get data "template") common/nonblank-str keyword)
                                       nil)
          access-mode                (if (contains? data "access_mode")
                                       (parse-provider-access-mode (get data "access_mode"))
                                       nil)
          credential-source          (cond
                                       (contains? data "credential_source")
                                       (parse-provider-credential-source (get data "credential_source"))

                                       (contains? data "auth_type")
                                       (parse-provider-credential-source (get data "auth_type"))

                                       :else
                                       nil)
          oauth-account-id           (if (contains? data "oauth_account")
                                       (some-> (get data "oauth_account") common/nonblank-str keyword)
                                       nil)
          vision?                    (when (contains? data "vision")
                                       (true? (get data "vision")))
          allow-private-network?     (when (contains? data "allow_private_network")
                                       (true? (get data "allow_private_network")))
          workloads                  (when (contains? data "workloads")
                                       (parse-provider-workloads (get data "workloads")))
          system-prompt-budget       (common/parse-optional-positive-long
                                       (get data "system_prompt_budget")
                                       "system_prompt_budget")
          history-budget             (common/parse-optional-positive-long
                                       (get data "history_budget")
                                       "history_budget")
          context-window             (common/parse-optional-positive-long
                                       (get data "context_window")
                                       "context_window")
          context-window-source      (when-let [source (common/nonblank-str (get data "context_window_source"))]
                                       (keyword source))
          recommended-system-budget  (common/parse-optional-positive-long
                                       (get data "recommended_system_prompt_budget")
                                       "recommended_system_prompt_budget")
          recommended-history-budget (common/parse-optional-positive-long
                                       (get data "recommended_history_budget")
                                       "recommended_history_budget")
          recommended-input-budget   (common/parse-optional-positive-long
                                       (get data "recommended_input_budget_cap")
                                       "recommended_input_budget_cap")
          rate-limit-per-minute      (common/parse-optional-positive-long
                                       (get data "rate_limit_per_minute")
                                       "rate_limit_per_minute")
          make-default               (true? (get data "default"))
          has-default?               (some? (db/get-default-provider))
          reused-api-key             (when reuse-api-key-provider-id
                                       (let [provider (db/get-provider reuse-api-key-provider-id)]
                                         (when-not provider
                                           (throw (ex-info "unknown reuse_api_key_provider_id"
                                                           {:field "reuse_api_key_provider_id"
                                                            :value (name reuse-api-key-provider-id)})))
                                         (or (common/nonblank-str (:llm.provider/api-key provider))
                                             (throw (ex-info "reuse_api_key_provider_id does not have a stored API key"
                                                             {:field "reuse_api_key_provider_id"
                                                              :value (name reuse-api-key-provider-id)})))))
          inferred-api-key           (when (and (= credential-source :api-key)
                                                (nil? api-key)
                                                (nil? reused-api-key)
                                                (nil? (common/nonblank-str (:llm.provider/api-key existing-provider))))
                                       (infer-reusable-provider-api-key {:provider-id provider-id
                                                                         :template-id template-id
                                                                         :base-url    base-url}))
          effective-api-key          (or api-key reused-api-key inferred-api-key)
          normalized-access-mode     (llm/provider-access-mode {:access-mode       access-mode
                                                                :credential-source credential-source
                                                                :template          template-id
                                                                :base-url          base-url
                                                                :oauth-account     oauth-account-id
                                                                :api-key           effective-api-key})]
      (when-not base-url
        (throw (ex-info "missing 'base_url' field" {:field "base_url"})))
      (when-not model
        (throw (ex-info "missing 'model' field" {:field "model"})))
      (when (and template-id
                 (nil? (llm-provider-template/get-template template-id)))
        (throw (ex-info "unknown template"
                        {:field "template"
                         :value (name template-id)})))
      (when (and (= credential-source :oauth-account)
                 (nil? oauth-account-id))
        (throw (ex-info "oauth_account is required for oauth-account credential_source"
                        {:field "oauth_account"})))
      (when (contains? data "browser_session")
        (throw (ex-info "browser_session is no longer supported; use API key or OAuth API sign-in."
                        {:field "browser_session"})))
      (when (and oauth-account-id
                 (nil? (db/get-oauth-account oauth-account-id)))
        (throw (ex-info "unknown oauth_account"
                        {:field "oauth_account"
                         :value (name oauth-account-id)})))
      (db/upsert-provider! (cond-> {:id                    provider-id
                                    :name                  name
                                    :base-url              base-url
                                    :model                 model
                                    :system-prompt-budget  system-prompt-budget
                                    :history-budget        history-budget
                                    :rate-limit-per-minute rate-limit-per-minute}
                             (contains? data "context_window")
                             (assoc :context-window context-window)
                             (contains? data "context_window_source")
                             (assoc :context-window-source context-window-source)
                             (contains? data "recommended_system_prompt_budget")
                             (assoc :recommended-system-prompt-budget recommended-system-budget)
                             (contains? data "recommended_history_budget")
                             (assoc :recommended-history-budget recommended-history-budget)
                             (contains? data "recommended_input_budget_cap")
                             (assoc :recommended-input-budget-cap recommended-input-budget)
                             (contains? data "template")
                             (assoc :template template-id)
                             (contains? data "access_mode")
                             (assoc :access-mode normalized-access-mode)
                             (or (contains? data "credential_source")
                                 (contains? data "auth_type"))
                             (assoc :credential-source credential-source
                                    :auth-type credential-source)
                             (contains? data "oauth_account")
                             (assoc :oauth-account oauth-account-id)
                             (contains? data "vision")
                             (assoc :vision? vision?)
                             (contains? data "allow_private_network")
                             (assoc :allow-private-network? allow-private-network?)
                             (contains? data "workloads")
                             (assoc :workloads workloads)
                             effective-api-key
                             (assoc :api-key effective-api-key)))
      (when (or make-default (not has-default?))
        (db/set-default-provider! provider-id))
      (when (setup/needs-setup?)
        (db/set-config! :setup/complete "true"))
      (common/json-response deps 200 {:provider (provider->admin-body (db/get-provider provider-id))}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-delete-provider
  [deps req]
  (try
    (let [data        (or (common/read-body deps req) {})
          provider-id (common/parse-keyword-id (get data "id") "id")]
      (when-not (db/get-provider provider-id)
        (throw (ex-info "provider not found" {:type :http/not-found :field "id"})))
      (db/delete-provider! provider-id)
      (common/json-response deps 200 {:deleted (name provider-id)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))
