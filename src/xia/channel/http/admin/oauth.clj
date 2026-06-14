(ns xia.channel.http.admin.oauth
  "OAuth account admin HTTP handlers."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [xia.autonomous.access :as autonomous-access]
            [xia.channel.http.admin.common :as common]
            [xia.channel.http.request :as http-request]
            [xia.db :as db]
            [xia.oauth :as oauth]
            [xia.oauth-template :as oauth-template]
            [xia.runtime-overlay :as runtime-overlay])
  (:import [java.util Date]))

(def ^:private oauth-account-connection-modes #{:oauth-flow :manual-token})

(defn- html-response
  [body]
  {:status  200
   :headers {"Content-Type" "text/html; charset=utf-8"}
   :body    body})

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
      (if (or (common/nonblank-str (:oauth.account/authorize-url account))
              (common/nonblank-str (:oauth.account/token-url account))
              (common/nonblank-str (:oauth.account/client-id account)))
        :oauth-flow
        :manual-token)))

(defn oauth-account->admin-body
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
   :client_secret_configured (boolean (common/nonblank-str (:oauth.account/client-secret account)))
   :access_token_configured  (boolean (common/nonblank-str (:oauth.account/access-token account)))
   :refresh_token_configured (boolean (common/nonblank-str (:oauth.account/refresh-token account)))
   :token_type               (:oauth.account/token-type account)
   :autonomous_approved      (boolean (autonomous-access/oauth-account-autonomous-approved? account))
   :connected                (boolean (common/nonblank-str (:oauth.account/access-token account)))
   :expires_at               (common/instant->str deps (:oauth.account/expires-at account))
   :connected_at             (common/instant->str deps (:oauth.account/connected-at account))})

(defn oauth-template->admin-body
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

(defn list-templates
  []
  (oauth-template/list-templates))

(defn- oauth-account-template-service-spec
  [account]
  (when-let [template-id (:oauth.account/provider-template account)]
    (when-let [template (oauth-template/get-template template-id)]
      (let [service-id   (some-> (:service-id template) common/nonblank-str keyword)
            service-name (or (common/nonblank-str (:service-name template))
                             (common/nonblank-str (:name template)))
            api-base-url (common/nonblank-str (:api-base-url template))]
        (when (and service-id api-base-url)
          {:id       service-id
           :name     (or service-name (name service-id))
           :base-url api-base-url})))))

(defn- approve-template-oauth-account!
  [account]
  (if (and (oauth-account-template-service-spec account)
           (not (autonomous-access/oauth-account-autonomous-approved? account)))
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
        (db/save-service! {:id                     id
                           :name                   (or (some-> (:service/name existing) common/nonblank-str)
                                                       name)
                           :base-url               base-url
                           :auth-type              :oauth-account
                           :oauth-account          (:oauth.account/id account)
                           :autonomous-approved?   true})
        (db/get-service id)))))

(defn- auto-managed-template-service-for-oauth-account
  [account]
  (when-let [{:keys [id base-url]} (oauth-account-template-service-spec account)]
    (let [service (db/get-service id)]
      (when (and service
                 (= :oauth-account (:service/auth-type service))
                 (= (:oauth.account/id account) (:service/oauth-account service))
                 (= base-url (common/nonblank-str (:service/base-url service))))
        service))))

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

(defn handle-save-oauth-account
  [deps req]
  (try
    (let [data                  (or (common/read-body deps req) {})
          account-id            (common/parse-keyword-id (get data "id") "id")
          existing              (db/get-oauth-account account-id)
          name                  (or (common/nonblank-str (get data "name"))
                                    (name account-id))
          connection-mode       (let [parsed (if (contains? data "connection_mode")
                                               (some-> (get data "connection_mode") common/nonblank-str keyword)
                                               (when existing
                                                 (oauth-account-connection-mode existing)))]
                                  (when (and parsed
                                             (not (oauth-account-connection-modes parsed)))
                                    (throw (ex-info "unknown connection_mode"
                                                    {:field "connection_mode"
                                                     :value (name parsed)})))
                                  (or parsed :oauth-flow))
          authorize-url         (common/nonblank-str (get data "authorize_url"))
          token-url             (common/nonblank-str (get data "token_url"))
          client-id             (common/nonblank-str (get data "client_id"))
          client-secret         (or (common/nonblank-str (get data "client_secret"))
                                    (:oauth.account/client-secret existing)
                                    "")
          access-token          (or (common/nonblank-str (get data "access_token"))
                                    (:oauth.account/access-token existing))
          refresh-token         (or (common/nonblank-str (get data "refresh_token"))
                                    (:oauth.account/refresh-token existing))
          token-type            (or (common/nonblank-str (get data "token_type"))
                                    (:oauth.account/token-type existing)
                                    "Bearer")
          expires-at            (if (contains? data "expires_at")
                                  (parse-iso-instant (get data "expires_at") "expires_at")
                                  (:oauth.account/expires-at existing))
          connected-at          (cond
                                  (common/nonblank-str (get data "access_token")) (Date.)
                                  access-token (:oauth.account/connected-at existing)
                                  :else nil)
          provider-template-id  (if (contains? data "provider_template")
                                  (some-> (get data "provider_template") common/nonblank-str keyword)
                                  (:oauth.account/provider-template existing))
          scopes                (or (common/nonblank-str (get data "scopes")) "")
          redirect-uri          (common/nonblank-str (get data "redirect_uri"))
          auth-params           (common/parse-json-object-string (get data "auth_params") "auth_params")
          token-params          (common/parse-json-object-string (get data "token_params") "token_params")
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
        (common/json-response deps 200 {:oauth_account (oauth-account->admin-body deps saved-account)})))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-delete-oauth-account
  [deps account-id]
  (try
    (let [oauth-id             (common/parse-keyword-id account-id "oauth_account_id")
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
        (common/json-response deps 404 {:error "oauth account not found"})

        auto-managed-only?
        (do
          (db/remove-service! (:service/id auto-managed-service))
          (db/remove-oauth-account! oauth-id)
          (common/json-response deps 200 {:status "deleted"
                                          :oauth_account_id (name oauth-id)}))

        (or (seq linked-providers) (seq linked-services))
        (common/json-response deps 409 {:error "oauth account is still referenced by a provider or service"})

        :else
        (do
          (db/remove-oauth-account! oauth-id)
          (common/json-response deps 200 {:status "deleted"
                                          :oauth_account_id (name oauth-id)}))))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-start-oauth-connect
  [deps account-id req]
  (try
    (let [oauth-id     (common/parse-keyword-id account-id "oauth_account_id")
          account      (or (db/get-oauth-account oauth-id)
                           (throw (ex-info "unknown oauth_account"
                                           {:field "oauth_account_id"
                                            :value (name oauth-id)})))
          callback-url (str (or (common/request-base-url deps req)
                                (throw (ex-info "cannot determine callback base URL"
                                                {:field "host"})))
                            "/oauth/callback")
          _            (when (= :manual-token (oauth-account-connection-mode account))
                         (throw (ex-info "manual-token connections do not support Connect Now"
                                         {:field "connection_mode"})))
          started      (oauth/start-authorization! oauth-id callback-url)]
      (common/json-response deps 200 {:oauth_account_id   (name oauth-id)
                                      :authorization_url (:authorization-url started)
                                      :redirect_uri      (:redirect-uri started)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-refresh-oauth-account
  [deps account-id]
  (try
    (let [oauth-id          (common/parse-keyword-id account-id "oauth_account_id")
          current-account   (or (db/get-oauth-account oauth-id)
                                (throw (ex-info "unknown oauth_account"
                                                {:field "oauth_account_id"
                                                 :value (name oauth-id)})))
          _                 (when-not (common/nonblank-str (:oauth.account/refresh-token current-account))
                              (throw (ex-info "refresh token is not configured for this connection"
                                              {:field "refresh_token"})))
          _                 (when (= :manual-token (oauth-account-connection-mode current-account))
                              (throw (ex-info "manual-token connections do not support Refresh"
                                              {:field "connection_mode"})))
          refreshed-account (oauth/refresh-account! oauth-id)]
      (common/json-response deps 200 {:oauth_account (oauth-account->admin-body deps refreshed-account)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

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
