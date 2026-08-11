(ns xia.channel.http.admin.services
  "Service admin HTTP handlers."
  (:require [xia.autonomous.access :as autonomous-access]
            [xia.channel.http.admin.common :as common]
            [xia.db :as db]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.service :as service-proxy]))

(def ^:private service-auth-types #{:bearer :basic :api-key-header :query-param :oauth-account})
(def ^:private service-email-backends #{:imap-smtp})
(def ^:private mail-security-modes #{:ssl :starttls :none})

(defn- parse-auth-type
  [value]
  (let [auth-type (some-> value common/nonblank-str keyword)]
    (when-not (contains? service-auth-types auth-type)
      (throw (ex-info "invalid auth_type"
                      {:field "auth_type"
                       :value value})))
    auth-type))

(defn- parse-optional-service-email-backend
  [value]
  (when-let [backend (some-> value common/nonblank-str keyword)]
    (when-not (contains? service-email-backends backend)
      (throw (ex-info "invalid email_backend"
                      {:field "email_backend"
                       :value value})))
    backend))

(defn- parse-optional-mail-security
  [value field]
  (when-let [security (some-> value common/nonblank-str keyword)]
    (when-not (contains? mail-security-modes security)
      (throw (ex-info (str "invalid " field)
                      {:field field
                       :value value})))
    security))

(defn service->admin-body
  [service]
  (let [oauth-account  (some-> (:service/oauth-account service) db/get-oauth-account)
        runtime-source (when-let [service-id (:service/id service)]
                         (name (runtime-overlay/entity-source :service service-id)))]
    {:id                                (some-> (:service/id service) name)
     :runtime_source                    runtime-source
     :name                              (:service/name service)
     :base_url                          (:service/base-url service)
     :smtp_url                          (:service/smtp-url service)
     :auth_type                         (some-> (:service/auth-type service) name)
     :email_backend                     (some-> (:service/email-backend service) name)
     :auth_header                       (:service/auth-header service)
     :auth_username                     (:service/auth-username service)
     :email_address                     (:service/email-address service)
     :imap_security                     (some-> (:service/imap-security service) name)
     :smtp_security                     (some-> (:service/smtp-security service) name)
     :inbox_folder                      (:service/inbox-folder service)
     :drafts_folder                     (:service/drafts-folder service)
     :sent_folder                       (:service/sent-folder service)
     :archive_folder                    (:service/archive-folder service)
     :trash_folder                      (:service/trash-folder service)
     :oauth_account                     (some-> (:service/oauth-account service) name)
     :oauth_account_name                (:oauth.account/name oauth-account)
     :oauth_account_connected           (boolean (common/nonblank-str (:oauth.account/access-token oauth-account)))
     :oauth_account_autonomous_approved (boolean (and oauth-account
                                                      (autonomous-access/oauth-account-autonomous-approved? oauth-account)))
     :rate_limit_per_minute             (:service/rate-limit-per-minute service)
     :allow_private_network             (boolean (:service/allow-private-network? service))
     :effective_rate_limit_per_minute   (service-proxy/effective-rate-limit-per-minute service)
     :autonomous_approved               (boolean (autonomous-access/service-autonomous-approved? service))
     :enabled                           (boolean (:service/enabled? service))
     :auth_key_configured               (boolean (common/nonblank-str (:service/auth-key service)))}))

(defn handle-save-service
  [deps req]
  (try
    (let [data                   (or (common/read-body deps req) {})
          service-id             (common/parse-keyword-id (get data "id") "id")
          existing               (db/get-service service-id)
          base-url               (common/nonblank-str (get data "base_url"))
          smtp-url               (if (contains? data "smtp_url")
                                   (common/nonblank-str (get data "smtp_url"))
                                   (:service/smtp-url existing))
          name                   (or (common/nonblank-str (get data "name"))
                                     (name service-id))
          auth-type              (parse-auth-type (get data "auth_type"))
          email-backend          (if (contains? data "email_backend")
                                   (parse-optional-service-email-backend (get data "email_backend"))
                                   (:service/email-backend existing))
          entered-auth-key       (common/nonblank-str (get data "auth_key"))
          auth-username          (or (common/nonblank-str (get data "auth_username"))
                                     (:service/auth-username existing))
          email-address          (or (common/nonblank-str (get data "email_address"))
                                     (:service/email-address existing))
          imap-security          (or (parse-optional-mail-security (get data "imap_security") "imap_security")
                                     (:service/imap-security existing))
          smtp-security          (or (parse-optional-mail-security (get data "smtp_security") "smtp_security")
                                     (:service/smtp-security existing))
          inbox-folder           (or (common/nonblank-str (get data "inbox_folder"))
                                     (:service/inbox-folder existing))
          drafts-folder          (or (common/nonblank-str (get data "drafts_folder"))
                                     (:service/drafts-folder existing))
          sent-folder            (or (common/nonblank-str (get data "sent_folder"))
                                     (:service/sent-folder existing))
          archive-folder         (or (common/nonblank-str (get data "archive_folder"))
                                     (:service/archive-folder existing))
          trash-folder           (or (common/nonblank-str (get data "trash_folder"))
                                     (:service/trash-folder existing))
          rate-limit-per-minute  (common/parse-optional-positive-long (get data "rate_limit_per_minute")
                                                                      "rate_limit_per_minute")
          allow-private-network? (when (contains? data "allow_private_network")
                                   (true? (get data "allow_private_network")))
          autonomous-approved?   (when (contains? data "autonomous_approved")
                                   (true? (get data "autonomous_approved")))
          enabled?               (if (contains? data "enabled")
                                   (true? (get data "enabled"))
                                   true)
          oauth-account-id       (when (= :oauth-account auth-type)
                                   (let [value (or (common/nonblank-str (get data "oauth_account"))
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
          entered-header         (common/nonblank-str (get data "auth_header"))
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
      (common/json-response deps 200 {:service (service->admin-body (db/get-service service-id))}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))
