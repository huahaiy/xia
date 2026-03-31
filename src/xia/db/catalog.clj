(ns xia.db.catalog
  "Catalog and registry persistence helpers for skills, credentials, services, tools,
   managed child instances, and OAuth accounts."
  (:require [clojure.string :as str])
  (:import [java.net URI]))

(defn- q*
  [deps query & inputs]
  (apply (:q deps) query inputs))

(defn- transact!*
  [deps tx-data]
  ((:transact! deps) tx-data))

(defn- raw-entity*
  [deps eid]
  ((:raw-entity deps) eid))

(defn- decrypt-entity*
  [deps entity-map]
  ((:decrypt-entity deps) entity-map))

(def ^:private service-loopback-hosts #{"localhost" "127.0.0.1" "::1" "[::1]"})

(defn- loopback-service-base-url?
  [base-url]
  (try
    (let [uri    (URI. (or base-url ""))
          scheme (some-> (.getScheme uri) str/lower-case)
          host   (some-> (.getHost uri) str/lower-case)]
      (and (= "http" scheme)
           (contains? service-loopback-hosts host)))
    (catch Exception _
      false)))

(defn- validate-service-base-url!
  [base-url allow-private-network?]
  (when (str/blank? (or base-url ""))
    (throw (ex-info "Service base URL is required"
                    {:field "base_url"})))
  (let [uri (try
              (URI. base-url)
              (catch Exception e
                (throw (ex-info "Service base URL must be a valid absolute URL"
                                {:field "base_url"
                                 :value base-url}
                                e))))
        scheme (some-> (.getScheme uri) str/lower-case)
        host   (.getHost uri)]
    (when-not (and (some? host)
                   (or (= "https" scheme)
                       (and allow-private-network?
                            (loopback-service-base-url? base-url))))
      (throw (ex-info "Service base URL must use HTTPS (loopback HTTP is allowed only when private-network access is enabled)"
                      {:field "base_url"
                       :value base-url
                       :allow-private-network? (boolean allow-private-network?)})))
    base-url))

(defn install-skill!
  [deps
   {:keys [id name description content doc version tags
           source-format source-path source-url source-name
           import-warnings
           imported-from-openclaw?]}]
  (transact!*
    deps
    [(cond-> {:skill/id           id
              :skill/name         (or name (clojure.core/name id))
              :skill/description  (or description "")
              :skill/content      (or content "")
              :skill/version      (or version "0.1.0")
              :skill/tags         (or tags #{})
              :skill/enabled?     true
              :skill/installed-at (java.util.Date.)}
       source-format (assoc :skill/source-format source-format)
       source-path (assoc :skill/source-path source-path)
       source-url (assoc :skill/source-url source-url)
       source-name (assoc :skill/source-name source-name)
       (seq import-warnings) (assoc :skill/import-warnings import-warnings)
       (some? imported-from-openclaw?) (assoc :skill/imported-from-openclaw? imported-from-openclaw?)
       doc (assoc :skill/doc doc))]))

(defn save-skill!
  [deps
   {:keys [id name description content doc version tags enabled? installed-at
           source-format source-path source-url source-name
           import-warnings imported-from-openclaw?
           clear-doc?]}]
  (let [eid      (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :skill/id ?id]] id))
        existing (when eid (raw-entity* deps eid))
        new-tags (if (some? tags) tags (or (:skill/tags existing) #{}))
        base-map (cond-> {:skill/id           id
                          :skill/name         (or name
                                                  (:skill/name existing)
                                                  (clojure.core/name id))
                          :skill/description  (or description
                                                  (:skill/description existing)
                                                  "")
                          :skill/content      (or content
                                                  (:skill/content existing)
                                                  "")
                          :skill/version      (or version
                                                  (:skill/version existing)
                                                  "0.1.0")
                          :skill/tags         new-tags
                          :skill/enabled?     (if (some? enabled?)
                                                enabled?
                                                (if (contains? existing :skill/enabled?)
                                                  (:skill/enabled? existing)
                                                  true))
                          :skill/installed-at (or installed-at
                                                  (:skill/installed-at existing)
                                                  (java.util.Date.))}
                   (or source-format
                       (:skill/source-format existing))
                   (assoc :skill/source-format (or source-format
                                                   (:skill/source-format existing)))
                   (or source-path
                       (:skill/source-path existing))
                   (assoc :skill/source-path (or source-path
                                                 (:skill/source-path existing)))
                   (or source-url
                       (:skill/source-url existing))
                   (assoc :skill/source-url (or source-url
                                                (:skill/source-url existing)))
                   (or source-name
                       (:skill/source-name existing))
                   (assoc :skill/source-name (or source-name
                                                 (:skill/source-name existing)))
                   (or (seq import-warnings)
                       (seq (:skill/import-warnings existing)))
                   (assoc :skill/import-warnings (or import-warnings
                                                     (:skill/import-warnings existing)))
                   (or (some? imported-from-openclaw?)
                       (contains? existing :skill/imported-from-openclaw?))
                   (assoc :skill/imported-from-openclaw? (if (some? imported-from-openclaw?)
                                                           imported-from-openclaw?
                                                           (:skill/imported-from-openclaw? existing)))
                   doc
                   (assoc :skill/doc doc))
        tx-data  (vec
                   (concat
                     (for [tag (:skill/tags existing)
                           :when (not (contains? new-tags tag))]
                       [:db/retract eid :skill/tags tag])
                     (when (and clear-doc? eid (contains? existing :skill/doc))
                       [[:db/retract eid :skill/doc (:skill/doc existing)]])
                     [base-map]))]
    (transact!* deps tx-data)))

(defn get-skill
  [deps skill-id]
  (let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :skill/id ?id]] skill-id))]
    (when eid
      (raw-entity* deps eid))))

(defn list-skills
  [deps]
  (let [eids (q* deps '[:find ?e :where [?e :skill/id _]])]
    (mapv #(raw-entity* deps (first %)) eids)))

(defn remove-skill!
  [deps skill-id]
  (when-let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :skill/id ?id]] skill-id))]
    (transact!* deps [[:db/retractEntity eid]])))

(defn enable-skill!
  [deps skill-id enabled?]
  (transact!* deps [{:skill/id skill-id :skill/enabled? enabled?}]))

(defn find-skills-by-tags
  [deps tags]
  (let [eids (q* deps '[:find ?e
                        :in $ [?tag ...]
                        :where
                        [?e :skill/tags ?tag]
                        [?e :skill/enabled? true]]
                  tags)]
    (mapv #(raw-entity* deps (first %)) eids)))

(defn save-site-cred!
  [deps
   {:keys [id name login-url username-field password-field
           username password form-selector extra-fields autonomous-approved?]}]
  (let [eid     (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :site-cred/id ?id]] id))
        current (when eid (raw-entity* deps eid))
        tx-data (cond-> [{:site-cred/id             id
                          :site-cred/name           (or name (clojure.core/name id))
                          :site-cred/login-url      login-url
                          :site-cred/username-field (or username-field "username")
                          :site-cred/password-field (or password-field "password")
                          :site-cred/username       (or username "")
                          :site-cred/password       (or password "")
                          :site-cred/autonomous-approved? (if (some? autonomous-approved?)
                                                            autonomous-approved?
                                                            (if (contains? current :site-cred/autonomous-approved?)
                                                              (:site-cred/autonomous-approved? current)
                                                              true))}]
                  form-selector
                  (update 0 assoc :site-cred/form-selector form-selector)

                  extra-fields
                  (update 0 assoc :site-cred/extra-fields extra-fields)

                  (and eid
                       (nil? form-selector)
                       (contains? current :site-cred/form-selector))
                  (conj [:db/retract eid
                         :site-cred/form-selector
                         (:site-cred/form-selector current)])

                  (and eid
                       (nil? extra-fields)
                       (contains? current :site-cred/extra-fields))
                  (conj [:db/retract eid
                         :site-cred/extra-fields
                         (:site-cred/extra-fields current)]))]
    (transact!* deps tx-data)))

(defn register-site-cred!
  [deps site-cred]
  (save-site-cred! deps site-cred))

(defn get-site-cred
  [deps site-id]
  (let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :site-cred/id ?id]] site-id))]
    (when eid
      (decrypt-entity* deps (raw-entity* deps eid)))))

(defn list-site-creds
  [deps]
  (let [eids (q* deps '[:find ?e :where [?e :site-cred/id _]])]
    (mapv #(decrypt-entity* deps (raw-entity* deps (first %))) eids)))

(defn remove-site-cred!
  [deps site-id]
  (when-let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :site-cred/id ?id]] site-id))]
    (transact!* deps [[:db/retractEntity eid]])))

(defn save-service!
  [deps
   {:keys [id name base-url auth-type auth-key auth-header oauth-account enabled?
           autonomous-approved?] :as service}]
  (let [allow-private-network?     (or (:service/allow-private-network? service)
                                       (:allow-private-network? service))
        base-url                   (validate-service-base-url! base-url allow-private-network?)
        eid                        (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :service/id ?id]] id))
        current                    (when eid (raw-entity* deps eid))
        rate-limit-per-minute      (or (:service/rate-limit-per-minute service)
                                       (:rate-limit-per-minute service))
        has-rate-limit?            (or (contains? service :service/rate-limit-per-minute)
                                       (contains? service :rate-limit-per-minute))
        has-allow-private-network? (or (contains? service :service/allow-private-network?)
                                       (contains? service :allow-private-network?))
        tx-data                    (cond-> [{:service/id        id
                                             :service/name      (or name (clojure.core/name id))
                                             :service/base-url  base-url
                                             :service/auth-type (or auth-type :bearer)
                                             :service/auth-key  (or auth-key "")
                                             :service/autonomous-approved? (if (some? autonomous-approved?)
                                                                            autonomous-approved?
                                                                            (if (contains? current :service/autonomous-approved?)
                                                                              (:service/autonomous-approved? current)
                                                                              true))
                                             :service/enabled?  (if (nil? enabled?) true enabled?)}]
                                     auth-header
                                     (update 0 assoc :service/auth-header auth-header)

                                     oauth-account
                                     (update 0 assoc :service/oauth-account oauth-account)

                                     (and has-rate-limit?
                                          (some? rate-limit-per-minute))
                                     (update 0 assoc :service/rate-limit-per-minute rate-limit-per-minute)

                                     has-allow-private-network?
                                     (update 0 assoc :service/allow-private-network? (boolean allow-private-network?))

                                     (and eid
                                          (nil? auth-header)
                                          (contains? current :service/auth-header))
                                     (conj [:db/retract eid
                                            :service/auth-header
                                            (:service/auth-header current)])

                                     (and eid
                                          (nil? oauth-account)
                                          (contains? current :service/oauth-account))
                                     (conj [:db/retract eid
                                            :service/oauth-account
                                            (:service/oauth-account current)])

                                     (and eid
                                          has-rate-limit?
                                          (nil? rate-limit-per-minute)
                                          (contains? current :service/rate-limit-per-minute))
                                     (conj [:db/retract eid
                                            :service/rate-limit-per-minute
                                            (:service/rate-limit-per-minute current)]))]
    (transact!* deps tx-data)))

(defn register-service!
  [deps service]
  (save-service! deps service))

(defn get-service
  [deps service-id]
  (let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :service/id ?id]] service-id))]
    (when eid
      (decrypt-entity* deps (raw-entity* deps eid)))))

(defn list-services
  [deps]
  (let [eids (q* deps '[:find ?e :where [?e :service/id _]])]
    (mapv #(decrypt-entity* deps (raw-entity* deps (first %))) eids)))

(defn remove-service!
  [deps service-id]
  (when-let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :service/id ?id]] service-id))]
    (transact!* deps [[:db/retractEntity eid]])))

(defn enable-service!
  [deps service-id enabled?]
  (transact!* deps [{:service/id service-id :service/enabled? enabled?}]))

(defn save-managed-child!
  [deps
   {:keys [id name service-id service-name base-url template-instance state pid
           log-path started-at exited-at exit-code]}]
  (let [eid     (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :managed.child/id ?id]] id))
        current (when eid (raw-entity* deps eid))
        now     (java.util.Date.)
        tx-data (cond-> [{:managed.child/id         id
                          :managed.child/name       (or name
                                                        (:managed.child/name current)
                                                        (clojure.core/name id))
                          :managed.child/created-at (or (:managed.child/created-at current)
                                                        now)
                          :managed.child/updated-at now}]
                  service-id
                  (update 0 assoc :managed.child/service-id service-id)

                  service-name
                  (update 0 assoc :managed.child/service-name service-name)

                  base-url
                  (update 0 assoc :managed.child/base-url base-url)

                  template-instance
                  (update 0 assoc :managed.child/template-instance template-instance)

                  state
                  (update 0 assoc :managed.child/state state)

                  (some? pid)
                  (update 0 assoc :managed.child/pid (long pid))

                  log-path
                  (update 0 assoc :managed.child/log-path log-path)

                  started-at
                  (update 0 assoc :managed.child/started-at started-at)

                  exited-at
                  (update 0 assoc :managed.child/exited-at exited-at)

                  (some? exit-code)
                  (update 0 assoc :managed.child/exit-code (long exit-code))

                  (and eid
                       (nil? service-id)
                       (contains? current :managed.child/service-id))
                  (conj [:db/retract eid
                         :managed.child/service-id
                         (:managed.child/service-id current)])

                  (and eid
                       (nil? service-name)
                       (contains? current :managed.child/service-name))
                  (conj [:db/retract eid
                         :managed.child/service-name
                         (:managed.child/service-name current)])

                  (and eid
                       (nil? base-url)
                       (contains? current :managed.child/base-url))
                  (conj [:db/retract eid
                         :managed.child/base-url
                         (:managed.child/base-url current)])

                  (and eid
                       (nil? template-instance)
                       (contains? current :managed.child/template-instance))
                  (conj [:db/retract eid
                         :managed.child/template-instance
                         (:managed.child/template-instance current)])

                  (and eid
                       (nil? state)
                       (contains? current :managed.child/state))
                  (conj [:db/retract eid
                         :managed.child/state
                         (:managed.child/state current)])

                  (and eid
                       (nil? pid)
                       (contains? current :managed.child/pid))
                  (conj [:db/retract eid
                         :managed.child/pid
                         (:managed.child/pid current)])

                  (and eid
                       (nil? log-path)
                       (contains? current :managed.child/log-path))
                  (conj [:db/retract eid
                         :managed.child/log-path
                         (:managed.child/log-path current)])

                  (and eid
                       (nil? started-at)
                       (contains? current :managed.child/started-at))
                  (conj [:db/retract eid
                         :managed.child/started-at
                         (:managed.child/started-at current)])

                  (and eid
                       (nil? exited-at)
                       (contains? current :managed.child/exited-at))
                  (conj [:db/retract eid
                         :managed.child/exited-at
                         (:managed.child/exited-at current)])

                  (and eid
                       (nil? exit-code)
                       (contains? current :managed.child/exit-code))
                  (conj [:db/retract eid
                         :managed.child/exit-code
                         (:managed.child/exit-code current)]))]
    (transact!* deps tx-data)))

(defn get-managed-child
  [deps instance-id]
  (let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :managed.child/id ?id]] instance-id))]
    (when eid
      (raw-entity* deps eid))))

(defn list-managed-children
  [deps]
  (let [eids (q* deps '[:find ?e :where [?e :managed.child/id _]])]
    (mapv #(raw-entity* deps (first %)) eids)))

(defn remove-managed-child!
  [deps instance-id]
  (when-let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :managed.child/id ?id]] instance-id))]
    (transact!* deps [[:db/retractEntity eid]])))

(defn save-oauth-account!
  [deps
   {:keys [id name connection-mode authorize-url token-url client-id client-secret scopes
           provider-template redirect-uri auth-params token-params access-token refresh-token
           token-type expires-at connected-at autonomous-approved?]}]
  (let [eid     (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :oauth.account/id ?id]] id))
        current (when eid (raw-entity* deps eid))
        now     (java.util.Date.)
        tx-data (cond-> [{:oauth.account/id            id
                          :oauth.account/name          (or name (clojure.core/name id))
                          :oauth.account/scopes        (or scopes "")
                          :oauth.account/autonomous-approved? (if (some? autonomous-approved?)
                                                                autonomous-approved?
                                                                (if (contains? current :oauth.account/autonomous-approved?)
                                                                  (:oauth.account/autonomous-approved? current)
                                                                  true))
                          :oauth.account/updated-at    now}]
                  connection-mode
                  (update 0 assoc :oauth.account/connection-mode connection-mode)

                  authorize-url
                  (update 0 assoc :oauth.account/authorize-url authorize-url)

                  token-url
                  (update 0 assoc :oauth.account/token-url token-url)

                  client-id
                  (update 0 assoc :oauth.account/client-id client-id)

                  (some? client-secret)
                  (update 0 assoc :oauth.account/client-secret (or client-secret ""))

                  provider-template
                  (update 0 assoc :oauth.account/provider-template provider-template)

                  redirect-uri
                  (update 0 assoc :oauth.account/redirect-uri redirect-uri)

                  auth-params
                  (update 0 assoc :oauth.account/auth-params auth-params)

                  token-params
                  (update 0 assoc :oauth.account/token-params token-params)

                  access-token
                  (update 0 assoc :oauth.account/access-token access-token)

                  refresh-token
                  (update 0 assoc :oauth.account/refresh-token refresh-token)

                  token-type
                  (update 0 assoc :oauth.account/token-type token-type)

                  expires-at
                  (update 0 assoc :oauth.account/expires-at expires-at)

                  connected-at
                  (update 0 assoc :oauth.account/connected-at connected-at)

                  (and eid
                       (nil? connection-mode)
                       (contains? current :oauth.account/connection-mode))
                  (conj [:db/retract eid
                         :oauth.account/connection-mode
                         (:oauth.account/connection-mode current)])

                  (and eid
                       (nil? authorize-url)
                       (contains? current :oauth.account/authorize-url))
                  (conj [:db/retract eid
                         :oauth.account/authorize-url
                         (:oauth.account/authorize-url current)])

                  (and eid
                       (nil? token-url)
                       (contains? current :oauth.account/token-url))
                  (conj [:db/retract eid
                         :oauth.account/token-url
                         (:oauth.account/token-url current)])

                  (and eid
                       (nil? client-id)
                       (contains? current :oauth.account/client-id))
                  (conj [:db/retract eid
                         :oauth.account/client-id
                         (:oauth.account/client-id current)])

                  (and eid
                       (nil? provider-template)
                       (contains? current :oauth.account/provider-template))
                  (conj [:db/retract eid
                         :oauth.account/provider-template
                         (:oauth.account/provider-template current)])

                  (and eid
                       (nil? redirect-uri)
                       (contains? current :oauth.account/redirect-uri))
                  (conj [:db/retract eid
                         :oauth.account/redirect-uri
                         (:oauth.account/redirect-uri current)])

                  (and eid
                       (nil? auth-params)
                       (contains? current :oauth.account/auth-params))
                  (conj [:db/retract eid
                         :oauth.account/auth-params
                         (:oauth.account/auth-params current)])

                  (and eid
                       (nil? token-params)
                       (contains? current :oauth.account/token-params))
                  (conj [:db/retract eid
                         :oauth.account/token-params
                         (:oauth.account/token-params current)])

                  (and eid
                       (nil? access-token)
                       (contains? current :oauth.account/access-token))
                  (conj [:db/retract eid
                         :oauth.account/access-token
                         (:oauth.account/access-token current)])

                  (and eid
                       (nil? refresh-token)
                       (contains? current :oauth.account/refresh-token))
                  (conj [:db/retract eid
                         :oauth.account/refresh-token
                         (:oauth.account/refresh-token current)])

                  (and eid
                       (nil? token-type)
                       (contains? current :oauth.account/token-type))
                  (conj [:db/retract eid
                         :oauth.account/token-type
                         (:oauth.account/token-type current)])

                  (and eid
                       (nil? expires-at)
                       (contains? current :oauth.account/expires-at))
                  (conj [:db/retract eid
                         :oauth.account/expires-at
                         (:oauth.account/expires-at current)])

                  (and eid
                       (nil? connected-at)
                       (contains? current :oauth.account/connected-at))
                  (conj [:db/retract eid
                         :oauth.account/connected-at
                         (:oauth.account/connected-at current)]))]
    (transact!* deps tx-data)))

(defn register-oauth-account!
  [deps oauth-account]
  (save-oauth-account! deps oauth-account))

(defn get-oauth-account
  [deps account-id]
  (let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :oauth.account/id ?id]] account-id))]
    (when eid
      (decrypt-entity* deps (raw-entity* deps eid)))))

(defn list-oauth-accounts
  [deps]
  (let [eids (q* deps '[:find ?e :where [?e :oauth.account/id _]])]
    (mapv #(decrypt-entity* deps (raw-entity* deps (first %))) eids)))

(defn remove-oauth-account!
  [deps account-id]
  (when-let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :oauth.account/id ?id]] account-id))]
    (transact!* deps [[:db/retractEntity eid]])))

(defn oauth-account-in-use?
  [deps account-id]
  (boolean
    (ffirst
      (q* deps '[:find ?e :in $ ?id
                 :where
                 (or [?e :service/oauth-account ?id]
                     [?e :llm.provider/oauth-account ?id])]
          account-id))))

(declare get-tool)

(defn install-tool!
  [deps
   {:keys [id name description tags parameters handler approval execution-mode enabled? installed-at]}]
  (let [existing (when id (get-tool deps id))]
    (transact!*
      deps
      [(cond-> {:tool/id           id
                :tool/name         (or name
                                       (:tool/name existing)
                                       (clojure.core/name id))
                :tool/description  (or description
                                       (:tool/description existing)
                                       "")
                :tool/tags         (or tags
                                       (:tool/tags existing)
                                       #{})
                :tool/parameters   (or parameters
                                       (:tool/parameters existing)
                                       {})
                :tool/handler      (or handler
                                       (:tool/handler existing)
                                       "")
                :tool/approval     (or approval
                                       (:tool/approval existing)
                                       :auto)
                :tool/enabled?     (if (some? enabled?)
                                     enabled?
                                     (if (contains? existing :tool/enabled?)
                                       (:tool/enabled? existing)
                                       true))
                :tool/installed-at (or installed-at
                                       (:tool/installed-at existing)
                                       (java.util.Date.))}
         (some? execution-mode)
         (assoc :tool/execution-mode execution-mode))])))

(defn get-tool
  [deps tool-id]
  (let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :tool/id ?id]] tool-id))]
    (when eid
      (raw-entity* deps eid))))

(defn list-tools
  [deps]
  (let [eids (q* deps '[:find ?e :where [?e :tool/id _]])]
    (mapv #(raw-entity* deps (first %)) eids)))

(defn enable-tool!
  [deps tool-id enabled?]
  (transact!* deps [{:tool/id tool-id :tool/enabled? enabled?}]))
