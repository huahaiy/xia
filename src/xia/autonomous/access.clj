(ns xia.autonomous.access
  "DB-backed approval checks for autonomous execution."
  (:require [xia.db :as db]))

(defn- enabled-by-default?
  [value]
  (not (false? value)))

(defn oauth-account-autonomous-approved?
  [account]
  (when account
    (enabled-by-default? (:oauth.account/autonomous-approved? account))))

(defn site-autonomous-approved?
  [site]
  (when site
    (enabled-by-default? (:site-cred/autonomous-approved? site))))

(defn service-autonomous-approved?
  [service]
  (when service
    (enabled-by-default? (:service/autonomous-approved? service))))

(defn oauth-account-approved?
  [account-id]
  (when account-id
    (oauth-account-autonomous-approved? (db/get-oauth-account account-id))))

(defn site-approved?
  [site-id]
  (when site-id
    (site-autonomous-approved? (db/get-site-cred site-id))))

(defn service-approved?
  [service-id]
  (when-let [service (db/get-service service-id)]
    (and (service-autonomous-approved? service)
         (if (= :oauth-account (:service/auth-type service))
           (oauth-account-approved? (:service/oauth-account service))
           true))))

(defn scope-available?
  [scope]
  (case scope
    :service (boolean (some #(service-approved? (:service/id %))
                            (db/list-services)))
    :site    (boolean (some #(site-approved? (:site-cred/id %))
                            (db/list-site-creds)))
    false))
