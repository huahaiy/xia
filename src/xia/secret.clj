(ns xia.secret
  "Credential and secret protection.

   Defines which DB attributes and config keys are sensitive, and provides
   safe wrappers for use in the SCI sandbox. System code (xia.llm, xia.setup)
   accesses credentials directly through xia.db; sandboxed tool handlers go
   through these filtered functions instead."
  (:require [clojure.string :as str]
            [xia.db :as db]))

;; ---------------------------------------------------------------------------
;; Secret definitions
;; ---------------------------------------------------------------------------

(def ^:private secret-attrs
  "DB attributes that must never be returned to sandboxed code."
  #{:llm.provider/api-key :service/auth-key
    :site-cred/username :site-cred/password})

(def ^:private secret-attr-namespaces
  "Attribute namespace prefixes that are always secret."
  #{"credential" "secret"})

(def ^:private secret-config-prefixes
  "Config key namespace prefixes that are secret."
  #{"credential" "secret" "api-key" "oauth" "token"})

(defn secret-attr?
  "True if the given attribute keyword is secret."
  [attr]
  (or (contains? secret-attrs attr)
      (when-let [ns (namespace attr)]
        (some #(str/starts-with? ns %) secret-attr-namespaces))))

(defn secret-config-key?
  "True if the given config key should be treated as secret."
  [k]
  (when-let [ns (namespace k)]
    (some #(str/starts-with? ns %) secret-config-prefixes)))

;; ---------------------------------------------------------------------------
;; Safe wrappers for SCI sandbox
;; ---------------------------------------------------------------------------

(defn safe-get-config
  "Like db/get-config but refuses to return secret keys."
  [k]
  (when (secret-config-key? k)
    (throw (ex-info "Access denied: cannot read secret config key from tool"
                    {:key k})))
  (db/get-config k))

(defn safe-set-config!
  "Like db/set-config! but refuses to write to secret keys."
  [k v]
  (when (secret-config-key? k)
    (throw (ex-info "Access denied: cannot write secret config key from tool"
                    {:key k})))
  (db/set-config! k v))

(def ^:private blocked-attrs-pattern
  "Regex matching attribute names that tools cannot query."
  (re-pattern
    (str "(?i)"
         (str/join "|"
                   ["api.key" "api-key" "apikey"
                    "password" "passwd"
                    "secret" "credential"
                    "token" "oauth"
                    "private.key" "private-key"]))))

(defn- query-references-secret?
  "Check if a Datalog query references any secret attributes."
  [query]
  (let [walk-form (fn walk [form]
                    (cond
                      (keyword? form)
                      (or (secret-attr? form)
                          (when-let [n (name form)]
                            (re-find blocked-attrs-pattern n)))

                      (coll? form)
                      (some walk form)

                      :else false))]
    (boolean (walk-form query))))

(defn safe-q
  "Restricted Datalog query for the SCI sandbox.
   Rejects queries that reference secret attributes."
  [query & inputs]
  (when (query-references-secret? query)
    (throw (ex-info "Access denied: query references secret attributes"
                    {:query query})))
  (apply db/q query inputs))
