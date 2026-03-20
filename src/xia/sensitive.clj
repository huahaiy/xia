(ns xia.sensitive
  "Shared classification for sensitive attributes and config keys."
  (:require [clojure.string :as str]))

(def encrypted-attrs
  "DB attributes that should be encrypted at rest."
  #{:llm.provider/api-key
    :service/auth-key
    :oauth.account/client-secret
    :oauth.account/access-token
    :oauth.account/refresh-token
    :remote.device/push-token
    :site-cred/username
    :site-cred/password
    :session/history-recap
    :message/content
    :schedule-run/result
    :schedule-run/error})

(def sandbox-only-secret-attrs
  "DB attributes that are redacted from sandboxed code but not encrypted at rest."
  #{:message/tool-calls
    :message/tool-result
    :schedule-run/actions})

(def secret-attr-namespaces
  "Attribute namespace prefixes that are always treated as secret."
  #{"credential" "secret"})

(def secret-config-prefixes
  "Config key namespace prefixes that are secret."
  #{"credential" "secret" "api-key" "oauth" "token"})

(defn encrypted-attr?
  "True if the given attribute keyword should be encrypted at rest."
  [attr]
  (or (contains? encrypted-attrs attr)
      (when-let [ns (namespace attr)]
        (some #(str/starts-with? ns %) secret-attr-namespaces))))

(defn secret-attr?
  "True if the given attribute keyword is secret to sandboxed code."
  [attr]
  (or (encrypted-attr? attr)
      (contains? sandbox-only-secret-attrs attr)))

(defn secret-config-key?
  "True if the given config key should be treated as secret."
  [k]
  (when-let [ns (namespace k)]
    (some #(str/starts-with? ns %) secret-config-prefixes)))
