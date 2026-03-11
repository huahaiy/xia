(ns xia.sensitive
  "Shared classification for sensitive attributes and config keys."
  (:require [clojure.string :as str]))

(def secret-attrs
  "DB attributes that must never be exposed to sandboxed code and should be encrypted at rest."
  #{:llm.provider/api-key
    :service/auth-key
    :site-cred/username
    :site-cred/password
    :message/content
    :message/tool-calls
    :schedule-run/result
    :schedule-run/error})

(def secret-attr-namespaces
  "Attribute namespace prefixes that are always treated as secret."
  #{"credential" "secret"})

(def secret-config-prefixes
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
