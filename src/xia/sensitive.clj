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
    :site-cred/username
    :site-cred/password})

(def plaintext-user-content-attrs
  "Durable user-content attributes that are intentionally plaintext at rest so
   Xia can search, resume, and render them. They remain hidden from sandboxed
   code because their values can contain credentials pasted by a user."
  #{:session/history-recap
    :session/tool-recap
    :message/content
    :message/tool-calls
    :message/tool-result
    :audit.event/data
    :schedule-run/result
    :schedule-run/error
    :schedule-run/actions})

(def plaintext-diagnostic-attrs
  "Disposable LLM diagnostic payloads. These are plaintext, sandbox-redacted,
   disabled by default, and governed by the LLM log retention policy."
  #{:llm.log/messages
    :llm.log/tools
    :llm.log/response
    :llm.log/error})

(def sandbox-only-secret-attrs
  "DB attributes that are redacted from sandboxed code but not encrypted at rest."
  (into plaintext-user-content-attrs plaintext-diagnostic-attrs))

(def secret-attr-namespaces
  "Attribute namespace prefixes that are always treated as secret."
  #{"credential" "secret"})

(def secret-config-prefixes
  "Config key namespace prefixes that are secret."
  #{"credential" "secret" "api-key" "oauth" "token"})

(def secret-config-keys
  "Specific config keys that are secret even when their namespace does not
   match the generic secret prefixes."
  #{:web/search-brave-api-key})

(def sandbox-blocked-config-write-keys
  "Non-secret settings whose mutation would change a security or privacy
   boundary. Sandboxed code may inspect these values but cannot change them."
  #{:llm/log-full-payloads?
    :llm/log-retention-days})

(def ^:private blocked-ident-pattern
  "Conservative regex matching identifier names that sandboxed code cannot
   query. It intentionally blocks operational names such as token counters and
   password-field selectors; callers must not treat a match as an at-rest
   encryption decision."
  (re-pattern
   (str "(?i)"
        (str/join "|"
                  ["api.key" "api-key" "apikey"
                   "password" "passwd"
                   "secret" "credential"
                   "token" "oauth"
                   "private.key" "private-key"]))))

(defn- named-ident?
  [value]
  (instance? clojure.lang.Named value))

(defn- ident-name
  [form]
  (when (named-ident? form)
    (name form)))

(defn encrypted-attr?
  "True if the given attribute keyword should be encrypted at rest."
  [attr]
  (or (contains? encrypted-attrs attr)
      (when-let [ns (namespace attr)]
        (some #(str/starts-with? ns %) secret-attr-namespaces))))

(defn secret-attr?
  "True for explicitly protected stored attributes.

   This includes encrypted credential fields and plaintext payload fields that
   are redacted from sandboxed code. Pattern-only conservative query denials
   are reported by `secret-query-ident?`, not by this predicate."
  [attr]
  (or (encrypted-attr? attr)
      (contains? sandbox-only-secret-attrs attr)))

(defn secret-config-key?
  "True if the given config key is encrypted and hidden from sandboxed reads.

   Namespace-prefix matching is intentionally conservative: namespaces such
   as `oauth2`, `tokenizer`, or `secretary` match the protected prefixes."
  [k]
  (or (contains? secret-config-keys k)
      (when-let [ns (when (named-ident? k)
                      (namespace k))]
        (some #(str/starts-with? ns %) secret-config-prefixes))))

(defn sandbox-blocked-config-write-key?
  "True if sandboxed code must not mutate the config key."
  [k]
  (or (secret-config-key? k)
      (contains? sandbox-blocked-config-write-keys k)))

(defn secret-query-ident?
  "True if the given query identifier or literal should be hidden from
   sandboxed code.

   This is deliberately broader than `secret-attr?` and
   `secret-config-key?`: secret-like names are denied even when the underlying
   field is harmless operational metadata."
  [form]
  (or (and (keyword? form)
           (secret-attr? form))
      (and (named-ident? form)
           (secret-config-key? form))
      (when-let [n (ident-name form)]
        (re-find blocked-ident-pattern n))))
