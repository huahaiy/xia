(ns xia.db
  "Datalevin database — the single source of truth for a xia instance.
   All state lives here: config, identity, memory, messages, skills, tools."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.core :as d]
            [datalevin.embedding :as emb]
            [xia.crypto :as crypto]
            [xia.sensitive :as sensitive])
  (:import [java.io InputStream]
           [java.net URI]
           [java.net.http HttpClient HttpClient$Redirect HttpRequest HttpResponse$BodyHandlers]
           [java.nio.file Files Path Paths StandardCopyOption]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant]
           [java.util UUID]))

;; ---------------------------------------------------------------------------
;; Schema
;; ---------------------------------------------------------------------------

(def episode-text-domain "episode-text")
(def kg-node-domain "kg-node")
(def kg-fact-domain "kg-fact")
(def kg-edge-domain "kg-edge")
(def local-doc-domain "local-doc")
(def skill-content-domain "skill-content")

(def schema
  {;; --- Config (key-value pairs) ---
   :config/key   {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :config/value {:db/valueType :db.type/string}

   ;; --- Identity / Soul ---
   :identity/key   {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :identity/value {:db/valueType :db.type/string}

   ;; --- LLM Provider ---
   :llm.provider/id       {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :llm.provider/name     {:db/valueType :db.type/string}
   :llm.provider/base-url {:db/valueType :db.type/string}
   :llm.provider/api-key  {:db/valueType :db.type/string}
   :llm.provider/model    {:db/valueType :db.type/string}
   :llm.provider/workloads {:db/valueType :db.type/keyword
                            :db/cardinality :db.cardinality/many}
   :llm.provider/system-prompt-budget {:db/valueType :db.type/long}
   :llm.provider/history-budget {:db/valueType :db.type/long}
   :llm.provider/default? {:db/valueType :db.type/boolean}

   ;; --- Episodic Memory ---
   :episode/id           {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :episode/type         {:db/valueType :db.type/keyword} ; :conversation :event :observation
   :episode/summary      {:db/valueType :db.type/string
                          :db/fulltext true
                          :db.fulltext/domains [episode-text-domain]
                          :db/embedding true
                          :db.embedding/domains [episode-text-domain]}
   :episode/context      {:db/valueType :db.type/string
                          :db/fulltext true
                          :db.fulltext/domains [episode-text-domain]
                          :db/embedding true
                          :db.embedding/domains [episode-text-domain]} ; situational context
   :episode/participants {:db/valueType :db.type/string}  ; who was involved
   :episode/channel      {:db/valueType :db.type/string}
   :episode/session-id   {:db/valueType :db.type/string}  ; link back to session
   :episode/timestamp    {:db/valueType :db.type/instant}
   :episode/importance   {:db/valueType :db.type/float}
   :episode/processed?   {:db/valueType :db.type/boolean}  ; consolidation reached a terminal state
   :episode/consolidation-error {:db/valueType :db.type/string}
   :episode/consolidation-failed-at {:db/valueType :db.type/instant}

   ;; --- Knowledge Graph: Nodes ---
   :kg.node/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :kg.node/name       {:db/valueType :db.type/string
                        :db/fulltext true
                        :db.fulltext/domains [kg-node-domain]
                        :db/embedding true
                        :db.embedding/domains [kg-node-domain]}
   :kg.node/type       {:db/valueType :db.type/keyword} ; :person :place :thing :concept :preference
   :kg.node/properties {:db/valueType :db.type/idoc :db/domain "node-props"} ; structured properties (idoc)
   :kg.node/created-at {:db/valueType :db.type/instant}
   :kg.node/updated-at {:db/valueType :db.type/instant}

   ;; --- Knowledge Graph: Edges (relationships) ---
   :kg.edge/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :kg.edge/from       {:db/valueType :db.type/ref}     ; → kg.node
   :kg.edge/to         {:db/valueType :db.type/ref}     ; → kg.node
   :kg.edge/type       {:db/valueType :db.type/keyword} ; :knows :likes :works-at :uses etc.
   :kg.edge/label      {:db/valueType :db.type/string
                        :db/fulltext true
                        :db.fulltext/domains [kg-edge-domain]} ; human-readable description
   :kg.edge/weight     {:db/valueType :db.type/float}   ; confidence/strength
   :kg.edge/source     {:db/valueType :db.type/ref}     ; → episode (provenance)
   :kg.edge/created-at {:db/valueType :db.type/instant}

   ;; --- Knowledge Graph: Facts (atomic knowledge about a node) ---
   :kg.fact/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :kg.fact/node       {:db/valueType :db.type/ref}     ; → kg.node
   :kg.fact/content    {:db/valueType :db.type/string
                        :db/fulltext true
                        :db.fulltext/domains [kg-fact-domain]
                        :db/embedding true
                        :db.embedding/domains [kg-fact-domain]}
   :kg.fact/confidence {:db/valueType :db.type/float}
   :kg.fact/utility    {:db/valueType :db.type/float}
   :kg.fact/source     {:db/valueType :db.type/ref}     ; → episode (provenance)
   :kg.fact/created-at {:db/valueType :db.type/instant}
   :kg.fact/updated-at {:db/valueType :db.type/instant}
   :kg.fact/decayed-at {:db/valueType :db.type/instant}
   :kg.fact/bottomed-at {:db/valueType :db.type/instant}

   ;; --- Session ---
   :session/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :session/channel    {:db/valueType :db.type/keyword} ; :terminal :http
   :session/created-at {:db/valueType :db.type/instant}
   :session/active?    {:db/valueType :db.type/boolean}

   ;; --- Message ---
   :message/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :message/session    {:db/valueType :db.type/ref}
   :message/role       {:db/valueType :db.type/keyword} ; :user :assistant :system :tool
   :message/content    {:db/valueType :db.type/string}
   :message/created-at {:db/valueType :db.type/instant}
   :message/tool-calls {:db/valueType :db.type/idoc :db/domain "message-tool-calls"}
   :message/tool-result {:db/valueType :db.type/idoc :db/domain "message-tool-result"}
   :message/tool-id    {:db/valueType :db.type/string}  ; for tool-result messages
   :message.local-doc-ref/id      {:db/valueType :db.type/uuid :db/unique :db.unique/identity}
   :message.local-doc-ref/message {:db/valueType :db.type/ref}
   :message.local-doc-ref/doc     {:db/valueType :db.type/ref}

   ;; --- Local Documents (user-selected local file content) ---
   :local.doc/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :local.doc/session    {:db/valueType :db.type/ref}
   :local.doc/name       {:db/valueType :db.type/string
                          :db/fulltext true
                          :db.fulltext/domains [local-doc-domain]
                          :db/embedding true
                          :db.embedding/domains [local-doc-domain]}
   :local.doc/media-type {:db/valueType :db.type/string}
   :local.doc/source     {:db/valueType :db.type/keyword}
   :local.doc/size-bytes {:db/valueType :db.type/long}
   :local.doc/sha256     {:db/valueType :db.type/string}
   :local.doc/status     {:db/valueType :db.type/keyword}
   :local.doc/error      {:db/valueType :db.type/string}
   :local.doc/text       {:db/valueType :db.type/string
                          :db/fulltext true
                          :db.fulltext/domains [local-doc-domain]
                          :db/embedding true
                          :db.embedding/domains [local-doc-domain]}
   :local.doc/preview    {:db/valueType :db.type/string}

   ;; --- Skill (markdown/text instructions the LLM follows) ---
   :skill/id           {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :skill/name         {:db/valueType :db.type/string}
   :skill/description  {:db/valueType :db.type/string}  ; short summary for selection
   :skill/content      {:db/valueType :db.type/string
                        :db/fulltext true
                        :db.fulltext/domains [skill-content-domain]} ; raw markdown for prompt injection + FTS
   :skill/doc          {:db/valueType :db.type/idoc   :db/idocFormat :markdown :db/domain "skills"} ; parsed structure for section queries
   :skill/version      {:db/valueType :db.type/string}
   :skill/tags         {:db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   :skill/enabled?     {:db/valueType :db.type/boolean}
   :skill/installed-at {:db/valueType :db.type/instant}

   ;; --- Working Memory (crash-recovery snapshots) ---
   :wm/id               {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :wm/session           {:db/valueType :db.type/ref}     ; → session
   :wm/topics            {:db/valueType :db.type/string}
   :wm/updated-at        {:db/valueType :db.type/instant}

   :wm.slot/id           {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :wm.slot/wm           {:db/valueType :db.type/ref}     ; → wm
   :wm.slot/node         {:db/valueType :db.type/ref}     ; → kg.node
   :wm.slot/relevance    {:db/valueType :db.type/float}
   :wm.slot/pinned?      {:db/valueType :db.type/boolean}
   :wm.slot/added-at     {:db/valueType :db.type/instant}

   :wm.episode-ref/id       {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :wm.episode-ref/wm       {:db/valueType :db.type/ref}     ; → wm
   :wm.episode-ref/episode  {:db/valueType :db.type/ref}     ; → episode
   :wm.episode-ref/relevance {:db/valueType :db.type/float}

   :wm.local-doc-ref/id        {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :wm.local-doc-ref/wm        {:db/valueType :db.type/ref}     ; → wm
   :wm.local-doc-ref/doc       {:db/valueType :db.type/ref}     ; → local.doc
   :wm.local-doc-ref/relevance {:db/valueType :db.type/float}

   ;; --- Site Credentials (login credentials for websites) ---
   :site-cred/id              {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :site-cred/name            {:db/valueType :db.type/string}
   :site-cred/login-url       {:db/valueType :db.type/string}
   :site-cred/username-field  {:db/valueType :db.type/string}  ; form field name (e.g. "email")
   :site-cred/password-field  {:db/valueType :db.type/string}  ; form field name (e.g. "password")
   :site-cred/username        {:db/valueType :db.type/string}  ; actual username — SECRET
   :site-cred/password        {:db/valueType :db.type/string}  ; actual password — SECRET
   :site-cred/form-selector   {:db/valueType :db.type/string}  ; optional CSS selector for the form
   :site-cred/extra-fields    {:db/valueType :db.type/string}  ; JSON: additional fields to fill
   :site-cred/autonomous-approved? {:db/valueType :db.type/boolean}

   ;; --- Service (registered external services with auth credentials) ---
   :service/id          {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :service/name        {:db/valueType :db.type/string}
   :service/base-url    {:db/valueType :db.type/string}   ; e.g. "https://gmail.googleapis.com"
   :service/auth-type   {:db/valueType :db.type/keyword}  ; :bearer :basic :api-key-header :query-param :oauth-account
   :service/auth-key    {:db/valueType :db.type/string}    ; the secret — token, key, password
   :service/auth-header {:db/valueType :db.type/string}    ; custom header/param name (for :api-key-header / :query-param)
   :service/oauth-account {:db/valueType :db.type/keyword} ; linked OAuth account for :oauth-account auth
   :service/rate-limit-per-minute {:db/valueType :db.type/long}
   :service/autonomous-approved? {:db/valueType :db.type/boolean}
   :service/enabled?    {:db/valueType :db.type/boolean}

   ;; --- OAuth Accounts (authorization-code + PKCE / refresh-token auth) ---
   :oauth.account/id            {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :oauth.account/name          {:db/valueType :db.type/string}
   :oauth.account/authorize-url {:db/valueType :db.type/string}
   :oauth.account/token-url     {:db/valueType :db.type/string}
   :oauth.account/client-id     {:db/valueType :db.type/string}
   :oauth.account/client-secret {:db/valueType :db.type/string}
   :oauth.account/provider-template {:db/valueType :db.type/keyword}
   :oauth.account/scopes        {:db/valueType :db.type/string}
   :oauth.account/redirect-uri  {:db/valueType :db.type/string}
   :oauth.account/auth-params   {:db/valueType :db.type/string}
   :oauth.account/token-params  {:db/valueType :db.type/string}
   :oauth.account/access-token  {:db/valueType :db.type/string}
   :oauth.account/refresh-token {:db/valueType :db.type/string}
   :oauth.account/token-type    {:db/valueType :db.type/string}
   :oauth.account/expires-at    {:db/valueType :db.type/instant}
   :oauth.account/connected-at  {:db/valueType :db.type/instant}
   :oauth.account/autonomous-approved? {:db/valueType :db.type/boolean}
   :oauth.account/updated-at    {:db/valueType :db.type/instant}

   ;; --- Schedule (cron-based task scheduling) ---
   :schedule/id          {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :schedule/name        {:db/valueType :db.type/string}
   :schedule/description {:db/valueType :db.type/string}
   :schedule/spec        {:db/valueType :db.type/string}     ; EDN schedule spec
   :schedule/type        {:db/valueType :db.type/keyword}    ; :tool or :prompt
   :schedule/tool-id     {:db/valueType :db.type/keyword}    ; for :tool type
   :schedule/tool-args   {:db/valueType :db.type/idoc :db/domain "schedule-tool-args"}
   :schedule/prompt      {:db/valueType :db.type/string}     ; for :prompt type
   :schedule/trusted?    {:db/valueType :db.type/boolean}    ; user-approved to bypass approval for autonomous-approved tools
   :schedule/enabled?    {:db/valueType :db.type/boolean}
   :schedule/last-run    {:db/valueType :db.type/instant}
   :schedule/next-run    {:db/valueType :db.type/instant}
   :schedule/created-at  {:db/valueType :db.type/instant}

   ;; --- Schedule Run (execution log) ---
   :schedule-run/id          {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :schedule-run/schedule-id {:db/valueType :db.type/keyword}
   :schedule-run/started-at  {:db/valueType :db.type/instant}
   :schedule-run/finished-at {:db/valueType :db.type/instant}
   :schedule-run/status      {:db/valueType :db.type/keyword} ; :success :error
   :schedule-run/result      {:db/valueType :db.type/string}
   :schedule-run/error       {:db/valueType :db.type/string}
   :schedule-run/actions     {:db/valueType :db.type/idoc :db/domain "schedule-run-actions"}

   ;; --- Tool (executable code the LLM can call via function-calling) ---
   :tool/id            {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :tool/name          {:db/valueType :db.type/string}
   :tool/description   {:db/valueType :db.type/string}
   :tool/tags          {:db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   :tool/parameters    {:db/valueType :db.type/idoc :db/domain "tool-parameters"}
   :tool/handler       {:db/valueType :db.type/string}  ; SCI code → fn
   :tool/approval      {:db/valueType :db.type/keyword} ; :auto :session :always
   :tool/execution-mode {:db/valueType :db.type/keyword} ; :sequential :parallel-safe
   :tool/enabled?      {:db/valueType :db.type/boolean}
   :tool/installed-at  {:db/valueType :db.type/instant}})

;; ---------------------------------------------------------------------------
;; Connection management
;; ---------------------------------------------------------------------------

(defonce ^:private conn-atom (atom nil))
(declare migrate-secrets!)

(def ^:private default-embedding-provider-id
  ;; Keep Xia's provider key stable even if the underlying model changes.
  :xia-default)

(def ^:private default-embedding-model-file
  "nomic-embed-text-v2-moe-q8_0.gguf")

(def ^:private default-embedding-model-url
  (str "https://huggingface.co/ggml-org/Nomic-Embed-Text-V2-GGUF/resolve/main/"
       default-embedding-model-file
       "?download=true"))

(def ^:private embedding-model-lock
  (Object.))

(def ^:private default-embedding-provider-spec
  ;; Keep Xia's provider choice centralized so the default model can change
  ;; without touching the rest of the DB wiring.
  {:provider :llama.cpp
   :model-id "nomic-ai/nomic-embed-text-v2-moe"
   :model-filename default-embedding-model-file
   :model-url default-embedding-model-url
   :embedding-metadata
   {:embedding/provider
    {:kind :local
     :id :llama.cpp
     :model-id "nomic-ai/nomic-embed-text-v2-moe"}
    :embedding/output
    {:dimensions 768
     :max-tokens 2048}
    :embedding/artifact
    {:format :gguf
     :file default-embedding-model-file
     :quantization :q8_0}}})

(def ^:private default-datalevin-opts-map
  {:embedding-opts      {:provider default-embedding-provider-id
                         :metric-type :cosine}
   :validate-data?      true
   :auto-entity-time?   true
   :embedding-providers {default-embedding-provider-id
                         default-embedding-provider-spec}})

(defn- deep-merge
  [left right]
  (merge-with (fn [x y]
                (if (and (map? x) (map? y))
                  (deep-merge x y)
                  y))
              (or left {})
              (or right {})))

(defn default-datalevin-opts
  []
  default-datalevin-opts-map)

(defn- resolve-datalevin-opts
  [options]
  (deep-merge default-datalevin-opts-map
              (:datalevin-opts options)))

(defn- resolve-embedding-provider-spec
  [db-path datalevin-opts]
  (let [provider-id   (or (get-in datalevin-opts [:embedding-opts :provider])
                          :default)
        runtime-entry (get-in datalevin-opts [:embedding-providers provider-id])]
    (cond
      (satisfies? emb/IEmbeddingProvider runtime-entry)
      runtime-entry

      (map? runtime-entry)
      (let [provider-spec (merge {:provider provider-id
                                  :dir      db-path}
                                 runtime-entry)]
        (cond-> provider-spec
          (and (nil? (:model provider-spec))
               (nil? (:model-path provider-spec))
               (string? (:model-filename provider-spec)))
          (assoc :model-path
                 (str db-path java.io.File/separator
                      "embed"
                      java.io.File/separator
                      (:model-filename provider-spec)))))

      (keyword? runtime-entry)
      {:provider runtime-entry
       :dir      db-path}

      (nil? runtime-entry)
      {:provider provider-id
       :dir      db-path}

      :else
      runtime-entry)))

(defn- create-http-client
  []
  (-> (HttpClient/newBuilder)
      (.followRedirects HttpClient$Redirect/NORMAL)
      (.build)))

(defn- move-file!
  [^Path source ^Path target]
  (try
    (Files/move source target
                (into-array java.nio.file.CopyOption
                            [StandardCopyOption/ATOMIC_MOVE
                             StandardCopyOption/REPLACE_EXISTING]))
    (catch Exception _
      (Files/move source target
                  (into-array java.nio.file.CopyOption
                              [StandardCopyOption/REPLACE_EXISTING])))))

(defn- download-file!
  [url target-path]
  (let [^Path target  (Paths/get target-path (make-array String 0))
        ^Path parent  (.getParent target)
        tmp-dir       (or parent (Paths/get "." (make-array String 0)))
        _             (when parent
                        (Files/createDirectories parent (make-array FileAttribute 0)))
        prefix        (str (.getFileName target) ".part-")
        suffix        ".tmp"
        tmp           (Files/createTempFile tmp-dir prefix suffix
                                            (make-array FileAttribute 0))
        ^HttpClient client (create-http-client)
        ^HttpRequest req   (-> (HttpRequest/newBuilder (URI/create url))
                               (.header "User-Agent" "xia")
                               (.header "Accept" "application/octet-stream")
                               (.GET)
                               (.build))
        ^"[Ljava.nio.file.CopyOption;" copy-opts
        (into-array java.nio.file.CopyOption
                    [StandardCopyOption/REPLACE_EXISTING])]
    (try
      (let [resp   (.send client req (HttpResponse$BodyHandlers/ofInputStream))
            status (.statusCode resp)]
        (when-not (= 200 status)
          (throw (ex-info "Failed to download embedding model"
                          {:url url :status status :target target-path})))
        (with-open [^InputStream in (.body resp)]
          (Files/copy in ^Path tmp copy-opts))
        (move-file! tmp target)
        target-path)
      (finally
        (when (Files/exists tmp (make-array java.nio.file.LinkOption 0))
          (try
            (Files/deleteIfExists tmp)
            (catch Exception _)))))))

(defn- ensure-managed-embedding-model!
  [provider-spec]
  (let [model-path (or (:model provider-spec) (:model-path provider-spec))]
    (cond
      (not (map? provider-spec))
      provider-spec

      (or (nil? model-path)
          (nil? (:model-url provider-spec))
          (.exists (io/file model-path)))
      provider-spec

      :else
      (locking embedding-model-lock
        (when-not (.exists (io/file model-path))
          (download-file! (:model-url provider-spec) model-path))
        provider-spec))))

(defn- bootstrap-embedding-provider!
  [db-path datalevin-opts]
  (let [provider-spec (-> (resolve-embedding-provider-spec db-path datalevin-opts)
                          ensure-managed-embedding-model!)]
    (when-not (satisfies? emb/IEmbeddingProvider provider-spec)
      (with-open [provider (d/new-embedding-provider provider-spec)]
        (d/embedding-dimensions provider)))))

(defn connect!
  "Open (or create) the Datalevin database at `db-path`."
  ([db-path] (connect! db-path nil))
  ([db-path crypto-opts]
   (let [datalevin-opts (resolve-datalevin-opts crypto-opts)
         c              (d/get-conn db-path schema datalevin-opts)]
     (try
       (reset! conn-atom c)
       (crypto/configure! db-path crypto-opts)
       (bootstrap-embedding-provider! db-path datalevin-opts)
       (migrate-secrets!)
       c
       (catch Throwable t
         (reset! conn-atom nil)
         (try
           (d/close c)
           (catch Exception _))
         (throw t))))))

(defn conn
  "Return the current connection. Throws if not connected."
  []
  (or @conn-atom
      (throw (ex-info "Database not connected. Call (xia.db/connect!) first." {}))))

(defn close! []
  (when-let [c @conn-atom]
    (d/close c)
    (reset! conn-atom nil)))

;; ---------------------------------------------------------------------------
;; Generic helpers
;; ---------------------------------------------------------------------------

(defn- config-aad [k]
  (str "config:" (name k)))

(defn- attr-aad [attr]
  (str "attr:" (namespace attr) "/" (name attr)))

(defn- decrypt-secret-attr [attr value]
  (if (and value (sensitive/encrypted-attr? attr))
    (crypto/decrypt value (attr-aad attr))
    value))

(defn- maybe-encrypt-config-value [k value]
  (if (sensitive/secret-config-key? k)
    (crypto/encrypt value (config-aad k))
    (str value)))

(defn- coerce-boolean
  [value]
  (cond
    (boolean? value) value
    (string? value)  (let [normalized (-> value str/trim str/lower-case)]
                       (cond
                         (#{"true" "1" "yes" "on"} normalized) true
                         (#{"false" "0" "no" "off"} normalized) false
                         :else value))
    :else            value))

(defn- coerce-ref
  [value]
  (cond
    (number? value) value
    (and (string? value) (re-matches #"-?\\d+" value)) (Long/parseLong value)
    :else value))

(defn- coerce-inst
  [value]
  (cond
    (instance? java.util.Date value) value
    (integer? value)                 (java.util.Date. (long value))
    (string? value)                  (java.util.Date/from (Instant/parse value))
    :else                            value))

(defn- coerce-uuid
  [value]
  (cond
    (uuid? value)   value
    (string? value) (java.util.UUID/fromString value)
    :else           value))

(defn- coerce-scalar-by-type
  [value-type value]
  (if (nil? value)
    nil
    (case value-type
      :db.type/string  (str value)
      :db.type/bigint  (biginteger value)
      :db.type/bigdec  (bigdec value)
      :db.type/long    (if (string? value) (Long/parseLong value) (long value))
      :db.type/ref     (coerce-ref value)
      :db.type/float   (if (string? value) (Float/parseFloat value) (float value))
      :db.type/double  (if (string? value) (Double/parseDouble value) (double value))
      :db.type/keyword (if (keyword? value) value (keyword value))
      :db.type/symbol  (if (symbol? value) value (symbol value))
      :db.type/boolean (coerce-boolean value)
      :db.type/instant (coerce-inst value)
      :db.type/uuid    (coerce-uuid value)
      :db.type/tuple   (if (vector? value) value (vec value))
      value)))

(defn- coerce-collection-like
  [original values]
  (cond
    (set? original)    (set values)
    (vector? original) (vec values)
    (list? original)   (apply list values)
    :else              (vec values)))

(defn- coerce-attr-value
  [attr value]
  (let [value-type  (get-in schema [attr :db/valueType])
        cardinality (get-in schema [attr :db/cardinality])]
    (cond
      (or (nil? value-type)
          (nil? value))
      value

      (and (= :db.cardinality/many cardinality)
           (coll? value)
           (not (map? value))
           (not (string? value)))
      (coerce-collection-like value (map #(coerce-scalar-by-type value-type %) value))

      :else
      (coerce-scalar-by-type value-type value))))

(defn- encrypt-secret-attrs [record]
  (reduce-kv
    (fn [acc k v]
      (assoc acc k
             (if (sensitive/encrypted-attr? k)
               (crypto/encrypt v (attr-aad k))
               v)))
    record
    record))

(defn- coerce-tx-item
  [item]
  (cond
    (map? item)
    (reduce-kv (fn [acc k v]
                 (assoc acc k
                        (if (keyword? k)
                          (coerce-attr-value k v)
                          v)))
               {}
               item)

    (and (vector? item)
         (= :db/add (first item))
         (= 4 (count item))
         (keyword? (nth item 2)))
    (let [[op eid attr value] item]
      [op eid attr (coerce-attr-value attr value)])

    :else
    item))

(defn- prepare-tx-item [item]
  (let [item (coerce-tx-item item)]
    (cond
      (map? item)
      (let [item (if (contains? item :config/key)
                   (update item :config/value #(maybe-encrypt-config-value (:config/key item) %))
                   item)]
        (encrypt-secret-attrs item))

      (and (vector? item)
           (= :db/add (first item))
           (= 4 (count item))
           (keyword? (nth item 2))
           (sensitive/encrypted-attr? (nth item 2)))
      (let [[op eid attr value] item]
        [op eid attr (crypto/encrypt value (attr-aad attr))])

      :else
      item)))

(defn transact! [tx-data]
  (d/transact! (conn) (mapv prepare-tx-item tx-data)))

(defn q [query & inputs]
  (apply d/q query (d/db (conn)) inputs))

(defn entity [eid]
  (let [e (into {} (d/entity (d/db (conn)) eid))]
    (reduce-kv (fn [acc k v]
                 (assoc acc k (decrypt-secret-attr k v)))
               {}
               e)))

(defn- entity-created-at
  [entity-map]
  (or (:session/created-at entity-map)
      (:message/created-at entity-map)
      (some-> (:db/created-at entity-map) long java.util.Date.)))

(defn- entity-updated-at
  [entity-map]
  (or (:wm/updated-at entity-map)
      (:oauth.account/updated-at entity-map)
      (some-> (:db/updated-at entity-map) long java.util.Date.)))

(defn- raw-entity [eid]
  (into {} (d/entity (d/db (conn)) eid)))

(defn- decrypt-entity [entity-map]
  (reduce-kv (fn [acc k v]
               (assoc acc k (decrypt-secret-attr k v)))
             {}
             entity-map))

(defn- migrate-secret-attr! [eid attr value]
  (when (and (string? value)
             (not (str/blank? value))
                (not (crypto/encrypted? value)))
    (transact! [[:db/add eid attr (crypto/encrypt value (attr-aad attr))]])
    1))

(defn- migrate-secret-config! [eid config-key value]
  (when (and (sensitive/secret-config-key? config-key)
             (string? value)
             (not (str/blank? value))
             (not (crypto/encrypted? value)))
    (transact! [[:db/add eid :config/value (crypto/encrypt value (config-aad config-key))]])
    1))

(defn- migrate-secrets!
  []
  (let [db (d/db (conn))
        config-count
        (reduce
          +
          0
          (map (fn [[eid k v]]
                 (or (migrate-secret-config! eid k v) 0))
               (d/q '[:find ?e ?k ?v
                      :where
                      [?e :config/key ?k]
                      [?e :config/value ?v]]
                    db)))
        attr-count
        (reduce
          +
          0
          (for [attr sensitive/encrypted-attrs
                [eid value] (d/q '[:find ?e ?v
                                   :in $ ?attr
                                   :where
                                   [?e ?attr ?v]]
                                 db attr)]
            (or (migrate-secret-attr! eid attr value) 0)))]
    (+ config-count attr-count)))

;; ---------------------------------------------------------------------------
;; Config
;; ---------------------------------------------------------------------------

(defn set-config! [k v]
  (transact! [{:config/key k :config/value (str v)}]))

(defn delete-config! [k]
  (when-let [eid (ffirst (q '[:find ?e :in $ ?k
                              :where [?e :config/key ?k]]
                            k))]
    (transact! [[:db/retractEntity eid]])))

(defn get-config [k]
  (let [value (ffirst (q '[:find ?v :in $ ?k :where [?e :config/key ?k] [?e :config/value ?v]] k))]
    (if (sensitive/secret-config-key? k)
      (some-> value (crypto/decrypt (config-aad k)))
      value)))

;; ---------------------------------------------------------------------------
;; Identity
;; ---------------------------------------------------------------------------

(defn set-identity! [k v]
  (transact! [{:identity/key k :identity/value (str v)}]))

(defn get-identity [k]
  (ffirst (q '[:find ?v :in $ ?k :where [?e :identity/key ?k] [?e :identity/value ?v]] k)))

;; ---------------------------------------------------------------------------
;; LLM Providers
;; ---------------------------------------------------------------------------

(defn upsert-provider! [{:keys [id] :as provider}]
  (let [provider-id                  (or id (:llm.provider/id provider))
        provider-eid                 (ffirst (q '[:find ?e :in $ ?id
                                                  :where [?e :llm.provider/id ?id]]
                                                provider-id))
        workloads                    (some-> (or (:llm.provider/workloads provider)
                                                (:workloads provider))
                                             set)
        system-prompt-budget         (or (:llm.provider/system-prompt-budget provider)
                                         (:system-prompt-budget provider))
        history-budget               (or (:llm.provider/history-budget provider)
                                         (:history-budget provider))
        has-workloads?               (or (contains? provider :llm.provider/workloads)
                                         (contains? provider :workloads))
        has-system-prompt-budget?    (or (contains? provider :llm.provider/system-prompt-budget)
                                         (contains? provider :system-prompt-budget))
        has-history-budget?          (or (contains? provider :llm.provider/history-budget)
                                         (contains? provider :history-budget))
        provider-tx                  (cond-> {:llm.provider/id provider-id}
                                       (contains? provider :llm.provider/name)
                                       (assoc :llm.provider/name (:llm.provider/name provider))
                                       (contains? provider :name)
                                       (assoc :llm.provider/name (:name provider))
                                       (contains? provider :llm.provider/base-url)
                                       (assoc :llm.provider/base-url (:llm.provider/base-url provider))
                                       (contains? provider :base-url)
                                       (assoc :llm.provider/base-url (:base-url provider))
                                       (contains? provider :llm.provider/api-key)
                                       (assoc :llm.provider/api-key (:llm.provider/api-key provider))
                                       (contains? provider :api-key)
                                       (assoc :llm.provider/api-key (:api-key provider))
                                       (contains? provider :llm.provider/model)
                                       (assoc :llm.provider/model (:llm.provider/model provider))
                                       (contains? provider :model)
                                       (assoc :llm.provider/model (:model provider))
                                       (and has-workloads?
                                            (seq workloads))
                                       (assoc :llm.provider/workloads workloads)
                                       (and has-system-prompt-budget?
                                            (some? system-prompt-budget))
                                       (assoc :llm.provider/system-prompt-budget system-prompt-budget)
                                       (and has-history-budget?
                                            (some? history-budget))
                                       (assoc :llm.provider/history-budget history-budget)
                                       (contains? provider :llm.provider/default?)
                                       (assoc :llm.provider/default? (:llm.provider/default? provider))
                                       (contains? provider :default?)
                                       (assoc :llm.provider/default? (:default? provider)))
        retracts                     (cond-> []
                                       (and provider-eid
                                            has-workloads?)
                                       (into (mapv (fn [workload]
                                                     [:db/retract provider-eid
                                                      :llm.provider/workloads
                                                      workload])
                                                   (or (:llm.provider/workloads (raw-entity provider-eid))
                                                       [])))
                                       (and provider-eid
                                            has-system-prompt-budget?
                                            (nil? system-prompt-budget))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/system-prompt-budget])
                                       (and provider-eid
                                            has-history-budget?
                                            (nil? history-budget))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/history-budget]))]
    (transact! (conj retracts provider-tx))))

(defn get-default-provider []
  (let [results (q '[:find ?e :where
                     [?e :llm.provider/default? true]])]
    (when-let [eid (ffirst results)]
      (decrypt-entity (raw-entity eid)))))

(defn get-provider [provider-id]
  (let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :llm.provider/id ?id]]
                       provider-id))]
    (when eid
      (decrypt-entity (raw-entity eid)))))

(defn list-providers []
  (let [eids (q '[:find ?e :where [?e :llm.provider/id _]])]
    (mapv #(decrypt-entity (raw-entity (first %))) eids)))

(defn set-default-provider!
  "Mark exactly one provider as the default."
  [provider-id]
  (let [providers (list-providers)
        tx-data   (mapv (fn [provider]
                          {:llm.provider/id       (:llm.provider/id provider)
                           :llm.provider/default? (= provider-id
                                                     (:llm.provider/id provider))})
                        providers)]
    (when (seq tx-data)
      (transact! tx-data))
    provider-id))

;; ---------------------------------------------------------------------------
;; Memory — episodic and knowledge graph operations are in xia.memory
;; The DB layer just provides the schema; memory.clj has the logic.
;; ---------------------------------------------------------------------------

;; ---------------------------------------------------------------------------
;; Working Memory snapshots (crash-recovery)
;; ---------------------------------------------------------------------------

(defn save-wm-snapshot!
  "Persist working memory state to DB for crash recovery."
  [{:keys [session-id topics slots episode-refs local-doc-refs]}]
  (let [session-eid (ffirst (q '[:find ?e :in $ ?sid
                                  :where [?e :session/id ?sid]]
                                session-id))]
    (when-not session-eid
      (throw (ex-info "Cannot save WM snapshot: session not found"
                      {:session-id session-id})))
    (let [wm-id       (random-uuid)
        wm-tx       {:wm/id         wm-id
                      :wm/session    session-eid
                      :wm/topics     (or topics "")
                      :wm/updated-at (java.util.Date.)}
        ;; Delete old snapshot for this session
        old-wm-eids (mapv first (q '[:find ?e :in $ ?s
                                      :where [?e :wm/session ?s]]
                                    session-eid))
        retracts    (mapv (fn [eid] [:db/retractEntity eid]) old-wm-eids)]
    (transact! (into retracts [wm-tx]))
    ;; Now add slots and episode-refs pointing to the new WM entity
    (let [wm-eid (ffirst (q '[:find ?e :in $ ?id :where [?e :wm/id ?id]] wm-id))]
      (when (seq slots)
        (transact!
          (mapv (fn [[_node-eid slot]]
                  {:wm.slot/id        (random-uuid)
                   :wm.slot/wm        wm-eid
                   :wm.slot/node      (:node-eid slot)
                   :wm.slot/relevance (float (:relevance slot))
                   :wm.slot/pinned?   (boolean (:pinned? slot))
                   :wm.slot/added-at  (java.util.Date.)})
                slots)))
      (when (seq episode-refs)
        (transact!
          (mapv (fn [eref]
                  {:wm.episode-ref/id        (random-uuid)
                   :wm.episode-ref/wm        wm-eid
                   :wm.episode-ref/episode   (:episode-eid eref)
                   :wm.episode-ref/relevance (float (:relevance eref))})
                episode-refs)))
      (when (seq local-doc-refs)
        (transact!
          (keep (fn [dref]
                  (when-let [doc-eid (ffirst
                                       (q '[:find ?e :in $ ?id
                                            :where [?e :local.doc/id ?id]]
                                          (:doc-id dref)))]
                    {:wm.local-doc-ref/id        (random-uuid)
                     :wm.local-doc-ref/wm        wm-eid
                     :wm.local-doc-ref/doc       doc-eid
                     :wm.local-doc-ref/relevance (float (:relevance dref))}))
                local-doc-refs))))
    wm-id)))

(defn load-wm-snapshot
  "Load the most recent WM snapshot for a session."
  [session-id]
  (let [session-eid (ffirst (q '[:find ?e :in $ ?sid
                                  :where [?e :session/id ?sid]]
                                session-id))]
    (when session-eid
      (when-let [wm-eid (ffirst (q '[:find ?e :in $ ?s
                                      :where [?e :wm/session ?s]]
                                    session-eid))]
        (let [wm-entity (into {} (d/entity (d/db (conn)) wm-eid))]
          {:topics (:wm/topics wm-entity)
           :updated-at (entity-updated-at wm-entity)})))))

(defn latest-session-episode
  "Get the most recent episode summary for any session."
  []
  (first
    (sort-by :timestamp #(compare %2 %1)
             (map (fn [[summary ctx ts]]
                    {:summary summary
                     :context (when-not (= "" ctx) ctx)
                     :timestamp ts})
                  (q '[:find ?summary ?ctx ?ts
                       :where
                       [?e :episode/summary ?summary]
                       [?e :episode/timestamp ?ts]
                       [(get-else $ ?e :episode/context "") ?ctx]])))))

;; ---------------------------------------------------------------------------
;; Sessions & Messages
;; ---------------------------------------------------------------------------

(defn create-session! [channel]
  (let [id (random-uuid)]
    (transact! [{:session/id         id
                 :session/channel    channel
                 :session/active?    true}])
    id))

(defn list-sessions
  "List all sessions with basic metadata, newest first."
  []
  (->> (q '[:find ?s ?sid ?channel ?active
            :where
            [?s :session/id ?sid]
            [?s :session/channel ?channel]
            [(get-else $ ?s :session/active? false) ?active]])
       (map (fn [[eid sid channel active?]]
              (let [entity-map (raw-entity eid)]
                {:id         sid
                 :channel    channel
                 :created-at (entity-created-at entity-map)
                 :active?    active?})))
       (sort-by :created-at #(compare %2 %1))
       vec))

(defn- tool-calls-doc
  [tool-calls]
  {:calls (vec tool-calls)})

(defn- read-tool-calls-doc
  [value]
  (cond
    (map? value)        (vec (or (:calls value) []))
    (sequential? value) (vec value)
    (= "" value)        nil
    :else               value))

(defn- tool-result-doc
  [tool-result]
  {:result tool-result})

(defn- read-tool-result-doc
  [value]
  (cond
    (map? value)        (if (contains? value :result) (:result value) value)
    (= "" value)        nil
    :else               value))

(declare empty->nil)

(defn- normalize-message-local-doc-id
  [value]
  (cond
    (instance? UUID value) value
    (string? value)        (try
                             (UUID/fromString (str/trim value))
                             (catch Exception _
                               nil))
    :else                  nil))

(defn- valid-session-local-doc-ids
  [session-id local-doc-ids]
  (let [doc-ids (->> local-doc-ids
                     (keep normalize-message-local-doc-id)
                     distinct
                     vec)]
    (if-not (seq doc-ids)
      []
      (let [valid-ids (->> (q '[:find ?doc-id
                                :in $ ?sid [?doc-id ...]
                                :where
                                [?session :session/id ?sid]
                                [?doc :local.doc/session ?session]
                                [?doc :local.doc/id ?doc-id]]
                              session-id
                              doc-ids)
                           (map first)
                           set)]
        (filterv valid-ids doc-ids)))))

(defn- message-local-docs
  [message-eid]
  (->> (q '[:find ?doc-id ?name ?status
            :in $ ?message
            :where
            [?ref :message.local-doc-ref/message ?message]
            [?ref :message.local-doc-ref/doc ?doc]
            [?doc :local.doc/id ?doc-id]
            [(get-else $ ?doc :local.doc/name "") ?name]
            [(get-else $ ?doc :local.doc/status :ready) ?status]]
          message-eid)
       (mapv (fn [[doc-id name status]]
               {:id     doc-id
                :name   (empty->nil name)
                :status status}))))

(defn add-message! [session-id role content & {:keys [tool-calls tool-id tool-result local-doc-ids]}]
  (let [session-eid (ffirst (q '[:find ?e :in $ ?sid
                                 :where [?e :session/id ?sid]]
                               session-id))
        message-id   (random-uuid)
        doc-ids      (valid-session-local-doc-ids session-id local-doc-ids)]
    (transact!
      (into
        [(cond-> {:message/id         message-id
                  :message/session    session-eid
                  :message/role       role
                  :message/content    (or content "")}
           tool-calls (assoc :message/tool-calls (tool-calls-doc tool-calls))
           (some? tool-result) (assoc :message/tool-result (tool-result-doc tool-result))
           tool-id    (assoc :message/tool-id tool-id))]
        (map (fn [doc-id]
               {:message.local-doc-ref/id      (random-uuid)
                :message.local-doc-ref/message [:message/id message-id]
                :message.local-doc-ref/doc     [:local.doc/id doc-id]})
             doc-ids)))))

(defn- empty->nil [s] (when-not (= "" s) s))

(defn session-messages
  "Get all messages for a session, ordered by creation time."
  [session-id]
  (sort-by :created-at
           (map (fn [[eid mid content role tool-calls tool-result tool-id]]
                  (let [tool-result* (read-tool-result-doc tool-result)]
                    {:id          mid
                     :role        role
                     :content     (when-not (and (= role :tool) (some? tool-result*))
                                    (decrypt-secret-attr :message/content content))
                     :created-at  (entity-created-at (raw-entity eid))
                     :local-docs  (not-empty (message-local-docs eid))
                     :tool-calls  (read-tool-calls-doc tool-calls)
                     :tool-result tool-result*
                     :tool-id     (empty->nil tool-id)}))
                (q '[:find ?m ?mid ?content ?role ?tc ?tr ?tid
                     :in $ ?sid
                     :where
                     [?s :session/id ?sid]
                     [?m :message/session ?s]
                     [?m :message/id ?mid]
                     [?m :message/role ?role]
                     [?m :message/content ?content]
                     [(get-else $ ?m :message/tool-calls "") ?tc]
                     [(get-else $ ?m :message/tool-result "") ?tr]
                     [(get-else $ ?m :message/tool-id "") ?tid]]
                   session-id))))

;; ---------------------------------------------------------------------------
;; Skills (markdown instructions)
;; ---------------------------------------------------------------------------

(defn install-skill! [{:keys [id name description content doc version tags]}]
  (transact! [(cond-> {:skill/id           id
                        :skill/name         (or name (clojure.core/name id))
                        :skill/description  (or description "")
                        :skill/content      (or content "")
                        :skill/version      (or version "0.1.0")
                        :skill/tags         (or tags #{})
                        :skill/enabled?     true
                        :skill/installed-at (java.util.Date.)}
                doc (assoc :skill/doc doc))]))

(defn get-skill [skill-id]
  (let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :skill/id ?id]] skill-id))]
    (when eid (raw-entity eid))))

(defn list-skills []
  (let [eids (q '[:find ?e :where [?e :skill/id _]])]
    (mapv #(raw-entity (first %)) eids)))

(defn enable-skill! [skill-id enabled?]
  (transact! [{:skill/id skill-id :skill/enabled? enabled?}]))

(defn find-skills-by-tags
  "Find skills matching any of the given tags."
  [tags]
  (let [eids (q '[:find ?e
                  :in $ [?tag ...]
                  :where
                  [?e :skill/tags ?tag]
                  [?e :skill/enabled? true]]
                tags)]
    (mapv #(into {} (d/entity (d/db (conn)) (first %))) eids)))

;; ---------------------------------------------------------------------------
;; Site Credentials (website login credentials)
;; ---------------------------------------------------------------------------

(defn save-site-cred!
  [{:keys [id name login-url username-field password-field
           username password form-selector extra-fields autonomous-approved?]}]
  (let [eid     (ffirst (q '[:find ?e :in $ ?id :where [?e :site-cred/id ?id]] id))
        current (when eid (raw-entity eid))
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
    (transact! tx-data)))

(defn register-site-cred!
  [site-cred]
  (save-site-cred! site-cred))

(defn get-site-cred [site-id]
  (let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :site-cred/id ?id]] site-id))]
    (when eid (decrypt-entity (raw-entity eid)))))

(defn list-site-creds []
  (let [eids (q '[:find ?e :where [?e :site-cred/id _]])]
    (mapv #(decrypt-entity (raw-entity (first %))) eids)))

(defn remove-site-cred! [site-id]
  (when-let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :site-cred/id ?id]] site-id))]
    (transact! [[:db/retractEntity eid]])))

;; ---------------------------------------------------------------------------
;; Services (external service registrations)
;; ---------------------------------------------------------------------------

(defn save-service!
  [{:keys [id name base-url auth-type auth-key auth-header oauth-account enabled?
           autonomous-approved?] :as service}]
  (let [eid     (ffirst (q '[:find ?e :in $ ?id :where [?e :service/id ?id]] id))
        current (when eid (raw-entity eid))
        rate-limit-per-minute (or (:service/rate-limit-per-minute service)
                                  (:rate-limit-per-minute service))
        has-rate-limit? (or (contains? service :service/rate-limit-per-minute)
                            (contains? service :rate-limit-per-minute))
        tx-data (cond-> [{:service/id        id
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
    (transact! tx-data)))

(defn register-service! [service]
  (save-service! service))

(defn get-service [service-id]
  (let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :service/id ?id]] service-id))]
    (when eid (decrypt-entity (raw-entity eid)))))

(defn list-services []
  (let [eids (q '[:find ?e :where [?e :service/id _]])]
    (mapv #(decrypt-entity (raw-entity (first %))) eids)))

(defn enable-service! [service-id enabled?]
  (transact! [{:service/id service-id :service/enabled? enabled?}]))

;; ---------------------------------------------------------------------------
;; OAuth accounts
;; ---------------------------------------------------------------------------

(defn save-oauth-account!
  [{:keys [id name authorize-url token-url client-id client-secret scopes
           provider-template redirect-uri auth-params token-params access-token refresh-token
           token-type expires-at connected-at autonomous-approved?]}]
  (let [eid     (ffirst (q '[:find ?e :in $ ?id :where [?e :oauth.account/id ?id]] id))
        current (when eid (raw-entity eid))
        now     (java.util.Date.)
        tx-data (cond-> [{:oauth.account/id            id
                          :oauth.account/name          (or name (clojure.core/name id))
                          :oauth.account/authorize-url authorize-url
                          :oauth.account/token-url     token-url
                          :oauth.account/client-id     client-id
                          :oauth.account/scopes        (or scopes "")
                          :oauth.account/autonomous-approved? (if (some? autonomous-approved?)
                                                                autonomous-approved?
                                                                (if (contains? current :oauth.account/autonomous-approved?)
                                                                  (:oauth.account/autonomous-approved? current)
                                                                  true))
                          :oauth.account/updated-at    now}]
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
    (transact! tx-data)))

(defn register-oauth-account!
  [oauth-account]
  (save-oauth-account! oauth-account))

(defn get-oauth-account [account-id]
  (let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :oauth.account/id ?id]] account-id))]
    (when eid (decrypt-entity (raw-entity eid)))))

(defn list-oauth-accounts []
  (let [eids (q '[:find ?e :where [?e :oauth.account/id _]])]
    (mapv #(decrypt-entity (raw-entity (first %))) eids)))

(defn remove-oauth-account! [account-id]
  (when-let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :oauth.account/id ?id]] account-id))]
    (transact! [[:db/retractEntity eid]])))

(defn oauth-account-in-use?
  [account-id]
  (boolean
    (ffirst
      (q '[:find ?e :in $ ?id
           :where
           [?e :service/oauth-account ?id]]
         account-id))))

;; ---------------------------------------------------------------------------
;; Tools (executable code)
;; ---------------------------------------------------------------------------

(declare get-tool)

(defn install-tool!
  [{:keys [id name description tags parameters handler approval execution-mode enabled? installed-at]}]
  (let [existing (when id (get-tool id))]
    (transact! [(cond-> {:tool/id           id
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

(defn get-tool [tool-id]
  (let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :tool/id ?id]] tool-id))]
    (when eid (raw-entity eid))))

(defn list-tools []
  (let [eids (q '[:find ?e :where [?e :tool/id _]])]
    (mapv #(raw-entity (first %)) eids)))

(defn enable-tool! [tool-id enabled?]
  (transact! [{:tool/id tool-id :tool/enabled? enabled?}]))
