(ns xia.db
  "Datalevin database — the single source of truth for a xia instance.
   All state lives here: config, identity, memory, messages, skills, tools."
  (:require [charred.api :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.core :as d]
            [datalevin.embedding :as emb]
            [datalevin.llm :as llm]
            [taoensso.timbre :as log]
            [xia.crypto :as crypto]
            [xia.db.catalog :as db-catalog]
            [xia.db.provider :as db-provider]
            [xia.db.session :as db-session]
            [xia.db.task :as db-task]
            [xia.paths :as paths]
            [xia.runtime-state :as runtime-state]
            [xia.sensitive :as sensitive])
  (:import [java.io InputStream]
           [java.net URI]
           [java.net.http HttpClient HttpClient$Redirect HttpRequest HttpResponse$BodyHandlers]
           [java.nio.file Files Path Paths StandardCopyOption]
           [java.nio.file.attribute FileAttribute]
           [java.time Duration Instant]
           [java.util UUID]))

;; ---------------------------------------------------------------------------
;; Schema
;; ---------------------------------------------------------------------------

(def episode-text-domain "episode-text")
(def kg-node-domain "kg-node")
(def kg-fact-domain "kg-fact")
(def kg-edge-domain "kg-edge")
(def local-doc-domain "local-doc")
(def local-doc-chunk-domain "local-doc-chunk")
(def artifact-domain "artifact")
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
   :llm.provider/template {:db/valueType :db.type/keyword}
   :llm.provider/access-mode {:db/valueType :db.type/keyword}
   :llm.provider/credential-source {:db/valueType :db.type/keyword}
   :llm.provider/auth-type {:db/valueType :db.type/keyword}
   :llm.provider/oauth-account {:db/valueType :db.type/keyword}
   :llm.provider/browser-session {:db/valueType :db.type/string}
   :llm.provider/model    {:db/valueType :db.type/string}
   :llm.provider/workloads {:db/valueType :db.type/keyword
                            :db/cardinality :db.cardinality/many}
   :llm.provider/vision? {:db/valueType :db.type/boolean}
   :llm.provider/allow-private-network? {:db/valueType :db.type/boolean}
   :llm.provider/system-prompt-budget {:db/valueType :db.type/long}
   :llm.provider/history-budget {:db/valueType :db.type/long}
   :llm.provider/rate-limit-per-minute {:db/valueType :db.type/long}
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
   :session/external-key {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :session/external-meta {:db/valueType :db.type/idoc :db/domain "session-external-meta"}
   :session/parent-id  {:db/valueType :db.type/uuid}
   :session/worker?    {:db/valueType :db.type/boolean}
   :session/label      {:db/valueType :db.type/string}
   :session/history-recap {:db/valueType :db.type/string}
   :session/history-recap-count {:db/valueType :db.type/long}
   :session/history-recap-updated-at {:db/valueType :db.type/instant}
   :session/tool-recap {:db/valueType :db.type/string}
   :session/tool-recap-count {:db/valueType :db.type/long}
   :session/tool-recap-updated-at {:db/valueType :db.type/instant}
   :session/created-at {:db/valueType :db.type/instant}
   :session/active?    {:db/valueType :db.type/boolean}

   ;; --- Task Runtime ---
   :task/id            {:db/valueType :db.type/uuid :db/unique :db.unique/identity}
   :task/session       {:db/valueType :db.type/ref}
   :task/parent-id     {:db/valueType :db.type/uuid}
   :task/channel       {:db/valueType :db.type/keyword}
   :task/type          {:db/valueType :db.type/keyword}
   :task/state         {:db/valueType :db.type/keyword}
   :task/title         {:db/valueType :db.type/string}
   :task/summary       {:db/valueType :db.type/string}
   :task/stop-reason   {:db/valueType :db.type/keyword}
   :task/error         {:db/valueType :db.type/string}
   :task/meta          {:db/valueType :db.type/idoc :db/domain "task-meta"}
   :task/autonomy-state {:db/valueType :db.type/idoc :db/domain "task-autonomy-state"}
   :task/current-turn-id {:db/valueType :db.type/uuid}
   :task/created-at    {:db/valueType :db.type/instant}
   :task/updated-at    {:db/valueType :db.type/instant}
   :task/started-at    {:db/valueType :db.type/instant}
   :task/finished-at   {:db/valueType :db.type/instant}

   :task.session-link/task       {:db/valueType :db.type/ref}
   :task.session-link/session    {:db/valueType :db.type/ref}
   :task.session-link/role       {:db/valueType :db.type/keyword}
   :task.session-link/created-at {:db/valueType :db.type/instant}
   :task.session-link/updated-at {:db/valueType :db.type/instant}

   :task.turn/id                   {:db/valueType :db.type/uuid :db/unique :db.unique/identity}
   :task.turn/task                 {:db/valueType :db.type/ref}
   :task.turn/index                {:db/valueType :db.type/long}
   :task.turn/operation            {:db/valueType :db.type/keyword}
   :task.turn/state                {:db/valueType :db.type/keyword}
   :task.turn/input                {:db/valueType :db.type/string}
   :task.turn/summary              {:db/valueType :db.type/string}
   :task.turn/error                {:db/valueType :db.type/string}
   :task.turn/meta                 {:db/valueType :db.type/idoc :db/domain "task-turn-meta"}
   :task.turn/interrupting-turn-id {:db/valueType :db.type/uuid}
   :task.turn/created-at           {:db/valueType :db.type/instant}
   :task.turn/updated-at           {:db/valueType :db.type/instant}
   :task.turn/started-at           {:db/valueType :db.type/instant}
   :task.turn/finished-at          {:db/valueType :db.type/instant}

   :task.item/id          {:db/valueType :db.type/uuid :db/unique :db.unique/identity}
   :task.item/turn        {:db/valueType :db.type/ref}
   :task.item/index       {:db/valueType :db.type/long}
   :task.item/type        {:db/valueType :db.type/keyword}
   :task.item/status      {:db/valueType :db.type/keyword}
   :task.item/role        {:db/valueType :db.type/keyword}
   :task.item/summary     {:db/valueType :db.type/string}
   :task.item/message-id  {:db/valueType :db.type/uuid}
   :task.item/llm-call-id {:db/valueType :db.type/uuid}
   :task.item/tool-id     {:db/valueType :db.type/string}
   :task.item/tool-call-id {:db/valueType :db.type/string}
   :task.item/data        {:db/valueType :db.type/idoc :db/domain "task-item-data"}
   :task.item/created-at  {:db/valueType :db.type/instant}

   ;; --- Message ---
   :message/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :message/session    {:db/valueType :db.type/ref}
   :message/role       {:db/valueType :db.type/keyword} ; :user :assistant :system :tool
   :message/content    {:db/valueType :db.type/string}
   :message/token-estimate {:db/valueType :db.type/long}
   :message/created-at {:db/valueType :db.type/instant}
   :message/tool-calls {:db/valueType :db.type/idoc :db/domain "message-tool-calls"}
   :message/tool-result {:db/valueType :db.type/idoc :db/domain "message-tool-result"}
   :message/tool-id    {:db/valueType :db.type/string}  ; for tool-result messages
   :message/tool-call-id {:db/valueType :db.type/string}
   :message/tool-name   {:db/valueType :db.type/string}
   :message/llm-call-id {:db/valueType :db.type/uuid}
   :message/provider-id {:db/valueType :db.type/keyword}
   :message/model       {:db/valueType :db.type/string}
   :message/workload    {:db/valueType :db.type/keyword}
   :message.local-doc-ref/id      {:db/valueType :db.type/uuid :db/unique :db.unique/identity}
   :message.local-doc-ref/message {:db/valueType :db.type/ref}
   :message.local-doc-ref/doc     {:db/valueType :db.type/ref}
   :message.artifact-ref/id       {:db/valueType :db.type/uuid :db/unique :db.unique/identity}
   :message.artifact-ref/message  {:db/valueType :db.type/ref}
   :message.artifact-ref/artifact {:db/valueType :db.type/ref}

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
   :local.doc/summary-source {:db/valueType :db.type/keyword}
   :local.doc/summarized-at {:db/valueType :db.type/instant}
   :local.doc/summary    {:db/valueType :db.type/string
                          :db/fulltext true
                          :db.fulltext/domains [local-doc-domain]
                          :db/embedding true
                          :db.embedding/domains [local-doc-domain]}
   :local.doc/text       {:db/valueType :db.type/string
                          :db/fulltext true
                          :db.fulltext/domains [local-doc-domain]
                          :db/embedding true
                          :db.embedding/domains [local-doc-domain]}
   :local.doc/preview    {:db/valueType :db.type/string}
   :local.doc/chunk-count {:db/valueType :db.type/long}
   :local.doc/chunks     {:db/valueType :db.type/ref
                          :db/cardinality :db.cardinality/many}

   :local.doc.chunk/id      {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :local.doc.chunk/doc     {:db/valueType :db.type/ref}
   :local.doc.chunk/session {:db/valueType :db.type/ref}
   :local.doc.chunk/index   {:db/valueType :db.type/long}
   :local.doc.chunk/summary-source {:db/valueType :db.type/keyword}
   :local.doc.chunk/summarized-at {:db/valueType :db.type/instant}
   :local.doc.chunk/summary {:db/valueType :db.type/string
                             :db/fulltext true
                             :db.fulltext/domains [local-doc-chunk-domain]
                             :db/embedding true
                             :db.embedding/domains [local-doc-chunk-domain]}
   :local.doc.chunk/text    {:db/valueType :db.type/string
                             :db/fulltext true
                             :db.fulltext/domains [local-doc-chunk-domain]
                             :db/embedding true
                             :db.embedding/domains [local-doc-chunk-domain]}
   :local.doc.chunk/preview {:db/valueType :db.type/string}

   ;; --- Generated Artifacts (session-scoped outputs users can download) ---
   :artifact/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :artifact/session    {:db/valueType :db.type/ref}
   :artifact/name       {:db/valueType :db.type/string
                         :db/fulltext true
                         :db.fulltext/domains [artifact-domain]
                         :db/embedding true
                         :db.embedding/domains [artifact-domain]}
   :artifact/title      {:db/valueType :db.type/string
                         :db/fulltext true
                         :db.fulltext/domains [artifact-domain]
                         :db/embedding true
                         :db.embedding/domains [artifact-domain]}
   :artifact/kind       {:db/valueType :db.type/keyword}
   :artifact/media-type {:db/valueType :db.type/string}
   :artifact/extension  {:db/valueType :db.type/string}
   :artifact/source     {:db/valueType :db.type/keyword}
   :artifact/status     {:db/valueType :db.type/keyword}
   :artifact/size-bytes {:db/valueType :db.type/long}
   :artifact/sha256     {:db/valueType :db.type/string}
   :artifact/blob-id    {:db/valueType :db.type/uuid}
   :artifact/blob-codec {:db/valueType :db.type/keyword}
   :artifact/compressed-size-bytes {:db/valueType :db.type/long}
   :artifact/error      {:db/valueType :db.type/string}
   :artifact/meta       {:db/valueType :db.type/idoc    :db/domain "artifact-meta"}
   :artifact/text       {:db/valueType :db.type/string
                         :db/fulltext true
                         :db.fulltext/domains [artifact-domain]
                         :db/embedding true
                         :db.embedding/domains [artifact-domain]}
   :artifact/preview    {:db/valueType :db.type/string}

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
   :skill/source-format {:db/valueType :db.type/keyword}
   :skill/source-path   {:db/valueType :db.type/string}
   :skill/source-url    {:db/valueType :db.type/string}
   :skill/source-name   {:db/valueType :db.type/string}
   :skill/import-warnings {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   :skill/imported-from-openclaw? {:db/valueType :db.type/boolean}

   ;; --- Remote Bridge (notification/status bridge state) ---
   :remote.bridge/id           {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :remote.bridge/enabled?     {:db/valueType :db.type/boolean}
   :remote.bridge/instance-id  {:db/valueType :db.type/string}
   :remote.bridge/instance-label {:db/valueType :db.type/string}
   :remote.bridge/relay-url    {:db/valueType :db.type/string}
   :remote.bridge/public-key   {:db/valueType :db.type/string}
   :remote.bridge/connected-at {:db/valueType :db.type/instant}
   :remote.bridge/last-seen-at {:db/valueType :db.type/instant}

   :remote.device/id          {:db/valueType :db.type/uuid :db/unique :db.unique/identity}
   :remote.device/name        {:db/valueType :db.type/string}
   :remote.device/public-key  {:db/valueType :db.type/string}
   :remote.device/platform    {:db/valueType :db.type/keyword}
   :remote.device/push-token  {:db/valueType :db.type/string}
   :remote.device/status      {:db/valueType :db.type/keyword}
   :remote.device/topics      {:db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   :remote.device/muted?      {:db/valueType :db.type/boolean}
   :remote.device/created-at  {:db/valueType :db.type/instant}
   :remote.device/last-seen-at {:db/valueType :db.type/instant}

   :remote.event/id          {:db/valueType :db.type/uuid :db/unique :db.unique/identity}
   :remote.event/type        {:db/valueType :db.type/keyword}
   :remote.event/topic       {:db/valueType :db.type/keyword}
   :remote.event/severity    {:db/valueType :db.type/keyword}
   :remote.event/title       {:db/valueType :db.type/string}
   :remote.event/detail      {:db/valueType :db.type/string}
   :remote.event/metadata    {:db/valueType :db.type/idoc :db/domain "remote-event-metadata"}
   :remote.event/status      {:db/valueType :db.type/keyword}
   :remote.event/device-id   {:db/valueType :db.type/uuid}
   :remote.event/created-at  {:db/valueType :db.type/instant}
   :remote.event/delivered-at {:db/valueType :db.type/instant}

   ;; --- Working Memory (crash-recovery snapshots) ---
   :wm/id               {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :wm/session           {:db/valueType :db.type/ref}     ; → session
   :wm/topics            {:db/valueType :db.type/string}
   :wm/autonomy-state    {:db/valueType :db.type/string}
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
   :service/allow-private-network? {:db/valueType :db.type/boolean}
   :service/autonomous-approved? {:db/valueType :db.type/boolean}
   :service/enabled?    {:db/valueType :db.type/boolean}

   ;; --- Managed Child Xia Instances (controller-side durable supervision records) ---
   :managed.child/id          {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :managed.child/name        {:db/valueType :db.type/string}
   :managed.child/service-id  {:db/valueType :db.type/keyword}
   :managed.child/service-name {:db/valueType :db.type/string}
   :managed.child/base-url    {:db/valueType :db.type/string}
   :managed.child/template-instance {:db/valueType :db.type/string}
   :managed.child/state       {:db/valueType :db.type/keyword}
   :managed.child/pid         {:db/valueType :db.type/long}
   :managed.child/log-path    {:db/valueType :db.type/string}
   :managed.child/created-at  {:db/valueType :db.type/instant}
   :managed.child/updated-at  {:db/valueType :db.type/instant}
   :managed.child/started-at  {:db/valueType :db.type/instant}
   :managed.child/exited-at   {:db/valueType :db.type/instant}
   :managed.child/exit-code   {:db/valueType :db.type/long}

   ;; --- OAuth Accounts (authorization-code + PKCE / refresh-token auth) ---
   :oauth.account/id            {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :oauth.account/name          {:db/valueType :db.type/string}
   :oauth.account/connection-mode {:db/valueType :db.type/keyword}
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

   ;; --- Schedule State (durable recovery/checkpoint state) ---
   :schedule.state/schedule-id {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :schedule.state/status {:db/valueType :db.type/keyword}
   :schedule.state/phase {:db/valueType :db.type/keyword}
   :schedule.state/task-id {:db/valueType :db.type/uuid}
   :schedule.state/checkpoint {:db/valueType :db.type/idoc :db/domain "schedule-state-checkpoint"}
   :schedule.state/checkpoint-at {:db/valueType :db.type/instant}
   :schedule.state/last-success-at {:db/valueType :db.type/instant}
   :schedule.state/last-success-summary {:db/valueType :db.type/string}
   :schedule.state/last-failure-at {:db/valueType :db.type/instant}
   :schedule.state/last-error {:db/valueType :db.type/string}
   :schedule.state/last-failure-signature {:db/valueType :db.type/string}
   :schedule.state/last-recovery-hint {:db/valueType :db.type/string}
   :schedule.state/last-policy {:db/valueType :db.type/idoc :db/domain "schedule-state-policy"}
   :schedule.state/consecutive-failures {:db/valueType :db.type/long}
   :schedule.state/backoff-until {:db/valueType :db.type/instant}

   ;; --- Schedule Run (execution log) ---
   :schedule-run/id          {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :schedule-run/schedule-id {:db/valueType :db.type/keyword}
   :schedule-run/started-at  {:db/valueType :db.type/instant}
   :schedule-run/finished-at {:db/valueType :db.type/instant}
   :schedule-run/status      {:db/valueType :db.type/keyword} ; :success :error
   :schedule-run/result      {:db/valueType :db.type/string}
   :schedule-run/error       {:db/valueType :db.type/string}
   :schedule-run/actions     {:db/valueType :db.type/idoc :db/domain "schedule-run-actions"}
   :schedule-run/meta        {:db/valueType :db.type/idoc :db/domain "schedule-run-meta"}

   ;; --- LLM Call Log (debug/observability for every LLM request) ---
   :llm.log/id          {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :llm.log/session-id  {:db/valueType :db.type/uuid}
   :llm.log/provider-id {:db/valueType :db.type/keyword}
   :llm.log/model       {:db/valueType :db.type/string}
   :llm.log/workload    {:db/valueType :db.type/keyword}
   :llm.log/messages    {:db/valueType :db.type/string}   ; JSON — input messages array
   :llm.log/tools       {:db/valueType :db.type/string}   ; JSON — tool definitions (if any)
   :llm.log/response    {:db/valueType :db.type/string}   ; JSON — full response body
   :llm.log/status      {:db/valueType :db.type/keyword}  ; :ok :error
   :llm.log/error       {:db/valueType :db.type/string}
   :llm.log/duration-ms {:db/valueType :db.type/long}
   :llm.log/prompt-tokens   {:db/valueType :db.type/long}
   :llm.log/completion-tokens {:db/valueType :db.type/long}
   :llm.log/created-at  {:db/valueType :db.type/instant}

   ;; --- Session Audit Event (interactive audit trail) ---
   :audit.event/id          {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :audit.event/session-id  {:db/valueType :db.type/uuid}
   :audit.event/channel     {:db/valueType :db.type/keyword}
   :audit.event/actor       {:db/valueType :db.type/keyword}
   :audit.event/type        {:db/valueType :db.type/keyword}
   :audit.event/message-id  {:db/valueType :db.type/uuid}
   :audit.event/llm-call-id {:db/valueType :db.type/uuid}
   :audit.event/tool-id     {:db/valueType :db.type/string}
   :audit.event/tool-call-id {:db/valueType :db.type/string}
   :audit.event/data        {:db/valueType :db.type/string}
   :audit.event/created-at  {:db/valueType :db.type/instant}

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
(defonce ^:private embedding-provider-atom (atom nil))
(defonce ^:private llm-provider-atom (atom nil))
(defonce ^:private db-path-atom (atom nil))
(defonce ^:private instance-id-atom (atom nil))
(defonce ^:private last-connect-event-atom (atom nil))
(defonce ^:private last-close-event-atom (atom nil))

(defn- stack-frame->summary
  [^StackTraceElement frame]
  (str (.getClassName frame) "/" (.getMethodName frame) ":" (.getLineNumber frame)))

(defn- capture-callsite
  [label]
  (let [throwable (Throwable. label)]
    {:throwable throwable
     :summary   (->> (.getStackTrace throwable)
                     (drop 1)
                     (map stack-frame->summary)
                     (take 12)
                     vec)}))

(defn- lifecycle-event
  [phase callsite]
  {:at          (Instant/now)
   :phase       phase
   :db-path     @db-path-atom
   :instance-id @instance-id-atom
   :callsite    (:summary callsite)})

(defn last-connect-event
  "Return the most recent successful connect event for debugging."
  []
  @last-connect-event-atom)

(defn last-close-event
  "Return the most recent close event for debugging."
  []
  @last-close-event-atom)
(declare migrate-secrets!)

(def ^:private default-embedding-provider-id
  ;; Datalevin persists embedding domain provider ids and only accepts its
  ;; built-in ids when reopening a store, so Xia's managed default must use
  ;; the concrete provider keyword here.
  :llama.cpp)

(def ^:private default-embedding-model-file
  "nomic-embed-text-v2-moe-q8_0.gguf")

(def ^:private default-embedding-model-url
  (str "https://huggingface.co/ggml-org/Nomic-Embed-Text-V2-GGUF/resolve/main/"
       default-embedding-model-file
       "?download=true"))

(def ^:private embedding-model-lock
  (Object.))

(def ^:private default-llm-model-file
  "gemma-3-4b-it.Q4_K_M.gguf")

(def ^:private default-llm-model-url
  (str "https://huggingface.co/MaziyarPanahi/gemma-3-4b-it-GGUF/resolve/main/"
       default-llm-model-file
       "?download=true"))

(def ^:private llm-model-lock
  (Object.))

(def ^:private default-embedding-provider-spec
  ;; Keep Xia's provider choice centralized so the default model can change
  ;; without touching the rest of the DB wiring. Let Datalevin derive
  ;; embedding metadata from the GGUF manifest and runtime provider output
  ;; instead of hard-coding dimensions here.
  {:provider :llama.cpp
   :model-id "nomic-ai/nomic-embed-text-v2-moe"
   :model-filename default-embedding-model-file
   :model-url default-embedding-model-url})

(def ^:private default-llm-provider-spec
  ;; Keep local summarization inside the Xia binary by using Datalevin's
  ;; in-process llama.cpp runtime plus a managed GGUF support file. This is
  ;; opt-in at the summarizer layer, so the default managed model should be
  ;; capable enough to matter when users enable it.
  {:provider :llama.cpp
   :model-id "google/gemma-3-4b-it"
   :model-filename default-llm-model-file
   :model-url default-llm-model-url
   :ctx-size 4096})

(defn- default-embedding-domains
  []
  (->> schema
       vals
       (mapcat #(or (:db.embedding/domains %) []))
       set
       sort
       (map (fn [domain]
              [domain {:provider default-embedding-provider-id}]))
       (into {})))

(def ^:private default-datalevin-opts-map
  {:embedding-opts      {:provider default-embedding-provider-id
                         :metric-type :cosine}
   :embedding-domains   (default-embedding-domains)
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
                 (str (paths/managed-embed-dir db-path)
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
      (.connectTimeout (Duration/ofSeconds 20))
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
                               (.timeout (Duration/ofMinutes 30))
                               (.GET)
                               (.build))
        ^"[Ljava.nio.file.CopyOption;" copy-opts
        (into-array java.nio.file.CopyOption
                    [StandardCopyOption/REPLACE_EXISTING])]
    (try
      (let [resp   (.send client req (HttpResponse$BodyHandlers/ofInputStream))
            status (.statusCode resp)]
        (when-not (= 200 status)
          (throw (ex-info "Failed to download managed model"
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

(defn- announce-managed-model-download!
  [artifact-label provider-spec]
  (let [model-path (or (:model provider-spec) (:model-path provider-spec))
        model-id   (or (:model-id provider-spec)
                       (:model-filename provider-spec)
                       "managed-model")
        message    (str "Downloading Xia "
                        artifact-label
                        " model "
                        model-id
                        " to "
                        model-path
                        ". This may take a few minutes the first time.")]
    (log/info message)
    (println message)
    (flush)))

(defn- ensure-managed-model!
  [provider-spec lock artifact-label]
  (let [model-path (or (:model provider-spec) (:model-path provider-spec))]
    (cond
      (not (map? provider-spec))
      provider-spec

      (or (nil? model-path)
          (nil? (:model-url provider-spec))
          (.exists (io/file model-path)))
      provider-spec

      :else
      (locking lock
        (when-not (.exists (io/file model-path))
          (announce-managed-model-download! artifact-label provider-spec)
          (download-file! (:model-url provider-spec) model-path))
        provider-spec))))

(defn- ensure-managed-embedding-model!
  [provider-spec]
  (ensure-managed-model! provider-spec embedding-model-lock "embedding"))

(defn- close-embedding-provider! []
  (when-let [provider @embedding-provider-atom]
    (try
      (d/close-embedding-provider provider)
      (catch Exception _))
    (reset! embedding-provider-atom nil)))

(defn- resolve-llm-provider-spec
  [db-path options]
  (let [runtime-entry (if (contains? options :local-llm-provider)
                        (:local-llm-provider options)
                        default-llm-provider-spec)]
    (cond
      (or (false? runtime-entry) (= :disabled runtime-entry))
      nil

      (satisfies? llm/ILLMProvider runtime-entry)
      runtime-entry

      (map? runtime-entry)
      (let [provider-spec (merge {:provider :llama.cpp
                                  :dir      db-path}
                                 runtime-entry)]
        (cond-> provider-spec
          (and (nil? (:model provider-spec))
               (nil? (:model-path provider-spec))
               (string? (:model-filename provider-spec)))
          (assoc :model-path
                 (str (paths/managed-llm-dir db-path)
                      java.io.File/separator
                      (:model-filename provider-spec)))))

      (keyword? runtime-entry)
      {:provider runtime-entry
       :dir      db-path}

      (nil? runtime-entry)
      nil

      :else
      runtime-entry)))

(defn- lazy-managed-llm-provider
  [provider-spec]
  (let [provider* (atom nil)
        closed?   (atom false)
        ensure!   (fn []
                    (when @closed?
                      (throw (ex-info "Local LLM provider is closed"
                                      {:provider-spec provider-spec})))
                    (or @provider*
                        (locking provider*
                          (or @provider*
                              (let [managed-spec (ensure-managed-model! provider-spec
                                                                       llm-model-lock)
                                    provider (d/new-llm-provider managed-spec)]
                                (reset! provider* provider)
                                provider)))))]
    (reify
      llm/ILLMProvider
      (generate-text* [_ prompt max-tokens opts]
        (llm/generate-text* (ensure!) prompt max-tokens opts))
      (summarize-text* [_ text max-tokens opts]
        (llm/summarize-text* (ensure!) text max-tokens opts))
      (llm-metadata [_]
        (llm/llm-metadata (ensure!)))
      (llm-context-size [_]
        (llm/llm-context-size (ensure!)))
      (close-llm-provider [_]
        (when (compare-and-set! closed? false true)
          (when-let [provider @provider*]
            (llm/close-llm-provider provider))))

      java.lang.AutoCloseable
      (close [this]
        (llm/close-llm-provider this)))))

(defn- close-llm-provider! []
  (when-let [provider @llm-provider-atom]
    (try
      (d/close-llm-provider provider)
      (catch Exception _))
    (reset! llm-provider-atom nil)))

(defn- prepare-managed-embedding-runtime!
  [datalevin-opts db-path]
  (let [provider-spec (resolve-embedding-provider-spec db-path datalevin-opts)]
    (ensure-managed-embedding-model! provider-spec)
    datalevin-opts))

(defn- init-embedding-provider!
  [db-path datalevin-opts]
  (let [provider-spec (-> (resolve-embedding-provider-spec db-path datalevin-opts)
                          ensure-managed-embedding-model!)]
    (close-embedding-provider!)
    (let [provider (if (satisfies? emb/IEmbeddingProvider provider-spec)
                     provider-spec
                     (d/new-embedding-provider provider-spec))]
      (try
        (d/embedding-dimensions provider)
        (reset! embedding-provider-atom provider)
        provider
        (catch Throwable t
          (when-not (identical? provider provider-spec)
            (try
              (d/close-embedding-provider provider)
              (catch Exception _)))
          (throw t))))))

(defn- init-llm-provider!
  [db-path options]
  (close-llm-provider!)
  (when-let [provider-spec (resolve-llm-provider-spec db-path options)]
    (let [provider (if (satisfies? llm/ILLMProvider provider-spec)
                     provider-spec
                     (lazy-managed-llm-provider provider-spec))]
      (reset! llm-provider-atom provider)
      provider)))

(defn connect!
  "Open (or create) the Datalevin database at `db-path`."
  ([db-path] (connect! db-path nil))
  ([db-path crypto-opts]
   (let [callsite       (capture-callsite "db/connect! callsite")
         instance-id    (paths/resolve-instance-id (:instance-id crypto-opts))
         datalevin-opts (-> (resolve-datalevin-opts crypto-opts)
                            (prepare-managed-embedding-runtime! db-path))
         c              (d/get-conn db-path schema datalevin-opts)]
     (try
       (reset! conn-atom c)
       (reset! db-path-atom db-path)
       (reset! instance-id-atom instance-id)
       (crypto/configure! db-path crypto-opts)
       (init-embedding-provider! db-path datalevin-opts)
       (init-llm-provider! db-path crypto-opts)
       (reset! last-connect-event-atom
               (assoc (lifecycle-event (runtime-state/phase) callsite)
                      :db-path db-path
                      :instance-id instance-id))
       c
       (catch Throwable t
         (close-llm-provider!)
         (close-embedding-provider!)
         (reset! conn-atom nil)
         (reset! db-path-atom nil)
         (reset! instance-id-atom nil)
         (try
           (d/close c)
           (catch Exception _))
         (throw t))))))

(defn conn
  "Return the current connection. Throws if not connected."
  []
  (or @conn-atom
      (throw (ex-info "Database not connected. Call (xia.db/connect!) first." {}))))

(defn current-embedding-provider
  []
  @embedding-provider-atom)

(defn current-llm-provider
  []
  @llm-provider-atom)

(defn current-db-path
  []
  @db-path-atom)

(defn current-instance-id
  []
  @instance-id-atom)

(defn close! []
  (let [phase    (runtime-state/phase)
        callsite (capture-callsite "db/close! callsite")
        event    (lifecycle-event phase callsite)]
    (reset! last-close-event-atom event)
    (if (= phase :running)
      (log/error (:throwable callsite)
                 "Unexpected db/close! while Xia runtime is still marked running"
                 "db-path" (:db-path event)
                 "instance" (:instance-id event))
      (log/info "Closing database"
                "phase" (name phase)
                "db-path" (:db-path event)
                "instance" (:instance-id event))))
  (close-llm-provider!)
  (close-embedding-provider!)
  (when-let [c @conn-atom]
    (d/close c)
    (reset! conn-atom nil))
  (reset! db-path-atom nil)
  (reset! instance-id-atom nil))

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

(defn- epoch-millis->date
  [millis]
  (java.util.Date. (long millis)))

(defn- entity-created-at
  [entity-map]
  (or (:session/created-at entity-map)
      (:message/created-at entity-map)
      (some-> (:db/created-at entity-map) long epoch-millis->date)))

(defn- entity-updated-at
  [entity-map]
  (or (:wm/updated-at entity-map)
      (:oauth.account/updated-at entity-map)
      (some-> (:db/updated-at entity-map) long epoch-millis->date)))

(defn- raw-entity [eid]
  (into {} (d/entity (d/db (conn)) eid)))

(defn- decrypt-entity [entity-map]
  (reduce-kv (fn [acc k v]
               (assoc acc k (decrypt-secret-attr k v)))
             {}
             entity-map))

(defn- migrate-secret-attr-tx [eid attr value]
  (when (and (string? value)
             (not (str/blank? value))
             (not (crypto/encrypted? value)))
    [:db/add eid attr value]))

(defn- migrate-secret-config-tx [eid config-key value]
  (when (and (sensitive/secret-config-key? config-key)
             (string? value)
             (not (str/blank? value))
             (not (crypto/encrypted? value)))
    {:db/id eid
     :config/key config-key
     :config/value value}))

(def ^:private secret-migration-batch-size 200)

(defn- migrate-secrets!
  []
  (let [config-tx
        (into []
              (keep (fn [[eid k v]]
                      (migrate-secret-config-tx eid k v)))
              (vec
                (q '[:find ?e ?k ?v
                     :where
                     [?e :config/key ?k]
                     [?e :config/value ?v]])))
        attr-tx
        (into []
              (keep identity)
              (for [attr sensitive/encrypted-attrs
                    [eid value] (vec
                                  (q '[:find ?e ?v
                                       :in $ ?attr
                                       :where
                                       [?e ?attr ?v]]
                                     attr))]
                (migrate-secret-attr-tx eid attr value)))
        tx-data (into config-tx attr-tx)]
    (when (seq tx-data)
      (log/info "Migrating" (count tx-data) "legacy secret values in"
                (count (partition-all secret-migration-batch-size tx-data))
                "batches"))
    (doseq [batch (partition-all secret-migration-batch-size tx-data)]
      (transact! batch))
    (count tx-data)))

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

(def ^:private template-identity-keys
  [:name :role :description :personality :guidelines])

(def ^:private template-config-keys
  #{:user/name
    :web/search-backend
    :web/search-brave-api-key
    :web/search-searxng-url
    :context/recent-history-message-limit
    :memory/episode-full-resolution-ms
    :memory/episode-decay-half-life-ms
    :memory/episode-retained-decayed-count
    :memory/knowledge-decay-grace-period-ms
    :memory/knowledge-decay-half-life-ms
    :memory/knowledge-decay-min-confidence
    :memory/knowledge-decay-maintenance-step-ms
    :memory/knowledge-decay-archive-after-bottom-ms
    :local-doc/model-summaries-enabled?
    :local-doc/model-summary-backend
    :local-doc/model-summary-provider-id
    :local-doc/chunk-summary-max-tokens
    :local-doc/doc-summary-max-tokens
    :local-doc/ocr-enabled?
    :local-doc/ocr-backend
    :local-doc/ocr-provider-id
    :local-doc/ocr-command
    :local-doc/ocr-model-path
    :local-doc/ocr-mmproj-path
    :local-doc/ocr-spotting-mmproj-path
    :local-doc/ocr-timeout-ms
    :local-doc/ocr-max-tokens
    :backup/enabled?
    :backup/directory
    :backup/interval-hours
    :backup/retain-count})

(declare list-oauth-accounts
         list-providers
         list-services
         list-site-creds
         save-oauth-account!
         upsert-provider!
         set-default-provider!
         save-service!
         save-site-cred!)

(defn initial-settings-empty?
  []
  (and (nil? (get-config :setup/complete))
       (every? #(nil? (get-identity %)) template-identity-keys)
       (every? #(nil? (get-config %)) template-config-keys)
       (empty? (list-oauth-accounts))
       (empty? (list-providers))
       (empty? (list-services))
       (empty? (list-site-creds))))

(defn- decrypt-template-secret-attr
  [attr value skipped-secret-count]
  (if (and value (sensitive/encrypted-attr? attr))
    (try
      (crypto/decrypt value (attr-aad attr))
      (catch Exception _
        (swap! skipped-secret-count inc)
        nil))
    value))

(defn- decrypt-template-config-value
  [config-key value skipped-secret-count]
  (if (and value (sensitive/secret-config-key? config-key))
    (try
      (crypto/decrypt value (config-aad config-key))
      (catch Exception _
        (swap! skipped-secret-count inc)
        nil))
    value))

(defn- source-db-entities
  [db-value unique-attr]
  (->> (d/q '[:find ?e :in $ ?attr :where [?e ?attr _]]
            db-value
            unique-attr)
       (mapv (fn [[eid]]
               (into {} (d/entity db-value eid))))))

(defn- compact-map
  [m]
  (reduce-kv (fn [acc k v]
               (if (nil? v)
                 acc
                 (assoc acc k v)))
             {}
             m))

(defn- decrypt-template-entity
  [entity-map skipped-secret-count]
  (reduce-kv
    (fn [acc k v]
      (if (keyword? k)
        (let [value* (decrypt-template-secret-attr k v skipped-secret-count)]
          (if (nil? value*)
            acc
            (assoc acc k value*)))
        (assoc acc k v)))
    {}
    entity-map))

(defn- template-provider-record
  [provider]
  (let [credential-source (:llm.provider/credential-source provider)]
    (cond-> {:id       (:llm.provider/id provider)
             :name     (:llm.provider/name provider)
             :base-url (:llm.provider/base-url provider)
             :model    (:llm.provider/model provider)}
      (contains? provider :llm.provider/api-key)
      (assoc :api-key (:llm.provider/api-key provider))
      (contains? provider :llm.provider/template)
      (assoc :template (:llm.provider/template provider))
      (contains? provider :llm.provider/access-mode)
      (assoc :access-mode (:llm.provider/access-mode provider))
      (and (contains? provider :llm.provider/credential-source)
           (not= credential-source :browser-session))
      (assoc :credential-source credential-source
             :auth-type credential-source)
      (contains? provider :llm.provider/oauth-account)
      (assoc :oauth-account (:llm.provider/oauth-account provider))
      (contains? provider :llm.provider/workloads)
      (assoc :workloads (:llm.provider/workloads provider))
      (contains? provider :llm.provider/vision?)
      (assoc :vision? (:llm.provider/vision? provider))
      (contains? provider :llm.provider/allow-private-network?)
      (assoc :allow-private-network? (:llm.provider/allow-private-network? provider))
      (contains? provider :llm.provider/system-prompt-budget)
      (assoc :system-prompt-budget (:llm.provider/system-prompt-budget provider))
      (contains? provider :llm.provider/history-budget)
      (assoc :history-budget (:llm.provider/history-budget provider))
      (contains? provider :llm.provider/rate-limit-per-minute)
      (assoc :rate-limit-per-minute (:llm.provider/rate-limit-per-minute provider))
      (contains? provider :llm.provider/default?)
      (assoc :default? (:llm.provider/default? provider)))))

(defn- template-oauth-account-record
  [account]
  (let [has-token? (or (contains? account :oauth.account/access-token)
                       (contains? account :oauth.account/refresh-token))]
    (cond-> {:id     (:oauth.account/id account)
             :name   (:oauth.account/name account)
             :scopes (:oauth.account/scopes account)}
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
      (and has-token?
           (contains? account :oauth.account/token-type))
      (assoc :token-type (:oauth.account/token-type account))
      (and has-token?
           (contains? account :oauth.account/expires-at))
      (assoc :expires-at (:oauth.account/expires-at account))
      (and has-token?
           (contains? account :oauth.account/connected-at))
      (assoc :connected-at (:oauth.account/connected-at account))
      (contains? account :oauth.account/autonomous-approved?)
      (assoc :autonomous-approved? (:oauth.account/autonomous-approved? account)))))

(defn- template-service-record
  [service]
  (cond-> {:id       (:service/id service)
           :name     (:service/name service)
           :base-url (:service/base-url service)
           :auth-type (:service/auth-type service)}
    (contains? service :service/auth-key)
    (assoc :auth-key (:service/auth-key service))
    (contains? service :service/auth-header)
    (assoc :auth-header (:service/auth-header service))
    (contains? service :service/oauth-account)
    (assoc :oauth-account (:service/oauth-account service))
    (contains? service :service/rate-limit-per-minute)
    (assoc :rate-limit-per-minute (:service/rate-limit-per-minute service))
    (contains? service :service/allow-private-network?)
    (assoc :allow-private-network? (:service/allow-private-network? service))
    (contains? service :service/autonomous-approved?)
    (assoc :autonomous-approved? (:service/autonomous-approved? service))
    (contains? service :service/enabled?)
    (assoc :enabled? (:service/enabled? service))))

(defn- template-site-record
  [site]
  (cond-> {:id             (:site-cred/id site)
           :name           (:site-cred/name site)
           :login-url      (:site-cred/login-url site)
           :username-field (:site-cred/username-field site)
           :password-field (:site-cred/password-field site)}
    (contains? site :site-cred/username)
    (assoc :username (:site-cred/username site))
    (contains? site :site-cred/password)
    (assoc :password (:site-cred/password site))
    (contains? site :site-cred/form-selector)
    (assoc :form-selector (:site-cred/form-selector site))
    (contains? site :site-cred/extra-fields)
    (assoc :extra-fields (:site-cred/extra-fields site))
    (contains? site :site-cred/autonomous-approved?)
    (assoc :autonomous-approved? (:site-cred/autonomous-approved? site))))

(defn- read-template-snapshot
  [source-conn skipped-secret-count]
  (let [source-db  (d/db source-conn)
        identities (reduce
                     (fn [acc identity-key]
                       (if-let [value (ffirst (d/q '[:find ?v :in $ ?k
                                                     :where
                                                     [?e :identity/key ?k]
                                                     [?e :identity/value ?v]]
                                                   source-db
                                                   identity-key))]
                         (assoc acc identity-key value)
                         acc))
                     {}
                     template-identity-keys)
        configs    (reduce
                     (fn [acc [config-key value]]
                       (let [value* (decrypt-template-config-value config-key
                                                                   value
                                                                   skipped-secret-count)]
                         (if (nil? value*)
                           acc
                           (assoc acc config-key value*))))
                     {}
                     (for [[config-key value] (d/q '[:find ?k ?v
                                                     :where
                                                     [?e :config/key ?k]
                                                     [?e :config/value ?v]]
                                                   source-db)
                           :when (contains? template-config-keys config-key)]
                       [config-key value]))
        oauth-accounts (->> (source-db-entities source-db :oauth.account/id)
                            (map #(decrypt-template-entity % skipped-secret-count))
                            (map template-oauth-account-record)
                            (map compact-map)
                            vec)
        providers      (->> (source-db-entities source-db :llm.provider/id)
                            (map #(decrypt-template-entity % skipped-secret-count))
                            (map template-provider-record)
                            (map compact-map)
                            vec)
        services       (->> (source-db-entities source-db :service/id)
                            (map #(decrypt-template-entity % skipped-secret-count))
                            (map template-service-record)
                            (map compact-map)
                            vec)
        sites          (->> (source-db-entities source-db :site-cred/id)
                            (map #(decrypt-template-entity % skipped-secret-count))
                            (map template-site-record)
                            (map compact-map)
                            vec)]
    {:identities         identities
     :configs            configs
     :oauth-accounts     oauth-accounts
     :providers          providers
     :services           services
     :sites              sites
     :default-provider-id (some->> providers
                                   (filter :default?)
                                   first
                                   :id)}))

(defn seed-initial-settings-from-db!
  [{:keys [source-db-path crypto-opts]}]
  (let [target-db-path (current-db-path)]
    (when-not target-db-path
      (throw (ex-info "Target database is not connected"
                      {:source-db-path source-db-path})))
    (when-not (.exists (io/file source-db-path))
      (throw (ex-info "Template source database does not exist"
                      {:source-db-path source-db-path})))
    (if-not (initial-settings-empty?)
      {:seeded? false
       :reason :target-not-empty}
      (let [skipped-secret-count (atom 0)
            snapshot             (try
                                   (crypto/configure! source-db-path crypto-opts)
                                   (let [source-conn (d/get-conn source-db-path
                                                                 schema
                                                                 (resolve-datalevin-opts crypto-opts))]
                                     (try
                                       (read-template-snapshot source-conn skipped-secret-count)
                                       (finally
                                         (d/close source-conn))))
                                   (finally
                                     (crypto/configure! target-db-path crypto-opts)))]
        (doseq [[identity-key value] (:identities snapshot)]
          (set-identity! identity-key value))
        (doseq [[config-key value] (:configs snapshot)]
          (set-config! config-key value))
        (doseq [account (:oauth-accounts snapshot)]
          (save-oauth-account! account))
        (doseq [provider (:providers snapshot)]
          (upsert-provider! provider))
        (when-let [default-provider-id (:default-provider-id snapshot)]
          (set-default-provider! default-provider-id))
        (doseq [service (:services snapshot)]
          (save-service! service))
        (doseq [site (:sites snapshot)]
          (save-site-cred! site))
        (when (seq (:providers snapshot))
          (set-config! :setup/complete "true"))
        {:seeded? true
         :identity-count (count (:identities snapshot))
         :config-count (count (:configs snapshot))
         :oauth-account-count (count (:oauth-accounts snapshot))
         :provider-count (count (:providers snapshot))
         :service-count (count (:services snapshot))
         :site-count (count (:sites snapshot))
         :skipped-secret-count @skipped-secret-count}))))

(declare provider-deps)

;; ---------------------------------------------------------------------------
;; LLM Providers
;; ---------------------------------------------------------------------------

(defn upsert-provider! [{:keys [id] :as provider}]
  (db-provider/upsert-provider! (provider-deps) provider))

(defn delete-provider! [provider-id]
  (db-provider/delete-provider! (provider-deps) provider-id))

(defn get-default-provider []
  (db-provider/get-default-provider (provider-deps)))

(defn get-provider [provider-id]
  (db-provider/get-provider (provider-deps) provider-id))

(defn list-providers []
  (db-provider/list-providers (provider-deps)))

(defn set-default-provider!
  "Mark exactly one provider as the default."
  [provider-id]
  (db-provider/set-default-provider! (provider-deps) provider-id))

;; ---------------------------------------------------------------------------
;; Memory — episodic and knowledge graph operations are in xia.memory
;; The DB layer just provides the schema; memory.clj has the logic.
;; ---------------------------------------------------------------------------

;; ---------------------------------------------------------------------------
;; Working Memory snapshots (crash-recovery)
;; ---------------------------------------------------------------------------

(defn- session-deps
  []
  {:conn                conn
   :decrypt-entity      decrypt-entity
   :decrypt-secret-attr decrypt-secret-attr
   :entity-created-at   entity-created-at
   :entity-updated-at   entity-updated-at
   :epoch-millis->date  epoch-millis->date
   :q                   q
   :raw-entity          raw-entity
   :transact!           transact!})

(defn- catalog-deps
  []
  {:decrypt-entity decrypt-entity
   :q              q
   :raw-entity     raw-entity
   :transact!      transact!})

(defn- provider-deps
  []
  {:decrypt-entity decrypt-entity
   :q              q
   :raw-entity     raw-entity
   :transact!      transact!})

(defn- task-deps
  []
  {:decrypt-entity    decrypt-entity
   :entity-created-at entity-created-at
   :entity-updated-at entity-updated-at
   :q                 q
   :raw-entity        raw-entity
   :transact!         transact!})

(defn save-wm-snapshot!
  "Persist working memory state to DB for crash recovery."
  [snapshot]
  (db-session/save-wm-snapshot! (session-deps) snapshot))

(defn load-wm-snapshot
  "Load the most recent WM snapshot for a session."
  [session-id]
  (db-session/load-wm-snapshot (session-deps) session-id))

(defn latest-session-episode
  "Get the most recent episode summary for any session."
  []
  (db-session/latest-session-episode (session-deps)))

;; ---------------------------------------------------------------------------
;; Sessions & Messages
;; ---------------------------------------------------------------------------

(defn create-session!
  ([channel]
   (create-session! channel nil))
  ([channel opts]
   (db-session/create-session! (session-deps) channel opts)))

(defn find-session-by-external-key
  [external-key]
  (db-session/find-session-by-external-key (session-deps) external-key))

(defn session-external-meta
  [session-id]
  (db-session/session-external-meta (session-deps) session-id))

(defn save-session-external-meta!
  [session-id external-meta]
  (db-session/save-session-external-meta! (session-deps) session-id external-meta))

(defn list-sessions
  "List all sessions with basic metadata, newest first."
  ([] (list-sessions nil))
  ([{:keys [include-workers?] :or {include-workers? false}}]
   (db-session/list-sessions (session-deps)
                             {:include-workers? include-workers?})))

(defn set-session-active!
  [session-id active?]
  (db-session/set-session-active! (session-deps) session-id active?))

(defn add-message!
  [session-id role content & {:keys [tool-calls tool-id tool-call-id tool-name tool-result
                                     llm-call-id provider-id model workload
                                     local-doc-ids artifact-ids]}]
  (apply db-session/add-message! (session-deps) session-id role content
         (concat [:tool-calls tool-calls
                  :tool-id tool-id
                  :tool-call-id tool-call-id
                  :tool-name tool-name
                  :tool-result tool-result
                  :llm-call-id llm-call-id
                  :provider-id provider-id
                  :model model
                  :workload workload
                  :local-doc-ids local-doc-ids
                  :artifact-ids artifact-ids])))

(defn session-history-recap
  [session-id]
  (db-session/session-history-recap (session-deps) session-id))

(defn save-session-history-recap!
  [session-id content message-count]
  (db-session/save-session-history-recap! (session-deps) session-id content message-count))

(defn session-tool-recap
  [session-id]
  (db-session/session-tool-recap (session-deps) session-id))

(defn save-session-tool-recap!
  [session-id content message-count]
  (db-session/save-session-tool-recap! (session-deps) session-id content message-count))

(defn session-message-metadata
  [session-id]
  (db-session/session-message-metadata (session-deps) session-id))

(defn session-message-count
  [session-id]
  (db-session/session-message-count (session-deps) session-id))

(defn session-message-eids-range
  "Return message entity ids for a session in stable oldest->newest order for
   the half-open range [start, end). When `total-count` is supplied, reuse it
   to avoid an extra count query."
  ([session-id start end]
   (session-message-eids-range session-id start end nil))
  ([session-id start end total-count]
   (db-session/session-message-eids-range (session-deps) session-id start end total-count)))

(defn session-message-metadata-range
  "Return message metadata for a session in stable oldest->newest order for the
   half-open range [start, end). Only the requested window is hydrated."
  ([session-id start end]
   (session-message-metadata-range session-id start end nil))
  ([session-id start end total-count]
   (db-session/session-message-metadata-range (session-deps) session-id start end total-count)))

(defn session-messages-by-eids
  [message-eids]
  (db-session/session-messages-by-eids (session-deps) message-eids))

(defn latest-session-message
  ([session-id]
   (latest-session-message session-id nil))
  ([session-id roles]
   (db-session/latest-session-message (session-deps) session-id roles)))

(defn session-messages
  "Get all messages for a session, ordered by creation time."
  [session-id]
  (db-session/session-messages (session-deps) session-id))

;; ---------------------------------------------------------------------------
;; Tasks
;; ---------------------------------------------------------------------------

(defn create-task!
  [task]
  (db-task/create-task! (task-deps) task))

(defn update-task!
  [task-id attrs]
  (db-task/update-task! (task-deps) task-id attrs))

(defn get-task
  [task-id]
  (db-task/get-task (task-deps) task-id))

(defn list-tasks
  ([] (list-tasks nil))
  ([opts]
   (db-task/list-tasks (task-deps) opts)))

(defn current-session-task
  [session-id]
  (db-task/current-session-task (task-deps) session-id))

(defn attach-task-session!
  ([task-id session-id]
   (attach-task-session! task-id session-id nil))
  ([task-id session-id opts]
   (db-task/attach-task-session! (task-deps) task-id session-id opts)))

(defn task-session-links
  [task-id]
  (db-task/task-session-links (task-deps) task-id))

(defn start-task-turn!
  [task-id opts]
  (db-task/start-task-turn! (task-deps) task-id opts))

(defn update-task-turn!
  [turn-id attrs]
  (db-task/update-task-turn! (task-deps) turn-id attrs))

(defn task-turns
  [task-id]
  (db-task/task-turns (task-deps) task-id))

(defn get-task-turn
  [turn-id]
  (db-task/get-task-turn (task-deps) turn-id))

(defn add-task-item!
  [turn-id item]
  (db-task/add-task-item! (task-deps) turn-id item))

(defn turn-items
  [turn-id]
  (db-task/turn-items (task-deps) turn-id))

(defn get-task-item
  [item-id]
  (db-task/get-task-item (task-deps) item-id))

;; ---------------------------------------------------------------------------
;; Skills (markdown instructions)
;; ---------------------------------------------------------------------------

(defn install-skill!
  [skill]
  (db-catalog/install-skill! (catalog-deps) skill))

(defn save-skill!
  [skill]
  (db-catalog/save-skill! (catalog-deps) skill))

(defn get-skill [skill-id]
  (db-catalog/get-skill (catalog-deps) skill-id))

(defn list-skills []
  (db-catalog/list-skills (catalog-deps)))

(defn remove-skill! [skill-id]
  (db-catalog/remove-skill! (catalog-deps) skill-id))

(defn enable-skill! [skill-id enabled?]
  (db-catalog/enable-skill! (catalog-deps) skill-id enabled?))

(defn find-skills-by-tags
  "Find skills matching any of the given tags."
  [tags]
  (db-catalog/find-skills-by-tags (catalog-deps) tags))

;; ---------------------------------------------------------------------------
;; Site Credentials (website login credentials)
;; ---------------------------------------------------------------------------

(defn save-site-cred!
  [site-cred]
  (db-catalog/save-site-cred! (catalog-deps) site-cred))

(defn register-site-cred!
  [site-cred]
  (db-catalog/register-site-cred! (catalog-deps) site-cred))

(defn get-site-cred [site-id]
  (db-catalog/get-site-cred (catalog-deps) site-id))

(defn list-site-creds []
  (db-catalog/list-site-creds (catalog-deps)))

(defn remove-site-cred! [site-id]
  (db-catalog/remove-site-cred! (catalog-deps) site-id))

;; ---------------------------------------------------------------------------
;; Services (external service registrations)
;; ---------------------------------------------------------------------------

(defn save-service!
  [service]
  (db-catalog/save-service! (catalog-deps) service))

(defn register-service! [service]
  (db-catalog/register-service! (catalog-deps) service))

(defn get-service [service-id]
  (db-catalog/get-service (catalog-deps) service-id))

(defn list-services []
  (db-catalog/list-services (catalog-deps)))

(defn remove-service! [service-id]
  (db-catalog/remove-service! (catalog-deps) service-id))

(defn enable-service! [service-id enabled?]
  (db-catalog/enable-service! (catalog-deps) service-id enabled?))

;; ---------------------------------------------------------------------------
;; Managed child Xia instances (durable controller-side records)
;; ---------------------------------------------------------------------------

(defn save-managed-child!
  [child]
  (db-catalog/save-managed-child! (catalog-deps) child))

(defn get-managed-child
  [instance-id]
  (db-catalog/get-managed-child (catalog-deps) instance-id))

(defn list-managed-children
  []
  (db-catalog/list-managed-children (catalog-deps)))

(defn remove-managed-child!
  [instance-id]
  (db-catalog/remove-managed-child! (catalog-deps) instance-id))

;; ---------------------------------------------------------------------------
;; OAuth accounts
;; ---------------------------------------------------------------------------

(defn save-oauth-account!
  [oauth-account]
  (db-catalog/save-oauth-account! (catalog-deps) oauth-account))

(defn register-oauth-account!
  [oauth-account]
  (db-catalog/register-oauth-account! (catalog-deps) oauth-account))

(defn get-oauth-account [account-id]
  (db-catalog/get-oauth-account (catalog-deps) account-id))

(defn list-oauth-accounts []
  (db-catalog/list-oauth-accounts (catalog-deps)))

(defn remove-oauth-account! [account-id]
  (db-catalog/remove-oauth-account! (catalog-deps) account-id))

(defn oauth-account-in-use?
  [account-id]
  (db-catalog/oauth-account-in-use? (catalog-deps) account-id))

;; ---------------------------------------------------------------------------
;; Tools (executable code)
;; ---------------------------------------------------------------------------

(defn install-tool!
  [tool]
  (db-catalog/install-tool! (catalog-deps) tool))

(defn get-tool [tool-id]
  (db-catalog/get-tool (catalog-deps) tool-id))

(defn list-tools []
  (db-catalog/list-tools (catalog-deps)))

(defn enable-tool! [tool-id enabled?]
  (db-catalog/enable-tool! (catalog-deps) tool-id enabled?))

;; ---------------------------------------------------------------------------
;; LLM Call Log
;; ---------------------------------------------------------------------------

(defn log-llm-call!
  "Write an LLM call log entry. `entry` is a map with keys matching :llm.log/* attrs.
   Automatically prunes entries beyond the retention limit."
  [entry]
  (db-session/log-llm-call! (session-deps) entry))

(defn list-llm-calls
  "Return recent LLM call log entries, newest first. `limit` defaults to 50."
  ([] (list-llm-calls 50))
  ([limit]
   (list-llm-calls limit nil))
  ([limit session-id]
   (db-session/list-llm-calls (session-deps) limit session-id)))

(defn get-llm-call
  "Return a single LLM call log entry with full messages/response."
  [call-id]
  (db-session/get-llm-call (session-deps) call-id))

(defn log-audit-event!
  [entry]
  (db-session/log-audit-event! (session-deps) entry))

(defn session-audit-events
  ([session-id]
   (session-audit-events session-id 500))
  ([session-id limit]
   (db-session/session-audit-events (session-deps) session-id limit)))
