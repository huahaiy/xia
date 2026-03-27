(ns xia.db
  "Datalevin database — the single source of truth for a xia instance.
   All state lives here: config, identity, memory, messages, skills, tools."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.core :as d]
            [datalevin.embedding :as emb]
            [datalevin.llm :as llm]
            [taoensso.timbre :as log]
            [xia.crypto :as crypto]
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
   :schedule.state/checkpoint {:db/valueType :db.type/idoc :db/domain "schedule-state-checkpoint"}
   :schedule.state/checkpoint-at {:db/valueType :db.type/instant}
   :schedule.state/last-success-at {:db/valueType :db.type/instant}
   :schedule.state/last-success-summary {:db/valueType :db.type/string}
   :schedule.state/last-failure-at {:db/valueType :db.type/instant}
   :schedule.state/last-error {:db/valueType :db.type/string}
   :schedule.state/last-failure-signature {:db/valueType :db.type/string}
   :schedule.state/last-recovery-hint {:db/valueType :db.type/string}
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
       (migrate-secrets!)
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
    (+ (long config-count) (long attr-count))))

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

;; ---------------------------------------------------------------------------
;; LLM Providers
;; ---------------------------------------------------------------------------

(defn upsert-provider! [{:keys [id] :as provider}]
  (let [provider-id                  (or id (:llm.provider/id provider))
        provider-eid                 (ffirst (q '[:find ?e :in $ ?id
                                                  :where [?e :llm.provider/id ?id]]
                                                provider-id))
        template-id                  (or (:llm.provider/template provider)
                                         (:template provider))
        access-mode                  (or (:llm.provider/access-mode provider)
                                         (:access-mode provider))
        credential-source            (or (:llm.provider/credential-source provider)
                                         (:credential-source provider)
                                         (:llm.provider/auth-type provider)
                                         (:auth-type provider))
        auth-type                    (or (:llm.provider/auth-type provider)
                                         (:auth-type provider))
        oauth-account                (or (:llm.provider/oauth-account provider)
                                         (:oauth-account provider))
        browser-session              (or (:llm.provider/browser-session provider)
                                         (:browser-session provider))
        workloads                    (some-> (or (:llm.provider/workloads provider)
                                                (:workloads provider))
                                             set)
        system-prompt-budget         (or (:llm.provider/system-prompt-budget provider)
                                         (:system-prompt-budget provider))
        history-budget               (or (:llm.provider/history-budget provider)
                                         (:history-budget provider))
        rate-limit-per-minute        (or (:llm.provider/rate-limit-per-minute provider)
                                         (:rate-limit-per-minute provider))
        vision?                      (or (:llm.provider/vision? provider)
                                         (:vision? provider))
        allow-private-network?       (or (:llm.provider/allow-private-network? provider)
                                         (:allow-private-network? provider))
        has-workloads?               (or (contains? provider :llm.provider/workloads)
                                         (contains? provider :workloads))
        has-template?                (or (contains? provider :llm.provider/template)
                                         (contains? provider :template))
        has-access-mode?             (or (contains? provider :llm.provider/access-mode)
                                         (contains? provider :access-mode))
        has-credential-source?       (or (contains? provider :llm.provider/credential-source)
                                         (contains? provider :credential-source)
                                         (contains? provider :llm.provider/auth-type)
                                         (contains? provider :auth-type))
        has-oauth-account?           (or (contains? provider :llm.provider/oauth-account)
                                         (contains? provider :oauth-account))
        has-browser-session?         (or (contains? provider :llm.provider/browser-session)
                                         (contains? provider :browser-session))
        has-vision?                  (or (contains? provider :llm.provider/vision?)
                                         (contains? provider :vision?))
        has-allow-private-network?   (or (contains? provider :llm.provider/allow-private-network?)
                                         (contains? provider :allow-private-network?))
        has-system-prompt-budget?    (or (contains? provider :llm.provider/system-prompt-budget)
                                         (contains? provider :system-prompt-budget))
        has-history-budget?          (or (contains? provider :llm.provider/history-budget)
                                         (contains? provider :history-budget))
        has-rate-limit?              (or (contains? provider :llm.provider/rate-limit-per-minute)
                                         (contains? provider :rate-limit-per-minute))
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
                                       (and has-template?
                                            (some? template-id))
                                       (assoc :llm.provider/template template-id)
                                       (and has-access-mode?
                                            (some? access-mode))
                                       (assoc :llm.provider/access-mode access-mode)
                                       (and has-credential-source?
                                            (some? credential-source))
                                       (assoc :llm.provider/credential-source credential-source)
                                       (and has-credential-source?
                                            (some? credential-source))
                                       (assoc :llm.provider/auth-type credential-source)
                                       (and has-oauth-account?
                                            (some? oauth-account))
                                       (assoc :llm.provider/oauth-account oauth-account)
                                       (and has-browser-session?
                                            (some? browser-session))
                                       (assoc :llm.provider/browser-session browser-session)
                                       (contains? provider :llm.provider/model)
                                       (assoc :llm.provider/model (:llm.provider/model provider))
                                       (contains? provider :model)
                                       (assoc :llm.provider/model (:model provider))
                                       (and has-workloads?
                                            (seq workloads))
                                       (assoc :llm.provider/workloads workloads)
                                       has-vision?
                                       (assoc :llm.provider/vision? (boolean vision?))
                                       has-allow-private-network?
                                       (assoc :llm.provider/allow-private-network? (boolean allow-private-network?))
                                       (and has-system-prompt-budget?
                                            (some? system-prompt-budget))
                                       (assoc :llm.provider/system-prompt-budget system-prompt-budget)
                                       (and has-history-budget?
                                            (some? history-budget))
                                       (assoc :llm.provider/history-budget history-budget)
                                       (and has-rate-limit?
                                            (some? rate-limit-per-minute))
                                       (assoc :llm.provider/rate-limit-per-minute rate-limit-per-minute)
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
                                            has-template?
                                            (nil? template-id))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/template])
                                       (and provider-eid
                                            has-access-mode?
                                            (nil? access-mode))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/access-mode])
                                       (and provider-eid
                                            has-credential-source?
                                            (nil? credential-source))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/credential-source])
                                       (and provider-eid
                                            has-credential-source?
                                            (nil? credential-source))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/auth-type])
                                       (and provider-eid
                                            has-oauth-account?
                                            (nil? oauth-account))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/oauth-account])
                                       (and provider-eid
                                            has-browser-session?
                                            (nil? browser-session))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/browser-session])
                                       (and provider-eid
                                            has-system-prompt-budget?
                                            (nil? system-prompt-budget))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/system-prompt-budget])
                                       (and provider-eid
                                            has-history-budget?
                                            (nil? history-budget))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/history-budget])
                                       (and provider-eid
                                            has-rate-limit?
                                            (nil? rate-limit-per-minute))
                                       (conj [:db/retract provider-eid
                                              :llm.provider/rate-limit-per-minute]))]
    (transact! (conj retracts provider-tx))))

(defn delete-provider! [provider-id]
  (when-let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :llm.provider/id ?id]]
                             provider-id))]
    (transact! [[:db/retractEntity eid]])))

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

(defn create-session!
  ([channel]
   (create-session! channel nil))
  ([channel {:keys [parent-session-id worker? label active?]
             :or   {worker? false
                    active? true}}]
   (let [id (random-uuid)]
     (transact!
       [(cond-> {:session/id      id
                 :session/channel channel
                 :session/worker? worker?
                 :session/active? active?}
          parent-session-id (assoc :session/parent-id parent-session-id)
          (some? label) (assoc :session/label label))])
     id)))

(defn list-sessions
  "List all sessions with basic metadata, newest first."
  ([] (list-sessions nil))
  ([{:keys [include-workers?] :or {include-workers? false}}]
   (->> (q '[:find ?s ?sid ?channel
             :where
             [?s :session/id ?sid]
             [?s :session/channel ?channel]])
        (map (fn [[eid sid channel]]
               (let [entity-map (raw-entity eid)]
                 {:id         sid
                  :channel    channel
                  :created-at (entity-created-at entity-map)
                  :active?    (boolean (:session/active? entity-map))
                  :worker?    (boolean (:session/worker? entity-map))
                  :parent-id  (:session/parent-id entity-map)
                  :label      (:session/label entity-map)})))
        (remove (fn [{:keys [worker?]}]
                  (and worker? (not include-workers?))))
        (sort-by :created-at #(compare %2 %1))
        vec)))

(defn set-session-active!
  [session-id active?]
  (when-let [session-eid (ffirst (q '[:find ?e :in $ ?sid
                                      :where [?e :session/id ?sid]]
                                    session-id))]
    (transact! [{:db/id            session-eid
                 :session/active? (boolean active?)}])
    true))

(defn- session-eid
  [session-id]
  (ffirst (q '[:find ?e :in $ ?sid
               :where [?e :session/id ?sid]]
             session-id)))

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

(defn- valid-session-artifact-ids
  [session-id artifact-ids]
  (let [artifact-ids* (->> artifact-ids
                           (keep normalize-message-local-doc-id)
                           distinct
                           vec)]
    (if-not (seq artifact-ids*)
      []
      (let [valid-ids (->> (q '[:find ?artifact-id
                                :in $ ?sid [?artifact-id ...]
                                :where
                                [?session :session/id ?sid]
                                [?artifact :artifact/session ?session]
                                [?artifact :artifact/id ?artifact-id]]
                              session-id
                              artifact-ids*)
                           (map first)
                           set)]
        (filterv valid-ids artifact-ids*)))))

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

(defn- message-artifacts
  [message-eid]
  (->> (q '[:find ?artifact-id ?name ?title ?status
            :in $ ?message
            :where
            [?ref :message.artifact-ref/message ?message]
            [?ref :message.artifact-ref/artifact ?artifact]
            [?artifact :artifact/id ?artifact-id]
            [(get-else $ ?artifact :artifact/name "") ?name]
            [(get-else $ ?artifact :artifact/title "") ?title]
            [(get-else $ ?artifact :artifact/status :ready) ?status]]
          message-eid)
       (mapv (fn [[artifact-id name title status]]
               {:id     artifact-id
                :name   (empty->nil name)
                :title  (empty->nil title)
                :status status}))))

(defn- message-token-estimate
  [{:keys [role content tool-result]}]
  (let [payload (cond
                  (string? tool-result) tool-result
                  (some? tool-result)   (pr-str tool-result)
                  :else                 (or content ""))
        role-overhead (case role
                        :tool 16
                        :assistant 8
                        :user 8
                        :system 8
                        8)]
    (+ role-overhead
       (max 1 (quot (count payload) 4)))))

(defn add-message!
  [session-id role content & {:keys [tool-calls tool-id tool-result local-doc-ids artifact-ids]}]
  (let [session-eid    (session-eid session-id)
        message-id     (random-uuid)
        doc-ids        (valid-session-local-doc-ids session-id local-doc-ids)
        artifact-ids* (valid-session-artifact-ids session-id artifact-ids)]
    (transact!
      (into
        [(cond-> {:message/id         message-id
                  :message/session    session-eid
                  :message/role       role
                  :message/content    (or content "")
                  :message/token-estimate (message-token-estimate {:role role
                                                                   :content content
                                                                   :tool-result tool-result})}
           tool-calls (assoc :message/tool-calls (tool-calls-doc tool-calls))
           (some? tool-result) (assoc :message/tool-result (tool-result-doc tool-result))
           tool-id    (assoc :message/tool-id tool-id))]
        (concat
          (map (fn [doc-id]
                 {:message.local-doc-ref/id      (random-uuid)
                  :message.local-doc-ref/message [:message/id message-id]
                  :message.local-doc-ref/doc     [:local.doc/id doc-id]})
               doc-ids)
          (map (fn [artifact-id]
                 {:message.artifact-ref/id       (random-uuid)
                  :message.artifact-ref/message  [:message/id message-id]
                  :message.artifact-ref/artifact [:artifact/id artifact-id]})
              artifact-ids*))))))

(defn- empty->nil [s] (when-not (= "" s) s))

(defn session-history-recap
  [session-id]
  (when-let [eid (session-eid session-id)]
    (let [entity-map (decrypt-entity (raw-entity eid))
          recap      (empty->nil (:session/history-recap entity-map))]
      (when recap
        {:content       recap
         :message-count (long (or (:session/history-recap-count entity-map) 0))
         :updated-at    (or (:session/history-recap-updated-at entity-map)
                            (entity-updated-at entity-map))}))))

(defn save-session-history-recap!
  [session-id content message-count]
  (when-let [eid (session-eid session-id)]
    (transact! [{:db/id eid
                 :session/history-recap content
                 :session/history-recap-count (long message-count)
                 :session/history-recap-updated-at (java.util.Date.)}])
    true))

(defn session-tool-recap
  [session-id]
  (when-let [eid (session-eid session-id)]
    (let [entity-map (decrypt-entity (raw-entity eid))
          recap      (empty->nil (:session/tool-recap entity-map))]
      (when recap
        {:content       recap
         :message-count (long (or (:session/tool-recap-count entity-map) 0))
         :updated-at    (or (:session/tool-recap-updated-at entity-map)
                            (entity-updated-at entity-map))}))))

(defn save-session-tool-recap!
  [session-id content message-count]
  (when-let [eid (session-eid session-id)]
    (transact! [{:db/id eid
                 :session/tool-recap content
                 :session/tool-recap-count (long message-count)
                 :session/tool-recap-updated-at (java.util.Date.)}])
    true))

(defn session-message-metadata
  [session-id]
  (->> (q '[:find ?m ?mid ?dca ?tokens
            :in $ ?sid
            :where
            [?s :session/id ?sid]
            [?m :message/session ?s]
            [?m :message/id ?mid]
            [(get-else $ ?m :db/created-at 0) ?dca]
            [(get-else $ ?m :message/token-estimate 0) ?tokens]]
          session-id)
       (map (fn [[eid mid created-at token-estimate]]
              {:eid eid
               :id mid
               :created-at (epoch-millis->date created-at)
               :token-estimate (long token-estimate)}))
       (sort-by (juxt :created-at :eid))
       vec))

(defn session-messages-by-eids
  [message-eids]
  (let [message-eids* (vec message-eids)]
    (if-not (seq message-eids*)
      []
      (let [by-eid
            (into {}
                  (map (fn [[eid mid content role tool-calls tool-result tool-id]]
                         [eid
                          (let [tool-result* (read-tool-result-doc tool-result)]
                            {:id          mid
                             :role        role
                             :content     (when-not (and (= role :tool) (some? tool-result*))
                                            (decrypt-secret-attr :message/content content))
                             :created-at  (entity-created-at (raw-entity eid))
                             :local-docs  (not-empty (message-local-docs eid))
                             :artifacts   (not-empty (message-artifacts eid))
                             :tool-calls  (read-tool-calls-doc tool-calls)
                             :tool-result tool-result*
                             :tool-id     (empty->nil tool-id)})])
                       (q '[:find ?m ?mid ?content ?role ?tc ?tr ?tid
                            :in $ [?m ...]
                            :where
                            [?m :message/id ?mid]
                            [?m :message/role ?role]
                            [?m :message/content ?content]
                            [(get-else $ ?m :message/tool-calls "") ?tc]
                            [(get-else $ ?m :message/tool-result "") ?tr]
                            [(get-else $ ?m :message/tool-id "") ?tid]]
                          message-eids*)))]
        (->> message-eids*
             (keep by-eid)
             vec)))))

(defn session-messages
  "Get all messages for a session, ordered by creation time."
  [session-id]
  (->> (session-message-metadata session-id)
       (mapv :eid)
       session-messages-by-eids))

;; ---------------------------------------------------------------------------
;; Skills (markdown instructions)
;; ---------------------------------------------------------------------------

(defn install-skill!
  [{:keys [id name description content doc version tags
           source-format source-path source-url source-name
           import-warnings
           imported-from-openclaw?]}]
  (transact! [(cond-> {:skill/id           id
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

(defn save-service!
  [{:keys [id name base-url auth-type auth-key auth-header oauth-account enabled?
           autonomous-approved?] :as service}]
  (let [allow-private-network? (or (:service/allow-private-network? service)
                                   (:allow-private-network? service))
        base-url (validate-service-base-url! base-url allow-private-network?)
        eid     (ffirst (q '[:find ?e :in $ ?id :where [?e :service/id ?id]] id))
        current (when eid (raw-entity eid))
        rate-limit-per-minute (or (:service/rate-limit-per-minute service)
                                  (:rate-limit-per-minute service))
        has-rate-limit? (or (contains? service :service/rate-limit-per-minute)
                            (contains? service :rate-limit-per-minute))
        has-allow-private-network? (or (contains? service :service/allow-private-network?)
                                       (contains? service :allow-private-network?))
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
    (transact! tx-data)))

(defn register-service! [service]
  (save-service! service))

(defn get-service [service-id]
  (let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :service/id ?id]] service-id))]
    (when eid (decrypt-entity (raw-entity eid)))))

(defn list-services []
  (let [eids (q '[:find ?e :where [?e :service/id _]])]
    (mapv #(decrypt-entity (raw-entity (first %))) eids)))

(defn remove-service! [service-id]
  (when-let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :service/id ?id]] service-id))]
    (transact! [[:db/retractEntity eid]])))

(defn enable-service! [service-id enabled?]
  (transact! [{:service/id service-id :service/enabled? enabled?}]))

;; ---------------------------------------------------------------------------
;; OAuth accounts
;; ---------------------------------------------------------------------------

(defn save-oauth-account!
  [{:keys [id name connection-mode authorize-url token-url client-id client-secret scopes
           provider-template redirect-uri auth-params token-params access-token refresh-token
           token-type expires-at connected-at autonomous-approved?]}]
  (let [eid     (ffirst (q '[:find ?e :in $ ?id :where [?e :oauth.account/id ?id]] id))
        current (when eid (raw-entity eid))
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
           (or [?e :service/oauth-account ?id]
               [?e :llm.provider/oauth-account ?id])]
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

;; ---------------------------------------------------------------------------
;; LLM Call Log
;; ---------------------------------------------------------------------------

(def ^:private llm-log-retention-ms (* 30 24 60 60 1000)) ; 30 days

(defn- prune-llm-log!
  "Delete entries older than 30 days."
  []
  (let [cutoff (java.util.Date. (- (.getTime (java.util.Date.)) llm-log-retention-ms))
        old    (q '[:find ?e :in $ ?cutoff
                    :where
                    [?e :llm.log/id _]
                    [?e :llm.log/created-at ?t]
                    [(< ?t ?cutoff)]]
                  cutoff)]
    (when (seq old)
      (transact! (mapv (fn [[eid]] [:db/retractEntity eid]) old)))))

(defn log-llm-call!
  "Write an LLM call log entry. `entry` is a map with keys matching :llm.log/* attrs.
   Automatically prunes entries beyond the retention limit."
  [entry]
  (transact! [(merge {:llm.log/id         (or (:id entry) (random-uuid))
                      :llm.log/created-at (or (:created-at entry) (java.util.Date.))}
                     (when-let [v (:session-id entry)]  {:llm.log/session-id v})
                     (when-let [v (:provider-id entry)] {:llm.log/provider-id v})
                     (when-let [v (:model entry)]       {:llm.log/model v})
                     (when-let [v (:workload entry)]    {:llm.log/workload v})
                     (when-let [v (:messages entry)]    {:llm.log/messages v})
                     (when-let [v (:tools entry)]       {:llm.log/tools v})
                     (when-let [v (:response entry)]    {:llm.log/response v})
                     (when-let [v (:status entry)]      {:llm.log/status v})
                     (when-let [v (:error entry)]       {:llm.log/error v})
                     (when-let [v (:duration-ms entry)] {:llm.log/duration-ms v})
                     (when-let [v (:prompt-tokens entry)]     {:llm.log/prompt-tokens v})
                     (when-let [v (:completion-tokens entry)] {:llm.log/completion-tokens v}))])
  (prune-llm-log!))

(defn list-llm-calls
  "Return recent LLM call log entries, newest first. `limit` defaults to 50."
  ([] (list-llm-calls 50))
  ([limit]
   (->> (q '[:find ?e ?t
             :where
             [?e :llm.log/id _]
             [?e :llm.log/created-at ?t]])
        (sort-by second #(compare %2 %1))
        (take limit)
        (mapv (fn [[eid _]]
                (let [e (raw-entity eid)]
                  {:id               (:llm.log/id e)
                   :session-id       (:llm.log/session-id e)
                   :provider-id      (:llm.log/provider-id e)
                   :model            (:llm.log/model e)
                   :workload         (:llm.log/workload e)
                   :status           (:llm.log/status e)
                   :error            (:llm.log/error e)
                   :duration-ms      (:llm.log/duration-ms e)
                   :prompt-tokens    (:llm.log/prompt-tokens e)
                   :completion-tokens (:llm.log/completion-tokens e)
                   :created-at       (:llm.log/created-at e)}))))))

(defn get-llm-call
  "Return a single LLM call log entry with full messages/response."
  [call-id]
  (when-let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :llm.log/id ?id]] call-id))]
    (let [e (raw-entity eid)]
      {:id               (:llm.log/id e)
       :session-id       (:llm.log/session-id e)
       :provider-id      (:llm.log/provider-id e)
       :model            (:llm.log/model e)
       :workload         (:llm.log/workload e)
       :messages         (:llm.log/messages e)
       :tools            (:llm.log/tools e)
       :response         (:llm.log/response e)
       :status           (:llm.log/status e)
       :error            (:llm.log/error e)
       :duration-ms      (:llm.log/duration-ms e)
       :prompt-tokens    (:llm.log/prompt-tokens e)
       :completion-tokens (:llm.log/completion-tokens e)
       :created-at       (:llm.log/created-at e)})))
