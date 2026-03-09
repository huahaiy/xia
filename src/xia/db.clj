(ns xia.db
  "Datalevin database — the single source of truth for a xia instance.
   All state lives here: config, identity, memory, messages, skills, tools."
  (:require [datalevin.core :as d]))

;; ---------------------------------------------------------------------------
;; Schema
;; ---------------------------------------------------------------------------

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
   :llm.provider/default? {:db/valueType :db.type/boolean}

   ;; --- Episodic Memory ---
   :episode/id           {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :episode/type         {:db/valueType :db.type/keyword} ; :conversation :event :observation
   :episode/summary      {:db/valueType :db.type/string  :db/fulltext true}
   :episode/context      {:db/valueType :db.type/string  :db/fulltext true} ; situational context
   :episode/participants {:db/valueType :db.type/string}  ; who was involved
   :episode/channel      {:db/valueType :db.type/string}
   :episode/session-id   {:db/valueType :db.type/string}  ; link back to session
   :episode/timestamp    {:db/valueType :db.type/instant}
   :episode/processed?   {:db/valueType :db.type/boolean}  ; consolidated by hippocampus?

   ;; --- Knowledge Graph: Nodes ---
   :kg.node/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :kg.node/name       {:db/valueType :db.type/string  :db/fulltext true}
   :kg.node/type       {:db/valueType :db.type/keyword} ; :person :place :thing :concept :preference
   :kg.node/properties {:db/valueType :db.type/idoc :db/domain "node-props"} ; structured properties (idoc)
   :kg.node/created-at {:db/valueType :db.type/instant}
   :kg.node/updated-at {:db/valueType :db.type/instant}

   ;; --- Knowledge Graph: Edges (relationships) ---
   :kg.edge/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :kg.edge/from       {:db/valueType :db.type/ref}     ; → kg.node
   :kg.edge/to         {:db/valueType :db.type/ref}     ; → kg.node
   :kg.edge/type       {:db/valueType :db.type/keyword} ; :knows :likes :works-at :uses etc.
   :kg.edge/label      {:db/valueType :db.type/string  :db/fulltext true} ; human-readable description
   :kg.edge/weight     {:db/valueType :db.type/float}   ; confidence/strength
   :kg.edge/source     {:db/valueType :db.type/ref}     ; → episode (provenance)
   :kg.edge/created-at {:db/valueType :db.type/instant}

   ;; --- Knowledge Graph: Facts (atomic knowledge about a node) ---
   :kg.fact/id         {:db/valueType :db.type/uuid    :db/unique :db.unique/identity}
   :kg.fact/node       {:db/valueType :db.type/ref}     ; → kg.node
   :kg.fact/content    {:db/valueType :db.type/string  :db/fulltext true}
   :kg.fact/confidence {:db/valueType :db.type/float}
   :kg.fact/source     {:db/valueType :db.type/ref}     ; → episode (provenance)
   :kg.fact/created-at {:db/valueType :db.type/instant}
   :kg.fact/updated-at {:db/valueType :db.type/instant}

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
   :message/tool-calls {:db/valueType :db.type/string}  ; JSON-encoded tool calls
   :message/tool-id    {:db/valueType :db.type/string}  ; for tool-result messages

   ;; --- Skill (markdown/text instructions the LLM follows) ---
   :skill/id           {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :skill/name         {:db/valueType :db.type/string}
   :skill/description  {:db/valueType :db.type/string}  ; short summary for selection
   :skill/content      {:db/valueType :db.type/string  :db/fulltext true} ; raw markdown for prompt injection + FTS
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

   ;; --- Tool (executable code the LLM can call via function-calling) ---
   :tool/id            {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :tool/name          {:db/valueType :db.type/string}
   :tool/description   {:db/valueType :db.type/string}
   :tool/parameters    {:db/valueType :db.type/string}  ; JSON schema for parameters
   :tool/handler       {:db/valueType :db.type/string}  ; SCI code → fn
   :tool/enabled?      {:db/valueType :db.type/boolean}
   :tool/installed-at  {:db/valueType :db.type/instant}})

;; ---------------------------------------------------------------------------
;; Connection management
;; ---------------------------------------------------------------------------

(defonce ^:private conn-atom (atom nil))

(defn connect!
  "Open (or create) the Datalevin database at `db-path`."
  [db-path]
  (let [c (d/get-conn db-path schema)]
    (reset! conn-atom c)
    c))

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

(defn transact! [tx-data]
  (d/transact! (conn) tx-data))

(defn q [query & inputs]
  (apply d/q query (d/db (conn)) inputs))

(defn entity [eid]
  (d/entity (d/db (conn)) eid))

;; ---------------------------------------------------------------------------
;; Config
;; ---------------------------------------------------------------------------

(defn set-config! [k v]
  (transact! [{:config/key k :config/value (str v)}]))

(defn get-config [k]
  (ffirst (q '[:find ?v :in $ ?k :where [?e :config/key ?k] [?e :config/value ?v]] k)))

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
  (transact! [(assoc provider :llm.provider/id id)]))

(defn get-default-provider []
  (let [results (q '[:find ?e :where
                     [?e :llm.provider/default? true]])]
    (when-let [eid (ffirst results)]
      (into {} (d/entity (d/db (conn)) eid)))))

(defn list-providers []
  (let [eids (q '[:find ?e :where [?e :llm.provider/id _]])]
    (mapv #(into {} (d/entity (d/db (conn)) (first %))) eids)))

;; ---------------------------------------------------------------------------
;; Memory — episodic and knowledge graph operations are in xia.memory
;; The DB layer just provides the schema; memory.clj has the logic.
;; ---------------------------------------------------------------------------

;; ---------------------------------------------------------------------------
;; Working Memory snapshots (crash-recovery)
;; ---------------------------------------------------------------------------

(defn save-wm-snapshot!
  "Persist working memory state to DB for crash recovery."
  [{:keys [session-id topics slots episode-refs]}]
  (let [session-eid (ffirst (q '[:find ?e :in $ ?sid
                                  :where [?e :session/id ?sid]]
                                session-id))
        wm-id       (random-uuid)
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
                episode-refs))))
    wm-id))

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
           :updated-at (:wm/updated-at wm-entity)})))))

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
                 :session/created-at (java.util.Date.)
                 :session/active?    true}])
    id))

(defn add-message! [session-id role content & {:keys [tool-calls tool-id]}]
  (let [session-eid (ffirst (q '[:find ?e :in $ ?sid
                                 :where [?e :session/id ?sid]]
                               session-id))]
    (transact! [(cond-> {:message/id         (random-uuid)
                         :message/session    session-eid
                         :message/role       role
                         :message/content    (or content "")
                         :message/created-at (java.util.Date.)}
                  tool-calls (assoc :message/tool-calls tool-calls)
                  tool-id    (assoc :message/tool-id tool-id))])))

(defn- empty->nil [s] (when-not (= "" s) s))

(defn session-messages
  "Get all messages for a session, ordered by creation time."
  [session-id]
  (sort-by :created-at
           (map (fn [[content role created tool-calls tool-id]]
                  {:role       role
                   :content    content
                   :created-at created
                   :tool-calls (empty->nil tool-calls)
                   :tool-id    (empty->nil tool-id)})
                (q '[:find ?content ?role ?created ?tc ?tid
                     :in $ ?sid
                     :where
                     [?s :session/id ?sid]
                     [?m :message/session ?s]
                     [?m :message/role ?role]
                     [?m :message/content ?content]
                     [?m :message/created-at ?created]
                     [(get-else $ ?m :message/tool-calls "") ?tc]
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
    (when eid (into {} (d/entity (d/db (conn)) eid)))))

(defn list-skills []
  (let [eids (q '[:find ?e :where [?e :skill/id _]])]
    (mapv #(into {} (d/entity (d/db (conn)) (first %))) eids)))

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
;; Tools (executable code)
;; ---------------------------------------------------------------------------

(defn install-tool! [{:keys [id name description parameters handler]}]
  (transact! [{:tool/id           id
               :tool/name         (or name (clojure.core/name id))
               :tool/description  (or description "")
               :tool/parameters   (or parameters "{}")
               :tool/handler      (or handler "")
               :tool/enabled?     true
               :tool/installed-at (java.util.Date.)}]))

(defn get-tool [tool-id]
  (let [eid (ffirst (q '[:find ?e :in $ ?id :where [?e :tool/id ?id]] tool-id))]
    (when eid (into {} (d/entity (d/db (conn)) eid)))))

(defn list-tools []
  (let [eids (q '[:find ?e :where [?e :tool/id _]])]
    (mapv #(into {} (d/entity (d/db (conn)) (first %))) eids)))

(defn enable-tool! [tool-id enabled?]
  (transact! [{:tool/id tool-id :tool/enabled? enabled?}]))
