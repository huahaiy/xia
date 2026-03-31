(ns xia.db.session
  "Session, message, WM snapshot, LLM log, and audit persistence helpers."
  (:require [charred.api :as json]
            [datalevin.core :as d])
  (:import [java.util UUID]))

(def ^:private llm-log-retention-ms (* 30 24 60 60 1000))

(defn- q*
  [deps query & inputs]
  (apply (:q deps) query inputs))

(defn- transact!*
  [deps tx-data]
  ((:transact! deps) tx-data))

(defn- conn*
  [deps]
  ((:conn deps)))

(defn- raw-entity*
  [deps eid]
  ((:raw-entity deps) eid))

(defn- decrypt-entity*
  [deps entity-map]
  ((:decrypt-entity deps) entity-map))

(defn- decrypt-secret-attr*
  [deps attr value]
  ((:decrypt-secret-attr deps) attr value))

(defn- entity-created-at*
  [deps entity-map]
  ((:entity-created-at deps) entity-map))

(defn- entity-updated-at*
  [deps entity-map]
  ((:entity-updated-at deps) entity-map))

(defn- epoch-millis->date*
  [deps value]
  ((:epoch-millis->date deps) value))

(defn- session-eid
  [deps session-id]
  (ffirst (q* deps '[:find ?e :in $ ?sid
                     :where [?e :session/id ?sid]]
             session-id)))

(defn save-wm-snapshot!
  [deps {:keys [session-id topics slots episode-refs local-doc-refs autonomy-state]}]
  (let [session-eid* (ffirst (q* deps '[:find ?e :in $ ?sid
                                        :where [?e :session/id ?sid]]
                                  session-id))]
    (when-not session-eid*
      (throw (ex-info "Cannot save WM snapshot: session not found"
                      {:session-id session-id})))
    (let [wm-id       (random-uuid)
          wm-tx       (cond-> {:wm/id         wm-id
                               :wm/session    session-eid*
                               :wm/topics     (or topics "")
                               :wm/updated-at (java.util.Date.)}
                        (some? autonomy-state)
                        (assoc :wm/autonomy-state (json/write-json-str autonomy-state)))
          old-wm-eids (mapv first (q* deps '[:find ?e :in $ ?s
                                             :where [?e :wm/session ?s]]
                                       session-eid*))
          retracts    (mapv (fn [eid] [:db/retractEntity eid]) old-wm-eids)]
      (transact!* deps (into retracts [wm-tx]))
      (let [wm-eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :wm/id ?id]] wm-id))]
        (when (seq slots)
          (transact!*
            deps
            (mapv (fn [[_node-eid slot]]
                    {:wm.slot/id        (random-uuid)
                     :wm.slot/wm        wm-eid
                     :wm.slot/node      (:node-eid slot)
                     :wm.slot/relevance (float (:relevance slot))
                     :wm.slot/pinned?   (boolean (:pinned? slot))
                     :wm.slot/added-at  (java.util.Date.)})
                  slots)))
        (when (seq episode-refs)
          (transact!*
            deps
            (mapv (fn [eref]
                    {:wm.episode-ref/id        (random-uuid)
                     :wm.episode-ref/wm        wm-eid
                     :wm.episode-ref/episode   (:episode-eid eref)
                     :wm.episode-ref/relevance (float (:relevance eref))})
                  episode-refs)))
        (when (seq local-doc-refs)
          (transact!*
            deps
            (keep (fn [dref]
                    (when-let [doc-eid (ffirst
                                         (q* deps '[:find ?e :in $ ?id
                                                    :where [?e :local.doc/id ?id]]
                                             (:doc-id dref)))]
                      {:wm.local-doc-ref/id        (random-uuid)
                       :wm.local-doc-ref/wm        wm-eid
                       :wm.local-doc-ref/doc       doc-eid
                       :wm.local-doc-ref/relevance (float (:relevance dref))}))
                  local-doc-refs))))
      wm-id)))

(defn load-wm-snapshot
  [deps session-id]
  (let [session-eid* (ffirst (q* deps '[:find ?e :in $ ?sid
                                        :where [?e :session/id ?sid]]
                                  session-id))]
    (when session-eid*
      (when-let [wm-eid (ffirst (q* deps '[:find ?e :in $ ?s
                                           :where [?e :wm/session ?s]]
                                     session-eid*))]
        (let [wm-entity (into {} (d/entity (d/db (conn* deps)) wm-eid))]
          {:topics         (:wm/topics wm-entity)
           :autonomy-state (when-let [text (:wm/autonomy-state wm-entity)]
                             (try
                               (json/read-json text)
                               (catch Exception _
                                 nil)))
           :updated-at     (entity-updated-at* deps wm-entity)})))))

(defn latest-session-episode
  [deps]
  (first
    (sort-by :timestamp #(compare %2 %1)
             (map (fn [[summary ctx ts]]
                    {:summary   summary
                     :context   (when-not (= "" ctx) ctx)
                     :timestamp ts})
                  (q* deps '[:find ?summary ?ctx ?ts
                              :where
                              [?e :episode/summary ?summary]
                              [?e :episode/timestamp ?ts]
                              [(get-else $ ?e :episode/context "") ?ctx]])))))

(defn create-session!
  [deps channel & [opts]]
  (let [{:keys [parent-session-id worker? label active? external-key external-meta]
         :or   {worker? false
                active? true}} opts
        id (random-uuid)]
    (transact!*
      deps
      [(cond-> {:session/id      id
                :session/channel channel
                :session/worker? worker?
                :session/active? active?}
         parent-session-id (assoc :session/parent-id parent-session-id)
         (some? label) (assoc :session/label label)
         (some? external-key) (assoc :session/external-key external-key)
         (some? external-meta) (assoc :session/external-meta external-meta))])
    id))

(defn find-session-by-external-key
  [deps external-key]
  (when-let [eid (ffirst (q* deps '[:find ?e :in $ ?external-key
                                    :where [?e :session/external-key ?external-key]]
                              external-key))]
    (let [entity-map (raw-entity* deps eid)]
      {:id            (:session/id entity-map)
       :channel       (:session/channel entity-map)
       :external-key  (:session/external-key entity-map)
       :external-meta (:session/external-meta entity-map)
       :active?       (boolean (:session/active? entity-map))
       :worker?       (boolean (:session/worker? entity-map))
       :parent-id     (:session/parent-id entity-map)
       :label         (:session/label entity-map)})))

(defn session-external-meta
  [deps session-id]
  (when-let [eid (session-eid deps session-id)]
    (:session/external-meta (raw-entity* deps eid))))

(defn save-session-external-meta!
  [deps session-id external-meta]
  (when-let [eid (session-eid deps session-id)]
    (transact!* deps [{:db/id eid
                       :session/external-meta external-meta}])
    true))

(defn list-sessions
  [deps & [opts]]
  (let [{:keys [include-workers?] :or {include-workers? false}} opts]
    (->> (q* deps '[:find ?s ?sid ?channel
                    :where
                    [?s :session/id ?sid]
                    [?s :session/channel ?channel]])
         (map (fn [[eid sid channel]]
                (let [entity-map (raw-entity* deps eid)]
                  {:id         sid
                   :channel    channel
                   :external-key (:session/external-key entity-map)
                   :external-meta (:session/external-meta entity-map)
                   :created-at (entity-created-at* deps entity-map)
                   :active?    (boolean (:session/active? entity-map))
                   :worker?    (boolean (:session/worker? entity-map))
                   :parent-id  (:session/parent-id entity-map)
                   :label      (:session/label entity-map)})))
         (remove (fn [{:keys [worker?]}]
                   (and worker? (not include-workers?))))
         (sort-by :created-at #(compare %2 %1))
         vec)))

(defn set-session-active!
  [deps session-id active?]
  (when-let [session-eid* (ffirst (q* deps '[:find ?e :in $ ?sid
                                             :where [?e :session/id ?sid]]
                                       session-id))]
    (transact!* deps [{:db/id            session-eid*
                       :session/active? (boolean active?)}])
    true))

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

(defn- json-doc
  [value]
  (json/write-json-str value))

(defn- read-json-doc
  [value]
  (when (string? value)
    (json/read-json value)))

(declare empty->nil session-messages)

(defn- normalize-message-local-doc-id
  [value]
  (cond
    (instance? UUID value) value
    (string? value)        (try
                             (UUID/fromString (clojure.string/trim value))
                             (catch Exception _
                               nil))
    :else                  nil))

(defn- valid-session-local-doc-ids
  [deps session-id local-doc-ids]
  (let [doc-ids (->> local-doc-ids
                     (keep normalize-message-local-doc-id)
                     distinct
                     vec)]
    (if-not (seq doc-ids)
      []
      (let [valid-ids (->> (q* deps '[:find ?doc-id
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
  [deps session-id artifact-ids]
  (let [artifact-ids* (->> artifact-ids
                           (keep normalize-message-local-doc-id)
                           distinct
                           vec)]
    (if-not (seq artifact-ids*)
      []
      (let [valid-ids (->> (q* deps '[:find ?artifact-id
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
  [deps message-eid]
  (->> (q* deps '[:find ?doc-id ?name ?status
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
  [deps message-eid]
  (->> (q* deps '[:find ?artifact-id ?name ?title ?status
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
  [deps session-id role content & {:keys [tool-calls tool-id tool-call-id tool-name tool-result
                                          llm-call-id provider-id model workload
                                          local-doc-ids artifact-ids]}]
  (let [session-eid*   (session-eid deps session-id)
        message-id     (random-uuid)
        doc-ids        (valid-session-local-doc-ids deps session-id local-doc-ids)
        artifact-ids*  (valid-session-artifact-ids deps session-id artifact-ids)]
    (transact!*
      deps
      (into
        [(cond-> {:message/id             message-id
                  :message/session        session-eid*
                  :message/role           role
                  :message/content        (or content "")
                  :message/token-estimate (message-token-estimate {:role role
                                                                   :content content
                                                                   :tool-result tool-result})}
           tool-calls (assoc :message/tool-calls (tool-calls-doc tool-calls))
           (some? tool-result) (assoc :message/tool-result (tool-result-doc tool-result))
           tool-id (assoc :message/tool-id tool-id)
           tool-call-id (assoc :message/tool-call-id tool-call-id)
           tool-name (assoc :message/tool-name tool-name)
           llm-call-id (assoc :message/llm-call-id llm-call-id)
           provider-id (assoc :message/provider-id provider-id)
           model (assoc :message/model model)
           workload (assoc :message/workload workload))]
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
               artifact-ids*))))
    message-id))

(defn- empty->nil [s]
  (when-not (= "" s) s))

(defn session-history-recap
  [deps session-id]
  (when-let [eid (session-eid deps session-id)]
    (let [entity-map (decrypt-entity* deps (raw-entity* deps eid))
          recap      (empty->nil (:session/history-recap entity-map))]
      (when recap
        {:content       recap
         :message-count (long (or (:session/history-recap-count entity-map) 0))
         :updated-at    (or (:session/history-recap-updated-at entity-map)
                            (entity-updated-at* deps entity-map))}))))

(defn save-session-history-recap!
  [deps session-id content message-count]
  (when-let [eid (session-eid deps session-id)]
    (transact!* deps [{:db/id eid
                       :session/history-recap content
                       :session/history-recap-count (long message-count)
                       :session/history-recap-updated-at (java.util.Date.)}])
    true))

(defn session-tool-recap
  [deps session-id]
  (when-let [eid (session-eid deps session-id)]
    (let [entity-map (decrypt-entity* deps (raw-entity* deps eid))
          recap      (empty->nil (:session/tool-recap entity-map))]
      (when recap
        {:content       recap
         :message-count (long (or (:session/tool-recap-count entity-map) 0))
         :updated-at    (or (:session/tool-recap-updated-at entity-map)
                            (entity-updated-at* deps entity-map))}))))

(defn save-session-tool-recap!
  [deps session-id content message-count]
  (when-let [eid (session-eid deps session-id)]
    (transact!* deps [{:db/id eid
                       :session/tool-recap content
                       :session/tool-recap-count (long message-count)
                       :session/tool-recap-updated-at (java.util.Date.)}])
    true))

(defn session-message-metadata
  [deps session-id]
  (->> (q* deps '[:find ?m ?mid ?dca ?tokens
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
               :created-at (epoch-millis->date* deps created-at)
               :token-estimate (long token-estimate)}))
       (sort-by (juxt :created-at :eid))
       vec))

(defn session-message-count
  [deps session-id]
  (if-let [count* (ffirst (q* deps '[:find (count ?m)
                                     :in $ ?sid
                                     :where
                                     [?s :session/id ?sid]
                                     [?m :message/session ?s]]
                               session-id))]
    (long count*)
    0))

(defn session-message-eids-range
  ([deps session-id start end]
   (session-message-eids-range deps session-id start end nil))
  ([deps session-id start end total-count]
   (let [start* (max 0 (long (or start 0)))
         end0   (max 0 (long (or end 0)))]
     (if-let [sid-eid (session-eid deps session-id)]
       (let [db*          (d/db (conn* deps))
             total*       (long (or total-count (session-message-count deps session-id)))
             end*         (max start* (min total* end0))
             count-needed (- end* start*)]
         (if (<= count-needed 0)
           []
           (if (<= start* (- total* end*))
             (->> (d/datoms db* :ave :message/session sid-eid)
                  (drop start*)
                  (take count-needed)
                  (mapv :e))
             (->> (d/rseek-datoms db* :ave :message/session sid-eid)
                  (drop (- total* end*))
                  (take count-needed)
                  (map :e)
                  reverse
                  vec))))
       []))))

(defn session-message-metadata-range
  ([deps session-id start end]
   (session-message-metadata-range deps session-id start end nil))
  ([deps session-id start end total-count]
   (let [message-eids (session-message-eids-range deps session-id start end total-count)
         message-eids* (vec message-eids)]
     (if-not (seq message-eids*)
       []
       (let [by-eid
             (into {}
                   (map (fn [[eid mid created-at token-estimate]]
                          [eid
                           {:eid eid
                            :id mid
                            :created-at (epoch-millis->date* deps created-at)
                            :token-estimate (long token-estimate)}]))
                   (q* deps '[:find ?m ?mid ?dca ?tokens
                              :in $ [?m ...]
                              :where
                              [?m :message/id ?mid]
                              [(get-else $ ?m :db/created-at 0) ?dca]
                              [(get-else $ ?m :message/token-estimate 0) ?tokens]]
                       message-eids*))]
         (->> message-eids*
              (keep by-eid)
              vec))))))

(defn session-messages-by-eids
  [deps message-eids]
  (let [message-eids* (vec message-eids)]
    (if-not (seq message-eids*)
      []
      (let [by-eid
            (into {}
                  (map (fn [[eid mid content role tool-calls tool-result tool-id]]
                         [eid
                          (let [tool-result* (read-tool-result-doc tool-result)
                                entity-map    (raw-entity* deps eid)]
                            {:id           mid
                             :role         role
                             :content      (when-not (and (= role :tool) (some? tool-result*))
                                             (decrypt-secret-attr* deps :message/content content))
                             :created-at   (entity-created-at* deps entity-map)
                             :local-docs   (not-empty (message-local-docs deps eid))
                             :artifacts    (not-empty (message-artifacts deps eid))
                             :tool-calls   (read-tool-calls-doc tool-calls)
                             :tool-result  tool-result*
                             :tool-id      (empty->nil tool-id)
                             :tool-call-id (empty->nil (:message/tool-call-id entity-map))
                             :tool-name    (empty->nil (:message/tool-name entity-map))
                             :llm-call-id  (:message/llm-call-id entity-map)
                             :provider-id  (:message/provider-id entity-map)
                             :model        (empty->nil (:message/model entity-map))
                             :workload     (:message/workload entity-map)})])
                       (q* deps '[:find ?m ?mid ?content ?role ?tc ?tr ?tid
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

(defn latest-session-message
  [deps session-id & [roles]]
  (let [roles* (some->> roles set)]
    (->> (session-messages deps session-id)
         (filter #(or (nil? roles*)
                      (contains? roles* (:role %))))
         last)))

(defn session-messages
  [deps session-id]
  (->> (session-message-metadata deps session-id)
       (mapv :eid)
       (session-messages-by-eids deps)))

(defn- prune-llm-log!
  [deps]
  (let [cutoff (java.util.Date. (- (.getTime (java.util.Date.)) llm-log-retention-ms))
        old    (q* deps '[:find ?e :in $ ?cutoff
                          :where
                          [?e :llm.log/id _]
                          [?e :llm.log/created-at ?t]
                          [(< ?t ?cutoff)]]
                  cutoff)]
    (when (seq old)
      (transact!* deps (mapv (fn [[eid]] [:db/retractEntity eid]) old)))))

(defn log-llm-call!
  [deps entry]
  (transact!* deps
              [(merge {:llm.log/id         (or (:id entry) (random-uuid))
                       :llm.log/created-at (or (:created-at entry) (java.util.Date.))}
                      (when-let [v (:session-id entry)] {:llm.log/session-id v})
                      (when-let [v (:provider-id entry)] {:llm.log/provider-id v})
                      (when-let [v (:model entry)] {:llm.log/model v})
                      (when-let [v (:workload entry)] {:llm.log/workload v})
                      (when-let [v (:messages entry)] {:llm.log/messages v})
                      (when-let [v (:tools entry)] {:llm.log/tools v})
                      (when-let [v (:response entry)] {:llm.log/response v})
                      (when-let [v (:status entry)] {:llm.log/status v})
                      (when-let [v (:error entry)] {:llm.log/error v})
                      (when-let [v (:duration-ms entry)] {:llm.log/duration-ms v})
                      (when-let [v (:prompt-tokens entry)] {:llm.log/prompt-tokens v})
                      (when-let [v (:completion-tokens entry)] {:llm.log/completion-tokens v}))])
  (prune-llm-log! deps))

(defn- llm-call-related-messages
  [deps call-id]
  (->> (q* deps '[:find ?m ?mid ?role ?created-at
                  :in $ ?call-id
                  :where
                  [?m :message/llm-call-id ?call-id]
                  [?m :message/id ?mid]
                  [?m :message/role ?role]
                  [(get-else $ ?m :db/created-at 0) ?created-at]]
            call-id)
       (sort-by (juxt #(nth % 3) first))
       (mapv (fn [[eid message-id role created-at]]
               (let [entity-map (raw-entity* deps eid)]
                 {:id          message-id
                  :role        role
                  :provider-id (:message/provider-id entity-map)
                  :model       (empty->nil (:message/model entity-map))
                  :workload    (:message/workload entity-map)
                  :created-at  (epoch-millis->date* deps created-at)})))))

(defn list-llm-calls
  ([deps]
   (list-llm-calls deps 50))
  ([deps limit]
   (list-llm-calls deps limit nil))
  ([deps limit session-id]
   (->> (if session-id
          (q* deps '[:find ?e ?t
                     :in $ ?sid
                     :where
                     [?e :llm.log/id _]
                     [?e :llm.log/session-id ?sid]
                     [?e :llm.log/created-at ?t]]
              session-id)
          (q* deps '[:find ?e ?t
                     :where
                     [?e :llm.log/id _]
                     [?e :llm.log/created-at ?t]]))
        (sort-by second #(compare %2 %1))
        (take limit)
        (mapv (fn [[eid _]]
                (let [e (decrypt-entity* deps (raw-entity* deps eid))]
                  {:id                (:llm.log/id e)
                   :session-id        (:llm.log/session-id e)
                   :provider-id       (:llm.log/provider-id e)
                   :model             (:llm.log/model e)
                   :workload          (:llm.log/workload e)
                   :status            (:llm.log/status e)
                   :error             (:llm.log/error e)
                   :duration-ms       (:llm.log/duration-ms e)
                   :prompt-tokens     (:llm.log/prompt-tokens e)
                   :completion-tokens (:llm.log/completion-tokens e)
                   :created-at        (:llm.log/created-at e)}))))))

(defn get-llm-call
  [deps call-id]
  (when-let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :llm.log/id ?id]] call-id))]
    (let [e (decrypt-entity* deps (raw-entity* deps eid))]
      {:id                (:llm.log/id e)
       :session-id        (:llm.log/session-id e)
       :provider-id       (:llm.log/provider-id e)
       :model             (:llm.log/model e)
       :workload          (:llm.log/workload e)
       :messages          (:llm.log/messages e)
       :tools             (:llm.log/tools e)
       :response          (:llm.log/response e)
       :status            (:llm.log/status e)
       :error             (:llm.log/error e)
       :duration-ms       (:llm.log/duration-ms e)
       :prompt-tokens     (:llm.log/prompt-tokens e)
       :completion-tokens (:llm.log/completion-tokens e)
       :related-messages  (llm-call-related-messages deps call-id)
       :created-at        (:llm.log/created-at e)})))

(defn log-audit-event!
  [deps entry]
  (transact!* deps
              [(merge {:audit.event/id         (or (:id entry) (random-uuid))
                       :audit.event/created-at (or (:created-at entry) (java.util.Date.))}
                      (when-let [v (:session-id entry)] {:audit.event/session-id v})
                      (when-let [v (:channel entry)] {:audit.event/channel v})
                      (when-let [v (:actor entry)] {:audit.event/actor v})
                      (when-let [v (:type entry)] {:audit.event/type v})
                      (when-let [v (:message-id entry)] {:audit.event/message-id v})
                      (when-let [v (:llm-call-id entry)] {:audit.event/llm-call-id v})
                      (when-let [v (:tool-id entry)] {:audit.event/tool-id v})
                      (when-let [v (:tool-call-id entry)] {:audit.event/tool-call-id v})
                      (when-let [v (:data entry)] {:audit.event/data (json-doc v)}))]))

(defn session-audit-events
  ([deps session-id]
   (session-audit-events deps session-id 500))
  ([deps session-id limit]
   (->> (q* deps '[:find ?e ?created-at
                   :in $ ?sid
                   :where
                   [?e :audit.event/session-id ?sid]
                   [?e :audit.event/created-at ?created-at]]
             session-id)
        (sort-by (juxt second first))
        (take-last limit)
        (mapv (fn [[eid _]]
                (let [e (decrypt-entity* deps (raw-entity* deps eid))]
                  {:id           (:audit.event/id e)
                   :session-id   (:audit.event/session-id e)
                   :channel      (:audit.event/channel e)
                   :actor        (:audit.event/actor e)
                   :type         (:audit.event/type e)
                   :message-id   (:audit.event/message-id e)
                   :llm-call-id  (:audit.event/llm-call-id e)
                   :tool-id      (empty->nil (:audit.event/tool-id e))
                   :tool-call-id (empty->nil (:audit.event/tool-call-id e))
                   :data         (read-json-doc (:audit.event/data e))
                   :created-at   (:audit.event/created-at e)}))))))
