(ns xia.db.provider
  "LLM provider persistence helpers.")

(defn- q*
  [deps query & inputs]
  (apply (:q deps) query inputs))

(defn- transact!*
  [deps tx-data]
  ((:transact! deps) tx-data))

(defn- raw-entity*
  [deps eid]
  ((:raw-entity deps) eid))

(defn- decrypt-entity*
  [deps entity-map]
  ((:decrypt-entity deps) entity-map))

(defn upsert-provider!
  [deps {:keys [id] :as provider}]
  (let [provider-id                (or id (:llm.provider/id provider))
        provider-eid               (ffirst (q* deps '[:find ?e :in $ ?id
                                                      :where [?e :llm.provider/id ?id]]
                                            provider-id))
        template-id                (or (:llm.provider/template provider)
                                       (:template provider))
        access-mode                (or (:llm.provider/access-mode provider)
                                       (:access-mode provider))
        credential-source          (or (:llm.provider/credential-source provider)
                                       (:credential-source provider)
                                       (:llm.provider/auth-type provider)
                                       (:auth-type provider))
        oauth-account              (or (:llm.provider/oauth-account provider)
                                       (:oauth-account provider))
        browser-session            (or (:llm.provider/browser-session provider)
                                       (:browser-session provider))
        workloads                  (some-> (or (:llm.provider/workloads provider)
                                              (:workloads provider))
                                           set)
        system-prompt-budget       (or (:llm.provider/system-prompt-budget provider)
                                       (:system-prompt-budget provider))
        history-budget             (or (:llm.provider/history-budget provider)
                                       (:history-budget provider))
        context-window             (or (:llm.provider/context-window provider)
                                       (:context-window provider))
        context-window-source      (or (:llm.provider/context-window-source provider)
                                       (:context-window-source provider))
        recommended-system-budget  (or (:llm.provider/recommended-system-prompt-budget provider)
                                       (:recommended-system-prompt-budget provider))
        recommended-history-budget (or (:llm.provider/recommended-history-budget provider)
                                       (:recommended-history-budget provider))
        recommended-input-budget   (or (:llm.provider/recommended-input-budget-cap provider)
                                       (:recommended-input-budget-cap provider))
        rate-limit-per-minute      (or (:llm.provider/rate-limit-per-minute provider)
                                       (:rate-limit-per-minute provider))
        vision?                    (or (:llm.provider/vision? provider)
                                       (:vision? provider))
        allow-private-network?     (or (:llm.provider/allow-private-network? provider)
                                       (:allow-private-network? provider))
        has-workloads?             (or (contains? provider :llm.provider/workloads)
                                       (contains? provider :workloads))
        has-template?              (or (contains? provider :llm.provider/template)
                                       (contains? provider :template))
        has-access-mode?           (or (contains? provider :llm.provider/access-mode)
                                       (contains? provider :access-mode))
        has-credential-source?     (or (contains? provider :llm.provider/credential-source)
                                       (contains? provider :credential-source)
                                       (contains? provider :llm.provider/auth-type)
                                       (contains? provider :auth-type))
        has-oauth-account?         (or (contains? provider :llm.provider/oauth-account)
                                       (contains? provider :oauth-account))
        has-browser-session?       (or (contains? provider :llm.provider/browser-session)
                                       (contains? provider :browser-session))
        has-vision?                (or (contains? provider :llm.provider/vision?)
                                       (contains? provider :vision?))
        has-allow-private-network? (or (contains? provider :llm.provider/allow-private-network?)
                                       (contains? provider :allow-private-network?))
        has-system-prompt-budget?  (or (contains? provider :llm.provider/system-prompt-budget)
                                       (contains? provider :system-prompt-budget))
        has-history-budget?        (or (contains? provider :llm.provider/history-budget)
                                       (contains? provider :history-budget))
        has-context-window?        (or (contains? provider :llm.provider/context-window)
                                       (contains? provider :context-window))
        has-context-window-source? (or (contains? provider :llm.provider/context-window-source)
                                       (contains? provider :context-window-source))
        has-recommended-system?    (or (contains? provider :llm.provider/recommended-system-prompt-budget)
                                       (contains? provider :recommended-system-prompt-budget))
        has-recommended-history?   (or (contains? provider :llm.provider/recommended-history-budget)
                                       (contains? provider :recommended-history-budget))
        has-recommended-input?     (or (contains? provider :llm.provider/recommended-input-budget-cap)
                                       (contains? provider :recommended-input-budget-cap))
        has-rate-limit?            (or (contains? provider :llm.provider/rate-limit-per-minute)
                                       (contains? provider :rate-limit-per-minute))
        provider-tx                (cond-> {:llm.provider/id provider-id}
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
                                     (and has-context-window?
                                          (some? context-window))
                                     (assoc :llm.provider/context-window context-window)
                                     (and has-context-window-source?
                                          (some? context-window-source))
                                     (assoc :llm.provider/context-window-source context-window-source)
                                     (and has-recommended-system?
                                          (some? recommended-system-budget))
                                     (assoc :llm.provider/recommended-system-prompt-budget
                                            recommended-system-budget)
                                     (and has-recommended-history?
                                          (some? recommended-history-budget))
                                     (assoc :llm.provider/recommended-history-budget
                                            recommended-history-budget)
                                     (and has-recommended-input?
                                          (some? recommended-input-budget))
                                     (assoc :llm.provider/recommended-input-budget-cap
                                            recommended-input-budget)
                                     (and has-rate-limit?
                                          (some? rate-limit-per-minute))
                                     (assoc :llm.provider/rate-limit-per-minute rate-limit-per-minute)
                                     (contains? provider :llm.provider/default?)
                                     (assoc :llm.provider/default? (:llm.provider/default? provider))
                                     (contains? provider :default?)
                                     (assoc :llm.provider/default? (:default? provider)))
        retracts                   (cond-> []
                                     (and provider-eid
                                          has-workloads?)
                                     (into (mapv (fn [workload]
                                                   [:db/retract provider-eid
                                                    :llm.provider/workloads
                                                    workload])
                                                 (or (:llm.provider/workloads (raw-entity* deps provider-eid))
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
                                          has-context-window?
                                          (nil? context-window))
                                     (conj [:db/retract provider-eid
                                            :llm.provider/context-window])
                                     (and provider-eid
                                          has-context-window-source?
                                          (nil? context-window-source))
                                     (conj [:db/retract provider-eid
                                            :llm.provider/context-window-source])
                                     (and provider-eid
                                          has-recommended-system?
                                          (nil? recommended-system-budget))
                                     (conj [:db/retract provider-eid
                                            :llm.provider/recommended-system-prompt-budget])
                                     (and provider-eid
                                          has-recommended-history?
                                          (nil? recommended-history-budget))
                                     (conj [:db/retract provider-eid
                                            :llm.provider/recommended-history-budget])
                                     (and provider-eid
                                          has-recommended-input?
                                          (nil? recommended-input-budget))
                                     (conj [:db/retract provider-eid
                                            :llm.provider/recommended-input-budget-cap])
                                     (and provider-eid
                                          has-rate-limit?
                                          (nil? rate-limit-per-minute))
                                     (conj [:db/retract provider-eid
                                            :llm.provider/rate-limit-per-minute]))]
    (transact!* deps (conj retracts provider-tx))))

(defn delete-provider!
  [deps provider-id]
  (when-let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :llm.provider/id ?id]]
                               provider-id))]
    (transact!* deps [[:db/retractEntity eid]])))

(defn get-default-provider
  [deps]
  (let [results (q* deps '[:find ?e :where
                           [?e :llm.provider/default? true]])]
    (when-let [eid (ffirst results)]
      (decrypt-entity* deps (raw-entity* deps eid)))))

(defn get-provider
  [deps provider-id]
  (let [eid (ffirst (q* deps '[:find ?e :in $ ?id :where [?e :llm.provider/id ?id]]
                           provider-id))]
    (when eid
      (decrypt-entity* deps (raw-entity* deps eid)))))

(defn list-providers
  [deps]
  (let [eids (q* deps '[:find ?e :where [?e :llm.provider/id _]])]
    (mapv #(decrypt-entity* deps (raw-entity* deps (first %))) eids)))

(defn set-default-provider!
  [deps provider-id]
  (let [providers (list-providers deps)
        tx-data   (mapv (fn [provider]
                          {:llm.provider/id       (:llm.provider/id provider)
                           :llm.provider/default? (= provider-id
                                                     (:llm.provider/id provider))})
                        providers)]
    (when (seq tx-data)
      (transact!* deps tx-data))
    provider-id))
