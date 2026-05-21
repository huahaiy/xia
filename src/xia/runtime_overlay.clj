(ns xia.runtime-overlay
  "In-memory runtime overlay for managed/cloud-specific config and catalog data.

   The overlay is loaded from an EDN bundle at startup and never persisted back
   into the tenant Datalevin DB. Reads can resolve as:

   runtime overlay > tenant DB > code defaults"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [xia.db-schema :as db-schema]
            [xia.sensitive :as sensitive]
            [taoensso.timbre :as log])
  (:import [java.time Instant]))

(def ^:private current-overlay-schema-version 1)
(def ^:private current-db-schema-version db-schema/current-version)
(def ^:private current-db-schema (db-schema/current-schema))

(def ^:private required-top-level-keys
  #{:overlay/schema-version
    :snapshot/id
    :tenant/id
    :runtime/id
    :generated-at
    :config-overrides
    :bounded-config
    :tx-data
    :forced-keys})

(def ^:private allowed-top-level-keys
  required-top-level-keys)

(def ^:private overlay-kinds
  {:provider      {:identity :llm.provider/id
                   :attr-namespaces #{"llm.provider"}}
   :service       {:identity :service/id
                   :attr-namespaces #{"service"}}
   :oauth-account {:identity :oauth.account/id
                   :attr-namespaces #{"oauth.account"}}
   :site-cred     {:identity :site-cred/id
                   :attr-namespaces #{"site-cred"}}})

(defonce ^:private overlay-atom (atom nil))
(defonce ^:private activation-lock (Object.))

(declare clear!)

(defn- input-overlay-schema-version
  [overlay]
  (:overlay/schema-version overlay))

(defn- nonblank-string?
  [value]
  (boolean (some-> value str str/trim not-empty)))

(defn- config-value->db-string
  [value]
  (cond
    (nil? value) nil
    (string? value) value
    (keyword? value) (name value)
    (symbol? value) (name value)
    (boolean? value) (if value "true" "false")
    (number? value) (str value)
    :else (pr-str value)))

(defn- config-rule?
  [value]
  (and (map? value)
       (contains? value :merge)))

(defn- normalize-config-override-entry
  [value]
  {:merge :replace
   :value value})

(defn- normalize-bounded-config-entry
  [value]
  {:merge :cap
   :value value})

(def ^:private supported-secret-ref-keys
  #{:secret/file})

(def ^:private unsupported-secret-ref-keys
  #{:secret/ref
    :secret-env
    :secret-file})

(def ^:private known-secret-ref-keys
  (set/union supported-secret-ref-keys unsupported-secret-ref-keys))

(defn- secret-ref?
  [value]
  (and (map? value)
       (boolean (some #(contains? value %) known-secret-ref-keys))))

(defn- nonblank-secret-ref
  [value key]
  (some-> (get value key) str str/trim not-empty))

(defn- read-env-secret
  [env-name]
  (System/getenv env-name))

(defn- read-file-secret
  [path]
  (slurp (io/file path)))

(defn- trim-trailing-newline
  [value]
  (some-> value (str/replace #"(?:\r?\n)\z" "")))

(defn- resolve-secret-ref
  [value context]
  (let [present-ref-keys (set/intersection (set (keys value))
                                           known-secret-ref-keys)
        file-path        (nonblank-secret-ref value :secret/file)]
    (cond
      (not= 1 (count present-ref-keys))
      (throw (ex-info "Runtime overlay secret ref must use exactly one source."
                      (assoc context :secret-ref value)))

      (not= (set (keys value)) present-ref-keys)
      (throw (ex-info "Runtime overlay secret ref contains unsupported fields."
                      (assoc context
                             :secret-ref value
                             :supported-secret-ref-keys (sort supported-secret-ref-keys))))

      (not (contains? supported-secret-ref-keys (first present-ref-keys)))
      (throw (ex-info "Runtime overlay secret refs currently support only :secret/file."
                      (assoc context
                             :secret-ref value
                             :supported-secret-ref-keys (sort supported-secret-ref-keys))))

      file-path
      (let [file (io/file file-path)]
        (when-not (.exists file)
          (throw (ex-info "Runtime overlay secret file does not exist."
                          (assoc context :secret/file file-path))))
        (some-> (read-file-secret file-path) trim-trailing-newline))

      :else
      (throw (ex-info "Runtime overlay secret ref requires :secret/file."
                      (assoc context :secret-ref value))))))

(defn- entity-kind-entry
  [entity]
  (some (fn [[kind {:keys [identity]}]]
          (when (contains? entity identity)
            [kind identity]))
        overlay-kinds))

(defn- valid-db-schema-version
  [value]
  (and (integer? value)
       (pos? (long value))
       (long value)))

(defn- validate-overlay-entity-attr!
  [kind attr entry]
  (let [{:keys [attr-namespaces]} (get overlay-kinds kind)]
    (when-not (keyword? attr)
      (throw (ex-info "Runtime overlay entity attrs must be keywords."
                      {:entity-kind kind
                       :attr attr
                       :entry entry})))
    (when-not (contains? current-db-schema attr)
      (throw (ex-info "Runtime overlay entity attr is not in the current DB schema."
                      {:entity-kind kind
                       :attr attr
                       :entry entry
                       :current-db-schema-version current-db-schema-version})))
    (when-not (contains? attr-namespaces (namespace attr))
      (throw (ex-info "Runtime overlay entity attr is not valid for this entity kind."
                      {:entity-kind kind
                       :attr attr
                       :allowed-namespaces (sort attr-namespaces)
                       :entry entry})))))

(defn- validate-overlay!
  [overlay]
  (when-not (map? overlay)
    (throw (ex-info "Runtime overlay must be an EDN map."
                    {:overlay overlay})))
  (let [unknown-keys (set/difference (set (keys overlay))
                                     allowed-top-level-keys)
        missing-keys (set/difference required-top-level-keys
                                     (set (keys overlay)))]
    (when (seq unknown-keys)
      (throw (ex-info "Runtime overlay contains unknown top-level keys."
                      {:unknown-keys (sort unknown-keys)
                       :allowed-keys (sort allowed-top-level-keys)})))
    (when (seq missing-keys)
      (throw (ex-info "Runtime overlay is missing required top-level keys."
                      {:missing-keys (sort missing-keys)}))))
  (when-not (= current-overlay-schema-version (:overlay/schema-version overlay))
    (throw (ex-info "Unsupported runtime overlay schema version."
                    {:supported-schema-version current-overlay-schema-version
                     :overlay/schema-version (:overlay/schema-version overlay)})))
  (when-not (nonblank-string? (:snapshot/id overlay))
    (throw (ex-info "Runtime overlay requires a non-empty :snapshot/id."
                    {:snapshot/id (:snapshot/id overlay)})))
  (when-not (nonblank-string? (:tenant/id overlay))
    (throw (ex-info "Runtime overlay requires a non-empty :tenant/id."
                    {:tenant/id (:tenant/id overlay)})))
  (when-not (nonblank-string? (:runtime/id overlay))
    (throw (ex-info "Runtime overlay requires a non-empty :runtime/id."
                    {:runtime/id (:runtime/id overlay)})))
  (try
    (Instant/parse (:generated-at overlay))
    (catch Exception _
      (throw (ex-info "Runtime overlay :generated-at must be an ISO-8601 UTC timestamp string."
                      {:generated-at (:generated-at overlay)}))))
  (when-not (map? (:config-overrides overlay))
    (throw (ex-info "Runtime overlay :config-overrides must be a map."
                    {:value (:config-overrides overlay)})))
  (when-not (every? keyword? (keys (:config-overrides overlay)))
    (throw (ex-info "Runtime overlay :config-overrides keys must be keywords."
                    {:keys (keys (:config-overrides overlay))})))
  (when-not (map? (:bounded-config overlay))
    (throw (ex-info "Runtime overlay :bounded-config must be a map."
                    {:value (:bounded-config overlay)})))
  (when-not (every? keyword? (keys (:bounded-config overlay)))
    (throw (ex-info "Runtime overlay :bounded-config keys must be keywords."
                    {:keys (keys (:bounded-config overlay))})))
  (let [overlap (set/intersection (set (keys (:config-overrides overlay)))
                                  (set (keys (:bounded-config overlay))))]
    (when (seq overlap)
      (throw (ex-info "Runtime overlay keys cannot appear in both :config-overrides and :bounded-config."
                      {:config-keys (sort overlap)}))))
  (doseq [[config-key value] (:config-overrides overlay)]
    (when (config-rule? value)
      (throw (ex-info "Runtime overlay :config-overrides values must be literal override values."
                      {:config-key config-key
                       :value value})))
      (when (secret-ref? value)
        (when-not (sensitive/secret-config-key? config-key)
          (throw (ex-info "Runtime overlay secret refs are only allowed for secret config keys."
                          {:config-key config-key})))
      (resolve-secret-ref value {:config-key config-key})))
  (doseq [[config-key value] (:bounded-config overlay)]
    (when (secret-ref? value)
      (throw (ex-info "Runtime overlay :bounded-config cannot contain secret refs."
                      {:config-key config-key}))))
  (when-not (set? (:forced-keys overlay))
    (throw (ex-info "Runtime overlay :forced-keys must be a set of keywords."
                    {:value (:forced-keys overlay)})))
  (when-not (every? keyword? (:forced-keys overlay))
    (throw (ex-info "Runtime overlay :forced-keys entries must be keywords."
                    {:forced-keys (:forced-keys overlay)})))
  (when-not (vector? (:tx-data overlay))
    (throw (ex-info "Runtime overlay :tx-data must be a vector of entity maps."
                    {:value (:tx-data overlay)})))
  (doseq [entity (:tx-data overlay)]
    (when-not (map? entity)
      (throw (ex-info "Runtime overlay :tx-data entries must be maps."
                      {:entry entity})))
    (when-not (entity-kind-entry entity)
      (throw (ex-info "Runtime overlay contains an unsupported overlay entity."
                      {:entry entity
                       :supported-kinds (sort (keys overlay-kinds))})))
    (let [[kind _identity-attr] (entity-kind-entry entity)]
      (doseq [attr (keys entity)]
        (validate-overlay-entity-attr! kind attr entity)))
    (doseq [[attr value] entity
            :when (and (keyword? attr)
                       (secret-ref? value))]
      (when-not (sensitive/encrypted-attr? attr)
        (throw (ex-info "Runtime overlay secret refs are only allowed for encrypted attrs."
                        {:attr attr
                         :entry entity})))
      (resolve-secret-ref value {:attr attr})))
  overlay)

(defn- normalize-overlay
  [overlay source-schema-version]
  (let [provider-default-ids (->> (:tx-data overlay)
                                  (keep (fn [entity]
                                          (when (and (contains? entity :llm.provider/default?)
                                                     (true? (:llm.provider/default? entity)))
                                            (:llm.provider/id entity))))
                                  vec)]
    (when (> (count provider-default-ids) 1)
      (throw (ex-info "Runtime overlay may mark at most one provider as default."
                      {:provider-ids provider-default-ids})))
    (reduce
      (fn [acc entity]
        (let [[kind identity-attr] (entity-kind-entry entity)
              entity-id            (get entity identity-attr)]
          (-> acc
              (update-in [:entities kind entity-id] #(merge (or % {}) entity))
              (update-in [:entity-order kind]
                         (fn [ids]
                           (let [ids* (or ids [])]
                             (if (some #{entity-id} ids*)
                               ids*
                               (conj (vec ids*) entity-id))))))))
      (let [literal-overrides (into {}
                                    (map (fn [[config-key value]]
                                           [config-key (normalize-config-override-entry value)]))
                                    (:config-overrides overlay))
            bounded-overrides (into {}
                                    (map (fn [[config-key value]]
                                           [config-key (normalize-bounded-config-entry value)]))
                                    (:bounded-config overlay))]
        {:overlay/schema-version current-overlay-schema-version
         :source-overlay/schema-version source-schema-version
         :tenant/id (:tenant/id overlay)
         :runtime/id (:runtime/id overlay)
         :generated-at (:generated-at overlay)
         :overlay/requires-db-schema-version current-db-schema-version
         :snapshot/id (:snapshot/id overlay)
         :literal-config-overrides literal-overrides
         :bounded-config bounded-overrides
         :config-overrides (merge literal-overrides bounded-overrides)
         :forced-keys (:forced-keys overlay)
         :tx-data (:tx-data overlay)
         :provider-default-id (first provider-default-ids)})
      (:tx-data overlay))))

(defn- activate-overlay!
  [overlay source-path]
  (locking activation-lock
    (let [previous       @overlay-atom
          source-schema-version (input-overlay-schema-version overlay)
          normalized     (-> overlay
                             validate-overlay!
                             (normalize-overlay source-schema-version))
          overlay-state  (assoc normalized
                           :overlay/source-path (some-> source-path str str/trim not-empty)
                           :overlay/loaded-at-ms (System/currentTimeMillis)
                           :overlay/reload-count (inc (long (or (:overlay/reload-count previous) 0))))]
      (reset! overlay-atom overlay-state)
      (log/info "Activated runtime overlay" (:snapshot/id overlay-state))
      overlay-state)))

(defn activate!
  ([overlay]
   (activate-overlay! overlay nil))
  ([overlay source-path]
   (activate-overlay! overlay source-path)))

(defn- read-overlay-file
  [overlay-path]
  (let [file (io/file overlay-path)]
    (when-not (.exists file)
      (throw (ex-info "Runtime overlay file does not exist."
                      {:overlay-path overlay-path})))
    (-> file
        slurp
        edn/read-string)))

(defn load-file!
  [overlay-path]
  (if (some-> overlay-path str str/trim not-empty)
    (activate-overlay! (read-overlay-file overlay-path) overlay-path)
    (do
      (clear!)
      nil)))

(defn reload!
  ([] (reload! nil))
  ([overlay-path]
   (let [resolved-path (or (some-> overlay-path str str/trim not-empty)
                           (:overlay/source-path @overlay-atom))]
     (when-not resolved-path
       (throw (ex-info "No runtime overlay source path is available to reload."
                       {:type :runtime-overlay/missing-source-path})))
     (activate-overlay! (read-overlay-file resolved-path) resolved-path))))

(defn clear!
  []
  (locking activation-lock
    (reset! overlay-atom nil)))

(defn current-overlay
  []
  @overlay-atom)

(defn active?
  []
  (boolean @overlay-atom))

(defn- key->overlay-name
  [value]
  (cond
    (keyword? value) (if-let [ns-part (namespace value)]
                       (str ns-part "/" (name value))
                       (name value))
    (symbol? value) (if-let [ns-part (namespace value)]
                      (str ns-part "/" (name value))
                      (name value))
    :else (str value)))

(defn snapshot-id
  []
  (:snapshot/id @overlay-atom))

(defn overlay-version
  []
  (:overlay/schema-version @overlay-atom))

(defn overlay-schema-version
  []
  (:overlay/schema-version @overlay-atom))

(defn source-path
  []
  (:overlay/source-path @overlay-atom))

(defn loaded-at-ms
  []
  (:overlay/loaded-at-ms @overlay-atom))

(defn config-override?
  [config-key]
  (contains? (get @overlay-atom :config-overrides {})
             config-key))

(defn forced-key?
  [config-key]
  (contains? (get @overlay-atom :forced-keys #{})
             config-key))

(defn config-value
  [config-key]
  (let [value (get-in @overlay-atom [:config-overrides config-key :value])]
    (if (and (sensitive/secret-config-key? config-key)
             (secret-ref? value))
      (resolve-secret-ref value {:config-key config-key})
      value)))

(defn config-merge-mode
  [config-key]
  (get-in @overlay-atom [:config-overrides config-key :merge]))

(defn config-db-value
  [config-key]
  (when (config-override? config-key)
    (config-value->db-string (config-value config-key))))

(defn entity
  [kind entity-id]
  (get-in @overlay-atom [:entities kind entity-id]))

(defn entity-managed?
  [kind entity-id]
  (boolean (entity kind entity-id)))

(defn entity-source
  [kind entity-id]
  (if (entity-managed? kind entity-id)
    :runtime-overlay
    :tenant-db))

(defn- resolve-overlay-entity
  [entity-map]
  (reduce-kv (fn [acc k v]
               (assoc acc k
                      (if (and (keyword? k)
                               (sensitive/encrypted-attr? k)
                               (secret-ref? v))
                        (resolve-secret-ref v {:attr k})
                        v)))
             {}
             entity-map))

(defn entities
  [kind]
  (let [entity-order (get-in @overlay-atom [:entity-order kind] [])
        entity-index (get-in @overlay-atom [:entities kind] {})]
    (mapv #(resolve-overlay-entity (get entity-index %))
          entity-order)))

(defn merge-entity
  [kind db-entity entity-id]
  (if-let [overlay-entity (entity kind entity-id)]
    (merge (or db-entity {}) (resolve-overlay-entity overlay-entity))
    db-entity))

(defn merge-entities
  [kind db-entities]
  (let [identity-attr (get-in overlay-kinds [kind :identity])
        entity-order  (get-in @overlay-atom [:entity-order kind] [])
        overlay-index (get-in @overlay-atom [:entities kind] {})
        merged-db     (mapv (fn [db-entity]
                              (let [entity-id (get db-entity identity-attr)]
                                (merge db-entity
                                       (some-> (get overlay-index entity-id)
                                               resolve-overlay-entity))))
                            db-entities)
        seen-ids      (set (keep #(get % identity-attr) db-entities))
        overlay-only  (->> entity-order
                           (remove seen-ids)
                           (mapv #(resolve-overlay-entity (get overlay-index %))))]
    (vec (concat merged-db overlay-only))))

(defn provider-default-id
  []
  (:provider-default-id @overlay-atom))

(defn admin-summary
  []
  (let [overlay @overlay-atom]
    {:active               (boolean overlay)
     :snapshot_id          (:snapshot/id overlay)
     :tenant_id            (:tenant/id overlay)
     :runtime_id           (:runtime/id overlay)
     :generated_at         (:generated-at overlay)
     :overlay_schema_version (:overlay/schema-version overlay)
     :source_overlay_schema_version (:source-overlay/schema-version overlay)
     :required_db_schema_version (:overlay/requires-db-schema-version overlay)
     :current_db_schema_version current-db-schema-version
     :source_path          (:overlay/source-path overlay)
     :loaded_at_ms         (:overlay/loaded-at-ms overlay)
     :reloadable           (boolean (:overlay/source-path overlay))
     :reload_count         (:overlay/reload-count overlay)
     :provider_default_id  (some-> (:provider-default-id overlay) name)
     :config_override_keys (->> (keys (:literal-config-overrides overlay))
                                (map key->overlay-name)
                                sort
                                vec)
     :bounded_config_keys  (->> (keys (:bounded-config overlay))
                                (map key->overlay-name)
                                sort
                                vec)
     :forced_keys          (->> (:forced-keys overlay)
                                (map key->overlay-name)
                                sort
                                vec)
     :entity_counts        {:providers      (count (entities :provider))
                            :services       (count (entities :service))
                            :oauth_accounts (count (entities :oauth-account))
                            :site_creds     (count (entities :site-cred))}}))
