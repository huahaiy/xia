(ns xia.runtime-overlay
  "In-memory runtime overlay for managed/cloud-specific config and catalog data.

   The overlay is loaded from an EDN bundle at startup and never persisted back
   into the tenant Datalevin DB. Reads can resolve as:

   runtime overlay > tenant DB > code defaults"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [xia.sensitive :as sensitive]
            [taoensso.timbre :as log]))

(def ^:private current-overlay-version 1)

(def ^:private supported-config-merge-modes
  #{:replace :cap :floor})

(def ^:private overlay-kinds
  {:provider      {:identity :llm.provider/id}
   :service       {:identity :service/id}
   :oauth-account {:identity :oauth.account/id}
   :site-cred     {:identity :site-cred/id}})

(defonce ^:private overlay-atom (atom nil))
(defonce ^:private activation-lock (Object.))

(declare clear!)

(defn- input-overlay-version
  [overlay]
  (or (:overlay/version overlay) 0))

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

(defn- normalize-config-entry
  [value]
  (if (config-rule? value)
    {:merge (:merge value)
     :value  (:value value)}
    {:merge :replace
     :value  value}))

(defn- secret-ref?
  [value]
  (and (map? value)
       (or (contains? value :secret-env)
           (contains? value :secret-file))))

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
  (let [env-name  (nonblank-secret-ref value :secret-env)
        file-path (nonblank-secret-ref value :secret-file)]
    (cond
      (and env-name file-path)
      (throw (ex-info "Runtime overlay secret ref must use exactly one source."
                      (assoc context :secret-ref value)))

      env-name
      (or (some-> (read-env-secret env-name) trim-trailing-newline)
          (throw (ex-info "Runtime overlay secret env is not set."
                          (assoc context :secret-env env-name))))

      file-path
      (let [file (io/file file-path)]
        (when-not (.exists file)
          (throw (ex-info "Runtime overlay secret file does not exist."
                          (assoc context :secret-file file-path))))
        (some-> (read-file-secret file-path) trim-trailing-newline))

      :else
      (throw (ex-info "Runtime overlay secret ref requires :secret-env or :secret-file."
                      (assoc context :secret-ref value))))))

(defn- entity-kind-entry
  [entity]
  (some (fn [[kind {:keys [identity]}]]
          (when (contains? entity identity)
            [kind identity]))
        overlay-kinds))

(defn- migrate-v0->v1
  [overlay]
  (assoc overlay :overlay/version current-overlay-version))

(def ^:private overlay-migrations
  {0 migrate-v0->v1})

(defn- migrate-overlay
  [overlay]
  (loop [overlay* overlay
         version  (input-overlay-version overlay)]
    (cond
      (= version current-overlay-version)
      overlay*

      (> version current-overlay-version)
      (throw (ex-info "Unsupported runtime overlay version."
                      {:supported-version current-overlay-version
                       :overlay-version version}))

      :else
      (if-let [step (get overlay-migrations version)]
        (let [next-overlay (step overlay*)
              next-version (input-overlay-version next-overlay)]
          (when (<= next-version version)
            (throw (ex-info "Runtime overlay migration did not advance the version."
                            {:from-version version
                             :to-version next-version})))
          (recur next-overlay next-version))
        (throw (ex-info "Unsupported runtime overlay version."
                        {:supported-version current-overlay-version
                         :overlay-version version}))))))

(defn- validate-overlay!
  [overlay]
  (when-not (map? overlay)
    (throw (ex-info "Runtime overlay must be an EDN map."
                    {:overlay overlay})))
  (when-not (= current-overlay-version (:overlay/version overlay))
    (throw (ex-info "Unsupported runtime overlay version."
                    {:supported-version current-overlay-version
                     :overlay-version (:overlay/version overlay)})))
  (when-not (nonblank-string? (:snapshot/id overlay))
    (throw (ex-info "Runtime overlay requires a non-empty :snapshot/id."
                    {:snapshot/id (:snapshot/id overlay)})))
  (when-not (or (nil? (:config-overrides overlay))
                (map? (:config-overrides overlay)))
    (throw (ex-info "Runtime overlay :config-overrides must be a map."
                    {:value (:config-overrides overlay)})))
  (when-not (every? keyword? (keys (:config-overrides overlay)))
    (throw (ex-info "Runtime overlay :config-overrides keys must be keywords."
                    {:keys (keys (:config-overrides overlay))})))
  (doseq [[config-key entry] (:config-overrides overlay)]
    (let [{:keys [merge value]} (normalize-config-entry entry)]
      (when-not (contains? supported-config-merge-modes merge)
        (throw (ex-info "Runtime overlay config override uses an unsupported merge mode."
                        {:config-key config-key
                         :merge merge
                         :supported-modes (sort supported-config-merge-modes)})))
      (when (and (config-rule? entry)
                 (not (contains? entry :value)))
        (throw (ex-info "Runtime overlay config override rules require a :value."
                        {:config-key config-key
                         :value entry})))
      (when (secret-ref? value)
        (when-not (sensitive/secret-config-key? config-key)
          (throw (ex-info "Runtime overlay secret refs are only allowed for secret config keys."
                          {:config-key config-key})))
        (resolve-secret-ref value {:config-key config-key}))))
  (when-not (or (nil? (:forced-keys overlay))
                (set? (:forced-keys overlay)))
    (throw (ex-info "Runtime overlay :forced-keys must be a set of keywords."
                    {:value (:forced-keys overlay)})))
  (when-not (every? keyword? (:forced-keys overlay))
    (throw (ex-info "Runtime overlay :forced-keys entries must be keywords."
                    {:forced-keys (:forced-keys overlay)})))
  (when-not (or (nil? (:tx-data overlay))
                (vector? (:tx-data overlay)))
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
  [overlay source-version]
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
      {:overlay/version current-overlay-version
       :source-overlay/version source-version
       :snapshot/id (:snapshot/id overlay)
       :config-overrides (into {}
                               (map (fn [[config-key value]]
                                     [config-key (normalize-config-entry value)]))
                               (or (:config-overrides overlay) {}))
       :forced-keys (or (:forced-keys overlay) #{})
       :tx-data (or (:tx-data overlay) [])
       :provider-default-id (first provider-default-ids)}
      (:tx-data overlay))))

(defn- activate-overlay!
  [overlay source-path]
  (locking activation-lock
    (let [previous       @overlay-atom
          source-version (input-overlay-version overlay)
          normalized     (-> overlay
                             migrate-overlay
                             validate-overlay!
                             (normalize-overlay source-version))
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
  (:overlay/version @overlay-atom))

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
     :overlay_version      (:overlay/version overlay)
     :source_overlay_version (:source-overlay/version overlay)
     :source_path          (:overlay/source-path overlay)
     :loaded_at_ms         (:overlay/loaded-at-ms overlay)
     :reloadable           (boolean (:overlay/source-path overlay))
     :reload_count         (:overlay/reload-count overlay)
     :provider_default_id  (some-> (:provider-default-id overlay) name)
     :config_override_keys (->> (keys (:config-overrides overlay))
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
