(ns xia.db-schema
  "Versioned Datalevin schema resources and migration registry."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datalevin.core :as d])
  (:import [java.math BigInteger]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util Date]))

(def current-version 2)

(def ^:private frozen-schema-digests
  {1 "9bde4d7aa57384b041ff356140deeedbd62e4f858b32b1a89d24de98c1caabda"})

(def schema-version-meta-key :db/schema-version)
(def schema-resource-path-meta-key :db/schema-resource-path)
(def schema-applied-at-meta-key :db/schema-applied-at)
(def schema-migration-history-meta-key :db/schema-migration-history)

(declare schema-resource-path)

(defn frozen-schema-versions
  []
  (->> (keys frozen-schema-digests)
       sort
       vec))

(defn released-schema-version
  []
  (or (last (frozen-schema-versions))
      0))

(defn schema-frozen?
  [version]
  (contains? frozen-schema-digests version))

(defn- meta-value
  [conn meta-key]
  (ffirst (d/q '[:find ?v :in $ ?k
                 :where
                 [?e :xia.meta/key ?k]
                 [?e :xia.meta/value ?v]]
               (d/db conn)
               meta-key)))

(defn- set-meta!
  [conn meta-key value]
  (d/transact! conn [{:xia.meta/key meta-key
                      :xia.meta/value (str value)
                      :xia.meta/updated-at (Date.)}]))

(defn migration-history-value
  [conn]
  (or (some-> (meta-value conn schema-migration-history-meta-key)
              edn/read-string
              vec)
      []))

(defn stored-schema-resource-path
  [conn]
  (meta-value conn schema-resource-path-meta-key))

(defn stored-schema-applied-at
  [conn]
  (meta-value conn schema-applied-at-meta-key))

(defn record-migration-history!
  [conn entry]
  (let [history (migration-history-value conn)
        next-history (conj (or history []) entry)]
    (set-meta! conn schema-migration-history-meta-key (pr-str next-history))))

(defn- now-text
  []
  (.toString (java.time.Instant/now)))

(def ^:private migration-registry
  {1 {:to-version 2
      :description "Add mail service fields for IMAP/SMTP support."
      :apply! (fn [_conn]
                nil)}})

(defn ensure-current-metadata!
  [conn]
  (let [current-version-str  (str current-version)
        current-resource-path (schema-resource-path current-version)
        stored-version        (meta-value conn schema-version-meta-key)
        stored-resource-path  (stored-schema-resource-path conn)
        stored-applied-at     (stored-schema-applied-at conn)]
    (when-not (= stored-version current-version-str)
      (set-meta! conn schema-version-meta-key current-version-str))
    (when-not (= stored-resource-path current-resource-path)
      (set-meta! conn schema-resource-path-meta-key current-resource-path))
    (when (or (not= stored-version current-version-str)
              (not= stored-resource-path current-resource-path)
              (nil? stored-applied-at))
      (set-meta! conn schema-applied-at-meta-key (.toString (java.time.Instant/now))))))

(defn schema-resource-path
  [version]
  (str "xia/schema/" version ".edn"))

(defn- load-schema-text
  [version]
  (let [resource-path (schema-resource-path version)
        resource      (io/resource resource-path)]
    (when-not resource
      (throw (ex-info "Database schema resource is missing."
                      {:version version
                       :resource-path resource-path})))
    (slurp resource)))

(defn- sha256-hex
  [text]
  (let [digest (MessageDigest/getInstance "SHA-256")
        bytes  (.digest digest (.getBytes ^String text StandardCharsets/UTF_8))]
    (format "%064x" (BigInteger. 1 bytes))))

(defn schema-digest
  [version]
  (sha256-hex (load-schema-text version)))

(defn ensure-frozen-schema-integrity!
  []
  (doseq [[version expected-digest] (sort-by key frozen-schema-digests)]
    (let [resource-path (schema-resource-path version)
          actual-digest (schema-digest version)]
      (when-not (= expected-digest actual-digest)
        (throw (ex-info "Frozen schema resource was modified. Bump the schema version instead of editing a frozen schema."
                        {:reason :db-schema/frozen-schema-modified
                         :version version
                         :resource-path resource-path
                         :expected-sha256 expected-digest
                         :actual-sha256 actual-digest
                         :released-schema-version (released-schema-version)
                         :current-schema-version current-version})))))
  true)

(defn load-schema
  [version]
  (-> version
      load-schema-text
      edn/read-string))

(defn current-schema
  []
  (ensure-frozen-schema-integrity!)
  (load-schema current-version))

(defn migration-step
  [from-version]
  (get migration-registry from-version))

(defn migration-path
  [from-version]
  (loop [version from-version
         steps   []]
    (cond
      (= version current-version)
      steps

      (> version current-version)
      nil

      :else
      (when-let [step (migration-step version)]
        (recur (:to-version step)
               (conj steps (assoc step :from-version version)))))))

(defn migration-registry-summary
  []
  (->> migration-registry
       (map (fn [[from-version {:keys [to-version description]}]]
              {:from-version from-version
               :to-version to-version
               :description description}))
       (sort-by (juxt :from-version :to-version))
       vec))

(defn apply-migrations!
  [conn from-version]
  (when (and (some? from-version)
             (pos? (long from-version))
             (< from-version current-version))
    (let [steps (migration-path from-version)]
      (when-not steps
        (throw (ex-info "Database schema migration path is missing."
                        {:from-version from-version
                         :current-schema-version current-version
                         :reason :db-schema/migration-path-missing})))
      (doseq [{:keys [from-version to-version description apply!]} steps]
        (when apply!
          (apply! conn))
        (record-migration-history! conn
                                   {:from-version from-version
                                    :to-version to-version
                                    :description description
                                    :resource-path (schema-resource-path to-version)
                                    :applied-at (now-text)}))))
  true)
