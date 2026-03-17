(ns xia.test-helpers
  "Shared test fixtures and helpers for xia tests."
  (:require [clojure.string :as str]
            [datalevin.embedding :as emb]
            [xia.db :as db]
            [xia.working-memory :as wm])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-db-path []
  (str (Files/createTempDirectory "xia-test"
         (into-array FileAttribute []))))

(def ^:private test-embedding-dimensions
  32)

(def ^:private synonym->canonical
  {"auto" "vehicle"
   "automobile" "vehicle"
   "automobiles" "vehicle"
   "car" "vehicle"
   "cars" "vehicle"
   "vehicle" "vehicle"
   "vehicles" "vehicle"})

(def ^:private test-embedding-metadata
  {:embedding/provider {:kind :test
                        :id :default
                        :model-id "xia-test-embedder"}
   :embedding/output   {:dimensions test-embedding-dimensions
                        :normalize? true}
   :embedding/artifact {:format :memory
                        :file "xia-test-embedder"}})

(defn- provider-text
  [item]
  (cond
    (string? item) item
    (map? item)    (or (:text item) "")
    :else          (str item)))

(defn- canonical-token
  [token]
  (get synonym->canonical token token))

(defn- tokenize
  [text]
  (->> (str/split (str/lower-case (or text "")) #"[^\p{Alnum}]+")
       (remove str/blank?)
       (map canonical-token)))

(defn- token-slot
  [token]
  (Math/floorMod (int (hash token)) (int test-embedding-dimensions)))

(defn- normalize-vector
  [values]
  (let [norm (Math/sqrt
               (reduce (fn [sum value]
                         (+ sum (* value value)))
                       0.0
                       values))]
    (if (pos? norm)
      (mapv #(float (/ % norm)) values)
      values)))

(defn- embed-text
  [text]
  (normalize-vector
    (reduce (fn [values token]
              (update values (token-slot token) + 1.0))
            (vec (repeat test-embedding-dimensions 0.0))
            (tokenize text))))

(defn test-embedding-provider
  []
  (reify
    emb/IEmbeddingProvider
    (embedding [_ items _opts]
      (mapv (comp embed-text provider-text) items))
    (embedding-metadata [_]
      test-embedding-metadata)
    (embedding-dimensions [_]
      test-embedding-dimensions)
    (close-provider [_]
      nil)

    java.lang.AutoCloseable
    (close [_]
      nil)))

(defn test-connect-options
  ([]
   (test-connect-options nil))
  ([options]
   (let [provider-id (get-in (db/default-datalevin-opts) [:embedding-opts :provider])]
     (update (merge options
                    {:datalevin-opts
                     {:embedding-providers {provider-id (test-embedding-provider)}}})
             :datalevin-opts
             #(merge (db/default-datalevin-opts) %)))))

(defn with-test-db
  "Fixture: create a temp Datalevin DB for the duration of the test."
  [f]
  (let [path (temp-db-path)]
    (wm/clear-wm!)
    (db/connect! path (test-connect-options
                        {:passphrase-provider (constantly "xia-test-passphrase")}))
    (try
      (f)
      (finally
        (wm/clear-wm!)
        (db/close!)))))

(defn seed-node!
  "Helper: create a KG node and return its entity id."
  [name type]
  (let [id (java.util.UUID/randomUUID)]
    (db/transact! [{:kg.node/id         id
                    :kg.node/name       name
                    :kg.node/type       (keyword type)
                    :kg.node/created-at (java.util.Date.)
                    :kg.node/updated-at (java.util.Date.)}])
    (ffirst (db/q '[:find ?e :in $ ?id :where [?e :kg.node/id ?id]] id))))

(defn seed-episode!
  "Helper: create an episode and return its entity id."
  [summary & {:keys [context processed? timestamp session-id channel type importance]
              :or {processed? false
                   timestamp  (java.util.Date.)
                   type       :conversation}}]
  (let [id (java.util.UUID/randomUUID)]
    (db/transact! [(cond-> {:episode/id         id
                            :episode/type       type
                            :episode/summary    summary
                            :episode/timestamp  timestamp
                            :episode/processed? processed?}
                     context    (assoc :episode/context context)
                     importance (assoc :episode/importance (float importance))
                     session-id (assoc :episode/session-id (str session-id))
                     channel    (assoc :episode/channel (name channel)))])
    (ffirst (db/q '[:find ?e :in $ ?id :where [?e :episode/id ?id]] id))))

(defn seed-fact!
  "Helper: add a fact to a node and return the fact entity id."
  [node-eid content & {:keys [confidence utility] :or {confidence 1.0}}]
  (let [id (java.util.UUID/randomUUID)]
    (db/transact! [(cond-> {:kg.fact/id         id
                            :kg.fact/node       node-eid
                            :kg.fact/content    content
                            :kg.fact/confidence (float confidence)
                            :kg.fact/created-at (java.util.Date.)
                            :kg.fact/updated-at (java.util.Date.)}
                     (some? utility)
                     (assoc :kg.fact/utility (float utility)))])
    (ffirst (db/q '[:find ?e :in $ ?id :where [?e :kg.fact/id ?id]] id))))
