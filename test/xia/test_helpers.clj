(ns xia.test-helpers
  "Shared test fixtures and helpers for xia tests."
  (:require [xia.db :as db])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-db-path []
  (str (Files/createTempDirectory "xia-test"
         (into-array FileAttribute []))))

(defn with-test-db
  "Fixture: create a temp Datalevin DB for the duration of the test."
  [f]
  (let [path (temp-db-path)]
    (db/connect! path {:passphrase-provider (constantly "xia-test-passphrase")})
    (try
      (f)
      (finally
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
  [summary & {:keys [context processed?] :or {processed? false}}]
  (let [id (java.util.UUID/randomUUID)]
    (db/transact! [(cond-> {:episode/id         id
                            :episode/type       :conversation
                            :episode/summary    summary
                            :episode/timestamp  (java.util.Date.)
                            :episode/processed? processed?}
                     context (assoc :episode/context context))])
    (ffirst (db/q '[:find ?e :in $ ?id :where [?e :episode/id ?id]] id))))

(defn seed-fact!
  "Helper: add a fact to a node and return the fact entity id."
  [node-eid content & {:keys [confidence] :or {confidence 1.0}}]
  (let [id (java.util.UUID/randomUUID)]
    (db/transact! [{:kg.fact/id         id
                    :kg.fact/node       node-eid
                    :kg.fact/content    content
                    :kg.fact/confidence (float confidence)
                    :kg.fact/created-at (java.util.Date.)
                    :kg.fact/updated-at (java.util.Date.)}])
    (ffirst (db/q '[:find ?e :in $ ?id :where [?e :kg.fact/id ?id]] id))))
