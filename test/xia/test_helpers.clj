(ns xia.test-helpers
  "Shared test fixtures and helpers for xia tests."
  (:require [xia.db :as db]
            [xia.working-memory :as wm])
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
    (wm/clear-wm!)
    (db/connect! path {:passphrase-provider (constantly "xia-test-passphrase")})
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
