(ns xia.db-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.db :as db]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest default-datalevin-opts-use-xia-managed-nomic-provider
  (let [opts        (db/default-datalevin-opts)
        provider-id (get-in opts [:embedding-opts :provider])
        provider    (get-in opts [:embedding-providers provider-id])]
    (is (= :xia-default provider-id))
    (is (= :llama.cpp (:provider provider)))
    (is (= "nomic-embed-text-v2-moe-q8_0.gguf" (:model-filename provider)))
    (is (= 768 (get-in provider [:embedding-metadata :embedding/output :dimensions])))
    (is (true? (:validate-data? opts)))
    (is (true? (:auto-entity-time? opts)))))

(deftest datalevin-type-coercion-and-auto-entity-time-are-enabled
  (let [session-id (str (random-uuid))]
    (db/transact! [{:session/id      session-id
                    :session/channel "terminal"
                    :session/active? true}])
    (let [session-eid (ffirst (db/q '[:find ?e :in $ ?sid :where [?e :session/id ?sid]]
                                    (java.util.UUID/fromString session-id)))
          entity      (db/entity session-eid)
          session     (first (db/list-sessions))]
      (is (uuid? (:session/id entity)))
      (is (= :terminal (:session/channel entity)))
      (is (integer? (:db/created-at entity)))
      (is (instance? java.util.Date (:created-at session))))))
