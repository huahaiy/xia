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

(deftest providers-persist-vision-capability
  (db/upsert-provider! {:id       :openai
                        :name     "OpenAI"
                        :base-url "https://api.openai.com/v1"
                        :model    "gpt-4o"
                        :vision?  true})
  (is (true? (:llm.provider/vision? (db/get-provider :openai))))
  (db/upsert-provider! {:id       :openai
                        :name     "OpenAI"
                        :base-url "https://api.openai.com/v1"
                        :model    "gpt-4o-mini"
                        :vision?  false})
  (is (false? (:llm.provider/vision? (db/get-provider :openai)))))

(deftest providers-persist-rate-limit-and-can-clear-it
  (db/upsert-provider! {:id                    :openai
                        :name                  "OpenAI"
                        :base-url              "https://api.openai.com/v1"
                        :model                 "gpt-4o"
                        :rate-limit-per-minute 45})
  (is (= 45 (:llm.provider/rate-limit-per-minute (db/get-provider :openai))))
  (db/upsert-provider! {:id                    :openai
                        :name                  "OpenAI"
                        :base-url              "https://api.openai.com/v1"
                        :model                 "gpt-4o-mini"
                        :rate-limit-per-minute nil})
  (is (nil? (:llm.provider/rate-limit-per-minute (db/get-provider :openai)))))

(deftest worker-sessions-are-hidden-by-default
  (let [parent (db/create-session! :terminal)
        child  (db/create-session! :branch {:parent-session-id parent
                                            :worker? true
                                            :label "site-a"})]
    (is (= [parent]
           (mapv :id (db/list-sessions))))
    (is (= #{parent child}
           (set (map :id (db/list-sessions {:include-workers? true})))))
    (let [worker (some #(when (= child (:id %)) %) (db/list-sessions {:include-workers? true}))]
      (is (true? (:worker? worker)))
      (is (= parent (:parent-id worker)))
      (is (= "site-a" (:label worker))))))
