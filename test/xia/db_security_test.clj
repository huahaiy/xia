(ns xia.db-security-test
  (:require [clojure.test :refer :all]
            [datalevin.core :as d]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.schedule :as schedule]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(defn- raw-entity [eid]
  (into {} (d/entity (d/db (db/conn)) eid)))

(deftest service-auth-is-encrypted-at-rest
  (db/register-service! {:id :github
                         :base-url "https://api.github.com"
                         :auth-type :bearer
                         :auth-key "ghp-secret-token"})
  (let [eid (ffirst (db/q '[:find ?e :where [?e :service/id :github]]))
        raw (raw-entity eid)
        svc (db/get-service :github)]
    (is (crypto/encrypted? (:service/auth-key raw)))
    (is (not= "ghp-secret-token" (:service/auth-key raw)))
    (is (= "ghp-secret-token" (:service/auth-key svc)))))

(deftest site-credentials-are-encrypted-at-rest
  (db/register-site-cred! {:id :portal
                           :login-url "https://portal.example/login"
                           :username "alice@example.com"
                           :password "s3cret"})
  (let [eid  (ffirst (db/q '[:find ?e :where [?e :site-cred/id :portal]]))
        raw  (raw-entity eid)
        cred (db/get-site-cred :portal)]
    (is (crypto/encrypted? (:site-cred/username raw)))
    (is (crypto/encrypted? (:site-cred/password raw)))
    (is (= "alice@example.com" (:site-cred/username cred)))
    (is (= "s3cret" (:site-cred/password cred)))))

(deftest provider-api-key-is-encrypted-at-rest
  (db/upsert-provider! {:id :default
                        :name "default"
                        :base-url "https://api.openai.com/v1"
                        :api-key "sk-secret"
                        :model "gpt-test"
                        :default? true})
  (let [eid      (ffirst (db/q '[:find ?e :where [?e :llm.provider/id :default]]))
        raw      (raw-entity eid)
        provider (db/get-default-provider)]
    (is (crypto/encrypted? (:llm.provider/api-key raw)))
    (is (= "sk-secret" (:llm.provider/api-key provider)))))

(deftest secret-config-values-are-encrypted-at-rest
  (db/set-config! :token/github "gho_secret")
  (let [raw-value (ffirst (db/q '[:find ?v :where
                                  [?e :config/key :token/github]
                                  [?e :config/value ?v]]))]
    (is (crypto/encrypted? raw-value))
    (is (= "gho_secret" (db/get-config :token/github)))))

(deftest transcripts-and-tool-payloads-are-encrypted-at-rest
  (let [sid        (db/create-session! :terminal)
        tool-calls "[{\"id\":\"call_1\",\"function\":{\"name\":\"service-request\",\"arguments\":\"{\\\"query\\\":\\\"secret\\\"}\"}}]"]
    (db/add-message! sid :user "pasted local secret")
    (db/add-message! sid :assistant "checking service"
                     :tool-calls tool-calls)
    (db/add-message! sid :tool "{\"token\":\"top-secret\"}"
                     :tool-id "call_1")
    (let [raw-messages (->> (db/q '[:find ?e ?role
                                    :in $ ?sid
                                    :where
                                    [?s :session/id ?sid]
                                    [?e :message/session ?s]
                                    [?e :message/role ?role]]
                                  sid)
                            (map (fn [[eid role]] [role (raw-entity eid)]))
                            (into {}))
          messages     (db/session-messages sid)]
      (is (crypto/encrypted? (:message/content (:user raw-messages))))
      (is (crypto/encrypted? (:message/content (:assistant raw-messages))))
      (is (crypto/encrypted? (:message/tool-calls (:assistant raw-messages))))
      (is (crypto/encrypted? (:message/content (:tool raw-messages))))
      (is (= [{:role :user
               :content "pasted local secret"
               :created-at (:created-at (first messages))
               :tool-calls nil
               :tool-id nil}
              {:role :assistant
               :content "checking service"
               :created-at (:created-at (second messages))
               :tool-calls tool-calls
               :tool-id nil}
              {:role :tool
               :content "{\"token\":\"top-secret\"}"
               :created-at (:created-at (nth messages 2))
               :tool-calls nil
               :tool-id "call_1"}]
             messages)))))

(deftest schedule-run-payloads-are-encrypted-at-rest
  (schedule/create-schedule!
    {:id :security-hist
     :spec {:minute #{0} :hour #{9}}
     :type :tool
     :tool-id :x})
  (schedule/record-run! :security-hist
    {:started-at  (java.util.Date.)
     :finished-at (java.util.Date.)
     :status      :error
     :result      "{\"records\":[\"secret\"]}"
     :error       "token leak"})
  (let [eid     (ffirst (db/q '[:find ?e :where
                                [?e :schedule-run/schedule-id :security-hist]]))
        raw     (raw-entity eid)
        history (schedule/schedule-history :security-hist)]
    (is (crypto/encrypted? (:schedule-run/result raw)))
    (is (crypto/encrypted? (:schedule-run/error raw)))
    (is (= "{\"records\":[\"secret\"]}" (:result (first history))))
    (is (= "token leak" (:error (first history))))))

(deftest plaintext-secrets-are-migrated
  (d/transact! (db/conn)
               [{:service/id        :legacy
                 :service/base-url  "https://legacy.example"
                 :service/auth-type :bearer
                 :service/auth-key  "legacy-token"
                 :service/enabled?  true}])
  (#'xia.db/migrate-secrets!)
  (let [eid (ffirst (db/q '[:find ?e :where [?e :service/id :legacy]]))
        raw (raw-entity eid)
        svc (db/get-service :legacy)]
    (is (crypto/encrypted? (:service/auth-key raw)))
    (is (= "legacy-token" (:service/auth-key svc)))))

(deftest plaintext-transcripts-are-migrated
  (let [sid         (db/create-session! :terminal)
        session-eid (ffirst (db/q '[:find ?e :in $ ?sid
                                    :where [?e :session/id ?sid]]
                                  sid))
        message-id  (random-uuid)]
    (d/transact! (db/conn)
                 [{:message/id         message-id
                   :message/session    session-eid
                   :message/role       :assistant
                   :message/content    "legacy transcript"
                   :message/tool-calls "[{\"id\":\"call_1\"}]"
                   :message/created-at (java.util.Date.)}])
    (#'xia.db/migrate-secrets!)
    (let [eid      (ffirst (db/q '[:find ?e :in $ ?id :where [?e :message/id ?id]]
                                 message-id))
          raw      (raw-entity eid)
          messages (db/session-messages sid)]
      (is (crypto/encrypted? (:message/content raw)))
      (is (crypto/encrypted? (:message/tool-calls raw)))
      (is (= "legacy transcript" (:content (first messages))))
      (is (= "[{\"id\":\"call_1\"}]" (:tool-calls (first messages)))))))
