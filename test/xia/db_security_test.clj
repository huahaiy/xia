(ns xia.db-security-test
  (:require [clojure.test :refer :all]
            [datalevin.core :as d]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.schedule :as schedule]
            [xia.test-helpers :as th :refer [with-test-db]]))

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

(deftest oauth-account-secrets-are-encrypted-at-rest
  (db/register-oauth-account! {:id            :google
                               :name          "Google"
                               :authorize-url "https://accounts.google.com/o/oauth2/v2/auth"
                               :token-url     "https://oauth2.googleapis.com/token"
                               :client-id     "client-123"
                               :client-secret "client-secret"
                               :access-token  "access-123"
                               :refresh-token "refresh-123"})
  (let [eid     (ffirst (db/q '[:find ?e :where [?e :oauth.account/id :google]]))
        raw     (raw-entity eid)
        account (db/get-oauth-account :google)]
    (is (crypto/encrypted? (:oauth.account/client-secret raw)))
    (is (crypto/encrypted? (:oauth.account/access-token raw)))
    (is (crypto/encrypted? (:oauth.account/refresh-token raw)))
    (is (= "client-secret" (:oauth.account/client-secret account)))
    (is (= "access-123" (:oauth.account/access-token account)))
    (is (= "refresh-123" (:oauth.account/refresh-token account)))))

(deftest secret-config-values-are-encrypted-at-rest
  (db/set-config! :token/github "gho_secret")
  (let [raw-value (ffirst (db/q '[:find ?v :where
                                  [?e :config/key :token/github]
                                  [?e :config/value ?v]]))]
    (is (crypto/encrypted? raw-value))
    (is (= "gho_secret" (db/get-config :token/github)))))

(deftest llm-log-payloads-are-stored-in-plaintext-at-rest
  (let [call-id (random-uuid)]
    (db/log-llm-call! {:id call-id
                       :session-id (random-uuid)
                       :provider-id :openrouter
                       :model "moonshotai/kimi-k2.5"
                       :messages "[{\"role\":\"user\",\"content\":\"secret\"}]"
                       :response "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"
                       :tools "[{\"name\":\"browser-open\"}]"
                       :error "provider error"
                       :status :ok})
    (let [eid (ffirst (db/q '[:find ?e :in $ ?id :where [?e :llm.log/id ?id]] call-id))
          raw (raw-entity eid)
          call (db/get-llm-call call-id)]
      (is (= "[{\"role\":\"user\",\"content\":\"secret\"}]" (:llm.log/messages raw)))
      (is (= "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}" (:llm.log/response raw)))
      (is (= "[{\"name\":\"browser-open\"}]" (:llm.log/tools raw)))
      (is (= "provider error" (:llm.log/error raw)))
      (is (= "[{\"role\":\"user\",\"content\":\"secret\"}]" (:messages call)))
      (is (= "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}" (:response call)))
      (is (= "[{\"name\":\"browser-open\"}]" (:tools call)))
      (is (= "provider error" (:error call))))))

(deftest audit-event-payloads-are-stored-in-plaintext-at-rest
  (let [event-id (random-uuid)
        sid (db/create-session! :http)]
    (db/log-audit-event! {:id event-id
                          :session-id sid
                          :channel :http
                          :actor :user
                          :type :approval-decision
                          :data {:approved true
                                 :arguments {"secret" "value"}}})
    (let [eid (ffirst (db/q '[:find ?e :in $ ?id :where [?e :audit.event/id ?id]] event-id))
          raw (raw-entity eid)
          event (first (db/session-audit-events sid))]
      (is (= "{\"approved\":true,\"arguments\":{\"secret\":\"value\"}}"
             (:audit.event/data raw)))
      (is (= {"approved" true
              "arguments" {"secret" "value"}}
             (:data event))))))

(deftest transcripts-keep-structured-tool-payloads
  (let [sid        (db/create-session! :terminal)
        tool-calls [{"id"       "call_1"
                     "function" {"name"      "service-request"
                                 "arguments" "{\"query\":\"secret\"}"}}]]
    (db/add-message! sid :user "pasted local secret")
    (db/add-message! sid :assistant "checking service"
                     :tool-calls tool-calls)
    (db/add-message! sid :tool nil
                     :tool-result {"token" "top-secret"}
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
      (is (= "pasted local secret" (:message/content (:user raw-messages))))
      (is (= "checking service" (:message/content (:assistant raw-messages))))
      (is (= {:calls tool-calls}
             (:message/tool-calls (:assistant raw-messages))))
      (is (= {:result {"token" "top-secret"}}
             (:message/tool-result (:tool raw-messages))))
      (is (= [{:role :user
               :id (:id (first messages))
               :content "pasted local secret"
               :created-at (:created-at (first messages))
               :local-docs nil
               :artifacts nil
               :tool-calls nil
               :tool-result nil
               :tool-id nil
               :tool-call-id nil
               :tool-name nil
               :llm-call-id nil
               :provider-id nil
               :model nil
               :workload nil}
              {:role :assistant
               :id (:id (second messages))
               :content "checking service"
               :created-at (:created-at (second messages))
               :local-docs nil
               :artifacts nil
               :tool-calls tool-calls
               :tool-result nil
               :tool-id nil
               :tool-call-id nil
               :tool-name nil
               :llm-call-id nil
               :provider-id nil
               :model nil
               :workload nil}
              {:role :tool
               :id (:id (nth messages 2))
               :content nil
               :created-at (:created-at (nth messages 2))
               :local-docs nil
               :artifacts nil
               :tool-calls nil
               :tool-result {"token" "top-secret"}
               :tool-id "call_1"
               :tool-call-id nil
               :tool-name nil
               :llm-call-id nil
               :provider-id nil
               :model nil
               :workload nil}]
             messages)))))

(deftest schedule-run-payloads-are-stored-in-plaintext-at-rest
  (schedule/create-schedule!
    {:id :security-hist
     :spec {:minute #{0} :hour #{9}}
     :type :tool
     :tool-id :x})
  (schedule/record-run! :security-hist
    {:started-at  (java.util.Date.)
     :finished-at (java.util.Date.)
     :status      :error
     :actions     [{:tool-id "service-call"
                    :status "blocked"
                    :arguments {"endpoint" "/gmail/v1/messages"}}]
     :result      "{\"records\":[\"secret\"]}"
     :error       "token leak"})
  (let [eid     (ffirst (db/q '[:find ?e :where
                                [?e :schedule-run/schedule-id :security-hist]]))
        raw     (raw-entity eid)
        history (schedule/schedule-history :security-hist)]
    (is (= "{\"records\":[\"secret\"]}" (:schedule-run/result raw)))
    (is (= "token leak" (:schedule-run/error raw)))
    (is (= {:events [{:tool-id "service-call"
                      :status "blocked"
                      :arguments {"endpoint" "/gmail/v1/messages"}}]}
           (:schedule-run/actions raw)))
    (is (= [{:tool-id "service-call"
             :status "blocked"
             :arguments {"endpoint" "/gmail/v1/messages"}}]
           (:actions (first history))))
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

(deftest plaintext-secret-migration-batches-configs-and-attrs
  (d/transact! (db/conn)
               [{:service/id        :legacy-batch
                 :service/base-url  "https://legacy.example"
                 :service/auth-type :bearer
                 :service/auth-key  "legacy-batch-token"
                 :service/enabled?  true}
                {:config/key   :token/github
                 :config/value "gho_batch_secret"}])
  (#'xia.db/migrate-secrets!)
  (let [eid       (ffirst (db/q '[:find ?e :where [?e :service/id :legacy-batch]]))
        raw       (raw-entity eid)
        raw-value (ffirst (db/q '[:find ?v :where
                                  [?e :config/key :token/github]
                                  [?e :config/value ?v]]))]
    (is (crypto/encrypted? (:service/auth-key raw)))
    (is (crypto/encrypted? raw-value))
    (is (= "legacy-batch-token" (:service/auth-key (db/get-service :legacy-batch))))
    (is (= "gho_batch_secret" (db/get-config :token/github)))))

(deftest plaintext-secret-migration-commits-in-batches
  (let [orig-transact! xia.db/transact!
        batch-sizes    (atom [])]
    (doseq [n (range 450)]
      (d/transact! (db/conn)
                   [{:service/id        (keyword (str "legacy-batch-" n))
                     :service/base-url  (str "https://legacy-" n ".example")
                     :service/auth-type :bearer
                     :service/auth-key  (str "token-" n)
                     :service/enabled?  true}]))
    (with-redefs [xia.db/transact!
                  (fn [tx-data]
                    (swap! batch-sizes conj (count tx-data))
                    (orig-transact! tx-data))]
      (#'xia.db/migrate-secrets!))
    (is (= [200 200 50] @batch-sizes))
    (is (= "token-0" (:service/auth-key (db/get-service :legacy-batch-0))))
    (is (= "token-449" (:service/auth-key (db/get-service :legacy-batch-449))))))

(deftest plaintext-transcripts-are-left-plain-by-migration
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
                   :message/tool-calls {:calls [{"id" "call_1"}]}
                   :message/created-at (java.util.Date.)}])
    (#'xia.db/migrate-secrets!)
    (let [eid      (ffirst (db/q '[:find ?e :in $ ?id :where [?e :message/id ?id]]
                                 message-id))
          raw      (raw-entity eid)
          messages (db/session-messages sid)]
      (is (= "legacy transcript" (:message/content raw)))
      (is (= {:calls [{"id" "call_1"}]} (:message/tool-calls raw)))
      (is (= "legacy transcript" (:content (first messages))))
      (is (= [{"id" "call_1"}] (:tool-calls (first messages)))))))
