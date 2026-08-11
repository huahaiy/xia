(ns xia.secret-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.secret :as secret]
            [xia.sensitive :as sensitive]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

;; ---------------------------------------------------------------------------
;; secret-attr? tests
;; ---------------------------------------------------------------------------

(deftest secret-attr?-test
  (testing "known secret attributes"
    (is (secret/secret-attr? :llm.provider/api-key))
    (is (secret/secret-attr? :oauth.account/client-secret))
    (is (secret/secret-attr? :message/content))
    (is (secret/secret-attr? :message/tool-result))
    (is (secret/secret-attr? :llm.log/messages))
    (is (secret/secret-attr? :llm.log/response))
    (is (secret/secret-attr? :audit.event/data))
    (is (secret/secret-attr? :schedule-run/result))
    (is (secret/secret-attr? :schedule-run/actions)))
  (testing "secret namespace prefixes"
    (is (secret/secret-attr? :credential/gmail-token))
    (is (secret/secret-attr? :secret/my-key)))
  (testing "non-secret attributes"
    (is (not (secret/secret-attr? :config/key)))
    (is (not (secret/secret-attr? :llm.provider/model)))
    (is (not (secret/secret-attr? :user/name)))))

;; ---------------------------------------------------------------------------
;; secret-config-key? tests
;; ---------------------------------------------------------------------------

(deftest secret-config-key?-test
  (testing "secret config key prefixes"
    (is (secret/secret-config-key? :credential/gmail))
    (is (secret/secret-config-key? :secret/something))
    (is (secret/secret-config-key? :api-key/openai))
    (is (secret/secret-config-key? :oauth/google))
    (is (secret/secret-config-key? :token/refresh)))
  (testing "non-secret config keys"
    (is (not (secret/secret-config-key? :user/name)))
    (is (not (secret/secret-config-key? :context/budget)))))

(deftest secret-query-ident?-test
  (testing "uses the shared config-key classifier"
    (is (sensitive/secret-query-ident? :oauth/google))
    (is (sensitive/secret-query-ident? 'oauth/google))
    (is (sensitive/secret-query-ident? :web/search-brave-api-key)))
  (testing "keeps non-secret config keys query-safe as literals"
    (is (not (sensitive/secret-query-ident? :user/name)))))

(deftest encrypted-attr?-test
  (testing "credentials stay encrypted at rest"
    (is (sensitive/encrypted-attr? :llm.provider/api-key))
    (is (sensitive/encrypted-attr? :oauth.account/access-token))
    (is (sensitive/encrypted-attr? :site-cred/password)))
  (testing "transcript and audit payloads stay plaintext at rest"
    (is (not (sensitive/encrypted-attr? :message/content)))
    (is (not (sensitive/encrypted-attr? :llm.log/response)))
    (is (not (sensitive/encrypted-attr? :audit.event/data)))
    (is (not (sensitive/encrypted-attr? :schedule-run/result))))
  (testing "plaintext sensitive classes are explicit, disjoint, and sandbox-redacted"
    (is (contains? sensitive/plaintext-user-content-attrs :message/tool-calls))
    (is (contains? sensitive/plaintext-user-content-attrs :audit.event/data))
    (is (contains? sensitive/plaintext-user-content-attrs :schedule-run/actions))
    (is (contains? sensitive/plaintext-diagnostic-attrs :llm.log/messages))
    (is (contains? sensitive/plaintext-diagnostic-attrs :llm.log/error))
    (is (empty? (filter sensitive/encrypted-attrs
                        sensitive/sandbox-only-secret-attrs)))
    (is (every? sensitive/secret-attr?
                sensitive/sandbox-only-secret-attrs))))

;; ---------------------------------------------------------------------------
;; safe-get-config tests
;; ---------------------------------------------------------------------------

(deftest safe-get-config-test
  (testing "allows non-secret config reads"
    (db/set-config! :user/name "Alice")
    (is (= "Alice" (secret/safe-get-config :user/name))))
  (testing "blocks secret config reads"
    (db/set-config! :credential/gmail "token123")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Access denied"
                          (secret/safe-get-config :credential/gmail)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Access denied"
                          (secret/safe-get-config :secret/my-key)))))

;; ---------------------------------------------------------------------------
;; safe-set-config! tests
;; ---------------------------------------------------------------------------

(deftest safe-set-config!-test
  (testing "allows non-secret config writes"
    (secret/safe-set-config! :user/name "Bob")
    (is (= "Bob" (db/get-config :user/name))))
  (testing "blocks secret config writes"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Access denied"
                          (secret/safe-set-config! :credential/gmail "stolen")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Access denied"
                          (secret/safe-set-config! :secret/stuff "nope"))))
  (testing "blocks privacy-boundary config writes"
    (is (= "false" (do
                     (db/set-config! :llm/log-full-payloads? false)
                     (secret/safe-get-config :llm/log-full-payloads?))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Access denied"
                          (secret/safe-set-config! :llm/log-full-payloads? true)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Access denied"
                          (secret/safe-set-config! :llm/log-retention-days 3650)))))

;; ---------------------------------------------------------------------------
;; safe-q tests
;; ---------------------------------------------------------------------------

(deftest safe-q-blocks-secret-queries
  ;; Seed a provider with an API key
  (db/transact! [{:llm.provider/id       :test
                  :llm.provider/name     "test"
                  :llm.provider/base-url "http://localhost"
                  :llm.provider/api-key  "sk-super-secret"
                  :llm.provider/model    "test-model"
                  :llm.provider/default? true}])

  (testing "blocks direct api-key query"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?v :where [?e :llm.provider/api-key ?v]]))))

  (testing "blocks query with api-key in where clause"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?e :where
                          [?e :llm.provider/api-key "sk-super-secret"]]))))

  (testing "blocks queries referencing credential namespace"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?v :where [?e :credential/token ?v]]))))

  (testing "allows non-secret queries"
    (let [results (secret/safe-q '[:find ?name :where
                                   [?e :llm.provider/name ?name]])]
      (is (= #{["test"]} (set results))))))

(deftest safe-q-blocks-pattern-based-secrets
  (testing "blocks queries with secret-like attribute names"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?v :where [?e :service/password ?v]])))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?v :where [?e :service/api-key ?v]])))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?v :where [?e :auth/oauth-token ?v]])))))

(deftest safe-q-blocks-indirect-attribute-access
  (db/transact! [{:service/id        :leak
                  :service/name      "Leak"
                  :service/base-url  "https://example.com"
                  :service/auth-type :bearer
                  :service/auth-key  "top-secret"
                  :service/enabled?  true}])

  (testing "blocks wildcard-style attr scans"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?a ?v :where
                          [?e :service/id :leak]
                          [?e ?a ?v]]))))

  (testing "blocks attr-position variables from :in"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?v :in $ ?attr :where [?e ?attr ?v]]
                        :service/auth-key)))))

(deftest safe-q-blocks-raw-config-secret-access
  (db/set-config! :oauth/google "refresh-token")
  (db/set-config! :user/name "Alice")

  (testing "blocks direct secret config key lookups"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?e :where
                          [?e :config/key :oauth/google]]))))

  (testing "blocks config key enumeration"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?k :where
                          [?e :config/key ?k]]))))

  (testing "blocks raw config value reads even for non-secret keys"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?v :where
                          [?e :config/key :user/name]
                          [?e :config/value ?v]]))))

  (testing "safe-get-config remains the allowed path for non-secret config"
    (is (= "Alice" (secret/safe-get-config :user/name)))))

(deftest safe-q-blocks-non-vector-and-symbol-built-secret-queries
  (testing "rejects non-vector query forms outright"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '(:find ?v :where [?e :llm.provider/api-key ?v])))))

  (testing "rejects secret-like symbols constructed dynamically"
    (let [attr  (symbol (str "llm.provider/" "api-key"))
          query [:find '?v :where ['?e attr '?v]]]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Access denied"
           (secret/safe-q query))))))

(deftest safe-q-blocks-pull
  (db/transact! [{:llm.provider/id       :test
                  :llm.provider/name     "test"
                  :llm.provider/base-url "http://localhost"
                  :llm.provider/api-key  "sk-super-secret"
                  :llm.provider/model    "test-model"
                  :llm.provider/default? true}])

  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo #"Access denied"
       (secret/safe-q '[:find (pull ?e [*]) :where
                        [?e :llm.provider/id :test]]))))

(deftest safe-q-blocks-computed-where-clauses
  (db/transact! [{:service/id        :leak
                  :service/name      "Leak"
                  :service/base-url  "https://example.com"
                  :service/auth-type :bearer
                  :service/auth-key  "top-secret"
                  :service/enabled?  true}
                 {:llm.provider/id       :test
                  :llm.provider/name     "test"
                  :llm.provider/base-url "http://localhost"
                  :llm.provider/api-key  "sk-super-secret"
                  :llm.provider/model    "test-model"
                  :llm.provider/default? true}])

  (testing "blocks entity-returning Datalevin function clauses"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?entity :where
                          [?e :service/id :leak]
                          [(datalevin.core/entity $ ?e) ?entity]]))))

  (testing "blocks non-secret computed clauses as part of the sandbox policy"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?label :where
                          [?e :llm.provider/name ?name]
                          [(str ?name "-suffix") ?label]])))))

(deftest safe-q-blocks-transcript-and-run-history-queries
  (let [sid         (db/create-session! :terminal)
        session-eid (ffirst (db/q '[:find ?e :in $ ?sid
                                    :where [?e :session/id ?sid]]
                                  sid))]
    (db/transact! [{:message/id         (random-uuid)
                    :message/session    session-eid
                    :message/role       :user
                    :message/content    "copied secret"
                    :message/created-at (java.util.Date.)}
                   {:schedule-run/id          (random-uuid)
                    :schedule-run/schedule-id :hist
                    :schedule-run/started-at  (java.util.Date.)
                    :schedule-run/status      :success
                    :schedule-run/result      "{\"token\":\"secret\"}"}
                   {:message/id          (random-uuid)
                    :message/session     session-eid
                    :message/role        :tool
                    :message/content     ""
                    :message/tool-result {:result {"token" "secret"}}
                    :message/tool-id     "call_1"
                    :message/created-at  (java.util.Date.)}])

    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?content :where [?m :message/content ?content]])))

    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?result :where [?m :message/tool-result ?result]])))

    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?result :where [?run :schedule-run/result ?result]])))))
