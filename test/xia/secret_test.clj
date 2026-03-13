(ns xia.secret-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.secret :as secret]
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
                          (secret/safe-set-config! :secret/stuff "nope")))))

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
