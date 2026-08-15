(ns xia.secret-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [xia.db :as db]
            [xia.secret :as secret]
            [xia.sensitive :as sensitive]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(def ^:private attribute-classification-matrix
  [{:ident :llm.provider/api-key
    :encrypted? true
    :protected-payload? true
    :query-blocked? true
    :class :credential}
   {:ident :message/content
    :encrypted? false
    :protected-payload? true
    :query-blocked? true
    :class :plaintext-user-content}
   {:ident :llm.log/response
    :encrypted? false
    :protected-payload? true
    :query-blocked? true
    :class :plaintext-diagnostic}
   {:ident :llm.provider/model
    :encrypted? false
    :protected-payload? false
    :query-blocked? false
    :class :public-operational-metadata}
   {:ident :audit.event/type
    :encrypted? false
    :protected-payload? false
    :query-blocked? false
    :class :public-operational-metadata}
   {:ident :schedule-run/status
    :encrypted? false
    :protected-payload? false
    :query-blocked? false
    :class :public-operational-metadata}
   {:ident :llm.log/prompt-tokens
    :encrypted? false
    :protected-payload? false
    :query-blocked? true
    :class :conservative-name-match}
   {:ident :message/token-estimate
    :encrypted? false
    :protected-payload? false
    :query-blocked? true
    :class :conservative-name-match}
   {:ident :site-cred/password-field
    :encrypted? false
    :protected-payload? false
    :query-blocked? true
    :class :conservative-name-match}
   {:ident :oauth.account/token-url
    :encrypted? false
    :protected-payload? false
    :query-blocked? true
    :class :conservative-name-match}
   {:ident :llm.provider/credential-source
    :encrypted? false
    :protected-payload? false
    :query-blocked? true
    :class :conservative-name-match}
   {:ident :service/oauth-account
    :encrypted? false
    :protected-payload? false
    :query-blocked? true
    :class :conservative-name-match}
   {:ident :credentialed/label
    :encrypted? true
    :protected-payload? true
    :query-blocked? true
    :class :conservative-namespace-prefix}
   {:ident :secretary/name
    :encrypted? true
    :protected-payload? true
    :query-blocked? true
    :class :conservative-namespace-prefix}])

(def ^:private config-classification-matrix
  [{:key :credential/gmail
    :secret? true
    :write-blocked? true
    :query-blocked? true
    :class :secret-namespace}
   {:key :secret/something
    :secret? true
    :write-blocked? true
    :query-blocked? true
    :class :secret-namespace}
   {:key :api-key/openai
    :secret? true
    :write-blocked? true
    :query-blocked? true
    :class :secret-namespace}
   {:key :oauth/google
    :secret? true
    :write-blocked? true
    :query-blocked? true
    :class :secret-namespace}
   {:key :token/refresh
    :secret? true
    :write-blocked? true
    :query-blocked? true
    :class :secret-namespace}
   {:key :web/search-brave-api-key
    :secret? true
    :write-blocked? true
    :query-blocked? true
    :class :explicit-secret}
   {:key :llm/log-full-payloads?
    :secret? false
    :write-blocked? true
    :query-blocked? false
    :class :readable-privacy-boundary}
   {:key :llm/log-retention-days
    :secret? false
    :write-blocked? true
    :query-blocked? false
    :class :readable-privacy-boundary}
   {:key :user/name
    :secret? false
    :write-blocked? false
    :query-blocked? false
    :class :public-config}
   {:key :context/budget
    :secret? false
    :write-blocked? false
    :query-blocked? false
    :class :public-config}
   {:key :web/search-searxng-url
    :secret? false
    :write-blocked? false
    :query-blocked? false
    :class :public-config}
   {:key :local-doc/chunk-summary-max-tokens
    :secret? false
    :write-blocked? false
    :query-blocked? true
    :class :conservative-query-name-match}
   {:key :vendor/password-policy
    :secret? false
    :write-blocked? false
    :query-blocked? true
    :class :conservative-query-name-match}
   {:key :oauth2/enabled?
    :secret? true
    :write-blocked? true
    :query-blocked? true
    :class :conservative-namespace-prefix}
   {:key :tokenizer/model
    :secret? true
    :write-blocked? true
    :query-blocked? true
    :class :conservative-namespace-prefix}
   {:key :secretary/name
    :secret? true
    :write-blocked? true
    :query-blocked? true
    :class :conservative-namespace-prefix}
   {:key :credentialed/theme
    :secret? true
    :write-blocked? true
    :query-blocked? true
    :class :conservative-namespace-prefix}])

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

(defn- call-outcome
  [f]
  (try
    {:status :allowed
     :value (f)}
    (catch clojure.lang.ExceptionInfo e
      {:status :blocked
       :type (:type (ex-data e))})))

(deftest config-classification-allow-deny-matrix
  (doseq [{:keys [key secret? write-blocked? query-blocked? class]}
          config-classification-matrix]
    (testing (str key " classified as " class)
      (is (= secret?
             (boolean (sensitive/secret-config-key? key))))
      (is (= write-blocked?
             (boolean (sensitive/sandbox-blocked-config-write-key? key))))
      (is (= query-blocked?
             (boolean (sensitive/secret-query-ident? key))))
      (let [db-calls (atom [])
            read-result
            (with-redefs [db/get-config
                          (fn [actual-key]
                            (swap! db-calls conj [:read actual-key])
                            ::config-value)]
              (call-outcome #(secret/safe-get-config key)))
            write-result
            (with-redefs [db/set-config!
                          (fn [actual-key value]
                            (swap! db-calls conj [:write actual-key value])
                            ::config-written)]
              (call-outcome #(secret/safe-set-config! key "value")))]
        (is (= (if secret? :blocked :allowed)
               (:status read-result)))
        (is (= (if write-blocked? :blocked :allowed)
               (:status write-result)))
        (is (= (cond-> []
                 (not secret?) (conj [:read key])
                 (not write-blocked?) (conj [:write key "value"]))
               @db-calls))))))

;; ---------------------------------------------------------------------------
;; safe-q tests
;; ---------------------------------------------------------------------------

(def ^:private forwarded-query ::forwarded-query)

(defn- safe-q-policy-allows?
  [query & inputs]
  (with-redefs [db/q (fn [& _] forwarded-query)]
    (try
      (= forwarded-query (apply secret/safe-q query inputs))
      (catch Throwable _
        false))))

(defn- safe-q-policy-blocks?
  [query & inputs]
  (with-redefs [db/q (fn [& _] forwarded-query)]
    (try
      (apply secret/safe-q query inputs)
      false
      (catch clojure.lang.ExceptionInfo e
        (boolean (re-find #"Access denied" (.getMessage e))))
      (catch Throwable _
        false))))

(deftest explicit-sensitive-attribute-classes-are-disjoint-and-closed
  (testing "every encrypted attribute is sandbox-protected"
    (doseq [attr (sort-by str sensitive/encrypted-attrs)]
      (is (sensitive/encrypted-attr? attr) (str attr))
      (is (sensitive/secret-attr? attr) (str attr))
      (is (sensitive/secret-query-ident? attr) (str attr))))
  (testing "every plaintext payload remains unencrypted but sandbox-protected"
    (doseq [attr (sort-by str sensitive/sandbox-only-secret-attrs)]
      (is (false? (boolean (sensitive/encrypted-attr? attr))) (str attr))
      (is (sensitive/secret-attr? attr) (str attr))
      (is (sensitive/secret-query-ident? attr) (str attr))))
  (testing "the explicit storage classes do not overlap"
    (is (empty? (filter sensitive/encrypted-attrs
                        sensitive/sandbox-only-secret-attrs)))
    (is (empty? (filter sensitive/plaintext-user-content-attrs
                        sensitive/plaintext-diagnostic-attrs)))))

(deftest attribute-classification-allow-deny-matrix
  (doseq [{:keys [ident encrypted? protected-payload? query-blocked? class]}
          attribute-classification-matrix]
    (testing (str ident " classified as " class)
      (is (= encrypted?
             (boolean (sensitive/encrypted-attr? ident))))
      (is (= protected-payload?
             (boolean (sensitive/secret-attr? ident))))
      (is (= query-blocked?
             (boolean (sensitive/secret-query-ident? ident))))
      (is (= query-blocked?
             (safe-q-policy-blocks?
              [:find '?value :where ['?entity ident '?value]])))
      (is (= (not query-blocked?)
             (safe-q-policy-allows?
              [:find '?value :where ['?entity ident '?value]]))))))

(deftest config-query-allow-deny-matrix
  (doseq [{:keys [key query-blocked? class]} config-classification-matrix]
    (testing (str key " query policy for " class)
      (let [query [:find '?entity :where ['?entity :config/key key]]]
        (is (= query-blocked?
               (safe-q-policy-blocks? query)))
        (is (= (not query-blocked?)
               (safe-q-policy-allows? query)))))))

(def ^:private short-text-gen
  (gen/fmap #(apply str %)
            (gen/vector (gen/elements (seq "abcxyz012")) 0 5)))

(defn- mixed-case
  [value uppercase-mask]
  (apply str
         (map-indexed (fn [idx ch]
                        (if (nth uppercase-mask
                                 (mod idx (count uppercase-mask)))
                          (let [code (int ch)]
                            (if (<= (int \a) code (int \z))
                              (char (- code 32))
                              ch))
                          ch))
                      value)))

(def ^:private secret-marker-gen
  (gen/elements ["api-key"
                 "api_key"
                 "apikey"
                 "password"
                 "passwd"
                 "client-secret"
                 "credential"
                 "access-token"
                 "oauth"
                 "private-key"
                 "private_key"]))

(def ^:private secret-marker-attr-gen
  (gen/fmap
   (fn [[marker uppercase-mask prefix suffix attr-ns]]
     (keyword attr-ns
              (str prefix (mixed-case marker uppercase-mask) suffix)))
   (gen/tuple secret-marker-gen
              (gen/vector gen/boolean 32)
              short-text-gen
              short-text-gen
              (gen/elements ["public" "service" "integration" "vendor"]))))

(def ^:private secret-namespace-attr-gen
  (gen/fmap (fn [[attr-ns suffix attr-name]]
              (keyword (str attr-ns suffix)
                       (str "value" attr-name)))
            (gen/tuple (gen/elements ["credential" "secret"])
                       short-text-gen
                       short-text-gen)))

(def ^:private explicit-secret-attr-gen
  (gen/elements
   (vec (concat sensitive/encrypted-attrs
                sensitive/sandbox-only-secret-attrs))))

(def ^:private secret-ident-gen
  (gen/frequency [[6 secret-marker-attr-gen]
                  [2 secret-namespace-attr-gen]
                  [2 explicit-secret-attr-gen]]))

(def ^:private safe-attr-gen
  (gen/fmap (fn [n]
              (keyword (str "public" n) (str "field" n)))
            gen/nat))

(def ^:private secret-query-case-gen
  (gen/let [attr  secret-ident-gen
            shape (gen/elements [:attribute
                                 :find
                                 :vector-find
                                 :or
                                 :not
                                 :or-join
                                 :lookup-entity
                                 :lookup-value
                                 :config-key
                                 :duplicate-where])]
    {:attr attr
     :query
     (case shape
       :attribute
       [:find '?v :where ['?e attr '?v]]

       :find
       [:find (list 'count attr) :where ['?e :public/value '?v]]

       :vector-find
       [:find ['count attr] :where ['?e :public/value '?v]]

       :or
       [:find '?v :where
        (list 'or
              ['?e :public/value '?v]
              ['?e attr '?v])]

       :not
       [:find '?v :where
        ['?e :public/value '?v]
        (list 'not ['?e attr '?secret-value])]

       :or-join
       [:find '?v :where
        (list 'or-join
              ['?e '?v]
              ['?e :public/value '?v]
              ['?e attr '?v])]

       :lookup-entity
       [:find '?v :where [[attr "guess"] :public/value '?v]]

       :lookup-value
       [:find '?e :where ['?e :public/ref [attr "guess"]]]

       :config-key
       [:find '?e :where ['?e :config/key attr]]

       :duplicate-where
       [:find '?v
        :where ['?e attr '?v]
        :where ['?e :public/value '?v]])}))

(def ^:private safe-query-gen
  (gen/let [attr  safe-attr-gen
            shape (gen/elements [:attribute
                                 :or
                                 :not
                                 :or-join
                                 :lookup-entity
                                 :lookup-value
                                 :config-key])]
    (case shape
      :attribute
      [:find '?v :where ['?e attr '?v]]

      :or
      [:find '?v :where
       (list 'or
             ['?e attr '?v]
             ['?e :public/fallback '?v])]

      :not
      [:find '?v :where
       ['?e attr '?v]
       (list 'not ['?e :public/disabled? true])]

      :or-join
      [:find '?v :where
       (list 'or-join
             ['?e '?v]
             ['?e attr '?v]
             ['?e :public/fallback '?v])]

      :lookup-entity
      [:find '?v :where [[attr "known"] :public/value '?v]]

      :lookup-value
      [:find '?e :where ['?e :public/ref [attr "known"]]]

      :config-key
      [:find '?e :where ['?e :config/key :user/name]])))

(def ^:private allowed-aggregate-query-gen
  (gen/let [attr safe-attr-gen
            op   (gen/elements '[avg count count-distinct distinct max median
                                 min rand sample stddev sum variance vec])
            vector-form? gen/boolean]
    [:find ((if vector-form? vec #(apply list %))
            (if (#{'rand 'sample} op)
              [op 1 '?v]
              [op '?v]))
     :where ['?e attr '?v]]))

(def ^:private host-aggregate-query-gen
  (gen/fmap (fn [[op vector-form?]]
              [:find ((if vector-form? vec #(apply list %)) [op '?v])
               :where ['?e :public/value '?v]])
            (gen/tuple
             (gen/elements '[aggregate
                             clojure.core/count
                             clojure.core/eval
                             clojure.core/slurp
                             clojure.core/spit
                             clojure.java.shell/sh
                             datalevin.core/entity
                             pull
                             pull-many
                             user/aggregate])
             gen/boolean)))

(def ^:private secret-rule-case-gen
  (gen/let [attr      secret-ident-gen
            rule-name (gen/fmap #(symbol (str "generated-rule-" %)) gen/nat)]
    {:query [:find '?v :in '$ '% :where
             (list rule-name '?e '?v)]
     :rules [[(list rule-name '?e '?v)
              ['?e attr '?v]]]}))

(defspec safe-q-rejects-generated-secret-references 500
  (prop/for-all [{:keys [attr query]} secret-query-case-gen]
                (and (sensitive/secret-query-ident? attr)
                     (safe-q-policy-blocks? query))))

(defspec safe-q-allows-generated-public-data-patterns 300
  (prop/for-all [query safe-query-gen]
                (safe-q-policy-allows? query)))

(defspec safe-q-rejects-generated-rule-input-bypasses 250
  (prop/for-all [{:keys [query rules]} secret-rule-case-gen]
                (safe-q-policy-blocks? query rules)))

(defspec safe-q-rejects-generated-host-aggregate-resolution 200
  (prop/for-all [query host-aggregate-query-gen]
                (safe-q-policy-blocks? query)))

(defspec safe-q-allows-only-reviewed-built-in-aggregates 250
  (prop/for-all [query allowed-aggregate-query-gen]
                (safe-q-policy-allows? query)))

(deftest safe-q-blocks-rule-and-aggregate-code-execution
  (db/transact! [{:llm.provider/id       :safe-q-execution
                  :llm.provider/name     "safe-q-execution"
                  :llm.provider/base-url "http://localhost"
                  :llm.provider/api-key  "sk-safe-q-secret"
                  :llm.provider/model    "test-model"
                  :llm.provider/default? false}])

  (testing "rules supplied through % cannot hide secret data patterns"
    (let [rules '[[(leak-provider-key ?e ?value)
                   [?e :llm.provider/api-key ?value]]]]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Access denied"
           (secret/safe-q '[:find ?value :in $ % :where
                            (leak-provider-key ?e ?value)]
                          rules)))))

  (testing "custom aggregate functions never reach the Datalevin executor"
    (let [invoked?  (atom false)
          aggregate (fn [values]
                      (reset! invoked? true)
                      (count values))]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Access denied"
           (secret/safe-q '[:find (aggregate ?aggregate ?name)
                            :in $ ?aggregate
                            :where [?e :llm.provider/name ?name]]
                          aggregate)))
      (is (false? @invoked?))))

  (testing "reviewed built-in aggregates remain available"
    (is (= [[1]]
           (secret/safe-q '[:find (count ?name) :where
                            [?e :llm.provider/name ?name]])))))

(deftest safe-q-blocks-duplicate-sections-and-secret-lookup-refs
  (testing "a later duplicate section cannot erase an unsafe earlier section"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?value
                          :where [?e :llm.provider/api-key ?value]
                          :where [?e :llm.provider/name ?name]]))))

  (testing "lookup refs cannot use a secret attribute as an existence oracle"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?name :where
                          [[:llm.provider/api-key "guessed-key"]
                           :llm.provider/name
                           ?name]]))))

  (testing "secret-like config literals are rejected conservatively"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q '[:find ?e :where
                          [?e :config/key :vendor/password]]))))

  (testing "vector-shaped computed clauses are rejected like list calls"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Access denied"
         (secret/safe-q
          [:find '?value :where
           [(vector 'clojure.core/slurp "/tmp/sci-escape") '?value]])))))

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
