(ns xia.service-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.secret :as secret]
            [xia.service :as service]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.util.concurrent ConcurrentHashMap CountDownLatch]
           [java.util.concurrent.atomic AtomicLong]))

(use-fixtures :each with-test-db)

;; ---------------------------------------------------------------------------
;; URL safety
;; ---------------------------------------------------------------------------

(deftest safe-resolve-url-rejects-absolute-urls
  (testing "rejects http:// URLs"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Absolute URLs not allowed"
          (#'service/safe-resolve-url "https://api.example.com" "http://evil.com/exfil"))))
  (testing "rejects https:// URLs"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Absolute URLs not allowed"
          (#'service/safe-resolve-url "https://api.example.com" "https://evil.com/exfil"))))
  (testing "rejects protocol-relative URLs"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Absolute URLs not allowed"
          (#'service/safe-resolve-url "https://api.example.com" "//evil.com/exfil")))))

(deftest safe-resolve-url-rejects-path-traversal
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"Path traversal not allowed"
        (#'service/safe-resolve-url "https://api.example.com" "/../../../etc/passwd"))))

(deftest safe-resolve-url-resolves-correctly
  (testing "path with leading slash"
    (is (= "https://api.example.com/v1/messages"
           (#'service/safe-resolve-url "https://api.example.com" "/v1/messages"))))
  (testing "path without leading slash"
    (is (= "https://api.example.com/v1/messages"
           (#'service/safe-resolve-url "https://api.example.com" "v1/messages"))))
  (testing "base URL with trailing slash"
    (is (= "https://api.example.com/v1/messages"
           (#'service/safe-resolve-url "https://api.example.com/" "/v1/messages")))))

;; ---------------------------------------------------------------------------
;; Auth injection
;; ---------------------------------------------------------------------------

(deftest inject-auth-bearer
  (let [req  {:uri "https://api.example.com/test" :method :get}
        svc  {:auth-type :bearer :auth-key "my-token"}
        result (#'service/inject-auth req svc)]
    (is (= "Bearer my-token" (get-in result [:headers "Authorization"])))))

(deftest inject-auth-basic
  (let [req  {:uri "https://api.example.com/test" :method :get}
        svc  {:auth-type :basic :auth-key "user:pass"}
        result (#'service/inject-auth req svc)]
    (is (= "Basic dXNlcjpwYXNz" (get-in result [:headers "Authorization"])))))

(deftest inject-auth-api-key-header
  (let [req  {:uri "https://api.example.com/test" :method :get}
        svc  {:auth-type :api-key-header :auth-key "abc123" :auth-header "X-API-Key"}
        result (#'service/inject-auth req svc)]
    (is (= "abc123" (get-in result [:headers "X-API-Key"])))))

(deftest inject-auth-query-param
  (let [req  {:uri "https://api.example.com/test" :method :get}
        svc  {:auth-type :query-param :auth-key "abc123" :auth-header "api_key"}
        result (#'service/inject-auth req svc)]
    (is (= "abc123" (get-in result [:query-params "api_key"])))))

(deftest inject-auth-query-param-normalizes-equivalent-keys
  (let [req    {:uri "https://api.example.com/test"
                :method :get
                :query-params {:api_key "evil"
                               "page" 1}}
        svc    {:auth-type :query-param :auth-key "abc123" :auth-header "api_key"}
        result (#'service/inject-auth req svc)]
    (is (= {"api_key" "abc123"
            "page" 1}
           (:query-params result)))))

(deftest inject-auth-oauth-account
  (let [req    {:uri "https://api.example.com/test" :method :get}
        svc    {:auth-type :oauth-account
                :oauth-account {:oauth.account/access-token "oauth-token"
                                :oauth.account/token-type "Bearer"}}
        result (#'service/inject-auth req svc)]
    (is (= "Bearer oauth-token" (get-in result [:headers "Authorization"])))))

(deftest inject-auth-api-key-header-requires-auth-header
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"auth-header required"
        (#'service/inject-auth {} {:auth-type :api-key-header :auth-key "x"}))))

(deftest inject-auth-query-param-requires-auth-header
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"auth-header required"
        (#'service/inject-auth {} {:auth-type :query-param :auth-key "x"}))))

;; ---------------------------------------------------------------------------
;; Tool headers cannot override auth
;; ---------------------------------------------------------------------------

(deftest tool-headers-cannot-override-auth
  ;; The proxy merges tool headers UNDER auth headers (auth takes precedence)
  (db/register-service! {:id       :test-svc
                         :base-url "https://api.example.com"
                         :auth-type :bearer
                         :auth-key "real-token"})
  ;; We can't easily test the full request flow without an HTTP server,
  ;; but we can test the merge order via inject-auth + merge logic.
  ;; The service.clj code does: (merge headers auth-injected-headers)
  ;; which means auth headers overwrite tool-supplied ones.
  (let [req    {:uri "https://api.example.com/test" :method :get
                :headers {"Authorization" "Bearer EVIL-TOKEN"}}
        svc    {:auth-type :bearer :auth-key "real-token"}
        result (#'service/inject-auth req svc)]
    (is (= "Bearer real-token" (get-in result [:headers "Authorization"])))))

;; ---------------------------------------------------------------------------
;; list-services never exposes credentials
;; ---------------------------------------------------------------------------

(deftest list-services-hides-credentials
  (db/register-service! {:id       :gmail
                         :name     "Gmail"
                         :base-url "https://gmail.googleapis.com"
                         :auth-type :bearer
                         :auth-key "ya29.super-secret-oauth-token"})
  (db/register-service! {:id       :github
                         :name     "GitHub"
                         :base-url "https://api.github.com"
                         :auth-type :bearer
                         :auth-key "ghp_secret_token"})
  (let [services (service/list-services)]
    (is (= 2 (count services)))
    (doseq [svc services]
      (is (contains? svc :id))
      (is (contains? svc :name))
      (is (contains? svc :base-url))
      (is (contains? svc :rate-limit-per-minute))
      (is (not (contains? svc :auth-key)))
      (is (not (contains? svc :auth-type))))))

(deftest list-services-filters-unapproved-services-during-autonomous-runs
  (db/register-service! {:id                   :gmail
                         :name                 "Gmail"
                         :base-url             "https://gmail.googleapis.com"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :autonomous-approved? false})
  (db/register-service! {:id                   :github
                         :name                 "GitHub"
                         :base-url             "https://api.github.com"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :autonomous-approved? true})
  (binding [prompt/*interaction-context* {:channel          :scheduler
                                          :autonomous-run?  true
                                          :approval-bypass? true}]
    (is (= [:github]
           (mapv :id (service/list-services))))))

;; ---------------------------------------------------------------------------
;; Service resolution
;; ---------------------------------------------------------------------------

(deftest resolve-service-unknown
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"Unknown service"
        (#'service/resolve-service :nonexistent))))

(deftest resolve-service-disabled
  (db/register-service! {:id :disabled-svc :base-url "https://example.com"
                         :auth-type :bearer :auth-key "tok"})
  (db/enable-service! :disabled-svc false)
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"disabled"
        (#'service/resolve-service :disabled-svc))))

(deftest resolve-service-oauth-account-required
  (db/register-service! {:id :oauth-svc
                         :base-url "https://example.com"
                         :auth-type :oauth-account})
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"missing an OAuth account"
        (#'service/resolve-service :oauth-svc))))

(deftest resolve-service-applies-default-rate-limit
  (db/register-service! {:id :default-rate-limit
                         :base-url "https://example.com"
                         :auth-type :bearer
                         :auth-key "tok"})
  (is (= service/default-rate-limit-per-minute
         (:rate-limit-per-minute (#'service/resolve-service :default-rate-limit)))))

(deftest request-enforces-configured-rate-limit
  (db/register-service! {:id                    :limited
                         :base-url              "https://api.example.com"
                         :auth-type             :bearer
                         :auth-key              "tok"
                         :rate-limit-per-minute 2})
  (let [request-count (atom 0)
        now-values    (atom [0 1000 2000])]
    (with-redefs [xia.service/service-rate-limits (ConcurrentHashMap.)
                  xia.service/current-time-ms     (fn []
                                                    (let [value (first @now-values)]
                                                      (swap! now-values rest)
                                                      value))
                  xia.http-client/request         (fn [_req]
                                                    (swap! request-count inc)
                                                    {:status 200
                                                     :headers {"content-type" "application/json"}
                                                     :body "{}"})]
      (is (= 200 (:status (service/request :limited :get "/one"))))
      (is (= 200 (:status (service/request :limited :get "/two"))))
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Rate limit exceeded for service limited"
            (service/request :limited :get "/three")))
      (is (= 2 @request-count)))))

(deftest check-rate-limit-overflow-does-not-mutate-state
  (let [now   60000
        state (atom {:timestamps [59001 59002]
                     :cleaned    now})
        limits (doto (ConcurrentHashMap.)
                 (.put :limited state))]
    (with-redefs [xia.service/service-rate-limits limits
                  xia.service/current-time-ms     (constantly now)]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Rate limit exceeded for service limited"
            (#'service/check-rate-limit! :limited {:rate-limit-per-minute 2})))
      (is (= {:timestamps [59001 59002]
              :cleaned    now}
             @state)))))

(deftest request-blocks-private-base-url-by-default
  (db/register-service! {:id        :private-svc
                         :base-url  "http://127.0.0.1:8080"
                         :auth-type :bearer
                         :auth-key  "tok"})
  (with-redefs [xia.http-client/send-request! (fn [_]
                                                (throw (ex-info "should not send" {})))]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"private/internal network"
          (service/request :private-svc :get "/status")))))

(deftest request-allows-private-base-url-when-explicitly-configured
  (db/register-service! {:id                     :private-svc
                         :base-url               "http://127.0.0.1:8080"
                         :auth-type              :bearer
                         :auth-key               "tok"
                         :allow-private-network? true})
  (with-redefs [xia.http-client/send-request! (fn [_]
                                                {:status 200
                                                 :headers {"content-type" "application/json"}
                                                 :body "{}"})]
    (is (= 200
           (:status (service/request :private-svc :get "/status"))))))

(deftest check-rate-limit-remains-bounded-under-concurrency
  (let [now    60000
        state  (atom {:timestamps []
                      :cleaned    now})
        limits (doto (ConcurrentHashMap.)
                 (.put :limited state))
        start  (CountDownLatch. 1)]
    (with-redefs [xia.service/service-rate-limits limits
                  xia.service/current-time-ms     (constantly now)]
      (let [results (doall
                      (for [_ (range 8)]
                        (future
                          (.await start)
                          (try
                            (#'service/check-rate-limit! :limited {:rate-limit-per-minute 2})
                            :ok
                            (catch clojure.lang.ExceptionInfo _
                              :limited)))))]
        (.countDown start)
        (let [outcomes (mapv deref results)]
          (is (= 2 (count (filter #{:ok} outcomes))))
          (is (= 6 (count (filter #{:limited} outcomes))))
          (is (= 2 (count (:timestamps @state)))))))))

(deftest stale-service-rate-limit-entries-are-evicted
  (let [now         120000
        stale-id    :stale
        active-id   :active
        stale-state (atom {:timestamps [1000 2000]
                           :cleaned    1000})
        limits      (doto (ConcurrentHashMap.)
                      (.put stale-id stale-state))]
    (with-redefs [xia.service/service-rate-limits limits
                  xia.service/service-rate-limit-cleanup (AtomicLong. 0)
                  xia.service/current-time-ms (constantly now)]
      (#'service/check-rate-limit! active-id {:rate-limit-per-minute 2})
      (is (nil? (.get limits stale-id)))
      (is (some? (.get limits active-id))))))

(deftest request-query-param-auth-overrides-tool-param-by-normalized-name
  (db/register-service! {:id          :query-auth-svc
                         :base-url    "https://api.example.com"
                         :auth-type   :query-param
                         :auth-key    "real-key"
                         :auth-header "api_key"})
  (with-redefs [xia.http-client/request
                (fn [req]
                  (is (= {"api_key" "real-key"
                          "page" 1}
                         (:query-params req)))
                  {:status 200
                   :headers {"content-type" "application/json"}
                   :body "{}"})]
    (is (= 200
           (:status (service/request :query-auth-svc
                                     :get
                                     "/items"
                                     :query-params {:api_key "evil-key"
                                                    :page 1}))))))

(deftest request-tool-headers-cannot-override-auth-case-insensitively
  (db/register-service! {:id        :header-auth-svc
                         :base-url  "https://api.example.com"
                         :auth-type :bearer
                         :auth-key  "real-token"})
  (with-redefs [xia.http-client/request
                (fn [req]
                  (is (= {"Authorization" "Bearer real-token"
                          "X-Trace-Id" "trace-123"}
                         (:headers req)))
                  {:status 200
                   :headers {"content-type" "application/json"}
                   :body "{}"})]
    (is (= 200
           (:status (service/request :header-auth-svc
                                     :get
                                     "/items"
                                     :headers {"authorization" "Bearer evil-token"
                                               "X-Trace-Id" "trace-123"}))))))

(deftest request-blocks-unapproved-service-during-autonomous-runs
  (db/register-service! {:id                   :gmail
                         :base-url             "https://gmail.googleapis.com"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :autonomous-approved? false})
  (binding [prompt/*interaction-context* {:channel          :scheduler
                                          :autonomous-run?  true
                                          :approval-bypass? true
                                          :audit-log        (atom [])}]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"not approved for autonomous execution"
          (service/request :gmail :get "/messages")))))

(deftest request-allows-implicitly-approved-service-during-autonomous-runs
  (db/register-service! {:id        :github
                         :base-url  "https://api.github.com"
                         :auth-type :bearer
                         :auth-key  "tok"})
  (binding [prompt/*interaction-context* {:channel          :scheduler
                                          :autonomous-run?  true
                                          :approval-bypass? true
                                          :audit-log        (atom [])}]
    (with-redefs [xia.http-client/request (fn [_req]
                                            {:status 200
                                             :headers {"content-type" "application/json"}
                                             :body "{\"ok\":true}"})]
      (is (= 200 (:status (service/request :github :get "/user")))))))

(deftest request-blocks-oauth-service-when-account-is-unapproved
  (db/register-oauth-account! {:id                    :github-oauth
                               :name                  "GitHub OAuth"
                               :authorize-url         "https://github.com/login/oauth/authorize"
                               :token-url             "https://github.com/login/oauth/access_token"
                               :client-id             "client-id"
                               :client-secret         "client-secret"
                               :autonomous-approved?  false
                               :access-token          "oauth-token"
                               :token-type            "Bearer"})
  (db/register-service! {:id                   :github
                         :base-url             "https://api.github.com"
                         :auth-type            :oauth-account
                         :oauth-account        :github-oauth
                         :autonomous-approved? true})
  (binding [prompt/*interaction-context* {:channel          :scheduler
                                          :autonomous-run?  true
                                          :approval-bypass? true
                                          :audit-log        (atom [])}]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"OAuth account github-oauth is not approved"
          (service/request :github :get "/user")))))

;; ---------------------------------------------------------------------------
;; Secret module protects service/auth-key
;; ---------------------------------------------------------------------------

(deftest secret-blocks-service-auth-key-query
  (db/register-service! {:id :secret-test :base-url "https://example.com"
                         :auth-type :bearer :auth-key "top-secret"})
  (testing "safe-q blocks queries on :service/auth-key"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Access denied"
          (secret/safe-q '[:find ?v :where [?e :service/auth-key ?v]])))))

(deftest request-refreshes-expiring-oauth-account
  (db/register-oauth-account! {:id            :github-oauth
                               :name          "GitHub OAuth"
                               :authorize-url "https://github.com/login/oauth/authorize"
                               :token-url     "https://github.com/login/oauth/access_token"
                               :client-id     "client-id"
                               :client-secret "client-secret"
                               :access-token  "old-access"
                               :refresh-token "old-refresh"
                               :token-type    "Bearer"
                               :expires-at    (java.util.Date. 0)})
  (db/register-service! {:id            :github
                         :name          "GitHub"
                         :base-url      "https://api.github.com"
                         :auth-type     :oauth-account
                         :oauth-account :github-oauth})
  (let [calls (atom [])]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (let [url (or (:url req) (:uri req))]
                      (swap! calls conj (assoc (select-keys req [:method :headers :body]) :url url))
                      (case url
                        "https://github.com/login/oauth/access_token"
                        {:status 200
                         :body "{\"access_token\":\"new-access\",\"refresh_token\":\"new-refresh\",\"token_type\":\"Bearer\",\"expires_in\":3600}"}

                        "https://api.github.com/user"
                        (do
                          (is (= "Bearer new-access" (get-in req [:headers "Authorization"])))
                          {:status 200
                           :headers {"content-type" "application/json"}
                           :body "{\"login\":\"hyang\"}"})

                        (throw (ex-info "unexpected request" {:url url})))))]
      (let [response (service/request :github :get "/user")
            account  (db/get-oauth-account :github-oauth)]
        (is (= 200 (:status response)))
        (is (= "hyang" (get-in response [:body "login"])))
        (is (= "new-access" (:oauth.account/access-token account)))
        (is (= "new-refresh" (:oauth.account/refresh-token account)))
        (is (= 2 (count @calls)))))))
