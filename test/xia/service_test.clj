(ns xia.service-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.secret :as secret]
            [xia.service :as service]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.util.concurrent ConcurrentHashMap]))

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
