(ns xia.channel.http-test
  (:require [charred.api :as json]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.channel.http :as http]
            [xia.channel.http.auth :as http-auth]
            [xia.db :as db]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(def ^:private tenant-origin "https://t-test.tenants.example.com")
(def ^:private tenant-id "tenant-test")
(def ^:private runtime-id "runtime-test")

(defn- temp-secret-file
  [payload]
  (let [file (.toFile
              (java.nio.file.Files/createTempFile
               "xia-managed-proxy-secret"
               ".txt"
               (make-array java.nio.file.attribute.FileAttribute 0)))]
    (.deleteOnExit file)
    (spit file payload)
    (str file)))

(defn- overlay
  [secret-file]
  {:overlay/schema-version 1
   :snapshot/id "managed-proxy-test"
   :tenant/id tenant-id
   :runtime/id runtime-id
   :generated-at "2026-04-04T10:15:00Z"
   :config-overrides {:http/managed-proxy-enabled? true
                      :http/managed-proxy-secret-file secret-file
                      :http/managed-tenant-origin tenant-origin}
   :bounded-config {}
   :tx-data []
   :forced-keys #{}})

(defn- response-body
  [response]
  (json/read-json (:body response)))

(defn- protected-response
  [req]
  (#'http/protected-route-response
   req
   (fn []
     (#'http/json-response 200 {:ok true}))))

(defn- base-tenant-req
  []
  {:request-method :post
   :uri "/chat"
   :query-string ""
   :remote-addr "10.0.0.10"
   :headers {"origin" tenant-origin
             "x-forwarded-proto" "https"
             "x-forwarded-host" "t-test.tenants.example.com"}})

(defn- sign-managed-req
  [req secret & {:keys [request-id user-id timestamp]
                 :or {user-id "user-test"}}]
  (let [request-id* (or request-id (str (random-uuid)))
        timestamp*  (or timestamp (str (System/currentTimeMillis)))
        req*        (assoc req :headers
                           (merge (:headers req)
                                  {"x-xia-proxy-mode" "tenant"
                                   "x-xia-tenant-id" tenant-id
                                   "x-xia-runtime-id" runtime-id
                                   "x-xia-user-id" user-id
                                   "x-xia-request-id" request-id*
                                   "x-xia-proxy-timestamp" timestamp*}))
        payload     (#'http/managed-proxy-signing-payload
                     req*
                     timestamp*
                     request-id*
                     tenant-id
                     runtime-id
                     user-id)
        signature   (#'http/hmac-sha256-base64url secret payload)]
    (assoc-in req* [:headers "x-xia-proxy-signature"] (str "v1:" signature))))

(deftest protected-route-keeps-local-ui-cookie-origin-auth
  (let [req      {:request-method :get
                  :uri "/sessions"
                  :query-string ""
                  :remote-addr "127.0.0.1"
                  :headers {"origin" "http://localhost:3008"
                            "cookie" (str "xia-local-session=" (#'http/session-secret))}}
        response (protected-response req)]
    (is (= 200 (:status response)))
    (is (= true (get (response-body response) "ok")))))

(deftest tenant-origin-without-managed-proof-is-rejected
  (let [secret-file (temp-secret-file "proxy-secret\n")]
    (runtime-overlay/activate! (overlay secret-file))
    (let [response (protected-response (base-tenant-req))]
      (is (= 403 (:status response)))
      (is (= "forbidden origin" (get (response-body response) "error"))))))

(deftest local-session-bootstrap-stays-local-only
  (let [response (#'http/handle-local-session-bootstrap (base-tenant-req))]
    (is (= 403 (:status response)))
    (is (= "forbidden origin" (get (response-body response) "error")))
    (is (nil? (get-in response [:headers "Set-Cookie"])))))

(deftest signed-managed-proxy-request-does-not-need-local-cookie
  (let [secret      "proxy-secret"
        secret-file (temp-secret-file (str secret "\n"))]
    (runtime-overlay/activate! (overlay secret-file))
    (let [response (protected-response (sign-managed-req (base-tenant-req) secret))]
      (is (= 200 (:status response)))
      (is (= true (get (response-body response) "ok"))))))

(deftest spoofed-managed-proxy-headers-are-rejected
  (let [secret      "proxy-secret"
        secret-file (temp-secret-file (str secret "\n"))
        req         (assoc-in (sign-managed-req (base-tenant-req) secret)
                              [:headers "x-xia-proxy-signature"]
                              "v1:not-valid")]
    (runtime-overlay/activate! (overlay secret-file))
    (let [response (protected-response req)]
      (is (= 403 (:status response)))
      (is (= "forbidden origin" (get (response-body response) "error"))))))

(deftest managed-proxy-origin-must-match-overlay-origin
  (let [secret      "proxy-secret"
        secret-file (temp-secret-file (str secret "\n"))
        req         (sign-managed-req
                     (assoc-in (base-tenant-req)
                               [:headers "origin"]
                               "https://other.tenants.example.com")
                     secret)]
    (runtime-overlay/activate! (overlay secret-file))
    (let [response (protected-response req)]
      (is (= 403 (:status response)))
      (is (= "forbidden origin" (get (response-body response) "error"))))))

(deftest managed-proxy-request-ids-cannot-be-replayed
  (let [secret      "proxy-secret"
        secret-file (temp-secret-file (str secret "\n"))
        req         (sign-managed-req (base-tenant-req) secret
                                      :request-id "request-1")]
    (runtime-overlay/activate! (overlay secret-file))
    (is (= 200 (:status (protected-response req))))
    (let [response (protected-response req)]
      (is (= 403 (:status response)))
      (is (= "forbidden origin" (get (response-body response) "error"))))))

;; ---------------------------------------------------------------------------
;; Phase 0.1: loopback / remote / forwarded-header matrix
;; ---------------------------------------------------------------------------

(defn- local-bootstrap-response
  [remote-addr headers]
  (#'http/handle-local-session-bootstrap
   {:request-method :get
    :uri "/local-session"
    :query-string ""
    :remote-addr remote-addr
    :headers headers}))

(defn- local-cookie-req
  [remote-addr extra-headers]
  {:request-method :get
   :uri "/sessions"
   :query-string ""
   :remote-addr remote-addr
   :headers (merge {"origin" "http://localhost:3008"
                    "cookie" (str "xia-local-session=" (#'http/session-secret))}
                   extra-headers)})

(defn- without-command-token-env
  [f]
  (with-redefs-fn {#'xia.channel.http.auth/env-value (constantly nil)} f))

(deftest home-route-does-not-issue-local-session-cookie
  (doseq [remote-addr ["127.0.0.1" "203.0.113.5"]]
    (let [response (#'http/handle-home {:request-method :get
                                        :uri "/"
                                        :remote-addr remote-addr
                                        :headers {}})]
      (is (= 200 (:status response)))
      (is (nil? (get-in response [:headers "Set-Cookie"]))
          (str "home route leaked a local-session cookie to " remote-addr)))))

(deftest local-session-bootstrap-requires-loopback-remote-addr
  (let [response (local-bootstrap-response "10.0.0.10" {"origin" "http://localhost:3008"})]
    (is (= 403 (:status response)))
    (is (= "forbidden origin" (get (response-body response) "error")))
    (is (nil? (get-in response [:headers "Set-Cookie"]))))
  (let [response (local-bootstrap-response "192.168.1.20" {"origin" "http://127.0.0.1:3008"})]
    (is (= 403 (:status response))))
  (let [response (local-bootstrap-response "203.0.113.5" {})]
    (is (= 403 (:status response)))
    (is (nil? (get-in response [:headers "Set-Cookie"])))))

(deftest local-session-bootstrap-allows-loopback-ipv4-range
  (doseq [addr ["127.0.0.1" "127.0.0.2" "127.1.2.3" "127.255.255.255"]]
    (let [response (local-bootstrap-response addr {"origin" "http://localhost:3008"})]
      (is (= 200 (:status response)) (str "loopback addr should be allowed: " addr))
      (is (= true (get (response-body response) "ok")))
      (is (some? (get-in response [:headers "Set-Cookie"])) (str addr)))))

(deftest local-session-bootstrap-rejects-hostnames-and-malformed-addresses
  (doseq [addr [nil "" "localhost" "127.example.invalid" "127.0.0.999"]]
    (let [response (local-bootstrap-response addr {"origin" "http://localhost:3008"})]
      (is (= 403 (:status response))
          (str "non-numeric or malformed peer address must be rejected: " (pr-str addr)))
      (is (nil? (get-in response [:headers "Set-Cookie"]))))))

(deftest local-session-bootstrap-allows-loopback-ipv6
  (doseq [addr ["::1" "0:0:0:0:0:0:0:1" "127.0.0.1"]]
    (let [response (local-bootstrap-response addr {"origin" "http://[::1]:3008"})]
      (is (= 200 (:status response)) (str "ipv6 loopback should be allowed: " addr))))
  (doseq [addr ["::1" "0:0:0:0:0:0:0:1"]]
    (let [response (local-bootstrap-response addr {})]
      (is (= 200 (:status response)) (str "loopback without origin should be allowed: " addr)))))

(deftest local-cookie-auth-requires-loopback-remote-addr
  (let [resp (protected-response (local-cookie-req "10.0.0.10" {}))]
    (is (not= 200 (:status resp)) "remote addr with valid cookie must not be authorized")
    (is (contains? #{401 403} (:status resp))))
  (let [resp (protected-response (assoc (local-cookie-req "10.0.0.10" {})
                                        :headers {"cookie" (str "xia-local-session=" (#'http/session-secret))}))]
    (is (not= 200 (:status resp)) "remote without origin header must still be rejected"))
  (let [resp (protected-response (local-cookie-req "192.168.1.5" {"origin" "http://localhost:3008"}))]
    (is (not= 200 (:status resp)))))

(deftest local-cookie-auth-ignores-spoofed-forwarded-headers
  (let [base (local-cookie-req "10.0.0.10" {"x-forwarded-for" "127.0.0.1"
                                           "x-real-ip" "127.0.0.1"
                                           "x-forwarded-host" "localhost"
                                           "x-forwarded-proto" "http"})]
    (let [resp (protected-response base)]
      (is (not= 200 (:status resp)) "spoofed X-Forwarded-For must not bypass loopback check")))
  (let [base (local-cookie-req "10.0.0.10" {"x-forwarded-for" "127.0.0.1, 10.0.0.10"
                                           "origin" "http://localhost:3008"})]
    (let [resp (protected-response base)]
      (is (not= 200 (:status resp)) "spoofed forwarded list must not bypass"))))

(deftest local-cookie-auth-allows-loopback-with-valid-cookie
  (doseq [addr ["127.0.0.1" "127.0.0.2" "::1" "0:0:0:0:0:0:0:1"]]
    (let [resp (protected-response (local-cookie-req addr {}))]
      (is (= 200 (:status resp)) (str "loopback cookie auth should pass for " addr)))))

(deftest managed-proxy-still-works-with-remote-addr
  (let [secret      "proxy-secret"
        secret-file (temp-secret-file (str secret "\n"))]
    (runtime-overlay/activate! (overlay secret-file))
    (let [req  (sign-managed-req (base-tenant-req) secret)
          resp (protected-response req)]
      (is (= 200 (:status resp)) "managed proxy must be allowed even though remote-addr is non-loopback"))
    (let [req  (assoc (sign-managed-req (base-tenant-req) secret) :remote-addr "192.168.1.10")
          resp (protected-response req)]
      (is (= 200 (:status resp)) "managed proxy with different remote addr still allowed"))))

(deftest managed-proxy-mode-disables-local-cookie-auth
  (let [secret      "proxy-secret"
        secret-file (temp-secret-file secret)]
    (runtime-overlay/activate! (overlay secret-file))
    (is (= 403 (:status (local-bootstrap-response
                         "127.0.0.1"
                         {"origin" "http://localhost:3008"}))))
    (is (= 401 (:status (protected-response
                         (local-cookie-req "127.0.0.1" {})))))
    (let [req  (assoc (base-tenant-req) :remote-addr "127.0.0.1")
          resp (protected-response (sign-managed-req req secret))]
      (is (= 200 (:status resp))
          "signed managed-proxy requests remain valid through a loopback proxy"))))

(deftest bind-validation-refuses-wildcard-without-managed-proxy
  (without-command-token-env
    #(do
       (is (thrown? clojure.lang.ExceptionInfo
                    (http-auth/validate-bind-host! "0.0.0.0")))
       (is (thrown? clojure.lang.ExceptionInfo
                    (http-auth/validate-bind-host! "::")))
       (is (thrown? clojure.lang.ExceptionInfo
                    (http-auth/validate-bind-host! "[::]")))
       (is (thrown? clojure.lang.ExceptionInfo
                    (http-auth/validate-bind-host! nil)))
       (is (thrown? clojure.lang.ExceptionInfo
                    (http-auth/validate-bind-host! "")))
       (is (thrown? clojure.lang.ExceptionInfo
                    (http-auth/validate-bind-host! "127.0.0.999")))
       (is (= "127.0.0.1" (http-auth/validate-bind-host! "127.0.0.1")))
       (is (= "::1" (http-auth/validate-bind-host! "::1")))
       (is (false? (http-auth/non-loopback-bind? "localhost")))
       (is (.isLoopbackAddress
            (java.net.InetAddress/getByName
             (http-auth/validate-bind-host! "localhost")))))))

(deftest bind-validation-allows-wildcard-when-managed-proxy-enabled
  (let [secret-file (temp-secret-file "proxy-secret")]
    (runtime-overlay/activate! (overlay secret-file))
    (is (= "0.0.0.0" (http-auth/validate-bind-host! "0.0.0.0")))
    (is (= "::" (http-auth/validate-bind-host! "::")))
    (is (= "::" (http-auth/validate-bind-host! "[::]")))))

(deftest bind-validation-requires-usable-managed-proxy-secret
  (let [secret-file (temp-secret-file "\n")]
    (runtime-overlay/activate! (overlay secret-file))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"missing or empty"
         (http-auth/validate-bind-host! "0.0.0.0")))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"missing or empty"
         (http-auth/validate-bind-host! "127.0.0.1")))))

(deftest bind-validation-allows-command-authenticated-remote-mode
  (without-command-token-env
    #(do
       (db/set-config! :secret/command-channel-token "command-secret")
       (is (= "0.0.0.0" (http-auth/validate-bind-host! "0.0.0.0")))
       (is (= "::" (http-auth/validate-bind-host! "::"))))))

(deftest bind-validation-allows-loopback-range
  (is (= "127.0.0.2" (http-auth/validate-bind-host! "127.0.0.2")))
  (is (= "127.255.255.255" (http-auth/validate-bind-host! "127.255.255.255")))
  (is (false? (http-auth/non-loopback-bind? "127.0.0.1")))
  (is (false? (http-auth/non-loopback-bind? "127.0.0.2")))
  (is (false? (http-auth/non-loopback-bind? "::1")))
  (is (true? (http-auth/non-loopback-bind? "0.0.0.0")))
  (is (true? (http-auth/non-loopback-bind? "192.168.1.10")))
  (is (true? (http-auth/non-loopback-bind? "127.example.invalid")))
  (is (true? (http-auth/non-loopback-bind? "127.0.0.999"))))
