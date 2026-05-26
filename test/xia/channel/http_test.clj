(ns xia.channel.http-test
  (:require [charred.api :as json]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.channel.http :as http]
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
