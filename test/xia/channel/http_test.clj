(ns xia.channel.http-test
  (:require [charred.api :as json]
            [clojure.test :refer :all]
            [clojure.string :as str]
            [xia.agent]
            [xia.channel.http :as http]
            [xia.db :as db]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.io ByteArrayInputStream]
           [java.nio.charset StandardCharsets]
           [java.util UUID]))

(use-fixtures :each with-test-db)

(defn- request-body [payload]
  (ByteArrayInputStream.
    (.getBytes (json/write-json-str payload) StandardCharsets/UTF_8)))

(defn- response-json [response]
  (json/read-json (:body response)))

(defn- local-session-cookie []
  (let [response (#'http/router {:uri "/" :request-method :get})]
    (first (str/split (get-in response [:headers "Set-Cookie"]) #";"))))

(defn- ui-headers []
  {"origin" "http://localhost:18790"
   "cookie" (local-session-cookie)})

(defn- wait-for
  [f]
  (loop [attempt 0]
    (let [result (f)]
      (cond
        result result
        (>= attempt 49) nil
        :else (do (Thread/sleep 20)
                  (recur (inc attempt)))))))

(deftest root-route-serves-local-web-interface
  (let [response (#'http/router {:uri "/" :request-method :get})]
    (is (= 200 (:status response)))
    (is (= "text/html; charset=utf-8" (get-in response [:headers "Content-Type"])))
    (is (re-find #"xia-local-session=" (get-in response [:headers "Set-Cookie"])))
    (is (re-find #"HttpOnly" (get-in response [:headers "Set-Cookie"])))
    (is (re-find #"SameSite=Strict" (get-in response [:headers "Set-Cookie"])))
    (is (re-find #"Paste Input" (:body response)))
    (is (re-find #"Approval Required" (:body response)))
    (is (re-find #"Copy transcript" (:body response)))
    (is (re-find #"<textarea" (:body response)))
    (is (re-find #"sessionStorage\.getItem" (:body response)))
    (is (not (re-find #"localStorage\.setItem\(storageKeys\.messages" (:body response))))))

(deftest create-session-route-returns-session-id
  (let [response (#'http/router {:uri            "/sessions"
                                 :request-method :post
                                 :headers        (ui-headers)})
        body     (response-json response)
        sid      (UUID/fromString (get body "session_id"))]
    (is (= 200 (:status response)))
    (is (= :http
           (ffirst (db/q '[:find ?channel :in $ ?sid
                           :where
                           [?s :session/id ?sid]
                           [?s :session/channel ?channel]]
                         sid))))))

(deftest chat-route-creates-http-session
  (with-redefs [xia.agent/process-message (fn [_session-id user-message & _]
                                            (str "echo: " user-message))]
    (let [response (#'http/router {:uri            "/chat"
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"message" "hello"})})
          body     (response-json response)
          sid      (UUID/fromString (get body "session_id"))]
      (is (= 200 (:status response)))
      (is (= "application/json" (get-in response [:headers "Content-Type"])))
      (is (= "assistant" (get body "role")))
      (is (= "echo: hello" (get body "content")))
      (is (= :http
             (ffirst (db/q '[:find ?channel :in $ ?sid
                             :where
                             [?s :session/id ?sid]
                             [?s :session/channel ?channel]]
                           sid)))))))

(deftest chat-route-reuses-provided-session-id
  (let [seen-session (atom nil)
        sid          (random-uuid)]
    (with-redefs [xia.agent/process-message (fn [session-id _user-message & _]
                                              (reset! seen-session session-id)
                                              "ok")]
      (let [response (#'http/router {:uri            "/chat"
                                     :request-method :post
                                     :headers        (ui-headers)
                                     :body           (request-body {"message" "hello"
                                                                    "session_id" (str sid)})})
            body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= (str sid) (get body "session_id")))
        (is (= sid @seen-session))))))

(deftest chat-route-blocks-non-local-origins
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :headers        {"origin" "https://evil.example"}
                                 :body           (request-body {"message" "hello"})})
        body     (response-json response)]
    (is (= 403 (:status response)))
    (is (= "forbidden origin" (get body "error")))))

(deftest chat-route-blocks-missing-session-secret
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :headers        {"origin" "http://localhost:18790"}
                                 :body           (request-body {"message" "hello"})})
        body     (response-json response)]
    (is (= 401 (:status response)))
    (is (= "missing or invalid local session secret" (get body "error")))))

(deftest chat-route-blocks-no-origin-without-session-secret
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :body           (request-body {"message" "hello"})})
        body     (response-json response)]
    (is (= 401 (:status response)))
    (is (= "missing or invalid local session secret" (get body "error")))))

(deftest chat-route-allows-local-origins-with-session-secret
  (with-redefs [xia.agent/process-message (fn [_session-id _user-message & _] "ok")]
    (let [response (#'http/router {:uri            "/chat"
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"message" "hello"})})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= "ok" (get body "content"))))))

(deftest chat-route-allows-direct-local-client-with-session-secret
  (with-redefs [xia.agent/process-message (fn [_session-id _user-message & _] "ok")]
    (let [response (#'http/router {:uri            "/chat"
                                   :request-method :post
                                   :headers        {"cookie" (local-session-cookie)}
                                   :body           (request-body {"message" "hello"})})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= "ok" (get body "content"))))))

(deftest chat-route-validates-required-message
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"session_id" (str (random-uuid))})})
        body     (response-json response)]
    (is (= 400 (:status response)))
    (is (= "missing 'message' field" (get body "error")))))

(deftest session-messages-route-returns-transcript
  (let [sid (db/create-session! :http)]
    (db/add-message! sid :user "hello")
    (db/add-message! sid :assistant "hi there")
    (db/add-message! sid :tool "internal")
    (let [response (#'http/router {:uri            (str "/sessions/" sid "/messages")
                                   :request-method :get
                                   :headers        (ui-headers)})
          body     (response-json response)
          messages (get body "messages")]
      (is (= 200 (:status response)))
      (is (= 2 (count messages)))
      (is (= "user" (get (first messages) "role")))
      (is (= "hello" (get (first messages) "content")))
      (is (= "assistant" (get (second messages) "role")))
      (is (= "hi there" (get (second messages) "content"))))))

(deftest session-messages-route-blocks-non-local-origins
  (let [response (#'http/router {:uri            (str "/sessions/" (random-uuid) "/messages")
                                 :request-method :get
                                 :headers        {"origin" "https://evil.example"}})
        body     (response-json response)]
    (is (= 403 (:status response)))
    (is (= "forbidden origin" (get body "error")))))

(deftest session-messages-route-blocks-missing-session-secret
  (let [response (#'http/router {:uri            (str "/sessions/" (random-uuid) "/messages")
                                 :request-method :get
                                 :headers        {"origin" "http://localhost:18790"}})
        body     (response-json response)]
    (is (= 401 (:status response)))
    (is (= "missing or invalid local session secret" (get body "error")))))

(deftest approval-route-allows-round-trip
  (let [sid    (str (db/create-session! :http))
        waiter (future
                 (#'http/http-approval-handler
                   {:session-id   sid
                    :tool-id      :browser-login
                    :tool-name    "browser-login"
                    :description  "Log into a site"
                    :arguments    {"site" "jira"}
                    :reason       "uses stored site credentials"
                    :policy       :session}))]
    (let [pending-body (wait-for
                         #(let [response (#'http/router {:uri            (str "/sessions/" sid "/approval")
                                                         :request-method :get
                                                         :headers        (ui-headers)})
                                body     (response-json response)]
                            (when (get body "pending")
                              body)))]
      (is (some? pending-body))
      (is (= "browser-login" (get-in pending-body ["approval" "tool_name"])))
      (let [approval-id (get-in pending-body ["approval" "approval_id"])
            submit      (#'http/router {:uri            (str "/sessions/" sid "/approval")
                                        :request-method :post
                                        :headers        (ui-headers)
                                        :body           (request-body {"approval_id" approval-id
                                                                       "decision" "allow"})})
            submit-body (response-json submit)]
        (is (= 200 (:status submit)))
        (is (= "recorded" (get submit-body "status")))))
    (is (= true (deref waiter 2000 ::timeout)))))

(deftest start-binds-to-loopback-by-default
  (let [captured (atom nil)]
    (with-redefs [org.httpkit.server/run-server
                  (fn [_handler opts]
                    (reset! captured opts)
                    (fn [] nil))]
      (http/start! 18790)
      (http/stop!)
      (is (= "127.0.0.1" (:ip @captured)))
      (is (= 18790 (:port @captured))))))

(deftest start-allows-explicit-bind-host
  (let [captured (atom nil)]
    (with-redefs [org.httpkit.server/run-server
                  (fn [_handler opts]
                    (reset! captured opts)
                    (fn [] nil))]
      (http/start! "0.0.0.0" 18790)
      (http/stop!)
      (is (= "0.0.0.0" (:ip @captured)))
      (is (= 18790 (:port @captured))))))
