(ns xia.channel.http-test
  (:require [charred.api :as json]
            [clojure.test :refer :all]
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

(deftest root-route-serves-local-web-interface
  (let [response (#'http/router {:uri "/" :request-method :get})]
    (is (= 200 (:status response)))
    (is (= "text/html; charset=utf-8" (get-in response [:headers "Content-Type"])))
    (is (re-find #"Paste Input" (:body response)))
    (is (re-find #"Copy transcript" (:body response)))
    (is (re-find #"<textarea" (:body response)))))

(deftest chat-route-creates-http-session
  (with-redefs [xia.agent/process-message (fn [_session-id user-message & _]
                                            (str "echo: " user-message))]
    (let [response (#'http/router {:uri            "/chat"
                                   :request-method :post
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
                                     :body           (request-body {"message" "hello"
                                                                    "session_id" (str sid)})})
            body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= (str sid) (get body "session_id")))
        (is (= sid @seen-session))))))

(deftest chat-route-validates-required-message
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :body           (request-body {"session_id" (str (random-uuid))})})
        body     (response-json response)]
    (is (= 400 (:status response)))
    (is (= "missing 'message' field" (get body "error")))))
