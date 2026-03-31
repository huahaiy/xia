(ns xia.channel.http.messaging-test
  (:require [charred.api :as json]
            [clojure.test :refer :all]
            [xia.channel.http.messaging :as http-messaging]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.io ByteArrayInputStream]
           [java.nio.charset StandardCharsets]))

(use-fixtures :each with-test-db)

(defn- request-body
  [payload]
  (let [body ^String (json/write-json-str payload)]
    (ByteArrayInputStream.
     (.getBytes body StandardCharsets/UTF_8))))

(defn- response-json
  [response]
  (json/read-json (:body response)))

(defn- handler-deps
  []
  {:json-response (fn [status body]
                    {:status status
                     :headers {"Content-Type" "application/json"}
                     :body (json/write-json-str body)})
   :read-body-bytes (fn [body]
                      (when body
                        (.readAllBytes ^java.io.InputStream body)))
   :request-header (fn [req header-name]
                     (get-in req [:headers header-name]))})

(deftest slack-webhook-handler-rejects-disabled-channel
  (with-redefs [xia.channel.messaging/slack-enabled? (constantly false)]
    (let [response (http-messaging/handle-slack-events (handler-deps)
                                                       {:body (request-body {"type" "event_callback"})})
          body     (response-json response)]
      (is (= 503 (:status response)))
      (is (= "slack messaging channel is not enabled" (get body "error"))))))

(deftest slack-webhook-handler-accepts-url-verification
  (with-redefs [xia.channel.messaging/slack-enabled? (constantly true)
                xia.channel.messaging/valid-slack-signature? (constantly true)]
    (let [response (http-messaging/handle-slack-events
                    (handler-deps)
                    {:headers {"x-slack-signature" "sig"
                               "x-slack-request-timestamp" "1"}
                     :body (request-body {"type" "url_verification"
                                          "challenge" "challenge-token"})})]
      (is (= 200 (:status response)))
      (is (= "text/plain; charset=utf-8" (get-in response [:headers "Content-Type"])))
      (is (= "challenge-token" (:body response))))))

(deftest slack-webhook-handler-forwards-event-callbacks
  (let [seen (atom nil)]
    (with-redefs [xia.channel.messaging/slack-enabled? (constantly true)
                  xia.channel.messaging/valid-slack-signature? (constantly true)
                  xia.channel.messaging/handle-slack-event! (fn [payload]
                                                             (reset! seen payload))]
      (let [response (http-messaging/handle-slack-events
                      (handler-deps)
                      {:headers {"x-slack-signature" "sig"
                                 "x-slack-request-timestamp" "1"}
                       :body (request-body {"type" "event_callback"
                                            "event" {"type" "app_mention"
                                                     "text" "hello"}})})
            body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= {"ok" true} body))
        (is (= {"type" "event_callback"
                "event" {"type" "app_mention"
                         "text" "hello"}}
               @seen))))))

(deftest telegram-webhook-handler-rejects-invalid-secret
  (with-redefs [xia.channel.messaging/telegram-enabled? (constantly true)
                xia.channel.messaging/telegram-secret-valid? (constantly false)]
    (let [response (http-messaging/handle-telegram-webhook
                    (handler-deps)
                    {:headers {"x-telegram-bot-api-secret-token" "bad"}
                     :body (request-body {"update_id" 1})})
          body     (response-json response)]
      (is (= 401 (:status response)))
      (is (= "invalid telegram webhook secret" (get body "error"))))))

(deftest telegram-webhook-handler-forwards-valid-updates
  (let [seen (atom nil)]
    (with-redefs [xia.channel.messaging/telegram-enabled? (constantly true)
                  xia.channel.messaging/telegram-secret-valid? (constantly true)
                  xia.channel.messaging/handle-telegram-update! (fn [payload]
                                                                  (reset! seen payload))]
      (let [response (http-messaging/handle-telegram-webhook
                      (handler-deps)
                      {:headers {"x-telegram-bot-api-secret-token" "good"}
                       :body (request-body {"update_id" 7
                                            "message" {"text" "hello"}})})
            body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= {"ok" true} body))
        (is (= {"update_id" 7
                "message" {"text" "hello"}}
               @seen))))))
