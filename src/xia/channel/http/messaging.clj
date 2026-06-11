(ns xia.channel.http.messaging
  "Webhook handlers for external messaging channels."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [xia.channel.http.common :as http-common]
            [xia.channel.messaging :as messaging]))

(defn- plain-response
  [status body]
  {:status status
   :headers {"Content-Type" "text/plain; charset=utf-8"}
   :body body})

(defn handle-slack-events
  [deps req]
  (if-not (messaging/slack-enabled?)
    (http-common/json-response deps 503 {:error "slack messaging channel is not enabled"})
    (let [body-bytes (or (http-common/read-body-bytes deps (:body req)) (byte-array 0))
          body-text  (String. ^bytes body-bytes java.nio.charset.StandardCharsets/UTF_8)
          signature  (http-common/request-header deps req "x-slack-signature")
          timestamp  (http-common/request-header deps req "x-slack-request-timestamp")]
      (if-not (messaging/valid-slack-signature? body-bytes timestamp signature)
        (http-common/json-response deps 401 {:error "invalid slack signature"})
        (let [payload (json/read-json body-text)
              payload-type (get payload "type")]
          (cond
            (= "url_verification" payload-type)
            (plain-response 200 (or (get payload "challenge") ""))

            (= "event_callback" payload-type)
            (do
              (messaging/handle-slack-event! payload)
              (http-common/json-response deps 200 {:ok true}))

            :else
            (http-common/json-response deps 200 {:ok true})))))))

(defn handle-telegram-webhook
  [deps req]
  (if-not (messaging/telegram-enabled?)
    (http-common/json-response deps 503 {:error "telegram messaging channel is not enabled"})
    (let [secret-header (http-common/request-header deps req "x-telegram-bot-api-secret-token")]
      (if-not (messaging/telegram-secret-valid? secret-header)
        (http-common/json-response deps 401 {:error "invalid telegram webhook secret"})
        (let [body-bytes (or (http-common/read-body-bytes deps (:body req)) (byte-array 0))
              body-text  (String. ^bytes body-bytes java.nio.charset.StandardCharsets/UTF_8)
              payload    (if (str/blank? body-text) {} (json/read-json body-text))]
          (messaging/handle-telegram-update! payload)
          (http-common/json-response deps 200 {:ok true}))))))
