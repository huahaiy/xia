(ns xia.channel.http.websocket
  "WebSocket channel handler."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [org.httpkit.server :as http]
            [taoensso.timbre :as log]
            [xia.bridge :as bridge]
            [xia.channel.http.common :as http-common]))

(def ^:private receive-retry-delay-ms 5000)

(defn- clear-receive-failure!
  [deps session-id]
  (swap! (:websocket-receive-failures-atom deps) dissoc (str session-id)))

(defn- active-receive-failure
  [deps session-id]
  (let [sid-str  (str session-id)
        failure  (get @(:websocket-receive-failures-atom deps) sid-str)
        now-ms   (System/currentTimeMillis)
        retry-ms (long (or (:retry-not-before-ms failure) 0))]
    (cond
      (nil? failure)
      nil

      (<= retry-ms now-ms)
      (do
        (clear-receive-failure! deps sid-str)
        nil)

      :else
      (assoc failure :retry-after-ms (- retry-ms now-ms)))))

(defn- send-json!
  [ch payload]
  (try
    (http/send! ch (json/write-json-str payload))
    (catch Exception e
      (log/warn e "Failed to send WebSocket response"))))

(defn- send-error!
  [ch error-message & {:as extra}]
  (send-json! ch
              (merge {:type  "error"
                      :error (or (some-> error-message str/trim not-empty)
                                 "WebSocket request failed")}
                     extra)))

(defn- record-receive-failure!
  [deps session-id ^Throwable e]
  (let [sid-str             (str session-id)
        retry-not-before-ms (+ (System/currentTimeMillis)
                               receive-retry-delay-ms)
        failure             {:session-id          sid-str
                             :error               (or (some-> (http-common/throwable-message deps e)
                                                              str/trim
                                                              not-empty)
                                                      "WebSocket request failed")
                             :failed-at           (java.util.Date.)
                             :retry-not-before-ms retry-not-before-ms}]
    (swap! (:websocket-receive-failures-atom deps) assoc sid-str failure)
    failure))

(defn- handle-receive-failure!
  [deps ch session-id ^Throwable e]
  (try
    (bridge/cancel-session! session-id "websocket request failed")
    (catch Exception cancel-error
      (log/warn cancel-error
                "Failed to cancel WebSocket session after receive error"
                (str session-id))))
  (let [{:keys [error]} (record-receive-failure! deps session-id e)]
    (log/error e "WebSocket message error; temporarily blocking retries" (str session-id))
    (send-error! ch
                 error
                 :retry_after_ms receive-retry-delay-ms)))

(defn- finalize-session!
  [deps ch]
  (when-let [sid (get @(:ws-sessions-atom deps) ch)]
    (clear-receive-failure! deps sid)
    (bridge/finalize-channel-session! sid
                                      :websocket
                                      :reason :websocket-close
                                      :consolidation-mode :sync)
    (swap! (:session-statuses-atom deps) dissoc (str sid)))
  (swap! (:ws-sessions-atom deps) dissoc ch)
  (log/info "WebSocket disconnected"))

(defn handler
  [deps req]
  ((:protected-route-response deps)
   req
   #(http/as-channel req
                     {:on-open
                      (fn [ch]
                        (let [{:keys [session-id]} (bridge/create-session! :websocket)
                              sid session-id]
                          (swap! (:ws-sessions-atom deps) assoc ch sid)
                          (log/info "WebSocket connected, session:" sid)
                          (send-json! ch {:type "connected" :session-id (str sid)})))

                      :on-receive
                      (fn [ch msg]
                        (if-let [sid (get @(:ws-sessions-atom deps) ch)]
                          (if-let [{:keys [retry-after-ms]} (active-receive-failure deps sid)]
                            (send-error! ch
                                         "Previous WebSocket request failed; wait before retrying."
                                         :retry_after_ms retry-after-ms)
                            (let [data (try
                                         (json/read-json msg)
                                         (catch Exception e
                                           (send-error! ch (http-common/throwable-message deps e))
                                           ::invalid-message))]
                              (when-not (= ::invalid-message data)
                                (try
                                  (let [text     (get data "message" (get data "content" msg))
                                        response (bridge/send-message! sid text :channel :websocket)]
                                    (clear-receive-failure! deps sid)
                                    (send-json! ch {:type    "message"
                                                    :role    "assistant"
                                                    :content response}))
                                  (catch Throwable t
                                    (handle-receive-failure! deps ch sid t)
                                    (when (instance? Error t)
                                      (throw t)))))))
                          (send-error! ch "Session not found")))

                      :on-close
                      (fn [ch _status]
                        (finalize-session! deps ch))})))
