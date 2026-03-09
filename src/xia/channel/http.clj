(ns xia.channel.http
  "HTTP/WebSocket channel — enables remote clients and web UIs."
  (:require [org.httpkit.server :as http]
            [charred.api :as json]
            [clojure.tools.logging :as log]
            [xia.db :as db]
            [xia.agent :as agent]
            [xia.working-memory :as wm]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private server-atom (atom nil))
(defonce ^:private ws-sessions (atom {})) ; channel → session-id

;; ---------------------------------------------------------------------------
;; WebSocket handler
;; ---------------------------------------------------------------------------

(defn- ws-handler [req]
  (http/as-channel req
    {:on-open
     (fn [ch]
       (let [sid (db/create-session! :websocket)]
         (swap! ws-sessions assoc ch sid)
         (wm/create-wm! sid)
         (wm/warm-start!)
         (log/info "WebSocket connected, session:" sid)
         (http/send! ch (json/write-json-str {:type "connected" :session-id (str sid)}))))

     :on-receive
     (fn [ch msg]
       (let [sid (get @ws-sessions ch)]
         (try
           (let [data     (json/read-json msg)
                 text     (get data "message" (get data "content" msg))
                 response (agent/process-message sid text :channel :websocket)]
             (http/send! ch (json/write-json-str {:type    "message"
                                                   :role    "assistant"
                                                   :content response})))
           (catch Exception e
             (log/error e "WebSocket message error")
             (http/send! ch (json/write-json-str {:type  "error"
                                                   :error (.getMessage e)}))))))

     :on-close
     (fn [ch _status]
       (wm/snapshot!)
       (wm/clear-wm!)
       (swap! ws-sessions dissoc ch)
       (log/info "WebSocket disconnected"))}))

;; ---------------------------------------------------------------------------
;; REST endpoints
;; ---------------------------------------------------------------------------

(defn- read-body [req]
  (when-let [body (:body req)]
    (json/read-json (slurp body))))

(defn- json-response [status body]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/write-json-str body)})

(defn- handle-chat [req]
  (let [data       (read-body req)
        message    (get data "message")
        session-id (get data "session_id")]
    (if-not message
      (json-response 400 {:error "missing 'message' field"})
      (let [sid      (if session-id
                       (java.util.UUID/fromString session-id)
                       (db/create-session! :http))
            _        (do (wm/create-wm! sid) (wm/warm-start!))
            response (agent/process-message sid message :channel :http)]
        (json-response 200 {:session_id (str sid)
                            :role       "assistant"
                            :content    response})))))

(defn- handle-skills [_req]
  (json-response 200 {:skills (mapv (fn [s]
                                      {:id          (name (:skill/id s))
                                       :name        (:skill/name s)
                                       :description (:skill/description s)
                                       :type        (name (:skill/type s))
                                       :enabled     (:skill/enabled? s)})
                                    (db/list-skills))}))

(defn- handle-health [_req]
  (json-response 200 {:status "ok" :version "0.1.0"}))

;; ---------------------------------------------------------------------------
;; Router
;; ---------------------------------------------------------------------------

(defn- router [req]
  (let [uri    (:uri req)
        method (:request-method req)]
    (cond
      ;; WebSocket upgrade
      (and (= uri "/ws") (http/websocket-upgrade-request? req))
      (ws-handler req)

      ;; REST
      (and (= method :post) (= uri "/chat"))
      (handle-chat req)

      (and (= method :get) (= uri "/skills"))
      (handle-skills req)

      (and (= method :get) (= uri "/health"))
      (handle-health req)

      :else
      (json-response 404 {:error "not found"}))))

;; ---------------------------------------------------------------------------
;; Server lifecycle
;; ---------------------------------------------------------------------------

(defn start!
  "Start the HTTP/WebSocket server on the given port."
  [port]
  (when @server-atom
    (log/warn "Server already running"))
  (let [s (http/run-server router {:port port})]
    (reset! server-atom s)
    (log/info "HTTP/WebSocket server started on port" port)
    s))

(defn stop! []
  (when-let [s @server-atom]
    (s) ; http-kit stop fn
    (reset! server-atom nil)
    (log/info "Server stopped")))
