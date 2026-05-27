(ns xia.channel.http
  "HTTP/WebSocket channel — enables remote clients and web UIs."
  (:require [clojure.string :as str]
            [org.httpkit.server :as http]
            [ring.middleware.multipart-params :as multipart]
            [taoensso.timbre :as log]
            [xia.bridge :as bridge]
            [xia.db :as db]
            [xia.channel.http.admin :as http-admin]
            [xia.channel.http.assets :as http-assets]
            [xia.channel.http.auth :as http-auth]
            [xia.channel.http.command :as http-command]
            [xia.channel.http.interaction :as http-interaction]
            [xia.channel.http.knowledge :as http-knowledge]
            [xia.channel.http.messaging :as http-messaging]
            [xia.channel.http.request :as http-request]
            [xia.channel.http.response :as http-response]
            [xia.channel.http.session :as http-session]
            [xia.channel.http.status :as http-status]
            [xia.channel.http.task-board :as http-task-board]
            [xia.channel.http.value :as http-value]
            [xia.channel.http.websocket :as http-websocket]
            [xia.channel.http.workspace :as http-workspace]
            [xia.channel.messaging :as messaging]
            [xia.runtime-state :as runtime-state]
            [xia.session-lifecycle :as session-life])
  (:import [java.net BindException]
    [java.security SecureRandom]
    [java.util Base64]
    [java.util.concurrent ConcurrentHashMap Executors ScheduledExecutorService ScheduledFuture TimeUnit]
    [java.util.concurrent.atomic AtomicLong]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private installed-runtime-atom (atom nil))

(def ^:private default-rest-session-idle-timeout-ms (* 30 60 1000))
(def ^:private session-finalize-lock-count session-life/default-finalize-lock-count)
(def ^:private http-port-search-limit 100)
(def ^:private rest-session-channels #{:http :command})
(def ^:private local-ui-session-channels #{:http :websocket})
(def ^:private busy-session-states #{:running :waiting_input :waiting_approval})
(declare install-runtime! clear-runtime!)

(defn- make-runtime
  []
  {:server-atom                         (atom nil)
   :ws-sessions-atom                    (atom {})
   :websocket-receive-failures-atom     (atom {})
   :session-statuses-atom               (atom {})
   :task-runtime-events-atom            (atom {})
   :task-runtime-stream-subscribers-atom (atom {})
   :web-dev-state-atom                  (atom {:enabled? false
                                               :root nil})
   :command-shutdown-handler-atom       (atom nil)
   :local-session-secret                (delay
                                          (let [bytes (byte-array 32)
                                                _     (.nextBytes (SecureRandom.) bytes)]
                                            (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bytes)))
   :ingress-rate-limits                 (ConcurrentHashMap.)
   :ingress-rate-limit-cleanup          (AtomicLong. 0)
   :command-auth-nonces                 (ConcurrentHashMap.)
   :command-auth-nonce-cleanup          (AtomicLong. 0)
   :managed-proxy-nonces                (ConcurrentHashMap.)
   :managed-proxy-nonce-cleanup         (AtomicLong. 0)
   :rest-session-finalizer-executor-atom (atom nil)
   :rest-session-finalizers-atom        (atom {})
   :session-finalize-locks              (session-life/make-finalize-locks session-finalize-lock-count)})

(defn- maybe-current-runtime
  []
  @installed-runtime-atom)

(defn- current-runtime
  []
  (or (maybe-current-runtime)
      (throw (ex-info "HTTP runtime is not installed"
                      {:component :xia/http}))))

(defn- server-atom
  []
  (:server-atom (current-runtime)))

(defn- maybe-server-atom
  []
  (some-> (maybe-current-runtime) :server-atom))

(defn- ws-sessions-atom
  []
  (:ws-sessions-atom (current-runtime)))

(defn- session-statuses-atom
  []
  (:session-statuses-atom (current-runtime)))

(defn- websocket-receive-failures-atom
  []
  (:websocket-receive-failures-atom (current-runtime)))

(defn- task-runtime-events-atom
  []
  (:task-runtime-events-atom (current-runtime)))

(defn- task-runtime-stream-subscribers-atom
  []
  (:task-runtime-stream-subscribers-atom (current-runtime)))

(defn- web-dev-state-atom
  []
  (:web-dev-state-atom (current-runtime)))

(defn- command-shutdown-handler-atom
  []
  (:command-shutdown-handler-atom (current-runtime)))

(defn- ^ConcurrentHashMap ingress-rate-limits
  []
  (:ingress-rate-limits (current-runtime)))

(defn- ^AtomicLong ingress-rate-limit-cleanup
  []
  (:ingress-rate-limit-cleanup (current-runtime)))

(defn- ^ConcurrentHashMap command-auth-nonces
  []
  (:command-auth-nonces (current-runtime)))

(defn- ^AtomicLong command-auth-nonce-cleanup
  []
  (:command-auth-nonce-cleanup (current-runtime)))

(defn- ^ConcurrentHashMap managed-proxy-nonces
  []
  (:managed-proxy-nonces (current-runtime)))

(defn- ^AtomicLong managed-proxy-nonce-cleanup
  []
  (:managed-proxy-nonce-cleanup (current-runtime)))

(defn- maybe-command-shutdown-handler-atom
  []
  (some-> (maybe-current-runtime) :command-shutdown-handler-atom))

(defn- local-session-secret-delay
  []
  (:local-session-secret (current-runtime)))

(defn- rest-session-finalizer-executor-atom
  []
  (:rest-session-finalizer-executor-atom (current-runtime)))

(defn- rest-session-finalizers-atom
  []
  (:rest-session-finalizers-atom (current-runtime)))

(defn- session-finalize-locks
  []
  (:session-finalize-locks (current-runtime)))

(defn- throwable-message
  [^Throwable e]
  (.getMessage e))

(defn- instant->str
  [value]
  (http-value/instant->str value))

(defn- date->millis
  [value]
  (http-value/date->millis value))

(defn- request-header
  [req header-name]
  (http-request/request-header req header-name))

(defn- multipart-form-request?
  [req]
  (http-request/multipart-form-request? req))

(declare nonblank-str parse-session-id session-exists? session-id-str)

(defn- request-base-url
  [req]
  (http-auth/request-base-url req))

(defn- parse-query-string
  [query-string]
  (http-request/parse-query-string query-string))

(defn- read-body-bytes
  [body]
  (http-request/read-body-bytes body))

(defn- read-body
  [req]
  (http-request/read-body req))

(defn- auth-secret-deps
  []
  {:local-session-secret-delay local-session-secret-delay})

(defn- session-secret []
  (http-auth/session-secret (auth-secret-deps)))

(defn- session-cookie-header []
  (str http-auth/local-session-cookie-name "=" (session-secret)
       "; Path=/; HttpOnly; SameSite=Strict"))

(defn- hmac-sha256-base64url
  [secret message]
  (http-auth/hmac-sha256-base64url secret message))

(defn- managed-proxy-signing-payload
  [req timestamp request-id tenant-id runtime-id user-id]
  (http-auth/managed-proxy-signing-payload req
                                           timestamp
                                           request-id
                                           tenant-id
                                           runtime-id
                                           user-id))

(defn- trusted-local-origin?
  "Allow loopback browser origins and direct local clients with no origin
   headers. Origin checks prevent cross-site requests from using the cookie."
  [req]
  (http-auth/trusted-local-origin? req))

(defn- json-response [status body]
  (http-response/json-response status body))

(defn- asset-handler-deps
  []
  {:json-response         json-response
   :session-cookie-header session-cookie-header
   :web-dev-state-atom    (web-dev-state-atom)})

(defn- configure-web-dev!
  [enabled?]
  (http-assets/configure-web-dev! (asset-handler-deps) enabled?))

(defn- static-asset-uri?
  [uri]
  (http-assets/static-asset-uri? uri))

(defn- static-asset-response
  [uri]
  (http-assets/static-asset-response (asset-handler-deps) uri))

(defn- reset-runtime-ingress-rate-limits!
  [runtime]
  (http-auth/reset-runtime-ingress-rate-limits! runtime))

(defn- reset-runtime-command-auth!
  [runtime]
  (http-auth/reset-runtime-command-auth! runtime))

(defn- reset-runtime-managed-proxy-auth!
  [runtime]
  (http-auth/reset-runtime-managed-proxy-auth! runtime))

(defn- download-response
  [filename media-type body]
  (http-response/download-response filename media-type body))

(declare runtime-unavailable-response)

(def ^:private db-disconnected-message
  "Database not connected. Call (xia.db/connect!) first.")

(defn- runtime-unavailable-throwable?
  [^Throwable e]
  (loop [current e
         seen    #{}]
    (cond
      (nil? current)
      false

      (contains? seen current)
      false

      :else
      (let [message (or (throwable-message current)
                        (some-> (ex-data current) :error str))]
        (or (str/includes? (or message "") db-disconnected-message)
            (recur (.getCause current) (conj seen current)))))))

(defn- exception-response
  [^Throwable e]
  (if (runtime-unavailable-throwable? e)
    (runtime-unavailable-response)
    (let [data    (ex-data e)
          status  (or (:status data) 400)
          details (not-empty (dissoc data :status :error :type))
          body    (cond-> {:error (or (:error data) (throwable-message e))}
                    details (assoc :details details))]
      (json-response status body))))

(defn register-command-shutdown-handler!
  [handler]
  (when-not (maybe-current-runtime)
    (install-runtime!))
  (reset! (command-shutdown-handler-atom) handler)
  nil)

(defn clear-command-shutdown-handler!
  []
  (when-let [handler-atom (maybe-command-shutdown-handler-atom)]
    (reset! handler-atom nil))
  nil)

(defn- runtime-available?
  []
  (try
    (db/conn)
    true
    (catch Exception _
      false)))

(defn- runtime-unavailable-response []
  (let [restarting?  (runtime-state/restarting?)
        error        (if restarting?
                       "server is restarting; try again in a moment"
                       "database became unavailable unexpectedly; check server logs")
        phase        (runtime-state/phase)
        db-path      (db/current-db-path)
        instance-id  (db/current-instance-id)
        last-connect (db/last-connect-event)
        last-close   (db/last-close-event)]
    (when-not restarting?
      (log/error "HTTP request hit database-unavailable state"
                 "phase" (name phase)
                 "db-path" db-path
                 "instance" instance-id
                 "last-connect" (pr-str last-connect)
                 "last-close" (pr-str last-close)))
    (json-response 503 {:error error})))

(defn- auth-deps
  []
  {:command-auth-nonce-cleanup   command-auth-nonce-cleanup
   :command-auth-nonces          command-auth-nonces
   :ingress-rate-limit-cleanup   ingress-rate-limit-cleanup
   :ingress-rate-limits          ingress-rate-limits
   :json-response                json-response
   :local-session-secret-delay   local-session-secret-delay
   :managed-proxy-nonce-cleanup  managed-proxy-nonce-cleanup
   :managed-proxy-nonces         managed-proxy-nonces
   :read-body-bytes              read-body-bytes
   :runtime-available?           runtime-available?
   :runtime-unavailable-response runtime-unavailable-response})

(defn- handle-local-session-bootstrap
  [req]
  (http-auth/handle-local-session-bootstrap (auth-deps) req))

(defn- protected-route-response
  [req allowed-fn]
  (http-auth/protected-route-response (auth-deps) req allowed-fn))

(defn- command-route-response
  [req allowed-fn]
  (http-auth/command-route-response (auth-deps) req allowed-fn))

(defn- websocket-handler-deps
  []
  {:protected-route-response protected-route-response
   :session-statuses-atom (session-statuses-atom)
   :throwable-message throwable-message
   :websocket-receive-failures-atom (websocket-receive-failures-atom)
   :ws-sessions-atom (ws-sessions-atom)})

(defn- ws-handler
  [req]
  (http-websocket/handler (websocket-handler-deps) req))

(declare touch-rest-session!)

(defn- parse-session-id
  [session-id]
  (session-life/parse-session-id session-id))

(defn- session-exists?
  [session-id]
  (session-life/session-exists? session-id))

(defn- session-id-str
  [session-id]
  (session-life/session-id-str session-id))

(defn- session-uuid
  [session-id]
  (session-life/session-uuid session-id))

(defn- session-channel
  [session-id]
  (session-life/session-channel session-id))

(defn- session-accessible?
  [session-id expected-channel]
  (session-life/session-accessible? session-id expected-channel))

(defn- session-active?
  [session-id]
  (session-life/active? session-id))

(defn- maybe-resume-http-session!
  [session-id expected-channel]
  (when (and (= expected-channel :http)
             (session-accessible? session-id expected-channel)
             (not (session-active? session-id)))
    (bridge/resume-session! session-id
                            :expected-channel expected-channel
                            :locks (session-finalize-locks)
                            :touch! touch-rest-session!)))

(defn- session-busy?
  [session-id]
  (let [state (:state (get @(session-statuses-atom) (str session-id)))]
    (contains? busy-session-states
               (cond
                 (keyword? state) state
                 (string? state) (keyword state)
                 :else state))))

(defn- rest-session-idle-timeout-ms
  []
  default-rest-session-idle-timeout-ms)

(declare finalize-rest-session!)

(defn- cancel-rest-session-finalizer!
  [session-id]
  (when-let [sid (session-id-str session-id)]
    (when-let [^ScheduledFuture future (get @(rest-session-finalizers-atom) sid)]
      (.cancel future false))
    (swap! (rest-session-finalizers-atom) dissoc sid)))

(defn- clear-rest-session-finalizers!
  []
  (doseq [[_ ^ScheduledFuture future] @(rest-session-finalizers-atom)]
    (.cancel future false))
  (reset! (rest-session-finalizers-atom) {}))

(defn- schedule-rest-session-finalizer!
  [session-id]
  (when-let [sid (session-id-str session-id)]
    (cancel-rest-session-finalizer! sid)
    (when-let [^ScheduledExecutorService exec @(rest-session-finalizer-executor-atom)]
      (let [delay-ms (rest-session-idle-timeout-ms)
            task     ^Runnable
            (fn []
              (swap! (rest-session-finalizers-atom) dissoc sid)
              (finalize-rest-session! sid :idle-timeout))]
        (swap! (rest-session-finalizers-atom) assoc
               sid
               (.schedule exec task (long delay-ms) TimeUnit/MILLISECONDS))))))

(defn- touch-rest-session!
  [session-id]
  (when (session-active? session-id)
    (schedule-rest-session-finalizer! session-id)))

(defn- truncate-text
  [value limit]
  (http-value/truncate-text value limit))

(defn- status-handler-deps
  []
  {:session-statuses-atom (session-statuses-atom)
   :task-runtime-events-atom (task-runtime-events-atom)
   :task-runtime-stream-subscribers-atom (task-runtime-stream-subscribers-atom)})

(defn- clear-session-status!
  [session-id]
  (http-status/clear-session-status! (status-handler-deps) session-id))

(defn- register-task-runtime-stream-subscriber!
  [task-id subscriber-id callback]
  (http-status/register-runtime-stream-subscriber!
   (status-handler-deps)
   task-id
   subscriber-id
   callback))

(defn- unregister-task-runtime-stream-subscriber!
  [task-id subscriber-id]
  (http-status/unregister-runtime-stream-subscriber!
   (status-handler-deps)
   task-id
   subscriber-id))

(defn- clear-rest-session-state!
  [session-id]
  (session-life/clear-session-state! session-id
                                     :clear-status! clear-session-status!
                                     :cancel-finalizer! cancel-rest-session-finalizer!))

(defn- http-status-handler
  [status]
  (http-status/status-handler (status-handler-deps) status))

(defn- http-runtime-event-handler
  [event]
  (http-status/runtime-event-handler (status-handler-deps) event))

(defn- finalize-rest-session!
  ([session-id]
   (finalize-rest-session! session-id :explicit))
  ([session-id reason]
   (bridge/finalize-session! session-id
                             :locks (session-finalize-locks)
                             :reason reason
                             :default-channel :http
                             :clear-state! clear-rest-session-state!
                             :mark-inactive? true
                             :consolidation-mode :sync)))

(defn- nonblank-str
  [value]
  (http-value/nonblank-str value))

(defn- parse-keyword-id
  [value field-name]
  (http-value/parse-keyword-id value field-name))

(defn- parse-optional-positive-long
  [value field-name]
  (http-value/parse-optional-positive-long value field-name))

(defn- workspace-handler-deps
  []
  {:download-response            download-response
   :exception-response           exception-response
   :instant->str                 instant->str
   :json-response                json-response
   :multipart-form-request?      multipart-form-request?
   :nonblank-str                 nonblank-str
   :parse-optional-positive-long parse-optional-positive-long
   :parse-query-string           parse-query-string
   :parse-session-id             parse-session-id
   :read-body                    read-body
   :read-body-bytes              read-body-bytes
   :request-header               request-header
   :session-exists?              session-exists?
   :throwable-message            throwable-message
   :touch-rest-session!          touch-rest-session!})

(defn- knowledge-handler-deps
  []
  {:instant->str                 instant->str
   :json-response                json-response
   :nonblank-str                 nonblank-str
   :parse-optional-positive-long parse-optional-positive-long
   :parse-query-string           parse-query-string})

(defn- session-handler-deps
  []
  {:approval->body               http-interaction/approval->body
   :cancel-rest-session-finalizer! cancel-rest-session-finalizer!
   :date->millis                 date->millis
   :exception-response           exception-response
   :finalize-rest-session!       finalize-rest-session!
   :instant->str                 instant->str
   :json-response                json-response
   :maybe-resume-http-session!   maybe-resume-http-session!
   :parse-keyword-id             parse-keyword-id
   :parse-query-string           parse-query-string
   :parse-session-id             parse-session-id
   :prompt->body                 http-interaction/prompt->body
   :read-body                    read-body
   :register-task-runtime-stream-subscriber! register-task-runtime-stream-subscriber!
   :session-accessible?          session-accessible?
   :session-active?              session-active?
   :session-busy?                session-busy?
   :session-statuses-atom        (session-statuses-atom)
   :task-runtime-events-atom     (task-runtime-events-atom)
   :throwable-message            throwable-message
   :touch-rest-session!          touch-rest-session!
   :truncate-text                truncate-text
   :unregister-task-runtime-stream-subscriber! unregister-task-runtime-stream-subscriber!})

(defn- admin-handler-deps
  []
  {:exception-response exception-response
   :instant->str       instant->str
   :json-response      json-response
   :read-body          read-body
   :request-base-url   request-base-url
   :truncate-text      truncate-text})

(defn- command-handler-deps
  []
  {:command-shutdown-handler #(some-> (maybe-command-shutdown-handler-atom) deref)
   :instant->str             instant->str
   :json-response            json-response
   :nonblank-str             nonblank-str
   :read-body                read-body})

(defn- task-board-handler-deps
  []
  {:instant->str  instant->str
   :json-response json-response})

(defn- handle-create-session
  ([] (handle-create-session :http))
  ([channel]
   (http-session/handle-create-session (session-handler-deps) channel)))

(defn- handle-chat
  ([req]
   (handle-chat req :http))
  ([req channel]
   (http-session/handle-chat (session-handler-deps) req channel)))

(defn- handle-get-status
  ([session-id]
   (handle-get-status session-id nil))
  ([session-id expected-channel]
   (http-session/handle-get-status (session-handler-deps) session-id expected-channel)))

(defn- local-ui-session-allowed?
  [session-id]
  (contains? local-ui-session-channels
             (session-channel session-id)))

(defn- handle-local-get-status
  [session-id]
  (if-not (session-id-str session-id)
    (handle-get-status session-id)
    (if (local-ui-session-allowed? session-id)
      (handle-get-status session-id
                         (when (= :http (session-channel session-id))
                           :http))
      (json-response 404 {:error "session not found"}))))

(defn- handle-get-current-task
  ([session-id]
   (handle-get-current-task session-id nil))
  ([session-id expected-channel]
   (http-session/handle-get-current-task (session-handler-deps) session-id expected-channel)))

(defn- handle-get-goal
  ([session-id]
   (handle-get-goal session-id nil))
  ([session-id expected-channel]
   (http-session/handle-get-goal (session-handler-deps) session-id expected-channel)))

(defn- handle-set-goal
  ([session-id req]
   (handle-set-goal session-id req nil))
  ([session-id req expected-channel]
   (http-session/handle-set-goal (session-handler-deps) session-id req expected-channel)))

(defn- handle-pause-goal
  ([session-id]
   (handle-pause-goal session-id nil))
  ([session-id expected-channel]
   (http-session/handle-pause-goal (session-handler-deps) session-id expected-channel)))

(defn- handle-resume-goal
  ([session-id]
   (handle-resume-goal session-id nil))
  ([session-id expected-channel]
   (http-session/handle-resume-goal (session-handler-deps) session-id expected-channel)))

(defn- handle-clear-goal
  ([session-id]
   (handle-clear-goal session-id nil))
  ([session-id expected-channel]
   (http-session/handle-clear-goal (session-handler-deps) session-id expected-channel)))

(defn- handle-local-goal
  [session-id handler]
  (if-not (session-id-str session-id)
    (handler session-id nil)
    (if (local-ui-session-allowed? session-id)
      (handler session-id
               (when (= :http (session-channel session-id))
                 :http))
      (json-response 404 {:error "session not found"}))))

(defn- handle-get-approval
  ([session-id]
   (handle-get-approval session-id nil))
  ([session-id expected-channel]
   (http-session/handle-get-approval (session-handler-deps) session-id expected-channel)))

(defn- handle-get-prompt
  ([session-id]
   (handle-get-prompt session-id nil))
  ([session-id expected-channel]
   (http-session/handle-get-prompt (session-handler-deps) session-id expected-channel)))

(defn- handle-submit-prompt
  ([session-id req]
   (handle-submit-prompt session-id req nil))
  ([session-id req expected-channel]
   (http-session/handle-submit-prompt (session-handler-deps) session-id req expected-channel)))

(defn- handle-submit-approval
  ([session-id req]
   (handle-submit-approval session-id req nil))
  ([session-id req expected-channel]
   (http-session/handle-submit-approval (session-handler-deps) session-id req expected-channel)))

(defn- handle-session-messages
  ([session-id]
   (handle-session-messages session-id nil))
  ([session-id expected-channel]
   (http-session/handle-session-messages (session-handler-deps) session-id expected-channel)))

(defn- handle-close-session
  ([session-id]
   (handle-close-session session-id nil))
  ([session-id expected-channel]
   (http-session/handle-close-session (session-handler-deps) session-id expected-channel)))

(defn- handle-local-close-session
  [session-id]
  (if-not (session-id-str session-id)
    (handle-close-session session-id)
    (if (local-ui-session-allowed? session-id)
      (handle-close-session session-id)
      (json-response 404 {:error "session not found"}))))

(defn- handle-history-sessions []
  (http-session/handle-history-sessions (session-handler-deps)))

(defn- handle-history-tasks []
  (http-session/handle-history-tasks (session-handler-deps)))

(defn- handle-task-board []
  (http-task-board/handle-board (task-board-handler-deps)))

(defn- handle-get-task [task-id]
  (http-session/handle-get-task (session-handler-deps) task-id))

(defn- handle-get-task-events [task-id]
  (http-session/handle-get-task-events (session-handler-deps) task-id))

(defn- handle-get-live-task-events [task-id req]
  (http-session/handle-get-live-task-events (session-handler-deps) task-id req))

(defn- handle-get-task-prompt [task-id]
  (http-session/handle-get-task-prompt (session-handler-deps) task-id))

(defn- handle-submit-task-prompt [task-id req]
  (http-session/handle-submit-task-prompt (session-handler-deps) task-id req))

(defn- handle-get-task-approval [task-id]
  (http-session/handle-get-task-approval (session-handler-deps) task-id))

(defn- handle-submit-task-approval [task-id req]
  (http-session/handle-submit-task-approval (session-handler-deps) task-id req))

(defn- handle-get-task-event-stream [task-id req]
  (http-session/handle-get-task-event-stream (session-handler-deps) task-id req))

(defn- handle-pause-task [task-id]
  (http-session/handle-pause-task (session-handler-deps) task-id))

(defn- handle-stop-task [task-id]
  (http-session/handle-stop-task (session-handler-deps) task-id))

(defn- handle-interrupt-task [task-id]
  (http-session/handle-interrupt-task (session-handler-deps) task-id))

(defn- handle-steer-task [task-id req]
  (http-session/handle-steer-task (session-handler-deps) task-id req))

(defn- handle-fork-task [task-id req]
  (http-session/handle-fork-task (session-handler-deps) task-id req))

(defn- handle-resume-task [task-id req]
  (http-session/handle-resume-task (session-handler-deps) task-id req))

(defn- handle-history-schedules []
  (http-session/handle-history-schedules (session-handler-deps)))

(defn- handle-history-schedule-runs
  [schedule-id]
  (http-session/handle-history-schedule-runs (session-handler-deps) schedule-id))

(defn- handle-list-llm-calls [req]
  (http-session/handle-list-llm-calls (session-handler-deps) req))

(defn- handle-get-llm-call [call-id]
  (http-session/handle-get-llm-call (session-handler-deps) call-id))

(defn- handle-session-audit
  ([session-id]
   (handle-session-audit session-id nil))
  ([session-id expected-channel]
   (http-session/handle-session-audit (session-handler-deps) session-id expected-channel)))

(defn- handle-list-scratch-pads [session-id]
  (http-workspace/handle-list-scratch-pads (workspace-handler-deps) session-id))

(defn- handle-create-scratch-pad [session-id req]
  (http-workspace/handle-create-scratch-pad (workspace-handler-deps) session-id req))

(defn- handle-get-scratch-pad [session-id pad-id]
  (http-workspace/handle-get-scratch-pad (workspace-handler-deps) session-id pad-id))

(defn- handle-save-scratch-pad [session-id pad-id req]
  (http-workspace/handle-save-scratch-pad (workspace-handler-deps) session-id pad-id req))

(defn- handle-edit-scratch-pad [session-id pad-id req]
  (http-workspace/handle-edit-scratch-pad (workspace-handler-deps) session-id pad-id req))

(defn- handle-delete-scratch-pad [session-id pad-id]
  (http-workspace/handle-delete-scratch-pad (workspace-handler-deps) session-id pad-id))

(defn- handle-list-local-docs [session-id]
  (http-workspace/handle-list-local-docs (workspace-handler-deps) session-id))

(defn- handle-create-local-docs [session-id req]
  (http-workspace/handle-create-local-docs (workspace-handler-deps) session-id req))

(defn- handle-get-local-doc [session-id doc-id]
  (http-workspace/handle-get-local-doc (workspace-handler-deps) session-id doc-id))

(defn- handle-delete-local-doc [session-id doc-id]
  (http-workspace/handle-delete-local-doc (workspace-handler-deps) session-id doc-id))

(defn- handle-create-local-doc-scratch-pad [session-id doc-id]
  (http-workspace/handle-create-local-doc-scratch-pad (workspace-handler-deps) session-id doc-id))

(defn- handle-create-artifact-scratch-pad [session-id artifact-id]
  (http-workspace/handle-create-artifact-scratch-pad (workspace-handler-deps) session-id artifact-id))

(defn- handle-list-artifacts [session-id]
  (http-workspace/handle-list-artifacts (workspace-handler-deps) session-id))

(defn- handle-create-artifact [session-id req]
  (http-workspace/handle-create-artifact (workspace-handler-deps) session-id req))

(defn- handle-get-artifact [session-id artifact-id]
  (http-workspace/handle-get-artifact (workspace-handler-deps) session-id artifact-id))

(defn- handle-download-artifact [session-id artifact-id]
  (http-workspace/handle-download-artifact (workspace-handler-deps) session-id artifact-id))

(defn- handle-delete-artifact [session-id artifact-id]
  (http-workspace/handle-delete-artifact (workspace-handler-deps) session-id artifact-id))

(defn- handle-list-workspace-items [req]
  (http-workspace/handle-list-workspace-items (workspace-handler-deps) req))

(defn- handle-download-workspace-item [item-id req]
  (http-workspace/handle-download-workspace-item (workspace-handler-deps) item-id req))

(defn- handle-search-knowledge-nodes [req]
  (http-knowledge/handle-search-nodes (knowledge-handler-deps) req))

(defn- handle-list-knowledge-node-facts [node-id]
  (http-knowledge/handle-list-node-facts (knowledge-handler-deps) node-id))

(defn- handle-delete-knowledge-fact [fact-id]
  (http-knowledge/handle-delete-fact (knowledge-handler-deps) fact-id))

(defn- handle-health [_req]
  (json-response 200 {:status "ok" :version "0.1.0"}))

(defn- handle-web-dev-reload [_req]
  (http-assets/handle-web-dev-reload (asset-handler-deps)))

(defn- handle-home [_req]
  (http-assets/handle-home (asset-handler-deps)))

;; ---------------------------------------------------------------------------
;; Router
;; ---------------------------------------------------------------------------

(defn- router* [req]
  (try
    (let [uri    (:uri req)
          method (:request-method req)
          session-close-match (re-matches #"/sessions/([0-9a-fA-F-]+)" uri)
          session-match      (re-matches #"/sessions/([0-9a-fA-F-]+)/messages" uri)
          session-audit-match (re-matches #"/sessions/([0-9a-fA-F-]+)/audit" uri)
          session-task-match (re-matches #"/sessions/([0-9a-fA-F-]+)/task" uri)
          session-goal-match (re-matches #"/sessions/([0-9a-fA-F-]+)/goal" uri)
          session-goal-status-match (re-matches #"/sessions/([0-9a-fA-F-]+)/goal/status" uri)
          session-goal-pause-match (re-matches #"/sessions/([0-9a-fA-F-]+)/goal/pause" uri)
          session-goal-resume-match (re-matches #"/sessions/([0-9a-fA-F-]+)/goal/resume" uri)
          session-goal-clear-match (re-matches #"/sessions/([0-9a-fA-F-]+)/goal/clear" uri)
          status-match       (re-matches #"/sessions/([0-9a-fA-F-]+)/status" uri)
          prompt-match       (re-matches #"/sessions/([0-9a-fA-F-]+)/prompt" uri)
          approval-match     (re-matches #"/sessions/([0-9a-fA-F-]+)/approval" uri)
          command-session-close-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)" uri)
          command-session-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/messages" uri)
          command-session-audit-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/audit" uri)
          command-session-task-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/task" uri)
          command-session-goal-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/goal" uri)
          command-session-goal-status-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/goal/status" uri)
          command-session-goal-pause-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/goal/pause" uri)
          command-session-goal-resume-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/goal/resume" uri)
          command-session-goal-clear-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/goal/clear" uri)
          command-status-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/status" uri)
          command-runtime-status-match (= uri "/command/runtime/status")
          command-runtime-drain-match (= uri "/command/runtime/drain")
          command-runtime-undrain-match (= uri "/command/runtime/undrain")
          command-mcp-match (= uri "/command/mcp")
          command-managed-checkpoints-match (= uri "/command/managed/checkpoints")
          command-managed-checkpoint-match (re-matches #"/command/managed/checkpoints/([^/]+)" uri)
          command-managed-snapshots-match (= uri "/command/managed/snapshots")
          command-wake-projection-match (= uri "/command/managed/wake-projection")
          command-prompt-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/prompt" uri)
          command-approval-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/approval" uri)
          task-board-match    (= uri "/tasks/board")
          task-match         (re-matches #"/tasks/([0-9a-fA-F-]+)" uri)
          task-events-match  (re-matches #"/tasks/([0-9a-fA-F-]+)/events" uri)
          task-live-events-match (re-matches #"/tasks/([0-9a-fA-F-]+)/live-events" uri)
          task-stream-match  (re-matches #"/tasks/([0-9a-fA-F-]+)/stream" uri)
          task-prompt-match  (re-matches #"/tasks/([0-9a-fA-F-]+)/prompt" uri)
          task-approval-match (re-matches #"/tasks/([0-9a-fA-F-]+)/approval" uri)
          task-pause-match   (re-matches #"/tasks/([0-9a-fA-F-]+)/pause" uri)
          task-stop-match    (re-matches #"/tasks/([0-9a-fA-F-]+)/stop" uri)
          task-interrupt-match (re-matches #"/tasks/([0-9a-fA-F-]+)/interrupt" uri)
          task-steer-match   (re-matches #"/tasks/([0-9a-fA-F-]+)/steer" uri)
          task-fork-match    (re-matches #"/tasks/([0-9a-fA-F-]+)/fork" uri)
          task-resume-match  (re-matches #"/tasks/([0-9a-fA-F-]+)/resume" uri)
          history-schedule-match (re-matches #"/history/schedules/([^/]+)/runs" uri)
          scratch-list-match (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads" uri)
          scratch-pad-match  (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads/([^/]+)" uri)
          scratch-edit-match (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads/([^/]+)/edit" uri)
          local-doc-list-match (re-matches #"/sessions/([0-9a-fA-F-]+)/local-documents" uri)
          local-doc-match      (re-matches #"/sessions/([0-9a-fA-F-]+)/local-documents/([^/]+)" uri)
          local-doc-scratch-match (re-matches #"/sessions/([0-9a-fA-F-]+)/local-documents/([^/]+)/scratch-pads" uri)
          artifact-list-match (re-matches #"/sessions/([0-9a-fA-F-]+)/artifacts" uri)
          artifact-match      (re-matches #"/sessions/([0-9a-fA-F-]+)/artifacts/([^/]+)" uri)
          artifact-scratch-match (re-matches #"/sessions/([0-9a-fA-F-]+)/artifacts/([^/]+)/scratch-pads" uri)
          artifact-download-match (re-matches #"/sessions/([0-9a-fA-F-]+)/artifacts/([^/]+)/download" uri)
          workspace-list-match (= uri "/workspace/items")
          workspace-download-match (re-matches #"/workspace/items/([^/]+)/download" uri)
          knowledge-node-facts-match (re-matches #"/knowledge/nodes/([^/]+)/facts" uri)
          knowledge-fact-match (re-matches #"/knowledge/facts/([^/]+)" uri)
          admin-managed-instance-stop-match (re-matches #"/admin/managed-instances/([^/]+)/stop" uri)
          admin-site-match   (re-matches #"/admin/sites/([^/]+)" uri)
          admin-schedule-match (re-matches #"/admin/schedules/([^/]+)" uri)
          admin-schedule-pause-match (re-matches #"/admin/schedules/([^/]+)/pause" uri)
          admin-schedule-resume-match (re-matches #"/admin/schedules/([^/]+)/resume" uri)
          admin-plugin-enable-match (re-matches #"/admin/plugins/([^/]+)/enable" uri)
          admin-plugin-disable-match (re-matches #"/admin/plugins/([^/]+)/disable" uri)
          admin-skill-update-check-match (re-matches #"/admin/skills/([^/]+)/update-check" uri)
          admin-skill-match  (re-matches #"/admin/skills/([^/]+)" uri)
          llm-call-match (re-matches #"/llm-calls/([0-9a-fA-F-]+)" uri)
          admin-oauth-match  (re-matches #"/admin/oauth-accounts/([^/]+)" uri)
          admin-oauth-connect-match (re-matches #"/admin/oauth-accounts/([^/]+)/connect" uri)
          admin-oauth-refresh-match (re-matches #"/admin/oauth-accounts/([^/]+)/refresh" uri)]
      (cond
        (and (= method :get) (= uri "/"))
        (handle-home req)

        (and (= method :get) (= uri "/local-session"))
        (handle-local-session-bootstrap req)

        (and (= method :get) (static-asset-uri? uri))
        (static-asset-response uri)

        (and (= method :get) (= uri "/__dev/web-reload"))
        (handle-web-dev-reload req)

        (and (= method :get) (= uri "/oauth/callback"))
        (http-admin/handle-oauth-callback (admin-handler-deps) req)

        ;; WebSocket upgrade
        (and (= uri "/ws") (http/websocket-handshake-check req))
        (ws-handler req)

        ;; REST
        (and (= method :post) (= uri "/sessions"))
        (protected-route-response req handle-create-session)

        (and (= method :post) (= uri "/chat"))
        (protected-route-response req #(handle-chat req))

        ;; Machine command channel
        (and (= method :post) (= uri "/command/sessions"))
        (command-route-response req (fn [_req] (handle-create-session :command)))

        (and (= method :post) (= uri "/command/chat"))
        (command-route-response req #(handle-chat % :command))

        (and (= method :post) (= uri "/hooks/slack/events"))
        (http-messaging/handle-slack-events (workspace-handler-deps) req)

        (and (= method :post) (= uri "/hooks/telegram"))
        (http-messaging/handle-telegram-webhook (workspace-handler-deps) req)

        (and (= method :post) (= uri "/command/shutdown"))
        (command-route-response req #(http-command/handle-shutdown (command-handler-deps) %))

        (and (= method :get) command-runtime-status-match)
        (command-route-response req #(http-command/handle-runtime-status (command-handler-deps) %))

        (and (= method :post) command-runtime-drain-match)
        (command-route-response req #(http-command/handle-runtime-drain (command-handler-deps) %))

        (and (= method :post) command-runtime-undrain-match)
        (command-route-response req #(http-command/handle-runtime-undrain (command-handler-deps) %))

        (and (= method :post) command-mcp-match)
        (command-route-response req #(http-command/handle-mcp (command-handler-deps) %))

        (and (= method :post) command-managed-checkpoints-match)
        (command-route-response req #(http-command/handle-create-checkpoint (command-handler-deps) %))

        (and (= method :get) command-managed-checkpoint-match)
        (command-route-response req
                                (fn [_req]
                                  (http-command/handle-get-checkpoint
                                   (command-handler-deps)
                                   (second command-managed-checkpoint-match))))

        (and (= method :get) command-managed-snapshots-match)
        (command-route-response req #(http-command/handle-list-snapshots (command-handler-deps) %))

        (and (= method :post) command-managed-snapshots-match)
        (command-route-response req #(http-command/handle-create-snapshot (command-handler-deps) %))

        (and (= method :get) command-wake-projection-match)
        (command-route-response req #(http-command/handle-wake-projection (command-handler-deps) %))

        (and (= method :delete) command-session-close-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-close-session (second command-session-close-match) :command)))

        (and (= method :get) command-status-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-get-status (second command-status-match) :command)))

        (and (= method :get) command-session-task-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-get-current-task (second command-session-task-match) :command)))

        (and (= method :get) command-session-goal-status-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-get-goal (second command-session-goal-status-match) :command)))

        (and (= method :get) command-session-goal-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-get-goal (second command-session-goal-match) :command)))

        (and (= method :post) command-session-goal-match)
        (command-route-response req
                                #(handle-set-goal (second command-session-goal-match) % :command))

        (and (= method :post) command-session-goal-pause-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-pause-goal (second command-session-goal-pause-match) :command)))

        (and (= method :post) command-session-goal-resume-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-resume-goal (second command-session-goal-resume-match) :command)))

        (or (and (= method :post) command-session-goal-clear-match)
            (and (= method :delete) command-session-goal-match))
        (command-route-response req
                                (fn [_req]
                                  (handle-clear-goal (second (or command-session-goal-clear-match
                                                                  command-session-goal-match))
                                                     :command)))

        (and (= method :get) command-prompt-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-get-prompt (second command-prompt-match) :command)))

        (and (= method :post) command-prompt-match)
        (command-route-response req #(handle-submit-prompt (second command-prompt-match) % :command))

        (and (= method :get) command-approval-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-get-approval (second command-approval-match) :command)))

        (and (= method :post) command-approval-match)
        (command-route-response req #(handle-submit-approval (second command-approval-match) % :command))

        (and (= method :get) command-session-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-session-messages (second command-session-match) :command)))

        (and (= method :get) command-session-audit-match)
        (command-route-response req
                                (fn [_req]
                                  (handle-session-audit (second command-session-audit-match) :command)))

        (and (= method :delete) session-close-match)
        (protected-route-response req #(handle-local-close-session (second session-close-match)))

        (and (= method :get) status-match)
        (protected-route-response req #(handle-local-get-status (second status-match)))

        (and (= method :get) session-task-match)
        (protected-route-response req #(handle-get-current-task (second session-task-match) :http))

        (and (= method :get) session-goal-status-match)
        (protected-route-response req
                                  #(handle-local-goal (second session-goal-status-match)
                                                      handle-get-goal))

        (and (= method :get) session-goal-match)
        (protected-route-response req
                                  #(handle-local-goal (second session-goal-match)
                                                      handle-get-goal))

        (and (= method :post) session-goal-match)
        (protected-route-response req
                                  #(handle-local-goal (second session-goal-match)
                                                      (fn [sid expected-channel]
                                                        (handle-set-goal sid req expected-channel))))

        (and (= method :post) session-goal-pause-match)
        (protected-route-response req
                                  #(handle-local-goal (second session-goal-pause-match)
                                                      handle-pause-goal))

        (and (= method :post) session-goal-resume-match)
        (protected-route-response req
                                  #(handle-local-goal (second session-goal-resume-match)
                                                      handle-resume-goal))

        (or (and (= method :post) session-goal-clear-match)
            (and (= method :delete) session-goal-match))
        (protected-route-response req
                                  #(handle-local-goal (second (or session-goal-clear-match
                                                                  session-goal-match))
                                                      handle-clear-goal))

        (and (= method :get) prompt-match)
        (protected-route-response req #(handle-get-prompt (second prompt-match) :http))

        (and (= method :post) prompt-match)
        (protected-route-response req #(handle-submit-prompt (second prompt-match) req :http))

        (and (= method :get) approval-match)
        (protected-route-response req #(handle-get-approval (second approval-match) :http))

        (and (= method :post) approval-match)
        (protected-route-response req #(handle-submit-approval (second approval-match) req :http))

        (and (= method :get) session-match)
        (protected-route-response req #(handle-session-messages (second session-match) :http))

        (and (= method :get) session-audit-match)
        (protected-route-response req #(handle-session-audit (second session-audit-match) :http))

        (and (= method :get) (= uri "/history/sessions"))
        (protected-route-response req handle-history-sessions)

        (and (= method :get) (= uri "/history/tasks"))
        (protected-route-response req handle-history-tasks)

        (and (= method :get) (= uri "/history/schedules"))
        (protected-route-response req handle-history-schedules)

        (and (= method :get) history-schedule-match)
        (protected-route-response req #(handle-history-schedule-runs (second history-schedule-match)))

        (and (= method :get) task-board-match)
        (protected-route-response req handle-task-board)

        (and (= method :get) task-events-match)
        (protected-route-response req #(handle-get-task-events (second task-events-match)))

        (and (= method :get) task-live-events-match)
        (protected-route-response req #(handle-get-live-task-events (second task-live-events-match) req))

        (and (= method :get) task-stream-match)
        (protected-route-response req #(handle-get-task-event-stream (second task-stream-match) req))

        (and (= method :get) task-prompt-match)
        (protected-route-response req #(handle-get-task-prompt (second task-prompt-match)))

        (and (= method :post) task-prompt-match)
        (protected-route-response req #(handle-submit-task-prompt (second task-prompt-match) req))

        (and (= method :get) task-approval-match)
        (protected-route-response req #(handle-get-task-approval (second task-approval-match)))

        (and (= method :post) task-approval-match)
        (protected-route-response req #(handle-submit-task-approval (second task-approval-match) req))

        (and (= method :get) task-match)
        (protected-route-response req #(handle-get-task (second task-match)))

        (and (= method :post) task-pause-match)
        (protected-route-response req #(handle-pause-task (second task-pause-match)))

        (and (= method :post) task-stop-match)
        (protected-route-response req #(handle-stop-task (second task-stop-match)))

        (and (= method :post) task-interrupt-match)
        (protected-route-response req #(handle-interrupt-task (second task-interrupt-match)))

        (and (= method :post) task-steer-match)
        (protected-route-response req #(handle-steer-task (second task-steer-match) req))

        (and (= method :post) task-fork-match)
        (protected-route-response req #(handle-fork-task (second task-fork-match) req))

        (and (= method :post) task-resume-match)
        (protected-route-response req #(handle-resume-task (second task-resume-match) req))

        (and (= method :get) (= uri "/llm-calls"))
        (protected-route-response req #(handle-list-llm-calls req))

        (and (= method :get) llm-call-match)
        (protected-route-response req #(handle-get-llm-call (second llm-call-match)))

        (and (= method :get) scratch-list-match)
        (protected-route-response req #(handle-list-scratch-pads (second scratch-list-match)))

        (and (= method :post) scratch-list-match)
        (protected-route-response req #(handle-create-scratch-pad (second scratch-list-match) req))

        (and (= method :get) scratch-pad-match)
        (protected-route-response req #(handle-get-scratch-pad (second scratch-pad-match)
                                                               (nth scratch-pad-match 2)))

        (and (= method :put) scratch-pad-match)
        (protected-route-response req #(handle-save-scratch-pad (second scratch-pad-match)
                                                                (nth scratch-pad-match 2)
                                                                req))

        (and (= method :delete) scratch-pad-match)
        (protected-route-response req #(handle-delete-scratch-pad (second scratch-pad-match)
                                                                  (nth scratch-pad-match 2)))

        (and (= method :post) scratch-edit-match)
        (protected-route-response req #(handle-edit-scratch-pad (second scratch-edit-match)
                                                                (nth scratch-edit-match 2)
                                                                req))

        (and (= method :get) local-doc-list-match)
        (protected-route-response req #(handle-list-local-docs (second local-doc-list-match)))

        (and (= method :post) local-doc-list-match)
        (protected-route-response req #(handle-create-local-docs (second local-doc-list-match) req))

        (and (= method :get) local-doc-match)
        (protected-route-response req #(handle-get-local-doc (second local-doc-match)
                                                             (nth local-doc-match 2)))

        (and (= method :delete) local-doc-match)
        (protected-route-response req #(handle-delete-local-doc (second local-doc-match)
                                                                (nth local-doc-match 2)))

        (and (= method :post) local-doc-scratch-match)
        (protected-route-response req #(handle-create-local-doc-scratch-pad (second local-doc-scratch-match)
                                                                            (nth local-doc-scratch-match 2)))

        (and (= method :get) artifact-list-match)
        (protected-route-response req #(handle-list-artifacts (second artifact-list-match)))

        (and (= method :post) artifact-list-match)
        (protected-route-response req #(handle-create-artifact (second artifact-list-match) req))

        (and (= method :get) artifact-match)
        (protected-route-response req #(handle-get-artifact (second artifact-match)
                                                            (nth artifact-match 2)))

        (and (= method :get) artifact-download-match)
        (protected-route-response req #(handle-download-artifact (second artifact-download-match)
                                                                 (nth artifact-download-match 2)))

        (and (= method :post) artifact-scratch-match)
        (protected-route-response req #(handle-create-artifact-scratch-pad (second artifact-scratch-match)
                                                                           (nth artifact-scratch-match 2)))

        (and (= method :delete) artifact-match)
        (protected-route-response req #(handle-delete-artifact (second artifact-match)
                                                               (nth artifact-match 2)))

        (and (= method :get) workspace-list-match)
        (protected-route-response req #(handle-list-workspace-items req))

        (and (= method :get) workspace-download-match)
        (protected-route-response req #(handle-download-workspace-item
                                         (second workspace-download-match)
                                         req))

        (and (= method :get) (= uri "/knowledge/nodes"))
        (protected-route-response req #(handle-search-knowledge-nodes req))

        (and (= method :get) knowledge-node-facts-match)
        (protected-route-response req #(handle-list-knowledge-node-facts
                                         (second knowledge-node-facts-match)))

        (and (= method :delete) knowledge-fact-match)
        (protected-route-response req #(handle-delete-knowledge-fact
                                         (second knowledge-fact-match)))

        (and (= method :get) (= uri "/admin/config"))
        (protected-route-response req #(http-admin/handle-admin-config (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/runtime-overlay/reload"))
        (protected-route-response req #(http-admin/handle-reload-runtime-overlay (admin-handler-deps) req))

        (and (= method :get) (= uri "/admin/managed-instances"))
        (protected-route-response req #(http-admin/handle-list-managed-instances (admin-handler-deps) req))

        (and (= method :post) admin-managed-instance-stop-match)
        (protected-route-response req #(http-admin/handle-stop-managed-instance (admin-handler-deps)
                                                                                (second admin-managed-instance-stop-match)))

        (and (= method :post) (= uri "/admin/providers"))
        (protected-route-response req #(http-admin/handle-save-provider (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/provider-models"))
        (protected-route-response req #(http-admin/handle-fetch-provider-models (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/provider-model-metadata"))
        (protected-route-response req #(http-admin/handle-fetch-provider-model-metadata (admin-handler-deps) req))

        (and (= method :delete) (= uri "/admin/providers"))
        (protected-route-response req #(http-admin/handle-delete-provider (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/memory-retention"))
        (protected-route-response req #(http-admin/handle-save-memory-retention (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/identity"))
        (protected-route-response req #(http-admin/handle-save-identity (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/web-search"))
        (protected-route-response req #(http-admin/handle-save-web-search (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/context"))
        (protected-route-response req #(http-admin/handle-save-conversation-context (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/knowledge-decay"))
        (protected-route-response req #(http-admin/handle-save-knowledge-decay (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/local-doc-summarization"))
        (protected-route-response req #(http-admin/handle-save-local-doc-summarization (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/local-doc-ocr"))
        (protected-route-response req #(http-admin/handle-save-local-doc-ocr (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/database-backup"))
        (protected-route-response req #(http-admin/handle-save-database-backup (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/messaging"))
        (protected-route-response req #(http-admin/handle-save-messaging (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/oauth-accounts"))
        (protected-route-response req #(http-admin/handle-save-oauth-account (admin-handler-deps) req))

        (and (= method :post) admin-oauth-connect-match)
        (protected-route-response req #(http-admin/handle-start-oauth-connect (admin-handler-deps)
                                                                              (second admin-oauth-connect-match)
                                                                              req))

        (and (= method :post) admin-oauth-refresh-match)
        (protected-route-response req #(http-admin/handle-refresh-oauth-account (admin-handler-deps)
                                                                                (second admin-oauth-refresh-match)))

        (and (= method :delete) admin-oauth-match)
        (protected-route-response req #(http-admin/handle-delete-oauth-account (admin-handler-deps)
                                                                               (second admin-oauth-match)))

        (and (= method :post) (= uri "/admin/services"))
        (protected-route-response req #(http-admin/handle-save-service (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/sites"))
        (protected-route-response req #(http-admin/handle-save-site (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/schedules"))
        (protected-route-response req #(http-admin/handle-save-schedule (admin-handler-deps) req))

        (and (= method :post) admin-schedule-pause-match)
        (protected-route-response req #(http-admin/handle-pause-schedule (admin-handler-deps)
                                                                         (second admin-schedule-pause-match)))

        (and (= method :post) admin-schedule-resume-match)
        (protected-route-response req #(http-admin/handle-resume-schedule (admin-handler-deps)
                                                                          (second admin-schedule-resume-match)))

        (and (= method :post) (= uri "/admin/skills"))
        (protected-route-response req #(http-admin/handle-save-skill (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/skills/import-openclaw"))
        (protected-route-response req #(http-admin/handle-import-openclaw-skill (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/skills/curate"))
        (protected-route-response req #(http-admin/handle-curate-skills (admin-handler-deps) req))

        (and (= method :post) (= uri "/admin/plugins"))
        (protected-route-response req #(http-admin/handle-save-plugin (admin-handler-deps) req))

        (and (= method :post) admin-plugin-enable-match)
        (protected-route-response req #(http-admin/handle-enable-plugin
                                         (admin-handler-deps)
                                         (second admin-plugin-enable-match)
                                         true))

        (and (= method :post) admin-plugin-disable-match)
        (protected-route-response req #(http-admin/handle-enable-plugin
                                         (admin-handler-deps)
                                         (second admin-plugin-disable-match)
                                         false))

        (and (= method :post) admin-skill-update-check-match)
        (protected-route-response req #(http-admin/handle-check-skill-update
                                         (admin-handler-deps)
                                         (second admin-skill-update-check-match)))

        (and (= method :delete) admin-site-match)
        (protected-route-response req #(http-admin/handle-delete-site (admin-handler-deps)
                                                                      (second admin-site-match)))

        (and (= method :delete) admin-schedule-match)
        (protected-route-response req #(http-admin/handle-delete-schedule (admin-handler-deps)
                                                                          (second admin-schedule-match)))

        (and (= method :get) admin-skill-match)
        (protected-route-response req #(http-admin/handle-get-skill (admin-handler-deps)
                                                                    (second admin-skill-match)))

        (and (= method :delete) admin-skill-match)
        (protected-route-response req #(http-admin/handle-delete-skill (admin-handler-deps)
                                                                       (second admin-skill-match)))

        (and (= method :get) (= uri "/skills"))
        (protected-route-response req #(http-admin/handle-skills (admin-handler-deps) req))

        (and (= method :get) (= uri "/health"))
        (handle-health req)

        :else
        (json-response 404 {:error "not found"})))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(def ^:private multipart-router
  (multipart/wrap-multipart-params router*))

(defn- router
  [req]
  (try
    (multipart-router req)
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))
    (catch Exception e
      (exception-response e))))

;; ---------------------------------------------------------------------------
;; Server lifecycle
;; ---------------------------------------------------------------------------

(defn current-port
  []
  (some-> (maybe-server-atom) deref :port))

(defn- port-bind-conflict?
  [^Throwable error]
  (boolean
    (some (fn [^Throwable cause]
            (or (instance? BindException cause)
                (str/includes? (str/lower-case (or (.getMessage cause) ""))
                               "address already in use")))
          (take-while some? (iterate #(some-> ^Throwable % .getCause) error)))))

(defn- start-server-with-port-fallback
  [bind-host requested-port]
  (loop [port (int requested-port)
         attempts 0]
    (let [result (try
                   {:stop-fn (http/run-server router {:ip bind-host :port port})
                    :port port}
                   (catch Exception e
                     (if (port-bind-conflict? e)
                       {:retry? true :error e}
                       (throw e))))]
      (if (:retry? result)
        (if (< attempts http-port-search-limit)
          (do
            (log/warn "HTTP/WebSocket port" port "is unavailable on" bind-host ", trying" (inc port))
            (recur (inc port) (inc attempts)))
          (throw (ex-info "Could not find an available HTTP/WebSocket port"
                          {:bind-host bind-host
                           :requested-port requested-port
                           :attempted-port-start requested-port
                           :attempted-port-end port}
                          (:error result))))
        result))))

(defn start!
  "Start the HTTP/WebSocket server.
   Defaults to loopback-only binding."
  ([port]
   (start! "127.0.0.1" port nil))
  ([bind-host port]
   (start! bind-host port nil))
  ([bind-host port {:keys [web-dev?] :or {web-dev? false}}]
   (when-let [{:keys [bind-host port]} @(server-atom)]
     (throw (ex-info "HTTP/WebSocket server already running"
                     {:bind-host bind-host
                      :port port})))
   (configure-web-dev! web-dev?)
   (bridge/register-channel-adapter! :http
                                     {:prompt http-interaction/prompt-handler
                                      :approval http-interaction/approval-handler
                                      :status http-status-handler
                                      :runtime-event http-runtime-event-handler})
   (bridge/register-channel-adapter! :command
                                     {:prompt http-interaction/prompt-handler
                                      :approval http-interaction/approval-handler
                                      :status http-status-handler
                                      :runtime-event http-runtime-event-handler})
   (bridge/register-channel-adapter! :websocket
                                     {:approval http-interaction/approval-handler
                                      :status http-status-handler
                                      :runtime-event http-runtime-event-handler})
   (let [^ScheduledExecutorService finalizer-exec
         (Executors/newSingleThreadScheduledExecutor)
         {:keys [stop-fn port]} (start-server-with-port-fallback bind-host port)]
     (reset! (rest-session-finalizer-executor-atom) finalizer-exec)
     (reset! (server-atom) {:stop-fn stop-fn
                            :bind-host bind-host
                            :port port})
     (log/info "HTTP/WebSocket server started on" bind-host ":" port)
     stop-fn)))

(defn stop! []
  (when-let [{:keys [stop-fn]} @(server-atom)]
    (doseq [{:keys [id channel active?]} (db/list-sessions {:include-workers? true})
            :when (and (contains? rest-session-channels channel) active?)]
      (finalize-rest-session! id :server-stop))
    (stop-fn) ; http-kit stop fn
    (when-let [^ScheduledExecutorService exec @(rest-session-finalizer-executor-atom)]
      (clear-rest-session-finalizers!)
      (.shutdown exec)
      (try
        (.awaitTermination exec 5 TimeUnit/SECONDS)
        (catch InterruptedException _
          (.shutdownNow exec)))
      (reset! (rest-session-finalizer-executor-atom) nil))
    (bridge/clear-channel-adapter! :http)
    (bridge/clear-channel-adapter! :command)
    (bridge/clear-channel-adapter! :websocket)
    (reset! (websocket-receive-failures-atom) {})
    (reset! (session-statuses-atom) {})
    (reset! (task-runtime-events-atom) {})
    (reset-runtime-ingress-rate-limits! (current-runtime))
    (reset-runtime-command-auth! (current-runtime))
    (reset-runtime-managed-proxy-auth! (current-runtime))
    (clear-command-shutdown-handler!)
    (configure-web-dev! false)
    (reset! (server-atom) nil)
    (log/info "Server stopped")))

(defn install-runtime!
  ([] (install-runtime! (make-runtime)))
  ([runtime]
   (when-let [current (maybe-current-runtime)]
     (when-not (identical? current runtime)
       (clear-runtime!)))
   (reset! installed-runtime-atom runtime)
   runtime))

(defn clear-runtime!
  []
  (when-let [runtime (maybe-current-runtime)]
    (when (some-> (:server-atom runtime) deref some?)
      (stop!))
    (when-let [^ScheduledExecutorService exec @(:rest-session-finalizer-executor-atom runtime)]
      (clear-rest-session-finalizers!)
      (.shutdown exec)
      (try
        (.awaitTermination exec 5 TimeUnit/SECONDS)
        (catch InterruptedException _
          (.shutdownNow exec))))
    (reset! (:server-atom runtime) nil)
    (reset! (:ws-sessions-atom runtime) {})
    (reset! (:websocket-receive-failures-atom runtime) {})
    (reset! (:session-statuses-atom runtime) {})
    (reset! (:task-runtime-events-atom runtime) {})
    (reset! (:task-runtime-stream-subscribers-atom runtime) {})
    (reset! (:web-dev-state-atom runtime) {:enabled? false
                                           :root nil})
    (reset! (:command-shutdown-handler-atom runtime) nil)
    (reset-runtime-ingress-rate-limits! runtime)
    (reset-runtime-command-auth! runtime)
    (reset-runtime-managed-proxy-auth! runtime)
    (reset! (:rest-session-finalizer-executor-atom runtime) nil)
    (reset! (:rest-session-finalizers-atom runtime) {})
    (reset! installed-runtime-atom nil))
  nil)
