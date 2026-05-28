(ns xia.channel.http
  "HTTP/WebSocket channel — enables remote clients and web UIs."
  (:require [clojure.string :as str]
            [org.httpkit.server :as http]
            [ring.middleware.multipart-params :as multipart]
            [taoensso.timbre :as log]
            [xia.bridge :as bridge]
            [xia.db :as db]
            [xia.channel.http.assets :as http-assets]
            [xia.channel.http.auth :as http-auth]
            [xia.channel.http.interaction :as http-interaction]
            [xia.channel.http.knowledge :as http-knowledge]
            [xia.channel.http.request :as http-request]
            [xia.channel.http.response :as http-response]
            [xia.channel.http.routes :as http-routes]
            [xia.channel.http.session :as http-session]
            [xia.channel.http.session-lifecycle :as http-session-life]
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
    [java.util.concurrent ConcurrentHashMap Executors ScheduledExecutorService TimeUnit]
    [java.util.concurrent.atomic AtomicLong]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private installed-runtime-atom (atom nil))

(def ^:private session-finalize-lock-count session-life/default-finalize-lock-count)
(def ^:private http-port-search-limit 100)
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

(declare clear-session-status!)

(defn- session-lifecycle-deps
  []
  {:clear-session-status! clear-session-status!
   :rest-session-finalizer-executor-atom (rest-session-finalizer-executor-atom)
   :rest-session-finalizers-atom (rest-session-finalizers-atom)
   :session-finalize-locks (session-finalize-locks)
   :session-statuses-atom (session-statuses-atom)})

(defn- parse-session-id
  [session-id]
  (http-session-life/parse-session-id session-id))

(defn- session-exists?
  [session-id]
  (http-session-life/session-exists? session-id))

(defn- session-id-str
  [session-id]
  (http-session-life/session-id-str session-id))

(defn- session-channel
  [session-id]
  (http-session-life/session-channel session-id))

(defn- session-accessible?
  [session-id expected-channel]
  (http-session-life/session-accessible? session-id expected-channel))

(defn- session-active?
  [session-id]
  (http-session-life/active? session-id))

(defn- maybe-resume-http-session!
  [session-id expected-channel]
  (http-session-life/maybe-resume! (session-lifecycle-deps) session-id expected-channel))

(defn- session-busy?
  [session-id]
  (http-session-life/session-busy? (session-lifecycle-deps) session-id))

(defn- cancel-rest-session-finalizer!
  [session-id]
  (http-session-life/cancel-finalizer! (session-lifecycle-deps) session-id))

(defn- clear-rest-session-finalizers!
  []
  (http-session-life/clear-finalizers! (session-lifecycle-deps)))

(defn- touch-rest-session!
  [session-id]
  (http-session-life/touch! (session-lifecycle-deps) session-id))

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
   (http-session-life/finalize! (session-lifecycle-deps) session-id reason)))

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
  (http-session-life/local-ui-session-allowed? session-id))

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

(defn- route-deps
  []
  {:admin-handler-deps admin-handler-deps
   :command-handler-deps command-handler-deps
   :command-route-response command-route-response
   :exception-response exception-response
   :handle-chat handle-chat
   :handle-clear-goal handle-clear-goal
   :handle-close-session handle-close-session
   :handle-create-artifact handle-create-artifact
   :handle-create-artifact-scratch-pad handle-create-artifact-scratch-pad
   :handle-create-local-doc-scratch-pad handle-create-local-doc-scratch-pad
   :handle-create-local-docs handle-create-local-docs
   :handle-create-scratch-pad handle-create-scratch-pad
   :handle-create-session handle-create-session
   :handle-delete-artifact handle-delete-artifact
   :handle-delete-knowledge-fact handle-delete-knowledge-fact
   :handle-delete-local-doc handle-delete-local-doc
   :handle-delete-scratch-pad handle-delete-scratch-pad
   :handle-download-artifact handle-download-artifact
   :handle-download-workspace-item handle-download-workspace-item
   :handle-edit-scratch-pad handle-edit-scratch-pad
   :handle-fork-task handle-fork-task
   :handle-get-approval handle-get-approval
   :handle-get-artifact handle-get-artifact
   :handle-get-current-task handle-get-current-task
   :handle-get-goal handle-get-goal
   :handle-get-live-task-events handle-get-live-task-events
   :handle-get-llm-call handle-get-llm-call
   :handle-get-local-doc handle-get-local-doc
   :handle-get-prompt handle-get-prompt
   :handle-get-scratch-pad handle-get-scratch-pad
   :handle-get-status handle-get-status
   :handle-get-task handle-get-task
   :handle-get-task-approval handle-get-task-approval
   :handle-get-task-event-stream handle-get-task-event-stream
   :handle-get-task-events handle-get-task-events
   :handle-get-task-prompt handle-get-task-prompt
   :handle-health handle-health
   :handle-history-schedule-runs handle-history-schedule-runs
   :handle-history-schedules handle-history-schedules
   :handle-history-sessions handle-history-sessions
   :handle-history-tasks handle-history-tasks
   :handle-home handle-home
   :handle-interrupt-task handle-interrupt-task
   :handle-list-artifacts handle-list-artifacts
   :handle-list-knowledge-node-facts handle-list-knowledge-node-facts
   :handle-list-llm-calls handle-list-llm-calls
   :handle-list-local-docs handle-list-local-docs
   :handle-list-scratch-pads handle-list-scratch-pads
   :handle-list-workspace-items handle-list-workspace-items
   :handle-local-close-session handle-local-close-session
   :handle-local-get-status handle-local-get-status
   :handle-local-goal handle-local-goal
   :handle-local-session-bootstrap handle-local-session-bootstrap
   :handle-pause-goal handle-pause-goal
   :handle-pause-task handle-pause-task
   :handle-resume-goal handle-resume-goal
   :handle-resume-task handle-resume-task
   :handle-save-scratch-pad handle-save-scratch-pad
   :handle-search-knowledge-nodes handle-search-knowledge-nodes
   :handle-session-audit handle-session-audit
   :handle-session-messages handle-session-messages
   :handle-set-goal handle-set-goal
   :handle-steer-task handle-steer-task
   :handle-stop-task handle-stop-task
   :handle-submit-approval handle-submit-approval
   :handle-submit-prompt handle-submit-prompt
   :handle-submit-task-approval handle-submit-task-approval
   :handle-submit-task-prompt handle-submit-task-prompt
   :handle-task-board handle-task-board
   :handle-web-dev-reload handle-web-dev-reload
   :json-response json-response
   :protected-route-response protected-route-response
   :static-asset-response static-asset-response
   :static-asset-uri? static-asset-uri?
   :websocket-handshake? #(http/websocket-handshake-check %)
   :workspace-handler-deps workspace-handler-deps
   :ws-handler ws-handler})

;; ---------------------------------------------------------------------------
;; Router
;; ---------------------------------------------------------------------------

(defn- router* [req]
  (http-routes/route (route-deps) req))

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
            :when (and (http-session-life/rest-session-channel? channel) active?)]
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
