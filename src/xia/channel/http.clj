(ns xia.channel.http
  "HTTP/WebSocket channel — enables remote clients and web UIs."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [org.httpkit.server :as http]
            [charred.api :as json]
            [ring.middleware.multipart-params :as multipart]
            [taoensso.timbre :as log]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.hippocampus :as hippo]
            [xia.memory :as memory]
            [xia.channel.http.admin :as http-admin]
            [xia.channel.http.messaging :as http-messaging]
            [xia.channel.http.session :as http-session]
            [xia.channel.http.workspace :as http-workspace]
            [xia.channel.messaging :as messaging]
            [xia.checkpoint :as checkpoint]
            [xia.runtime-health :as runtime-health]
            [xia.runtime-state :as runtime-state]
            [xia.agent :as agent]
            [xia.prompt :as prompt]
            [xia.util :as util]
            [xia.wake-projection :as wake-projection]
            [xia.working-memory :as wm])
  (:import [java.io ByteArrayOutputStream InputStream]
    [java.net BindException]
    [java.nio.charset StandardCharsets]
    [java.nio.file Files LinkOption Path Paths]
    [java.security SecureRandom]
    [java.util Base64 Date]
    [java.util.concurrent Executors ScheduledExecutorService ScheduledFuture TimeUnit]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private installed-runtime-atom (atom nil))

(def ^:private local-hosts #{"localhost" "127.0.0.1" "::1" "[::1]"})
(def ^:private approval-timeout-ms (* 5 60 1000))
(def ^:private local-session-cookie-name "xia-local-session")
(def ^:private command-channel-token-config-key :secret/command-channel-token)
(def ^:private max-request-body-bytes (* 16 1024 1024)) ; 16 MiB
(def ^:private default-rest-session-idle-timeout-ms (* 30 60 1000))
(def ^:private max-live-task-runtime-events 200)
(def ^:private websocket-receive-retry-delay-ms 5000)
(def ^:private session-finalize-lock-count 256)
(def ^:private http-port-search-limit 100)
(def ^:private rest-session-channels #{:http :command})
(def ^:private local-ui-session-channels #{:http :websocket})
(def ^:private busy-session-states #{:running :waiting_input :waiting_approval})
(def ^:private byte-array-class (class (byte-array 0)))
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
   :rest-session-finalizer-executor-atom (atom nil)
   :rest-session-finalizers-atom        (atom {})
   :session-finalize-locks              (vec (repeatedly session-finalize-lock-count #(Object.)))})

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

;; ---------------------------------------------------------------------------
;; Local web UI
;; ---------------------------------------------------------------------------

(def ^:private read-bundled-resource
  (memoize
    (fn [path]
      (some-> (str "web/" path)
              io/resource
              slurp))))

(def ^:private read-bundled-resource-bytes
  (memoize
    (fn [path]
      (when-let [resource (some-> (str "web/" path) io/resource)]
        (with-open [in (io/input-stream resource)
                    out (ByteArrayOutputStream.)]
          (io/copy in out)
          (.toByteArray out))))))

(def ^:private web-dev-poll-interval-ms 1000)

(def ^:private web-dev-no-cache-headers
  {"Cache-Control" "no-store, no-cache, must-revalidate, max-age=0"
   "Pragma" "no-cache"
   "Expires" "0"})

(defn- web-dev-enabled?
  []
  (true? (:enabled? @(web-dev-state-atom))))

(defn- resolve-web-dev-root
  []
  (try
    (when-let [resource (io/resource "web/index.html")]
      (when (= "file" (.getProtocol resource))
        (.getParent (Paths/get (.toURI resource)))))
    (catch Exception _
      nil)))

(defn- configure-web-dev!
  [enabled?]
  (if-not enabled?
    (reset! (web-dev-state-atom) {:enabled? false
                                  :root nil})
    (if-let [root (resolve-web-dev-root)]
      (do
        (reset! (web-dev-state-atom) {:enabled? true
                                      :root root})
        (log/info "Web dev mode enabled; serving live web assets from" (str root)))
      (do
        (reset! (web-dev-state-atom) {:enabled? false
                                      :root nil})
        (log/warn "Web dev mode requested, but web resources are not file-backed; falling back to bundled assets")))))

(defn- web-dev-root
  []
  (:root @(web-dev-state-atom)))

(defn- web-dev-path
  ^Path [path]
  (when-let [^Path root (web-dev-root)]
    (.normalize (.resolve root ^String path))))

(defn- read-web-dev-resource
  [path]
  (when-let [^Path p (web-dev-path path)]
    (when (Files/isRegularFile p (make-array LinkOption 0))
      (slurp (.toFile p)))))

(defn- read-web-dev-resource-bytes
  [path]
  (when-let [^Path p (web-dev-path path)]
    (when (Files/isRegularFile p (make-array LinkOption 0))
      (Files/readAllBytes p))))

(defn- read-resource
  [path]
  (if (web-dev-enabled?)
    (or (read-web-dev-resource path)
        (read-bundled-resource path))
    (read-bundled-resource path)))

(defn- read-resource-bytes
  [path]
  (if (web-dev-enabled?)
    (or (read-web-dev-resource-bytes path)
        (read-bundled-resource-bytes path))
    (read-bundled-resource-bytes path)))

(defn- with-web-dev-headers
  [response]
  (if (web-dev-enabled?)
    (update response :headers #(merge web-dev-no-cache-headers (or % {})))
    response))

(defn- web-dev-version
  []
  (when-let [^Path root (web-dev-root)]
    (try
      (with-open [stream (Files/walk root (make-array java.nio.file.FileVisitOption 0))]
        (let [{:keys [file-count max-modified total-size]}
              (reduce (fn [{:keys [file-count max-modified total-size]} ^Path path]
                        (if (Files/isRegularFile path (make-array LinkOption 0))
                          {:file-count   (unchecked-inc-int (int file-count))
                           :max-modified (max (long max-modified)
                                              (.toMillis (Files/getLastModifiedTime path
                                                                                    (make-array LinkOption 0))))
                           :total-size   (+ (long total-size) (Files/size path))}
                          {:file-count file-count
                           :max-modified max-modified
                           :total-size total-size}))
                      {:file-count 0
                       :max-modified 0
                       :total-size 0}
                      (iterator-seq (.iterator stream)))]
          (str file-count ":" max-modified ":" total-size)))
      (catch Exception e
        (log/debug e "Failed to compute web dev version")
        nil))))

(defn- inject-web-dev-client
  [html]
  (if-not (web-dev-enabled?)
    html
    (let [version (or (web-dev-version) "0")
          script  (str "<script>"
                       "(function(){"
                       "var currentVersion=" (pr-str version) ";"
                       "async function poll(){"
                       "try{"
                       "var response=await fetch('/__dev/web-reload',{cache:'no-store'});"
                       "if(!response.ok){return;}"
                       "var payload=await response.json();"
                       "if(payload && payload.version && payload.version!==currentVersion){"
                       "window.location.reload();"
                       "return;}"
                       "if(payload && payload.version){currentVersion=payload.version;}"
                       "}catch(_err){}"
                       "}"
                       "window.setInterval(function(){"
                       "if(document.visibilityState!=='hidden'){poll();}"
                       "},"
                       web-dev-poll-interval-ms
                       ");"
                       "})();"
                       "</script>")]
      (if (str/includes? html "</body>")
        (str/replace html "</body>" (str script "</body>"))
        (str html script)))))

(defn- resource-response [path content-type]
  (if-let [content (read-resource path)]
    (with-web-dev-headers
      {:status  200
       :headers {"Content-Type" content-type}
       :body    content})
    {:status 404 :body "Not Found"}))

(defn- binary-resource-response [path content-type]
  (if-let [content (read-resource-bytes path)]
    (with-web-dev-headers
      {:status  200
       :headers {"Content-Type" content-type}
       :body    content})
    {:status 404 :body "Not Found"}))

(defn- throwable-message
  [^Throwable e]
  (.getMessage e))

(defn- instant->str
  [value]
  (cond
    (instance? Date value) (str (.toInstant ^Date value))
    (instance? java.time.Instant value) (str value)
    :else nil))

(defn- parse-iso-instant
  [value field]
  (when-let [text (some-> value str str/trim not-empty)]
    (try
      (Date/from (java.time.Instant/parse text))
      (catch Exception _
        (throw (ex-info (str "invalid '" field "' field")
                        {:field field}))))))

(defn- date->millis
  [value]
  (when (instance? Date value)
    (.getTime ^Date value)))

(def ^:private web-static-assets
  {"/style.css"                     {:path "style.css" :content-type "text/css"}
   "/app.js"                        {:path "app.js" :content-type "text/javascript"}
   "/favicon.ico"                   {:path "favicon/favicon.ico" :content-type "image/x-icon" :binary? true}
   "/favicon/favicon.svg"           {:path "favicon/favicon.svg" :content-type "image/svg+xml"}
   "/favicon/favicon-96x96.png"     {:path "favicon/favicon-96x96.png" :content-type "image/png" :binary? true}
   "/favicon/apple-touch-icon.png"  {:path "favicon/apple-touch-icon.png" :content-type "image/png" :binary? true}
   "/favicon/web-app-manifest-192x192.png" {:path "favicon/web-app-manifest-192x192.png" :content-type "image/png" :binary? true}
   "/favicon/web-app-manifest-512x512.png" {:path "favicon/web-app-manifest-512x512.png" :content-type "image/png" :binary? true}
   "/favicon/site.webmanifest"      {:path "favicon/site.webmanifest"
                                     :content-type "application/manifest+json; charset=utf-8"}})

(defn- static-asset-response
  [uri]
  (when-let [{:keys [path content-type binary?]} (get web-static-assets uri)]
    (if binary?
      (binary-resource-response path content-type)
      (resource-response path content-type))))

;; ---------------------------------------------------------------------------
;; WebSocket handler
;; ---------------------------------------------------------------------------

(declare protected-route-response with-session-finalize-lock touch-rest-session!)

(defn- record-session-finalization-failure!
  [session-id channel step ^Throwable e]
  (let [sid-str (str session-id)]
    (swap! (session-statuses-atom) assoc
           sid-str
           {:session-id         sid-str
            :state              :error
            :phase              :finalizing
            :message            (str "Failed to finalize "
                                     (name channel)
                                     " session; working memory preserved for retry.")
            :error              (throwable-message e)
            :finalization-step  step
            :updated-at         (java.util.Date.)})
    (log/error e
               "Failed to finalize session; preserving working memory for retry"
               sid-str
               "channel" (name channel)
               "step" (name step))))

(defn- clear-websocket-receive-failure!
  [session-id]
  (swap! (websocket-receive-failures-atom) dissoc (str session-id)))

(defn- active-websocket-receive-failure
  [session-id]
  (let [sid-str  (str session-id)
        failure  (get @(websocket-receive-failures-atom) sid-str)
        now-ms   (System/currentTimeMillis)
        retry-ms (long (or (:retry-not-before-ms failure) 0))]
    (cond
      (nil? failure)
      nil

      (<= retry-ms now-ms)
      (do
        (clear-websocket-receive-failure! sid-str)
        nil)

      :else
      (assoc failure :retry-after-ms (- retry-ms now-ms)))))

(defn- send-websocket-json!
  [ch payload]
  (try
    (http/send! ch (json/write-json-str payload))
    (catch Exception e
      (log/warn e "Failed to send WebSocket response"))))

(defn- send-websocket-error!
  [ch error-message & {:as extra}]
  (send-websocket-json! ch
                        (merge {:type  "error"
                                :error (or (some-> error-message str/trim not-empty)
                                           "WebSocket request failed")}
                               extra)))

(defn- record-websocket-receive-failure!
  [session-id ^Throwable e]
  (let [sid-str             (str session-id)
        retry-not-before-ms (+ (System/currentTimeMillis)
                               websocket-receive-retry-delay-ms)
        failure             {:session-id          sid-str
                             :error               (or (some-> (throwable-message e) str/trim not-empty)
                                                      "WebSocket request failed")
                             :failed-at           (java.util.Date.)
                             :retry-not-before-ms retry-not-before-ms}]
    (swap! (websocket-receive-failures-atom) assoc sid-str failure)
    failure))

(defn- handle-websocket-receive-failure!
  [ch session-id ^Throwable e]
  (try
    (agent/cancel-session! session-id "websocket request failed")
    (catch Exception cancel-error
      (log/warn cancel-error
                "Failed to cancel WebSocket session after receive error"
                (str session-id))))
  (let [{:keys [error]} (record-websocket-receive-failure! session-id e)]
    (log/error e "WebSocket message error; temporarily blocking retries" (str session-id))
    (send-websocket-error! ch
                           error
                           :retry_after_ms websocket-receive-retry-delay-ms)))

(defn- finalize-websocket-session!
  [ch]
  (when-let [sid (get @(ws-sessions-atom) ch)]
    (clear-websocket-receive-failure! sid)
    (let [topics-or-failure
          (try
            (:topics (wm/get-wm sid))
            (catch Exception e
              (record-session-finalization-failure! sid :websocket :load-working-memory e)
              ::finalization-failed))]
      (when-not (= ::finalization-failed topics-or-failure)
        (let [finalized?
              (try
                (wm/clear-autonomy-state! sid)
                (wm/snapshot! sid)
                (hippo/record-conversation! sid :websocket
                                            :topics topics-or-failure
                                            :consolidation-mode :sync)
                true
                (catch Exception e
                  (record-session-finalization-failure! sid :websocket :persist-session e)
                  false))]
          (when finalized?
            (swap! (session-statuses-atom) dissoc (str sid))
            (try
              (wm/clear-wm! sid)
              (catch Exception e
                (log/error e "Failed to clear WebSocket working memory"))))))))
  (swap! (ws-sessions-atom) dissoc ch)
  (log/info "WebSocket disconnected"))

(defn- ws-handler [req]
  (protected-route-response
    req
    #(http/as-channel req
       {:on-open
        (fn [ch]
          (let [sid (db/create-session! :websocket)]
            (swap! (ws-sessions-atom) assoc ch sid)
            (wm/ensure-wm! sid)
            (log/info "WebSocket connected, session:" sid)
            (send-websocket-json! ch {:type "connected" :session-id (str sid)})))

        :on-receive
        (fn [ch msg]
          (if-let [sid (get @(ws-sessions-atom) ch)]
            (if-let [{:keys [retry-after-ms]} (active-websocket-receive-failure sid)]
              (send-websocket-error! ch
                                     "Previous WebSocket request failed; wait before retrying."
                                     :retry_after_ms retry-after-ms)
              (let [data (try
                           (json/read-json msg)
                           (catch Exception e
                             (send-websocket-error! ch (throwable-message e))
                             ::invalid-message))]
                (when-not (= ::invalid-message data)
                  (try
                    (let [text     (get data "message" (get data "content" msg))
                          response (agent/process-message sid text :channel :websocket)]
                      (clear-websocket-receive-failure! sid)
                      (send-websocket-json! ch {:type    "message"
                                                :role    "assistant"
                                                :content response}))
                    (catch Throwable t
                      (handle-websocket-receive-failure! ch sid t)
                      (when (instance? Error t)
                        (throw t)))))))
            (send-websocket-error! ch "Session not found")))

        :on-close
        (fn [ch _status]
          (finalize-websocket-session! ch))})))

;; ---------------------------------------------------------------------------
;; REST endpoints
;; ---------------------------------------------------------------------------

(defn- request-body-too-large-ex
  []
  (ex-info "request body too large"
           {:type :http/request-body-too-large
            :status 413
            :error "request body too large"
            :max_bytes max-request-body-bytes}))

(defn- invalid-json-body-ex
  [cause]
  (ex-info "invalid JSON request body"
           {:type :http/invalid-json-body
            :status 400
            :error "invalid JSON request body"}
           cause))

(declare read-body-bytes)

(defn- read-body-text
  [body]
  (when-let [body-bytes (read-body-bytes body)]
    (String. ^bytes body-bytes StandardCharsets/UTF_8)))

(defn- read-body-bytes
  [body]
  (cond
    (nil? body)
    nil

    (string? body)
    (let [body-bytes (.getBytes ^String body StandardCharsets/UTF_8)]
      (when (> (long (alength body-bytes)) (long max-request-body-bytes))
        (throw (request-body-too-large-ex)))
      body-bytes)

    (instance? byte-array-class body)
    (let [body-bytes ^bytes body]
      (when (> (long (alength body-bytes)) (long max-request-body-bytes))
        (throw (request-body-too-large-ex)))
      body-bytes)

    :else
    (with-open [^InputStream in (io/input-stream body)
                out (ByteArrayOutputStream.)]
      (let [buffer (byte-array 8192)]
        (loop [total 0]
          (let [read-count (.read in buffer)]
            (cond
              (neg? read-count)
              (.toByteArray out)

              (> (+ (long total) read-count) (long max-request-body-bytes))
              (throw (request-body-too-large-ex))

              :else
              (do
                (.write out buffer 0 read-count)
                (recur (+ total read-count))))))))))

(defn- read-body [req]
  (when-let [body (:body req)]
    (let [body-text (read-body-text body)]
      (try
        (json/read-json body-text)
        (catch clojure.lang.ExceptionInfo e
          (throw e))
        (catch Exception e
          (throw (invalid-json-body-ex e)))))))

(defn- request-header
  [req header-name]
  (let [target (str/lower-case header-name)]
    (or (get-in req [:headers header-name])
        (get-in req [:headers target])
        (some (fn [[k v]]
                (when (= target (str/lower-case (str k)))
                  v))
              (:headers req)))))

(defn- request-content-type
  [req]
  (some-> (request-header req "content-type")
          str
          str/lower-case))

(defn- multipart-form-request?
  [req]
  (some-> (request-content-type req)
          (str/starts-with? "multipart/form-data")))

(declare nonblank-str parse-session-id session-exists? session-id-str)

(defn- first-forwarded
  [value]
  (some-> value str (str/split #",") first str/trim nonblank-str))

(defn- request-base-url
  [req]
  (or (when-let [origin (nonblank-str (request-header req "origin"))]
        (let [uri (java.net.URI. origin)]
          (str (.getScheme uri) "://" (.getAuthority uri))))
      (let [scheme (or (first-forwarded (request-header req "x-forwarded-proto"))
                       (some-> (:scheme req) name)
                       "http")
            host   (or (first-forwarded (request-header req "x-forwarded-host"))
                       (nonblank-str (request-header req "host")))]
        (when host
          (str scheme "://" host)))))

(defn- env-value
  [k]
  (System/getenv k))

(defn- parse-query-string
  [query-string]
  (into {}
        (keep (fn [part]
                (let [[^String k ^String v] (str/split (str part) #"=" 2)]
                  (when (seq k)
                    [(java.net.URLDecoder/decode k "UTF-8")
                     (some-> v ^String (java.net.URLDecoder/decode "UTF-8"))]))))
        (str/split (or query-string "") #"&")))

(defn- session-secret []
  @(local-session-secret-delay))

(defn- session-cookie-value []
  (str local-session-cookie-name "=" (session-secret)))

(defn- session-cookie-header []
  (str (session-cookie-value) "; Path=/; HttpOnly; SameSite=Strict"))

(defn- bearer-token
  [value]
  (when-let [header (some-> value str str/trim not-empty)]
    (let [[scheme token & extra] (str/split header #"\s+")]
      (when (and (= "bearer" (some-> scheme str/lower-case))
                 (seq token)
                 (empty? extra))
        token))))

(defn- request-bearer-token
  [req]
  (bearer-token (request-header req "authorization")))

(defn- command-channel-token
  []
  (or (some-> (env-value "XIA_COMMAND_TOKEN") nonblank-str)
      (some-> (cfg/string-option command-channel-token-config-key nil) nonblank-str)))

(defn- cookie-map
  [req]
  (let [cookie-header (request-header req "cookie")]
    (into {}
          (keep (fn [part]
                  (let [[k v] (str/split (str/trim part) #"=" 2)]
                    (when (and (seq k) (some? v))
                      [k v]))))
          (str/split (or cookie-header "") #";"))))

(defn- local-origin?
  [origin]
  (try
    (let [host (.getHost (java.net.URI. origin))]
      (contains? local-hosts host))
    (catch Exception _
      false)))

(defn- valid-session-secret?
  [req]
  (= (get (cookie-map req) local-session-cookie-name)
     (session-secret)))

(defn- trusted-local-origin?
  "Allow loopback browser origins and direct local clients with no origin
   headers. Origin checks prevent cross-site requests from using the cookie."
  [req]
  (let [origin  (request-header req "origin")
        referer (request-header req "referer")]
    (cond
      (seq origin)  (local-origin? origin)
      (seq referer) (local-origin? referer)
      :else         true)))

(defn- json-response [status body]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/write-json-str body)})

(defn- utf8-download-media-type
  [media-type]
  (let [base (some-> media-type str str/trim not-empty)]
    (cond
      (nil? base) "application/octet-stream"
      (re-find #";\s*charset=" base) base
      (or (str/starts-with? base "text/")
          (= base "application/json")
          (= base "application/edn")
          (= base "application/xml"))
      (str base "; charset=utf-8")
      :else
      base)))

(defn- quoted-filename
  [filename]
  (-> (or (some-> filename str str/trim not-empty) "download")
      (str/replace #"[\\\"\r\n]+" "_")))

(defn- download-response
  [filename media-type body]
  {:status  200
   :headers {"Content-Type"        (utf8-download-media-type media-type)
             "Content-Disposition" (str "attachment; filename=\"" (quoted-filename filename) "\"")}
   :body    body})

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

(defn- html-response [body]
  {:status  200
   :headers {"Content-Type" "text/html; charset=utf-8"}
   :body    body})

(defn- escape-html
  [value]
  (-> (str (or value ""))
      (str/replace "&" "&amp;")
      (str/replace "<" "&lt;")
      (str/replace ">" "&gt;")
      (str/replace "\"" "&quot;")
      (str/replace "'" "&#39;")))

(defn- forbidden-response []
  (json-response 403 {:error "forbidden origin"}))

(defn- unauthorized-response []
  (json-response 401 {:error "missing or invalid local session secret"}))

(defn- handle-local-session-bootstrap
  [req]
  (if-not (trusted-local-origin? req)
    (forbidden-response)
    (-> (json-response 200 {:ok true})
        (assoc-in [:headers "Set-Cookie"] (session-cookie-header)))))

(defn- command-channel-unavailable-response []
  (json-response 503 {:error "command channel is not configured"}))

(defn- command-unauthorized-response []
  (json-response 401 {:error "missing or invalid command token"}))

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

(declare runtime-idle-body)

(defn- handle-command-shutdown
  [_req]
  (if-let [handler (some-> (maybe-command-shutdown-handler-atom) deref)]
    (let [{:keys [shutdown-allowed?] :as status} (runtime-health/idle-status)]
      (if shutdown-allowed?
        (do
          (future
            (try
              (handler)
              (catch Throwable e
                (log/error e "Command shutdown handler failed"))))
          (json-response 202 {:status "stopping"}))
        (json-response 409
                       (assoc (runtime-idle-body)
                              :error "runtime must be draining and idle before shutdown"))))
    (json-response 503 {:error "shutdown control unavailable"})))

(defn- runtime-idle-body
  []
  (let [{:keys [phase draining? drain-requested-at accepting-new-work? idle? shutdown-allowed? blockers activity]}
        (runtime-health/idle-status)
        memory-consolidation (hippo/consolidation-summary)]
    {:phase (some-> phase name)
     :draining draining?
     :drain_requested_at (instant->str drain-requested-at)
     :accepting_new_work accepting-new-work?
     :idle idle?
     :shutdown_allowed shutdown-allowed?
     :blockers (mapv (fn [{:keys [component kind count reason]}]
                       {:component (some-> component name)
                        :kind (some-> kind name)
                        :count count
                        :reason reason})
                     blockers)
     :activity {"agent" {"active_session_turn_count" (get-in activity [:agent :active-session-turn-count] 0)
                         "active_session_run_count" (get-in activity [:agent :active-session-run-count] 0)
                         "active_task_run_count" (get-in activity [:agent :active-task-run-count] 0)}
                "scheduler" {"running" (boolean (get-in activity [:scheduler :running?]))
                             "running_schedule_count" (get-in activity [:scheduler :running-schedule-count] 0)
                             "maintenance_running" (boolean (get-in activity [:scheduler :maintenance-running?]))}
                "hippocampus" {"accepting" (boolean (get-in activity [:hippocampus :accepting?]))
                               "pending_background_task_count" (get-in activity [:hippocampus :pending-background-task-count] 0)}
                "llm" {"accepting" (boolean (get-in activity [:llm :accepting?]))
                       "pending_log_write_count" (get-in activity [:llm :pending-log-write-count] 0)}}
     :memory_consolidation memory-consolidation}))

(defn- handle-command-runtime-status
  [_req]
  (json-response 200 (runtime-idle-body)))

(defn- handle-command-runtime-drain
  [_req]
  (runtime-state/request-drain!)
  (json-response 200 (runtime-idle-body)))

(defn- handle-command-runtime-undrain
  [_req]
  (runtime-state/clear-drain!)
  (json-response 200 (runtime-idle-body)))

(defn- handle-command-wake-projection
  [_req]
  (let [projection (wake-projection/current-snapshot)]
    (cond-> (json-response 200 projection)
      (:projection_seq projection)
      (assoc-in [:headers "ETag"] (str "\"" (:projection_seq projection) "\"")))))

(defn- handle-command-create-checkpoint
  [req]
  (let [body         (or (read-body req) {})
        staging-root (some-> (get body "staging_root")
                             nonblank-str)
        checkpoint*  (checkpoint/submit-online-checkpoint!
                       (cond-> {}
                         staging-root
                         (assoc :staging-root staging-root)))]
    (json-response 202 checkpoint*)))

(defn- handle-command-get-checkpoint
  [checkpoint-id]
  (if-let [status (checkpoint/checkpoint-status checkpoint-id)]
    (json-response 200 status)
    (json-response 404 {:error "checkpoint not found"})))

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

(defn- protected-route-response
  [req allowed-fn]
  (cond
    (not (trusted-local-origin? req))
    (forbidden-response)

    (not (valid-session-secret? req))
    (unauthorized-response)

    (not (runtime-available?))
    (runtime-unavailable-response)

    :else
    (allowed-fn)))

(defn- command-route-response
  [req allowed-fn]
  (cond
    (not (runtime-available?))
    (runtime-unavailable-response)

    :else
    (if-let [token (command-channel-token)]
      (if (= (request-bearer-token req) token)
        (allowed-fn)
        (command-unauthorized-response))
      (command-channel-unavailable-response))))

(defn- approval->body
  [{:keys [approval-id tool-id tool-name description arguments reason policy created-at]}]
  {:approval_id approval-id
   :tool_id     (name tool-id)
   :tool_name   tool-name
   :description description
   :arguments   arguments
   :reason      reason
   :policy      (name policy)
   :created_at  (instant->str created-at)})

(defn- prompt->body
  [{:keys [prompt-id label mask? created-at]}]
  {:prompt_id  prompt-id
   :label      label
   :masked     (boolean mask?)
   :created_at (instant->str created-at)})

(defn- current-session-task-id
  [session-id]
  (try
    (let [sid (java.util.UUID/fromString session-id)]
      (some-> (db/current-session-task sid) :id))
    (catch IllegalArgumentException _
      nil)))

(defn- http-prompt-handler
  [label & {:keys [mask?] :or {mask? false}}]
  (let [sid (some-> (:session-id prompt/*interaction-context*) str)]
    (when-not sid
      (throw (ex-info "HTTP prompt requires a session id"
                      {:label label})))
    (let [task-id   (or (:task-id prompt/*interaction-context*)
                        (current-session-task-id sid))
          prompt-id (str (random-uuid))
          response  (promise)
          prompt*   (prompt/register-interaction!
                     {:interaction-id prompt-id
                      :kind :prompt
                      :channel (or (:channel prompt/*interaction-context*) :http)
                      :session-id sid
                      :task-id task-id
                      :prompt-id  prompt-id
                      :label      label
                      :mask?      (boolean mask?)
                      :created-at (java.util.Date.)
                      :response   response})]
      (try
        (let [result (deref response approval-timeout-ms ::timeout)]
          (if (= result ::timeout)
            (throw (ex-info "Timed out waiting for interactive input"
                            {:label label
                             :session-id sid}))
            (str (or result ""))))
        (finally
          (prompt/clear-pending-interaction! {:interaction-id (:interaction-id prompt*)}))))))

(defn- http-approval-handler
  [{:keys [session-id tool-id tool-name description arguments reason policy]}]
  (let [sid (some-> session-id str)]
    (when-not sid
      (throw (ex-info "HTTP approval requires a session id"
                      {:tool-id tool-id})))
    (let [task-id     (or (:task-id prompt/*interaction-context*)
                          (current-session-task-id sid))
          approval-id (str (random-uuid))
          response    (promise)
          approval*   (prompt/register-interaction!
                       {:interaction-id approval-id
                        :kind :approval
                        :channel (or (:channel prompt/*interaction-context*) :http)
                        :session-id sid
                        :task-id task-id
                        :approval-id approval-id
                        :tool-id     tool-id
                        :tool-name   (or tool-name (name tool-id))
                        :description description
                        :arguments   arguments
                        :reason      reason
                        :policy      policy
                        :created-at  (java.util.Date.)
                        :response    response})]
      (try
        (let [result (deref response approval-timeout-ms ::timeout)]
          (case result
            :allow true
            :deny  false
            (throw (ex-info "Timed out waiting for tool approval"
                            {:tool-id tool-id
                             :session-id sid}))))
        (finally
          (prompt/clear-pending-interaction! {:interaction-id (:interaction-id approval*)}))))))

(defn- parse-session-id
  [session-id]
  (cond
    (instance? java.util.UUID session-id)
    (str session-id)

    :else
    (try
      (let [uuid (java.util.UUID/fromString session-id)]
        (str uuid))
      (catch IllegalArgumentException _
        nil))))

(defn- session-exists?
  [session-id]
  (when-let [sid (parse-session-id session-id)]
    (boolean
      (ffirst (db/q '[:find ?e :in $ ?sid
                      :where
                      [?e :session/id ?sid]]
                    (java.util.UUID/fromString sid))))))

(defn- session-id-str
  [session-id]
  (cond
    (instance? java.util.UUID session-id) (str session-id)
    :else                                 (parse-session-id session-id)))

(defn- session-uuid
  [session-id]
  (some-> (session-id-str session-id) java.util.UUID/fromString))

(defn- session-eid
  [session-id]
  (when-let [sid (session-uuid session-id)]
    (ffirst (db/q '[:find ?e :in $ ?sid
                    :where
                    [?e :session/id ?sid]]
                  sid))))

(defn- session-channel
  [session-id]
  (when-let [eid (session-eid session-id)]
    (ffirst (db/q '[:find ?channel :in $ ?e
                    :where
                    [?e :session/channel ?channel]]
                  eid))))

(defn- session-accessible?
  [session-id expected-channel]
  (when-let [sid (session-id-str session-id)]
    (and (session-exists? sid)
         (or (nil? expected-channel)
             (= expected-channel (session-channel sid))))))

(defn- session-active?
  [session-id]
  (when-let [eid (session-eid session-id)]
    (boolean (:session/active? (db/entity eid)))))

(defn- set-session-active!
  [session-id active?]
  (when-let [sid (session-uuid session-id)]
    (db/set-session-active! sid active?)))

(defn- maybe-resume-http-session!
  [session-id expected-channel]
  (when (and (= expected-channel :http)
             (session-accessible? session-id expected-channel)
             (not (session-active? session-id)))
    (when-let [sid (session-uuid session-id)]
      (with-session-finalize-lock
        sid
        (fn []
          (when (and (session-accessible? sid expected-channel)
                     (not (session-active? sid)))
            (set-session-active! sid true)
            (wm/ensure-wm! sid)
            (touch-rest-session! sid)
            (log/info "Resumed HTTP session" (str sid))
            true))))))

(defn- session-busy?
  [session-id]
  (let [state (:state (get @(session-statuses-atom) (str session-id)))]
    (contains? busy-session-states
               (cond
                 (keyword? state) state
                 (string? state) (keyword state)
                 :else state))))

(defn- session-finalize-lock
  [session-id]
  (when-let [sid (session-id-str session-id)]
    (nth (session-finalize-locks)
         (mod (bit-and Integer/MAX_VALUE (int (hash sid)))
              session-finalize-lock-count))))

(defn- with-session-finalize-lock
  [session-id f]
  (if-let [lock (session-finalize-lock session-id)]
    (locking lock
      (f))
    (f)))

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
  (let [text  (some-> value str str/trim)
        limit (long limit)]
    (when (seq text)
      (if (> (long (count text)) limit)
        (str (subs text 0 (util/long-max 0 (- limit 1))) "…")
        text))))

(defn- clear-session-status!
  [session-id]
  (when session-id
    (swap! (session-statuses-atom) dissoc (str session-id))))

(defn- append-task-runtime-event!
  [event]
  (when-let [task-id (some-> (:task-id event) str)]
    (let [received-at (java.util.Date.)]
      (-> (swap! (task-runtime-events-atom)
                 (fn [state]
                   (let [{:keys [next-index events]} (get state task-id)
                         next-index* (inc (long (or next-index 0)))
                         event* (assoc event
                                       :stream-index next-index*
                                       :received-at received-at)
                         events* (conj (vec (or events [])) event*)
                         trimmed (if (> (count events*) max-live-task-runtime-events)
                                   (subvec events* (- (count events*) max-live-task-runtime-events))
                                   events*)]
                     (assoc state task-id {:next-index next-index*
                                           :events trimmed}))))
          (get task-id)
          :events
          last))))

(defn- register-task-runtime-stream-subscriber!
  [task-id subscriber-id callback]
  (when (and task-id subscriber-id callback)
    (swap! (task-runtime-stream-subscribers-atom)
           update
           (str task-id)
           (fnil assoc {})
           subscriber-id
           callback)))

(defn- unregister-task-runtime-stream-subscriber!
  [task-id subscriber-id]
  (when (and task-id subscriber-id)
    (swap! (task-runtime-stream-subscribers-atom)
           (fn [state]
             (let [task-key (str task-id)
                   subscribers (dissoc (get state task-key {}) subscriber-id)]
               (if (seq subscribers)
                 (assoc state task-key subscribers)
                 (dissoc state task-key)))))))

(defn- notify-task-runtime-stream-subscribers!
  [event]
  (when-let [task-id (some-> (:task-id event) str)]
    (doseq [[subscriber-id callback] (get @(task-runtime-stream-subscribers-atom) task-id)]
      (try
        (callback event)
        (catch Exception e
          (log/warn e "Failed to deliver runtime event to task stream subscriber"
                    "task" task-id
                    "subscriber" subscriber-id)
          (unregister-task-runtime-stream-subscriber! task-id subscriber-id))))))

(defn- clear-rest-session-state!
  [session-id]
  (let [sid (str session-id)]
    (clear-session-status! sid)
    (prompt/clear-pending-interaction! {:session-id sid})
    (cancel-rest-session-finalizer! sid)))

(defn- terminal-status-state?
  [state]
  (contains? #{:completed :done :error :cancelled} state))

(defn- http-status-handler
  [{:keys [session-id state] :as status}]
  (when-let [sid (some-> session-id str)]
    (if (terminal-status-state? state)
      (clear-session-status! sid)
      (swap! (session-statuses-atom) assoc sid (assoc status :updated-at (java.util.Date.))))))

(defn- http-runtime-event-handler
  [event]
  (when-let [event* (append-task-runtime-event! event)]
    (notify-task-runtime-stream-subscribers! event*)))

(defn- finalize-rest-session!
  ([session-id]
   (finalize-rest-session! session-id :explicit))
  ([session-id reason]
   (when-let [sid (session-uuid session-id)]
     (with-session-finalize-lock
       sid
       (fn []
         (let [sid-str     (str sid)
               channel     (or (session-channel sid) :http)
               was-active? (session-active? sid)]
           (try
             (when was-active?
               (let [topics (:topics (wm/get-wm sid))]
                 (try
                   (wm/clear-autonomy-state! sid)
                   (catch Exception e
                     (log/error e "Failed to clear session autonomy state during finalization"
                                sid-str
                                "channel" (name channel)
                                "reason" (name reason))))
                 (try
                   (wm/snapshot! sid)
                   (catch Exception e
                     (log/error e "Failed to snapshot session working memory during finalization"
                                sid-str
                                "channel" (name channel)
                                "reason" (name reason))))
                (try
                   (hippo/record-conversation! sid channel
                                               :topics topics
                                               :consolidation-mode :sync)
                   (catch Exception e
                     (log/warn e "Failed to record session conversation during finalization"
                               sid-str
                               "channel" (name channel)
                               "reason" (name reason))))))
             (catch Exception e
               (log/error e "Failed to finalize session"
                          sid-str
                          "channel" (name channel)
                          "reason" (name reason)))
             (finally
               (try
                 (wm/clear-wm! sid)
                 (catch Exception e
                   (log/error e "Failed to clear session working memory"
                              sid-str
                              "channel" (name channel))))
               (clear-rest-session-state! sid)
               (when was-active?
                 (try
                   (set-session-active! sid false)
                   (catch Exception e
                     (log/error e "Failed to mark session inactive"
                                sid-str
                                "channel" (name channel)))))))
           (when was-active?
             (log/info "Finalized session"
                       sid-str
                       "channel" (name channel)
                       "reason" (name reason)))
           was-active?))))))

(defn- named-value->str
  [value]
  (cond
    (keyword? value) (name value)
    (symbol? value)  (name value)
    (some? value)    (str value)
    :else            nil))

(defn- knowledge-node->body
  [node]
  (let [eid (or (:eid node) (:db/id node))]
    {:id   (some-> eid str)
     :eid  eid
     :name (or (:name node) (:kg.node/name node))
     :type (named-value->str (or (:type node) (:kg.node/type node)))}))

(defn- knowledge-fact->body
  [fact]
  {:id         (some-> (:eid fact) str)
   :eid        (:eid fact)
   :node_id    (some-> (:node-eid fact) str)
   :node_eid   (:node-eid fact)
   :content    (:content fact)
   :confidence (:confidence fact)
   :utility    (:utility fact)
   :updated_at (instant->str (:updated-at fact))})

(defn- nonblank-str
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn- parse-keyword-id
  [value field-name]
  (let [id-str (nonblank-str value)]
    (cond
      (nil? id-str)
      (throw (ex-info (str "missing '" field-name "' field") {:field field-name}))

      (re-find #"\s" id-str)
      (throw (ex-info (str "'" field-name "' must not contain whitespace")
                      {:field field-name
                       :value value}))

      :else
      (keyword id-str))))

(defn- parse-optional-positive-long
  [value field-name]
  (let [text (nonblank-str value)]
    (when text
      (try
        (let [parsed (Long/parseLong text)]
          (when-not (pos? parsed)
            (throw (ex-info (str "'" field-name "' must be a positive integer")
                            {:field field-name
                             :value value})))
          parsed)
        (catch NumberFormatException _
          (throw (ex-info (str "'" field-name "' must be a positive integer")
                          {:field field-name
                           :value value})))))))

(def ^:private default-knowledge-search-top 12)
(def ^:private max-knowledge-search-top 25)

(defn- parse-knowledge-search-top
  [value]
  (-> (or (parse-optional-positive-long value "top")
          default-knowledge-search-top)
      long
      (util/long-min max-knowledge-search-top)
      int))

(defn- search-knowledge-nodes
  [query top]
  (loop [results (concat (memory/find-node query)
                         (memory/search-nodes query :top top))
         acc     []
         seen    #{}]
    (if-let [node (first results)]
      (let [node-eid (:eid node)]
        (if (or (nil? node-eid)
                (contains? seen node-eid))
          (recur (rest results) acc seen)
          (recur (rest results)
                 (cond-> acc
                   (< (count acc) top) (conj node))
                 (conj seen node-eid))))
      acc)))

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

(defn- session-handler-deps
  []
  {:approval->body               approval->body
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
   :prompt->body                 prompt->body
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
  (let [params (parse-query-string (:query-string req))
        query  (nonblank-str (get params "query"))
        top    (parse-knowledge-search-top (get params "top"))]
    (if-not query
      (json-response 400 {:error "missing query"})
      (json-response 200 {:query query
                          :nodes (mapv knowledge-node->body
                                       (search-knowledge-nodes query top))}))))

(defn- handle-list-knowledge-node-facts [node-id]
  (if-let [node-eid (parse-optional-positive-long node-id "node id")]
    (if-let [node (some-> node-eid memory/get-node not-empty)]
      (json-response 200
                     {:node  (knowledge-node->body (assoc node :db/id node-eid))
                      :facts (mapv (fn [fact]
                                     (knowledge-fact->body (assoc fact :node-eid node-eid)))
                                   (memory/node-facts-with-eids node-eid))})
      (json-response 404 {:error "node not found"}))
    (json-response 400 {:error "invalid node id"})))

(defn- handle-delete-knowledge-fact [fact-id]
  (if-let [fact-eid (parse-optional-positive-long fact-id "fact id")]
    (if-let [forgotten (memory/forget-fact! fact-eid)]
      (json-response 200 {:status "forgotten"
                          :fact   (knowledge-fact->body forgotten)})
      (json-response 404 {:error "fact not found"}))
    (json-response 400 {:error "invalid fact id"})))

(defn- handle-health [_req]
  (json-response 200 {:status "ok" :version "0.1.0"}))

(defn- handle-web-dev-reload [_req]
  (if (web-dev-enabled?)
    (with-web-dev-headers
      (json-response 200 {:enabled true
                          :version (or (web-dev-version) "0")}))
    (json-response 404 {:error "not found"})))

(defn- handle-home [_req]
  (if-let [html (read-resource "index.html")]
    (-> (html-response (inject-web-dev-client html))
        (assoc-in [:headers "Set-Cookie"] (session-cookie-header))
        (with-web-dev-headers))
    {:status 404 :body "Not Found"}))

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
          status-match       (re-matches #"/sessions/([0-9a-fA-F-]+)/status" uri)
          prompt-match       (re-matches #"/sessions/([0-9a-fA-F-]+)/prompt" uri)
          approval-match     (re-matches #"/sessions/([0-9a-fA-F-]+)/approval" uri)
          command-session-close-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)" uri)
          command-session-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/messages" uri)
          command-session-audit-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/audit" uri)
          command-session-task-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/task" uri)
          command-status-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/status" uri)
          command-runtime-status-match (= uri "/command/runtime/status")
          command-runtime-drain-match (= uri "/command/runtime/drain")
          command-runtime-undrain-match (= uri "/command/runtime/undrain")
          command-managed-checkpoints-match (= uri "/command/managed/checkpoints")
          command-managed-checkpoint-match (re-matches #"/command/managed/checkpoints/([^/]+)" uri)
          command-wake-projection-match (= uri "/command/managed/wake-projection")
          command-prompt-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/prompt" uri)
          command-approval-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/approval" uri)
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

        (and (= method :get) (contains? web-static-assets uri))
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
        (command-route-response req #(handle-create-session :command))

        (and (= method :post) (= uri "/command/chat"))
        (command-route-response req #(handle-chat req :command))

        (and (= method :post) (= uri "/hooks/slack/events"))
        (http-messaging/handle-slack-events (workspace-handler-deps) req)

        (and (= method :post) (= uri "/hooks/telegram"))
        (http-messaging/handle-telegram-webhook (workspace-handler-deps) req)

        (and (= method :post) (= uri "/command/shutdown"))
        (command-route-response req #(handle-command-shutdown req))

        (and (= method :get) command-runtime-status-match)
        (command-route-response req #(handle-command-runtime-status req))

        (and (= method :post) command-runtime-drain-match)
        (command-route-response req #(handle-command-runtime-drain req))

        (and (= method :post) command-runtime-undrain-match)
        (command-route-response req #(handle-command-runtime-undrain req))

        (and (= method :post) command-managed-checkpoints-match)
        (command-route-response req #(handle-command-create-checkpoint req))

        (and (= method :get) command-managed-checkpoint-match)
        (command-route-response req #(handle-command-get-checkpoint
                                      (second command-managed-checkpoint-match)))

        (and (= method :get) command-wake-projection-match)
        (command-route-response req #(handle-command-wake-projection req))

        (and (= method :delete) command-session-close-match)
        (command-route-response req #(handle-close-session (second command-session-close-match) :command))

        (and (= method :get) command-status-match)
        (command-route-response req #(handle-get-status (second command-status-match) :command))

        (and (= method :get) command-session-task-match)
        (command-route-response req #(handle-get-current-task (second command-session-task-match) :command))

        (and (= method :get) command-prompt-match)
        (command-route-response req #(handle-get-prompt (second command-prompt-match) :command))

        (and (= method :post) command-prompt-match)
        (command-route-response req #(handle-submit-prompt (second command-prompt-match) req :command))

        (and (= method :get) command-approval-match)
        (command-route-response req #(handle-get-approval (second command-approval-match) :command))

        (and (= method :post) command-approval-match)
        (command-route-response req #(handle-submit-approval (second command-approval-match) req :command))

        (and (= method :get) command-session-match)
        (command-route-response req #(handle-session-messages (second command-session-match) :command))

        (and (= method :get) command-session-audit-match)
        (command-route-response req #(handle-session-audit (second command-session-audit-match) :command))

        (and (= method :delete) session-close-match)
        (protected-route-response req #(handle-local-close-session (second session-close-match)))

        (and (= method :get) status-match)
        (protected-route-response req #(handle-local-get-status (second status-match)))

        (and (= method :get) session-task-match)
        (protected-route-response req #(handle-get-current-task (second session-task-match) :http))

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
   (prompt/register-channel-adapter! :http
                                     {:prompt http-prompt-handler
                                      :approval http-approval-handler
                                      :status http-status-handler
                                      :runtime-event http-runtime-event-handler})
   (prompt/register-channel-adapter! :command
                                     {:prompt http-prompt-handler
                                      :approval http-approval-handler
                                      :status http-status-handler
                                      :runtime-event http-runtime-event-handler})
   (prompt/register-channel-adapter! :websocket
                                     {:approval http-approval-handler
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
    (prompt/clear-channel-adapter! :http)
    (prompt/clear-channel-adapter! :command)
    (prompt/clear-channel-adapter! :websocket)
    (reset! (websocket-receive-failures-atom) {})
    (reset! (session-statuses-atom) {})
    (reset! (task-runtime-events-atom) {})
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
    (reset! (:rest-session-finalizer-executor-atom runtime) nil)
    (reset! (:rest-session-finalizers-atom runtime) {})
    (reset! installed-runtime-atom nil))
  nil)
