(ns xia.channel.http
  "HTTP/WebSocket channel — enables remote clients and web UIs."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [org.httpkit.server :as http]
            [charred.api :as json]
            [ring.middleware.multipart-params :as multipart]
            [taoensso.timbre :as log]
            [xia.backup :as backup]
            [xia.artifact :as artifact]
            [xia.scratch :as scratch]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.hippocampus :as hippo]
            [xia.local-doc :as local-doc]
            [xia.llm :as llm]
            [xia.memory :as memory]
            [xia.oauth :as oauth]
            [xia.oauth-template :as oauth-template]
            [xia.context :as context]
            [xia.remote-bridge :as remote-bridge]
            [xia.service :as service-proxy]
            [xia.schedule :as schedule]
            [xia.agent :as agent]
            [xia.prompt :as prompt]
            [xia.summarizer :as summarizer]
            [xia.skill.openclaw :as openclaw-skill]
            [xia.working-memory :as wm])
  (:import [java.io ByteArrayOutputStream InputStream]
    [java.nio.charset StandardCharsets]
    [java.security SecureRandom]
    [java.util Base64 Date]
    [java.util.concurrent Executors ScheduledExecutorService ScheduledFuture TimeUnit]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private server-atom (atom nil))
(defonce ^:private ws-sessions (atom {})) ; channel → session-id
(defonce ^:private pending-approvals (atom {})) ; session-id string → approval map
(defonce ^:private session-statuses (atom {})) ; session-id string → latest status map

(def ^:private local-hosts #{"localhost" "127.0.0.1" "::1" "[::1]"})
(def ^:private approval-timeout-ms (* 5 60 1000))
(def ^:private local-session-cookie-name "xia-local-session")
(def ^:private max-request-body-bytes (* 16 1024 1024)) ; 16 MiB
(def ^:private default-rest-session-idle-timeout-ms (* 30 60 1000))
(def ^:private session-finalize-lock-count 256)
(def ^:private service-auth-types #{:bearer :basic :api-key-header :query-param :oauth-account})
(def ^:private ms-per-day (* 24 60 60 1000))
(def ^:private byte-array-class (class (byte-array 0)))
(defn- long-max
  ^long [^long a ^long b]
  (if (> a b) a b))

(defn- long-min
  ^long [^long a ^long b]
  (if (< a b) a b))

(defn- days->ms
  [days]
  (when-some [days* (some-> days long)]
    (* (long days*) (long ms-per-day))))

(defonce ^:private local-session-secret
  (delay
    (let [bytes (byte-array 32)
          _     (.nextBytes (SecureRandom.) bytes)]
      (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bytes))))
(defonce ^:private rest-session-finalizer-executor (atom nil))
(defonce ^:private rest-session-finalizers (atom {})) ; session-id string → ScheduledFuture
(defonce ^:private session-finalize-locks
  (vec (repeatedly session-finalize-lock-count #(Object.))))

;; ---------------------------------------------------------------------------
;; Local web UI
;; ---------------------------------------------------------------------------

(def ^:private read-resource
  (memoize
    (fn [path]
      (some-> (str "web/" path)
              io/resource
              slurp))))

(def ^:private read-resource-bytes
  (memoize
    (fn [path]
      (when-let [resource (some-> (str "web/" path) io/resource)]
        (with-open [in (io/input-stream resource)
                    out (ByteArrayOutputStream.)]
          (io/copy in out)
          (.toByteArray out))))))

(defn- resource-response [path content-type]
  (if-let [content (read-resource path)]
    {:status  200
     :headers {"Content-Type" content-type}
     :body    content}
    {:status 404 :body "Not Found"}))

(defn- binary-resource-response [path content-type]
  (if-let [content (read-resource-bytes path)]
    {:status  200
     :headers {"Content-Type" content-type}
     :body    content}
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

(defn- date->millis
  [value]
  (when (instance? Date value)
    (.getTime ^Date value)))

(def ^:private web-static-assets
  {"/style.css"                     {:path "style.css" :content-type "text/css"}
   "/app.js"                        {:path "app.js" :content-type "text/javascript"}
   "/favicon.ico"                   {:path "favicon/favicon.ico" :content-type "image/x-icon" :binary? true}
   "/favicon/favicon-16x16.png"     {:path "favicon/favicon-16x16.png" :content-type "image/png" :binary? true}
   "/favicon/favicon-32x32.png"     {:path "favicon/favicon-32x32.png" :content-type "image/png" :binary? true}
   "/favicon/apple-touch-icon.png"  {:path "favicon/apple-touch-icon.png" :content-type "image/png" :binary? true}
   "/favicon/android-chrome-192x192.png" {:path "favicon/android-chrome-192x192.png" :content-type "image/png" :binary? true}
   "/favicon/android-chrome-512x512.png" {:path "favicon/android-chrome-512x512.png" :content-type "image/png" :binary? true}
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

(declare protected-route-response)

(defn- finalize-websocket-session!
  [ch]
  (when-let [sid (get @ws-sessions ch)]
    (try
      (let [topics (:topics (wm/get-wm sid))]
        (wm/snapshot! sid)
        (hippo/record-conversation! sid :websocket :topics topics))
      (catch Exception e
        (log/error e "Failed to finalize WebSocket session"))
      (finally
        (try
          (wm/clear-wm! sid)
          (catch Exception e
            (log/error e "Failed to clear WebSocket working memory"))))))
  (swap! ws-sessions dissoc ch)
  (log/info "WebSocket disconnected"))

(defn- ws-handler [req]
  (protected-route-response
    req
    #(http/as-channel req
       {:on-open
        (fn [ch]
          (let [sid (db/create-session! :websocket)]
            (swap! ws-sessions assoc ch sid)
            (wm/ensure-wm! sid)
            (log/info "WebSocket connected, session:" sid)
            (http/send! ch (json/write-json-str {:type "connected" :session-id (str sid)}))))

        :on-receive
        (fn [ch msg]
          (if-let [sid (get @ws-sessions ch)]
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
                                                     :error (.getMessage e)}))))
            (http/send! ch (json/write-json-str {:type "error" :error "Session not found"}))))

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

(declare nonblank-str parse-session-id session-exists?)

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
  @local-session-secret)

(defn- session-cookie-value []
  (str local-session-cookie-name "=" (session-secret)))

(defn- session-cookie-header []
  (str (session-cookie-value) "; Path=/; HttpOnly; SameSite=Strict"))

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

(defn- exception-response
  [^Throwable e]
  (let [data    (ex-data e)
        status  (or (:status data) 400)
        details (not-empty (dissoc data :status :error :type))
        body    (cond-> {:error (or (:error data) (throwable-message e))}
                  details (assoc :details details))]
    (json-response status body)))

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

(defn- protected-route-response
  [req allowed-fn]
  (cond
    (not (trusted-local-origin? req))
    (forbidden-response)

    (not (valid-session-secret? req))
    (unauthorized-response)

    :else
    (allowed-fn)))

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

(defn- clear-pending-approval!
  [session-id approval-id]
  (swap! pending-approvals
         (fn [pending]
           (let [current (get pending session-id)]
             (if (= approval-id (:approval-id current))
               (dissoc pending session-id)
               pending)))))

(defn- http-approval-handler
  [{:keys [session-id tool-id tool-name description arguments reason policy]}]
  (let [sid (some-> session-id str)]
    (when-not sid
      (throw (ex-info "HTTP approval requires a session id"
                      {:tool-id tool-id})))
    (let [approval-id (str (random-uuid))
          decision    (promise)
          approval    {:approval-id approval-id
                       :tool-id     tool-id
                       :tool-name   (or tool-name (name tool-id))
                       :description description
                       :arguments   arguments
                       :reason      reason
                       :policy      policy
                       :created-at  (java.util.Date.)
                       :decision    decision}]
      (swap! pending-approvals assoc sid approval)
      (try
        (let [result (deref decision approval-timeout-ms ::timeout)]
          (case result
            :allow true
            :deny  false
            (throw (ex-info "Timed out waiting for tool approval"
                            {:tool-id tool-id
                             :session-id sid}))))
        (finally
          (clear-pending-approval! sid approval-id))))))

(defn- parse-session-id
  [session-id]
  (try
    (let [uuid (java.util.UUID/fromString session-id)]
      (str uuid))
    (catch IllegalArgumentException _
      nil)))

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

(defn- session-active?
  [session-id]
  (when-let [eid (session-eid session-id)]
    (boolean
      (ffirst (db/q '[:find ?active :in $ ?e
                      :where
                      [(get-else $ ?e :session/active? false) ?active]]
                    eid)))))

(defn- set-session-active!
  [session-id active?]
  (when-let [eid (session-eid session-id)]
    (db/transact! [[:db/add eid :session/active? active?]])
    true))

(defn- session-busy?
  [session-id]
  (contains? @session-statuses (str session-id)))

(defn- session-finalize-lock
  [session-id]
  (when-let [sid (session-id-str session-id)]
    (nth session-finalize-locks
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
    (when-let [^ScheduledFuture future (get @rest-session-finalizers sid)]
      (.cancel future false))
    (swap! rest-session-finalizers dissoc sid)))

(defn- clear-rest-session-finalizers!
  []
  (doseq [[_ ^ScheduledFuture future] @rest-session-finalizers]
    (.cancel future false))
  (reset! rest-session-finalizers {}))

(defn- schedule-rest-session-finalizer!
  [session-id]
  (when-let [sid (session-id-str session-id)]
    (cancel-rest-session-finalizer! sid)
    (when-let [^ScheduledExecutorService exec @rest-session-finalizer-executor]
      (let [delay-ms (rest-session-idle-timeout-ms)
            task     ^Runnable
            (fn []
              (swap! rest-session-finalizers dissoc sid)
              (finalize-rest-session! sid :idle-timeout))]
        (swap! rest-session-finalizers assoc
               sid
               (.schedule exec task (long delay-ms) TimeUnit/MILLISECONDS))))))

(defn- touch-rest-session!
  [session-id]
  (when (session-active? session-id)
    (schedule-rest-session-finalizer! session-id)))

(def ^:private history-session-channels #{:http :websocket :terminal})

(defn- truncate-text
  [value limit]
  (let [text  (some-> value str str/trim)
        limit (long limit)]
    (when (seq text)
      (if (> (long (count text)) limit)
        (str (subs text 0 (long-max 0 (- limit 1))) "…")
        text))))

(defn- history-run->body
  [run]
  {:id          (some-> (:id run) str)
   :schedule_id (some-> (:schedule-id run) name)
   :started_at  (instant->str (:started-at run))
   :finished_at (instant->str (:finished-at run))
   :status      (some-> (:status run) name)
   :actions     (:actions run)
   :result      (:result run)
   :error       (:error run)})

(defn- history-schedule->body
  [sched]
  (let [latest-run (first (schedule/schedule-history (:id sched) 1))]
    {:id            (some-> (:id sched) name)
     :name          (:name sched)
     :type          (some-> (:type sched) name)
     :trusted       (boolean (:trusted? sched))
     :enabled       (boolean (:enabled? sched))
     :last_run      (instant->str (:last-run sched))
     :next_run      (instant->str (:next-run sched))
     :latest_status (some-> (:status latest-run) name)
     :latest_error  (truncate-text (:error latest-run) 160)}))

(defn- history-session->body
  [session]
  (let [messages     (->> (db/session-messages (:id session))
                          (filter #(#{:user :assistant} (:role %)))
                          vec)
        last-message (last messages)]
    {:id              (some-> (:id session) str)
     :channel         (some-> (:channel session) name)
     :created_at      (instant->str (:created-at session))
     :active          (boolean (:active? session))
     :message_count   (count messages)
     :last_message_at (instant->str (:created-at last-message))
     :preview         (truncate-text (:content last-message) 160)}))

(defn- status->body
  [status]
  (when status
    {:state      (some-> (:state status) name)
     :phase      (some-> (:phase status) name)
     :message    (:message status)
     :tool_id    (some-> (:tool-id status) name)
     :tool_name  (:tool-name status)
     :round      (:round status)
     :tool_count (:tool-count status)
     :updated_at (instant->str (:updated-at status))}))

(defn- clear-session-status!
  [session-id]
  (when session-id
    (swap! session-statuses dissoc (str session-id))))

(defn- clear-rest-session-state!
  [session-id]
  (let [sid (str session-id)]
    (clear-session-status! sid)
    (swap! pending-approvals dissoc sid)
    (cancel-rest-session-finalizer! sid)))

(defn- terminal-status-state?
  [state]
  (contains? #{:done :error} state))

(defn- http-status-handler
  [{:keys [session-id state] :as status}]
  (when-let [sid (some-> session-id str)]
    (if (terminal-status-state? state)
      (clear-session-status! sid)
      (swap! session-statuses assoc sid (assoc status :updated-at (java.util.Date.))))))

(defn- finalize-rest-session!
  ([session-id]
   (finalize-rest-session! session-id :explicit))
  ([session-id reason]
   (when-let [sid (session-uuid session-id)]
     (with-session-finalize-lock
       sid
       (fn []
         (let [sid-str     (str sid)
               was-active? (session-active? sid)]
           (try
             (when was-active?
               (let [topics (:topics (wm/get-wm sid))]
                 (wm/snapshot! sid)
                 (hippo/record-conversation! sid :http :topics topics)))
             (catch Exception e
               (log/error e "Failed to finalize REST session" sid-str "reason" (name reason)))
             (finally
               (try
                 (wm/clear-wm! sid)
                 (catch Exception e
                   (log/error e "Failed to clear REST working memory" sid-str)))
               (clear-rest-session-state! sid)
               (when was-active?
                 (try
                   (set-session-active! sid false)
                   (catch Exception e
                     (log/error e "Failed to mark REST session inactive" sid-str))))))
           (when was-active?
             (log/info "Finalized REST session" sid-str "reason" (name reason)))
           was-active?))))))

(defn- scratch-pad->body
  [pad]
  {:id         (:id pad)
   :scope      (name (:scope pad))
   :session_id (:session-id pad)
   :title      (:title pad)
   :content    (:content pad)
   :mime       (:mime pad)
   :version    (:version pad)
   :created_at (instant->str (:created-at pad))
   :updated_at (instant->str (:updated-at pad))})

(defn- scratch-metadata->body
  [pad]
  (dissoc (scratch-pad->body pad) :content))

(defn- local-doc->body
  [doc]
  {:id         (some-> (:id doc) str)
   :session_id (:session-id doc)
   :name       (:name doc)
   :media_type (:media-type doc)
   :source     (some-> (:source doc) name)
   :size_bytes (:size-bytes doc)
   :sha256     (:sha256 doc)
   :status     (some-> (:status doc) name)
   :error      (:error doc)
   :summary    (:summary doc)
   :text       (:text doc)
   :preview    (:preview doc)
   :chunk_count (:chunk-count doc)
   :created_at (instant->str (:created-at doc))
   :updated_at (instant->str (:updated-at doc))})

(defn- local-doc-metadata->body
  [doc]
  (dissoc (local-doc->body doc) :text :sha256))

(defn- artifact->body
  [artifact]
  {:id         (some-> (:id artifact) str)
   :session_id (:session-id artifact)
   :name       (:name artifact)
   :title      (:title artifact)
   :kind       (some-> (:kind artifact) name)
   :media_type (:media-type artifact)
   :extension  (:extension artifact)
   :source     (some-> (:source artifact) name)
   :status     (some-> (:status artifact) name)
   :size_bytes (:size-bytes artifact)
   :compressed_size_bytes (:compressed-size-bytes artifact)
   :has_blob   (boolean (:has-blob? artifact))
   :text_available (boolean (:text-available? artifact))
   :sha256     (:sha256 artifact)
   :error      (:error artifact)
   :meta       (:meta artifact)
   :text       (:text artifact)
   :preview    (:preview artifact)
   :created_at (instant->str (:created-at artifact))
   :updated_at (instant->str (:updated-at artifact))})

(defn- artifact-metadata->body
  [artifact]
  (dissoc (artifact->body artifact) :text :sha256 :meta))

(defn- local-doc-ref->body
  [doc]
  {:id     (some-> (:id doc) str)
   :name   (:name doc)
   :status (some-> (:status doc) name)})

(defn- artifact-ref->body
  [artifact]
  {:id     (some-> (:id artifact) str)
   :name   (:name artifact)
   :title  (:title artifact)
   :status (some-> (:status artifact) name)})

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

(defn- parse-provider-workloads
  [value]
  (let [entries (cond
                  (nil? value) []
                  (sequential? value) value
                  :else (str/split (str value) #","))]
    (->> entries
         (map nonblank-str)
         (remove nil?)
         distinct
         (mapv (fn [entry]
                 (let [workload (keyword entry)]
                   (when-not (llm/known-workload? workload)
                     (throw (ex-info "invalid workload"
                                     {:field "workloads"
                                      :value entry})))
                   workload))))))

(defn- parse-extra-fields
  [value]
  (let [text (nonblank-str value)]
    (when text
      (try
        (json/write-json-str (json/read-json text))
        (catch Exception _
          (throw (ex-info "extra_fields must be valid JSON"
                          {:field "extra_fields"})))))))

(defn- parse-json-object-string
  [value field-name]
  (let [text (nonblank-str value)]
    (when text
      (try
        (let [parsed (json/read-json text)]
          (when-not (map? parsed)
            (throw (ex-info (str field-name " must be a JSON object")
                            {:field field-name})))
          (json/write-json-str parsed))
        (catch clojure.lang.ExceptionInfo e
          (throw e))
        (catch Exception _
          (throw (ex-info (str field-name " must be valid JSON")
                          {:field field-name})))))))

(defn- parse-auth-type
  [value]
  (let [auth-type (some-> value nonblank-str keyword)]
    (when-not (contains? service-auth-types auth-type)
      (throw (ex-info "invalid auth_type"
                      {:field "auth_type"
                       :value value})))
    auth-type))

(defn- sort-by-name
  [entries]
  (->> entries
       (sort-by (fn [entry]
                  (str/lower-case (or (:name entry) (:id entry) ""))))
       vec))

(defn- provider->admin-body
  [provider]
  (let [provider-id (some-> (:llm.provider/id provider) name)
        health      (llm/provider-health-summary (:llm.provider/id provider))]
    {:id                    provider-id
     :name                  (:llm.provider/name provider)
     :base_url              (:llm.provider/base-url provider)
     :model                 (:llm.provider/model provider)
     :workloads             (->> (:llm.provider/workloads provider)
                                 (map name)
                                 sort
                                 vec)
     :vision                (boolean (:llm.provider/vision? provider))
     :system_prompt_budget  (:llm.provider/system-prompt-budget provider)
     :history_budget        (:llm.provider/history-budget provider)
     :health_status         (name (:status health))
     :health_failures       (:consecutive-failures health)
     :health_cooldown_ms    (:cooldown-remaining-ms health)
     :health_last_error     (:last-error health)
     :default               (boolean (:llm.provider/default? provider))
     :api_key_configured    (boolean (nonblank-str (:llm.provider/api-key provider)))}))

(defn- memory-retention->admin-body
  []
  (let [{:keys [full-resolution-ms decay-half-life-ms retained-decayed-count]}
        (memory/episode-retention-settings)]
    {:full_resolution_days (long (/ (long full-resolution-ms) (long ms-per-day)))
     :decay_half_life_days (long (/ (long decay-half-life-ms) (long ms-per-day)))
     :retained_count       (long retained-decayed-count)}))

(defn- knowledge-decay->admin-body
  []
  (let [{:keys [grace-period-ms half-life-ms min-confidence maintenance-step-ms archive-after-bottom-ms]}
        (hippo/knowledge-decay-settings)]
    {:grace_period_days         (long (/ (long grace-period-ms) (long ms-per-day)))
     :half_life_days            (long (/ (long half-life-ms) (long ms-per-day)))
     :min_confidence            min-confidence
     :maintenance_interval_days (long (/ (long maintenance-step-ms) (long ms-per-day)))
     :archive_after_bottom_days (long (/ (long archive-after-bottom-ms) (long ms-per-day)))}))

(defn- conversation-context->admin-body
  []
  {:recent_history_message_limit (context/recent-history-message-limit-config)})

(defn- local-doc-summarization->admin-body
  []
  {:model_summaries_enabled   (boolean (summarizer/enabled?))
   :model_summary_backend     (some-> (summarizer/summary-backend) name)
   :model_summary_provider_id (some-> (summarizer/external-provider-id) name)
   :chunk_summary_max_tokens  (summarizer/chunk-summary-max-tokens)
   :doc_summary_max_tokens    (summarizer/document-summary-max-tokens)})

(defn- database-backup->admin-body
  []
  (let [settings (backup/admin-body)]
    {:enabled           (boolean (:enabled settings))
     :directory         (:directory settings)
     :interval_hours    (:interval_hours settings)
     :retain_count      (:retain_count settings)
     :running           (boolean (:running settings))
     :started_at        (instant->str (:started_at settings))
     :last_attempt_at   (instant->str (:last_attempt_at settings))
     :last_success_at   (instant->str (:last_success_at settings))
     :last_archive_path (:last_archive_path settings)
     :last_error        (:last_error settings)
     :next_due_at       (instant->str (:next_due_at settings))}))

(defn- save-config-override!
  [config-key value]
  (if (some? value)
    (db/set-config! config-key value)
    (db/delete-config! config-key)))

(defn- service->admin-body
  [service]
  (let [oauth-account (some-> (:service/oauth-account service) db/get-oauth-account)]
    {:id                     (some-> (:service/id service) name)
     :name                   (:service/name service)
     :base_url               (:service/base-url service)
     :auth_type              (some-> (:service/auth-type service) name)
     :auth_header            (:service/auth-header service)
     :oauth_account          (some-> (:service/oauth-account service) name)
     :oauth_account_name     (:oauth.account/name oauth-account)
     :oauth_account_connected (boolean (nonblank-str (:oauth.account/access-token oauth-account)))
     :oauth_account_autonomous_approved (boolean (and oauth-account
                                                     (autonomous/oauth-account-autonomous-approved? oauth-account)))
     :rate_limit_per_minute  (:service/rate-limit-per-minute service)
     :effective_rate_limit_per_minute (service-proxy/effective-rate-limit-per-minute service)
     :autonomous_approved    (boolean (autonomous/service-autonomous-approved? service))
     :enabled                (boolean (:service/enabled? service))
     :auth_key_configured    (boolean (nonblank-str (:service/auth-key service)))}))

(defn- oauth-account->admin-body
  [account]
  {:id                       (some-> (:oauth.account/id account) name)
   :name                     (:oauth.account/name account)
   :authorize_url            (:oauth.account/authorize-url account)
   :token_url                (:oauth.account/token-url account)
   :client_id                (:oauth.account/client-id account)
   :provider_template        (some-> (:oauth.account/provider-template account) name)
   :scopes                   (:oauth.account/scopes account)
   :redirect_uri             (:oauth.account/redirect-uri account)
   :auth_params              (:oauth.account/auth-params account)
   :token_params             (:oauth.account/token-params account)
   :client_secret_configured (boolean (nonblank-str (:oauth.account/client-secret account)))
   :access_token_configured  (boolean (nonblank-str (:oauth.account/access-token account)))
   :refresh_token_configured (boolean (nonblank-str (:oauth.account/refresh-token account)))
   :autonomous_approved      (boolean (autonomous/oauth-account-autonomous-approved? account))
   :connected                (boolean (nonblank-str (:oauth.account/access-token account)))
   :expires_at               (instant->str (:oauth.account/expires-at account))
   :connected_at             (instant->str (:oauth.account/connected-at account))})

(defn- oauth-template->admin-body
  [template]
  {:id            (some-> (:id template) name)
   :name          (:name template)
   :description   (:description template)
   :authorize_url (:authorize-url template)
   :token_url     (:token-url template)
   :api_base_url  (:api-base-url template)
   :service_id    (:service-id template)
   :service_name  (:service-name template)
   :scopes        (:scopes template)
   :auth_params   (json/write-json-str (or (:auth-params template) {}))
   :token_params  (json/write-json-str (or (:token-params template) {}))
   :notes         (:notes template)})

(defn- site->admin-body
  [site]
  {:id                  (some-> (:site-cred/id site) name)
   :name                (:site-cred/name site)
   :login_url           (:site-cred/login-url site)
   :username_field      (:site-cred/username-field site)
   :password_field      (:site-cred/password-field site)
   :form_selector       (:site-cred/form-selector site)
   :extra_fields        (:site-cred/extra-fields site)
   :autonomous_approved (boolean (autonomous/site-autonomous-approved? site))
   :username_configured (boolean (nonblank-str (:site-cred/username site)))
   :password_configured (boolean (nonblank-str (:site-cred/password site)))})

(defn- tool->admin-body
  [tool]
  {:id          (some-> (:tool/id tool) name)
   :name        (:tool/name tool)
   :description (:tool/description tool)
   :approval    (some-> (:tool/approval tool) name)
   :enabled     (boolean (:tool/enabled? tool))})

(defn- skill->body
  [skill]
  {:id          (some-> (:skill/id skill) name)
   :name        (:skill/name skill)
   :description (:skill/description skill)
   :version     (:skill/version skill)
   :tags        (->> (or (:skill/tags skill) [])
                     (map name)
                     sort
                     vec)
   :enabled     (boolean (:skill/enabled? skill))
   :source_format (some-> (:skill/source-format skill) name)
   :source_path   (:skill/source-path skill)
   :source_url    (:skill/source-url skill)
   :source_name   (:skill/source-name skill)
   :import_warnings (->> (or (:skill/import-warnings skill) [])
                         sort
                         vec)
   :imported_from_openclaw (boolean (:skill/imported-from-openclaw? skill))})

(defn- skill->admin-body
  [skill]
  (skill->body skill))

(defn- remote-bridge->admin-body
  [bridge]
  {:id               (some-> (:id bridge) name)
   :enabled          (boolean (:enabled? bridge))
   :instance_id      (:instance-id bridge)
   :instance_label   (:instance-label bridge)
   :relay_url        (:relay-url bridge)
   :public_key       (:public-key bridge)
   :keypair_ready    (boolean (:keypair-ready? bridge))
   :connected_at     (instant->str (:connected-at bridge))
   :last_seen_at     (instant->str (:last-seen-at bridge))
   :connection_state (some-> (:connection-state bridge) name)})

(defn- remote-device->admin-body
  [device]
  {:id           (some-> (:id device) str)
   :name         (:name device)
   :public_key   (:public-key device)
   :platform     (some-> (:platform device) name)
   :status       (some-> (:status device) name)
   :topics       (->> (or (:topics device) [])
                      (map name)
                      sort
                      vec)
   :muted        (boolean (:muted? device))
   :created_at   (instant->str (:created-at device))
   :last_seen_at (instant->str (:last-seen-at device))})

(defn- remote-event->admin-body
  [event]
  {:id           (some-> (:id event) str)
   :type         (some-> (:type event) name)
   :topic        (some-> (:topic event) name)
   :severity     (some-> (:severity event) name)
   :title        (:title event)
   :detail       (:detail event)
   :metadata     (:metadata event)
   :status       (some-> (:status event) name)
   :device_id    (some-> (:device-id event) str)
   :created_at   (instant->str (:created-at event))
   :delivered_at (instant->str (:delivered-at event))})

(defn- remote-snapshot-run->admin-body
  [run]
  {:id            (some-> (:id run) str)
   :schedule_id   (some-> (:schedule-id run) name)
   :schedule_name (:schedule-name run)
   :status        (some-> (:status run) name)
   :started_at    (instant->str (:started-at run))
   :finished_at   (instant->str (:finished-at run))
   :detail        (:detail run)})

(defn- remote-snapshot-attention->admin-body
  [item]
  {:type             (some-> (:type item) name)
   :severity         (some-> (:severity item) name)
   :title            (:title item)
   :detail           (:detail item)
   :schedule_id      (some-> (:schedule-id item) name)
   :service_id       (some-> (:service-id item) name)
   :oauth_account_id (some-> (:oauth-account-id item) name)})

(defn- remote-snapshot-running->admin-body
  [item]
  {:schedule_id   (some-> (:schedule-id item) name)
   :schedule_name (:schedule-name item)
   :phase         (some-> (:phase item) name)
   :checkpoint_at (instant->str (:checkpoint-at item))})

(defn- remote-snapshot->admin-body
  [snapshot]
  {:instance {:id      (get-in snapshot [:instance :id])
              :label   (get-in snapshot [:instance :label])
              :version (get-in snapshot [:instance :version])}
   :connectivity {:enabled          (boolean (get-in snapshot [:connectivity :enabled?]))
                  :relay_url        (get-in snapshot [:connectivity :relay-url])
                  :connection_state (some-> (get-in snapshot [:connectivity :connection-state]) name)
                  :connected_at     (instant->str (get-in snapshot [:connectivity :connected-at]))
                  :last_seen_at     (instant->str (get-in snapshot [:connectivity :last-seen-at]))}
   :running (mapv remote-snapshot-running->admin-body (get snapshot :running))
   :recent_failures (mapv remote-snapshot-run->admin-body (get snapshot :recent-failures))
   :recent_successes (mapv remote-snapshot-run->admin-body (get snapshot :recent-successes))
   :attention (mapv remote-snapshot-attention->admin-body (get snapshot :attention))})

(defn- session-scratch-pad
  [session-id pad-id]
  (let [pad (scratch/get-pad pad-id)]
    (when (and pad
               (= :session (:scope pad))
               (= session-id (:session-id pad)))
      pad)))

(defn- session-local-doc
  [session-id doc-id]
  (local-doc/get-session-doc session-id doc-id))

(defn- session-artifact
  [session-id artifact-id]
  (artifact/get-session-artifact session-id artifact-id))

(defn- handle-create-session []
  (let [sid (db/create-session! :http)]
    (wm/ensure-wm! sid)
    (touch-rest-session! sid)
    (json-response 200 {:session_id (str sid)})))

(defn- internal-server-error-response
  [^Throwable e]
  (json-response 500 {:error (or (throwable-message e) "internal server error")}))

(defn- chat-request
  [req]
  (let [data       (read-body req)
        message    (get data "message")
        session-id (get data "session_id")
        local-doc-ids (when (sequential? (get data "local_doc_ids"))
                        (vec (keep #(when (some? %) (str %))
                                   (get data "local_doc_ids"))))
        artifact-ids (when (sequential? (get data "artifact_ids"))
                       (vec (keep #(when (some? %) (str %))
                                  (get data "artifact_ids"))))]
    (cond
      (not message)
      {:response (json-response 400 {:error "missing 'message' field"})}

      (and session-id (not (session-exists? session-id)))
      {:response (json-response 404 {:error "unknown session id"})}

      (and session-id (not (session-active? session-id)))
      {:response (json-response 409 {:error "session closed"})}

      :else
      (let [sid (if session-id
                  (java.util.UUID/fromString session-id)
                  (db/create-session! :http))]
        (wm/ensure-wm! sid)
        (cancel-rest-session-finalizer! sid)
        (clear-session-status! sid)
        {:session-id sid
         :message    message
         :local-doc-ids local-doc-ids
         :artifact-ids artifact-ids}))))

(defn- process-chat!
  [{:keys [session-id message local-doc-ids artifact-ids]}]
  (try
    (let [response (agent/process-message session-id
                                          message
                                          :channel :http
                                          :local-doc-ids local-doc-ids
                                          :artifact-ids artifact-ids)]
      (json-response 200 {:session_id (str session-id)
                          :role       "assistant"
                          :content    response}))
    (finally
      (touch-rest-session! session-id))))

(defn- handle-chat-sync
  [chat]
  (process-chat! chat))

(defn- handle-chat-async
  [req chat]
  (http/as-channel
    req
    {:on-open
     (fn [ch]
       (future
         (let [response (try
                          (process-chat! chat)
                          (catch clojure.lang.ExceptionInfo e
                            (exception-response e))
                          (catch Exception e
                            (log/error e "Async HTTP chat request failed")
                            (internal-server-error-response e)))]
           (http/send! ch response))))}))

(defn- handle-chat [req]
  (let [{:keys [response] :as chat} (chat-request req)]
    (cond
      response
      response

      (:async-channel req)
      (handle-chat-async req chat)

      :else
      (handle-chat-sync chat))))

(defn- handle-get-status [session-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (do
      (touch-rest-session! session-id)
      (json-response 200 {:session_id session-id
                          :status     (status->body (get @session-statuses session-id))}))))

(defn- handle-get-approval [session-id]
  (if-not (parse-session-id session-id)
    (json-response 400 {:error "invalid session id"})
    (do
      (touch-rest-session! session-id)
      (if-let [approval (get @pending-approvals session-id)]
        (json-response 200 {:pending true
                            :approval (approval->body approval)})
        (json-response 200 {:pending false})))))

(defn- handle-submit-approval [session-id req]
  (if-not (parse-session-id session-id)
    (json-response 400 {:error "invalid session id"})
    (let [data        (read-body req)
          approval-id (get data "approval_id")
          decision    (get data "decision")
          current     (get @pending-approvals session-id)
          decision*   (case decision
                        "allow" :allow
                        "deny"  :deny
                        nil)]
      (cond
        (nil? current)
        (json-response 404 {:error "no pending approval"})

        (not= approval-id (:approval-id current))
        (json-response 409 {:error "stale approval id"})

        (nil? decision*)
        (json-response 400 {:error "invalid decision"})

        :else
        (do
          (deliver (:decision current) decision*)
          (clear-pending-approval! session-id approval-id)
          (touch-rest-session! session-id)
          (json-response 200 {:status "recorded"}))))))

(defn- handle-session-messages [session-id]
  (try
    (let [sid      (java.util.UUID/fromString session-id)
          messages (->> (db/session-messages sid)
                        (into [] (comp
                                   (filter #(#{:user :assistant} (:role %)))
                                   (map (fn [{:keys [role content created-at local-docs artifacts]}]
                                          {:role       (name role)
                                           :content    content
                                           :created_at (instant->str created-at)
                                           :local_docs (into [] (map local-doc-ref->body)
                                                             (or local-docs []))
                                           :artifacts  (into [] (map artifact-ref->body)
                                                             (or artifacts []))})))))]
      (touch-rest-session! session-id)
      (json-response 200 {:session_id session-id
                          :messages   messages}))
    (catch IllegalArgumentException _
      (json-response 400 {:error "invalid session id"}))))

(defn- handle-close-session [session-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (session-busy? session-id)
    (json-response 409 {:error "session is still processing a request"})

    :else
    (let [closed? (finalize-rest-session! session-id :explicit)]
      (json-response 200 {:session_id      (parse-session-id session-id)
                          :status          (if closed? "closed" "already_closed")
                          :already_closed  (not closed?)}))))

(defn- handle-history-sessions []
  (json-response 200
                 {:sessions (->> (db/list-sessions)
                                 (into [] (comp (filter #(contains? history-session-channels (:channel %)))
                                                (map history-session->body))))}))

(defn- handle-history-schedules []
  (json-response 200
                 {:schedules (->> (schedule/list-schedules)
                                  (sort-by (fn [sched]
                                             (or (date->millis (:last-run sched))
                                                 (date->millis (:next-run sched))
                                                 Long/MIN_VALUE))
                                           >)
                                  (into [] (map history-schedule->body)))}))

(defn- handle-history-schedule-runs
  [schedule-id]
  (try
    (let [sid   (parse-keyword-id schedule-id "schedule_id")
          sched (schedule/get-schedule sid)]
      (if-not sched
        (json-response 404 {:error "schedule not found"})
        (json-response 200
                       {:schedule (history-schedule->body sched)
                        :runs     (into [] (map history-run->body)
                                        (schedule/schedule-history sid 20))})))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-list-scratch-pads [session-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (do
      (touch-rest-session! session-id)
      (json-response 200
                     {:session_id session-id
                      :pads       (into [] (map scratch-metadata->body)
                                         (scratch/list-pads {:scope :session
                                                             :session-id session-id}))}))))

(defn- handle-create-scratch-pad [session-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (let [data (or (read-body req) {})
          pad  (scratch/create-pad! {:scope      :session
                                     :session-id session-id
                                     :title      (get data "title")
                                     :content    (get data "content")
                                     :mime       (get data "mime")})]
      (touch-rest-session! session-id)
      (json-response 201 {:session_id session-id
                          :pad        (scratch-pad->body pad)}))))

(defn- handle-get-scratch-pad [session-id pad-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (if-let [pad (session-scratch-pad session-id pad-id)]
      (do
        (touch-rest-session! session-id)
        (json-response 200 {:session_id session-id
                            :pad        (scratch-pad->body pad)}))
      (json-response 404 {:error "scratch pad not found"}))))

(defn- handle-save-scratch-pad [session-id pad-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response 404 {:error "scratch pad not found"})

    :else
    (let [data    (or (read-body req) {})
          updates (cond-> {}
                    (contains? data "title")            (assoc :title (get data "title"))
                    (contains? data "content")          (assoc :content (get data "content"))
                    (contains? data "mime")             (assoc :mime (get data "mime"))
                    (contains? data "expected_version") (assoc :expected-version
                                                               (get data "expected_version")))]
      (try
        (touch-rest-session! session-id)
        (json-response 200
                       {:session_id session-id
                        :pad        (scratch-pad->body (scratch/save-pad! pad-id updates))})
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type]} (ex-data e)]
            (case type
              :scratch/version-conflict
              (json-response 409 {:error "scratch pad version conflict"
                                  :details (select-keys (ex-data e)
                                                        [:expected-version :actual-version])})
              :scratch/not-found
              (json-response 404 {:error "scratch pad not found"})
              (json-response 400 {:error (.getMessage e)}))))))))

(defn- handle-edit-scratch-pad [session-id pad-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response 404 {:error "scratch pad not found"})

    :else
    (let [data      (or (read-body req) {})
          operation (if (map? (get data "operation"))
                      (get data "operation")
                      data)
          edit      (cond-> {:op (get operation "op")}
                      (contains? operation "text")          (assoc :text (get operation "text"))
                      (contains? operation "separator")     (assoc :separator (get operation "separator"))
                      (contains? operation "match")         (assoc :match (get operation "match"))
                      (contains? operation "replacement")   (assoc :replacement (get operation "replacement"))
                      (contains? operation "occurrence")    (assoc :occurrence (get operation "occurrence"))
                      (contains? operation "offset")        (assoc :offset (get operation "offset"))
                      (contains? operation "start_line")    (assoc :start-line (get operation "start_line"))
                      (contains? operation "end_line")      (assoc :end-line (get operation "end_line"))
                      (contains? data "expected_version")   (assoc :expected-version
                                                                   (get data "expected_version"))
                      (contains? operation "expected_version") (assoc :expected-version
                                                                     (get operation "expected_version")))]
      (try
        (touch-rest-session! session-id)
        (json-response 200
                       {:session_id session-id
                        :pad        (scratch-pad->body (scratch/edit-pad! pad-id edit))})
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type]} (ex-data e)]
            (case type
              :scratch/version-conflict
              (json-response 409 {:error "scratch pad version conflict"
                                  :details (select-keys (ex-data e)
                                                        [:expected-version :actual-version])})
              :scratch/not-found
              (json-response 404 {:error "scratch pad not found"})
              (json-response 400 {:error (.getMessage e)}))))))))

(defn- handle-delete-scratch-pad [session-id pad-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response 404 {:error "scratch pad not found"})

    :else
    (do
      (scratch/delete-pad! pad-id)
      (touch-rest-session! session-id)
      (json-response 200 {:status "deleted"
                          :session_id session-id
                          :pad_id pad-id}))))

(defn- parse-local-doc-upload
  [entry]
  {:name       (get entry "name")
   :media-type (get entry "media_type")
   :size-bytes (get entry "size_bytes")
   :source     (get entry "source")
   :bytes-base64 (get entry "bytes_base64")
   :text       (get entry "text")})

(defn- json-local-doc-upload-entries
  [data]
  (let [entries (cond
                  (sequential? (get data "documents"))
                  (get data "documents")

                  (map? data)
                  [data]

                  :else
                  [])]
    (->> entries
         (into [] (comp (filter map?)
                        (map parse-local-doc-upload))))))

(defn- multipart-local-doc-upload?
  [value]
  (and (map? value)
       (or (contains? value :tempfile)
           (contains? value "tempfile")
           (contains? value :filename)
           (contains? value "filename"))))

(defn- local-doc-part-bytes
  [part]
  (let [tempfile (or (:tempfile part) (get part "tempfile"))
        body     (or tempfile
                     (:bytes part)
                     (get part "bytes")
                     (:stream part)
                     (get part "stream"))]
    (when-not body
      (throw (ex-info "missing uploaded file bytes"
                      {:type :local-doc/missing-file-bytes
                       :name (or (:filename part) (get part "filename"))})))
    (try
      (read-body-bytes body)
      (finally
        (when tempfile
          (.delete ^java.io.File tempfile))))))

(defn- multipart-local-doc-upload-entry
  [part]
  {:name       (or (:filename part) (get part "filename"))
   :media-type (or (:content-type part) (get part "content-type"))
   :size-bytes (or (:size part) (get part "size"))
   :source     (get part "source")
   :bytes      (local-doc-part-bytes part)})

(defn- multipart-local-doc-upload-entries
  [req]
  (let [params  (or (:multipart-params req) (:params req))
        uploads (or (get params "documents")
                    (get params :documents)
                    (get params "documents[]")
                    (get params :documents[]))]
    (->> (cond
           (sequential? uploads) uploads
           (some? uploads)       [uploads]
           :else                 [])
         (into [] (comp (filter multipart-local-doc-upload?)
                        (map multipart-local-doc-upload-entry))))))

(defn- local-doc-upload-entries
  [req]
  (if (multipart-form-request? req)
    (multipart-local-doc-upload-entries req)
    (json-local-doc-upload-entries (or (read-body req) {}))))

(defn- local-doc-error->body
  [upload ^Throwable e]
  {:name  (get upload :name)
   :error (throwable-message e)
   :code  (some-> (ex-data e) :type name)})

(defn- handle-list-local-docs [session-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (do
      (touch-rest-session! session-id)
      (json-response 200
                     {:session_id session-id
                      :documents  (into [] (map local-doc-metadata->body)
                                         (local-doc/list-docs session-id))}))))

(defn- handle-create-local-docs [session-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (let [uploads (local-doc-upload-entries req)]
      (if-not (seq uploads)
        (json-response 400 {:error "missing local documents"})
        (let [{:keys [documents errors]}
              (reduce (fn [acc upload]
                        (try
                          (update acc :documents conj
                                  (local-doc-metadata->body
                                    (local-doc/save-upload!
                                      {:session-id session-id
                                      :name       (:name upload)
                                      :media-type (:media-type upload)
                                      :size-bytes (:size-bytes upload)
                                      :source     (:source upload)
                                      :bytes      (:bytes upload)
                                      :bytes-base64 (:bytes-base64 upload)
                                      :text       (:text upload)})))
                          (catch clojure.lang.ExceptionInfo e
                            (let [failed-doc (try
                                               (local-doc/save-failed-upload!
                                                 {:session-id session-id
                                                  :name       (:name upload)
                                                  :media-type (:media-type upload)
                                                  :size-bytes (:size-bytes upload)
                                                  :source     (:source upload)
                                                  :bytes      (:bytes upload)
                                                  :bytes-base64 (:bytes-base64 upload)
                                                  :text       (:text upload)}
                                                 e)
                                               (catch Exception _
                                                 nil))]
                              (cond-> (update acc :errors conj (local-doc-error->body upload e))
                                failed-doc
                                (update :documents conj (local-doc-metadata->body failed-doc)))))))
                      {:documents [] :errors []}
                      uploads)
              status (if (seq documents) 201 400)]
          (touch-rest-session! session-id)
          (json-response status
                         {:session_id session-id
                          :documents  documents
                          :errors     errors}))))))

(defn- handle-get-local-doc [session-id doc-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (if-let [doc (session-local-doc session-id doc-id)]
      (do
        (touch-rest-session! session-id)
        (json-response 200 {:session_id session-id
                            :document   (local-doc->body doc)}))
      (json-response 404 {:error "local document not found"}))))

(defn- handle-delete-local-doc [session-id doc-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-local-doc session-id doc-id))
    (json-response 404 {:error "local document not found"})

    :else
    (do
      (local-doc/delete-doc! session-id doc-id)
      (touch-rest-session! session-id)
      (json-response 200 {:status "deleted"
                          :session_id session-id
                          :doc_id doc-id}))))

(defn- handle-create-local-doc-scratch-pad [session-id doc-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-local-doc session-id doc-id))
    (json-response 404 {:error "local document not found"})

    :else
    (let [{:keys [pad]} (local-doc/create-scratch-pad-from-doc! session-id doc-id)]
      (touch-rest-session! session-id)
      (json-response 201 {:session_id session-id
                          :pad        (scratch-pad->body pad)}))))

(defn- handle-create-artifact-scratch-pad [session-id artifact-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-artifact session-id artifact-id))
    (json-response 404 {:error "artifact not found"})

    :else
    (let [{:keys [pad]} (artifact/create-scratch-pad-from-artifact! session-id artifact-id)]
      (touch-rest-session! session-id)
      (json-response 201 {:session_id session-id
                          :pad        (scratch-pad->body pad)}))))

(defn- artifact-create-spec
  [data]
  (let [payload (if (map? (get data "artifact"))
                  (get data "artifact")
                  data)]
    (cond-> {:source (or (some-> (get payload "source") nonblank-str keyword)
                         :manual)}
      (contains? payload "name")
      (assoc :name (get payload "name"))

      (contains? payload "title")
      (assoc :title (get payload "title"))

      (or (contains? payload "kind")
          (contains? payload "format"))
      (assoc :kind (or (get payload "kind")
                       (get payload "format")))

      (contains? payload "media_type")
      (assoc :media-type (get payload "media_type"))

      (contains? payload "content")
      (assoc :content (get payload "content"))

      (contains? payload "bytes_base64")
      (assoc :bytes-base64 (get payload "bytes_base64"))

      (contains? payload "preview")
      (assoc :preview (get payload "preview"))

      (contains? payload "rows")
      (assoc :rows (get payload "rows"))

      (contains? payload "data")
      (assoc :data (get payload "data"))

      (contains? payload "meta")
      (assoc :meta (get payload "meta")))))

(defn- handle-list-artifacts [session-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (do
      (touch-rest-session! session-id)
      (json-response 200
                     {:session_id session-id
                      :artifacts  (into [] (map artifact-metadata->body)
                                         (artifact/list-artifacts session-id))}))))

(defn- handle-create-artifact [session-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (try
      (let [data    (or (read-body req) {})
            created (artifact/create-artifact! (assoc (artifact-create-spec data)
                                                      :session-id session-id))]
        (touch-rest-session! session-id)
        (json-response 201 {:session_id session-id
                            :artifact   (artifact->body created)}))
      (catch clojure.lang.ExceptionInfo e
        (json-response (or (:status (ex-data e)) 400)
                       {:error (.getMessage e)})))))

(defn- handle-get-artifact [session-id artifact-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (if-let [artifact (session-artifact session-id artifact-id)]
      (do
        (touch-rest-session! session-id)
        (json-response 200 {:session_id session-id
                            :artifact   (artifact->body artifact)}))
      (json-response 404 {:error "artifact not found"}))))

(defn- handle-download-artifact [session-id artifact-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (if-let [{:keys [name media-type bytes]} (artifact/artifact-download-data session-id artifact-id)]
      (do
        (touch-rest-session! session-id)
        (download-response name media-type (or bytes (byte-array 0))))
      (json-response 404 {:error "artifact not found"}))))

(defn- handle-delete-artifact [session-id artifact-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-artifact session-id artifact-id))
    (json-response 404 {:error "artifact not found"})

    :else
    (do
      (artifact/delete-artifact! session-id artifact-id)
      (touch-rest-session! session-id)
      (json-response 200 {:status "deleted"
                          :session_id session-id
                          :artifact_id artifact-id}))))

(defn- handle-admin-config [_req]
  (json-response
    200
    {:providers (->> (db/list-providers)
                     (into [] (map provider->admin-body))
                     sort-by-name)
     :conversation_context (conversation-context->admin-body)
     :memory_retention (memory-retention->admin-body)
     :knowledge_decay (knowledge-decay->admin-body)
     :local_doc_summarization (local-doc-summarization->admin-body)
     :database_backup (database-backup->admin-body)
     :llm_workloads (into [] (map (fn [{:keys [id label description]}]
                                    {:id          (name id)
                                     :label       label
                                     :description description}))
                          (llm/workload-routes))
     :oauth_provider_templates (->> (oauth-template/list-templates)
                                    (into [] (map oauth-template->admin-body))
                                    sort-by-name)
     :oauth_accounts (->> (db/list-oauth-accounts)
                          (into [] (map oauth-account->admin-body))
                          sort-by-name)
     :services  (->> (db/list-services)
                     (into [] (map service->admin-body))
                     sort-by-name)
     :sites     (->> (db/list-site-creds)
                     (into [] (map site->admin-body))
                     sort-by-name)
     :tools     (->> (db/list-tools)
                     (into [] (map tool->admin-body))
                     sort-by-name)
     :skills    (->> (db/list-skills)
                     (into [] (map skill->body))
                     sort-by-name)
     :remote_bridge   (remote-bridge->admin-body (remote-bridge/bridge-config))
     :remote_devices  (into [] (map remote-device->admin-body) (remote-bridge/list-devices))
     :remote_events   (into [] (map remote-event->admin-body) (remote-bridge/list-events 20))
     :remote_snapshot (remote-snapshot->admin-body (remote-bridge/status-snapshot))}))

(defn- handle-save-provider [req]
  (try
    (let [data         (or (read-body req) {})
          provider-id  (parse-keyword-id (get data "id") "id")
          base-url     (nonblank-str (get data "base_url"))
          model        (nonblank-str (get data "model"))
          name         (or (nonblank-str (get data "name"))
                           (name provider-id))
          api-key      (nonblank-str (get data "api_key"))
          vision?      (when (contains? data "vision")
                         (true? (get data "vision")))
          workloads    (when (contains? data "workloads")
                         (parse-provider-workloads (get data "workloads")))
          system-prompt-budget (parse-optional-positive-long (get data "system_prompt_budget")
                                                             "system_prompt_budget")
          history-budget (parse-optional-positive-long (get data "history_budget")
                                                       "history_budget")
          make-default (true? (get data "default"))
          has-default? (some? (db/get-default-provider))]
      (when-not base-url
        (throw (ex-info "missing 'base_url' field" {:field "base_url"})))
      (when-not model
        (throw (ex-info "missing 'model' field" {:field "model"})))
      (db/upsert-provider! (cond-> {:id       provider-id
                                    :name     name
                                    :base-url base-url
                                    :model    model
                                    :system-prompt-budget system-prompt-budget
                                    :history-budget history-budget}
                             (contains? data "vision")
                             (assoc :vision? vision?)
                             (contains? data "workloads")
                             (assoc :workloads workloads)
                             api-key
                             (assoc :api-key api-key)))
      (when (or make-default (not has-default?))
        (db/set-default-provider! provider-id))
      (json-response 200 {:provider (provider->admin-body (db/get-provider provider-id))}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-save-memory-retention [req]
  (try
    (let [data                 (or (read-body req) {})
          full-resolution-days (when (contains? data "full_resolution_days")
                                 (parse-optional-positive-long (get data "full_resolution_days")
                                                               "full_resolution_days"))
          decay-half-life-days (when (contains? data "decay_half_life_days")
                                 (parse-optional-positive-long (get data "decay_half_life_days")
                                                               "decay_half_life_days"))
          retained-count       (when (contains? data "retained_count")
                                 (parse-optional-positive-long (get data "retained_count")
                                                               "retained_count"))]
      (when (contains? data "full_resolution_days")
        (save-config-override! :memory/episode-full-resolution-ms
                               (days->ms full-resolution-days)))
      (when (contains? data "decay_half_life_days")
        (save-config-override! :memory/episode-decay-half-life-ms
                               (days->ms decay-half-life-days)))
      (when (contains? data "retained_count")
        (save-config-override! :memory/episode-retained-decayed-count
                               retained-count))
      (json-response 200 {:memory_retention (memory-retention->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-save-conversation-context [req]
  (try
    (let [data                         (or (read-body req) {})
          recent-history-message-limit (when (contains? data "recent_history_message_limit")
                                         (parse-optional-positive-long (get data "recent_history_message_limit")
                                                                       "recent_history_message_limit"))]
      (when (contains? data "recent_history_message_limit")
        (save-config-override! :context/recent-history-message-limit
                               recent-history-message-limit))
      (json-response 200 {:conversation_context (conversation-context->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- parse-optional-bounded-double
  [value field-name]
  (let [text (nonblank-str value)]
    (when text
      (try
        (let [parsed (Double/parseDouble text)]
          (when-not (<= 0.0 parsed 1.0)
            (throw (ex-info (str "'" field-name "' must be between 0.0 and 1.0")
                            {:field field-name
                             :value value})))
          parsed)
        (catch NumberFormatException _
          (throw (ex-info (str "'" field-name "' must be between 0.0 and 1.0")
                          {:field field-name
                           :value value})))))))

(defn- handle-save-knowledge-decay [req]
  (try
    (let [data                      (or (read-body req) {})
          grace-period-days         (when (contains? data "grace_period_days")
                                      (parse-optional-positive-long (get data "grace_period_days")
                                                                    "grace_period_days"))
          half-life-days            (when (contains? data "half_life_days")
                                      (parse-optional-positive-long (get data "half_life_days")
                                                                    "half_life_days"))
          min-confidence            (when (contains? data "min_confidence")
                                      (parse-optional-bounded-double (get data "min_confidence")
                                                                     "min_confidence"))
          maintenance-interval-days (when (contains? data "maintenance_interval_days")
                                      (parse-optional-positive-long (get data "maintenance_interval_days")
                                                                    "maintenance_interval_days"))
          archive-after-bottom-days (when (contains? data "archive_after_bottom_days")
                                      (parse-optional-positive-long (get data "archive_after_bottom_days")
                                                                    "archive_after_bottom_days"))]
      (when (contains? data "grace_period_days")
        (save-config-override! :memory/knowledge-decay-grace-period-ms
                               (days->ms grace-period-days)))
      (when (contains? data "half_life_days")
        (save-config-override! :memory/knowledge-decay-half-life-ms
                               (days->ms half-life-days)))
      (when (contains? data "min_confidence")
        (save-config-override! :memory/knowledge-decay-min-confidence
                               min-confidence))
      (when (contains? data "maintenance_interval_days")
        (save-config-override! :memory/knowledge-decay-maintenance-step-ms
                               (days->ms maintenance-interval-days)))
      (when (contains? data "archive_after_bottom_days")
        (save-config-override! :memory/knowledge-decay-archive-after-bottom-ms
                               (days->ms archive-after-bottom-days)))
      (json-response 200 {:knowledge_decay (knowledge-decay->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- parse-summary-backend
  [value field-name]
  (let [backend (some-> value nonblank-str keyword)]
    (when backend
      (when-not (contains? #{:local :external} backend)
        (throw (ex-info (str "'" field-name "' must be one of: local, external")
                        {:field field-name
                         :value value})))
      backend)))

(defn- parse-optional-provider-id
  [value field-name]
  (when-let [provider-id-str (nonblank-str value)]
    (let [provider-id (keyword provider-id-str)]
      (when-not (db/get-provider provider-id)
        (throw (ex-info (str "'" field-name "' must reference an existing provider")
                        {:field field-name
                         :value value})))
      provider-id)))

(defn- handle-save-local-doc-summarization [req]
  (try
    (let [data                     (or (read-body req) {})
          enabled?                 (when (contains? data "model_summaries_enabled")
                                     (true? (get data "model_summaries_enabled")))
          backend                  (when (contains? data "model_summary_backend")
                                     (parse-summary-backend (get data "model_summary_backend")
                                                            "model_summary_backend"))
          provider-id              (when (contains? data "model_summary_provider_id")
                                     (parse-optional-provider-id (get data "model_summary_provider_id")
                                                                 "model_summary_provider_id"))
          chunk-summary-max-tokens (when (contains? data "chunk_summary_max_tokens")
                                     (parse-optional-positive-long (get data "chunk_summary_max_tokens")
                                                                   "chunk_summary_max_tokens"))
          doc-summary-max-tokens   (when (contains? data "doc_summary_max_tokens")
                                     (parse-optional-positive-long (get data "doc_summary_max_tokens")
                                                                   "doc_summary_max_tokens"))
          effective-provider-id    (when (= backend :external) provider-id)]
      (when (contains? data "model_summaries_enabled")
        (save-config-override! :local-doc/model-summaries-enabled? enabled?))
      (when (contains? data "model_summary_backend")
        (save-config-override! :local-doc/model-summary-backend
                               (some-> backend name)))
      (when (contains? data "model_summary_provider_id")
        (save-config-override! :local-doc/model-summary-provider-id
                               (some-> effective-provider-id name)))
      (when (contains? data "chunk_summary_max_tokens")
        (save-config-override! :local-doc/chunk-summary-max-tokens
                               chunk-summary-max-tokens))
      (when (contains? data "doc_summary_max_tokens")
        (save-config-override! :local-doc/doc-summary-max-tokens
                               doc-summary-max-tokens))
      (json-response 200 {:local_doc_summarization (local-doc-summarization->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-save-database-backup [req]
  (try
    (let [data           (or (read-body req) {})
          enabled?       (when (contains? data "enabled")
                           (true? (get data "enabled")))
          directory      (when (contains? data "directory")
                           (nonblank-str (get data "directory")))
          interval-hours (when (contains? data "interval_hours")
                           (parse-optional-positive-long (get data "interval_hours")
                                                         "interval_hours"))
          retain-count   (when (contains? data "retain_count")
                           (parse-optional-positive-long (get data "retain_count")
                                                         "retain_count"))]
      (when (contains? data "enabled")
        (save-config-override! :backup/enabled? enabled?))
      (when (contains? data "directory")
        (save-config-override! :backup/directory directory))
      (when (contains? data "interval_hours")
        (save-config-override! :backup/interval-hours interval-hours))
      (when (contains? data "retain_count")
        (save-config-override! :backup/retain-count retain-count))
      (json-response 200 {:database_backup (database-backup->admin-body)}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-save-remote-bridge [req]
  (try
    (let [data          (or (read-body req) {})
          enabled?      (if (contains? data "enabled")
                          (true? (get data "enabled"))
                          false)
          instance-label (nonblank-str (get data "instance_label"))
          relay-url     (get data "relay_url")
          bridge        (remote-bridge/save-bridge-config!
                          {:enabled? enabled?
                           :instance-label instance-label
                           :relay-url relay-url})]
      (json-response 200 {:remote_bridge (remote-bridge->admin-body bridge)
                          :remote_snapshot (remote-snapshot->admin-body
                                             (remote-bridge/status-snapshot))}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-pair-remote-device [req]
  (try
    (let [data          (or (read-body req) {})
          pairing-token (nonblank-str (get data "pairing_token"))]
      (when-not pairing-token
        (throw (ex-info "missing 'pairing_token' field"
                        {:field "pairing_token"})))
      (let [device (remote-bridge/pair-device! pairing-token)]
        (json-response 200 {:device (remote-device->admin-body device)
                            :remote_devices (mapv remote-device->admin-body
                                                  (remote-bridge/list-devices))})))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-revoke-remote-device [device-id]
  (try
    (let [device (remote-bridge/revoke-device! device-id)]
      (json-response 200 {:device (remote-device->admin-body device)
                          :remote_devices (mapv remote-device->admin-body
                                                (remote-bridge/list-devices))}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-save-service [req]
  (try
    (let [data              (or (read-body req) {})
          service-id        (parse-keyword-id (get data "id") "id")
          existing          (db/get-service service-id)
          base-url          (nonblank-str (get data "base_url"))
          name              (or (nonblank-str (get data "name"))
                                (name service-id))
          auth-type         (parse-auth-type (get data "auth_type"))
          entered-auth-key  (nonblank-str (get data "auth_key"))
          rate-limit-per-minute (parse-optional-positive-long (get data "rate_limit_per_minute")
                                                              "rate_limit_per_minute")
          autonomous-approved? (when (contains? data "autonomous_approved")
                                 (true? (get data "autonomous_approved")))
          enabled?          (if (contains? data "enabled")
                              (true? (get data "enabled"))
                              true)
          oauth-account-id  (when (= :oauth-account auth-type)
                              (let [value (or (nonblank-str (get data "oauth_account"))
                                              (some-> (:service/oauth-account existing) name))]
                                (when-not value
                                  (throw (ex-info "oauth_account is required for oauth-account auth_type"
                                                  {:field "oauth_account"})))
                                (let [account-id (keyword value)]
                                  (when-not (db/get-oauth-account account-id)
                                    (throw (ex-info "unknown oauth_account"
                                                    {:field "oauth_account"
                                                     :value value})))
                                  account-id)))
          entered-header    (nonblank-str (get data "auth_header"))
          auth-header       (when (#{:api-key-header :query-param} auth-type)
                              (or entered-header
                                  (:service/auth-header existing)))
          auth-key          (when-not (= :oauth-account auth-type)
                              (or entered-auth-key
                                  (:service/auth-key existing)
                                  ""))]
      (when-not base-url
        (throw (ex-info "missing 'base_url' field" {:field "base_url"})))
      (when (and (#{:api-key-header :query-param} auth-type)
                 (nil? auth-header))
        (throw (ex-info "auth_header is required for the selected auth_type"
                        {:field "auth_header"})))
      (db/save-service! {:id          service-id
                         :name        name
                         :base-url    base-url
                         :auth-type   auth-type
                         :auth-key    (or auth-key "")
                         :auth-header auth-header
                         :oauth-account oauth-account-id
                         :rate-limit-per-minute rate-limit-per-minute
                         :autonomous-approved? autonomous-approved?
                         :enabled?    enabled?})
      (json-response 200 {:service (service->admin-body (db/get-service service-id))}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-save-oauth-account [req]
  (try
    (let [data           (or (read-body req) {})
          account-id     (parse-keyword-id (get data "id") "id")
          existing       (db/get-oauth-account account-id)
          name           (or (nonblank-str (get data "name"))
                             (name account-id))
          authorize-url  (nonblank-str (get data "authorize_url"))
          token-url      (nonblank-str (get data "token_url"))
          client-id      (nonblank-str (get data "client_id"))
          client-secret  (or (nonblank-str (get data "client_secret"))
                             (:oauth.account/client-secret existing)
                             "")
          provider-template-id (if (contains? data "provider_template")
                                 (some-> (get data "provider_template") nonblank-str keyword)
                                 (:oauth.account/provider-template existing))
          scopes         (or (nonblank-str (get data "scopes")) "")
          redirect-uri   (nonblank-str (get data "redirect_uri"))
          auth-params    (parse-json-object-string (get data "auth_params") "auth_params")
          token-params   (parse-json-object-string (get data "token_params") "token_params")
          autonomous-approved? (when (contains? data "autonomous_approved")
                                 (true? (get data "autonomous_approved")))]
      (when-not authorize-url
        (throw (ex-info "missing 'authorize_url' field" {:field "authorize_url"})))
      (when-not token-url
        (throw (ex-info "missing 'token_url' field" {:field "token_url"})))
      (when-not client-id
        (throw (ex-info "missing 'client_id' field" {:field "client_id"})))
      (when (and provider-template-id
                 (nil? (oauth-template/get-template provider-template-id)))
        (throw (ex-info "unknown provider_template"
                        {:field "provider_template"
                         :value (name provider-template-id)})))
      (db/save-oauth-account! {:id            account-id
                               :name          name
                               :authorize-url authorize-url
                               :token-url     token-url
                               :client-id     client-id
                               :client-secret client-secret
                               :provider-template provider-template-id
                               :scopes        scopes
                               :redirect-uri  redirect-uri
                               :auth-params   auth-params
                               :token-params  token-params
                               :autonomous-approved? autonomous-approved?
                               :access-token  (:oauth.account/access-token existing)
                               :refresh-token (:oauth.account/refresh-token existing)
                               :token-type    (:oauth.account/token-type existing)
                               :expires-at    (:oauth.account/expires-at existing)
                               :connected-at  (:oauth.account/connected-at existing)})
      (json-response 200 {:oauth_account (oauth-account->admin-body
                                           (db/get-oauth-account account-id))}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-delete-oauth-account [account-id]
  (try
    (let [oauth-id (parse-keyword-id account-id "oauth_account_id")]
      (cond
        (nil? (db/get-oauth-account oauth-id))
        (json-response 404 {:error "oauth account not found"})

        (db/oauth-account-in-use? oauth-id)
        (json-response 409 {:error "oauth account is still referenced by a service"})

        :else
        (do
          (db/remove-oauth-account! oauth-id)
          (json-response 200 {:status "deleted"
                              :oauth_account_id (name oauth-id)}))))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-start-oauth-connect [account-id req]
  (try
    (let [oauth-id    (parse-keyword-id account-id "oauth_account_id")
          callback-url (str (or (request-base-url req)
                                (throw (ex-info "cannot determine callback base URL"
                                                {:field "host"})))
                            "/oauth/callback")
          started     (oauth/start-authorization! oauth-id callback-url)]
      (json-response 200 {:oauth_account_id (name oauth-id)
                          :authorization_url (:authorization-url started)
                          :redirect_uri (:redirect-uri started)}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-refresh-oauth-account [account-id]
  (try
    (let [oauth-id (parse-keyword-id account-id "oauth_account_id")
          account  (oauth/refresh-account! oauth-id)]
      (json-response 200 {:oauth_account (oauth-account->admin-body account)}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- oauth-callback-page
  [status title message account-id]
  (let [title*   (escape-html title)
        message* (escape-html message)
        account* (some-> account-id name escape-html)]
    (str "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">"
       "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
       "<title>Xia OAuth</title>"
       "<style>body{margin:0;font-family:\"Avenir Next\",\"Segoe UI\",sans-serif;background:#f5efe3;color:#172119;display:grid;place-items:center;min-height:100vh;padding:24px;}main{max-width:36rem;background:rgba(255,252,246,.96);border:1px solid rgba(23,33,25,.12);border-radius:24px;padding:28px;box-shadow:0 20px 50px rgba(23,33,25,.12);}h1{margin:0 0 12px;font-size:2rem;}p{line-height:1.6;margin:0 0 10px;}code{font-family:\"SFMono-Regular\",Consolas,monospace;background:rgba(23,33,25,.06);padding:2px 6px;border-radius:8px;}</style>"
       "</head><body><main><h1>" title* "</h1><p>" message* "</p>"
       (when account*
         (str "<p>OAuth account: <code>" account* "</code></p>"))
       "<p>You can close this window and return to Xia.</p>"
       "<script>"
       "try {"
       "  if (window.opener && window.opener !== window) {"
       "    window.opener.postMessage({type:'xia-oauth-complete', status:" (json/write-json-str (name status)) ", account_id:" (json/write-json-str (some-> account-id name)) "}, window.location.origin);"
       "  }"
       "} catch (_err) {}"
       "setTimeout(() => { try { window.close(); } catch (_err) {} }, 1200);"
       "</script></main></body></html>")))

(defn- handle-oauth-callback [req]
  (let [params            (parse-query-string (:query-string req))
        state             (get params "state")
        pending-account-id (some-> (and (seq state) (oauth/callback-account-id state)) name)
        code              (get params "code")
        error-code        (get params "error")
        error-description (or (get params "error_description") error-code)]
    (cond
      (not (seq state))
      (html-response (oauth-callback-page "error"
                                          "OAuth failed"
                                          "Missing authorization state."
                                          nil))

      (seq error-code)
      (html-response (oauth-callback-page "error"
                                          "OAuth was not completed"
                                          (str "Provider returned: " error-description)
                                          pending-account-id))

      (not (seq code))
      (html-response (oauth-callback-page "error"
                                          "OAuth failed"
                                          "Missing authorization code."
                                          pending-account-id))

      :else
      (try
        (let [account (oauth/complete-authorization! state code)]
          (html-response (oauth-callback-page "ok"
                                              "OAuth connected"
                                              "Xia stored the new access token and can now use this account for online work."
                                              (some-> (:oauth.account/id account) name))))
        (catch clojure.lang.ExceptionInfo e
          (html-response (oauth-callback-page "error"
                                              "OAuth failed"
                                              (.getMessage e)
                                              pending-account-id)))))))

(defn- handle-save-site [req]
  (try
    (let [data            (or (read-body req) {})
          site-id         (parse-keyword-id (get data "id") "id")
          existing        (db/get-site-cred site-id)
          login-url       (nonblank-str (get data "login_url"))
          name            (or (nonblank-str (get data "name"))
                              (name site-id))
          username-field  (or (nonblank-str (get data "username_field"))
                              "username")
          password-field  (or (nonblank-str (get data "password_field"))
                              "password")
          username        (or (nonblank-str (get data "username"))
                              (:site-cred/username existing)
                              "")
          password        (or (nonblank-str (get data "password"))
                              (:site-cred/password existing)
                              "")
          form-selector   (nonblank-str (get data "form_selector"))
          extra-fields    (parse-extra-fields (get data "extra_fields"))
          autonomous-approved? (when (contains? data "autonomous_approved")
                                 (true? (get data "autonomous_approved")))]
      (when-not login-url
        (throw (ex-info "missing 'login_url' field" {:field "login_url"})))
      (db/save-site-cred! {:id             site-id
                           :name           name
                           :login-url      login-url
                           :username-field username-field
                           :password-field password-field
                           :username       username
                           :password       password
                           :form-selector  form-selector
                           :extra-fields   extra-fields
                           :autonomous-approved? autonomous-approved?})
      (json-response 200 {:site (site->admin-body (db/get-site-cred site-id))}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-delete-site [site-id]
  (try
    (let [site-key (parse-keyword-id site-id "site_id")]
      (if (db/get-site-cred site-key)
        (do
          (db/remove-site-cred! site-key)
          (json-response 200 {:status "deleted"
                              :site_id (name site-key)}))
        (json-response 404 {:error "site credential not found"})))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-import-openclaw-skill [req]
  (try
    (let [data    (or (read-body req) {})
          source  (nonblank-str (get data "source"))
          strict? (if (contains? data "strict")
                    (true? (get data "strict"))
                    true)]
      (when-not source
        (throw (ex-info "missing 'source' field" {:field "source"})))
      (let [report (openclaw-skill/import-openclaw-source! source :strict? strict?)
            skill  (db/get-skill (:skill-id report))]
        (json-response 200
                       {:import {:status         (some-> (:status report) name)
                                 :skill_id       (some-> (:skill-id report) name)
                                 :name           (:name report)
                                 :warnings       (vec (:warnings report))
                                 :ignored_fields (vec (:ignored-fields report))
                                 :resources      (mapv (fn [{:keys [path size-bytes]}]
                                                         {:path path
                                                          :size_bytes size-bytes})
                                                       (:resources report))
                                 :tool_aliases   (mapv (fn [{:keys [id from to]}]
                                                         {:id   (some-> id name)
                                                          :from from
                                                          :to   to})
                                                       (:tool-aliases report))
                                 :source         {:format (some-> (get-in report [:source :format]) name)
                                                  :path   (get-in report [:source :path])
                                                  :url    (get-in report [:source :url])
                                                  :name   (get-in report [:source :name])}}
                        :skill  (skill->body skill)})))
    (catch clojure.lang.ExceptionInfo e
      (exception-response e))))

(defn- handle-skills [_req]
  (json-response 200 {:skills (mapv skill->body (db/list-skills))}))

(defn- handle-health [_req]
  (json-response 200 {:status "ok" :version "0.1.0"}))

(defn- handle-home [_req]
  (if-let [html (read-resource "index.html")]
    (assoc-in (html-response html)
              [:headers "Set-Cookie"]
              (session-cookie-header))
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
          status-match       (re-matches #"/sessions/([0-9a-fA-F-]+)/status" uri)
          approval-match     (re-matches #"/sessions/([0-9a-fA-F-]+)/approval" uri)
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
          admin-remote-device-match (re-matches #"/admin/remote-bridge/devices/([0-9a-fA-F-]+)" uri)
          admin-site-match   (re-matches #"/admin/sites/([^/]+)" uri)
          admin-oauth-match  (re-matches #"/admin/oauth-accounts/([^/]+)" uri)
          admin-oauth-connect-match (re-matches #"/admin/oauth-accounts/([^/]+)/connect" uri)
          admin-oauth-refresh-match (re-matches #"/admin/oauth-accounts/([^/]+)/refresh" uri)]
      (cond
        (and (= method :get) (= uri "/"))
        (handle-home req)

        (and (= method :get) (contains? web-static-assets uri))
        (static-asset-response uri)

        (and (= method :get) (= uri "/oauth/callback"))
        (handle-oauth-callback req)

        ;; WebSocket upgrade
        (and (= uri "/ws") (http/websocket-handshake-check req))
        (ws-handler req)

        ;; REST
        (and (= method :post) (= uri "/sessions"))
        (protected-route-response req handle-create-session)

        (and (= method :post) (= uri "/chat"))
        (protected-route-response req #(handle-chat req))

        (and (= method :delete) session-close-match)
        (protected-route-response req #(handle-close-session (second session-close-match)))

        (and (= method :get) status-match)
        (protected-route-response req #(handle-get-status (second status-match)))

        (and (= method :get) approval-match)
        (protected-route-response req #(handle-get-approval (second approval-match)))

        (and (= method :post) approval-match)
        (protected-route-response req #(handle-submit-approval (second approval-match) req))

        (and (= method :get) session-match)
        (protected-route-response req #(handle-session-messages (second session-match)))

        (and (= method :get) (= uri "/history/sessions"))
        (protected-route-response req handle-history-sessions)

        (and (= method :get) (= uri "/history/schedules"))
        (protected-route-response req handle-history-schedules)

        (and (= method :get) history-schedule-match)
        (protected-route-response req #(handle-history-schedule-runs (second history-schedule-match)))

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

        (and (= method :get) (= uri "/admin/config"))
        (protected-route-response req #(handle-admin-config req))

        (and (= method :post) (= uri "/admin/providers"))
        (protected-route-response req #(handle-save-provider req))

        (and (= method :post) (= uri "/admin/memory-retention"))
        (protected-route-response req #(handle-save-memory-retention req))

        (and (= method :post) (= uri "/admin/context"))
        (protected-route-response req #(handle-save-conversation-context req))

        (and (= method :post) (= uri "/admin/knowledge-decay"))
        (protected-route-response req #(handle-save-knowledge-decay req))

        (and (= method :post) (= uri "/admin/local-doc-summarization"))
        (protected-route-response req #(handle-save-local-doc-summarization req))

        (and (= method :post) (= uri "/admin/database-backup"))
        (protected-route-response req #(handle-save-database-backup req))

        (and (= method :post) (= uri "/admin/remote-bridge"))
        (protected-route-response req #(handle-save-remote-bridge req))

        (and (= method :post) (= uri "/admin/remote-bridge/pair"))
        (protected-route-response req #(handle-pair-remote-device req))

        (and (= method :delete) admin-remote-device-match)
        (protected-route-response req #(handle-revoke-remote-device (second admin-remote-device-match)))

        (and (= method :post) (= uri "/admin/oauth-accounts"))
        (protected-route-response req #(handle-save-oauth-account req))

        (and (= method :post) admin-oauth-connect-match)
        (protected-route-response req #(handle-start-oauth-connect (second admin-oauth-connect-match) req))

        (and (= method :post) admin-oauth-refresh-match)
        (protected-route-response req #(handle-refresh-oauth-account (second admin-oauth-refresh-match)))

        (and (= method :delete) admin-oauth-match)
        (protected-route-response req #(handle-delete-oauth-account (second admin-oauth-match)))

        (and (= method :post) (= uri "/admin/services"))
        (protected-route-response req #(handle-save-service req))

	        (and (= method :post) (= uri "/admin/sites"))
	        (protected-route-response req #(handle-save-site req))

	        (and (= method :post) (= uri "/admin/skills/import-openclaw"))
	        (protected-route-response req #(handle-import-openclaw-skill req))

	        (and (= method :delete) admin-site-match)
	        (protected-route-response req #(handle-delete-site (second admin-site-match)))

        (and (= method :get) (= uri "/skills"))
        (protected-route-response req #(handle-skills req))

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
      (json-response 400 {:error (.getMessage e)}))))

;; ---------------------------------------------------------------------------
;; Server lifecycle
;; ---------------------------------------------------------------------------

(defn start!
  "Start the HTTP/WebSocket server.
   Defaults to loopback-only binding."
  ([port]
   (start! "127.0.0.1" port))
  ([bind-host port]
   (when @server-atom
     (log/warn "Server already running"))
   (prompt/register-approval! :http http-approval-handler)
   (prompt/register-approval! :websocket http-approval-handler)
   (prompt/register-status! :http http-status-handler)
   (prompt/register-status! :websocket http-status-handler)
   (let [^ScheduledExecutorService finalizer-exec
         (Executors/newSingleThreadScheduledExecutor)
         s (http/run-server router {:ip bind-host :port port})]
     (reset! rest-session-finalizer-executor finalizer-exec)
     (reset! server-atom s)
     (log/info "HTTP/WebSocket server started on" bind-host ":" port)
     s)))

(defn stop! []
  (when-let [s @server-atom]
    (doseq [{:keys [id channel active?]} (db/list-sessions {:include-workers? true})
            :when (and (= :http channel) active?)]
      (finalize-rest-session! id :server-stop))
    (s) ; http-kit stop fn
    (when-let [^ScheduledExecutorService exec @rest-session-finalizer-executor]
      (clear-rest-session-finalizers!)
      (.shutdown exec)
      (try
        (.awaitTermination exec 5 TimeUnit/SECONDS)
        (catch InterruptedException _
          (.shutdownNow exec)))
      (reset! rest-session-finalizer-executor nil))
    (prompt/register-approval! :http nil)
    (prompt/register-approval! :websocket nil)
    (prompt/register-status! :http nil)
    (prompt/register-status! :websocket nil)
    (reset! pending-approvals {})
    (reset! session-statuses {})
    (reset! server-atom nil)
    (log/info "Server stopped")))
