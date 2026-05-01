(ns xia.browser.remote
  "Remote browser backend backed by an external browser-runtime service.

   Xia remains the durable owner of browser session state. The remote service
   is treated as an execution engine: Xia persists browser snapshots locally
   and recreates remote sessions from those snapshots when a leased remote
   context disappears."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.browser.backend :as backend]
            [xia.config :as cfg]
            [xia.http-client :as http]))

(def ^:private backend-id :remote)
(def ^:private default-request-timeout-ms 120000)

(declare best-effort-close! missing-session-error? session-path)

(defn configured?
  []
  (and (cfg/boolean-option :browser/remote-enabled? false)
       (some-> (cfg/string-option :browser/remote-base-url nil)
               not-empty
               boolean)))

(defn config-resolutions
  []
  {:enabled
   (cfg/boolean-option-resolution :browser/remote-enabled? false)
   :base-url
   (cfg/string-option-resolution :browser/remote-base-url nil)
   :auth-token
   (cfg/string-option-resolution :browser/remote-auth-token nil)
   :timeout-ms
   (cfg/positive-long-resolution :browser/remote-timeout-ms
                                 default-request-timeout-ms)})

(defn- enabled?
  []
  (cfg/boolean-option :browser/remote-enabled? false))

(defn- service-base-url
  []
  (some-> (cfg/string-option :browser/remote-base-url nil)
          (str/replace #"/+$" "")))

(defn- auth-token
  []
  (cfg/string-option :browser/remote-auth-token nil))

(defn- request-timeout-ms
  []
  (cfg/positive-long :browser/remote-timeout-ms
                     default-request-timeout-ms))

(defn- now-ms
  []
  (System/currentTimeMillis))

(defn- request-headers
  [json-body?]
  (cond-> {"Accept" "application/json"}
    json-body?
    (assoc "Content-Type" "application/json")

    (seq (auth-token))
    (assoc "Authorization" (str "Bearer " (auth-token)))))

(defn- parse-response-body
  [body]
  (cond
    (nil? body) nil
    (map? body) body
    (vector? body) body
    (string? body)
    (let [text (str/trim body)]
      (when (seq text)
        (try
          (json/read-json text)
          (catch Exception _
            text))))
    :else body))

(defn- request!
  [method path body]
  (let [base-url (or (service-base-url)
                     (throw (ex-info "Remote browser backend is not configured."
                                     {:backend backend-id
                                      :status :unconfigured})))
        response (http/request {:method method
                                :headers (request-headers (some? body))
                                :body (when (some? body)
                                        (json/write-json-str body))
                                :allow-private-network? true
                                :timeout (request-timeout-ms)
                                :request-label (str "Remote browser " (name method) " " path)
                                :url (str base-url path)})
        payload (parse-response-body (:body response))]
    (if (<= 200 (int (:status response)) 299)
      payload
      (throw (ex-info (or (some-> payload (get "error") str)
                          (some-> payload (get "message") str)
                          (str "Remote browser request failed with status " (:status response)))
                      {:backend backend-id
                       :status (:status response)
                       :path path
                       :response payload})))))

(defn- lookup
  [m & ks]
  (when (map? m)
    (reduce (fn [_ k]
              (when (contains? m k)
                (reduced (get m k))))
            nil
            ks)))

(defn- long-value
  [value default-value]
  (cond
    (integer? value) (long value)
    (number? value) (long value)
    (string? value) (try
                      (Long/parseLong (str/trim value))
                      (catch Exception _
                        default-value))
    :else default-value))

(defn- deep-keywordize
  [value]
  (cond
    (map? value)
    (into {}
          (map (fn [[k v]]
                 [(if (keyword? k) k (keyword (str k)))
                  (deep-keywordize v)]))
          value)

    (vector? value)
    (mapv deep-keywordize value)

    (seq? value)
    (mapv deep-keywordize value)

    :else value))

(defn- backend-snapshots
  [ops]
  (->> ((:all-snapshots ops))
       (filter (fn [[_ snapshot]]
                 (= (name backend-id)
                    (lookup snapshot "backend" :backend))))))

(defn- snapshot-from-payload
  [ops session-id payload {:keys [default-url default-js]}]
  (let [existing      ((:read-snapshot ops) session-id)
        session       (or (lookup payload "session" :session) {})
        current-url   (or (lookup session "current_url" :current_url "url" :url)
                          (lookup payload "current_url" :current_url "url" :url)
                          (lookup existing "current_url" :current_url)
                          default-url)
        browser-state (or (lookup session "browser_state" :browser_state "storage_state" :storage_state)
                          (lookup payload "browser_state" :browser_state "storage_state" :storage_state)
                          (lookup existing "browser_state" :browser_state))
        created-at-ms (long-value
                       (or (lookup session "created_at_ms" :created_at_ms)
                           (lookup payload "created_at_ms" :created_at_ms)
                           (lookup existing "created_at_ms" :created_at_ms))
                       (now-ms))
        updated-at-ms (long-value
                       (or (lookup session "updated_at_ms" :updated_at_ms)
                           (lookup payload "updated_at_ms" :updated_at_ms))
                       (now-ms))
        last-access-ms (long-value
                        (or (lookup session "last_access_ms" :last_access_ms)
                            (lookup payload "last_access_ms" :last_access_ms)
                            updated-at-ms)
                        updated-at-ms)
        js-enabled   (let [value (or (lookup session "js_enabled" :js_enabled "js" :js)
                                     (lookup payload "js_enabled" :js_enabled "js" :js)
                                     (lookup existing "js_enabled" :js_enabled)
                                     default-js)]
                       (if (nil? value) true (boolean value)))]
    {"session_id" session-id
     "backend" (name backend-id)
     "current_url" current-url
     "created_at_ms" created-at-ms
     "updated_at_ms" updated-at-ms
     "last_access_ms" last-access-ms
     "js_enabled" js-enabled
     "browser_state" browser-state}))

(defn- persist-snapshot!
  [ops session-id payload defaults]
  (let [snapshot (snapshot-from-payload ops session-id payload defaults)]
    ((:write-snapshot! ops) session-id snapshot)
    snapshot))

(defn- payload-updates-snapshot?
  [payload]
  (or (some? (lookup payload "session" :session))
      (some? (lookup payload "browser_state" :browser_state "storage_state" :storage_state))
      (some? (lookup payload "current_url" :current_url "url" :url))))

(defn- result-from-payload
  [session-id payload]
  (let [result (or (lookup payload "result" :result) payload)]
    (assoc (if (map? result)
             (deep-keywordize result)
             {})
           :session-id session-id
           :backend backend-id)))

(defn- query-elements-body
  [opts]
  (cond-> {}
    (contains? opts :kind)
    (assoc "kind" (some-> (:kind opts) name))

    (contains? opts :selector)
    (assoc "selector" (:selector opts))

    (contains? opts :text-contains)
    (assoc "text_contains" (:text-contains opts))

    (contains? opts :visible-only)
    (assoc "visible_only" (boolean (:visible-only opts)))

    (contains? opts :offset)
    (assoc "offset" (:offset opts))

    (contains? opts :limit)
    (assoc "limit" (:limit opts))))

(defn- wait-body
  [opts]
  (cond-> {}
    (contains? opts :timeout-ms)
    (assoc "timeout_ms" (:timeout-ms opts))

    (contains? opts :interval-ms)
    (assoc "interval_ms" (:interval-ms opts))

    (contains? opts :selector)
    (assoc "selector" (:selector opts))

    (contains? opts :text)
    (assoc "text" (:text opts))

    (contains? opts :url-contains)
    (assoc "url_contains" (:url-contains opts))))

(defn- screenshot-body
  [opts]
  (cond-> {}
    (contains? opts :full-page)
    (assoc "full_page" (boolean (:full-page opts)))

    (contains? opts :detail)
    (assoc "detail" (:detail opts))))

(defn- remote-open-session!
  [ops session-id url {:keys [js storage-state headless channel]}]
  (let [payload (request! :post
                          "/sessions"
                          (cond-> {"session_id" session-id
                                   "url" url
                                   "js_enabled" (if (nil? js) true (boolean js))}
                            (some? storage-state)
                            (assoc "storage_state" storage-state)

                            (some? headless)
                            (assoc "headless" (boolean headless))

                            (some? channel)
                            (assoc "channel" channel)))]
    (try
      (persist-snapshot! ops session-id payload {:default-url url
                                                 :default-js (if (nil? js) true (boolean js))})
      (result-from-payload session-id payload)
      (catch Throwable t
        ;; The remote lease exists now, so clean it up if Xia fails to durably
        ;; checkpoint the session locally.
        (best-effort-close! session-id)
        (throw t)))))

(defn- snapshot-usable?
  [ops snapshot target-url]
  (let [validation (when-let [validate* (:snapshot-usable? ops)]
                     (validate* snapshot target-url))]
    (or (nil? validation) (:ok? validation))))

(defn- restore-session!
  [ops session-id]
  (when-let [snapshot ((:read-snapshot ops) session-id)]
    (let [target-url (lookup snapshot "current_url" :current_url)]
      (cond
        (and (:snapshot-expired? ops)
             ((:snapshot-expired? ops) snapshot))
        (do
          ((:delete-snapshot! ops) session-id)
          nil)

        (str/blank? (or target-url ""))
        nil

        (not (snapshot-usable? ops snapshot target-url))
        (do
          ((:delete-snapshot! ops) session-id)
          nil)

        :else
        (remote-open-session! ops
                              session-id
                              target-url
                              {:js (lookup snapshot "js_enabled" :js_enabled)
                               :storage-state (lookup snapshot "browser_state" :browser_state)})))))

(defn- export-session!
  [ops session-id]
  (try
    (let [payload (request! :post (session-path session-id "/export") nil)]
      (persist-snapshot! ops session-id payload {:default-url nil
                                                 :default-js true}))
    (catch clojure.lang.ExceptionInfo e
      (when-not (missing-session-error? e)
        (throw e)))))

(defn- missing-session-error?
  [e]
  (or (= 404 (:status (ex-data e)))
      (= :session-not-found (:reason (ex-data e)))))

(defn- with-restored-session
  [ops session-id f]
  (try
    (f)
    (catch clojure.lang.ExceptionInfo e
      (if (and (= backend-id (:backend (ex-data e)))
               (missing-session-error? e))
        (do
          (or (restore-session! ops session-id)
              (throw e))
          (f))
        (throw e)))))

(defn- session-path
  [session-id suffix]
  (str "/sessions/" session-id suffix))

(defn- invoke-session-action
  [ops session-id method suffix body]
  (with-restored-session
    ops
    session-id
    (fn []
      (let [payload (request! method (session-path session-id suffix) body)]
        (when (payload-updates-snapshot? payload)
          (persist-snapshot! ops session-id payload {:default-url nil
                                                     :default-js true}))
        (result-from-payload session-id payload)))))

(defn- best-effort-close!
  [session-id]
  (try
    (request! :delete (session-path session-id "") nil)
    (catch clojure.lang.ExceptionInfo e
      (when-not (= 404 (:status (ex-data e)))
        (log/warn e "Failed to close remote browser session" session-id)))
    (catch Exception e
      (log/warn e "Failed to close remote browser session" session-id))))

(defn- runtime-status
  []
  (cond
    (not (enabled?))
    {:backend backend-id
     :available? false
     :ready? false
     :running? false
     :status :disabled
     :message "Remote browser backend is disabled by config."}

    (not (seq (service-base-url)))
    {:backend backend-id
     :available? false
     :ready? false
     :running? false
     :status :unconfigured
     :message "Remote browser backend is enabled but no :browser/remote-base-url is configured."}

    :else
    (try
      (let [payload (request! :get "/runtime/status" nil)
            status  (or (some-> (lookup payload "status" :status) str keyword)
                        (some-> (lookup payload "phase" :phase) str keyword)
                        :ready)
            ready?  (if (contains? payload "ready")
                      (boolean (lookup payload "ready" :ready))
                      (not (contains? #{:draining :starting :stopped :error} status)))
            running? (if (contains? payload "running")
                       (boolean (lookup payload "running" :running))
                       (contains? #{:running :ready :idle :draining} status))]
        {:backend backend-id
         :available? true
         :ready? ready?
         :running? running?
         :status status
         :message (or (lookup payload "message" :message)
                      "Remote browser backend is reachable.")
         :service (deep-keywordize payload)})
      (catch Exception e
        {:backend backend-id
         :available? false
         :ready? false
         :running? false
         :status :unreachable
         :message (or (.getMessage e)
                      "Remote browser backend is unreachable.")
         :error (.getMessage e)}))))

(defn- unsupported-install-result
  []
  {:backend backend-id
   :supported? false
   :status :unsupported
   :message "Remote browser backend is managed externally; Xia cannot install its dependencies."})

(defn- remote-live-sessions
  []
  (let [payload (request! :get "/sessions" nil)
        sessions (or (lookup payload "sessions" :sessions) payload)]
    (->> sessions
         (filter map?)
         (map (fn [session]
                (let [session-id (str (or (lookup session "session_id" :session_id "id" :id)
                                          (random-uuid)))
                      age-seconds (long-value
                                   (or (lookup session "age_seconds" :age_seconds)
                                       (when-let [last-access-ms (lookup session "last_access_ms" :last_access_ms
                                                                         "updated_at_ms" :updated_at_ms)]
                                         (quot (- (now-ms)
                                                  (long-value last-access-ms (now-ms)))
                                               1000)))
                                   0)]
                  [session-id
                   {:session-id session-id
                    :backend backend-id
                    :url (or (lookup session "current_url" :current_url "url" :url)
                             nil)
                    :age-seconds age-seconds
                    :live? true
                    :resumable? true}])))
         (into {}))))

(defrecord RemoteBackend [ops]
  backend/BrowserBackend
  (backend-id [_]
    backend-id)
  (runtime-status* [_]
    (runtime-status))
  (bootstrap-runtime!* [_ _opts]
    (runtime-status))
  (install-browser-deps!* [_ _opts]
    (unsupported-install-result))
  (open-session* [_ url {:keys [js storage-state headless channel]}]
    (when-let [validate-url! (:validate-url! ops)]
      (validate-url! url))
    (remote-open-session! ops
                          (str (random-uuid))
                          url
                          {:js js
                           :storage-state storage-state
                           :headless headless
                           :channel channel}))
  (navigate* [_ session-id url]
    (when-let [validate-url! (:validate-url! ops)]
      (validate-url! url))
    (invoke-session-action ops session-id :post "/navigate" {"url" url}))
  (click* [_ session-id selector]
    (invoke-session-action ops session-id :post "/click" {"selector" selector}))
  (fill-selector* [_ session-id selector value _opts]
    (invoke-session-action ops session-id :post "/fill-selector"
                           {"selector" selector
                            "value" value}))
  (fill-form* [_ session-id fields {:keys [form-selector submit require-all-fields?]}]
    (invoke-session-action ops session-id :post "/fill-form"
                           (cond-> {"fields" fields}
                             (some? form-selector)
                             (assoc "form_selector" form-selector)

                             (some? submit)
                             (assoc "submit" (boolean submit))

                             (some? require-all-fields?)
                             (assoc "require_all_fields" (boolean require-all-fields?)))))
  (read-page* [_ session-id]
    (invoke-session-action ops session-id :get "/page" nil))
  (query-elements* [_ session-id opts]
    (invoke-session-action ops session-id :post "/query-elements" (query-elements-body opts)))
  (screenshot* [_ session-id opts]
    (invoke-session-action ops session-id :post "/screenshot" (screenshot-body opts)))
  (wait-for-page* [_ session-id opts]
    (invoke-session-action ops session-id :post "/wait" (wait-body opts)))
  (release-session* [_ session-id]
    (export-session! ops session-id)
    (best-effort-close! session-id)
    {:status "released"
     :session-id session-id
     :resumable? true})
  (close-session* [_ session-id]
    (best-effort-close! session-id)
    ((:delete-snapshot! ops) session-id)
    {:status "closed" :session-id session-id})
  (release-all-sessions!* [_]
    (doseq [[session-id _snapshot] (backend-snapshots ops)]
      (try
        (export-session! ops session-id)
        (finally
          (best-effort-close! session-id))))
    {:backend backend-id
     :status "released"})
  (close-all-sessions!* [_]
    (doseq [[session-id _snapshot] (backend-snapshots ops)]
      (best-effort-close! session-id)
      ((:delete-snapshot! ops) session-id))
    {:backend backend-id
     :status "closed"})
  (list-sessions* [_]
    (let [snapshot-map (into {}
                             (map (fn [[session-id snapshot]]
                                    (let [last-access-ms (long-value
                                                          (or (lookup snapshot "last_access_ms" :last_access_ms)
                                                              (lookup snapshot "updated_at_ms" :updated_at_ms))
                                                          0)]
                                      [session-id
                                       {:session-id session-id
                                        :backend backend-id
                                        :url (lookup snapshot "current_url" :current_url)
                                        :age-seconds (quot (- (now-ms) last-access-ms) 1000)
                                        :live? false
                                        :resumable? true}])))
                             (backend-snapshots ops))
          live-map     (if (configured?)
                         (try
                           (into {}
                                 (filter (fn [[session-id _]]
                                           (contains? snapshot-map session-id)))
                                 (remote-live-sessions))
                           (catch Exception e
                             (log/debug e "Unable to fetch remote live browser sessions")
                             {}))
                         {})]
      (->> (merge snapshot-map live-map)
           vals
           (sort-by :session-id)
           vec))))

(defn create-backend
  [ops]
  (->RemoteBackend ops))
