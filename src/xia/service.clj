(ns xia.service
  "Capability proxy for external service calls.

   Tools need to call authenticated APIs (Gmail, GitHub, etc.) but must never
   see the raw credentials. This module resolves that tension:

     1. User registers a service: ID, base URL, auth type, auth key
     2. Tools call (xia.service/request :gmail :get \"/messages\")
     3. The proxy looks up credentials, injects auth, makes the HTTP call
     4. Tool gets the response — never sees the token

   Auth types:
     :bearer         — Authorization: Bearer <auth-key>
     :basic          — Authorization: Basic <base64(auth-key)>  (auth-key = \"user:pass\")
     :api-key-header — <auth-header>: <auth-key>  (e.g. X-API-Key)
     :query-param    — appends <auth-header>=<auth-key> to query string
     :oauth-account  — Authorization header from a stored OAuth account"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [charred.api :as json]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.http-client :as http]
            [xia.oauth :as oauth])
  (:import [java.util Base64]
           [java.util.concurrent ConcurrentHashMap]))

(def default-rate-limit-per-minute 60)

;; ---------------------------------------------------------------------------
;; URL safety
;; ---------------------------------------------------------------------------

(defn- safe-resolve-url
  "Resolve a path against a base URL. Rejects absolute URLs, path traversal,
   and anything that would escape the base URL."
  [base-url path]
  (when (or (str/starts-with? path "http://")
            (str/starts-with? path "https://")
            (str/starts-with? path "//"))
    (throw (ex-info "Absolute URLs not allowed — use a relative path"
                    {:base-url base-url :path path})))
  (when (str/includes? path "..")
    (throw (ex-info "Path traversal not allowed"
                    {:base-url base-url :path path})))
  (let [base (str/replace base-url #"/+$" "")
        p    (if (str/starts-with? path "/") path (str "/" path))]
    (str base p)))

;; ---------------------------------------------------------------------------
;; Auth injection
;; ---------------------------------------------------------------------------

(defn- map-key-name
  [k]
  (cond
    (keyword? k) (name k)
    (string? k)  k
    (symbol? k)  (name k)
    :else        (str k)))

(defn- normalize-query-params
  [query-params]
  (reduce-kv (fn [m k v]
               (assoc m (map-key-name k) v))
             {}
             (or query-params {})))

(defn- merge-protected-headers
  [headers protected-headers]
  (let [protected-names (into #{}
                              (map (comp str/lower-case map-key-name key))
                              protected-headers)
        sanitized-headers
        (reduce-kv (fn [m k v]
                     (let [header-name (map-key-name k)]
                       (if (contains? protected-names (str/lower-case header-name))
                         m
                         (assoc m header-name v))))
                   {}
                   (or headers {}))]
    (merge sanitized-headers protected-headers)))

(defn- with-protected-header
  [req header-name header-value]
  (update req :headers merge-protected-headers {header-name header-value}))

(defn- inject-auth
  "Add authentication to a request map based on the service's auth-type."
  [req {:keys [auth-type auth-key auth-header oauth-account]}]
  (case auth-type
    :bearer
    (with-protected-header req "Authorization" (str "Bearer " auth-key))

    :basic
    (let [encoded (.encodeToString (Base64/getEncoder) (.getBytes ^String auth-key "UTF-8"))]
      (with-protected-header req "Authorization" (str "Basic " encoded)))

    :api-key-header
    (do (when-not auth-header
          (throw (ex-info "auth-header required for :api-key-header auth type" {})))
        (with-protected-header req (map-key-name auth-header) auth-key))

    :query-param
    (do (when-not auth-header
          (throw (ex-info "auth-header required for :query-param auth type" {})))
        (update req :query-params
                (fn [query-params]
                  (assoc (normalize-query-params query-params)
                         (map-key-name auth-header)
                         auth-key))))

    :oauth-account
    (with-protected-header req "Authorization" (oauth/oauth-header oauth-account))

    ;; No auth / unknown — pass through
    req))

;; ---------------------------------------------------------------------------
;; Rate limiting
;; ---------------------------------------------------------------------------

(defonce ^:private service-rate-limits (ConcurrentHashMap.))
(def ^:private rate-limit-window-ms 60000)

(defn effective-rate-limit-per-minute
  "Return the effective per-service request cap for a minute window."
  [service]
  (long (or (:service/rate-limit-per-minute service)
            (:rate-limit-per-minute service)
            default-rate-limit-per-minute)))

(defn- current-time-ms []
  (System/currentTimeMillis))

(defn- check-rate-limit!
  [service-id service]
  (let [limit (effective-rate-limit-per-minute service)
        now   (current-time-ms)
        state (.computeIfAbsent service-rate-limits service-id
                (reify java.util.function.Function
                  (apply [_ _] (atom {:timestamps [] :cleaned now}))))]
    (swap! state
      (fn [{:keys [timestamps]}]
        (let [cutoff (- now rate-limit-window-ms)
              recent (filterv #(> % cutoff) timestamps)]
          (when (>= (count recent) limit)
            (throw (ex-info (str "Rate limit exceeded for service " (name service-id)
                                 " (max " limit " requests/minute)")
                            {:service-id service-id
                             :limit      limit})))
          {:timestamps (conj recent now)
           :cleaned    now})))))

;; ---------------------------------------------------------------------------
;; Service resolution
;; ---------------------------------------------------------------------------

(defn- resolve-service
  "Load and validate a service from the DB."
  [service-id]
  (let [svc (db/get-service service-id)]
    (when-not svc
      (throw (ex-info (str "Unknown service: " (name service-id)
                           ". Register it first with xia.db/register-service!")
                      {:service-id service-id})))
    (when-not (:service/enabled? svc)
      (throw (ex-info (str "Service " (name service-id) " is disabled")
                      {:service-id service-id})))
    (let [auth-type (:service/auth-type svc)
          oauth-account-id (:service/oauth-account svc)]
      {:base-url      (:service/base-url svc)
       :service-id    service-id
       :service-name  (:service/name svc)
       :auth-type     auth-type
       :auth-key      (:service/auth-key svc)
       :auth-header   (:service/auth-header svc)
       :autonomous-approved? (autonomous/service-autonomous-approved? svc)
       :rate-limit-per-minute (effective-rate-limit-per-minute svc)
       :oauth-account-id (when (= :oauth-account auth-type)
                           (when-not oauth-account-id
                             (throw (ex-info (str "Service " (name service-id)
                                                  " is missing an OAuth account")
                                             {:service-id service-id})))
                           oauth-account-id)})))

;; ---------------------------------------------------------------------------
;; Public API — exposed to SCI sandbox
;; ---------------------------------------------------------------------------

(defn list-services
  "List registered services. Returns safe metadata only, never credentials."
  []
  (->> (db/list-services)
       (filter (fn [svc]
                 (or (not (autonomous/autonomous-run?))
                     (autonomous/service-approved? (:service/id svc)))))
       (mapv (fn [svc]
               {:id       (:service/id svc)
                :name     (:service/name svc)
                :base-url (:service/base-url svc)
                :rate-limit-per-minute (effective-rate-limit-per-minute svc)
                :autonomous-approved? (autonomous/service-autonomous-approved? svc)
                :enabled? (:service/enabled? svc)}))))

(defn- ensure-autonomous-service-access!
  [service-id {:keys [auth-type oauth-account-id autonomous-approved?]} method path]
  (when (autonomous/autonomous-run?)
    (cond
      (not (autonomous/trusted?))
      (do
        (autonomous/audit! {:type       "service-request"
                            :service-id (name service-id)
                            :method     (name method)
                            :path       path
                            :status     "blocked"
                            :error      "trusted autonomous execution is required for service access"})
        (throw (ex-info "service access requires trusted autonomous execution"
                        {:service-id service-id})))

      (not autonomous-approved?)
      (do
        (autonomous/audit! {:type       "service-request"
                            :service-id (name service-id)
                            :method     (name method)
                            :path       path
                            :status     "blocked"
                            :error      "service is not approved for autonomous execution"})
        (throw (ex-info (str "Service " (name service-id)
                             " is not approved for autonomous execution")
                        {:service-id service-id})))

      (and (= :oauth-account auth-type)
           (not (autonomous/oauth-account-approved? oauth-account-id)))
      (do
        (autonomous/audit! {:type             "service-request"
                            :service-id       (name service-id)
                            :oauth-account-id (some-> oauth-account-id name)
                            :method           (name method)
                            :path             path
                            :status           "blocked"
                            :error            "oauth account is not approved for autonomous execution"})
        (throw (ex-info (str "OAuth account " (name oauth-account-id)
                             " is not approved for autonomous execution")
                        {:service-id service-id
                         :oauth-account-id oauth-account-id}))))))

(defn request
  "Make an authenticated HTTP request to a registered service.

   Tools call this instead of making raw HTTP requests — credentials are
   injected automatically and never exposed to tool code.

   Arguments:
     service-id — keyword id of the registered service
     method     — :get :post :put :patch :delete
     path       — relative path (e.g. \"/users/me/messages\")
     opts       — optional map:
       :body          — request body (string or map; maps are JSON-encoded)
       :headers       — additional headers (merged after auth headers)
       :query-params  — URL query parameters
       :as            — response coercion: :json (default), :string, :raw

   Returns:
     {:status 200 :headers {...} :body <parsed>}"
  [service-id method path & {:keys [body headers query-params as]
                              :or   {as :json}}]
  (let [svc     (resolve-service service-id)
        url     (safe-resolve-url (:base-url svc) path)
        body-str (cond
                   (nil? body)    nil
                   (string? body) body
                   (map? body)    (json/write-json-str body)
                   :else          (str body))
        _       (ensure-autonomous-service-access! service-id svc method path)
        oauth-account (when (= :oauth-account (:auth-type svc))
                        (oauth/ensure-account-ready! (:oauth-account-id svc)))
        svc     (cond-> svc
                  oauth-account (assoc :oauth-account oauth-account))
        req     (cond-> {:url         url
                         :method      method
                         :request-label (str "Service request " (name service-id))}
                  body-str     (assoc :body body-str)
                  body-str     (assoc-in [:headers "Content-Type"] "application/json")
                  query-params (assoc :query-params query-params))
        ;; Inject service auth — this is the key security boundary
        req     (inject-auth req svc)
        ;; Merge any extra headers from the tool under existing request headers.
        ;; This preserves auth/content headers, even if the tool changes case.
        req     (if headers
                  (update req :headers #(merge-protected-headers headers %))
                  req)
        _       (check-rate-limit! service-id svc)
        _       (log/debug "Service request:" (name service-id) method path)]
    (try
      (let [resp (http/request req)]
        (autonomous/audit! (cond-> {:type        "service-request"
                                    :service-id  (name service-id)
                                    :method      (name method)
                                    :path        path
                                    :status      "success"
                                    :http-status (:status resp)}
                             (:oauth-account-id svc)
                             (assoc :oauth-account-id (name (:oauth-account-id svc)))))
        {:status  (:status resp)
         :headers (into {} (:headers resp))
         :body    (case as
                    :json   (try (json/read-json (:body resp))
                                 (catch Exception _ (:body resp)))
                    :string (:body resp)
                    :raw    (:body resp)
                    (:body resp))})
      (catch Exception e
        (autonomous/audit! (cond-> {:type       "service-request"
                                    :service-id (name service-id)
                                    :method     (name method)
                                    :path       path
                                    :status     "error"
                                    :error      (.getMessage e)}
                             (:oauth-account-id svc)
                             (assoc :oauth-account-id (name (:oauth-account-id svc)))))
        (throw e)))))
