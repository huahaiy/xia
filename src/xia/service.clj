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
     :query-param    — appends <auth-header>=<auth-key> to query string"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [hato.client :as hc]
            [charred.api :as json]
            [xia.db :as db])
  (:import [java.util Base64]))

;; ---------------------------------------------------------------------------
;; HTTP client (shared with xia.llm — could be extracted, but keeping simple)
;; ---------------------------------------------------------------------------

(defonce ^:private http-client (delay (hc/build-http-client {:connect-timeout 30000})))

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

(defn- inject-auth
  "Add authentication to a request map based on the service's auth-type."
  [req {:keys [auth-type auth-key auth-header]}]
  (case auth-type
    :bearer
    (assoc-in req [:headers "Authorization"] (str "Bearer " auth-key))

    :basic
    (let [encoded (.encodeToString (Base64/getEncoder) (.getBytes ^String auth-key "UTF-8"))]
      (assoc-in req [:headers "Authorization"] (str "Basic " encoded)))

    :api-key-header
    (do (when-not auth-header
          (throw (ex-info "auth-header required for :api-key-header auth type" {})))
        (assoc-in req [:headers auth-header] auth-key))

    :query-param
    (do (when-not auth-header
          (throw (ex-info "auth-header required for :query-param auth type" {})))
        (update req :query-params assoc auth-header auth-key))

    ;; No auth / unknown — pass through
    req))

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
    {:base-url    (:service/base-url svc)
     :auth-type   (:service/auth-type svc)
     :auth-key    (:service/auth-key svc)
     :auth-header (:service/auth-header svc)}))

;; ---------------------------------------------------------------------------
;; Public API — exposed to SCI sandbox
;; ---------------------------------------------------------------------------

(defn list-services
  "List registered services. Returns ID, name, base-url — never credentials."
  []
  (mapv (fn [svc]
          {:id       (:service/id svc)
           :name     (:service/name svc)
           :base-url (:service/base-url svc)
           :enabled? (:service/enabled? svc)})
        (db/list-services)))

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
        req     (cond-> {:uri         url
                         :method      method
                         :http-client @http-client}
                  body-str     (assoc :body body-str)
                  body-str     (assoc-in [:headers "Content-Type"] "application/json")
                  query-params (assoc :query-params query-params))
        ;; Inject service auth — this is the key security boundary
        req     (inject-auth req svc)
        ;; Merge any extra headers from the tool (AFTER auth, so tools can't
        ;; override Authorization — auth headers take precedence)
        req     (if headers
                  (update req :headers #(merge headers %))
                  req)
        _       (log/debug "Service request:" (name service-id) method path)
        resp    (hc/request req)]
    {:status  (:status resp)
     :headers (into {} (:headers resp))
     :body    (case as
               :json   (try (json/read-json (:body resp))
                            (catch Exception _ (:body resp)))
               :string (:body resp)
               :raw    (:body resp)
               (:body resp))}))
