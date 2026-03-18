(ns xia.http-client
  "Shared outbound HTTP helper with bounded request timeouts and retries."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [hato.client :as hc])
  (:import [java.net URI URLEncoder]
           [java.net.http HttpClient HttpHeaders HttpRequest HttpRequest$BodyPublishers
            HttpResponse HttpResponse$BodyHandler HttpResponse$BodyHandlers]
           [java.nio.charset Charset StandardCharsets]
           [java.time Duration]
           [java.util.concurrent CompletableFuture ExecutionException TimeUnit TimeoutException]))

(def ^:private default-connect-timeout-ms 30000)
(def ^:private default-request-timeout-ms 120000)
(def ^:private default-max-attempts 3)
(def ^:private default-initial-backoff-ms 1000)
(def ^:private default-max-backoff-ms 8000)
(def ^:private default-retry-statuses #{408 409 425 429 500 502 503 504})
(def ^:private default-retry-methods #{:delete :get :head :options :put :trace})

(defonce ^:private http-clients (atom {}))

(defn- http-client
  [connect-timeout-ms]
  (or ^HttpClient (get @http-clients connect-timeout-ms)
      (let [^HttpClient client (hc/build-http-client {:connect-timeout connect-timeout-ms})]
        (get (swap! http-clients
                    #(if (contains? % connect-timeout-ms)
                       %
                       (assoc % connect-timeout-ms client)))
             connect-timeout-ms))))

(defn- encode-query-params
  [query-params]
  (->> query-params
       (keep (fn [[k v]]
               (when (some? v)
                 (str (URLEncoder/encode (name k) "UTF-8")
                      "="
                      (URLEncoder/encode (str v) "UTF-8")))))
       (str/join "&")))

(defn- request-url
  [{:keys [url uri query-params]}]
  (let [base-url (or url uri)]
    (when-not (seq (or base-url ""))
      (throw (ex-info "HTTP request requires :url or :uri" {})))
    (if (seq query-params)
      (let [query (encode-query-params query-params)
            sep   (if (str/includes? base-url "?") "&" "?")]
        (str base-url sep query))
      base-url)))

(defn- body-publisher
  [body]
  (if (nil? body)
    (HttpRequest$BodyPublishers/noBody)
    (HttpRequest$BodyPublishers/ofString ^String (str body) StandardCharsets/UTF_8)))

(defn- build-http-request
  [{:keys [method headers body timeout] :as req}]
  (let [builder (HttpRequest/newBuilder (URI. (request-url req)))]
    (.timeout builder (Duration/ofMillis (long timeout)))
    (doseq [[header value] headers]
      (.header builder (str header) (str value)))
    (.method builder
             (str/upper-case (name (or method :get)))
             (body-publisher body))
    (.build builder)))

(defn- normalize-headers
  [^HttpHeaders headers]
  (into {}
        (map (fn [[header values]]
               [(str/lower-case header)
                (str/join "," values)]))
        (.map headers)))

(defn- response-charset
  [headers]
  (let [content-type (get headers "content-type")]
    (try
      (if-let [[_ charset] (some->> content-type
                                    (re-find #"(?i)(?:^|;)\s*charset=([^;]+)"))]
        (Charset/forName (str/trim charset))
        StandardCharsets/UTF_8)
      (catch Exception _
        StandardCharsets/UTF_8))))

(defn- decode-body
  [body-bytes headers]
  (String. ^bytes body-bytes ^Charset (response-charset headers)))

(defn- response-body
  [body headers body-format]
  (case body-format
    :byte-array body
    :string (decode-body body headers)
    (throw (ex-info "Unsupported HTTP response body format"
                    {:body-format body-format}))))

(defn- send-request!
  [{:keys [connect-timeout timeout as]
    :or   {as :string}
    :as   req}]
  (let [http-request    (build-http-request req)
        ^HttpResponse$BodyHandler body-handler (HttpResponse$BodyHandlers/ofByteArray)
        ^CompletableFuture response-future
        (.sendAsync ^HttpClient (http-client connect-timeout)
                    ^HttpRequest http-request
                    body-handler)]
    (try
      (let [^HttpResponse response (.get response-future timeout TimeUnit/MILLISECONDS)
            headers  (normalize-headers (.headers response))]
        {:status  (.statusCode response)
         :headers headers
         :body    (response-body (.body response) headers as)})
      (catch TimeoutException e
        (.cancel response-future true)
        (throw (ex-info (str "HTTP request timed out after " timeout " ms")
                        {:timeout-ms timeout
                         :url        (request-url req)}
                        e)))
      (catch InterruptedException e
        (.cancel response-future true)
        (.interrupt (Thread/currentThread))
        (throw e))
      (catch ExecutionException e
        (throw (or (.getCause e) e))))))

(defn- transient-exception?
  [e]
  (boolean
    (some #(instance? Throwable %)
          (filter (fn [cause]
                    (or (instance? TimeoutException cause)
                        (instance? java.net.http.HttpTimeoutException cause)
                        (instance? java.net.http.HttpConnectTimeoutException cause)
                        (instance? java.io.IOException cause)))
                  (take-while some? (iterate ex-cause e))))))

(defn- sleep-ms!
  [delay-ms]
  (Thread/sleep (long delay-ms)))

(defn- backoff-ms
  [attempt initial-backoff-ms max-backoff-ms]
  (min max-backoff-ms
       (* initial-backoff-ms (bit-shift-left 1 (dec attempt)))))

(defn- retry-enabled?
  [{:keys [method retry-enabled? retry-methods]}]
  (if (some? retry-enabled?)
    retry-enabled?
    (contains? (or retry-methods default-retry-methods)
               (or method :get))))

(defn request
  "Send an HTTP request with request-level timeout and retries.

   Request opts:
     :url / :uri           absolute URL
     :method               keyword, default :get
     :headers              string map
     :body                 stringable body
     :query-params         map appended to URL
     :timeout              full request timeout in ms, default 120000
     :connect-timeout      connect timeout in ms, default 30000
     :as                   response body format, default :string; supports :string and :byte-array
     :max-attempts         retry attempts, default 3
     :initial-backoff-ms   default 1000
     :max-backoff-ms       default 8000
     :retry-statuses       default #{408 409 425 429 500 502 503 504}
     :retry-methods        methods retried by default, default #{:delete :get :head :options :put :trace}
     :retry-enabled?       override automatic method-based retry gating
     :request-label        optional log label"
  [{:keys [connect-timeout timeout max-attempts initial-backoff-ms max-backoff-ms retry-statuses request-label]
    :or   {connect-timeout    default-connect-timeout-ms
           timeout            default-request-timeout-ms
           max-attempts       default-max-attempts
           initial-backoff-ms default-initial-backoff-ms
           max-backoff-ms     default-max-backoff-ms
           retry-statuses     default-retry-statuses}
    :as   req}]
  (let [req (assoc req
                   :connect-timeout connect-timeout
                   :timeout timeout)
        label (or request-label "HTTP request")
        can-retry? (retry-enabled? req)]
    (letfn [(retry! [attempt reason]
              (let [delay-ms (backoff-ms attempt initial-backoff-ms max-backoff-ms)]
                (log/warn reason
                          "Retrying request"
                          {:request label
                           :attempt attempt
                           :max-attempts max-attempts
                           :delay-ms delay-ms
                           :url (request-url req)})
                (sleep-ms! delay-ms)
                (attempt-request (inc attempt))))
            (attempt-request [attempt]
              (let [resp (try
                           (send-request! req)
                           (catch Exception e
                             (if (and can-retry?
                                      (< attempt max-attempts)
                                      (transient-exception? e))
                               (retry! attempt (.getMessage e))
                               (throw e))))
                    status (:status resp)]
                (if (and can-retry?
                         (< attempt max-attempts)
                         (contains? retry-statuses status))
                  (retry! attempt (str label " returned transient status " status))
                  (assoc resp :attempt (or (:attempt resp) attempt)))))]
      (attempt-request 1))))
