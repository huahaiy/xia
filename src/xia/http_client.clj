(ns xia.http-client
  "Shared outbound HTTP helper with bounded request timeouts and retries."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [hato.client :as hc]
            [xia.ssrf :as ssrf]
            [xia.task-policy :as task-policy])
  (:import [java.io BufferedReader InputStream InputStreamReader]
           [java.net URI URLEncoder]
           [java.net.http HttpClient HttpHeaders HttpRequest HttpRequest$BodyPublishers
            HttpResponse HttpResponse$BodyHandler HttpResponse$BodyHandlers]
           [java.nio.charset Charset StandardCharsets]
           [java.time Duration]
           [java.util.concurrent CompletableFuture ExecutionException TimeUnit TimeoutException]))

(def ^:private default-connect-timeout-ms 30000)
(def ^:private default-request-timeout-ms 120000)

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

(defn- validate-request-target!
  [{:keys [allow-private-network? url uri query-params]}]
  (let [request-url-str (request-url {:url url :uri uri :query-params query-params})]
    (ssrf/resolve-url! request-url-str
                      {:allow-private-network? (boolean allow-private-network?)})
    request-url-str))

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

(defn- send-streaming-request!
  [{:keys [connect-timeout timeout on-event]
    :as req}]
  (let [http-request    (build-http-request req)
        ^HttpResponse$BodyHandler body-handler (HttpResponse$BodyHandlers/ofInputStream)
        ^CompletableFuture response-future
        (.sendAsync ^HttpClient (http-client connect-timeout)
                    ^HttpRequest http-request
                    body-handler)]
    (try
      (let [^HttpResponse response (.get response-future timeout TimeUnit/MILLISECONDS)
            headers  (normalize-headers (.headers response))
            ^InputStream body-stream (.body response)]
        (with-open [stream body-stream]
          (let [status-code (.statusCode response)]
            (if (and (= 200 status-code)
                     (str/starts-with? (or (get headers "content-type") "")
                                       "text/event-stream"))
              (do
                (when-not on-event
                  (throw (ex-info "Streaming HTTP request requires :on-event callback"
                                  {:url (request-url req)})))
                (let [^BufferedReader reader (BufferedReader.
                                               (InputStreamReader. stream StandardCharsets/UTF_8))]
                  (loop [event-type nil
                         data-lines []]
                    (if-let [line (.readLine reader)]
                      (if (str/blank? line)
                        (do
                          (when (seq data-lines)
                            (on-event {:event (or event-type "message")
                                       :data (str/join "\n" data-lines)}))
                          (recur nil []))
                        (if (str/starts-with? line ":")
                          (recur event-type data-lines)
                          (let [[field raw-value] (str/split line #":" 2)
                                value (some-> raw-value (str/replace-first #"^\s" ""))]
                            (case field
                              "event" (recur value data-lines)
                              "data"  (recur event-type (conj data-lines (or value "")))
                              (recur event-type data-lines)))))
                      (do
                        (when (seq data-lines)
                          (on-event {:event (or event-type "message")
                                     :data (str/join "\n" data-lines)}))
                        {:status status-code
                         :headers headers
                         :streamed? true})))))
              {:status status-code
               :headers headers
               :body (slurp stream :encoding (str (response-charset headers)))}))))
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

(defn- successful-status?
  [status]
  (and (integer? status)
       (<= 200 (int status) 299)))

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
     :allow-private-network? bypass SSRF private-network blocking for explicitly trusted targets
     :request-label        optional log label
     :policy-observer      optional callback for retry-policy decisions"
  [{:keys [connect-timeout timeout request-label policy-observer]
    :or   {connect-timeout    default-connect-timeout-ms
           timeout            default-request-timeout-ms}
    :as   req}]
  (let [retry-config (task-policy/http-request-retry-config req)
        req (merge req
                   retry-config
                   {:connect-timeout connect-timeout
                    :timeout timeout})
        request-url-str (validate-request-target! req)
        label (or request-label "HTTP request")
        emit-policy! (fn [decision]
                       (when policy-observer
                         (policy-observer (assoc decision
                                                 :decision-type :http-retry-policy
                                                 :request-label label
                                                 :url request-url-str))))]
    (letfn [(retry! [decision reason]
              (let [delay-ms (:delay-ms decision)]
                (log/warn reason
                          "Retrying request"
                          {:request label
                           :attempt (:attempt decision)
                           :max-attempts (:max-attempts decision)
                           :delay-ms delay-ms
                           :url request-url-str})
                (sleep-ms! delay-ms)
                (attempt-request (inc (long (:attempt decision))))))
            (attempt-request [attempt]
              (let [resp (try
                           (send-request! req)
                           (catch Exception e
                             (let [decision (task-policy/http-request-retry-decision
                                             req
                                             attempt
                                             {:transient-exception? (transient-exception? e)
                                              :reason (.getMessage e)})]
                               (emit-policy! decision)
                               (if (:allowed? decision)
                                 (retry! decision (.getMessage e))
                                 (throw e)))))
                    status (:status resp)]
                (let [decision (task-policy/http-request-retry-decision
                                req
                                attempt
                                {:status status
                                 :reason (str label " returned transient status " status)})]
                  (when-not (successful-status? status)
                    (emit-policy! decision))
                  (if (:allowed? decision)
                    (retry! decision (str label " returned transient status " status))
                    (assoc resp :attempt (or (:attempt resp) attempt))))))]
      (attempt-request 1))))

(defn request-events
  "Send an HTTP request and stream `text/event-stream` responses via `:on-event`.

   Unlike `request`, this does not do automatic request-level retries. Callers that
   want replay/backoff semantics should implement them above this layer."
  [{:keys [connect-timeout timeout on-event]
    :or   {connect-timeout default-connect-timeout-ms
           timeout         default-request-timeout-ms}
    :as   req}]
  (let [req (assoc req
                   :connect-timeout connect-timeout
                   :timeout timeout)
        _   (validate-request-target! req)]
    (send-streaming-request! (assoc req :on-event on-event))))
