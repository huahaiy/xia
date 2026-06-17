(ns xia.http-client
  "Shared outbound HTTP helper with bounded request timeouts and retries."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.ssrf :as ssrf]
            [xia.policy.http :as http-policy])
  (:import [java.io BufferedInputStream BufferedOutputStream BufferedReader ByteArrayOutputStream
            EOFException InputStream InputStreamReader]
           [java.net InetAddress InetSocketAddress Socket SocketTimeoutException URI URLEncoder]
           [java.nio.charset Charset StandardCharsets]
           [java.nio.file Files Path Paths StandardCopyOption]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent TimeoutException]
           [javax.net.ssl SNIHostName SSLSocket SSLSocketFactory]))

(def ^:private default-connect-timeout-ms 30000)
(def ^:private default-request-timeout-ms 120000)
(def ^:private default-max-redirects 10)
(def ^:private byte-array-class (Class/forName "[B"))
(def ^:private redirect-statuses #{301 302 303 307 308})
(def ^:private cross-origin-sensitive-headers
  #{"authorization" "cookie" "proxy-authorization"})
(def ^:private body-headers
  #{"content-length" "content-type"})

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

(defn- trusted-request?
  [{:keys [allow-private-network? trusted? trusted]}]
  (boolean (or allow-private-network? trusted? trusted)))

(defn- validate-request-target!
  [{:keys [url uri query-params] :as req}]
  (let [request-url-str (request-url {:url url :uri uri :query-params query-params})
        parsed-uri      (URI. request-url-str)
        resolution      (ssrf/resolve-url! request-url-str
                                           {:allow-private-network? (trusted-request? req)})]
    (merge {:url request-url-str
            :uri parsed-uri
            :host (.getHost parsed-uri)}
           resolution)))

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

(defn- read-all-bytes!
  [^InputStream in]
  (let [out (ByteArrayOutputStream.)
        buf (byte-array 8192)]
    (loop []
      (let [n (.read in buf)]
        (if (= -1 n)
          (.toByteArray out)
          (do
            (.write out buf 0 n)
            (recur)))))))

(defn- body-bytes
  [body]
  (cond
    (nil? body) (byte-array 0)
    (instance? byte-array-class body) body
    (instance? InputStream body) (read-all-bytes! body)
    :else (.getBytes (str body) StandardCharsets/UTF_8)))

(defn- effective-port
  [^URI uri]
  (let [port (.getPort uri)]
    (if (pos? port)
      port
      (case (some-> (.getScheme uri) str/lower-case)
        "http" 80
        "https" 443
        (throw (ex-info "Only http:// and https:// URLs are allowed"
                        {:url (str uri)
                         :scheme (.getScheme uri)}))))))

(defn- default-port?
  [^URI uri port]
  (= (long port)
     (long (case (some-> (.getScheme uri) str/lower-case)
             "http" 80
             "https" 443
             -1))))

(defn- uri-origin
  [^URI uri]
  [(some-> (.getScheme uri) str/lower-case)
   (some-> (.getHost uri) str/lower-case)
   (effective-port uri)])

(defn- same-origin?
  [^URI a ^URI b]
  (= (uri-origin a) (uri-origin b)))

(defn- request-path
  [^URI uri]
  (str (if (str/blank? (.getRawPath uri))
         "/"
         (.getRawPath uri))
       (when-let [query (.getRawQuery uri)]
         (str "?" query))))

(defn- host-header
  [^URI uri host port]
  (let [host (if (and (str/includes? (or host "") ":")
                      (not (str/starts-with? host "[")))
               (str "[" host "]")
               host)]
    (if (default-port? uri port)
      host
      (str host ":" port))))

(defn- request-method-name
  [method]
  (str/lower-case (name (or method :get))))

(defn- header-name
  [header]
  (if (keyword? header)
    (name header)
    (str header)))

(defn- remove-headers
  [headers blocked]
  (into {}
        (remove (fn [[header _]]
                  (contains? blocked (str/lower-case (header-name header)))))
        (or headers {})))

(defn- remove-cross-origin-sensitive-headers
  [headers]
  (remove-headers headers cross-origin-sensitive-headers))

(defn- remove-body-headers
  [headers]
  (remove-headers headers body-headers))

(defn- write-header!
  [^BufferedOutputStream out header value]
  (.write out (.getBytes (str header ": " value "\r\n") StandardCharsets/ISO_8859_1)))

(defn- write-http-request!
  [^BufferedOutputStream out {:keys [method headers body resolved-target] :as req}]
  (let [^URI uri (:uri resolved-target)
        host     (:host resolved-target)
        port     (effective-port uri)
        body     (body-bytes body)
        method   (str/upper-case (request-method-name method))]
    (.write out (.getBytes (str method " " (request-path uri) " HTTP/1.1\r\n")
                           StandardCharsets/ISO_8859_1))
    (write-header! out "Host" (host-header uri host port))
    (doseq [[header value] headers
            :let [header-str (header-name header)
                  lower      (str/lower-case header-str)]
            :when (and (some? value)
                       (not (#{"host" "content-length" "connection"} lower)))]
      (write-header! out header-str value))
    (write-header! out "Connection" "close")
    (when (pos? (alength ^bytes body))
      (write-header! out "Content-Length" (alength ^bytes body)))
    (.write out (.getBytes "\r\n" StandardCharsets/ISO_8859_1))
    (when (pos? (alength ^bytes body))
      (.write out ^bytes body))
    (.flush out)))

(defn- read-line-crlf!
  [^InputStream in]
  (let [out (ByteArrayOutputStream.)]
    (loop []
      (let [b (.read in)]
        (cond
          (= -1 b)
          (when (pos? (.size out))
            (String. (.toByteArray out) StandardCharsets/ISO_8859_1))

          (= 10 b)
          (let [bytes (.toByteArray out)
                size  (alength bytes)
                size  (if (and (pos? size)
                               (= 13 (bit-and 0xff (aget bytes (dec size)))))
                        (dec size)
                        size)]
            (String. bytes 0 size StandardCharsets/ISO_8859_1))

          :else
          (do
            (.write out b)
            (recur)))))))

(defn- parse-status-code
  [status-line]
  (let [[_ code] (some->> status-line
                          (re-find #"^HTTP/\d(?:\.\d)?\s+(\d{3})\b"))]
    (if code
      (Integer/parseInt code)
      (throw (ex-info "Invalid HTTP response status line"
                      {:status-line status-line})))))

(defn- read-headers!
  [^InputStream in]
  (loop [headers {}]
    (let [line (read-line-crlf! in)]
      (cond
        (nil? line) headers
        (str/blank? line) headers
        :else
        (let [idx (.indexOf ^String line ":")]
          (if (neg? idx)
            (recur headers)
            (let [header (str/lower-case (str/trim (subs line 0 idx)))
                  value  (str/trim (subs line (inc idx)))]
              (recur (update headers header
                             (fn [existing]
                               (if (seq existing)
                                 (str existing "," value)
                                 value)))))))))))

(defn- parse-response-head!
  [^InputStream in]
  (let [status-line (read-line-crlf! in)
        status      (parse-status-code status-line)
        headers     (read-headers! in)]
    {:status status
     :headers headers}))

(defn- content-length
  [headers]
  (when-let [value (get headers "content-length")]
    (try
      (Long/parseLong (str/trim (first (str/split value #","))))
      (catch Exception _
        nil))))

(defn- chunked-transfer?
  [headers]
  (some #(= "chunked" (str/lower-case (str/trim %)))
        (str/split (or (get headers "transfer-encoding") "") #",")))

(defn- read-exactly!
  [^InputStream in n]
  (let [out       (ByteArrayOutputStream.)
        remaining (atom (long n))
        buf       (byte-array 8192)]
    (while (pos? @remaining)
      (let [n-read (.read in buf 0 (int (min (long (alength buf)) @remaining)))]
        (when (= -1 n-read)
          (throw (EOFException. "HTTP response ended before Content-Length bytes were read")))
        (.write out buf 0 n-read)
        (swap! remaining - n-read)))
    (.toByteArray out)))

(defn- read-until-eof!
  [^InputStream in]
  (read-all-bytes! in))

(defn- read-chunk-size!
  [^InputStream in]
  (let [line (read-line-crlf! in)
        size (some-> line
                     (str/split #";" 2)
                     first
                     str/trim)]
    (when (str/blank? size)
      (throw (EOFException. "HTTP chunked response ended before chunk size was read")))
    (Long/parseLong size 16)))

(defn- consume-crlf!
  [^InputStream in]
  (let [first-byte (.read in)]
    (cond
      (= -1 first-byte) nil
      (= 10 first-byte) nil
      (= 13 first-byte) (let [second-byte (.read in)]
                          (when-not (= 10 second-byte)
                            (throw (java.io.IOException.
                                     "Invalid HTTP chunk delimiter"))))
      :else (throw (java.io.IOException.
                     "Invalid HTTP chunk delimiter")))))

(defn- consume-trailing-headers!
  [^InputStream in]
  (loop []
    (let [line (read-line-crlf! in)]
      (when (and line (not (str/blank? line)))
        (recur)))))

(defn- read-chunked-body!
  [^InputStream in]
  (let [out (ByteArrayOutputStream.)
        buf (byte-array 8192)]
    (loop []
      (let [size (read-chunk-size! in)]
        (if (zero? size)
          (do
            (consume-trailing-headers! in)
            (.toByteArray out))
          (do
            (loop [remaining size]
              (when (pos? remaining)
                (let [n-read (.read in buf 0 (int (min (long (alength buf)) remaining)))]
                  (when (= -1 n-read)
                    (throw (EOFException. "HTTP chunked response ended before chunk bytes were read")))
                  (.write out buf 0 n-read)
                  (recur (- remaining n-read)))))
            (consume-crlf! in)
            (recur)))))))

(defn- bodyless-response?
  [method status]
  (or (= "head" (request-method-name method))
      (<= 100 (long status) 199)
      (#{204 304} (long status))))

(defn- read-response-body!
  [^InputStream in headers method status]
  (cond
    (bodyless-response? method status) (byte-array 0)
    (chunked-transfer? headers) (read-chunked-body! in)
    (some? (content-length headers)) (read-exactly! in (content-length headers))
    :else (read-until-eof! in)))

(defn- ip-literal?
  [host]
  (or (re-matches #"\d+\.\d+\.\d+\.\d+" (or host ""))
      (str/includes? (or host "") ":")))

(defn- sni-host
  [host]
  (when-not (ip-literal? host)
    (try
      (SNIHostName. host)
      (catch Exception _
        nil))))

(defn- pinned-addresses
  [{:keys [addresses url host]}]
  (or (seq addresses)
      (throw (ex-info "HTTP request target has no pinned address"
                      {:url url
                       :host host}))))

(defn- open-pinned-socket-to-address!
  [{:keys [connect-timeout timeout resolved-target]} ^InetAddress address]
  (let [^URI uri       (:uri resolved-target)
        host           (:host resolved-target)
        scheme         (str/lower-case (.getScheme uri))
        port           (effective-port uri)
        raw-socket     (Socket.)]
    (try
      (.connect raw-socket
                (InetSocketAddress. address (int port))
                (int connect-timeout))
      (.setSoTimeout raw-socket (int timeout))
      (if (= "https" scheme)
        (let [factory    (SSLSocketFactory/getDefault)
              ssl-socket ^SSLSocket (.createSocket factory raw-socket host (int port) true)
              params     (.getSSLParameters ssl-socket)]
          (.setEndpointIdentificationAlgorithm params "HTTPS")
          (when-let [server-name (sni-host host)]
            (.setServerNames params [server-name]))
          (.setSSLParameters ssl-socket params)
          (.setSoTimeout ssl-socket (int timeout))
          (.startHandshake ssl-socket)
          ssl-socket)
        raw-socket)
      (catch Exception e
        (try
          (.close raw-socket)
          (catch Exception _))
        (throw e)))))

(defn- open-pinned-socket!
  [{:keys [resolved-target] :as req}]
  (let [addresses (vec (pinned-addresses resolved-target))]
    (loop [idx 0]
      (let [result (try
                     {:socket (open-pinned-socket-to-address! req (nth addresses idx))}
                     (catch Exception e
                       {:error e}))]
        (if-let [socket (:socket result)]
          socket
          (if (< (inc idx) (count addresses))
            (recur (inc idx))
            (throw (:error result))))))))

(defn- timed-out!
  [timeout url cause]
  (throw (ex-info (str "HTTP request timed out after " timeout " ms")
                  {:timeout-ms timeout
                   :url        url}
                  cause)))

(defn- redirect-status?
  [status]
  (contains? redirect-statuses (long status)))

(defn- redirect-url
  [{:keys [resolved-target]} headers]
  (when-let [location (some-> (get headers "location") str/trim)]
    (when-not (str/blank? location)
      (str (.resolve ^URI (:uri resolved-target) location)))))

(defn- chunked-input-stream
  [^InputStream in]
  (let [state (atom {:remaining 0
                     :eof? false})]
    (proxy [InputStream] []
      (read
        ([]
         (let [buf (byte-array 1)
               n   (.read ^InputStream this buf 0 1)]
           (if (= -1 n)
             -1
             (bit-and 0xff (aget buf 0)))))
        ([buf off len]
         (loop []
           (let [{:keys [remaining eof?]} @state]
             (cond
               eof? -1
               (not (pos? len)) 0
               (pos? remaining)
               (let [n-read (.read in buf off (int (min (long len) (long remaining))))]
                 (if (= -1 n-read)
                   -1
                   (do
                     (let [remaining* (- (long remaining) n-read)]
                       (swap! state assoc :remaining remaining*)
                       (when (zero? remaining*)
                         (consume-crlf! in)))
                     n-read)))
               :else
               (let [size (read-chunk-size! in)]
                 (if (zero? size)
                   (do
                     (consume-trailing-headers! in)
                     (swap! state assoc :eof? true)
                     -1)
                   (do
                     (swap! state assoc :remaining size)
                     (recur))))))))))))

(defn- fixed-length-input-stream
  [^InputStream in n]
  (let [remaining (atom (long n))]
    (proxy [InputStream] []
      (read
        ([]
         (if (zero? @remaining)
           -1
           (let [byte (.read in)]
             (if (= -1 byte)
               (throw (EOFException. "HTTP response ended before Content-Length bytes were read"))
               (do
                 (swap! remaining dec)
                 byte)))))
        ([buf off len]
         (cond
           (zero? @remaining) -1
           (not (pos? len)) 0
           :else
           (let [n-read (.read in buf off (int (min (long len) @remaining)))]
             (if (= -1 n-read)
               (throw (EOFException. "HTTP response ended before Content-Length bytes were read"))
               (do
                 (swap! remaining - n-read)
                 n-read)))))))))

(defn- response-body-stream
  [^InputStream in headers]
  (cond
    (chunked-transfer? headers) (chunked-input-stream in)
    (some? (content-length headers)) (fixed-length-input-stream in (content-length headers))
    :else in))

(defn- send-streaming-request!
  [{:keys [connect-timeout timeout on-event]
    :as req}]
  (try
    (with-open [^Socket socket (open-pinned-socket! req)]
      (let [in  (BufferedInputStream. (.getInputStream socket))
            out (BufferedOutputStream. (.getOutputStream socket))]
        (write-http-request! out req)
        (let [{:keys [status headers]} (parse-response-head! in)]
          (if (and (= 200 status)
                   (str/starts-with? (or (get headers "content-type") "")
                                     "text/event-stream"))
            (do
              (when-not on-event
                (throw (ex-info "Streaming HTTP request requires :on-event callback"
                                {:url (request-url req)})))
              (let [^InputStream stream (response-body-stream in headers)
                    reader (BufferedReader.
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
                      {:status status
                       :headers headers
                       :streamed? true})))))
            {:status status
             :headers headers
             :body (decode-body (read-response-body! in headers (:method req) status)
                                headers)}))))
    (catch SocketTimeoutException e
      (timed-out! timeout (request-url req) e))))

(defn- send-request!
  [{:keys [connect-timeout timeout as]
    :or   {as :string}
    :as   req}]
  (try
    (with-open [^Socket socket (open-pinned-socket! req)]
      (let [in  (BufferedInputStream. (.getInputStream socket))
            out (BufferedOutputStream. (.getOutputStream socket))]
        (write-http-request! out req)
        (let [{:keys [status headers]} (parse-response-head! in)
              body (read-response-body! in headers (:method req) status)]
          {:status  status
           :headers headers
           :body    (response-body body headers as)})))
    (catch SocketTimeoutException e
      (timed-out! timeout (request-url req) e))))

(defn- move-file!
  [^Path source ^Path target]
  (try
    (Files/move source target
                (into-array java.nio.file.CopyOption
                            [StandardCopyOption/ATOMIC_MOVE
                             StandardCopyOption/REPLACE_EXISTING]))
    (catch Exception _
      (Files/move source target
                  (into-array java.nio.file.CopyOption
                              [StandardCopyOption/REPLACE_EXISTING])))))

(defn- copy-response-body-to-file!
  [^InputStream in headers ^Path target]
  (with-open [stream (response-body-stream in headers)]
    (Files/copy ^InputStream stream
                target
                (into-array java.nio.file.CopyOption
                            [StandardCopyOption/REPLACE_EXISTING]))))

(defn- delete-if-exists-quietly!
  [^Path path]
  (try
    (Files/deleteIfExists path)
    (catch Exception _)))

(defn- send-download-request!
  [{:keys [timeout target-path expected-status] :or {expected-status 200} :as req}]
  (try
    (with-open [^Socket socket (open-pinned-socket! req)]
      (let [in       (BufferedInputStream. (.getInputStream socket))
            out      (BufferedOutputStream. (.getOutputStream socket))
            ^Path target (Paths/get target-path (make-array String 0))]
        (write-http-request! out req)
        (let [{:keys [status headers]} (parse-response-head! in)]
          (if-let [location (when (redirect-status? status)
                              (redirect-url req headers))]
            {:status status
             :headers headers
             :redirect-url location}
            (let [^Path parent (.getParent target)
                  tmp-dir  (or parent (Paths/get "." (make-array String 0)))
                  _        (when parent
                             (Files/createDirectories parent (make-array FileAttribute 0)))
                  tmp      (Files/createTempFile tmp-dir
                                                 (str (.getFileName target) ".part-")
                                                 ".tmp"
                                                 (make-array FileAttribute 0))]
              (try
                (when-not (= (long expected-status) (long status))
                  (throw (ex-info "HTTP download returned unexpected status"
                                  {:url (request-url req)
                                   :status status
                                   :expected-status expected-status
                                   :target target-path})))
                (copy-response-body-to-file! in headers tmp)
                (move-file! tmp target)
                {:status status
                 :headers headers
                 :target-path target-path}
                (finally
                  (when (Files/exists tmp (make-array java.nio.file.LinkOption 0))
                    (delete-if-exists-quietly! tmp)))))))))
    (catch SocketTimeoutException e
      (timed-out! timeout (request-url req) e))))

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

(declare follow-response-redirect-request)

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
     :follow-redirects?    follow 301/302/303/307/308 redirects, default false
     :max-redirects        redirect limit when :follow-redirects? is true, default 10
     :allow-private-network? bypass SSRF private-network blocking for explicitly trusted targets
     :trusted? / :trusted    aliases for trusted private-network bypass
     :request-label        optional log label
     :policy-observer      optional callback for retry-policy decisions

   Redirects are not followed unless :follow-redirects? is true. Followed
   redirects are revalidated through the SSRF guard. Cross-origin redirects
   strip Authorization, Cookie, and Proxy-Authorization headers; 303 switches
   to GET and drops the request body."
  [{:keys [connect-timeout timeout request-label policy-observer follow-redirects? max-redirects]
    :or   {connect-timeout    default-connect-timeout-ms
           timeout            default-request-timeout-ms}
    :as   req}]
  (let [base-req (merge req
                        {:connect-timeout connect-timeout
                         :timeout timeout})
        max-redirects (long (or max-redirects default-max-redirects))]
    (letfn [(attempt-with-retries [req]
              (let [retry-config (http-policy/http-request-retry-config req)
                    req (merge req retry-config)
                    resolved-target (validate-request-target! req)
                    request-url-str (:url resolved-target)
                    req (assoc req :resolved-target resolved-target)
                    label (or request-label "HTTP request")
                    emit-policy! (fn [decision]
                                   (when policy-observer
                                     (policy-observer
                                      (assoc decision
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
                                         (let [decision (http-policy/http-request-retry-decision
                                                         req
                                                         attempt
                                                         {:transient-exception? (transient-exception? e)
                                                          :reason (.getMessage e)})]
                                           (emit-policy! decision)
                                           (if (:allowed? decision)
                                             (retry! decision (.getMessage e))
                                             (throw e)))))
                                status (:status resp)]
                            (let [decision (http-policy/http-request-retry-decision
                                            req
                                            attempt
                                            {:status status
                                             :reason (str label " returned transient status " status)})]
                              (when-not (successful-status? status)
                                (emit-policy! decision))
                              (if (:allowed? decision)
                                (retry! decision (str label " returned transient status " status))
                                (assoc resp
                                       :attempt (or (:attempt resp) attempt)
                                       :request req)))))]
                  (attempt-request 1))))]
      (loop [req base-req
             remaining-redirects max-redirects]
        (let [resp (attempt-with-retries req)]
          (if-let [redirect-url (when follow-redirects?
                                  (when (redirect-status? (:status resp))
                                    (redirect-url (:request resp) (:headers resp))))]
            (if (pos? remaining-redirects)
              (recur (follow-response-redirect-request (:request resp) resp redirect-url)
                     (dec remaining-redirects))
              (throw (ex-info "HTTP request exceeded redirect limit"
                              {:url (request-url (:request resp))
                               :redirect-url redirect-url
                               :max-redirects max-redirects})))
            (dissoc resp :request)))))))

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
        resolved-target (validate-request-target! req)]
    (send-streaming-request! (assoc req
                                    :on-event on-event
                                    :resolved-target resolved-target))))

(defn- follow-redirect-request
  [req redirect-url]
  (-> req
      (assoc :url redirect-url)
      (dissoc :uri :query-params :resolved-target)))

(defn- follow-response-redirect-request
  [req resp redirect-url]
  (let [old-uri    (:uri (:resolved-target req))
        new-uri    (URI. redirect-url)
        status     (:status resp)
        switch-get? (= 303 (long status))]
    (cond-> (follow-redirect-request req redirect-url)
      (and old-uri (not (same-origin? old-uri new-uri)))
      (update :headers remove-cross-origin-sensitive-headers)

      switch-get?
      (assoc :method :get)

      switch-get?
      (dissoc :body)

      switch-get?
      (update :headers remove-body-headers))))

(defn download!
  "Download an HTTP(S) resource to `:target-path` using the guarded egress path.

   Supports the same URL, timeout, header, and private-network policy options as
   `request`. The response body is streamed to a temporary file in the target
   directory and atomically moved into place when possible."
  [{:keys [connect-timeout timeout target-path headers max-redirects]
    :or   {connect-timeout default-connect-timeout-ms
           timeout         default-request-timeout-ms
           max-redirects   default-max-redirects}
    :as   req}]
  (when-not (seq (or target-path ""))
    (throw (ex-info "HTTP download requires :target-path" {})))
  (let [req (merge {:method :get
                    :headers {"User-Agent" "xia"
                              "Accept" "application/octet-stream"}}
                   req
                   {:connect-timeout connect-timeout
                    :timeout timeout
                    :headers (merge {"User-Agent" "xia"
                                     "Accept" "application/octet-stream"}
                                    headers)})]
    (loop [req req
           remaining-redirects (long max-redirects)]
      (let [resolved-target (validate-request-target! req)
            resp (send-download-request! (assoc req :resolved-target resolved-target))]
        (if-let [redirect-url (:redirect-url resp)]
          (if (pos? remaining-redirects)
            (recur (follow-redirect-request req redirect-url)
                   (dec remaining-redirects))
            (throw (ex-info "HTTP download exceeded redirect limit"
                            {:url (request-url req)
                             :redirect-url redirect-url
                             :max-redirects max-redirects})))
          resp)))))
