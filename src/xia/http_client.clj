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
           [java.util.concurrent ArrayBlockingQueue Callable ExecutionException Future RejectedExecutionException
            ThreadFactory ThreadPoolExecutor ThreadPoolExecutor$AbortPolicy TimeUnit TimeoutException]
           [javax.net.ssl SNIHostName SSLSocket SSLSocketFactory]))

(def ^:private default-connect-timeout-ms 30000)
(def ^:private default-request-timeout-ms 120000)
(def ^:private default-max-redirects 10)
(def ^:private default-max-response-bytes (* 64 1024 1024))
(def ^:private default-max-download-bytes (* 8 1024 1024 1024))
(def ^:private default-max-response-header-count 200)
(def ^:private default-max-response-header-line-bytes (* 16 1024))
(def ^:private default-max-response-header-bytes (* 64 1024))
(def ^:private dns-worker-count 4)
(def ^:private dns-queue-capacity 64)
(def ^:private byte-array-class (Class/forName "[B"))
(def ^:private redirect-statuses #{301 302 303 307 308})
(def ^:private cross-origin-sensitive-headers
  #{"authorization" "cookie" "proxy-authorization"})
(def ^:private body-headers
  #{"content-length" "content-type"})

(def ^:private dns-executor
  (delay
    (ThreadPoolExecutor.
      dns-worker-count
      dns-worker-count
      0
      TimeUnit/MILLISECONDS
      (ArrayBlockingQueue. dns-queue-capacity)
      (reify ThreadFactory
        (newThread [_ runnable]
          (doto (Thread. runnable "xia-http-dns")
            (.setDaemon true))))
      (ThreadPoolExecutor$AbortPolicy.))))

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

(defn- positive-long-option
  [value option-name]
  (let [parsed (try
                 (long value)
                 (catch Exception _ nil))]
    (when-not (and parsed (pos? parsed))
      (throw (ex-info (str option-name " must be a positive integer")
                      {:option option-name
                       :value value})))
    parsed))

(defn- now-nanos
  []
  (System/nanoTime))

(defn- deadline-after-ms
  [timeout-ms]
  (let [now   (now-nanos)
        delta (.toNanos TimeUnit/MILLISECONDS (long timeout-ms))]
    (unchecked-add now delta)))

(defn- timed-out!
  [req phase cause]
  (let [timeout (long (:timeout req))
        url     (request-url req)]
    (throw (ex-info (str "HTTP request timed out after " timeout " ms")
                    {:type       :http/deadline-exceeded
                     :phase      phase
                     :timeout-ms timeout
                     :url        url}
                    cause))))

(defn- deadline-exceeded?
  [error]
  (= :http/deadline-exceeded (:type (ex-data error))))

(defn- remaining-ms!
  ([req]
   (remaining-ms! req :request))
  ([req phase]
   (let [remaining-nanos (unchecked-subtract (long (:deadline-nanos req))
                                              (now-nanos))]
     (when-not (pos? remaining-nanos)
       (timed-out! req phase (TimeoutException. "HTTP request deadline exceeded")))
     (min (long Integer/MAX_VALUE)
          (inc (quot (dec remaining-nanos) 1000000))))))

(defn- bounded-timeout-ms
  [req configured-timeout phase]
  (int (min (positive-long-option configured-timeout :connect-timeout)
            (remaining-ms! req phase)
            (long Integer/MAX_VALUE))))

(defn- with-request-bounds
  [req timeout]
  (let [timeout* (positive-long-option timeout :timeout)]
    (merge {:max-response-bytes default-max-response-bytes
            :max-response-header-count default-max-response-header-count
            :max-response-header-line-bytes default-max-response-header-line-bytes
            :max-response-header-bytes default-max-response-header-bytes}
           req
           {:timeout timeout*
            :deadline-nanos (or (:deadline-nanos req)
                                (deadline-after-ms timeout*))
            :max-response-bytes (positive-long-option
                                  (or (:max-response-bytes req)
                                      default-max-response-bytes)
                                  :max-response-bytes)
            :max-response-header-count (positive-long-option
                                         (or (:max-response-header-count req)
                                             default-max-response-header-count)
                                         :max-response-header-count)
            :max-response-header-line-bytes (positive-long-option
                                              (or (:max-response-header-line-bytes req)
                                                  default-max-response-header-line-bytes)
                                              :max-response-header-line-bytes)
            :max-response-header-bytes (positive-long-option
                                         (or (:max-response-header-bytes req)
                                             default-max-response-header-bytes)
                                         :max-response-header-bytes)})))

(defn- trusted-request?
  [{:keys [allow-private-network? trusted? trusted]}]
  (boolean (or allow-private-network? trusted? trusted)))

(defn- resolve-url-before-deadline!
  [req request-url-str]
  (let [^ThreadPoolExecutor executor @dns-executor
        ^Future task (try
               (.submit executor
                        ^Callable
                        (reify Callable
                          (call [_]
                            (ssrf/resolve-url!
                              request-url-str
                              {:allow-private-network? (trusted-request? req)}))))
               (catch RejectedExecutionException e
                 (throw (ex-info "HTTP DNS resolver capacity exceeded"
                                 {:type :http/dns-capacity
                                  :url request-url-str
                                  :capacity dns-queue-capacity}
                                 e))))]
    (try
      (.get task (remaining-ms! req :dns) TimeUnit/MILLISECONDS)
      (catch java.util.concurrent.TimeoutException e
        (.cancel task true)
        (.purge executor)
        (timed-out! req :dns e))
      (catch InterruptedException e
        (.cancel task true)
        (.purge executor)
        (.interrupt (Thread/currentThread))
        (throw e))
      (catch ExecutionException e
        (throw (or (.getCause e) e))))))

(defn- validate-request-target!
  [{:keys [url uri query-params] :as req}]
  (let [request-url-str (request-url {:url url :uri uri :query-params query-params})
        parsed-uri      (URI. request-url-str)
        resolution      (resolve-url-before-deadline! req request-url-str)]
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

(defn- limit-exceeded!
  [req limit-name max-value actual-value]
  (throw (ex-info (str "HTTP response exceeded " (name limit-name) " limit")
                  {:type :http/limit-exceeded
                   :limit limit-name
                   :max max-value
                   :actual actual-value
                   :url (request-url req)})))

(defn- read-line-crlf!
  [^InputStream in req total-bytes-atom]
  (let [out      (ByteArrayOutputStream.)
        max-line (long (:max-response-header-line-bytes req))
        max-total (long (:max-response-header-bytes req))]
    (loop [line-bytes 0]
      (remaining-ms! req :response-headers)
      (let [b (.read in)]
        (when (not= -1 b)
          (let [total (swap! total-bytes-atom inc)]
            (when (> (long total) max-total)
              (limit-exceeded! req :header-bytes max-total total))))
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
            (String. bytes 0 (int size) StandardCharsets/ISO_8859_1))

          :else
          (let [line-bytes* (inc (long line-bytes))]
            (when (> line-bytes* max-line)
              (limit-exceeded! req :header-line-bytes max-line line-bytes*))
            (.write out b)
            (recur line-bytes*)))))))

(defn- parse-status-code
  [status-line]
  (let [[_ code] (some->> status-line
                          (re-find #"^HTTP/\d(?:\.\d)?\s+(\d{3})\b"))]
    (if code
      (Integer/parseInt code)
      (throw (ex-info "Invalid HTTP response status line"
                      {:status-line status-line})))))

(defn- read-headers!
  [^InputStream in req total-bytes-atom]
  (let [max-count (long (:max-response-header-count req))]
    (loop [headers {}
           count* 0]
      (let [line (read-line-crlf! in req total-bytes-atom)]
      (cond
        (nil? line) headers
        (str/blank? line) headers
        :else
        (let [count** (inc (long count*))
              idx     (.indexOf ^String line ":")]
          (when (> count** max-count)
            (limit-exceeded! req :header-count max-count count**))
          (when (neg? idx)
            (throw (ex-info "Invalid HTTP response header"
                            {:type :http/invalid-response-header
                             :url (request-url req)})))
          (let [header (str/lower-case (str/trim (subs line 0 idx)))
                value  (str/trim (subs line (inc idx)))]
            (when (str/blank? header)
              (throw (ex-info "Invalid HTTP response header name"
                              {:type :http/invalid-response-header
                               :url (request-url req)})))
            (recur (update headers header
                           (fn [existing]
                             (if (seq existing)
                               (str existing "," value)
                               value)))
                   count**))))))))

(defn- parse-response-head!
  [^InputStream in req]
  (let [total-bytes (atom 0)
        status-line (read-line-crlf! in req total-bytes)
        status      (parse-status-code status-line)
        headers     (read-headers! in req total-bytes)]
    {:status status
     :headers headers}))

(defn- content-length
  [headers]
  (when-let [value (get headers "content-length")]
    (let [values (mapv str/trim (str/split value #","))
          lengths (try
                    (mapv #(Long/parseLong %) values)
                    (catch Exception e
                      (throw (ex-info "Invalid HTTP Content-Length"
                                      {:type :http/invalid-content-length
                                       :value value}
                                      e))))]
      (when (or (some neg? lengths)
                (not (apply = lengths)))
        (throw (ex-info "Invalid or conflicting HTTP Content-Length"
                        {:type :http/invalid-content-length
                         :value value})))
      (first lengths))))

(defn- chunked-transfer?
  [headers]
  (some #(= "chunked" (str/lower-case (str/trim %)))
        (str/split (or (get headers "transfer-encoding") "") #",")))

(defn- read-chunk-size!
  [^InputStream in req trailer-bytes]
  (let [line (read-line-crlf! in req trailer-bytes)
        size-text (some-> line
                          (str/split #";" 2)
                          first
                          str/trim)]
    (when (str/blank? size-text)
      (throw (EOFException. "HTTP chunked response ended before chunk size was read")))
    (let [size (Long/parseLong size-text 16)]
      (when (neg? size)
        (throw (java.io.IOException. "Invalid negative HTTP chunk size")))
      size)))

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
  [^InputStream in req trailer-bytes]
  (let [max-count (long (:max-response-header-count req))]
    (loop [count* 0]
      (let [line (read-line-crlf! in req trailer-bytes)]
        (when (and line (not (str/blank? line)))
          (let [count** (inc (long count*))]
            (when (> count** max-count)
              (limit-exceeded! req :trailer-count max-count count**))
            (recur count**)))))))

(defn- bodyless-response?
  [method status]
  (or (= "head" (request-method-name method))
      (<= 100 (long status) 199)
      (#{204 304} (long status))))

(declare response-body-stream)

(defn- read-response-body!
  [^InputStream in headers method status req]
  (if (bodyless-response? method status)
    (byte-array 0)
    (with-open [^InputStream stream (response-body-stream in headers req)]
      (read-all-bytes! stream))))

(defn- ip-literal?
  [host]
  (or (re-matches #"\d+\.\d+\.\d+\.\d+" (or host ""))
      (str/includes? (or host "") ":")))

(defn- sni-host
  [host]
  (when-not (ip-literal? host)
    (try
      (SNIHostName. ^String host)
      (catch Exception _
        nil))))

(defn- pinned-addresses
  [{:keys [addresses url host]}]
  (or (seq addresses)
      (throw (ex-info "HTTP request target has no pinned address"
                      {:url url
                       :host host}))))

(defn- open-pinned-socket-to-address!
  [{:keys [connect-timeout resolved-target] :as req} ^InetAddress address]
  (let [^URI uri       (:uri resolved-target)
        host           (:host resolved-target)
        scheme         (str/lower-case (.getScheme uri))
        port           (effective-port uri)
        raw-socket     (Socket.)]
    (try
      (.connect raw-socket
                (InetSocketAddress. address (int port))
                (bounded-timeout-ms req connect-timeout :connect))
      (.setSoTimeout raw-socket
                     (int (remaining-ms! req :response)))
      (if (= "https" scheme)
        (let [factory    (SSLSocketFactory/getDefault)
              ssl-socket ^SSLSocket (.createSocket factory raw-socket ^String host (int port) true)
              params     (.getSSLParameters ssl-socket)]
          (.setEndpointIdentificationAlgorithm params "HTTPS")
          (when-let [server-name (sni-host host)]
            (.setServerNames params [server-name]))
          (.setSSLParameters ssl-socket params)
          (.setSoTimeout ssl-socket
                         (int (remaining-ms! req :tls-handshake)))
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
                       (if (deadline-exceeded? e)
                         (throw e)
                         {:error e})))]
        (if-let [socket (:socket result)]
          socket
          (if (< (inc idx) (count addresses))
            (recur (inc idx))
            (throw (:error result))))))))

(defn- deadline-input-stream
  [^Socket socket ^InputStream in req]
  (proxy [InputStream] []
    (read
      ([]
       (.setSoTimeout socket (int (remaining-ms! req :response-body)))
       (.read in))
      ([buf off len]
       (.setSoTimeout socket (int (remaining-ms! req :response-body)))
       (.read in buf off len)))
    (close []
      (.close in))))

(defn- redirect-status?
  [status]
  (contains? redirect-statuses (long status)))

(defn- redirect-url
  [{:keys [resolved-target]} headers]
  (when-let [location (some-> (get headers "location") str/trim)]
    (when-not (str/blank? location)
      (str (.resolve ^URI (:uri resolved-target) location)))))

(defn- chunked-input-stream
  [^InputStream in req]
  (let [state (atom {:remaining 0
                     :eof? false})
        framing-bytes (atom 0)]
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
                   (throw (EOFException. "HTTP chunked response ended before chunk bytes were read"))
                   (do
                     (let [remaining* (- (long remaining) n-read)]
                       (swap! state assoc :remaining remaining*)
                       (when (zero? remaining*)
                         (consume-crlf! in)))
                     n-read)))
               :else
               (let [size (read-chunk-size! in req framing-bytes)]
                 (if (zero? size)
                   (do
                     (consume-trailing-headers! in req framing-bytes)
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

(defn- limited-input-stream
  [^InputStream in req max-bytes]
  (let [total (atom 0)
        max-bytes (long max-bytes)]
    (proxy [InputStream] []
      (read
        ([]
         (let [byte (.read in)]
           (if (= -1 byte)
             -1
             (let [total* (swap! total inc)]
               (when (> (long total*) max-bytes)
                 (limit-exceeded! req :response-bytes max-bytes total*))
               byte))))
        ([buf]
         (.read ^InputStream this buf 0 (alength ^bytes buf)))
        ([buf off len]
         (if-not (pos? len)
           0
           (let [remaining (- max-bytes (long @total))
                 read-len  (int (min (long len) (inc (max 0 remaining))))
                 n-read    (.read in buf off read-len)]
             (if (= -1 n-read)
               -1
               (let [total* (swap! total + n-read)]
                 (when (> (long total*) max-bytes)
                   (limit-exceeded! req :response-bytes max-bytes total*))
                 n-read))))))
      (close []
        (.close in)))))

(defn- response-body-stream
  [^InputStream in headers req]
  (let [max-bytes (long (:max-response-bytes req))
        length    (content-length headers)]
    (when (and length (> (long length) max-bytes))
      (limit-exceeded! req :response-bytes max-bytes length))
    (limited-input-stream
      (cond
        (chunked-transfer? headers) (chunked-input-stream in req)
        (some? length) (fixed-length-input-stream in length)
        :else in)
      req
      max-bytes)))

(defn- send-streaming-request!
  [{:keys [on-event]
    :as req}]
  (try
    (with-open [^Socket socket (open-pinned-socket! req)]
      (let [in  (deadline-input-stream
                  socket
                  (BufferedInputStream. (.getInputStream socket))
                  req)
            out (BufferedOutputStream. (.getOutputStream socket))]
        (write-http-request! out req)
        (let [{:keys [status headers]} (parse-response-head! in req)]
          (if (and (= 200 status)
                   (str/starts-with? (or (get headers "content-type") "")
                                     "text/event-stream"))
            (do
              (when-not on-event
                (throw (ex-info "Streaming HTTP request requires :on-event callback"
                                {:url (request-url req)})))
              (let [^InputStream stream (response-body-stream in headers req)
                    reader (BufferedReader.
                             (InputStreamReader. stream StandardCharsets/UTF_8))]
                (loop [event-type nil
                       data-lines []]
                  (remaining-ms! req :response-body)
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
             :body (decode-body (read-response-body! in headers (:method req) status req)
                                headers)}))))
    (catch SocketTimeoutException e
      (timed-out! req :response-body e))))

(defn- send-request!
  [{:keys [as]
    :or   {as :string}
    :as   req}]
  (try
    (with-open [^Socket socket (open-pinned-socket! req)]
      (let [in  (deadline-input-stream
                  socket
                  (BufferedInputStream. (.getInputStream socket))
                  req)
            out (BufferedOutputStream. (.getOutputStream socket))]
        (write-http-request! out req)
        (let [{:keys [status headers]} (parse-response-head! in req)
              body (read-response-body! in headers (:method req) status req)]
          {:status  status
           :headers headers
           :body    (response-body body headers as)})))
    (catch SocketTimeoutException e
      (timed-out! req :response-body e))))

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
  [^InputStream in headers ^Path target req]
  (with-open [^InputStream stream (response-body-stream in headers req)]
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
  [{:keys [target-path expected-status] :or {expected-status 200} :as req}]
  (try
    (with-open [^Socket socket (open-pinned-socket! req)]
      (let [in       (deadline-input-stream
                       socket
                       (BufferedInputStream. (.getInputStream socket))
                       req)
            out      (BufferedOutputStream. (.getOutputStream socket))
            ^Path target (Paths/get target-path (make-array String 0))]
        (write-http-request! out req)
        (let [{:keys [status headers]} (parse-response-head! in req)]
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
                (copy-response-body-to-file! in headers tmp req)
                (move-file! tmp target)
                {:status status
                 :headers headers
                 :target-path target-path}
                (finally
                  (when (Files/exists tmp (make-array java.nio.file.LinkOption 0))
                    (delete-if-exists-quietly! tmp)))))))))
    (catch SocketTimeoutException e
      (timed-out! req :response-body e))))

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

(defn- sleep-before-deadline!
  [req delay-ms]
  (let [remaining (remaining-ms! req :retry-backoff)]
    (when (>= (long delay-ms) remaining)
      (timed-out! req :retry-backoff
                  (TimeoutException. "Retry backoff exceeds request deadline")))
    (sleep-ms! delay-ms)
    (remaining-ms! req :retry-backoff)))

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
     :max-response-bytes   maximum decoded response body bytes, default 64 MiB
     :max-response-header-count maximum response header fields, default 200
     :max-response-header-line-bytes maximum status/header line bytes, default 16 KiB
     :max-response-header-bytes maximum total status/header bytes, default 64 KiB
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
  (let [base-req (with-request-bounds
                   (merge req {:connect-timeout connect-timeout})
                   timeout)
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
                            (sleep-before-deadline! req delay-ms)
                            (attempt-request (inc (long (:attempt decision))))))
                        (attempt-request [attempt]
                          (remaining-ms! req :attempt)
                          (let [resp (try
                                       (send-request! req)
                                       (catch Exception e
                                         (if (deadline-exceeded? e)
                                           (throw e)
                                           (let [decision (http-policy/http-request-retry-decision
                                                           req
                                                           attempt
                                                           {:transient-exception? (transient-exception? e)
                                                            :reason (.getMessage e)})]
                                             (emit-policy! decision)
                                             (if (:allowed? decision)
                                               (retry! decision (.getMessage e))
                                               (throw e))))))
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
        (remaining-ms! req :redirect)
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
  (let [req (with-request-bounds
              (assoc req :connect-timeout connect-timeout)
              timeout)
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

(defn- follow-download-redirect-request
  [req redirect-url]
  (let [old-uri (:uri (:resolved-target req))
        new-uri (URI. redirect-url)]
    (cond-> (follow-redirect-request req redirect-url)
      (and old-uri (not (same-origin? old-uri new-uri)))
      (update :headers remove-cross-origin-sensitive-headers))))

(defn download!
  "Download an HTTP(S) resource to `:target-path` using the guarded egress path.

   Supports the same URL, timeout, header, and private-network policy options as
   `request`. The response body is streamed to a temporary file in the target
   directory and atomically moved into place when possible. Downloads default
   to an 8 GiB maximum; callers may lower it with `:max-download-bytes`."
  [{:keys [connect-timeout timeout target-path headers max-redirects max-download-bytes]
    :or   {connect-timeout default-connect-timeout-ms
           timeout         default-request-timeout-ms
           max-redirects   default-max-redirects}
    :as   req}]
  (when-not (seq (or target-path ""))
    (throw (ex-info "HTTP download requires :target-path" {})))
  (let [download-limit (or max-download-bytes
                           (:max-response-bytes req)
                           default-max-download-bytes)
        req (with-request-bounds
              (merge {:method :get
                      :headers {"User-Agent" "xia"
                                "Accept" "application/octet-stream"}}
                     req
                     {:connect-timeout connect-timeout
                      :max-response-bytes download-limit
                      :max-download-bytes (positive-long-option
                                            download-limit
                                            :max-download-bytes)
                      :headers (merge {"User-Agent" "xia"
                                       "Accept" "application/octet-stream"}
                                      headers)})
              timeout)]
    (loop [req req
           remaining-redirects (long max-redirects)]
      (remaining-ms! req :redirect)
      (let [resolved-target (validate-request-target! req)
            resp (send-download-request! (assoc req :resolved-target resolved-target))]
        (if-let [redirect-url (:redirect-url resp)]
          (if (pos? remaining-redirects)
            (recur (follow-download-redirect-request
                     (assoc req :resolved-target resolved-target)
                     redirect-url)
                   (dec remaining-redirects))
            (throw (ex-info "HTTP download exceeded redirect limit"
                            {:url (request-url req)
                             :redirect-url redirect-url
                             :max-redirects max-redirects})))
          resp)))))
