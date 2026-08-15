(ns xia.browser.egress-proxy
  "Loopback SOCKS5 proxy that makes Xia's SSRF resolver authoritative.

   The browser sends destination hostnames to this proxy. Each CONNECT request
   is resolved once through `resolve-url!`, and the outbound socket connects
   only to the returned address set. TLS and higher-level browser protocols are
   tunneled unchanged."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log])
  (:import [java.io BufferedInputStream BufferedOutputStream DataInputStream
            DataOutputStream InputStream OutputStream]
           [java.net ConnectException InetAddress InetSocketAddress
            NoRouteToHostException ServerSocket Socket SocketException
            SocketTimeoutException UnknownHostException]
           [java.nio.charset StandardCharsets]
           [java.util Set]
           [java.util.concurrent ExecutorService Executors Future Semaphore
            ThreadFactory TimeUnit]
           [java.util.concurrent.atomic AtomicLong]))

(def ^:private socks-version 5)
(def ^:private no-auth-method 0)
(def ^:private no-acceptable-methods 0xff)
(def ^:private connect-command 1)
(def ^:private ipv4-address-type 1)

(def ^:private reply-succeeded 0)
(def ^:private reply-ruleset-denied 2)
(def ^:private reply-network-unreachable 3)
(def ^:private reply-host-unreachable 4)
(def ^:private reply-connection-refused 5)
(def ^:private reply-command-unsupported 7)
(def ^:private reply-address-type-unsupported 8)

(def ^:private default-connect-timeout-ms 10000)
(def ^:private default-max-connections 32)
(def ^:private tunnel-buffer-size 16384)

(defn- loopback-address
  []
  (InetAddress/getByAddress
   (byte-array [(byte 127) (byte 0) (byte 0) (byte 1)])))

(defn- daemon-thread-factory
  [prefix]
  (let [counter (AtomicLong.)]
    (reify ThreadFactory
      (newThread [_ runnable]
        (doto (Thread. ^Runnable runnable)
          (.setName (str prefix (.incrementAndGet counter)))
          (.setDaemon true))))))

(defn- close-socket-quietly!
  [socket]
  (when (instance? Socket socket)
    (try
      (.close ^Socket socket)
      (catch Exception _))))

(defn- track-socket!
  [^Set sockets ^Socket socket]
  (.add sockets socket)
  socket)

(defn- untrack-socket!
  [^Set sockets ^Socket socket]
  (.remove sockets socket)
  socket)

(defn- socks-error
  ([message reply]
   (ex-info message {:socks-reply reply}))
  ([message reply cause]
   (ex-info message {:socks-reply reply} cause)))

(defn- write-method-selection!
  [^DataOutputStream out method]
  (let [^bytes response (byte-array [(unchecked-byte socks-version)
                                     (unchecked-byte method)])]
    (.write out response 0 (alength response))
    (.flush out)))

(defn- write-reply!
  [^DataOutputStream out reply]
  ;; An all-zero bound address is permitted when the proxy does not expose a
  ;; meaningful address to the client.
  (let [^bytes response (byte-array [(unchecked-byte socks-version)
                                     (unchecked-byte reply)
                                     (unchecked-byte 0)
                                     (unchecked-byte ipv4-address-type)
                                     (unchecked-byte 0)
                                     (unchecked-byte 0)
                                     (unchecked-byte 0)
                                     (unchecked-byte 0)
                                     (unchecked-byte 0)
                                     (unchecked-byte 0)])]
    (.write out response 0 (alength response))
    (.flush out)))

(defn- negotiate-no-auth!
  [^DataInputStream in ^DataOutputStream out]
  (when-not (= socks-version (.readUnsignedByte in))
    (throw (ex-info "Unsupported SOCKS version" {})))
  (let [method-count (.readUnsignedByte in)
        ^bytes methods (byte-array method-count)]
    (.readFully in methods)
    (if (some #(= no-auth-method (bit-and 0xff (int %))) methods)
      (write-method-selection! out no-auth-method)
      (do
        (write-method-selection! out no-acceptable-methods)
        (throw (ex-info "SOCKS client did not offer no-authentication mode" {}))))))

(defn- read-address-bytes!
  [^DataInputStream in length]
  (let [^bytes value (byte-array length)]
    (.readFully in value)
    value))

(defn- valid-domain?
  [host]
  (and (not (str/blank? host))
       (boolean (re-matches #"(?i)[a-z0-9._-]+" host))))

(defn- read-destination!
  [^DataInputStream in]
  (when-not (= socks-version (.readUnsignedByte in))
    (throw (socks-error "Unsupported SOCKS request version"
                        reply-command-unsupported)))
  (when-not (= connect-command (.readUnsignedByte in))
    (throw (socks-error "Only SOCKS CONNECT requests are supported"
                        reply-command-unsupported)))
  (when-not (zero? (.readUnsignedByte in))
    (throw (socks-error "Invalid SOCKS reserved byte"
                        reply-command-unsupported)))
  (let [address-type (.readUnsignedByte in)
        host (case address-type
               1 (.getHostAddress
                  (InetAddress/getByAddress
                   (read-address-bytes! in 4)))
               3 (let [length (.readUnsignedByte in)
                       value  (String. ^bytes (read-address-bytes! in length)
                                       StandardCharsets/US_ASCII)]
                   (when-not (valid-domain? value)
                     (throw (socks-error "Invalid SOCKS destination hostname"
                                         reply-address-type-unsupported)))
                   value)
               4 (.getHostAddress
                  (InetAddress/getByAddress
                   (read-address-bytes! in 16)))
               (throw (socks-error "Unsupported SOCKS address type"
                                   reply-address-type-unsupported)))
        port (.readUnsignedShort in)]
    (when (zero? port)
      (throw (socks-error "SOCKS destination port must be positive"
                          reply-ruleset-denied)))
    {:host host :port port}))

(defn- destination-url
  [host port]
  (str "http://"
       (if (str/includes? host ":")
         (str "[" host "]")
         host)
       ":" port "/"))

(defn- resolve-destination!
  [resolve-url! host port]
  (try
    (let [resolution (resolve-url! (destination-url host port))
          addresses  (vec (:addresses resolution))]
      (when (empty? addresses)
        (throw (UnknownHostException.
                (str "No addresses returned for " host))))
      addresses)
    (catch UnknownHostException e
      (throw (socks-error "SOCKS destination host is unreachable"
                          reply-host-unreachable
                          e)))
    (catch Exception e
      (throw (socks-error "SOCKS destination was denied by egress policy"
                          reply-ruleset-denied
                          e)))))

(defn- remaining-connect-ms
  [deadline-nanos]
  (let [remaining (quot (- deadline-nanos (System/nanoTime)) 1000000)]
    (int (max 1 (min Integer/MAX_VALUE remaining)))))

(defn- connection-reply
  [error]
  (cond
    (instance? ConnectException error) reply-connection-refused
    (instance? NoRouteToHostException error) reply-network-unreachable
    (instance? SocketTimeoutException error) reply-host-unreachable
    :else reply-host-unreachable))

(defn- connect-one!
  [^Set sockets ^InetAddress address port timeout-ms]
  (let [socket (track-socket! sockets (Socket.))]
    (try
      (.connect ^Socket socket
                (InetSocketAddress. address (int port))
                (int timeout-ms))
      (.setTcpNoDelay ^Socket socket true)
      {:socket socket}
      (catch Exception e
        (untrack-socket! sockets socket)
        (close-socket-quietly! socket)
        {:error e}))))

(defn- connect-pinned!
  [^Set sockets addresses port connect-timeout-ms]
  (let [deadline (+ (System/nanoTime)
                    (* 1000000 (long connect-timeout-ms)))]
    (loop [remaining addresses
           last-error nil]
      (if-let [^InetAddress address (first remaining)]
        (if (<= deadline (System/nanoTime))
          (throw (socks-error "Timed out connecting to pinned SOCKS destination"
                              reply-host-unreachable
                              (or last-error (SocketTimeoutException.))))
          (let [{:keys [socket error]}
                (connect-one! sockets
                              address
                              port
                              (remaining-connect-ms deadline))]
            (if socket
              socket
              (recur (next remaining) error))))
        (throw (socks-error "Could not connect to any pinned SOCKS destination"
                            (connection-reply last-error)
                            last-error))))))

(defn- pump!
  [^InputStream in ^OutputStream out]
  (let [^bytes buffer (byte-array tunnel-buffer-size)]
    (loop []
      (let [read-count (.read in buffer 0 (alength buffer))]
        (when-not (neg? read-count)
          (when (pos? read-count)
            (.write out buffer 0 read-count)
            (.flush out))
          (recur))))))

(defn- shutdown-output-quietly!
  [^Socket socket]
  (try
    (.shutdownOutput socket)
    (catch Exception _)))

(defn- tunnel!
  [^ExecutorService executor ^Socket client ^Socket remote]
  (.setSoTimeout client 0)
  (.setSoTimeout remote 0)
  (let [client-in  (BufferedInputStream. (.getInputStream client))
        client-out (BufferedOutputStream. (.getOutputStream client))
        remote-in  (BufferedInputStream. (.getInputStream remote))
        remote-out (BufferedOutputStream. (.getOutputStream remote))
        upstream   (.submit executor
                            ^Runnable
                            (reify Runnable
                              (run [_]
                                (try
                                  (pump! client-in remote-out)
                                  (catch Exception _)
                                  (finally
                                    (shutdown-output-quietly! remote))))))]
    (try
      (pump! remote-in client-out)
      (catch Exception _)
      (finally
        (shutdown-output-quietly! client)))
    (try
      (.get ^Future upstream)
      (catch Exception _))))

(defn- handle-client!
  [{:keys [resolve-url! connect-timeout-ms executor sockets]}
   ^Socket client]
  (let [request-phase? (atom false)
        connected?     (atom false)
        remote         (atom nil)]
    (try
      (.setSoTimeout client (int connect-timeout-ms))
      ;; Do not buffer the SOCKS handshake input separately from the tunnel:
      ;; a read-ahead buffer could consume early application bytes that the
      ;; subsequent tunnel streams would never see.
      (let [in  (DataInputStream. (.getInputStream client))
            out (DataOutputStream. (.getOutputStream client))]
        (negotiate-no-auth! in out)
        (reset! request-phase? true)
        (let [{:keys [host port]} (read-destination! in)
              addresses (resolve-destination! resolve-url! host port)
              remote*   (connect-pinned! sockets addresses port connect-timeout-ms)]
          (reset! remote remote*)
          (write-reply! out reply-succeeded)
          (reset! connected? true)
          (tunnel! executor client remote*)))
      (catch Exception e
        (when (and @request-phase? (not @connected?))
          (when-let [reply (:socks-reply (ex-data e))]
            (try
              (write-reply!
               (DataOutputStream. (.getOutputStream client))
               reply)
              (catch Exception _))))
        (log/debug "Playwright SOCKS connection closed"
                   {:message (.getMessage e)
                    :reply (:socks-reply (ex-data e))}))
      (finally
        (when-let [remote* @remote]
          (untrack-socket! sockets remote*)
          (close-socket-quietly! remote*))
        (untrack-socket! sockets client)
        (close-socket-quietly! client)))))

(defn- accept-loop!
  [{:keys [^ServerSocket listener running? ^ExecutorService executor
           ^Semaphore permits ^Set sockets]
    :as proxy}]
  (while @running?
    (try
      (let [client (.accept listener)]
        (if (.tryAcquire permits)
          (do
            (track-socket! sockets client)
            (try
              (.execute executor
                        ^Runnable
                        (reify Runnable
                          (run [_]
                            (try
                              (handle-client! proxy client)
                              (finally
                                (.release permits))))))
              (catch Exception e
                (.release permits)
                (untrack-socket! sockets client)
                (close-socket-quietly! client)
                (when @running?
                  (log/debug e "Playwright SOCKS worker rejected connection")))))
          (close-socket-quietly! client)))
      (catch SocketException e
        (when @running?
          (log/warn e "Playwright SOCKS listener failed")))
      (catch Exception e
        (when @running?
          (log/warn e "Playwright SOCKS accept failed"))))))

(defn start!
  "Start a loopback-only SOCKS5 proxy.

   `resolve-url!` must return the same pinned-resolution map as
   `xia.ssrf/resolve-public-url!`. The proxy owns no DNS fallback: it connects
   only to addresses in that map."
  [{:keys [resolve-url! connect-timeout-ms max-connections]
    :or   {connect-timeout-ms default-connect-timeout-ms
           max-connections default-max-connections}}]
  (when-not (fn? resolve-url!)
    (throw (ex-info "Playwright egress proxy requires resolve-url!" {})))
  (let [connect-timeout-ms (long (max 1 connect-timeout-ms))
        max-connections    (long (max 1 max-connections))
        listener           (ServerSocket.)
        running?           (atom true)
        sockets            (java.util.concurrent.ConcurrentHashMap/newKeySet)
        permits            (Semaphore. (int max-connections))
        executor           (Executors/newFixedThreadPool
                            (int (* 2 max-connections))
                            (daemon-thread-factory "xia-browser-egress-"))]
    (try
      (.setReuseAddress listener true)
      (.bind listener
             (InetSocketAddress. ^InetAddress (loopback-address) 0)
             50)
      (let [proxy-base {:listener listener
                        :running? running?
                        :executor executor
                        :permits permits
                        :sockets sockets
                        :resolve-url! resolve-url!
                        :connect-timeout-ms connect-timeout-ms
                        :port (.getLocalPort listener)}
            accept-thread (.newThread
                           ^ThreadFactory
                           (daemon-thread-factory "xia-browser-egress-accept-")
                           ^Runnable
                           (reify Runnable
                             (run [_]
                               (accept-loop! proxy-base))))]
        (.start ^Thread accept-thread)
        (assoc proxy-base :accept-thread accept-thread))
      (catch Exception e
        (reset! running? false)
        (try (.close listener) (catch Exception _))
        (.shutdownNow ^ExecutorService executor)
        (throw e)))))

(defn proxy-url
  [proxy]
  (str "socks5://127.0.0.1:" (:port proxy)))

(defn running?
  [proxy]
  (boolean (and proxy
                @(:running? proxy)
                (not (.isClosed ^ServerSocket (:listener proxy))))))

(defn stop!
  "Stop a proxy and close every accepted or outbound socket. Idempotent."
  [proxy]
  (when (and proxy (compare-and-set! (:running? proxy) true false))
    (try
      (.close ^ServerSocket (:listener proxy))
      (catch Exception _))
    (doseq [socket (seq ^Set (:sockets proxy))]
      (close-socket-quietly! socket))
    (.shutdownNow ^ExecutorService (:executor proxy))
    (try
      (.join ^Thread (:accept-thread proxy) 2000)
      (catch Exception _))
    (try
      (.awaitTermination ^ExecutorService (:executor proxy) 2000 TimeUnit/MILLISECONDS)
      (catch Exception _)))
  nil)
