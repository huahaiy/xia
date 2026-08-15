(ns xia.browser-egress-proxy-test
  (:require [clojure.test :refer [deftest is testing]]
            [xia.browser.egress-proxy :as egress-proxy]
            [xia.browser.playwright :as playwright]
            [xia.runtime-context :as runtime-context]
            [xia.ssrf :as ssrf])
  (:import [com.microsoft.playwright Browser Browser$NewContextOptions
            BrowserContext BrowserType$LaunchOptions Page Playwright]
           [com.microsoft.playwright.options Proxy]
           [java.io BufferedReader DataInputStream DataOutputStream File
            InputStreamReader OutputStream]
           [java.net ConnectException InetAddress InetSocketAddress ServerSocket
            Socket SocketTimeoutException]
           [java.nio.charset StandardCharsets]))

(defn- loopback-address
  []
  (InetAddress/getByAddress
   (byte-array [(byte 127) (byte 0) (byte 0) (byte 1)])))

(defn- ipv6-loopback-address
  []
  (InetAddress/getByAddress
   (byte-array [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1])))

(defn- loopback-socket-address
  [port]
  (InetSocketAddress. ^InetAddress (loopback-address) (int port)))

(defn- start-echo-server
  []
  (let [server (ServerSocket. 0 50 (loopback-address))
        worker (future
                 (with-open [^ServerSocket server server
                             ^Socket client (.accept server)]
                   (let [in      (DataInputStream. (.getInputStream client))
                         out     (DataOutputStream. (.getOutputStream client))
                         payload (byte-array 4)]
                     (.readFully in payload)
                     (.write out ^bytes payload 0 (alength ^bytes payload))
                     (.flush out))))]
    {:server server
     :port (.getLocalPort server)
     :worker worker}))

(defn- open-socks-request
  [proxy host port]
  (let [socket (Socket.)]
    (try
      (.connect socket
                (loopback-socket-address (:port proxy))
                1000)
      (.setSoTimeout socket 2000)
      (let [in         (DataInputStream. (.getInputStream socket))
            out        (DataOutputStream. (.getOutputStream socket))
            ^bytes host-bytes (.getBytes ^String host StandardCharsets/US_ASCII)]
        (.writeByte out 5)
        (.writeByte out 1)
        (.writeByte out 0)
        (.flush out)
        (when-not (= [5 0] [(.readUnsignedByte in) (.readUnsignedByte in)])
          (throw (ex-info "SOCKS authentication negotiation failed" {})))
        (.writeByte out 5)
        (.writeByte out 1)
        (.writeByte out 0)
        (.writeByte out 3)
        (.writeByte out (alength host-bytes))
        (.write out host-bytes 0 (alength host-bytes))
        (.writeShort out (int port))
        (.flush out)
        (let [reply-bytes (byte-array 10)]
          (.readFully in reply-bytes)
          {:socket socket
           :in in
           :out out
           :reply (bit-and 0xff (int (aget ^bytes reply-bytes 1)))}))
      (catch Exception e
        (.close socket)
        (throw e)))))

(defn- stop-server!
  [{:keys [server worker]}]
  (try
    (.close ^ServerSocket server)
    (catch Exception _))
  (when worker
    (try
      (deref worker 2000 :timeout)
      (catch Exception _))))

(defn- start-http-server
  []
  (let [server   (ServerSocket. 0 50 (loopback-address))
        running? (atom true)
        requests (atom [])
        worker   (future
                   (while @running?
                     (try
                       (with-open [^Socket client (.accept server)]
                         (let [reader (BufferedReader.
                                       (InputStreamReader.
                                        (.getInputStream client)
                                        StandardCharsets/US_ASCII))
                               request-line (.readLine reader)]
                           (loop []
                             (when-let [line (.readLine reader)]
                               (when-not (empty? line)
                                 (recur))))
                           (swap! requests conj request-line)
                           (let [body "proxied"
                                 response (str "HTTP/1.1 200 OK\r\n"
                                               "Content-Type: text/plain\r\n"
                                               "Content-Length: " (count body) "\r\n"
                                               "Connection: close\r\n\r\n"
                                               body)
                                 payload (.getBytes response StandardCharsets/US_ASCII)
                                 ^OutputStream out (.getOutputStream client)]
                             (.write out ^bytes payload 0 (alength ^bytes payload))
                             (.flush out))))
                       (catch Exception _))))]
    {:server server
     :running? running?
     :port (.getLocalPort server)
     :requests requests
     :worker worker}))

(defn- stop-http-server!
  [{:keys [server running? worker]}]
  (reset! running? false)
  (try
    (.close ^ServerSocket server)
    (catch Exception _))
  (try
    (deref worker 2000 :timeout)
    (catch Exception _)))

(deftest proxy-resolves-once-and-tunnels-through-the-pinned-address
  (let [{:keys [port] :as echo} (start-echo-server)
        calls (atom [])
        proxy (egress-proxy/start!
               {:resolve-url!
                (fn [url]
                  (swap! calls conj url)
                  {:addresses [(if (= 1 (count @calls))
                                 (loopback-address)
                                 (InetAddress/getByName "169.254.169.254"))]})
                :connect-timeout-ms 1000
                :max-connections 4})]
    (try
      (let [{:keys [socket in out reply]}
            (open-socks-request proxy "changes-after-validation.invalid" port)]
        (try
          (is (zero? reply))
          (let [payload (.getBytes "ping" StandardCharsets/US_ASCII)
                echoed  (byte-array 4)]
            (.write ^DataOutputStream out ^bytes payload 0 (alength ^bytes payload))
            (.flush ^DataOutputStream out)
            (.readFully ^DataInputStream in echoed)
            (is (= "ping" (String. ^bytes echoed StandardCharsets/US_ASCII))))
          (is (= [(str "http://changes-after-validation.invalid:" port "/")]
                 @calls))
          (finally
            (.close ^Socket socket))))
      (finally
        (egress-proxy/stop! proxy)
        (stop-server! echo)))))

(deftest proxy-falls-back-only-within-the-pinned-address-set
  (let [{:keys [port] :as echo} (start-echo-server)
        calls (atom 0)
        proxy (egress-proxy/start!
               {:resolve-url!
                (fn [_]
                  (swap! calls inc)
                  {:addresses [(ipv6-loopback-address) (loopback-address)]})
                :connect-timeout-ms 1000
                :max-connections 4})]
    (try
      (let [{:keys [socket in out reply]}
            (open-socks-request proxy "fallback.invalid" port)]
        (try
          (is (zero? reply))
          (let [payload (.getBytes "next" StandardCharsets/US_ASCII)
                echoed  (byte-array 4)]
            (.write ^DataOutputStream out ^bytes payload 0 (alength ^bytes payload))
            (.flush ^DataOutputStream out)
            (.readFully ^DataInputStream in echoed)
            (is (= "next" (String. ^bytes echoed StandardCharsets/US_ASCII))))
          (is (= 1 @calls))
          (finally
            (.close ^Socket socket))))
      (finally
        (egress-proxy/stop! proxy)
        (stop-server! echo)))))

(deftest production-policy-blocks-private-destinations-before-connect
  (let [target (ServerSocket. 0 50 (loopback-address))
        calls  (atom [])
        proxy  (egress-proxy/start!
                {:resolve-url!
                 (fn [url]
                   (swap! calls conj url)
                   (ssrf/resolve-public-url! (constantly [(loopback-address)]) url))
                 :connect-timeout-ms 1000
                 :max-connections 4})]
    (try
      (let [{:keys [socket reply]}
            (open-socks-request proxy "private-after-dns.invalid"
                                (.getLocalPort target))]
        (try
          (is (= 2 reply))
          (is (= 1 (count @calls)))
          (finally
            (.close ^Socket socket))))
      (.setSoTimeout target 250)
      (is (thrown? SocketTimeoutException (.accept target)))
      (finally
        (egress-proxy/stop! proxy)
        (.close target)))))

(deftest playwright-context-forces-all-browser-requests-through-the-proxy
  (let [^Browser$NewContextOptions opts
        (#'playwright/context-options false
                                      "{\"cookies\":[],\"origins\":[]}"
                                      "socks5://127.0.0.1:32123")
        ^Proxy proxy (.-proxy opts)]
    (testing "SOCKS is authoritative even for Chromium's implicit local ranges"
      (is (= "socks5://127.0.0.1:32123" (.-server proxy)))
      (is (= "<-loopback>" (.-bypass proxy))))
    (is (false? (.-javaScriptEnabled opts)))
    (is (= "{\"cookies\":[],\"origins\":[]}" (.-storageState opts)))
    (is (= #{"--disable-quic"
             "--force-webrtc-ip-handling-policy=disable_non_proxied_udp"}
           (set (var-get #'playwright/chromium-egress-args))))))

(deftest chromium-sends-hostnames-and-loopback-through-the-pinned-proxy
  (let [{:keys [port] :as http-server} (start-http-server)
        resolved-urls (atom [])
        runtime-context (runtime-context/make
                         {:xia/browser-runtime
                          {:runtime (playwright/make-runtime)}})
        proxy (egress-proxy/start!
               {:resolve-url!
                (fn [url]
                  (swap! resolved-urls conj url)
                  {:addresses [(loopback-address)]})
                :connect-timeout-ms 2000
                :max-connections 8})]
    (try
      (runtime-context/with-runtime-context
        runtime-context
        (fn []
          (try
            (with-open [^Playwright playwright (#'playwright/create-playwright)]
              (let [^String executable (.executablePath (.chromium playwright))]
                (if-not (.exists (File. executable))
                  (is true "Playwright Chromium is not installed; proxy protocol tests still run")
                  (with-open [^Browser browser
                              (.launch (.chromium playwright)
                                       (doto (BrowserType$LaunchOptions.)
                                         (.setHeadless true)
                                         (.setArgs
                                          ^java.util.List
                                          (vec (var-get #'playwright/chromium-egress-args)))))
                              ^BrowserContext context
                              (.newContext browser
                                           (#'playwright/context-options
                                            true nil (egress-proxy/proxy-url proxy)))]
                    (let [^Page page (.newPage context)
                          invalid-url (str "http://dns-must-stay-in-proxy.invalid:" port "/first")
                          loopback-url (str "http://127.0.0.1:" port "/loopback")]
                      (.navigate page invalid-url)
                      (is (= "proxied" (.textContent page "body")))
                      (.navigate page loopback-url)
                      (is (= "proxied" (.textContent page "body")))
                      (is (some #(= % (str "http://dns-must-stay-in-proxy.invalid:"
                                           port "/"))
                                @resolved-urls))
                      (is (some #(= % (str "http://127.0.0.1:" port "/"))
                                @resolved-urls)))))))
            (finally
              (playwright/clear-runtime!)))))
      (finally
        (egress-proxy/stop! proxy)
        (stop-http-server! http-server)))))

(deftest stopping-the-proxy-closes-the-listener
  (let [proxy (egress-proxy/start!
                {:resolve-url! (constantly {:addresses [(loopback-address)]})
                 :max-connections 1})
        port  (:port proxy)]
    (is (egress-proxy/running? proxy))
    (egress-proxy/stop! proxy)
    (egress-proxy/stop! proxy)
    (is (false? (egress-proxy/running? proxy)))
    (with-open [socket (Socket.)]
      (is (thrown? ConnectException
                   (.connect socket
                             (loopback-socket-address port)
                             250))))))
