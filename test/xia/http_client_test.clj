(ns xia.http-client-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [xia.http-client :as http-client]
            [xia.ssrf :as ssrf])
  (:import [java.io BufferedReader InputStreamReader OutputStream]
           [java.net InetAddress ServerSocket Socket SocketException URI]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files]))

(defn- temp-target
  []
  (let [dir (Files/createTempDirectory "xia-http-client-test"
                                       (make-array java.nio.file.attribute.FileAttribute 0))]
    (str (.resolve dir "asset.bin"))))

(defn- read-request-head!
  [socket]
  (let [reader (BufferedReader.
                 (InputStreamReader. (.getInputStream socket)
                                     StandardCharsets/US_ASCII))]
    (loop [lines []]
      (let [line (.readLine reader)]
        (if (and line (not= "" line))
          (recur (conj lines line))
          lines)))))

(defn- write-response!
  [socket response]
  (let [out (.getOutputStream socket)]
    (.write out (.getBytes response StandardCharsets/US_ASCII))
    (.flush out)))

(defn- thrown-ex-info
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      e)))

(defn- local-pinned-resolution
  [host]
  (fn [url _opts]
    {:url url
     :uri (URI. url)
     :host host
     :addresses [(InetAddress/getByName "127.0.0.1")]
     :private-network? true}))

(defn- request-raw-response
  [response request-opts]
  (let [server (ServerSocket. 0)
        port   (.getLocalPort server)
        worker (future
                 (try
                   (with-open [^ServerSocket server server
                               ^Socket socket (.accept server)]
                     (read-request-head! socket)
                     (write-response! socket response))
                   (catch SocketException _
                     :closed)))]
    (try
      (with-redefs [xia.ssrf/resolve-url! (local-pinned-resolution "bounded.example")]
        (http-client/request
          (merge {:url (str "http://bounded.example:" port "/resource")
                  :allow-private-network? true
                  :max-attempts 1
                  :timeout 1000}
                 request-opts)))
      (finally
        (.close server)
        (deref worker 2000 :timeout)))))

(deftest download-follows-redirects-and-stops-at-content-length
  (let [server        (ServerSocket. 0)
        release-final (promise)
        target        (temp-target)
        url           (str "http://127.0.0.1:" (.getLocalPort server) "/start")
        server-result (future
                        (try
                          (with-open [socket (.accept server)]
                            (read-request-head! socket)
                            (write-response! socket
                                             (str "HTTP/1.1 302 Found\r\n"
                                                  "Location: /asset\r\n"
                                                  "Content-Length: 0\r\n"
                                                  "Connection: close\r\n"
                                                  "\r\n")))
                          (with-open [socket (.accept server)]
                            (read-request-head! socket)
                            (write-response! socket
                                             (str "HTTP/1.1 200 OK\r\n"
                                                  "Content-Length: 5\r\n"
                                                  "Connection: keep-alive\r\n"
                                                  "\r\n"
                                                  "asset"))
                            (deref release-final 2000 :timeout)
                            :done)
                          (catch SocketException _
                            :closed)))]
    (try
      (let [resp (http-client/download! {:url url
                                         :target-path target
                                         :trusted true
                                         :connect-timeout 1000
                                         :timeout 1000})]
        (is (= {:status 200
                :target-path target}
               (select-keys resp [:status :target-path])))
        (is (= "asset" (slurp target)))
        (deliver release-final true)
        (is (= :done (deref server-result 2000 :timeout))))
      (finally
        (deliver release-final true)
        (.close server)))))

(deftest request-does-not-follow-redirects-by-default
  (let [server        (ServerSocket. 0)
        url           (str "http://127.0.0.1:" (.getLocalPort server) "/start")
        server-result (future
                        (try
                          (with-open [socket (.accept server)]
                            (read-request-head! socket)
                            (write-response! socket
                                             (str "HTTP/1.1 302 Found\r\n"
                                                  "Location: /final\r\n"
                                                  "Content-Length: 8\r\n"
                                                  "Connection: close\r\n"
                                                  "\r\n"
                                                  "redirect"))
                            :done)
                          (catch SocketException _
                            :closed)))]
    (try
      (let [resp (http-client/request {:url url
                                       :trusted true
                                       :connect-timeout 1000
                                       :timeout 1000})]
        (is (= 302 (:status resp)))
        (is (= "redirect" (:body resp)))
        (is (= :done (deref server-result 2000 :timeout))))
      (finally
        (.close server)))))

(deftest request-follows-redirects-when-enabled
  (let [redirect-server (ServerSocket. 0)
        final-server    (ServerSocket. 0)
        final-url       (str "http://127.0.0.1:" (.getLocalPort final-server) "/final")
        start-url       (str "http://127.0.0.1:" (.getLocalPort redirect-server) "/start")
        final-head      (promise)
        server-result   (future
                          (try
                            (with-open [socket (.accept redirect-server)]
                              (read-request-head! socket)
                              (write-response! socket
                                               (str "HTTP/1.1 303 See Other\r\n"
                                                    "Location: " final-url "\r\n"
                                                    "Content-Length: 0\r\n"
                                                    "Connection: close\r\n"
                                                    "\r\n")))
                            (with-open [socket (.accept final-server)]
                              (deliver final-head (read-request-head! socket))
                              (write-response! socket
                                               (str "HTTP/1.1 200 OK\r\n"
                                                    "Content-Length: 2\r\n"
                                                    "Connection: close\r\n"
                                                    "\r\n"
                                                    "ok"))
                              :done)
                            (catch SocketException _
                              :closed)))]
    (try
      (let [resp (http-client/request {:url start-url
                                       :method :post
                                       :body "payload"
                                       :headers {"Authorization" "Bearer secret"
                                                 "Content-Type" "text/plain"}
                                       :trusted true
                                       :follow-redirects? true
                                       :connect-timeout 1000
                                       :timeout 1000})
            head (deref final-head 2000 :timeout)]
        (is (= {:status 200
                :body "ok"}
               (select-keys resp [:status :body])))
        (is (= "GET /final HTTP/1.1" (first head)))
        (is (not-any? #(str/starts-with? % "Authorization:") head))
        (is (not-any? #(str/starts-with? % "Content-Type:") head))
        (is (= :done (deref server-result 2000 :timeout))))
      (finally
        (.close redirect-server)
        (.close final-server)))))

(deftest response-rejects-oversized-content-length-before-reading-body
  (let [ex (thrown-ex-info
             #(request-raw-response
                (str "HTTP/1.1 200 OK\r\n"
                     "Content-Length: 1000\r\n"
                     "Connection: close\r\n\r\n")
                {:max-response-bytes 32}))]
    (is (= :http/limit-exceeded (:type (ex-data ex))))
    (is (= :response-bytes (:limit (ex-data ex))))
    (is (= 32 (:max (ex-data ex))))
    (is (= 1000 (:actual (ex-data ex))))))

(deftest response-rejects-unbounded-chunked-body
  (let [server (ServerSocket. 0)
        port   (.getLocalPort server)
        worker (future
                 (try
                   (with-open [^ServerSocket server server
                               ^Socket socket (.accept server)
                               ^OutputStream out (.getOutputStream socket)]
                     (read-request-head! socket)
                     (.write out (.getBytes (str "HTTP/1.1 200 OK\r\n"
                                                 "Transfer-Encoding: chunked\r\n\r\n")
                                            StandardCharsets/US_ASCII))
                     (dotimes [_ 1000]
                       (.write out (.getBytes "1\r\nx\r\n" StandardCharsets/US_ASCII))
                       (.flush out)))
                   (catch Exception _
                     :client-closed)))
        ex (try
             (with-redefs [xia.ssrf/resolve-url! (local-pinned-resolution "chunks.example")]
               (thrown-ex-info
                 #(http-client/request {:url (str "http://chunks.example:" port "/stream")
                                        :allow-private-network? true
                                        :max-response-bytes 32
                                        :max-attempts 1
                                        :timeout 1000})))
             (finally
               (.close server)))]
    (is (= :http/limit-exceeded (:type (ex-data ex))))
    (is (= :response-bytes (:limit (ex-data ex))))
    (is (= 33 (:actual (ex-data ex))))
    (is (not= :timeout (deref worker 2000 :timeout)))))

(deftest response-enforces-header-count-line-and-total-byte-limits
  (doseq [[limit opts response]
          [[:header-count
            {:max-response-header-count 2}
            (str "HTTP/1.1 200 OK\r\n"
                 "X-One: 1\r\nX-Two: 2\r\nContent-Length: 0\r\n\r\n")]
           [:header-line-bytes
            {:max-response-header-line-bytes 24}
            (str "HTTP/1.1 200 OK\r\n"
                 "X-Long: 123456789012345678901234567890\r\n\r\n")]
           [:header-bytes
            {:max-response-header-bytes 32}
            (str "HTTP/1.1 200 OK\r\n"
                 "Content-Length: 0\r\n\r\n")]]]
    (let [ex (thrown-ex-info #(request-raw-response response opts))]
      (is (= :http/limit-exceeded (:type (ex-data ex))) (name limit))
      (is (= limit (:limit (ex-data ex))) (name limit)))))

(deftest timeout-is-an-absolute-deadline-for-slow-trickle-bodies
  (let [server (ServerSocket. 0)
        port   (.getLocalPort server)
        worker (future
                 (try
                   (with-open [^ServerSocket server server
                               ^Socket socket (.accept server)
                               ^OutputStream out (.getOutputStream socket)]
                     (read-request-head! socket)
                     (.write out (.getBytes "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n"
                                            StandardCharsets/US_ASCII))
                     (.flush out)
                     (dotimes [_ 30]
                       (Thread/sleep 40)
                       (.write out (int \x))
                       (.flush out)))
                   (catch Exception _
                     :client-closed)))
        started (System/currentTimeMillis)
        ex (try
             (with-redefs [xia.ssrf/resolve-url! (local-pinned-resolution "slow.example")]
               (thrown-ex-info
                 #(http-client/request {:url (str "http://slow.example:" port "/slow")
                                        :allow-private-network? true
                                        :timeout 180
                                        :max-attempts 1})))
             (finally
               (.close server)))
        elapsed (- (System/currentTimeMillis) started)]
    (is (= :http/deadline-exceeded (:type (ex-data ex))))
    (is (= :response-body (:phase (ex-data ex))))
    (is (< elapsed 700) (str "elapsed=" elapsed))
    (is (not= :timeout (deref worker 2000 :timeout)))))

(deftest timeout-bounds-dns-resolution
  (let [started (System/currentTimeMillis)
        ex (with-redefs [ssrf/resolve-url! (fn [& _]
                                            (Thread/sleep 1000)
                                            (throw (AssertionError. "late DNS result")))]
             (thrown-ex-info
               #(http-client/request {:url "https://slow-dns.example/resource"
                                      :timeout 75
                                      :max-attempts 1})))
        elapsed (- (System/currentTimeMillis) started)]
    (is (= :http/deadline-exceeded (:type (ex-data ex))))
    (is (= :dns (:phase (ex-data ex))))
    (is (< elapsed 500) (str "elapsed=" elapsed))))

(deftest retry-backoff-cannot-exceed-the-original-deadline
  (let [attempts (atom 0)
        started  (System/currentTimeMillis)
        ex (with-redefs [ssrf/resolve-url! (fn [url _]
                                            {:url url
                                             :uri (URI. url)
                                             :host "retry.example"
                                             :addresses []})
                         http-client/send-request! (fn [_]
                                                     (swap! attempts inc)
                                                     {:status 503 :headers {} :body "busy"})]
             (thrown-ex-info
               #(http-client/request {:url "https://retry.example/resource"
                                      :timeout 100
                                      :max-attempts 5
                                      :initial-backoff-ms 1000})))
        elapsed (- (System/currentTimeMillis) started)]
    (is (= 1 @attempts))
    (is (= :http/deadline-exceeded (:type (ex-data ex))))
    (is (= :retry-backoff (:phase (ex-data ex))))
    (is (< elapsed 500) (str "elapsed=" elapsed))))

(deftest redirect-targets-are-revalidated-before-connection
  (let [server (ServerSocket. 0)
        port   (.getLocalPort server)
        calls  (atom [])
        worker (future
                 (with-open [^ServerSocket server server
                             ^Socket socket (.accept server)]
                   (read-request-head! socket)
                   (write-response! socket
                                    (str "HTTP/1.1 302 Found\r\n"
                                         "Location: http://127.0.0.1:" port "/private\r\n"
                                         "Content-Length: 0\r\n\r\n"))))]
    (try
      (let [ex (with-redefs [ssrf/resolve-url!
                             (fn [url opts]
                               (swap! calls conj url)
                               (if (str/includes? url "public.example")
                                 {:url url
                                  :uri (URI. url)
                                  :host "public.example"
                                  :addresses [(InetAddress/getByName "127.0.0.1")]
                                  :private-network? false}
                                 (throw (ex-info "Access to private/internal network addresses is blocked"
                                                 {:url url :opts opts}))))]
                 (thrown-ex-info
                   #(http-client/request {:url (str "http://public.example:" port "/start")
                                          :follow-redirects? true
                                          :max-attempts 1
                                          :timeout 1000})))]
        (is (re-find #"private/internal" (.getMessage ^Throwable ex)))
        (is (= 2 (count @calls)))
        (is (str/includes? (second @calls) "127.0.0.1")))
      (finally
        (.close server)
        (deref worker 2000 :timeout)))))

(deftest download-limit-leaves-existing-target-untouched
  (let [server (ServerSocket. 0)
        port   (.getLocalPort server)
        target (temp-target)
        _      (spit target "existing")
        worker (future
                 (try
                   (with-open [^ServerSocket server server
                               ^Socket socket (.accept server)]
                     (read-request-head! socket)
                     (write-response! socket
                                      (str "HTTP/1.1 200 OK\r\n"
                                           "Content-Length: 100\r\n\r\n")))
                   (catch SocketException _ :closed)))]
    (try
      (let [ex (thrown-ex-info
                 #(http-client/download! {:url (str "http://127.0.0.1:" port "/large")
                                          :target-path target
                                          :trusted true
                                          :max-download-bytes 16
                                          :timeout 1000}))]
        (is (= :http/limit-exceeded (:type (ex-data ex))))
        (is (= :response-bytes (:limit (ex-data ex))))
        (is (= "existing" (slurp target))))
      (finally
        (.close server)
        (deref worker 2000 :timeout)))))
