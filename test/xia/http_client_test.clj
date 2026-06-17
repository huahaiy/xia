(ns xia.http-client-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [xia.http-client :as http-client])
  (:import [java.io BufferedReader InputStreamReader]
           [java.net ServerSocket SocketException]
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
