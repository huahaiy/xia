(ns xia.http-client-test
  (:require [clojure.test :refer [deftest is]]
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
    (loop []
      (let [line (.readLine reader)]
        (when (and line (not= "" line))
          (recur))))))

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
