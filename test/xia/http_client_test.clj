(ns xia.http-client-test
  (:require [clojure.test :refer :all]
            [xia.http-client :as http-client])
  (:import [java.net ServerSocket]
           [java.net.http HttpTimeoutException]))

(deftest request-retries-transient-failures
  (let [attempts (atom 0)
        sleeps   (atom [])]
    (with-redefs [xia.ssrf/resolve-url! (fn [url _opts]
                                          {:url url
                                           :host "api.example.com"
                                           :addresses []})
                  http-client/send-request! (fn [_]
                                              (case (int (swap! attempts inc))
                                                1 {:status 503
                                                   :headers {}
                                                   :body   "{\"error\":\"busy\"}"}
                                                2 (throw (HttpTimeoutException. "timed out"))
                                                3 {:status 200
                                                   :headers {"content-type" "application/json"}
                                                   :body   "{\"ok\":true}"}))
                  http-client/sleep-ms!     (fn [delay-ms]
                                              (swap! sleeps conj delay-ms))]
      (is (= {:status  200
              :headers {"content-type" "application/json"}
              :body    "{\"ok\":true}"
              :attempt 3}
             (http-client/request {:url           "https://api.example.com/test"
                                   :method        :get
                                   :headers       {"Content-Type" "application/json"}
                                   :body          "{}"
                                   :request-label "test request"})))
      (is (= 3 @attempts))
      (is (= [1000 2000] @sleeps)))))

(deftest request-does-not-retry-non-idempotent-methods-by-default
  (let [attempts (atom 0)
        sleeps   (atom [])]
    (with-redefs [xia.ssrf/resolve-url! (fn [url _opts]
                                          {:url url
                                           :host "api.example.com"
                                           :addresses []})
                  http-client/send-request! (fn [_]
                                              (swap! attempts inc)
                                              {:status 503
                                               :headers {}
                                               :body   "{\"error\":\"busy\"}"})
                  http-client/sleep-ms!     (fn [delay-ms]
                                              (swap! sleeps conj delay-ms))]
      (is (= {:status  503
              :headers {}
              :body    "{\"error\":\"busy\"}"
              :attempt 1}
             (http-client/request {:url           "https://api.example.com/test"
                                   :method        :post
                                   :headers       {"Content-Type" "application/json"}
                                   :body          "{}"
                                   :request-label "test request"})))
      (is (= 1 @attempts))
      (is (empty? @sleeps)))))

(deftest request-can-override-method-based-retry-gating
  (let [attempts (atom 0)
        sleeps   (atom [])]
    (with-redefs [xia.ssrf/resolve-url! (fn [url _opts]
                                          {:url url
                                           :host "api.example.com"
                                           :addresses []})
                  http-client/send-request! (fn [_]
                                              (case (int (swap! attempts inc))
                                                1 {:status 503
                                                   :headers {}
                                                   :body   "{\"error\":\"busy\"}"}
                                                2 {:status 200
                                                   :headers {"content-type" "application/json"}
                                                   :body   "{\"ok\":true}"}))
                  http-client/sleep-ms!     (fn [delay-ms]
                                              (swap! sleeps conj delay-ms))]
      (is (= {:status  200
              :headers {"content-type" "application/json"}
              :body    "{\"ok\":true}"
              :attempt 2}
             (http-client/request {:url            "https://api.example.com/test"
                                   :method         :post
                                   :headers        {"Content-Type" "application/json"}
                                   :body           "{}"
                                   :retry-enabled? true
                                   :request-label  "test request"})))
      (is (= 2 @attempts))
      (is (= [1000] @sleeps)))))

(deftest request-does-not-retry-permanent-status
  (let [attempts (atom 0)
        sleeps   (atom [])]
    (with-redefs [xia.ssrf/resolve-url! (fn [url _opts]
                                          {:url url
                                           :host "api.example.com"
                                           :addresses []})
                  http-client/send-request! (fn [_]
                                              (swap! attempts inc)
                                              {:status 400
                                               :headers {"content-type" "application/json"}
                                               :body   "{\"error\":\"bad_request\"}"})
                  http-client/sleep-ms!     (fn [delay-ms]
                                              (swap! sleeps conj delay-ms))]
      (is (= {:status  400
              :headers {"content-type" "application/json"}
              :body    "{\"error\":\"bad_request\"}"
              :attempt 1}
             (http-client/request {:url           "https://api.example.com/test"
                                   :method        :post
                                   :headers       {"Content-Type" "application/json"}
                                   :body          "{}"
                                   :request-label "test request"})))
      (is (= 1 @attempts))
      (is (empty? @sleeps)))))

(deftest request-validates-ssrf-targets-by-default
  (let [validated (atom nil)]
    (with-redefs [xia.ssrf/resolve-url! (fn [url opts]
                                          (reset! validated {:url url :opts opts})
                                          {:url url
                                           :host "api.example.com"
                                           :addresses []})
                  http-client/send-request! (fn [_]
                                              {:status 200
                                               :headers {}
                                               :body ""})]
      (is (= 200
             (:status (http-client/request {:url "https://api.example.com/test"}))))
      (is (= {:url "https://api.example.com/test"
              :opts {:allow-private-network? false}}
             @validated)))))

(deftest request-can-explicitly-allow-private-network-targets
  (let [validated (atom nil)]
    (with-redefs [xia.ssrf/resolve-url! (fn [url opts]
                                          (reset! validated {:url url :opts opts})
                                          {:url url
                                           :host "127.0.0.1"
                                           :addresses []})
                  http-client/send-request! (fn [_]
                                              {:status 200
                                               :headers {}
                                               :body ""})]
      (is (= 200
             (:status (http-client/request {:url "http://127.0.0.1/test"
                                            :allow-private-network? true}))))
      (is (= {:url "http://127.0.0.1/test"
              :opts {:allow-private-network? true}}
             @validated)))))

(deftest request-times-out-on-stalled-response-body
  (let [server (ServerSocket. 0)
        port   (.getLocalPort server)
        worker (future
                 (with-open [server server
                             socket (.accept server)
                             out    (.getOutputStream socket)]
                   (.write out (.getBytes (str "HTTP/1.1 200 OK\r\n"
                                               "Content-Type: application/json\r\n"
                                               "Content-Length: 1000\r\n"
                                               "\r\n"
                                               "{\"choices\": [")))
                   (.flush out)
                   (Thread/sleep 1000)))]
    (let [t0 (System/currentTimeMillis)
          ex (try
               (http-client/request {:url           (str "http://127.0.0.1:" port "/chat/completions")
                                     :method        :post
                                     :headers       {"Content-Type" "application/json"}
                                     :body          "{}"
                                     :allow-private-network? true
                                     :timeout       200
                                     :max-attempts  1
                                     :request-label "timeout test"})
               nil
               (catch clojure.lang.ExceptionInfo e
                 e))]
      (is (some? ex))
      (is (= 200 (:timeout-ms (ex-data ex))))
      (is (re-find #"timed out" (.getMessage ^Throwable ex)))
      (is (< (- (System/currentTimeMillis) t0) 800)))
    (is (not= ::timeout
              (deref worker 2000 ::timeout)))))
