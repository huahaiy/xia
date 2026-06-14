(ns xia.ssrf-test
  (:require [clojure.test :refer [deftest is]]
            [xia.ssrf :as ssrf])
  (:import [java.net InetAddress]))

(defn- addresses
  [& values]
  (mapv #(InetAddress/getByName ^String %) values))

(deftest public-url-resolution-blocks-private-addresses
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"private/internal"
       (ssrf/resolve-public-url! (fn [_] (addresses "127.0.0.1"))
                                 "http://example.test/resource")))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"private/internal"
       (ssrf/resolve-public-url! (fn [_] (addresses "10.0.0.5"))
                                 "https://service.example/resource"))))

(deftest private-addresses-require-explicit-opt-in
  (is (= {:host "service.example"
          :private-network? true}
         (select-keys
          (ssrf/resolve-url! (fn [_] (addresses "10.0.0.5"))
                             "https://service.example/resource"
                             {:allow-private-network? true})
          [:host :private-network?]))))

(deftest public-url-resolution-rejects-unsupported-schemes-and-missing-hosts
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Only http:// and https:// URLs are allowed"
       (ssrf/resolve-public-url! (fn [_] (addresses "203.0.113.10"))
                                 "file:///etc/passwd")))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"URL has no host"
       (ssrf/resolve-public-url! (fn [_] (addresses "203.0.113.10"))
                                 "https:///missing-host"))))
