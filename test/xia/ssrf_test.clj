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
                                 "https://service.example/resource")))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"private/internal"
       (ssrf/resolve-public-url! (fn [_] (addresses "100.64.0.1"))
                                 "https://service.example/resource")))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"private/internal"
       (ssrf/resolve-public-url! (fn [_] (addresses "100.127.255.254"))
                                 "https://service.example/resource")))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"private/internal"
       (ssrf/resolve-public-url! (fn [_] (addresses "fc00::1"))
                                 "https://service.example/resource")))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"private/internal"
       (ssrf/resolve-public-url! (fn [_] (addresses "fdff:ffff::1"))
                                 "https://service.example/resource"))))

(deftest public-url-resolution-rejects-mixed-public-and-private-results
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"private/internal"
       (ssrf/resolve-public-url! (fn [_] (addresses "93.184.216.34" "127.0.0.1"))
                                 "https://mixed.example/resource"))))

(deftest private-addresses-require-explicit-opt-in
  (is (= {:host "service.example"
          :private-network? true}
         (select-keys
          (ssrf/resolve-url! (fn [_] (addresses "10.0.0.5"))
                             "https://service.example/resource"
                             {:allow-private-network? true})
          [:host :private-network?]))))

(deftest public-url-resolution-allows-addresses-near-cgnat-and-ula-boundaries
  (is (false?
       (:private-network?
        (ssrf/resolve-public-url! (fn [_] (addresses "100.63.255.255"))
                                  "https://service.example/resource"))))
  (is (false?
       (:private-network?
        (ssrf/resolve-public-url! (fn [_] (addresses "100.128.0.0"))
                                  "https://service.example/resource"))))
  (is (false?
       (:private-network?
        (ssrf/resolve-public-url! (fn [_] (addresses "fbff:ffff::1"))
                                  "https://service.example/resource")))))

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
