(ns xia.ssrf-test
  (:require [clojure.test :refer [deftest is testing]]
            [xia.ssrf :as ssrf])
  (:import [java.net InetAddress Inet6Address]))

(defn- addresses
  [& values]
  (mapv #(InetAddress/getByName ^String %) values))

(defn- raw-ipv4-mapped
  [a b c d]
  (Inet6Address/getByAddress
   nil
   (byte-array (map #(unchecked-byte (int %))
                    [0 0 0 0 0 0 0 0 0 0 255 255 a b c d]))
   -1))

(def ^:private blocked-address-cases
  [["IPv4 this-network start" "0.0.0.0"]
   ["IPv4 this-network end" "0.255.255.255"]
   ["RFC1918 10/8 start" "10.0.0.0"]
   ["RFC1918 10/8 end" "10.255.255.255"]
   ["CGNAT start" "100.64.0.0"]
   ["CGNAT end" "100.127.255.255"]
   ["IPv4 loopback start" "127.0.0.0"]
   ["IPv4 loopback end" "127.255.255.255"]
   ["IPv4 link-local" "169.254.10.20"]
   ["RFC1918 172.16/12 start" "172.16.0.0"]
   ["RFC1918 172.16/12 end" "172.31.255.255"]
   ["IETF protocol assignments" "192.0.0.8"]
   ["NAT64 discovery" "192.0.0.170"]
   ["TEST-NET-1" "192.0.2.1"]
   ["deprecated 6to4 relay anycast" "192.88.99.2"]
   ["RFC1918 192.168/16" "192.168.255.255"]
   ["benchmarking start" "198.18.0.0"]
   ["benchmarking end" "198.19.255.255"]
   ["TEST-NET-2" "198.51.100.1"]
   ["TEST-NET-3" "203.0.113.1"]
   ["IPv4 local multicast" "224.0.0.1"]
   ["IPv4 administratively scoped multicast" "239.255.255.255"]
   ["IPv4 reserved start" "240.0.0.0"]
   ["IPv4 limited broadcast" "255.255.255.255"]
   ["IPv6 unspecified" "::"]
   ["IPv6 loopback" "::1"]
   ["deprecated IPv4-compatible IPv6" "::127.0.0.1"]
   ["local-use NAT64 prefix" "64:ff9b:1::1"]
   ["discard-only IPv6" "100::1"]
   ["IPv6 dummy prefix" "100:0:0:1::1"]
   ["Teredo" "2001::1"]
   ["IPv6 benchmarking" "2001:2::1"]
   ["IPv6 documentation" "2001:db8::1"]
   ["6to4" "2002::1"]
   ["IPv6 documentation 3fff/20" "3fff::1"]
   ["SRv6 SID prefix" "5f00::1"]
   ["IPv6 unique-local start" "fc00::1"]
   ["IPv6 unique-local end" "fdff:ffff::1"]
   ["deprecated IPv6 site-local" "fec0::1"]
   ["IPv6 link-local" "fe80::1"]
   ["IPv6 interface-local multicast" "ff01::1"]
   ["IPv6 global multicast" "ff0e::1"]
   ["below allocated IPv6 global-unicast space" "1fff:ffff::1"]
   ["above allocated IPv6 global-unicast space" "4000::1"]
   ["IPv6 future-use reserved space" "fbff:ffff::1"]
   ["NAT64 embedding loopback" "64:ff9b::127.0.0.1"]
   ["NAT64 embedding RFC1918" "64:ff9b::192.168.1.1"]
   ["raw IPv4-mapped loopback" [:ipv4-mapped 127 0 0 1]]
   ["raw IPv4-mapped RFC1918" [:ipv4-mapped 10 0 0 1]]])

(def ^:private public-address-cases
  [["ordinary public IPv4" "93.184.216.34"]
   ["below RFC1918 10/8" "9.255.255.255"]
   ["above RFC1918 10/8" "11.0.0.0"]
   ["below CGNAT" "100.63.255.255"]
   ["above CGNAT" "100.128.0.0"]
   ["below IPv4 loopback" "126.255.255.255"]
   ["above IPv4 loopback" "128.0.0.0"]
   ["below IPv4 link-local" "169.253.255.255"]
   ["above IPv4 link-local" "169.255.0.0"]
   ["below RFC1918 172.16/12" "172.15.255.255"]
   ["above RFC1918 172.16/12" "172.32.0.0"]
   ["PCP anycast exception" "192.0.0.9"]
   ["TURN anycast exception" "192.0.0.10"]
   ["above IETF protocol assignments" "192.0.1.0"]
   ["below benchmarking" "198.17.255.255"]
   ["above benchmarking" "198.20.0.0"]
   ["below IPv4 multicast" "223.255.255.255"]
   ["ordinary public IPv6" "2001:4860:4860::8888"]
   ["top of allocated IPv6 global-unicast space" "3fff:ffff::1"]
   ["PCP IPv6 anycast exception" "2001:1::1"]
   ["TURN IPv6 anycast exception" "2001:1::2"]
   ["DNS-SD IPv6 anycast exception" "2001:1::3"]
   ["AMT IPv6 exception" "2001:3::1"]
   ["AS112 IPv6 exception" "2001:4:112::1"]
   ["ORCHIDv2 exception" "2001:20::1"]
   ["DET exception" "2001:30::1"]
   ["NAT64 embedding public IPv4" "64:ff9b::8.8.8.8"]
   ["raw IPv4-mapped public IPv4" [:ipv4-mapped 8 8 8 8]]])

(defn- ->address
  [value]
  (cond
    (instance? InetAddress value) value
    (and (vector? value) (= :ipv4-mapped (first value)))
    (apply raw-ipv4-mapped (rest value))
    :else (InetAddress/getByName ^String value)))

(deftest public-egress-address-classification-matrix
  (testing "blocks private, multicast, transition, and reserved destinations"
    (doseq [[label value] blocked-address-cases
            :let [address (->address value)]]
      (is (true? (ssrf/private-ip? address)) label)
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"private/internal"
           (ssrf/resolve-public-url! (constantly [address])
                                     "https://service.example/resource"))
          label)))
  (testing "allows public ranges and explicit globally reachable exceptions"
    (doseq [[label value] public-address-cases
            :let [address (->address value)
                  result  (ssrf/resolve-public-url! (constantly [address])
                                                    "https://service.example/resource")]]
      (is (false? (ssrf/private-ip? address)) label)
      (is (false? (:private-network? result)) label))))

(deftest public-url-resolution-rejects-mixed-public-and-private-results
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"private/internal"
       (ssrf/resolve-public-url! (fn [_] (addresses "93.184.216.34" "127.0.0.1"))
                                 "https://mixed.example/resource"))))

(deftest resolution-pins-one-complete-dns-answer-set
  (let [calls   (atom 0)
        public  (first (addresses "93.184.216.34"))
        private (first (addresses "127.0.0.1"))
        resolver (fn [_]
                   (if (= 1 (swap! calls inc))
                     [public]
                     [private]))
        result  (ssrf/resolve-public-url! resolver
                                          "https://rebind.example/resource")]
    (is (= 1 @calls))
    (is (= [public] (:addresses result)))
    (is (false? (:private-network? result)))))

(deftest numeric-private-host-literals-cannot-bypass-resolution-policy
  (doseq [url ["http://2130706433/resource"
               "http://[::ffff:127.0.0.1]/resource"
               "http://[64:ff9b::127.0.0.1]/resource"
               "http://[ff02::1]/resource"]]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"private/internal"
         (ssrf/resolve-public-url! url))
        url)))

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
