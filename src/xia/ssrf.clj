(ns xia.ssrf
  "Helpers for validating outbound URLs against SSRF risks.

   Callers that will open a network connection must connect to one of the
   returned addresses rather than resolving the hostname again. Validation-only
   use is not sufficient for rebinding-safe HTTP clients."
  (:require [clojure.string :as str])
  (:import [java.net InetAddress URI UnknownHostException]))

(defn- address-octets
  [^InetAddress addr]
  (mapv #(bit-and (int %) 0xff) (.getAddress addr)))

(defn- literal-prefix
  [literal prefix-length]
  [(address-octets (InetAddress/getByName literal)) prefix-length])

(defn- prefix-match?
  [octets [prefix prefix-length]]
  (let [full-bytes (quot prefix-length 8)
        tail-bits  (mod prefix-length 8)]
    (and (= (count octets) (count prefix))
         (= (subvec octets 0 full-bytes)
            (subvec prefix 0 full-bytes))
         (or (zero? tail-bits)
             (let [mask (bit-and 0xff (bit-shift-left 0xff (- 8 tail-bits)))]
               (= (bit-and mask (octets full-bytes))
                  (bit-and mask (prefix full-bytes))))))))

;; These tables follow the IANA IPv4 and IPv6 Special-Purpose Address
;; Registries (retrieved 2026-08-14). The broad IPv6 2001::/23 reservation and
;; IPv4 192.0.0.0/24 reservation contain the explicit globally reachable
;; exceptions below. Multicast is handled separately by InetAddress.
(def ^:private non-public-ipv4-prefixes
  (mapv (fn [[literal prefix-length]]
          (literal-prefix literal prefix-length))
        [["0.0.0.0" 8]
         ["10.0.0.0" 8]
         ["100.64.0.0" 10]
         ["127.0.0.0" 8]
         ["169.254.0.0" 16]
         ["172.16.0.0" 12]
         ["192.0.0.0" 24]
         ["192.0.2.0" 24]
         ["192.88.99.0" 24]
         ["192.168.0.0" 16]
         ["198.18.0.0" 15]
         ["198.51.100.0" 24]
         ["203.0.113.0" 24]
         ["240.0.0.0" 4]]))

(def ^:private globally-reachable-ipv4-exceptions
  (mapv (fn [[literal prefix-length]]
          (literal-prefix literal prefix-length))
        [["192.0.0.9" 32]
         ["192.0.0.10" 32]]))

(def ^:private non-public-ipv6-prefixes
  (mapv (fn [[literal prefix-length]]
          (literal-prefix literal prefix-length))
        [["::" 96]
         ["64:ff9b:1::" 48]
         ["100::" 64]
         ["100:0:0:1::" 64]
         ["2001::" 23]
         ["2001:db8::" 32]
         ["2002::" 16]
         ["3fff::" 20]
         ["5f00::" 16]
         ["fc00::" 7]
         ["fe80::" 10]]))

(def ^:private globally-reachable-ipv6-exceptions
  (mapv (fn [[literal prefix-length]]
          (literal-prefix literal prefix-length))
        [["2001:1::1" 128]
         ["2001:1::2" 128]
         ["2001:1::3" 128]
         ["2001:3::" 32]
         ["2001:4:112::" 48]
         ["2001:20::" 28]
         ["2001:30::" 28]]))

(def ^:private ipv4-mapped-prefix
  [(vec (concat (repeat 10 0) [0xff 0xff] (repeat 4 0))) 96])

(def ^:private nat64-well-known-prefix
  (literal-prefix "64:ff9b::" 96))

(def ^:private ipv6-global-unicast-prefix
  ;; IANA currently limits global-unicast allocations to 2000::/3. Other
  ;; globally reachable special-purpose prefixes are handled explicitly.
  (literal-prefix "2000::" 3))

(defn- matches-any-prefix?
  [octets prefixes]
  (boolean (some #(prefix-match? octets %) prefixes)))

(defn- non-public-ipv4-octets?
  [octets]
  (and (matches-any-prefix? octets non-public-ipv4-prefixes)
       (not (matches-any-prefix? octets globally-reachable-ipv4-exceptions))))

(defn- embedded-ipv4-octets
  [octets prefix]
  (when (prefix-match? octets prefix)
    (subvec octets 12 16)))

(defn- non-public-special-range?
  [octets]
  (case (count octets)
    4 (non-public-ipv4-octets? octets)
    16 (if-let [embedded (or (embedded-ipv4-octets octets ipv4-mapped-prefix)
                             (embedded-ipv4-octets octets nat64-well-known-prefix))]
         ;; Treat mapped values as representations of their IPv4 destination,
         ;; not as independently routable IPv6 space.
         (non-public-ipv4-octets? embedded)
         (or (not (prefix-match? octets ipv6-global-unicast-prefix))
             (and (matches-any-prefix? octets non-public-ipv6-prefixes)
                  (not (matches-any-prefix? octets globally-reachable-ipv6-exceptions)))))
    true))

(defn private-ip?
  "True if an address is unsafe for untrusted public-network egress.

   In addition to private, loopback, link-local, ULA, and CGNAT addresses,
   this rejects multicast and non-globally-reachable IANA special-purpose
   ranges. IPv4-mapped IPv6 and well-known-prefix NAT64 addresses are checked
   using their embedded IPv4 destination."
  [^InetAddress addr]
  (or (.isLoopbackAddress addr)
      (.isLinkLocalAddress addr)
      (.isSiteLocalAddress addr)
      (.isAnyLocalAddress addr)
      (.isMulticastAddress addr)
      (non-public-special-range? (address-octets addr))))

(defn resolve-host-addresses
  [host]
  (vec (InetAddress/getAllByName host)))

(defn resolve-url!
  "Validate a URL and return the resolved host details.

   By default private/internal addresses are blocked. Callers that
   intentionally need to reach loopback or private-network services must opt in
   with :allow-private-network?. The returned :addresses are the pinned
   connection targets for rebinding-safe transports."
  ([url]
   (resolve-url! resolve-host-addresses url {}))
  ([url opts]
   (resolve-url! resolve-host-addresses url opts))
  ([resolver url {:keys [allow-private-network?]
                  :or   {allow-private-network? false}}]
   (let [uri (URI. url)]
     (when-not (#{"http" "https"} (.getScheme uri))
       (throw (ex-info "Only http:// and https:// URLs are allowed"
                       {:url url
                        :scheme (.getScheme uri)})))
     (let [host (.getHost uri)]
       (when (str/blank? host)
         (throw (ex-info "URL has no host" {:url url})))
       (let [addrs            (vec (resolver host))
             private-network? (boolean (some private-ip? addrs))]
         (when (empty? addrs)
           (throw (UnknownHostException.
                   (str "Host did not resolve to any addresses: " host))))
         (when (and (not allow-private-network?) private-network?)
           (throw (ex-info "Access to private/internal network addresses is blocked"
                           {:url url
                            :host host})))
         {:url              url
          :uri              uri
          :host             host
          :addresses        addrs
          :private-network? private-network?})))))

(defn resolve-public-url!
  "Validate a URL for safety and return the pinned resolution.

   Throws on disallowed schemes, missing hosts, or any non-public/internal
   address in the resolved set."
  ([url]
   (resolve-public-url! resolve-host-addresses url))
  ([resolver url]
   (resolve-url! resolver url {:allow-private-network? false})))

(defn validate-url!
  ([url]
   (validate-url! resolve-host-addresses url))
  ([resolver url]
   (resolve-public-url! resolver url)
   nil))
