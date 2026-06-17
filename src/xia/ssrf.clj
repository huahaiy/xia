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

(defn- cgnat-ipv4?
  "True for 100.64.0.0/10."
  [^InetAddress addr]
  (let [octets (address-octets addr)]
    (and (= 4 (count octets))
         (= 100 (octets 0))
         (<= 64 (octets 1) 127))))

(defn- unique-local-ipv6?
  "True for fc00::/7."
  [^InetAddress addr]
  (let [octets (address-octets addr)]
    (and (= 16 (count octets))
         (= 0xfc (bit-and 0xfe (octets 0))))))

(defn private-ip?
  "True if the address is private, loopback, link-local, ULA, or CGNAT."
  [^InetAddress addr]
  (or (.isLoopbackAddress addr)
      (.isLinkLocalAddress addr)
      (.isSiteLocalAddress addr)
      (cgnat-ipv4? addr)
      (unique-local-ipv6? addr)
      (.isAnyLocalAddress addr)))

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

   Throws on disallowed schemes, missing hosts, or any private/internal
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
