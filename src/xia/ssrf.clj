(ns xia.ssrf
  "Helpers for validating outbound URLs and pinning DNS results."
  (:require [clojure.string :as str])
  (:import [java.net InetAddress URI UnknownHostException]
           [org.apache.http.conn DnsResolver]))

(defn private-ip?
  "True if the address is private, loopback, or link-local."
  [^InetAddress addr]
  (or (.isLoopbackAddress addr)
      (.isLinkLocalAddress addr)
      (.isSiteLocalAddress addr)
      (.isAnyLocalAddress addr)))

(defn resolve-host-addresses
  [host]
  (vec (InetAddress/getAllByName host)))

(defn resolve-public-url!
  "Validate a URL for safety and return the pinned resolution.

   Throws on disallowed schemes, missing hosts, or any private/internal
   address in the resolved set."
  ([url]
   (resolve-public-url! resolve-host-addresses url))
  ([resolver url]
   (let [uri (URI. url)]
     (when-not (#{"http" "https"} (.getScheme uri))
       (throw (ex-info "Only http:// and https:// URLs are allowed"
                       {:url url
                        :scheme (.getScheme uri)})))
     (let [host (.getHost uri)]
       (when (str/blank? host)
         (throw (ex-info "URL has no host" {:url url})))
       (let [addrs (vec (resolver host))]
         (when (empty? addrs)
           (throw (UnknownHostException.
                    (str "Host did not resolve to any addresses: " host))))
         (when (some private-ip? addrs)
           (throw (ex-info "Access to private/internal network addresses is blocked"
                           {:url url
                            :host host})))
         {:url url
          :uri uri
          :host host
          :addresses addrs})))))

(defn validate-url!
  ([url]
   (validate-url! resolve-host-addresses url))
  ([resolver url]
   (resolve-public-url! resolver url)
   nil))

(defn pinned-dns-resolver
  "Create an Apache HttpClient DnsResolver that only returns the validated
   addresses for the expected host."
  [{:keys [host addresses]}]
  (let [expected-host (some-> host str/lower-case)
        pinned-addrs  (into-array InetAddress addresses)]
    (reify DnsResolver
      (resolve [_ requested-host]
        (if (= expected-host (some-> requested-host str/lower-case))
          pinned-addrs
          (throw (UnknownHostException.
                   (str "Unexpected DNS lookup for "
                        requested-host
                        " while pinned to "
                        host))))))))
