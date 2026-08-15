(ns xia.web-ssrf-test
  (:require [clojure.test :refer [deftest is]]
            [xia.web :as web])
  (:import [java.net InetAddress URI]))

(defn- public-address
  [host final-octet]
  (InetAddress/getByAddress
   host
   (byte-array [(byte 93) (byte -72) (byte 34) (byte final-octet)])))

(deftest web-fetch-resolves-and-pins-every-redirect-hop
  (let [calls (atom [])]
    (with-redefs-fn {#'web/resolve-host-addresses
                     (fn [host]
                       (case host
                         "example.com" [(public-address host 20)]
                         "next.example" [(public-address host 21)]))
                     #'web/check-rate-limit! (fn [_] nil)
                     #'web/fetch-url!
                     (fn [url _headers resolution]
                       (swap! calls conj
                              {:url url
                               :host (:host resolution)
                               :addresses (mapv #(.getHostAddress ^InetAddress %)
                                                (:addresses resolution))})
                       (if (= url "https://example.com/start")
                         {:status 302
                          :headers {"location" "https://next.example/next"}
                          :body ""}
                         {:status 200
                          :headers {"content-type" "text/plain"}
                          :body "ok"}))}
      #(let [result (#'web/fetch-raw "https://example.com/start")]
         (is (= [{:url "https://example.com/start"
                  :host "example.com"
                  :addresses ["93.184.34.20"]}
                 {:url "https://next.example/next"
                  :host "next.example"
                  :addresses ["93.184.34.21"]}]
                @calls))
         (is (= {:status 200
                 :body "ok"
                 :final-url "https://next.example/next"}
                (select-keys result [:status :body :final-url])))))))

(deftest web-fetch-blocks-a-private-redirect-before-the-second-request
  (let [network-calls (atom [])]
    (with-redefs-fn {#'web/resolve-host-addresses
                     (fn [host]
                       (case host
                         "public.example" [(public-address host 20)]
                         "127.0.0.1" [(InetAddress/getByName "127.0.0.1")]))
                     #'web/check-rate-limit! (fn [_] nil)
                     #'web/fetch-url!
                     (fn [url _headers _resolution]
                       (swap! network-calls conj url)
                       {:status 302
                        :headers {"location" "http://127.0.0.1/secret"}
                        :body ""})}
      #(do
         (is (thrown-with-msg?
              clojure.lang.ExceptionInfo
              #"private/internal"
              (#'web/fetch-raw "https://public.example/start")))
         (is (= ["https://public.example/start"] @network-calls))))))

(deftest web-fetch-transport-consumes-only-pinned-addresses
  (let [first-address  (public-address "rebind.example" 20)
        second-address (public-address "rebind.example" 21)
        attempts       (atom [])]
    (with-redefs-fn {#'web/fetch-address!
                     (fn [url _headers resolution address]
                       (swap! attempts conj
                              {:url url
                               :host (:host resolution)
                               :address (.getHostAddress ^InetAddress address)})
                       (if (= address first-address)
                         (throw (java.io.IOException. "connect failed"))
                         {:status 200
                          :headers {"content-type" "text/plain"}
                          :body "ok"}))}
      #(let [result (#'web/fetch-url! "https://rebind.example/start"
                                      {"User-Agent" "Xia/0.1"}
                                      {:url "https://rebind.example/start"
                                       :uri (URI. "https://rebind.example/start")
                                       :host "rebind.example"
                                       :addresses [first-address second-address]})]
         (is (= [{:url "https://rebind.example/start"
                  :host "rebind.example"
                  :address "93.184.34.20"}
                 {:url "https://rebind.example/start"
                  :host "rebind.example"
                  :address "93.184.34.21"}]
                @attempts))
         (is (= {:status 200 :body "ok"}
                (select-keys result [:status :body])))))))
