(ns xia.channel.http.request
  "Shared HTTP request parsing helpers."
  (:require [charred.api :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [xia.channel.http.auth :as http-auth])
  (:import [java.io ByteArrayOutputStream InputStream]
           [java.nio.charset StandardCharsets]))

(def ^:private max-body-bytes (* 16 1024 1024)) ; 16 MiB
(def ^:private byte-array-class (class (byte-array 0)))

(defn- body-too-large-ex
  []
  (ex-info "request body too large"
           {:type :http/request-body-too-large
            :status 413
            :error "request body too large"
            :max_bytes max-body-bytes}))

(defn- invalid-json-body-ex
  [cause]
  (ex-info "invalid JSON request body"
           {:type :http/invalid-json-body
            :status 400
            :error "invalid JSON request body"}
           cause))

(declare read-body-bytes)

(defn- read-body-text
  [body]
  (when-let [body-bytes (read-body-bytes body)]
    (String. ^bytes body-bytes StandardCharsets/UTF_8)))

(defn read-body-bytes
  [body]
  (cond
    (nil? body)
    nil

    (string? body)
    (let [body-bytes (.getBytes ^String body StandardCharsets/UTF_8)]
      (when (> (long (alength body-bytes)) (long max-body-bytes))
        (throw (body-too-large-ex)))
      body-bytes)

    (instance? byte-array-class body)
    (let [body-bytes ^bytes body]
      (when (> (long (alength body-bytes)) (long max-body-bytes))
        (throw (body-too-large-ex)))
      body-bytes)

    :else
    (with-open [^InputStream in (io/input-stream body)
                out (ByteArrayOutputStream.)]
      (let [buffer (byte-array 8192)]
        (loop [total 0]
          (let [read-count (.read in buffer)]
            (cond
              (neg? read-count)
              (.toByteArray out)

              (> (+ (long total) read-count) (long max-body-bytes))
              (throw (body-too-large-ex))

              :else
              (do
                (.write out buffer 0 read-count)
                (recur (+ total read-count))))))))))

(defn read-body
  [req]
  (when-let [body (:body req)]
    (let [body-text (read-body-text body)]
      (try
        (json/read-json body-text)
        (catch clojure.lang.ExceptionInfo e
          (throw e))
        (catch Exception e
          (throw (invalid-json-body-ex e)))))))

(defn request-header
  [req header-name]
  (http-auth/request-header req header-name))

(defn- request-content-type
  [req]
  (some-> (request-header req "content-type")
          str
          str/lower-case))

(defn multipart-form-request?
  [req]
  (some-> (request-content-type req)
          (str/starts-with? "multipart/form-data")))

(defn parse-query-string
  [query-string]
  (into {}
        (keep (fn [part]
                (let [[^String k ^String v] (str/split (str part) #"=" 2)]
                  (when (seq k)
                    [(java.net.URLDecoder/decode k "UTF-8")
                     (some-> v ^String (java.net.URLDecoder/decode "UTF-8"))]))))
        (str/split (or query-string "") #"&")))
