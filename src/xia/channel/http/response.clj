(ns xia.channel.http.response
  "Shared HTTP response helpers."
  (:require [charred.api :as json]
            [clojure.string :as str]))

(defn json-response
  [status body]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/write-json-str body)})

(defn- utf8-download-media-type
  [media-type]
  (let [base (some-> media-type str str/trim not-empty)]
    (cond
      (nil? base) "application/octet-stream"
      (re-find #";\s*charset=" base) base
      (or (str/starts-with? base "text/")
          (= base "application/json")
          (= base "application/edn")
          (= base "application/xml"))
      (str base "; charset=utf-8")
      :else
      base)))

(defn- quoted-filename
  [filename]
  (-> (or (some-> filename str str/trim not-empty) "download")
      (str/replace #"[\\\"\r\n]+" "_")))

(defn download-response
  [filename media-type body]
  {:status  200
   :headers {"Content-Type"        (utf8-download-media-type media-type)
             "Content-Disposition" (str "attachment; filename=\"" (quoted-filename filename) "\"")}
   :body    body})
