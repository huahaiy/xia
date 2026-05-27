(ns xia.channel.http.value
  "Small value formatting and coercion helpers for HTTP handlers."
  (:require [clojure.string :as str]
            [xia.util :as util])
  (:import [java.util Date]))

(defn instant->str
  [value]
  (cond
    (instance? Date value) (str (.toInstant ^Date value))
    (instance? java.time.Instant value) (str value)
    :else nil))

(defn date->millis
  [value]
  (when (instance? Date value)
    (.getTime ^Date value)))

(defn truncate-text
  [value limit]
  (let [text  (some-> value str str/trim)
        limit (long limit)]
    (when (seq text)
      (if (> (long (count text)) limit)
        (str (subs text 0 (util/long-max 0 (- limit 1))) "…")
        text))))

(defn nonblank-str
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn parse-keyword-id
  [value field-name]
  (let [id-str (nonblank-str value)]
    (cond
      (nil? id-str)
      (throw (ex-info (str "missing '" field-name "' field") {:field field-name}))

      (re-find #"\s" id-str)
      (throw (ex-info (str "'" field-name "' must not contain whitespace")
                      {:field field-name
                       :value value}))

      :else
      (keyword id-str))))

(defn parse-optional-positive-long
  [value field-name]
  (let [text (nonblank-str value)]
    (when text
      (try
        (let [parsed (Long/parseLong text)]
          (when-not (pos? parsed)
            (throw (ex-info (str "'" field-name "' must be a positive integer")
                            {:field field-name
                             :value value})))
          parsed)
        (catch NumberFormatException _
          (throw (ex-info (str "'" field-name "' must be a positive integer")
                          {:field field-name
                           :value value})))))))
