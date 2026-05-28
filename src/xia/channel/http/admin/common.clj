(ns xia.channel.http.admin.common
  "Shared helpers for admin HTTP handlers."
  (:require [charred.api :as json]
            [clojure.string :as str]))

(defn json-response
  [deps status body]
  ((:json-response deps) status body))

(defn exception-response
  [deps throwable]
  ((:exception-response deps) throwable))

(defn instant->str
  [deps value]
  ((:instant->str deps) value))

(defn read-body
  [deps req]
  ((:read-body deps) req))

(defn truncate-text
  [deps value limit]
  ((:truncate-text deps) value limit))

(defn nonblank-str
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn normalize-base-url
  [value]
  (some-> value nonblank-str (str/replace #"/+$" "")))

(defn normalize-id-segment
  [value]
  (some-> value
          str
          str/trim
          str/lower-case
          (str/replace #"[^a-z0-9]+" "-")
          (str/replace #"^-+|-+$" "")
          not-empty))

(defn next-available-id
  [base used-ids]
  (let [base*    (or (normalize-id-segment base) "item")
        used-set (set (keep #(when % (name %)) used-ids))]
    (loop [candidate base*
           suffix    2]
      (if (contains? used-set candidate)
        (recur (str base* "-" suffix) (inc suffix))
        (keyword candidate)))))

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

(defn parse-json-object-string
  [value field-name]
  (let [text (nonblank-str value)]
    (when text
      (try
        (let [parsed (json/read-json text)]
          (when-not (map? parsed)
            (throw (ex-info (str field-name " must be a JSON object")
                            {:field field-name})))
          (json/write-json-str parsed))
        (catch clojure.lang.ExceptionInfo e
          (throw e))
        (catch Exception _
          (throw (ex-info (str field-name " must be valid JSON")
                          {:field field-name})))))))

(defn parse-json-object-value
  [value field-name]
  (cond
    (nil? value)
    nil

    (map? value)
    value

    :else
    (some-> (parse-json-object-string value field-name)
            json/read-json)))
