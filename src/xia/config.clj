(ns xia.config
  "Helpers for reading validated configuration values from the DB."
  (:require [clojure.string :as str]
            [xia.db :as db]))

(defn positive-long
  [config-key default-value]
  (if-let [raw (db/get-config config-key)]
    (try
      (let [parsed (Long/parseLong (str raw))]
        (if (pos? parsed)
          parsed
          default-value))
      (catch Exception _
        default-value))
    default-value))

(defn keyword-option
  [config-key default-value allowed-values]
  (if-let [raw (db/get-config config-key)]
    (let [parsed (some-> raw str keyword)]
      (if (contains? allowed-values parsed)
        parsed
        default-value))
    default-value))

(defn boolean-option
  [config-key default-value]
  (if-let [raw (db/get-config config-key)]
    (let [normalized (some-> raw str str/trim str/lower-case)]
      (cond
        (#{"true" "1" "yes" "on"} normalized) true
        (#{"false" "0" "no" "off"} normalized) false
        :else default-value))
    default-value))
