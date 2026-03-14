(ns xia.config
  "Helpers for reading validated configuration values from the DB."
  (:require [xia.db :as db]))

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
