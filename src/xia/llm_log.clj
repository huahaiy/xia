(ns xia.llm-log
  "Pure policy helpers for persisted LLM diagnostics."
  (:require [clojure.string :as str]))

(def full-payloads-config-key :llm/log-full-payloads?)
(def retention-days-config-key :llm/log-retention-days)

(def default-full-payloads? false)
(def default-retention-days 30)
(def max-retention-days 3650)

(def full-payload-entry-keys
  "Entry keys that can contain prompts, tool definitions, provider responses,
   or provider error bodies. These fields are omitted unless detailed logging
   is explicitly enabled."
  #{:messages :tools :response :error})

(defn parse-boolean-option
  [value]
  (let [normalized (some-> value str str/trim str/lower-case)]
    (cond
      (#{"true" "1" "yes" "on"} normalized) true
      (#{"false" "0" "no" "off"} normalized) false
      :else nil)))

(defn parse-retention-days
  [value]
  (try
    (let [parsed (Long/parseLong (str value))]
      (when (<= 1 parsed max-retention-days)
        parsed))
    (catch Exception _
      nil)))

(defn settings
  "Resolve LLM logging settings using a raw config lookup function. Invalid or
   absent values fall back to the privacy-preserving defaults."
  [get-config]
  (let [full-payloads (some-> (get-config full-payloads-config-key)
                              parse-boolean-option)
        retention-days (some-> (get-config retention-days-config-key)
                               parse-retention-days)]
    {:full-payloads? (if (some? full-payloads)
                       full-payloads
                       default-full-payloads?)
     :retention-days (or retention-days default-retention-days)}))

(defn persisted-entry
  "Apply the detailed-payload capture policy to an LLM log entry. Operational
   metadata is retained in either mode."
  [{:keys [full-payloads?]} entry]
  (if full-payloads?
    entry
    (apply dissoc entry full-payload-entry-keys)))

(defn retention-cutoff
  "Return the oldest permitted creation timestamp for the supplied instant."
  [^java.util.Date now retention-days]
  (let [retention-ms (* (long retention-days) 24 60 60 1000)
        cutoff-ms    (max 0 (- (.getTime now) retention-ms))]
    (java.util.Date. cutoff-ms)))
