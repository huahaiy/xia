(ns xia.config
  "Helpers for reading validated configuration values from the DB."
  (:require [clojure.string :as str]
            [xia.db :as db]
            [xia.runtime-overlay :as runtime-overlay]))

(declare positive-long-resolution
         positive-double-resolution
         bounded-double-resolution
         custom-option-resolution
         keyword-option-resolution
         boolean-option-resolution
         string-option-resolution)

(defn- unsupported-merge-mode!
  [config-key merge-mode]
  (throw (ex-info "Runtime overlay merge mode is not supported for this config reader."
                  {:config-key config-key
                   :merge merge-mode})))

(defn- invalid-overlay-value!
  [config-key merge-mode raw-value]
  (throw (ex-info "Runtime overlay value is invalid for this config reader."
                  {:config-key config-key
                   :merge merge-mode
                   :value raw-value})))

(defn- effective-source
  [tenant-present? overlay-present? overlay-mode overlay-value effective-value default-value]
  (cond
    (and overlay-present?
         (= overlay-mode :replace)
         (some? overlay-value))
    :runtime-overlay

    (and overlay-present?
         (#{:cap :floor} overlay-mode)
         (some? overlay-value))
    (cond
      (and tenant-present? (= effective-value overlay-value))
      :runtime-overlay

      tenant-present?
      :tenant-db

      (= effective-value overlay-value)
      :runtime-overlay

      :else
      :default)

    tenant-present?
    :tenant-db

    :else
    :default))

(defn- resolution-map
  [config-key default-value tenant-raw tenant-value overlay-present? overlay-mode overlay-raw-value overlay-value effective-value]
  {:config-key       config-key
   :default-value    default-value
   :tenant-present?  (some? tenant-value)
   :tenant-value     tenant-value
   :overlay-present? overlay-present?
   :overlay-mode     overlay-mode
   :overlay-value    overlay-value
   :value            effective-value
   :source           (effective-source (some? tenant-value)
                                       overlay-present?
                                       overlay-mode
                                       overlay-value
                                       effective-value
                                       default-value)
   :raw              {:tenant tenant-raw
                       :overlay overlay-raw-value}})

(defn- tenant-config
  [config-key]
  (try
    (db/tenant-config-value config-key)
    (catch clojure.lang.ExceptionInfo ex
      (if (re-find #"Database not connected" (.getMessage ex))
        (db/get-config config-key)
        (throw ex)))))

(defn- overlay-merge-mode
  [config-key]
  (runtime-overlay/config-merge-mode config-key))

(defn- overlay-config
  [config-key]
  (runtime-overlay/config-value config-key))

(defn- resolve-replace-option
  [config-key default-value parse-fn]
  (let [tenant-raw         (tenant-config config-key)
        tenant-value       (some-> tenant-raw parse-fn)
        base-value         (if (some? tenant-value) tenant-value default-value)
        overlay-present?   (runtime-overlay/config-override? config-key)
        overlay-mode       (overlay-merge-mode config-key)
        overlay-raw-value  (overlay-config config-key)
        overlay-value      (some-> overlay-raw-value parse-fn)]
    (when (and overlay-present?
               (some? overlay-raw-value)
               (nil? overlay-value))
      (invalid-overlay-value! config-key overlay-mode overlay-raw-value))
    (let [effective-value (case overlay-mode
                            nil base-value
                            :replace (if (some? overlay-value)
                                       overlay-value
                                       base-value)
                            (unsupported-merge-mode! config-key overlay-mode))]
      (resolution-map config-key
                      default-value
                      tenant-raw
                      tenant-value
                      overlay-present?
                      overlay-mode
                      overlay-raw-value
                      overlay-value
                      effective-value))))

(defn- resolve-bounded-option
  [config-key default-value parse-fn]
  (let [tenant-raw         (tenant-config config-key)
        tenant-value       (some-> tenant-raw parse-fn)
        base-value         (if (some? tenant-value) tenant-value default-value)
        overlay-present?   (runtime-overlay/config-override? config-key)
        overlay-mode       (overlay-merge-mode config-key)
        overlay-raw-value  (overlay-config config-key)
        overlay-value      (some-> overlay-raw-value parse-fn)]
    (when (and overlay-present?
               (some? overlay-raw-value)
               (nil? overlay-value))
      (invalid-overlay-value! config-key overlay-mode overlay-raw-value))
    (let [effective-value (case overlay-mode
                            nil base-value
                            :replace (if (some? overlay-value)
                                       overlay-value
                                       base-value)
                            :cap (if (some? overlay-value)
                                   (min base-value overlay-value)
                                   base-value)
                            :floor (if (some? overlay-value)
                                     (max base-value overlay-value)
                                     base-value))]
      (resolution-map config-key
                      default-value
                      tenant-raw
                      tenant-value
                      overlay-present?
                      overlay-mode
                      overlay-raw-value
                      overlay-value
                      effective-value))))

(defn- parse-positive-long
  [raw]
  (try
    (let [parsed (Long/parseLong (str raw))]
      (when (pos? parsed)
        parsed))
    (catch Exception _
      nil)))

(defn- parse-positive-double
  [raw]
  (try
    (let [parsed (Double/parseDouble (str raw))]
      (when (pos? parsed)
        parsed))
    (catch Exception _
      nil)))

(defn- parse-bounded-double
  [raw]
  (try
    (let [parsed (Double/parseDouble (str raw))]
      (when (<= 0.0 parsed 1.0)
        parsed))
    (catch Exception _
      nil)))

(defn positive-long
  [config-key default-value]
  (:value (positive-long-resolution config-key default-value)))

(defn positive-long-resolution
  [config-key default-value]
  (resolve-bounded-option config-key default-value parse-positive-long))

(defn positive-double
  [config-key default-value]
  (:value (positive-double-resolution config-key default-value)))

(defn positive-double-resolution
  [config-key default-value]
  (resolve-bounded-option config-key default-value parse-positive-double))

(defn bounded-double
  [config-key default-value]
  (:value (bounded-double-resolution config-key default-value)))

(defn bounded-double-resolution
  [config-key default-value]
  (resolve-bounded-option config-key default-value parse-bounded-double))

(defn custom-option
  [config-key default-value parse-fn]
  (:value (custom-option-resolution config-key default-value parse-fn)))

(defn custom-option-resolution
  [config-key default-value parse-fn]
  (resolve-replace-option config-key default-value parse-fn))

(defn keyword-option
  [config-key default-value allowed-values]
  (:value (keyword-option-resolution config-key default-value allowed-values)))

(defn keyword-option-resolution
  [config-key default-value allowed-values]
  (resolve-replace-option config-key
                          default-value
                          (fn [raw]
                            (let [parsed (some-> raw str keyword)]
                              (when (contains? allowed-values parsed)
                                parsed)))))

(defn boolean-option
  [config-key default-value]
  (:value (boolean-option-resolution config-key default-value)))

(defn boolean-option-resolution
  [config-key default-value]
  (resolve-replace-option config-key
                          default-value
                          (fn [raw]
                            (let [normalized (some-> raw str str/trim str/lower-case)]
                              (cond
                                (#{"true" "1" "yes" "on"} normalized) true
                                (#{"false" "0" "no" "off"} normalized) false
                                :else nil)))))

(defn string-option
  [config-key default-value]
  (:value (string-option-resolution config-key default-value)))

(defn string-option-resolution
  [config-key default-value]
  (resolve-replace-option config-key
                          default-value
                          (fn [raw]
                            (let [parsed (some-> raw str str/trim)]
                              (when (seq parsed)
                                parsed)))))
