(ns xia.policy.ocr
  "Local document OCR policy."
  (:require [xia.config :as cfg]))

(def ^:private default-local-doc-ocr-timeout-ms 120000)
(def ^:private default-local-doc-ocr-max-tokens 2048)

(defn local-doc-ocr-timeout-ms
  []
  (cfg/positive-long :local-doc/ocr-timeout-ms
                     default-local-doc-ocr-timeout-ms))

(defn local-doc-ocr-max-tokens
  []
  (cfg/positive-long :local-doc/ocr-max-tokens
                     default-local-doc-ocr-max-tokens))
