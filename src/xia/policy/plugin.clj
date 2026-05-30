(ns xia.policy.plugin
  "Plugin sandbox and hook policy."
  (:require [xia.config :as cfg]))

(def ^:private default-plugin-hook-timeout-ms 5000)
(def ^:private default-plugin-hook-max-code-chars 12000)
(def ^:private default-plugin-max-hooks 32)
(def ^:private default-plugin-max-active-workers 16)

(defn plugin-hook-timeout-ms
  []
  (cfg/positive-long :plugin/hook-timeout-ms
                     default-plugin-hook-timeout-ms))

(defn plugin-hook-max-code-chars
  []
  (cfg/positive-long :plugin/hook-max-code-chars
                     default-plugin-hook-max-code-chars))

(defn plugin-max-hooks
  []
  (cfg/positive-long :plugin/max-hooks
                     default-plugin-max-hooks))

(defn plugin-max-active-workers
  []
  (cfg/positive-long :plugin/max-active-workers
                     default-plugin-max-active-workers))
