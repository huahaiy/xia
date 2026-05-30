(ns xia.policy.browser
  "Browser automation policy."
  (:require [xia.config :as cfg]))

(def ^:private default-browser-playwright-timeout-ms 15000)

(defn browser-playwright-timeout-ms
  []
  (cfg/positive-long :browser/playwright-timeout-ms
                     default-browser-playwright-timeout-ms))
