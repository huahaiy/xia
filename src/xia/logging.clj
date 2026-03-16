(ns xia.logging
  "Runtime logging configuration."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as timbre]))

(def ^:private file-appender-name :xia-file)

(defn- env-value
  [k]
  (System/getenv k))

(defn resolve-log-file
  [options]
  (let [cli-path (:log-file options)
        env-path (env-value "XIA_LOG_FILE")]
    (cond
      (and (string? cli-path) (not (str/blank? cli-path))) cli-path
      (and (string? env-path) (not (str/blank? env-path))) env-path
      :else nil)))

(defn- ensure-parent-dir!
  [path]
  (io/make-parents path))

(defn- detach-file-appender!
  []
  (timbre/merge-config! {:appenders {file-appender-name nil}}))

(defn configure!
  [options]
  (when-let [log-file (resolve-log-file options)]
    (ensure-parent-dir! log-file)
    (detach-file-appender!)
    (timbre/merge-config!
      {:appenders
       {file-appender-name
        (assoc (timbre/spit-appender {:fname log-file
                                      :append? true
                                      :locking? true})
               :enabled? true)}})
    {:path log-file
     :appender (name file-appender-name)}))
