(ns xia.logging
  "Runtime logging configuration."
  (:require [clojure.string :as str])
  (:import [ch.qos.logback.classic LoggerContext]
           [ch.qos.logback.classic.encoder PatternLayoutEncoder]
           [ch.qos.logback.core FileAppender]
           [java.io File]
           [org.slf4j Logger LoggerFactory]))

(def ^:private file-appender-name "XIA_FILE")
(def ^:private default-pattern "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n")

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

(defn- logger-context
  []
  (let [factory (LoggerFactory/getILoggerFactory)]
    (when (instance? LoggerContext factory)
      factory)))

(defn- ensure-parent-dir!
  [path]
  (when-let [parent (.getParentFile (File. path))]
    (.mkdirs parent)))

(defn- detach-file-appender!
  [root]
  (when-let [existing (.getAppender root file-appender-name)]
    (.detachAppender root existing)
    (.stop existing)))

(defn configure!
  [options]
  (when-let [log-file (resolve-log-file options)]
    (when-let [ctx (logger-context)]
      (ensure-parent-dir! log-file)
      (let [root    (.getLogger ctx Logger/ROOT_LOGGER_NAME)
            encoder (doto (PatternLayoutEncoder.)
                      (.setContext ctx)
                      (.setPattern default-pattern)
                      (.start))
            appender (doto (FileAppender.)
                       (.setContext ctx)
                       (.setName file-appender-name)
                       (.setFile log-file)
                       (.setAppend true)
                       (.setEncoder encoder)
                       (.start))]
        (detach-file-appender! root)
        (.addAppender root appender)
        {:path log-file
         :appender file-appender-name}))))
