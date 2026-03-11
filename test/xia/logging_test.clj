(ns xia.logging-test
  (:require [clojure.test :refer :all]
            [xia.logging :as logging])
  (:import [ch.qos.logback.classic LoggerContext]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [org.slf4j LoggerFactory]))

(defn- temp-log-path []
  (str (Files/createTempDirectory "xia-log-test"
         (into-array FileAttribute []))
       "/xia.log"))

(defn- root-logger []
  (.getLogger ^LoggerContext (LoggerFactory/getILoggerFactory)
              org.slf4j.Logger/ROOT_LOGGER_NAME))

(defn- detach-file-appender! []
  (let [root (root-logger)]
    (when-let [appender (.getAppender root "XIA_FILE")]
      (.detachAppender root appender)
      (.stop appender))))

(deftest resolve-log-file-prefers-cli-over-env
  (with-redefs [xia.logging/env-value (constantly "/tmp/from-env.log")]
    (is (= "/tmp/from-cli.log"
           (logging/resolve-log-file {:log-file "/tmp/from-cli.log"})))
    (is (= "/tmp/from-env.log"
           (logging/resolve-log-file {})))
    (is (= "/tmp/from-env.log"
           (logging/resolve-log-file {:log-file "  "})))
    (is (nil? (with-redefs [xia.logging/env-value (constantly " ")]
                (logging/resolve-log-file {}))))))

(deftest configure-attaches-file-appender-when-requested
  (detach-file-appender!)
  (let [log-path (temp-log-path)
        result   (with-redefs [xia.logging/env-value (constantly nil)]
                   (logging/configure! {:log-file log-path}))
        appender (.getAppender (root-logger) "XIA_FILE")]
    (try
      (is (= {:path log-path :appender "XIA_FILE"} result))
      (is (some? appender))
      (is (= log-path (.getFile appender)))
      (finally
        (detach-file-appender!)))))
