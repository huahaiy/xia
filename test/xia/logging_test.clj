(ns xia.logging-test
  (:require [clojure.test :refer :all]
            [taoensso.timbre :as timbre]
            [xia.logging :as logging])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-log-path []
  (str (Files/createTempDirectory "xia-log-test"
         (into-array FileAttribute []))
       "/xia.log"))

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
  (let [orig-config timbre/*config*
        log-path    (temp-log-path)
        result      (with-redefs [xia.logging/env-value (constantly nil)]
                      (logging/configure! {:log-file log-path}))]
    (try
      (is (= {:path log-path :appender "xia-file"} result))
      (timbre/info "hello from timbre file appender")
      (is (re-find #"hello from timbre file appender"
                   (slurp log-path)))
      (finally
        (timbre/set-config! orig-config)))))
