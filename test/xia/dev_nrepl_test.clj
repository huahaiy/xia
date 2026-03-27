(ns xia.dev-nrepl-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [xia.dev-nrepl :as dev-nrepl])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-file
  []
  (str (Files/createTempFile "xia-dev-nrepl"
                             ".port"
                             (into-array FileAttribute []))))

(use-fixtures :each
  (fn [f]
    (try
      (dev-nrepl/stop!)
      (f)
      (finally
        (dev-nrepl/stop!)))))

(deftest start-writes-port-file-and-stop-removes-it
  (let [port-file (temp-file)]
    (io/delete-file port-file true)
    (let [started (dev-nrepl/start! {:port 0
                                     :port-file port-file})]
      (is (= "127.0.0.1" (:bind started)))
      (is (pos? (:port started)))
      (is (= port-file (:port-file started)))
      (is (= (str (:port started))
             (some-> (slurp port-file) clojure.string/trim)))
      (is (= (select-keys started [:bind :port :port-file])
             (dev-nrepl/info))))
    (dev-nrepl/stop!)
    (is (nil? (dev-nrepl/info)))
    (is (false? (.exists (io/file port-file))))))
