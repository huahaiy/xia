(ns xia.browser-remote-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.browser.remote :as remote]
            [xia.db :as db]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(defn- temp-secret-file
  [payload]
  (let [path (str (java.nio.file.Files/createTempFile
                   "xia-remote-browser-token"
                   ".txt"
                   (make-array java.nio.file.attribute.FileAttribute 0)))]
    (spit (io/file path) payload)
    path))

(deftest remote-browser-auth-token-comes-from-token-file
  (let [token-file (temp-secret-file "browser-token\n")]
    (db/set-config! :browser/remote-token-file token-file)
    (db/set-config! :browser/remote-auth-token "legacy-inline-token")
    (is (= "browser-token" (#'remote/auth-token)))))
