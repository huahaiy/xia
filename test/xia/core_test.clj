(ns xia.core-test
  (:require [clojure.test :refer :all]
            [xia.core :as core]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.pack :as pack])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.util Base64]))

(defn- temp-dir []
  (str (Files/createTempDirectory "xia-core-test"
         (into-array FileAttribute []))))

(defn- encode-key [byte-value]
  (.encodeToString (Base64/getEncoder)
                   (byte-array (repeat 32 (byte byte-value)))))

(deftest main-opens-archive-argument-directly
  (let [dir          (temp-dir)
        db-path      (str dir "/db")
        archive      (str dir "/backup.xia")
        key-file     (str dir "/master.key")
        key-file-env {"XIA_MASTER_KEY_FILE" key-file}
        started      (atom nil)]
    (spit key-file (encode-key 11))
    (with-redefs-fn {#'xia.crypto/env-value (fn [k] (get key-file-env k))}
      #(do
         (db/connect! db-path)
         (try
           (db/set-config! :user/name "CLI Archive")
           (finally
             (db/close!)))))
    (with-redefs-fn {#'xia.core/start! (fn [options] (reset! started options))
                     #'xia.core/register-shutdown-hook! (fn [_] ::hook)
                     #'xia.core/remove-shutdown-hook! (fn [_] nil)
                     #'xia.core/make-cleanup (fn [_] (fn [] nil))}
      (fn []
         (with-redefs-fn {#'xia.pack/env-value (fn [k] (get key-file-env k))}
           (fn []
             (pack/pack! db-path archive)))
         (core/-main archive)))
    (is (string? (:db @started)))
    (is (= archive (get-in @started [:archive-context :archive-path])))
    (is (.endsWith ^String (:db @started) "/db"))
    (is (.endsWith ^String (:key-file (:crypto-opts @started))
                   "/db/.xia/master.key"))))
