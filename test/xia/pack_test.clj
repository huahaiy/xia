(ns xia.pack-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [xia.db :as db]
            [xia.pack :as pack])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.util Base64]
           [java.util.zip ZipFile]))

(defn- temp-dir []
  (str (Files/createTempDirectory "xia-pack-test"
         (into-array FileAttribute []))))

(defn- zip-entry-names [archive-path]
  (with-open [zip (ZipFile. archive-path)]
    (->> (enumeration-seq (.entries zip))
         (map #(.getName %))
         set)))

(defn- read-zip-entry [archive-path entry-name]
  (with-open [zip (ZipFile. archive-path)
              in  (.getInputStream zip (.getEntry zip entry-name))]
    (slurp in)))

(defn- encode-key [byte-value]
  (.encodeToString (Base64/getEncoder)
                   (byte-array (repeat 32 (byte byte-value)))))

(deftest pack-includes-db-and-salt-for-passphrase-mode
  (let [dir         (temp-dir)
        db-path     (str dir "/db")
        archive     (str dir "/backup.xia")]
    (db/connect! db-path {:passphrase-provider (constantly "pack-passphrase")})
    (try
      (db/set-config! :token/github "gh-secret")
      (finally
        (db/close!)))
    (let [result   (with-redefs-fn {#'xia.pack/env-value (constantly nil)}
                     #(pack/pack! db-path archive))
          entries  (zip-entry-names archive)
          manifest (edn/read-string (read-zip-entry archive "manifest.edn"))]
      (is (= (.getAbsolutePath (java.io.File. archive)) (:archive result)))
      (is (contains? entries "manifest.edn"))
      (is (contains? entries "db/.xia/master.salt"))
      (is (some #(= "db/data.mdb" %) entries))
      (is (not (contains? entries "db/lock.mdb")))
      (is (= :prompt-passphrase (:key-source manifest)))
      (is (= ["Provide the same master passphrase at startup."]
             (:restore-requires manifest))))))

(deftest pack-includes-explicit-key-file-support-material
  (let [dir          (temp-dir)
        db-path      (str dir "/db")
        archive      (str dir "/backup.xia")
        key-file     (str dir "/master.key")
        key-file-env {"XIA_MASTER_KEY_FILE" key-file}]
    (spit key-file (encode-key 7))
    (with-redefs-fn {#'xia.crypto/env-value (fn [k] (get key-file-env k))}
      #(do
         (db/connect! db-path)
         (try
           (db/set-config! :user/name "Pack Test")
           (finally
             (db/close!)))))
    (let [result   (with-redefs-fn {#'xia.pack/env-value (fn [k] (get key-file-env k))}
                     #(pack/pack! db-path archive))
          entries  (zip-entry-names archive)
          manifest (edn/read-string (read-zip-entry archive "manifest.edn"))]
      (is (= :env-file (:key-source result)))
      (is (contains? entries "db/.xia/master.key"))
      (is (not (contains? entries "db/.xia/master.salt")))
      (is (= :env-file (:key-source manifest)))
      (is (= ["Set XIA_MASTER_KEY_FILE to db/.xia/master.key after extracting the archive."]
             (:restore-requires manifest))))))

(deftest pack-default-archive-path
  (is (= "/tmp/xia-db.xia"
         (pack/default-archive-path "/tmp/xia-db"))))

(deftest open-archive-restores-db-layout
  (let [dir      (temp-dir)
        db-path  (str dir "/db")
        archive  (str dir "/backup.xia")]
    (db/connect! db-path {:passphrase-provider (constantly "open-passphrase")})
    (try
      (db/set-config! :token/github "gh-open-secret")
      (finally
        (db/close!)))
    (with-redefs-fn {#'xia.pack/env-value (constantly nil)}
      #(let [result (pack/pack! db-path archive)
             opened (pack/open-archive! archive)]
         (is (= (:archive result) (:archive-path opened)))
         (is (:refreshed? opened))
         (is (.exists (java.io.File. (:db-path opened))))
         (is (contains? (zip-entry-names archive) "manifest.edn"))
         (is (= :prompt-passphrase (get-in opened [:manifest :key-source])))
         (is (= {} (:crypto-opts opened)))))))

(deftest open-archive-infers-local-key-support-files
  (let [dir          (temp-dir)
        db-path      (str dir "/db")
        archive      (str dir "/backup.xia")
        key-file     (str dir "/master.key")
        key-file-env {"XIA_MASTER_KEY_FILE" key-file}]
    (spit key-file (encode-key 9))
    (with-redefs-fn {#'xia.crypto/env-value (fn [k] (get key-file-env k))}
      #(do
         (db/connect! db-path)
         (try
           (db/set-config! :user/name "Archive Restore")
           (finally
             (db/close!)))))
    (with-redefs-fn {#'xia.pack/env-value (fn [k] (get key-file-env k))}
      #(pack/pack! db-path archive))
    (let [opened (with-redefs-fn {#'xia.pack/env-value (constantly nil)}
                   #(pack/open-archive! archive))
          key-path (str (:db-path opened) "/.xia/master.key")]
      (is (= {:key-file key-path} (:crypto-opts opened)))
      (db/connect! (:db-path opened) (:crypto-opts opened))
      (try
        (is (= "Archive Restore" (db/get-config :user/name)))
        (finally
          (db/close!))))))
