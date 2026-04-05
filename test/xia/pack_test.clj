(ns xia.pack-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [clojure.java.io :as io]
            [xia.db :as db]
            [xia.pack :as pack]
            [xia.test-helpers :as th])
  (:import [java.io BufferedOutputStream FileOutputStream]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files LinkOption Paths]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.util Base64]
           [java.util.zip ZipEntry ZipFile ZipOutputStream]
           [java.nio.file FileSystemException]))

(defn- temp-dir []
  (str (Files/createTempDirectory "xia-pack-test"
         (into-array FileAttribute []))))

(defn- zip-entry-names [archive-path]
  (with-open [zip (ZipFile. ^String archive-path)]
    (->> (enumeration-seq (.entries zip))
         (map (fn [^java.util.zip.ZipEntry entry]
                (.getName entry)))
         set)))

(defn- read-zip-entry [archive-path entry-name]
  (with-open [zip (ZipFile. ^String archive-path)
              in  (.getInputStream zip (.getEntry zip entry-name))]
    (slurp in)))

(defn- write-zip!
  [archive-path entries]
  (with-open [fos (FileOutputStream. ^String archive-path)
              out (-> fos BufferedOutputStream. ZipOutputStream.)]
    (doseq [[entry-name data] entries]
      (.putNextEntry out (ZipEntry. ^String entry-name))
      (when (some? data)
        (.write out (.getBytes ^String data StandardCharsets/UTF_8)))
      (.closeEntry out))))

(defn- encode-key [byte-value]
  (.encodeToString (Base64/getEncoder)
                   (byte-array (repeat 32 (byte byte-value)))))

(defn- path-of
  [path]
  (Paths/get path (make-array String 0)))

(defn- maybe-set-owner-only-perms!
  [path]
  (try
    (Files/getPosixFilePermissions (path-of path) (make-array LinkOption 0))
    (Files/setPosixFilePermissions (path-of path)
                                   (PosixFilePermissions/fromString "rw-------"))
    (catch UnsupportedOperationException _)
    (catch Exception _)))

(deftest pack-includes-db-and-salt-for-passphrase-mode
  (let [dir         (temp-dir)
        db-path     (str dir "/db")
        archive     (str dir "/backup.xia")]
    (db/connect! db-path (th/test-connect-options
                           {:passphrase-provider (constantly "pack-passphrase")}))
    (try
      (db/set-config! :token/github "gh-secret")
      (finally
        (db/close!)))
    (let [result   (with-redefs-fn {#'xia.pack/env-value (constantly nil)}
                     #(pack/pack! db-path archive))
          entries  (zip-entry-names archive)
          manifest (edn/read-string (read-zip-entry archive "manifest.edn"))]
      (is (= (.getAbsolutePath (java.io.File. ^String archive)) (:archive result)))
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
    (maybe-set-owner-only-perms! key-file)
    (with-redefs-fn {#'xia.crypto/env-value (fn [k] (get key-file-env k))}
      #(do
         (db/connect! db-path (th/test-connect-options))
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
      (is (= ["Move db/.xia/master.key to a secure path outside the extracted DB, set owner-only permissions, then set XIA_MASTER_KEY_FILE to that path."]
             (:restore-requires manifest))))))

(deftest pack-default-archive-path
  (is (= "/tmp/xia-db.xia"
         (pack/default-archive-path "/tmp/xia-db"))))

(deftest open-archive-restores-db-layout
  (let [dir      (temp-dir)
        db-path  (str dir "/db")
        archive  (str dir "/backup.xia")]
    (db/connect! db-path (th/test-connect-options
                           {:passphrase-provider (constantly "open-passphrase")}))
    (try
      (db/set-config! :token/github "gh-open-secret")
      (finally
        (db/close!)))
    (with-redefs-fn {#'xia.pack/env-value (constantly nil)}
      #(let [result (pack/pack! db-path archive)
             opened (pack/open-archive! archive)]
         (is (= (:archive result) (:archive-path opened)))
         (is (:refreshed? opened))
         (is (.exists (java.io.File. ^String (:db-path opened))))
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
    (maybe-set-owner-only-perms! key-file)
    (with-redefs-fn {#'xia.crypto/env-value (fn [k] (get key-file-env k))}
      #(do
         (db/connect! db-path (th/test-connect-options))
         (try
           (db/set-config! :user/name "Archive Restore")
           (finally
             (db/close!)))))
    (with-redefs-fn {#'xia.pack/env-value (fn [k] (get key-file-env k))}
      #(pack/pack! db-path archive))
    (let [opened (with-redefs-fn {#'xia.pack/env-value (constantly nil)}
                   #(pack/open-archive! archive))
          key-path (str (:db-path opened) "/.xia/master.key")]
      (is (= {:key-file key-path
              :allow-insecure-key-file? true}
             (:crypto-opts opened)))
      (db/connect! (:db-path opened)
                   (th/test-connect-options (:crypto-opts opened)))
      (try
        (is (= "Archive Restore" (db/get-config :user/name)))
        (finally
          (db/close!))))))

(deftest unpack-rejects-symlinked-path-components
  (let [dir         (temp-dir)
        archive     (str dir "/backup.xia")
        dest-root   (str dir "/dest")
        outside-dir (str dir "/outside")
        linked-db   (str dest-root "/db")]
    (Files/createDirectories (path-of dest-root)
                             (into-array FileAttribute []))
    (Files/createDirectories (path-of outside-dir)
                             (into-array FileAttribute []))
    (write-zip! archive {"db/data.mdb" "evil"
                         "manifest.edn" "{:format :xia-pack/v1}"})
    (try
      (Files/createSymbolicLink (path-of linked-db)
                                (path-of outside-dir)
                                (into-array FileAttribute []))
      (let [ex (try
                 (pack/unpack! archive dest-root)
                 nil
                 (catch clojure.lang.ExceptionInfo e
                   e))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        (is (= "Archive entry resolves through a symbolic link"
               (.getMessage ^clojure.lang.ExceptionInfo ex)))
        (is (= "db/data.mdb" (:entry (ex-data ex))))
        (is (= linked-db (:path (ex-data ex))))
        (is (not (.exists (io/file (str outside-dir "/data.mdb"))))))
      (catch UnsupportedOperationException _
        (testing "symbolic links unsupported on this platform"
          (is true)))
      (catch FileSystemException _
        (testing "symbolic links unavailable in this environment"
          (is true))))))

(deftest public-manifest-entry-points-reject-reader-eval
  (let [dir     (temp-dir)
        archive (str dir "/evil.xia")]
    (write-zip! archive {"manifest.edn" "#=(+ 1 2)"})
    (is (thrown-with-msg?
          RuntimeException
          #"No dispatch macro for: ="
          (pack/read-manifest archive)))
    (is (thrown-with-msg?
          RuntimeException
          #"No dispatch macro for: ="
          (pack/open-archive! archive)))))
