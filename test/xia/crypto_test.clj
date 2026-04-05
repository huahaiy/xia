(ns xia.crypto-test
  (:require [clojure.test :refer :all]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.test-helpers :as th])
  (:import [java.nio.file Files LinkOption Paths]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]))

(defn- temp-db-path []
  (str (Files/createTempDirectory "xia-crypto-test"
         (into-array FileAttribute []))
       "/db"))

(defn- bytes=
  [a b]
  (= (seq a) (seq b)))

(defn- encode-key
  [byte-value]
  (.encodeToString (java.util.Base64/getEncoder)
                   (byte-array (repeat 32 (byte byte-value)))))

(defn- path-of
  [path]
  (Paths/get path (make-array String 0)))

(defn- posix-supported?
  [path]
  (try
    (Files/getPosixFilePermissions (path-of path) (make-array LinkOption 0))
    true
    (catch UnsupportedOperationException _
      false)))

(defn- set-posix-perms!
  [path perms]
  (Files/setPosixFilePermissions (path-of path)
                                 (PosixFilePermissions/fromString perms)))

(defn- maybe-set-owner-only-perms!
  [path]
  (when (posix-supported? path)
    (set-posix-perms! path "rw-------")))

(deftest configure-derives-stable-key-from-passphrase-provider
  (let [db-path   (temp-db-path)
        calls     (atom [])
        provider  (fn [{:keys [new?]}]
                    (swap! calls conj new?)
                    "correct horse battery staple")
        key-1     (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
                    #(crypto/configure! db-path {:passphrase-provider provider}))
        source-1  (crypto/current-key-source)
        key-2     (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
                    #(crypto/configure! db-path {:passphrase-provider provider}))
        source-2  (crypto/current-key-source)]
    (is (bytes= key-1 key-2))
    (is (= [true false] @calls))
    (is (= :prompt-passphrase (:source source-1)))
    (is (= :prompt-passphrase (:source source-2)))
    (is (some? (:salt-path source-2)))
    (is (.contains ^String (:salt-path source-2) "/db/.xia/master.salt"))
    (is (.exists (.toFile (java.nio.file.Paths/get (:salt-path source-2)
                                                   (make-array String 0)))))
    (is (not (.exists (.toFile (java.nio.file.Paths/get (str db-path ".key")
                                                        (make-array String 0))))))))

(deftest different-passphrases-derive-different-keys
  (let [db-path (temp-db-path)
        key-1   (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
                  #(crypto/configure! db-path
                                      {:passphrase-provider (constantly "alpha")}))]
    (let [key-2 (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
                  #(crypto/configure! db-path
                                      {:passphrase-provider (constantly "beta")}))]
      (is (not (bytes= key-1 key-2))))))

(deftest env-passphrase-does-not-call-passphrase-provider
  (let [db-path   (temp-db-path)
        calls     (atom 0)
        key-env   {"XIA_MASTER_PASSPHRASE" "env-passphrase"}
        provider  (fn [_]
                    (swap! calls inc)
                    "provider-passphrase")]
    (with-redefs-fn {#'xia.crypto/env-value (fn [k] (get key-env k))}
      #(do
         (crypto/configure! db-path {:passphrase-provider provider})
         (is (= :env-passphrase (:source (crypto/current-key-source))))
         (is (zero? @calls))))))

(deftest blank-passphrase-provider-is-rejected-at-provider-boundary
  (let [db-path (temp-db-path)]
    (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
      #(let [ex (try
                  (crypto/configure! db-path {:passphrase-provider (constantly "   ")})
                  nil
                  (catch clojure.lang.ExceptionInfo e
                    e))]
          (is (instance? clojure.lang.ExceptionInfo ex))
          (is (= "Master passphrase provider returned a blank value"
                 (.getMessage ^clojure.lang.ExceptionInfo ex)))
          (is (= :passphrase-provider (:source (ex-data ex))))))))

(deftest db-connect-accepts-crypto-options
  (let [db-path (temp-db-path)]
    (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
      #(do
         (db/connect! db-path (th/test-connect-options
                                {:passphrase-provider (constantly "db-passphrase")}))
         (try
           (is (= :prompt-passphrase (:source (crypto/current-key-source))))
           (finally
             (db/close!)))))))

(deftest configure-accepts-explicit-key-file
  (let [db-path  (temp-db-path)
        key-file (str (Files/createTempFile "xia-crypto-key" ".txt"
                          (into-array FileAttribute [])))]
    (spit key-file (encode-key 5))
    (maybe-set-owner-only-perms! key-file)
    (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
      #(do
         (crypto/configure! db-path {:key-file key-file})
         (is (= :key-file (:source (crypto/current-key-source))))
         (is (= key-file (:path (crypto/current-key-source))))))))

(deftest configure-rejects-key-file-inside-db-path-by-default
  (let [db-path     (temp-db-path)
        support-dir (str db-path "/.xia")
        key-file    (str support-dir "/master.key")]
    (Files/createDirectories (path-of support-dir)
                             (into-array FileAttribute []))
    (spit key-file (encode-key 6))
    (maybe-set-owner-only-perms! key-file)
    (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
      #(let [ex (try
                  (crypto/configure! db-path {:key-file key-file})
                  nil
                  (catch clojure.lang.ExceptionInfo e
                    e))]
          (is (instance? clojure.lang.ExceptionInfo ex))
          (is (= :inside-db-path (:reason (ex-data ex))))))))

(deftest configure-rejects-insecure-key-file-permissions
  (let [db-path  (temp-db-path)
        key-file (str (Files/createTempFile "xia-crypto-key" ".txt"
                          (into-array FileAttribute [])))]
    (spit key-file (encode-key 7))
    (if (posix-supported? key-file)
      (do
        (set-posix-perms! key-file "rw-r-----")
        (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
          #(let [ex (try
                      (crypto/configure! db-path {:key-file key-file})
                      nil
                      (catch clojure.lang.ExceptionInfo e
                        e))]
              (is (instance? clojure.lang.ExceptionInfo ex))
              (is (= :insecure-permissions (:reason (ex-data ex))))
              (is (= "rw-r-----" (:permissions (ex-data ex)))))))
      (testing "POSIX permissions unavailable"
        (is true)))))

(deftest configure-allows-explicit-override-for-db-local-key-file
  (let [db-path     (temp-db-path)
        support-dir (str db-path "/.xia")
        key-file    (str support-dir "/master.key")]
    (Files/createDirectories (path-of support-dir)
                             (into-array FileAttribute []))
    (spit key-file (encode-key 8))
    (maybe-set-owner-only-perms! key-file)
    (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
      #(do
         (crypto/configure! db-path {:key-file key-file
                                     :allow-insecure-key-file? true})
         (is (= :key-file (:source (crypto/current-key-source))))
         (is (= key-file (:path (crypto/current-key-source))))))))

(deftest configure-allows-key-file-when-posix-perms-cannot-be-verified
  (let [db-path  (temp-db-path)
        key-file (str (Files/createTempFile "xia-crypto-key" ".txt"
                          (into-array FileAttribute [])))]
    (spit key-file (encode-key 9))
    (with-redefs-fn {#'xia.crypto/get-posix-file-permissions
                     (fn [_]
                       (throw (UnsupportedOperationException. "no posix")))
                     #'xia.crypto/env-value (constantly nil)}
      #(do
         (crypto/configure! db-path {:key-file key-file})
         (is (= :key-file (:source (crypto/current-key-source))))
         (is (= key-file (:path (crypto/current-key-source))))))))

(deftest configure-derives-and-reuses-passphrase-key-when-owner-only-perms-cannot-be-set
  (let [db-path  (temp-db-path)
        provider (constantly "portable-passphrase")]
    (with-redefs-fn {#'xia.crypto/env-value (constantly nil)
                     #'xia.crypto/set-posix-file-permissions!
                     (fn [_ _]
                       (throw (UnsupportedOperationException. "no posix")))}
      (fn []
        (let [key-1     (crypto/configure! db-path {:passphrase-provider provider})
              source-1  (crypto/current-key-source)
              key-2     (crypto/configure! db-path {:passphrase-provider provider})
              source-2  (crypto/current-key-source)
              salt-file (java.io.File. ^String (:salt-path source-2))
              key-file  (java.io.File. ^String (str db-path ".key"))]
          (is (bytes= key-1 key-2))
          (is (= :prompt-passphrase (:source source-1)))
          (is (= :prompt-passphrase (:source source-2)))
          (is (.exists salt-file))
          (is (not (.exists key-file))))))))
