(ns xia.crypto-test
  (:require [clojure.test :refer :all]
            [xia.crypto :as crypto]
            [xia.db :as db])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

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

(deftest db-connect-accepts-crypto-options
  (let [db-path (temp-db-path)]
    (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
      #(do
         (db/connect! db-path {:passphrase-provider (constantly "db-passphrase")})
         (try
           (is (= :prompt-passphrase (:source (crypto/current-key-source))))
           (finally
             (db/close!)))))))

(deftest configure-accepts-explicit-key-file
  (let [db-path  (temp-db-path)
        key-file (str (Files/createTempFile "xia-crypto-key" ".txt"
                          (into-array FileAttribute [])))]
    (spit key-file (encode-key 5))
    (with-redefs-fn {#'xia.crypto/env-value (constantly nil)}
      #(do
         (crypto/configure! db-path {:key-file key-file})
         (is (= :key-file (:source (crypto/current-key-source))))
         (is (= key-file (:path (crypto/current-key-source))))))))
