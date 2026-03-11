(ns xia.crypto
  "Authenticated encryption for sensitive values stored outside the DB keyspace.

   Key loading order:
   1. XIA_MASTER_KEY      — base64-encoded 32-byte key
   2. XIA_MASTER_KEY_FILE — path to a file containing the same base64 key
   3. XIA_MASTER_PASSPHRASE / XIA_MASTER_PASSPHRASE_FILE — derive with PBKDF2
   4. Interactive passphrase provider — derive with PBKDF2 for new startups"
  (:require [clojure.string :as str])
  (:import [java.nio.charset StandardCharsets]
           [java.nio.file Files LinkOption Path Paths StandardCopyOption]
           [java.nio.file.attribute PosixFilePermissions]
           [java.security.spec KeySpec]
           [java.security SecureRandom]
           [java.util Arrays Base64]
           [javax.crypto SecretKeyFactory Cipher]
           [javax.crypto.spec GCMParameterSpec PBEKeySpec SecretKeySpec]))

(def ^:private envelope-prefix "enc:v1:")
(def ^:private key-bytes 32)
(def ^:private iv-bytes 12)
(def ^:private salt-bytes 16)
(def ^:private pbkdf2-iterations 210000)
(def ^:private support-dir-name ".xia")
(def ^:private salt-file-name "master.salt")
(def ^:private key-state (atom nil))

(defn- utf8-bytes [s]
  (.getBytes ^String s StandardCharsets/UTF_8))

(defn- decoder []
  (Base64/getDecoder))

(defn- encoder []
  (Base64/getEncoder))

(defn- decode-key [encoded]
  (let [trimmed (str/trim (or encoded ""))]
    (when (str/blank? trimmed)
      (throw (ex-info "Encryption key is blank" {})))
    (let [key (.decode (decoder) trimmed)]
      (when-not (= key-bytes (alength key))
        (throw (ex-info "Encryption key must decode to 32 bytes" {})))
      key)))

(defn- support-dir-path [db-path]
  (str (Paths/get db-path (into-array String [support-dir-name]))))

(defn- default-salt-path [db-path]
  (str (Paths/get db-path (into-array String [support-dir-name salt-file-name]))))

(defn- legacy-salt-path [db-path]
  (str db-path ".salt"))

(defn- env-value [k]
  (System/getenv k))

(defn- ensure-parent-dir! [^Path path]
  (when-let [parent (.getParent path)]
    (Files/createDirectories parent (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- set-owner-only-perms! [^Path path]
  (try
    (Files/setPosixFilePermissions
      path
      (PosixFilePermissions/fromString "rw-------"))
    (catch UnsupportedOperationException _)
    (catch Exception _)))

(defn- read-key-file [file-path]
  (let [path (Paths/get file-path (make-array String 0))]
    (when-not (Files/exists path (make-array LinkOption 0))
      (throw (ex-info "Encryption key file does not exist" {:path file-path})))
    (decode-key (slurp (.toFile path)))))

(defn- decode-bytes [encoded expected-length label]
  (let [trimmed (str/trim (or encoded ""))]
    (when (str/blank? trimmed)
      (throw (ex-info (str label " is blank") {})))
    (let [bytes (.decode (decoder) trimmed)]
      (when-not (= expected-length (alength bytes))
        (throw (ex-info (str label " must decode to " expected-length " bytes") {})))
      bytes)))

(defn- read-salt-file [file-path]
  (let [path (Paths/get file-path (make-array String 0))]
    (when-not (Files/exists path (make-array LinkOption 0))
      (throw (ex-info "Encryption salt file does not exist" {:path file-path})))
    (decode-bytes (slurp (.toFile path)) salt-bytes "Encryption salt")))

(defn- strip-trailing-newline [s]
  (str/replace (or s "") #"(?:\r?\n)+\z" ""))

(defn- read-passphrase-file [file-path]
  (let [path (Paths/get file-path (make-array String 0))]
    (when-not (Files/exists path (make-array LinkOption 0))
      (throw (ex-info "Master passphrase file does not exist" {:path file-path})))
    (strip-trailing-newline (slurp (.toFile path)))))

(defn- generate-salt []
  (let [bytes (byte-array salt-bytes)]
    (.nextBytes (SecureRandom.) bytes)
    bytes))

(defn- migrate-legacy-salt!
  [db-path file-path]
  (let [legacy-path (Paths/get (legacy-salt-path db-path) (make-array String 0))
        target-path (Paths/get file-path (make-array String 0))]
    (when (Files/exists legacy-path (make-array LinkOption 0))
      (ensure-parent-dir! target-path)
      (Files/move legacy-path target-path
                  (into-array StandardCopyOption [StandardCopyOption/REPLACE_EXISTING]))
      (set-owner-only-perms! target-path))))

(defn- ensure-salt-file! [db-path file-path]
  (let [path (Paths/get file-path (make-array String 0))]
    (when-not (Files/exists path (make-array LinkOption 0))
      (migrate-legacy-salt! db-path file-path))
    (ensure-parent-dir! path)
    (if (Files/exists path (make-array LinkOption 0))
      (do
        (set-owner-only-perms! path)
        (read-salt-file file-path))
      (let [salt    (generate-salt)
            encoded (.encodeToString (encoder) salt)]
        (spit (.toFile path) encoded)
        (set-owner-only-perms! path)
        salt))))

(defn- derive-key [passphrase salt]
  (when (str/blank? (or passphrase ""))
    (throw (ex-info "Master passphrase is blank" {})))
  (let [chars   (.toCharArray ^String passphrase)
        factory (SecretKeyFactory/getInstance "PBKDF2WithHmacSHA256")
        ^PBEKeySpec spec (PBEKeySpec. chars salt pbkdf2-iterations (* 8 key-bytes))]
    (try
      (.getEncoded (.generateSecret factory ^KeySpec spec))
      (finally
        (.clearPassword spec)
        (Arrays/fill chars (char 0))))))

(defn- passphrase-key
  [db-path passphrase source]
  (let [salt-path (default-salt-path db-path)
        existed?  (or (.exists (.toFile (Paths/get salt-path (make-array String 0))))
                      (.exists (.toFile (Paths/get (legacy-salt-path db-path) (make-array String 0)))))
        salt      (ensure-salt-file! db-path salt-path)]
    {:key       (derive-key passphrase salt)
     :source    source
     :salt-path salt-path
     :new?      (not existed?)}))

(defn- provider-passphrase
  [provider ctx]
  (when provider
    (let [value (provider ctx)]
      (when (some? value)
        (str value)))))

(defn- resolve-key
  ([db-path] (resolve-key db-path nil))
  ([db-path {:keys [key-file passphrase passphrase-file passphrase-provider]}]
   (let [env-key              (env-value "XIA_MASTER_KEY")
         env-key-file         (env-value "XIA_MASTER_KEY_FILE")
         env-passphrase       (env-value "XIA_MASTER_PASSPHRASE")
         env-passphrase-file  (env-value "XIA_MASTER_PASSPHRASE_FILE")
         salt-path            (default-salt-path db-path)
         salt-exists?         (or (.exists (.toFile (Paths/get salt-path (make-array String 0))))
                                  (.exists (.toFile (Paths/get (legacy-salt-path db-path) (make-array String 0)))))
         provider-passphrase  (provider-passphrase
                                passphrase-provider
                                {:db-path   db-path
                                 :salt-path salt-path
                                 :new?      (not salt-exists?)})]
    (cond
      (some? key-file)
      {:key (read-key-file key-file)
       :source :key-file
       :path key-file}

      (some? passphrase)
      (passphrase-key db-path passphrase :passphrase)

      (some? passphrase-file)
      (assoc (passphrase-key db-path (read-passphrase-file passphrase-file)
                             :passphrase-file)
             :path passphrase-file)

      (seq env-key)
      {:key (decode-key env-key)
       :source :env}

      (seq env-key-file)
      {:key (read-key-file env-key-file)
       :source :env-file
       :path env-key-file}

      (seq env-passphrase)
      (passphrase-key db-path env-passphrase :env-passphrase)

      (seq env-passphrase-file)
      (assoc (passphrase-key db-path (read-passphrase-file env-passphrase-file)
                             :env-passphrase-file)
             :path env-passphrase-file)

      (seq provider-passphrase)
      (passphrase-key db-path provider-passphrase :prompt-passphrase)

      :else
      (throw (ex-info "No master key or passphrase available"
                      {:supported-sources [:key-file
                                           :passphrase
                                           :passphrase-file
                                           :XIA_MASTER_KEY
                                           :XIA_MASTER_KEY_FILE
                                           :XIA_MASTER_PASSPHRASE
                                           :XIA_MASTER_PASSPHRASE_FILE
                                           :passphrase-provider]
                       :db-path db-path}))))))

(defn configure!
  "Load or create the encryption key for the given DB path."
  ([db-path] (configure! db-path nil))
  ([db-path opts]
   (let [{:keys [key] :as resolved} (resolve-key db-path opts)]
    (reset! key-state (assoc resolved :db-path db-path))
    key)))

(defn current-key-source []
  (select-keys @key-state [:source :path :salt-path :db-path]))

(defn- configured-key []
  (or (:key @key-state)
      (throw (ex-info "Encryption not configured. Call xia.crypto/configure! first." {}))))

(defn encrypted?
  [value]
  (and (string? value) (str/starts-with? value envelope-prefix)))

(defn encrypt
  "Encrypt a string with AES-GCM. Blank strings are returned as-is."
  [value aad]
  (cond
    (nil? value) nil
    (not (string? value)) (str value)
    (str/blank? value) value
    (encrypted? value) value
    :else
    (let [iv      (byte-array iv-bytes)
          _       (.nextBytes (SecureRandom.) iv)
          cipher  (Cipher/getInstance "AES/GCM/NoPadding")
          key     (SecretKeySpec. (configured-key) "AES")
          params  (GCMParameterSpec. 128 iv)
          aad     (utf8-bytes (str aad))
          plain   (utf8-bytes value)]
      (.init cipher Cipher/ENCRYPT_MODE key params)
      (.updateAAD cipher aad)
      (str envelope-prefix
           (.encodeToString (encoder) iv)
           ":"
           (.encodeToString (encoder) (.doFinal cipher plain))))))

(defn decrypt
  "Decrypt a value if it is an encrypted envelope, otherwise return it unchanged."
  [value aad]
  (cond
    (nil? value) nil
    (not (string? value)) value
    (not (encrypted? value)) value
    :else
    (let [payload          (subs value (count envelope-prefix))
          [iv-b64 data-b64] (str/split payload #":" 2)]
      (when (or (str/blank? iv-b64) (str/blank? data-b64))
        (throw (ex-info "Malformed encrypted value" {:value value})))
      (let [cipher (Cipher/getInstance "AES/GCM/NoPadding")
            key    (SecretKeySpec. (configured-key) "AES")
            iv     (.decode (decoder) iv-b64)
            data   (.decode (decoder) data-b64)
            params (GCMParameterSpec. 128 iv)]
        (.init cipher Cipher/DECRYPT_MODE key params)
        (.updateAAD cipher (utf8-bytes (str aad)))
        (String. (.doFinal cipher data) StandardCharsets/UTF_8)))))
