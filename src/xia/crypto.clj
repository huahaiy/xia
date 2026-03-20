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
           [java.nio.file.attribute PosixFilePermission PosixFilePermissions]
           [java.security.spec KeySpec]
           [java.security SecureRandom]
           [java.util Arrays Base64 Base64$Decoder Base64$Encoder]
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

(defn- ^Base64$Decoder decoder []
  (Base64/getDecoder))

(defn- ^Base64$Encoder encoder []
  (Base64/getEncoder))

(defn- decode-key [encoded]
  (let [trimmed (str/trim (or encoded ""))]
    (when (str/blank? trimmed)
      (throw (ex-info "Encryption key is blank" {})))
    (let [^bytes key (.decode ^Base64$Decoder (decoder) ^String trimmed)]
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

(def ^:private disallowed-secret-file-perms
  #{PosixFilePermission/OWNER_EXECUTE
    PosixFilePermission/GROUP_READ
    PosixFilePermission/GROUP_WRITE
    PosixFilePermission/GROUP_EXECUTE
    PosixFilePermission/OTHERS_READ
    PosixFilePermission/OTHERS_WRITE
    PosixFilePermission/OTHERS_EXECUTE})

(defn- ^Path path-of [path]
  (Paths/get path (make-array String 0)))

(defn- absolute-path [^Path path]
  (.normalize (.toAbsolutePath path)))

(defn- real-path [^Path path]
  (try
    (.toRealPath path (make-array LinkOption 0))
    (catch Exception _
      (absolute-path path))))

(defn- path-within?
  [^Path child ^Path parent]
  (.startsWith child parent))

(defn- file-under-db-path?
  [db-path file-path]
  (let [db-path*   (path-of db-path)
        file-path* (path-of file-path)]
    (or (path-within? (absolute-path file-path*) (absolute-path db-path*))
        (path-within? (real-path file-path*) (real-path db-path*)))))

(defn- insecure-secret-file-perms
  [^Path path]
  (try
    (let [perms     (Files/getPosixFilePermissions path (make-array LinkOption 0))
          insecure? (some disallowed-secret-file-perms perms)]
      (when insecure?
        {:permissions (PosixFilePermissions/toString perms)}))
    (catch UnsupportedOperationException _
      nil)))

(defn- validate-secret-file!
  [db-path file-path label allow-insecure-key-file?]
  (let [path (path-of file-path)]
    (when-not (Files/exists path (make-array LinkOption 0))
      (throw (ex-info (str label " does not exist") {:path file-path})))
    (when-not allow-insecure-key-file?
      (when (file-under-db-path? db-path file-path)
        (throw (ex-info
                 (str label " must not be stored inside the DB path unless "
                      ":allow-insecure-key-file? is true")
                 {:path file-path
                  :db-path db-path
                  :reason :inside-db-path})))
      (when-let [{:keys [permissions]} (insecure-secret-file-perms path)]
        (throw (ex-info
                 (str label " must use owner-only permissions unless "
                      ":allow-insecure-key-file? is true")
                 {:path file-path
                  :permissions permissions
                  :reason :insecure-permissions}))))
    path))

(defn- read-key-file [db-path file-path allow-insecure-key-file?]
  (let [^Path path (validate-secret-file! db-path file-path "Encryption key file"
                                          allow-insecure-key-file?)]
    (decode-key (slurp (str path)))))

(defn- decode-bytes [encoded expected-length label]
  (let [trimmed (str/trim (or encoded ""))]
    (when (str/blank? trimmed)
      (throw (ex-info (str label " is blank") {})))
    (let [^bytes bytes (.decode ^Base64$Decoder (decoder) ^String trimmed)]
      (when-not (= expected-length (alength bytes))
        (throw (ex-info (str label " must decode to " expected-length " bytes") {})))
      bytes)))

(defn- read-salt-file [file-path]
  (let [^Path path (Paths/get file-path (make-array String 0))]
    (when-not (Files/exists path (make-array LinkOption 0))
      (throw (ex-info "Encryption salt file does not exist" {:path file-path})))
    (decode-bytes (slurp (str path)) salt-bytes "Encryption salt")))

(defn- strip-trailing-newline [s]
  (str/replace (or s "") #"(?:\r?\n)+\z" ""))

(defn- read-passphrase-file [db-path file-path allow-insecure-key-file?]
  (let [^Path path (validate-secret-file! db-path file-path "Master passphrase file"
                                          allow-insecure-key-file?)]
    (strip-trailing-newline (slurp (str path)))))

(defn- generate-salt []
  (let [^bytes bytes (byte-array salt-bytes)]
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
  (let [^Path path (Paths/get file-path (make-array String 0))]
    (when-not (Files/exists path (make-array LinkOption 0))
      (migrate-legacy-salt! db-path file-path))
    (ensure-parent-dir! path)
    (if (Files/exists path (make-array LinkOption 0))
      (do
        (set-owner-only-perms! path)
        (read-salt-file file-path))
      (let [^bytes salt (generate-salt)
            encoded     (.encodeToString ^Base64$Encoder (encoder) salt)]
        (spit (str path) encoded)
        (set-owner-only-perms! path)
        salt))))

(defn- derive-key [passphrase salt]
  (when (str/blank? (or passphrase ""))
    (throw (ex-info "Master passphrase is blank" {})))
  (let [chars   (.toCharArray ^String passphrase)
        factory (SecretKeyFactory/getInstance "PBKDF2WithHmacSHA256")
        ^PBEKeySpec spec (PBEKeySpec. chars salt pbkdf2-iterations (* 8 (long key-bytes)))]
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
        (let [passphrase (str value)]
          (when (str/blank? passphrase)
            (throw (ex-info "Master passphrase provider returned a blank value"
                            {:source :passphrase-provider})))
          passphrase)))))

(defn- resolve-key
  ([db-path] (resolve-key db-path nil))
  ([db-path {:keys [key-file
                    passphrase
                    passphrase-file
                    passphrase-provider
                    allow-insecure-key-file?]}]
   (let [env-key              (env-value "XIA_MASTER_KEY")
         env-key-file         (env-value "XIA_MASTER_KEY_FILE")
         env-passphrase       (env-value "XIA_MASTER_PASSPHRASE")
         env-passphrase-file  (env-value "XIA_MASTER_PASSPHRASE_FILE")
         salt-path            (default-salt-path db-path)
         salt-exists?         (or (.exists (.toFile (Paths/get salt-path (make-array String 0))))
                                  (.exists (.toFile (Paths/get (legacy-salt-path db-path) (make-array String 0)))))
         provider-ctx         {:db-path   db-path
                               :salt-path salt-path
                               :new?      (not salt-exists?)}]
    (cond
      (some? key-file)
      {:key (read-key-file db-path key-file allow-insecure-key-file?)
       :source :key-file
       :path key-file}

      (some? passphrase)
      (passphrase-key db-path passphrase :passphrase)

      (some? passphrase-file)
      (assoc (passphrase-key db-path (read-passphrase-file db-path
                                                           passphrase-file
                                                           allow-insecure-key-file?)
                             :passphrase-file)
             :path passphrase-file)

      (seq env-key)
      {:key (decode-key env-key)
       :source :env}

      (seq env-key-file)
      {:key (read-key-file db-path env-key-file allow-insecure-key-file?)
       :source :env-file
       :path env-key-file}

      (seq env-passphrase)
      (passphrase-key db-path env-passphrase :env-passphrase)

      (seq env-passphrase-file)
      (assoc (passphrase-key db-path (read-passphrase-file db-path
                                                           env-passphrase-file
                                                           allow-insecure-key-file?)
                             :env-passphrase-file)
             :path env-passphrase-file)

      :else
      (if-let [provider-passphrase (provider-passphrase passphrase-provider provider-ctx)]
        (passphrase-key db-path provider-passphrase :prompt-passphrase)
        (throw (ex-info "No master key or passphrase available"
                        {:supported-sources [:key-file
                                             :passphrase
                                             :passphrase-file
                                             :XIA_MASTER_KEY
                                             :XIA_MASTER_KEY_FILE
                                             :XIA_MASTER_PASSPHRASE
                                             :XIA_MASTER_PASSPHRASE_FILE
                                             :passphrase-provider]
                         :db-path db-path})))))))

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
    (let [^bytes iv                     (byte-array iv-bytes)
          _                             (.nextBytes (SecureRandom.) iv)
          ^Cipher cipher                (Cipher/getInstance "AES/GCM/NoPadding")
          key                           (SecretKeySpec. (configured-key) "AES")
          params                        (GCMParameterSpec. 128 iv)
          ^bytes aad-bytes              (utf8-bytes (str aad))
          ^bytes plain                  (utf8-bytes value)
          ^Base64$Encoder base64-encoder (encoder)]
      (.init cipher Cipher/ENCRYPT_MODE key params)
      (.updateAAD cipher aad-bytes)
      (str envelope-prefix
           (.encodeToString base64-encoder iv)
           ":"
           (.encodeToString base64-encoder (.doFinal cipher plain))))))

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
      (let [^Cipher cipher       (Cipher/getInstance "AES/GCM/NoPadding")
            key                  (SecretKeySpec. (configured-key) "AES")
            ^bytes iv            (.decode ^Base64$Decoder (decoder) ^String iv-b64)
            ^bytes data          (.decode ^Base64$Decoder (decoder) ^String data-b64)
            params               (GCMParameterSpec. 128 iv)
            ^bytes aad-bytes     (utf8-bytes (str aad))]
        (.init cipher Cipher/DECRYPT_MODE key params)
        (.updateAAD cipher aad-bytes)
        (String. (.doFinal cipher data) StandardCharsets/UTF_8)))))
