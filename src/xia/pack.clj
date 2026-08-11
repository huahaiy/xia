(ns xia.pack
  "Portable archive packaging for Xia state."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io BufferedInputStream BufferedOutputStream ByteArrayOutputStream
            File FileInputStream FileOutputStream InputStream]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files LinkOption OpenOption Path Paths StandardOpenOption]
           [java.time Instant]
           [java.util.zip ZipEntry ZipFile ZipOutputStream]))

(def ^:private manifest-entry "manifest.edn")
(def ^:private buffer-size 8192)
(def ^:private archive-extension ".xia")
(def ^:private support-dir-name ".xia")
(def ^:private support-dir-entry "db/.xia")
(def ^:private default-max-archive-entries 100000)
(def ^:private default-max-archive-entry-bytes (* 8 1024 1024 1024))
(def ^:private default-max-archive-expanded-bytes (* 16 1024 1024 1024))
(def ^:private max-manifest-bytes (* 1024 1024))

(defn- epoch-millis->date
  [millis]
  (java.util.Date. (long millis)))

(defn- env-value [k]
  (System/getenv k))

(defn archive-path?
  [path]
  (and (string? path)
       (str/ends-with? (str/lower-case path) archive-extension)))

(defn- salt-path [db-path]
  (str (Paths/get db-path (into-array String [support-dir-name "master.salt"]))))

(defn- support-file
  [db-path filename]
  (io/file (str (Paths/get db-path (into-array String [support-dir-name filename])))))

(defn- support-entry [filename]
  (str support-dir-entry "/" filename))

(defn- normalize-entry-name [s]
  (.replace ^String s File/separator "/"))

(defn- db-root-entry [^File root ^File file]
  (if (.isDirectory root)
    (str "db/" (normalize-entry-name
                (str (.relativize (.toPath root) (.toPath file)))))
    "db"))

(defn- db-entries [db-path]
  (let [root (io/file db-path)]
    (when-not (.exists root)
      (throw (ex-info "Database path does not exist" {:db-path db-path})))
    (->> (if (.isDirectory root) (file-seq root) [root])
         (filter #(.isFile ^File %))
         (remove #(= "lock.mdb" (.getName ^File %)))
         (mapv (fn [^File file]
                 {:file  file
                  :entry (db-root-entry root file)})))))

(defn- maybe-file [path]
  (when (seq path)
    (let [file (io/file path)]
      (when-not (.exists ^File file)
        (throw (ex-info "Referenced support file does not exist" {:path path})))
      file)))

(defn- key-context [db-path]
  (let [salt-file            (io/file (salt-path db-path))
        local-key-file       (support-file db-path "master.key")
        local-passphrase-file (support-file db-path "master.passphrase")
        env-key              (env-value "XIA_MASTER_KEY")
        env-key-file         (env-value "XIA_MASTER_KEY_FILE")
        env-passphrase       (env-value "XIA_MASTER_PASSPHRASE")
        env-passphrase-file  (env-value "XIA_MASTER_PASSPHRASE_FILE")
        env-key-file*        (maybe-file env-key-file)
        env-passphrase-file* (maybe-file env-passphrase-file)]
    (cond
      (seq env-key)
      {:key-source       :env
       :archive-entries  []
       :restore-requires ["Set XIA_MASTER_KEY to the same base64 32-byte key before opening the archive DB."]}

      env-key-file*
      {:key-source       :env-file
       :archive-entries  [{:file env-key-file* :entry (support-entry "master.key")}]
       :restore-requires ["Move db/.xia/master.key to a secure path outside the extracted DB, set owner-only permissions, then set XIA_MASTER_KEY_FILE to that path."]}

      (seq env-passphrase)
      {:key-source       :env-passphrase
       :archive-entries  []
       :restore-requires ["Provide the same master passphrase at startup or via XIA_MASTER_PASSPHRASE."]}

      env-passphrase-file*
      {:key-source       :env-passphrase-file
       :archive-entries  [{:file env-passphrase-file* :entry (support-entry "master.passphrase")}]
       :restore-requires ["Move db/.xia/master.passphrase to a secure path outside the extracted DB, set owner-only permissions, then set XIA_MASTER_PASSPHRASE_FILE to that path."]}

      (.exists ^File local-key-file)
      {:key-source       :local-key-file
       :archive-entries  []
       :restore-requires ["Open the archive directly with `xia your-archive.xia`, or move db/.xia/master.key to a secure path outside the extracted DB and set XIA_MASTER_KEY_FILE to that path."]}

      (.exists ^File local-passphrase-file)
      {:key-source       :local-passphrase-file
       :archive-entries  []
       :restore-requires ["Open the archive directly with `xia your-archive.xia`, or move db/.xia/master.passphrase to a secure path outside the extracted DB and set XIA_MASTER_PASSPHRASE_FILE to that path."]}

      (.exists salt-file)
      {:key-source       :prompt-passphrase
       :archive-entries  []
       :restore-requires ["Provide the same master passphrase at startup."]}

      :else
      {:key-source       :none
       :archive-entries  []
       :restore-requires []})))

(defn- ensure-output-path! [archive-path force?]
  (let [file (io/file archive-path)]
    (when (and (.exists file) (not force?))
      (throw (ex-info "Archive already exists. Use --force to overwrite."
                      {:archive archive-path})))
    (when-let [parent (.getParentFile file)]
      (.mkdirs parent))
    file))

(defn- write-file-entry!
  [^ZipOutputStream zip {:keys [^File file entry]}]
  (let [zip-entry (doto (ZipEntry. ^String entry)
                    (.setTime (.lastModified file)))]
    (.putNextEntry zip zip-entry)
    (with-open [in (BufferedInputStream. (FileInputStream. file))]
      (let [buffer (byte-array buffer-size)]
        (loop []
          (let [n (.read in buffer)]
            (when (pos? n)
              (.write zip buffer 0 n)
              (recur))))))
    (.closeEntry zip)))

(defn- write-bytes-entry!
  [^ZipOutputStream zip entry data]
  (.putNextEntry zip (ZipEntry. ^String entry))
  (.write zip (.getBytes ^String data StandardCharsets/UTF_8))
  (.closeEntry zip))

(defn default-archive-path
  [db-path]
  (str db-path ".xia"))

(defn default-open-root
  [archive-path]
  (let [archive-file (.getAbsoluteFile (io/file archive-path))
        parent       (or (.getParentFile archive-file)
                         (.getAbsoluteFile (io/file ".")))]
    (.getAbsolutePath
     (io/file parent (str "." (.getName archive-file) ".open")))))

(defn- manifest-path
  [root-path]
  (str (Paths/get root-path (into-array String [manifest-entry]))))

(defn- open-db-path
  [root-path]
  (str (Paths/get root-path (into-array String ["db"]))))

(defn- read-manifest-file
  [root-path]
  (let [manifest-file (io/file (manifest-path root-path))]
    (when-not (.exists ^File manifest-file)
      (throw (ex-info "Archive manifest is missing from extracted archive"
                      {:root root-path
                       :manifest-path (.getAbsolutePath manifest-file)})))
    (edn/read-string (slurp manifest-file))))

(defn- archive-limit-exceeded!
  [limit-name max-value actual-value entry-name]
  (throw (ex-info (str "Archive exceeded " (name limit-name) " limit")
                  (cond-> {:type :archive/limit-exceeded
                           :limit limit-name
                           :max max-value
                           :actual actual-value}
                    entry-name (assoc :entry entry-name)))))

(defn- positive-long-option
  [value option-name]
  (let [parsed (try (long value) (catch Exception _ nil))]
    (when-not (and parsed (pos? parsed))
      (throw (ex-info (str option-name " must be a positive integer")
                      {:option option-name :value value})))
    parsed))

(defn- copy-limited-bytes!
  [^InputStream in max-bytes limit-name entry-name]
  (let [out    (ByteArrayOutputStream.)
        buffer (byte-array buffer-size)]
    (loop [total 0]
      (let [n (.read in buffer)]
        (if (neg? n)
          (.toByteArray out)
          (let [total* (+ (long total) n)]
            (when (> total* (long max-bytes))
              (archive-limit-exceeded! limit-name max-bytes total* entry-name))
            (.write out buffer 0 n)
            (recur total*)))))))

(defn read-manifest
  [archive-path]
  (with-open [zip (ZipFile. (io/file archive-path))]
    (when-let [entry (.getEntry zip manifest-entry)]
      (let [declared-size (.getSize entry)]
        (when (> declared-size max-manifest-bytes)
          (archive-limit-exceeded! :manifest-bytes
                                   max-manifest-bytes
                                   declared-size
                                   manifest-entry))
        (with-open [in (.getInputStream zip entry)]
          (edn/read-string
           (String. ^bytes (copy-limited-bytes! in
                                                max-manifest-bytes
                                                :manifest-bytes
                                                manifest-entry)
                    StandardCharsets/UTF_8)))))))

(defn- delete-tree!
  [path]
  (let [root (io/file path)]
    (when (.exists ^File root)
      (doseq [file (reverse (file-seq root))]
        (Files/deleteIfExists (.toPath ^File file))))))

(defn- root-path
  [root-file]
  (.normalize (.toPath ^File root-file)))

(defn- safe-target-path
  [root-file entry-name]
  (let [^Path root   (root-path root-file)
        ^Path target (.normalize (.resolve root ^String entry-name))]
    (when-not (.startsWith target root)
      (throw (ex-info "Archive entry escapes destination root"
                      {:entry entry-name
                       :root  (.getAbsolutePath ^File root-file)})))
    target))

(defn- path-exists?
  [^Path path]
  (Files/exists path (make-array LinkOption 0)))

(defn- nofollow-link-options []
  (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))

(defn- nofollow-open-options []
  (into-array OpenOption [StandardOpenOption/CREATE
                          StandardOpenOption/TRUNCATE_EXISTING
                          StandardOpenOption/WRITE
                          LinkOption/NOFOLLOW_LINKS]))

(defn- symlink-path-ex
  [^File root-file entry-name ^Path path]
  (ex-info "Archive entry resolves through a symbolic link"
           {:entry entry-name
            :root  (.getAbsolutePath ^File root-file)
            :path  (str path)}))

(defn- ensure-safe-directory-path!
  [^File root-file ^Path path entry-name]
  (let [^Path root     (root-path root-file)
        relative-parts (iterator-seq (.iterator (.relativize root path)))]
    (when (Files/isSymbolicLink root)
      (throw (symlink-path-ex root-file entry-name root)))
    (loop [^Path current root
           [^Path part & remaining] (seq relative-parts)]
      (when part
        (let [^Path next (.resolve current part)]
          (if (path-exists? next)
            (do
              (when (Files/isSymbolicLink next)
                (throw (symlink-path-ex root-file entry-name next)))
              (when-not (Files/isDirectory next (nofollow-link-options))
                (throw (ex-info "Archive entry parent is not a directory"
                                {:entry entry-name
                                 :root  (.getAbsolutePath ^File root-file)
                                 :path  (str next)}))))
            (Files/createDirectory next (make-array java.nio.file.attribute.FileAttribute 0)))
          (recur next remaining))))))

(defn- ensure-safe-file-target!
  [^File root-file ^Path target entry-name]
  (when (and (path-exists? target)
             (Files/isSymbolicLink target))
    (throw (symlink-path-ex root-file entry-name target))))

(defn- account-expanded-bytes!
  [state n limits entry-name entry-bytes]
  (let [entry-bytes* (+ (long entry-bytes) (long n))
        total-bytes* (+ (long (:bytes @state)) (long n))
        max-entry-bytes (long (:max-entry-bytes limits))
        max-expanded-bytes (long (:max-expanded-bytes limits))]
    (when (> entry-bytes* max-entry-bytes)
      (archive-limit-exceeded! :entry-bytes max-entry-bytes entry-bytes* entry-name))
    (when (> total-bytes* max-expanded-bytes)
      (archive-limit-exceeded! :expanded-bytes max-expanded-bytes total-bytes* entry-name))
    (swap! state assoc :bytes total-bytes*)
    entry-bytes*))

(defn- register-archive-entry!
  [state limits ^java.util.zip.ZipEntry entry]
  (let [entry-name (.getName entry)
        entry-count (inc (long (:entries @state)))
        max-entries (long (:max-entries limits))
        declared-size (.getSize entry)]
    (when (> entry-count max-entries)
      (archive-limit-exceeded! :entry-count max-entries entry-count entry-name))
    (when (and (not (.isDirectory entry))
               (not (neg? declared-size)))
      (when (> declared-size (long (:max-entry-bytes limits)))
        (archive-limit-exceeded! :entry-bytes
                                 (:max-entry-bytes limits)
                                 declared-size
                                 entry-name))
      (when (> declared-size
               (- (long (:max-expanded-bytes limits))
                  (long (:bytes @state))))
        (archive-limit-exceeded! :expanded-bytes
                                 (:max-expanded-bytes limits)
                                 (+ (long (:bytes @state)) declared-size)
                                 entry-name)))
    (swap! state assoc :entries entry-count)))

(defn- extract-entry!
  [^ZipFile zip ^File root-file ^java.util.zip.ZipEntry entry limits state]
  (register-archive-entry! state limits entry)
  (let [entry-name (.getName entry)
        ^Path target (safe-target-path root-file entry-name)]
    (when (and (= manifest-entry entry-name)
               (> (.getSize entry) max-manifest-bytes))
      (archive-limit-exceeded! :manifest-bytes
                               max-manifest-bytes
                               (.getSize entry)
                               entry-name))
    (if (.isDirectory entry)
      (ensure-safe-directory-path! root-file target entry-name)
      (do
        (when-let [^Path parent (.getParent target)]
          (ensure-safe-directory-path! root-file parent entry-name))
        (ensure-safe-file-target! root-file target entry-name)
        (with-open [in  (.getInputStream zip entry)
                    out (BufferedOutputStream. (Files/newOutputStream target (nofollow-open-options)))]
          (let [buffer (byte-array buffer-size)]
            (loop [entry-bytes 0]
              (let [n (.read in buffer)]
                (when (pos? n)
                  (let [entry-bytes* (account-expanded-bytes!
                                      state n limits entry-name entry-bytes)]
                    (when (and (= manifest-entry entry-name)
                               (> entry-bytes* max-manifest-bytes))
                      (archive-limit-exceeded! :manifest-bytes
                                               max-manifest-bytes
                                               entry-bytes*
                                               entry-name))
                    (.write out buffer 0 n)
                    (recur (long entry-bytes*))))))))
        (when (pos? (.getTime entry))
          (.setLastModified ^File (.toFile target) (.getTime entry)))))))

(defn unpack!
  "Extract a Xia archive with path and expansion safeguards.

   Defaults allow at most 100,000 entries, 8 GiB per entry, and 16 GiB total
   expanded data. Callers restoring a known larger archive may pass lower or
   higher positive `:max-entries`, `:max-entry-bytes`, and
   `:max-expanded-bytes` values explicitly."
  [archive-path dest-root & {:keys [force? max-entries max-entry-bytes max-expanded-bytes]
                             :or {force? false}}]
  (let [archive-file (io/file archive-path)
        root-file    (io/file dest-root)
        root-existed? (.exists ^File root-file)
        limits       {:max-entries (positive-long-option
                                    (or max-entries default-max-archive-entries)
                                    :max-entries)
                      :max-entry-bytes (positive-long-option
                                        (or max-entry-bytes default-max-archive-entry-bytes)
                                        :max-entry-bytes)
                      :max-expanded-bytes (positive-long-option
                                           (or max-expanded-bytes default-max-archive-expanded-bytes)
                                           :max-expanded-bytes)}
        state        (atom {:entries 0 :bytes 0})]
    (when-not (.exists ^File archive-file)
      (throw (ex-info "Archive does not exist" {:archive archive-path})))
    (when (and (.exists ^File root-file) force?)
      (delete-tree! dest-root))
    (when-not (.exists ^File root-file)
      (Files/createDirectories (.toPath root-file)
                               (make-array java.nio.file.attribute.FileAttribute 0)))
    (try
      (with-open [zip (ZipFile. archive-file)]
        (doseq [entry (enumeration-seq (.entries zip))]
          (extract-entry! zip root-file entry limits state)))
      {:archive-path (.getAbsolutePath archive-file)
       :root-path    (.getAbsolutePath root-file)
       :db-path      (open-db-path (.getAbsolutePath root-file))
       :manifest     (read-manifest-file (.getAbsolutePath root-file))}
      (catch Exception e
        (when (or force? (not root-existed?))
          (delete-tree! dest-root))
        (throw e)))))

(defn- tree-last-modified
  [path]
  (let [root (io/file path)]
    (if (.exists ^File root)
      (reduce max 0 (map #(.lastModified ^File %) (file-seq root)))
      0)))

(defn local-crypto-opts
  [db-path]
  (let [key-file        (support-file db-path "master.key")
        passphrase-file (support-file db-path "master.passphrase")]
    (cond
      (.exists ^File key-file)
      {:key-file (.getAbsolutePath ^File key-file)
       :allow-insecure-key-file? true}

      (.exists ^File passphrase-file)
      {:passphrase-file (.getAbsolutePath ^File passphrase-file)
       :allow-insecure-key-file? true}

      :else
      {})))

(defn open-archive!
  [archive-path]
  (let [archive-file (io/file archive-path)
        root-path    (default-open-root archive-path)
        root-file    (io/file root-path)
        db-path      (open-db-path root-path)
        manifest-file (io/file (manifest-path root-path))]
    (when-not (.exists ^File archive-file)
      (throw (ex-info "Archive does not exist" {:archive archive-path})))
    (let [needs-refresh? (or (not (.exists ^File root-file))
                             (not (.exists ^File (io/file db-path)))
                             (not (.exists ^File manifest-file))
                             (> (.lastModified archive-file)
                                (long (tree-last-modified root-path))))
          context      (if needs-refresh?
                         (assoc (unpack! archive-path root-path :force? true) :refreshed? true)
                         {:archive-path (.getAbsolutePath archive-file)
                          :root-path    (.getAbsolutePath root-file)
                          :db-path      db-path
                          :manifest     (read-manifest-file root-path)
                          :refreshed?   false})]
      (assoc context :crypto-opts (local-crypto-opts (:db-path context))))))

(defn- dedupe-entries
  [entries]
  (reduce (fn [acc entry]
            (if (some #(= (:entry %) (:entry entry)) acc)
              acc
              (conj acc entry)))
          []
          entries))

(defn pack!
  "Create a portable Xia archive.

   The archive is a zip file containing:
   - the DB contents under `db/`
   - required local support files under `db/.xia/`
   - `manifest.edn` with restore instructions"
  [db-path archive-path & {:keys [force? manifest-db-path]
                           :or {force? false}}]
  (let [^File archive-file (ensure-output-path! archive-path force?)
        db-files         (db-entries db-path)
        {:keys [key-source archive-entries restore-requires]} (key-context db-path)
        all-file-entries (dedupe-entries (vec (concat db-files archive-entries)))
        manifest         {:format           :xia-pack/v1
                          :created-at       (str (Instant/now))
                          :db-entry         "db"
                          :db-path          (or manifest-db-path db-path)
                          :key-source       key-source
                          :archive-entries  (mapv :entry all-file-entries)
                          :restore-requires restore-requires}]
    (with-open [^FileOutputStream fos (FileOutputStream. ^File archive-file)
                out (-> fos BufferedOutputStream. ZipOutputStream.)]
      (doseq [entry all-file-entries]
        (write-file-entry! out entry))
      (write-bytes-entry! out manifest-entry (pr-str manifest)))
    {:archive          (.getAbsolutePath archive-file)
     :key-source       key-source
     :entries          (conj (mapv :entry all-file-entries) manifest-entry)
     :restore-requires restore-requires}))
