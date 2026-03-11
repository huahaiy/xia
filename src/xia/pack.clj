(ns xia.pack
  "Portable archive packaging for Xia state."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io BufferedInputStream BufferedOutputStream
            File FileInputStream FileOutputStream]
           [java.nio.file Files Path Paths]
           [java.time Instant]
           [java.util.zip ZipEntry ZipFile ZipOutputStream]))

(def ^:private manifest-entry "manifest.edn")
(def ^:private buffer-size 8192)
(def ^:private archive-extension ".xia")
(def ^:private support-dir-name ".xia")
(def ^:private support-dir-entry "db/.xia")

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
       :restore-requires ["Set XIA_MASTER_KEY_FILE to db/.xia/master.key after extracting the archive."]}

      (seq env-passphrase)
      {:key-source       :env-passphrase
       :archive-entries  []
       :restore-requires ["Provide the same master passphrase at startup or via XIA_MASTER_PASSPHRASE."]}

      env-passphrase-file*
      {:key-source       :env-passphrase-file
       :archive-entries  [{:file env-passphrase-file* :entry (support-entry "master.passphrase")}]
       :restore-requires ["Set XIA_MASTER_PASSPHRASE_FILE to db/.xia/master.passphrase after extracting the archive."]}

      (.exists ^File local-key-file)
      {:key-source       :local-key-file
       :archive-entries  []
       :restore-requires ["Open the archive directly with `xia your-archive.xia`, or set XIA_MASTER_KEY_FILE to db/.xia/master.key when opening the extracted DB."]}

      (.exists ^File local-passphrase-file)
      {:key-source       :local-passphrase-file
       :archive-entries  []
       :restore-requires ["Open the archive directly with `xia your-archive.xia`, or set XIA_MASTER_PASSPHRASE_FILE to db/.xia/master.passphrase when opening the extracted DB."]}

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
  (let [zip-entry (doto (ZipEntry. entry)
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
  (.putNextEntry zip (ZipEntry. entry))
  (.write zip (.getBytes ^String data "UTF-8"))
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

(defn read-manifest
  [archive-path]
  (with-open [zip (ZipFile. (io/file archive-path))]
    (when-let [entry (.getEntry zip manifest-entry)]
      (edn/read-string (slurp (.getInputStream zip entry))))))

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
  (let [target (.normalize (.resolve ^Path (root-path root-file) ^String entry-name))]
    (when-not (.startsWith target (root-path root-file))
      (throw (ex-info "Archive entry escapes destination root"
                      {:entry entry-name
                       :root  (.getAbsolutePath ^File root-file)})))
    target))

(defn- extract-entry!
  [^ZipFile zip ^File root-file ^java.util.zip.ZipEntry entry]
  (let [target (safe-target-path root-file (.getName entry))]
    (if (.isDirectory entry)
      (Files/createDirectories target (make-array java.nio.file.attribute.FileAttribute 0))
      (do
        (when-let [parent (.getParent target)]
          (Files/createDirectories parent (make-array java.nio.file.attribute.FileAttribute 0)))
        (with-open [in  (.getInputStream zip entry)
                    out (BufferedOutputStream. (FileOutputStream. (.toFile target)))]
          (let [buffer (byte-array buffer-size)]
            (loop []
              (let [n (.read in buffer)]
                (when (pos? n)
                  (.write out buffer 0 n)
                  (recur))))))
        (when (pos? (.getTime entry))
          (.setLastModified (.toFile target) (.getTime entry)))))))

(defn unpack!
  [archive-path dest-root & {:keys [force?] :or {force? false}}]
  (let [archive-file (io/file archive-path)
        root-file    (io/file dest-root)]
    (when-not (.exists ^File archive-file)
      (throw (ex-info "Archive does not exist" {:archive archive-path})))
    (when (and (.exists ^File root-file) force?)
      (delete-tree! dest-root))
    (when-not (.exists ^File root-file)
      (Files/createDirectories (.toPath root-file)
                               (make-array java.nio.file.attribute.FileAttribute 0)))
    (with-open [zip (ZipFile. archive-file)]
      (doseq [entry (enumeration-seq (.entries zip))]
        (extract-entry! zip root-file entry)))
    {:archive-path (.getAbsolutePath archive-file)
     :root-path    (.getAbsolutePath root-file)
     :db-path      (open-db-path (.getAbsolutePath root-file))
     :manifest     (read-manifest-file (.getAbsolutePath root-file))}))

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
      {:key-file (.getAbsolutePath ^File key-file)}

      (.exists ^File passphrase-file)
      {:passphrase-file (.getAbsolutePath ^File passphrase-file)}

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
                                (tree-last-modified root-path)))
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
  [db-path archive-path & {:keys [force?] :or {force? false}}]
  (let [archive-file     (ensure-output-path! archive-path force?)
        db-files         (db-entries db-path)
        {:keys [key-source archive-entries restore-requires]} (key-context db-path)
        all-file-entries (dedupe-entries (vec (concat db-files archive-entries)))
        manifest         {:format           :xia-pack/v1
                          :created-at       (str (Instant/now))
                          :db-entry         "db"
                          :db-path          db-path
                          :key-source       key-source
                          :archive-entries  (mapv :entry all-file-entries)
                          :restore-requires restore-requires}]
    (with-open [out (-> archive-file FileOutputStream. BufferedOutputStream. ZipOutputStream.)]
      (doseq [entry all-file-entries]
        (write-file-entry! out entry))
      (write-bytes-entry! out manifest-entry (pr-str manifest)))
    {:archive          (.getAbsolutePath archive-file)
     :key-source       key-source
     :entries          (conj (mapv :entry all-file-entries) manifest-entry)
     :restore-requires restore-requires}))
