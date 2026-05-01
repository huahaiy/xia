(ns xia.snapshot
  "User-facing safety snapshots for rolling back Xia state before risky work."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.dump :as dump]
            [xia.db :as db]
            [xia.pack :as pack]
            [xia.paths :as paths])
  (:import [java.io File]
           [java.nio.file Files Path Paths StandardCopyOption]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant ZoneOffset]
           [java.time.format DateTimeFormatter]
           [java.util UUID]))

(def ^:private current-format :xia-safety-snapshot/v1)
(def ^:private db-archive-name "db.xia")
(def ^:private manifest-name "manifest.edn")
(def ^:private workspace-entry-name "workspace")
(def ^:private support-dir-name ".xia")
(def ^DateTimeFormatter ^:private snapshot-id-formatter
  (-> (DateTimeFormatter/ofPattern "yyyyMMdd-HHmmss-SSS")
      (.withZone ZoneOffset/UTC)))

(defn- path-str
  [base & more]
  (str (Paths/get base (into-array String more))))

(defn- absolute-path
  [path]
  (when (some? path)
    (.getAbsolutePath (io/file path))))

(defn- nonblank
  [value]
  (some-> value str str/trim not-empty))

(defn- safe-label
  [label]
  (when-let [text (nonblank label)]
    (some-> text
            str/lower-case
            (str/replace #"[^a-z0-9._-]+" "-")
            (str/replace #"^[._-]+|[._-]+$" "")
            not-empty)))

(defn- suffix
  []
  (subs (str (UUID/randomUUID)) 0 8))

(defn- snapshot-id
  [label]
  (str "snap-"
       (.format snapshot-id-formatter (Instant/now))
       (when-let [label* (safe-label label)]
         (str "-" label*))
       "-"
       (suffix)))

(defn snapshot-directory
  "Return the safety snapshot root for a DB path."
  ([] (snapshot-directory nil nil))
  ([db-path] (snapshot-directory db-path nil))
  ([db-path snapshot-root]
   (or (some-> snapshot-root absolute-path)
       (let [db-path* (or db-path
                          (db/current-db-path)
                          (paths/default-db-path))
             db-file  (io/file db-path*)
             parent   (or (.getParentFile db-file)
                          (.getAbsoluteFile (io/file ".")))]
         (.getAbsolutePath (io/file parent "snapshots"))))))

(defn- ensure-directory!
  [path]
  (let [^Path p (Paths/get ^String path (make-array String 0))]
    (Files/createDirectories p (make-array FileAttribute 0))
    path))

(defn- delete-tree!
  [path]
  (let [root (io/file path)]
    (when (.exists root)
      (doseq [file (reverse (file-seq root))]
        (Files/deleteIfExists (.toPath ^File file))))))

(defn- target-inside-source?
  [source target]
  (let [source-path (-> (io/file source) .toPath .toAbsolutePath .normalize)
        target-path (-> (io/file target) .toPath .toAbsolutePath .normalize)]
    (and (not= source-path target-path)
         (.startsWith ^Path target-path ^Path source-path))))

(defn- copy-options
  []
  (into-array java.nio.file.CopyOption
              [StandardCopyOption/REPLACE_EXISTING]))

(defn- copy-tree!
  [source target]
  (let [source-file (io/file source)
        target-file (io/file target)]
    (when (.exists source-file)
      (when (target-inside-source? source target)
        (throw (ex-info "Refusing to copy a directory into itself."
                        {:source source
                         :target target})))
      (doseq [^File file (file-seq source-file)]
        (let [source-path (.toPath file)]
          (when (Files/isSymbolicLink source-path)
            (throw (ex-info "Refusing to snapshot a symbolic link."
                            {:source (.getAbsolutePath file)})))
          (let [relative    (.relativize (.toPath source-file) source-path)
                target-path (.resolve (.toPath target-file) relative)]
            (if (.isDirectory file)
              (Files/createDirectories target-path (make-array FileAttribute 0))
              (do
                (when-let [parent (.getParent target-path)]
                  (Files/createDirectories parent (make-array FileAttribute 0)))
                (Files/copy source-path target-path (copy-options))))))))))

(defn- copy-support-dir!
  [source-db-path staged-db-path]
  (let [source (io/file (path-str source-db-path support-dir-name))
        target (io/file (path-str staged-db-path support-dir-name))]
    (when (.exists source)
      (copy-tree! source target))))

(defn- connected-to-db-path?
  [db-path]
  (and (db/connected?)
       (= (absolute-path db-path)
          (absolute-path (db/current-db-path)))))

(defn- pack-db!
  [db-path archive-path]
  (if (connected-to-db-path? db-path)
    (let [stage-root* (volatile! nil)]
      (try
        (let [stage-root     (str (Files/createTempDirectory
                                   "xia-snapshot-db"
                                   (make-array FileAttribute 0)))
              _              (vreset! stage-root* stage-root)
              staged-db-path (path-str stage-root "db")]
          (dump/copy @(db/conn) staged-db-path false)
          (copy-support-dir! db-path staged-db-path)
          (pack/pack! staged-db-path archive-path
                      :manifest-db-path db-path))
        (finally
          (when-let [stage-root @stage-root*]
            (delete-tree! stage-root)))))
    (pack/pack! db-path archive-path)))

(defn- manifest-path
  [snapshot-path]
  (path-str snapshot-path manifest-name))

(defn- db-archive-path
  [snapshot-path]
  (path-str snapshot-path db-archive-name))

(defn- workspace-snapshot-path
  [snapshot-path]
  (path-str snapshot-path workspace-entry-name))

(defn- read-manifest-file
  [file]
  (try
    (when (.isFile ^File file)
      (edn/read-string (slurp file)))
    (catch Exception _
      nil)))

(defn read-manifest
  [snapshot-path]
  (read-manifest-file (io/file (manifest-path snapshot-path))))

(defn create-snapshot!
  "Create a named safety snapshot.

   The DB is stored as a portable `.xia` archive. When Xia is currently
   connected to the target DB, the archive is built from a Datalevin snapshot
   copy so the process can stay online."
  [& {:keys [db-path snapshot-root label include-workspace?]
      :or   {include-workspace? true}}]
  (let [db-path*       (absolute-path (or db-path
                                          (db/current-db-path)
                                          (paths/default-db-path)))
        snapshot-root* (ensure-directory! (snapshot-directory db-path* snapshot-root))
        snapshot-id*   (snapshot-id label)
        snapshot-path* (path-str snapshot-root* snapshot-id*)
        archive-path   (db-archive-path snapshot-path*)
        workspace-root (absolute-path (paths/shared-workspace-root))
        workspace-path (workspace-snapshot-path snapshot-path*)]
    (when-not (.exists (io/file db-path*))
      (throw (ex-info "Database path does not exist."
                      {:db-path db-path*})))
    (ensure-directory! snapshot-path*)
    (try
      (pack-db! db-path* archive-path)
      (let [workspace-included? (and include-workspace?
                                     workspace-root
                                     (.exists (io/file workspace-root)))]
        (when workspace-included?
          (copy-tree! workspace-root workspace-path))
        (let [manifest {:format current-format
                        :snapshot/id snapshot-id*
                        :snapshot/label (nonblank label)
                        :snapshot/created-at (str (Instant/now))
                        :snapshot/path snapshot-path*
                        :db {:source-path db-path*
                             :archive db-archive-name}
                        :workspace {:included? (boolean workspace-included?)
                                    :source-root workspace-root
                                    :entry (when workspace-included?
                                             workspace-entry-name)}}]
          (spit (manifest-path snapshot-path*) (pr-str manifest))
          manifest))
      (catch Throwable t
        (delete-tree! snapshot-path*)
        (throw t)))))

(defn list-snapshots
  [& {:keys [db-path snapshot-root]}]
  (let [snapshot-root* (snapshot-directory db-path snapshot-root)
        root-file      (io/file snapshot-root*)]
    (if-not (.isDirectory root-file)
      []
      (->> (or (.listFiles root-file) (make-array File 0))
           (filter #(.isDirectory ^File %))
           (keep (fn [^File dir]
                   (when-let [manifest (read-manifest-file
                                         (io/file dir manifest-name))]
                     (assoc manifest
                            :snapshot/path (.getAbsolutePath dir)))))
           (sort-by :snapshot/created-at #(compare %2 %1))
           vec))))

(defn- snapshot-path
  [snapshot-id-or-path snapshot-root db-path]
  (let [candidate (io/file snapshot-id-or-path)]
    (if (.isDirectory candidate)
      (.getAbsolutePath candidate)
      (path-str (snapshot-directory db-path snapshot-root)
                snapshot-id-or-path))))

(defn- move-existing-aside!
  [target-path]
  (let [target-file (io/file target-path)]
    (when (.exists target-file)
      (let [target-path* (.toPath target-file)
            parent       (or (.getParent target-path*)
                             (.toPath (.getAbsoluteFile (io/file "."))))
            aside-name   (str (.getFileName target-path*)
                              ".pre-snapshot-restore-"
                              (.format snapshot-id-formatter (Instant/now))
                              "-"
                              (suffix))
            aside-path   (.resolve parent aside-name)]
        (Files/move target-path* aside-path
                    (into-array java.nio.file.CopyOption []))
        (str aside-path)))))

(defn- replace-tree!
  [source-path target-path force?]
  (let [source-file (io/file source-path)
        target-file (io/file target-path)]
    (when-not (.exists source-file)
      (throw (ex-info "Snapshot restore source path does not exist."
                      {:source-path source-path
                       :target-path target-path})))
    (when (and (.exists target-file) (not force?))
      (throw (ex-info "Target path already exists. Use --force to move it aside before restore."
                      {:target-path target-path})))
    (when-let [parent (.getParentFile target-file)]
      (.mkdirs parent))
    (let [moved-aside (when force?
                        (move-existing-aside! target-path))]
      (copy-tree! source-path target-path)
      moved-aside)))

(defn restore-snapshot!
  "Restore a safety snapshot into `db-path`.

   This must run while Xia is stopped. Existing DB/workspace directories are
   moved aside when `force?` is true; they are not deleted."
  [snapshot-id-or-path & {:keys [db-path snapshot-root force? include-workspace?]
                          :or   {include-workspace? true}}]
  (when (db/connected?)
    (throw (ex-info "Cannot restore a safety snapshot while Xia DB is connected."
                    {:db-path (db/current-db-path)})))
  (let [snapshot-path* (snapshot-path snapshot-id-or-path snapshot-root db-path)
        manifest       (or (read-manifest snapshot-path*)
                           (throw (ex-info "Safety snapshot manifest not found."
                                           {:snapshot snapshot-id-or-path
                                            :snapshot-path snapshot-path*})))
        db-path*       (absolute-path (or db-path
                                          (get-in manifest [:db :source-path])
                                          (paths/default-db-path)))
        unpack-root    (str (Files/createTempDirectory
                             "xia-snapshot-restore"
                             (make-array FileAttribute 0)))
        workspace-root (absolute-path (paths/shared-workspace-root))]
    (when-not (= current-format (:format manifest))
      (throw (ex-info "Unsupported safety snapshot format."
                      {:format (:format manifest)
                       :supported-format current-format})))
    (try
      (let [archive-path (path-str snapshot-path*
                                   (or (get-in manifest [:db :archive])
                                       db-archive-name))
            unpacked     (pack/unpack! archive-path unpack-root :force? true)
            db-aside     (replace-tree! (:db-path unpacked) db-path* force?)
            workspace-entry (get-in manifest [:workspace :entry])
            workspace-source (when workspace-entry
                               (path-str snapshot-path* workspace-entry))
            restore-workspace? (and include-workspace?
                                    (get-in manifest [:workspace :included?])
                                    workspace-source
                                    (.exists (io/file workspace-source)))
            workspace-aside (when restore-workspace?
                              (replace-tree! workspace-source
                                             workspace-root
                                             force?))]
        {:status :restored
         :snapshot/id (:snapshot/id manifest)
         :snapshot/path snapshot-path*
         :db {:path db-path*
              :moved-aside db-aside}
         :workspace {:included? (boolean restore-workspace?)
                     :path workspace-root
                     :moved-aside workspace-aside}})
      (finally
        (delete-tree! unpack-root)))))
