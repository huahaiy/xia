(ns xia.backup
  "Automatic database backups using a safe Datalevin LMDB copy.

   Backups are written as portable `.xia` archives so they can be restored
   directly, but the archive is built from a staged LMDB copy rather than the
   live database directory."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.dump :as dump]
            [taoensso.timbre :as log]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.paths :as paths]
            [xia.pack :as pack])
  (:import [java.io File]
           [java.nio.file Files Path Paths StandardCopyOption]
           [java.nio.file.attribute FileAttribute]
           [java.time Duration Instant ZoneOffset]
           [java.time.format DateTimeFormatter]
           [java.util UUID]))

(def ^:private default-backup-interval-hours 24)
(def ^:private default-backup-retain-count 7)
(def ^:private managed-backup-prefix "xia-backup-")
(def ^:private support-dir-name ".xia")
(def ^DateTimeFormatter ^:private backup-name-formatter
  (-> (DateTimeFormatter/ofPattern "yyyyMMdd-HHmmss-SSS")
      (.withZone ZoneOffset/UTC)))

(defonce ^:private run-state
  (atom {:running? false
         :started-at nil}))

(defn- start-run!
  []
  (let [started-at (Instant/now)
        acquired?  (atom false)]
    (swap! run-state
           (fn [state]
             (if (:running? state)
               state
               (do
                 (reset! acquired? true)
                 {:running? true
                  :started-at started-at}))))
    (when @acquired?
      started-at)))

(defn- path-str
  [base & more]
  (str (Paths/get base (into-array String more))))

(defn- absolute-path
  [path]
  (when (some? path)
    (.getAbsolutePath (io/file path))))

(defn- default-backup-directory
  []
  (let [db-path (or (db/current-db-path)
                    (paths/default-db-path))
        db-file (io/file db-path)
        parent  (or (.getParentFile db-file)
                    (.getAbsoluteFile (io/file ".")))]
    (.getAbsolutePath (io/file parent "backups"))))

(defn enabled?
  []
  (cfg/boolean-option :backup/enabled? true))

(defn backup-directory
  []
  (or (some-> (cfg/string-option :backup/directory nil)
              absolute-path)
      (default-backup-directory)))

(defn backup-interval-hours
  []
  (cfg/positive-long :backup/interval-hours
                     default-backup-interval-hours))

(defn backup-retain-count
  []
  (cfg/positive-long :backup/retain-count
                     default-backup-retain-count))

(defn running?
  []
  (boolean (:running? @run-state)))

(defn- parse-instant
  [value]
  (let [text (some-> value str str/trim)]
    (when (seq text)
      (try
        (Instant/parse text)
        (catch Exception _
          nil)))))

(defn last-attempt-at
  []
  (parse-instant (db/get-config :backup/last-attempt-at)))

(defn last-success-at
  []
  (parse-instant (db/get-config :backup/last-success-at)))

(defn last-archive-path
  []
  (some-> (db/get-config :backup/last-archive-path)
          str
          str/trim
          not-empty))

(defn last-error
  []
  (some-> (db/get-config :backup/last-error)
          str
          str/trim
          not-empty))

(defn next-due-at
  ([]
   (next-due-at (Instant/now)))
  ([now]
   (when (enabled?)
     (if-let [attempt (last-attempt-at)]
       (.plus ^Instant attempt (Duration/ofHours (long (backup-interval-hours))))
       now))))

(defn backup-due?
  ([]
   (backup-due? (Instant/now)))
  ([now]
   (boolean
     (and (enabled?)
          (not (running?))
          (let [due-at (next-due-at now)]
            (or (nil? due-at)
                (not (.isAfter ^Instant due-at ^Instant now))))))))

(defn admin-body
  []
  {:enabled           (enabled?)
   :directory         (backup-directory)
   :interval_hours    (backup-interval-hours)
   :retain_count      (backup-retain-count)
   :running           (running?)
   :started_at        (:started-at @run-state)
   :last_attempt_at   (last-attempt-at)
   :last_success_at   (last-success-at)
   :last_archive_path (last-archive-path)
   :last_error        (last-error)
   :next_due_at       (next-due-at)})

(defn- ensure-directory!
  [path]
  (let [^Path p (Paths/get path (make-array String 0))]
    (Files/createDirectories p (make-array FileAttribute 0))
    path))

(defn- copy-tree!
  [^File source ^File target]
  (doseq [^File file (file-seq source)]
    (let [relative (.relativize (.toPath source) (.toPath file))
          ^Path target-path (.resolve (.toPath target) relative)]
      (if (.isDirectory file)
        (Files/createDirectories target-path (make-array FileAttribute 0))
        (do
          (when-let [^Path parent (.getParent target-path)]
            (Files/createDirectories parent (make-array FileAttribute 0)))
          (let [^Path source-path (.toPath file)
                ^"[Ljava.nio.file.CopyOption;" copy-options
                (into-array java.nio.file.CopyOption
                            [StandardCopyOption/REPLACE_EXISTING])]
            (Files/copy source-path target-path copy-options)))))))

(defn- copy-support-dir!
  [source-db-path staged-db-path]
  (let [source (io/file (path-str source-db-path support-dir-name))
        target (io/file (path-str staged-db-path support-dir-name))]
    (when (.exists source)
      (copy-tree! source target))))

(defn- delete-tree!
  [path]
  (let [root (io/file path)]
    (when (.exists root)
      (doseq [file (reverse (file-seq root))]
        (Files/deleteIfExists (.toPath ^File file))))))

(defn- managed-backup-file?
  [^File file]
  (let [name (.getName file)]
    (and (.isFile file)
         (pack/archive-path? name)
         (str/starts-with? name managed-backup-prefix))))

(defn- managed-backup-files
  [backup-dir]
  (let [dir (io/file backup-dir)]
    (->> (or (.listFiles dir) (make-array File 0))
         (filter managed-backup-file?)
         (sort-by (fn [^File file]
                    [(.lastModified file) (.getName file)]))
         reverse
         vec)))

(defn- prune-old-backups!
  [backup-dir retain-count]
  (let [stale-files (drop retain-count (managed-backup-files backup-dir))]
    (doseq [^File file stale-files]
      (when-not (.delete file)
        (log/warn "Failed to delete old backup archive" (.getAbsolutePath file))))
    (mapv #(.getAbsolutePath ^File %) stale-files)))

(defn- backup-archive-path
  [backup-dir]
  (let [timestamp (.format ^DateTimeFormatter backup-name-formatter ^java.time.temporal.TemporalAccessor (Instant/now))
        suffix    (subs (str (UUID/randomUUID)) 0 8)]
    (.getAbsolutePath
      (io/file backup-dir
               (str managed-backup-prefix timestamp "-" suffix ".xia")))))

(defn run-backup!
  "Create a portable backup archive from a safe LMDB copy of the live DB.

   Returns a status map and records the latest outcome in config."
  []
  (let [source-db-path (db/current-db-path)]
    (when-not source-db-path
      (throw (ex-info "Database not connected. Cannot create backup."
                      {})))
    (if-let [started-at (start-run!)]
      (let [backup-dir   (ensure-directory! (backup-directory))
            retain-count (backup-retain-count)
            stage-root*  (volatile! nil)]
        (db/set-config! :backup/last-attempt-at (str started-at))
        (try
          (let [archive-path   (backup-archive-path backup-dir)
                stage-root     (str (Files/createTempDirectory "xia-backup-stage"
                                                               (make-array FileAttribute 0)))
                _              (vreset! stage-root* stage-root)
                staged-db-path (path-str stage-root "db")]
            (dump/copy @(db/conn) staged-db-path false)
            (copy-support-dir! source-db-path staged-db-path)
            (pack/pack! staged-db-path archive-path
                        :manifest-db-path source-db-path)
            (let [pruned (prune-old-backups! backup-dir retain-count)
                  result {:status       :success
                          :started-at   started-at
                          :finished-at  (Instant/now)
                          :archive-path archive-path
                          :pruned-paths pruned}]
              (db/set-config! :backup/last-success-at (str (:finished-at result)))
              (db/set-config! :backup/last-archive-path archive-path)
              (db/delete-config! :backup/last-error)
              (log/info "Created database backup" archive-path)
              result))
          (catch Throwable t
            (let [message (or (.getMessage t) (str t))
                  result  {:status      :error
                           :started-at  started-at
                           :finished-at (Instant/now)
                           :error       message}]
              (db/set-config! :backup/last-error message)
              (log/error t "Database backup failed")
              result))
          (finally
            (reset! run-state {:running? false
                               :started-at nil})
            (when-let [stage-root @stage-root*]
              (delete-tree! stage-root)))))
      {:status :running}
      )))

(defn run-scheduled-backup!
  []
  (if (backup-due?)
    (run-backup!)
    {:status :skipped}))
