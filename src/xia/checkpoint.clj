(ns xia.checkpoint
  "Managed online checkpoints for control-plane durability.

   A checkpoint is a restore-safe, host-local staged copy of the live Datalevin
   DB plus required non-secret DB support files. It is intended for control
   plane upload workflows, not direct user-facing archive export."
  (:require [clojure.java.io :as io]
            [datalevin.dump :as dump]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.paths :as paths])
  (:import [java.io File]
           [java.nio.file Files Path Paths StandardCopyOption]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant]
           [java.util UUID]))

(def ^:private checkpoint-prefix "xia-managed-checkpoint-")
(def ^:private support-dir-name ".xia")
(def ^:private checkpoint-manifest-name "checkpoint.edn")
(def ^:private checkpoint-ready-name "checkpoint.ready")
(def ^:private included-support-file-names #{"master.salt"})
(def ^:private excluded-support-file-names #{"master.key" "master.passphrase"})

(defn- absolute-path
  [path]
  (when (some? path)
    (.getAbsolutePath (io/file path))))

(defn- default-checkpoint-directory
  []
  (let [db-path (or (db/current-db-path)
                    (paths/default-db-path))
        db-file (io/file db-path)
        parent  (or (.getParentFile db-file)
                    (.getAbsoluteFile (io/file ".")))]
    (.getAbsolutePath (io/file parent "checkpoints"))))

(defn checkpoint-directory
  ([] (default-checkpoint-directory))
  ([path]
   (or (some-> path absolute-path)
       (default-checkpoint-directory))))

(defn- ensure-directory!
  [path]
  (let [^Path p (Paths/get path (make-array String 0))]
    (Files/createDirectories p (make-array FileAttribute 0))
    path))

(defn- copy-file!
  [^File source ^File target]
  (when-let [^Path parent (.getParent (.toPath target))]
    (Files/createDirectories parent (make-array FileAttribute 0)))
  (Files/copy (.toPath source)
              (.toPath target)
              (into-array java.nio.file.CopyOption
                          [StandardCopyOption/REPLACE_EXISTING])))

(defn- copy-required-support-files!
  [source-db-path staged-db-path]
  (let [source-dir (io/file (paths/support-dir-path source-db-path))
        target-dir (io/file (paths/support-dir-path staged-db-path))]
    (->> included-support-file-names
         (keep (fn [filename]
                 (let [source-file (io/file source-dir filename)]
                   (when (.exists source-file)
                     (copy-file! source-file (io/file target-dir filename))
                     filename))))
         sort
         vec)))

(defn- delete-tree!
  [path]
  (let [root (io/file path)]
    (when (.exists root)
      (doseq [file (reverse (file-seq root))]
        (Files/deleteIfExists (.toPath ^File file))))))

(defn- restore-requires
  [source]
  (case source
    :env
    ["Set XIA_MASTER_KEY to the same base64 32-byte key before opening the checkpoint DB."]

    (:key-file :env-file :local-key-file)
    ["Provide the same master key material via XIA_MASTER_KEY_FILE or equivalent secure host secret before opening the checkpoint DB."]

    (:passphrase
     :passphrase-file
     :env-passphrase
     :env-passphrase-file
     :local-passphrase-file
     :prompt-passphrase
     :passphrase-provider)
    ["Provide the same master passphrase before opening the checkpoint DB."]

    []))

(defn- checkpoint-manifest
  [{:keys [checkpoint-id created-at ready-at staged-path staged-db-path manifest-path
           ready-marker-path source-db-path support-files]}]
  (let [{:keys [source]} (crypto/current-key-source)]
    {:checkpoint_id            checkpoint-id
     :status                   :ready
     :created_at               (str created-at)
     :ready_at                 (str ready-at)
     :tenant_id                (or (db/current-instance-id) paths/default-instance-id)
     :format                   {:kind :datalevin-copy
                                :version 1}
     :staged_path              staged-path
     :staged_db_path           staged-db-path
     :manifest_path            manifest-path
     :ready_marker_path        ready-marker-path
     :source_db_path           source-db-path
     :schema_version           (db/schema-version)
     :support_files            support-files
     :excluded_support_files   (sort excluded-support-file-names)
     :includes_secret_material false
     :key_source               (some-> source name)
     :restore_requires         (restore-requires source)}))

(defn create-online-checkpoint!
  "Create a restore-safe staged checkpoint from the live DB while Xia remains online.

   Returns metadata describing a host-local staged checkpoint directory that is
   ready for an external uploader to consume."
  ([] (create-online-checkpoint! nil))
  ([{:keys [staging-root]}]
   (let [source-db-path (db/current-db-path)]
     (when-not source-db-path
       (throw (ex-info "Database not connected. Cannot create checkpoint."
                       {})))
     (let [source-db-path* (absolute-path source-db-path)
           created-at      (Instant/now)
           checkpoint-id (str "ckpt_" (UUID/randomUUID))
           staging-root* (ensure-directory! (checkpoint-directory staging-root))
           stage-path*   (str (Files/createTempDirectory
                                (Paths/get staging-root* (make-array String 0))
                                checkpoint-prefix
                                (make-array FileAttribute 0)))
           stage-root    (volatile! stage-path*)]
       (try
         (let [staged-db-path    (str (Paths/get stage-path* (into-array String ["db"])))
               _                 (dump/copy @(db/conn) staged-db-path false)
               support-files     (copy-required-support-files! source-db-path staged-db-path)
               manifest-path     (str (Paths/get stage-path*
                                                 (into-array String [checkpoint-manifest-name])))
               ready-marker-path (str (Paths/get stage-path*
                                                 (into-array String [checkpoint-ready-name])))
               ready-at          (Instant/now)
               manifest          (checkpoint-manifest {:checkpoint-id checkpoint-id
                                                      :created-at created-at
                                                      :ready-at ready-at
                                                      :staged-path stage-path*
                                                      :staged-db-path staged-db-path
                                                      :manifest-path manifest-path
                                                      :ready-marker-path ready-marker-path
                                                      :source-db-path source-db-path*
                                                      :support-files support-files})]
           (spit manifest-path (pr-str manifest))
           (spit ready-marker-path (str checkpoint-id "\n"))
           manifest)
         (catch Throwable t
           (when-let [path @stage-root]
             (delete-tree! path))
           (throw t)))))))
