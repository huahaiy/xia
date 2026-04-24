(ns xia.checkpoint
  "Managed online checkpoints for control-plane durability.

   A checkpoint is a restore-safe, host-local staged copy of the live Datalevin
   DB plus required non-secret DB support files. It is intended for control
   plane upload workflows, not direct user-facing archive export."
  (:require [clojure.java.io :as io]
            [datalevin.dump :as dump]
            [taoensso.timbre :as log]
            [xia.async :as async]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.paths :as paths])
  (:import [java.io File FileInputStream FileOutputStream]
           [java.nio.file Files Path Paths]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant]
           [java.util UUID]))

(def ^:private checkpoint-prefix "xia-managed-checkpoint-")
(def ^:private support-dir-name ".xia")
(def ^:private checkpoint-manifest-name "checkpoint.edn")
(def ^:private checkpoint-ready-name "checkpoint.ready")
(def ^:private included-support-file-names #{"master.salt"})
(def ^:private excluded-support-file-names #{"master.key" "master.passphrase"})
(defonce ^:private checkpoint-state
  (atom {:accepting? true
         :tasks {}
         :statuses {}}))
(defonce ^:private checkpoint-lock (Object.))

(defn- absolute-path
  [path]
  (when (some? path)
    (.getAbsolutePath (io/file path))))

(defn- instant-str
  [value]
  (when value
    (str value)))

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
  (let [^Path p (Paths/get ^String path (make-array String 0))]
    (Files/createDirectories p (make-array FileAttribute 0))
    path))

(defn- copy-file!
  [^File source ^File target]
  (let [^Path target-path (.toPath target)]
    (when-let [^Path parent (.getParent target-path)]
      (Files/createDirectories parent (make-array FileAttribute 0)))
    (with-open [in  (FileInputStream. source)
                out (FileOutputStream. target)]
      (let [buffer (byte-array 8192)]
        (loop []
          (let [read-count (.read in buffer)]
            (when-not (neg? read-count)
              (.write out buffer 0 read-count)
              (recur))))))))

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
        (Files/deleteIfExists ^Path (.toPath ^File file))))))

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
           ready-marker-path source-db-path support-files workspace-tx]}]
  (let [{:keys [source]} (crypto/current-key-source)]
    {:checkpoint_id            checkpoint-id
     :status                   :ready
     :created_at               (str created-at)
     :ready_at                 (str ready-at)
     :tenant_id                (or (db/current-instance-id) paths/default-instance-id)
     :workspace_tx             workspace-tx
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

(defn- checkpoint-request
  [{:keys [checkpoint-id created-at source-db-path staging-root]}]
  {:checkpoint_id checkpoint-id
   :status :pending
   :created_at (instant-str created-at)
   :source_db_path source-db-path
   :staging_root staging-root})

(defn- checkpoint-failure
  [{:keys [checkpoint-id created-at source-db-path staging-root]} ^Throwable t]
  {:checkpoint_id checkpoint-id
   :status :failed
   :created_at (instant-str created-at)
   :finished_at (instant-str (Instant/now))
   :source_db_path source-db-path
   :staging_root staging-root
   :error (or (.getMessage t)
              (str (class t)))})

(defn- update-checkpoint-status!
  [checkpoint-id f]
  (locking checkpoint-lock
    (when-let [status (get-in @checkpoint-state [:statuses checkpoint-id])]
      (let [next-status (f status)]
        (swap! checkpoint-state assoc-in [:statuses checkpoint-id] next-status)
        next-status))))

(defn reset-runtime!
  []
  (locking checkpoint-lock
    (reset! checkpoint-state {:accepting? true
                              :tasks {}
                              :statuses {}})))

(defn prepare-shutdown!
  []
  (locking checkpoint-lock
    (let [{:keys [tasks]}
          (swap! checkpoint-state assoc :accepting? false)]
      (count tasks))))

(defn await-background-tasks!
  []
  (loop []
    (when-let [task (locking checkpoint-lock
                      (first (vals (:tasks @checkpoint-state))))]
      (try
        @task
        (catch Exception _
          nil))
      (recur))))

(defn checkpoint-status
  [checkpoint-id]
  (locking checkpoint-lock
    (get-in @checkpoint-state [:statuses checkpoint-id])))

(defn create-online-checkpoint!
  "Create a restore-safe staged checkpoint from the live DB while Xia remains online.

   Returns metadata describing a host-local staged checkpoint directory that is
   ready for an external uploader to consume."
  ([] (create-online-checkpoint! nil))
  ([{:keys [staging-root checkpoint-id created-at db-snapshot]}]
   (let [source-db-path (db/current-db-path)]
     (when-not source-db-path
       (throw (ex-info "Database not connected. Cannot create checkpoint."
                       {})))
     (let [source-db-path* (absolute-path source-db-path)
           created-at*     (or created-at (Instant/now))
           checkpoint-id*  (or checkpoint-id (str "ckpt_" (UUID/randomUUID)))
           db-snapshot*    (or db-snapshot @(db/conn))
           workspace-tx    (some-> (:max-tx db-snapshot*) long)
          staging-root* (ensure-directory! (checkpoint-directory staging-root))
          ^Path staging-root-path (Paths/get ^String staging-root*
                                             (make-array String 0))
          stage-path*   (str (Files/createTempDirectory
                               staging-root-path
                               checkpoint-prefix
                               (make-array FileAttribute 0)))
           stage-root    (volatile! stage-path*)]
       (try
         (let [staged-db-path    (str (Paths/get stage-path* (into-array String ["db"])))
               _                 (dump/copy db-snapshot* staged-db-path false)
               support-files     (copy-required-support-files! source-db-path staged-db-path)
               manifest-path     (str (Paths/get stage-path*
                                                 (into-array String [checkpoint-manifest-name])))
               ready-marker-path (str (Paths/get stage-path*
                                                 (into-array String [checkpoint-ready-name])))
               ready-at          (Instant/now)
               manifest          (checkpoint-manifest {:checkpoint-id checkpoint-id*
                                                      :created-at created-at*
                                                      :ready-at ready-at
                                                      :staged-path stage-path*
                                                      :staged-db-path staged-db-path
                                                      :manifest-path manifest-path
                                                      :ready-marker-path ready-marker-path
                                                      :source-db-path source-db-path*
                                                      :support-files support-files
                                                      :workspace-tx workspace-tx})]
           (spit manifest-path (pr-str manifest))
           (spit ready-marker-path (str checkpoint-id* "\n"))
           manifest)
         (catch Throwable t
           (when-let [path @stage-root]
             (delete-tree! path))
           (throw t)))))))

(defn- run-checkpoint-task!
  [{:keys [checkpoint_id] :as request} created-at opts]
  (try
    (update-checkpoint-status! checkpoint_id
                               #(assoc %
                                       :status :creating
                                       :started_at (instant-str (Instant/now))))
    (let [manifest (create-online-checkpoint! (assoc opts
                                                     :checkpoint-id checkpoint_id
                                                     :created-at created-at
                                                     :db-snapshot @(db/conn)))]
      (locking checkpoint-lock
        (swap! checkpoint-state assoc-in [:statuses checkpoint_id] manifest))
      manifest)
    (catch Throwable t
      (let [failed (checkpoint-failure request t)]
        (locking checkpoint-lock
          (swap! checkpoint-state assoc-in [:statuses checkpoint_id] failed))
        (log/error t "Managed checkpoint creation failed" {:checkpoint-id checkpoint_id})
        failed))
    (finally
      (locking checkpoint-lock
        (swap! checkpoint-state update :tasks dissoc checkpoint_id)))))

(defn submit-online-checkpoint!
  ([] (submit-online-checkpoint! nil))
  ([{:keys [staging-root]}]
   (let [source-db-path (db/current-db-path)]
     (when-not source-db-path
       (throw (ex-info "Database not connected. Cannot create checkpoint."
                       {})))
     (let [source-db-path* (absolute-path source-db-path)
           created-at      (Instant/now)
           checkpoint-id   (str "ckpt_" (UUID/randomUUID))
           staging-root*   (checkpoint-directory staging-root)
           request         (checkpoint-request {:checkpoint-id checkpoint-id
                                                :created-at created-at
                                                :source-db-path source-db-path*
                                                :staging-root staging-root*})]
       (locking checkpoint-lock
         (when-not (:accepting? @checkpoint-state)
           (throw (ex-info "Checkpoint creation is shutting down."
                           {:status 503
                            :error "checkpoint creation is shutting down"})))
         (swap! checkpoint-state assoc-in [:statuses checkpoint-id] request))
       (if-let [task (async/submit-background!
                       (str "managed checkpoint " checkpoint-id)
                       #(run-checkpoint-task! request created-at {:staging-root staging-root*}))]
         (do
           (locking checkpoint-lock
             (swap! checkpoint-state assoc-in [:tasks checkpoint-id] task))
           request)
         (do
           (locking checkpoint-lock
             (swap! checkpoint-state update :statuses dissoc checkpoint-id))
           (throw (ex-info "Checkpoint queue is unavailable."
                           {:status 503
                            :error "checkpoint queue is unavailable"}))))))))
