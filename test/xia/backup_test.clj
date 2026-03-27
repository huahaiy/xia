(ns xia.backup-test
  (:require [clojure.test :refer :all]
            [xia.backup :as backup]
            [xia.db :as db]
            [xia.pack :as pack]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant]
           [java.util.zip ZipFile]))

(use-fixtures :each with-test-db)

(defn- temp-dir
  []
  (str (Files/createTempDirectory "xia-backup-test"
                                  (into-array FileAttribute []))))

(defn- zip-entry-names
  [archive-path]
  (with-open [zip (ZipFile. ^String archive-path)]
    (->> (enumeration-seq (.entries zip))
         (map (fn [^java.util.zip.ZipEntry entry]
                (.getName entry)))
         set)))

(defn- managed-backup-files
  [backup-dir]
  (->> (or (.listFiles (File. ^String backup-dir)) (make-array File 0))
       (filter (fn [^File file]
                 (and (.isFile file)
                      (.startsWith (.getName file) "xia-backup-")
                      (.endsWith (.getName file) ".xia"))))
       (sort-by #(.getName ^File %))
       vec))

(deftest backup-enabled-defaults-to-true
  (db/delete-config! :backup/enabled?)
  (is (true? (backup/enabled?))))

(deftest run-backup-creates-portable-archive-from-safe-copy
  (let [backup-dir (temp-dir)]
    (db/set-config! :user/name "Backup Test")
    (db/set-config! :backup/directory backup-dir)
    (db/set-config! :backup/retain-count 5)
    (let [result   (backup/run-backup!)
          archive  (:archive-path result)
          manifest (pack/read-manifest archive)
          entries  (zip-entry-names archive)]
      (is (= :success (:status result)))
      (is (.exists (File. ^String archive)))
      (is (= (db/current-db-path) (:db-path manifest)))
      (is (= :prompt-passphrase (:key-source manifest)))
      (is (contains? entries "manifest.edn"))
      (is (contains? entries "db/.xia/master.salt"))
      (is (contains? entries "db/data.mdb"))
      (is (some? (backup/last-success-at)))
      (is (= archive (backup/last-archive-path)))
      (is (nil? (backup/last-error))))))

(deftest run-backup-prunes-old-managed-archives
  (let [backup-dir (temp-dir)]
    (db/set-config! :backup/directory backup-dir)
    (db/set-config! :backup/retain-count 1)
    (let [first-result  (backup/run-backup!)
          first-archive (:archive-path first-result)]
      (Thread/sleep 10)
      (let [second-result  (backup/run-backup!)
            second-archive (:archive-path second-result)
            archives       (managed-backup-files backup-dir)]
        (is (= :success (:status first-result)))
        (is (= :success (:status second-result)))
        (is (not= first-archive second-archive))
        (is (= 1 (count archives)))
        (is (= [second-archive] (mapv #(.getAbsolutePath ^File %) archives)))
        (is (false? (.exists (File. ^String first-archive))))
        (is (= [first-archive] (:pruned-paths second-result)))))))

(deftest backup-due-respects-configured-interval
  (let [base (Instant/parse "2026-03-19T00:00:00Z")]
    (db/set-config! :backup/enabled? true)
    (db/delete-config! :backup/last-attempt-at)
    (db/delete-config! :backup/interval-hours)
    (is (true? (backup/backup-due? base)))
    (db/set-config! :backup/last-attempt-at (str base))
    (db/set-config! :backup/interval-hours 12)
    (is (false? (backup/backup-due? (.plusSeconds base (* 11 60 60)))))
    (is (true? (backup/backup-due? (.plusSeconds base (* 13 60 60)))))))
