(ns xia.checkpoint-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.checkpoint :as checkpoint]
            [xia.db :as db]
            [xia.paths :as paths]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(use-fixtures :each with-test-db)

(defn- temp-dir
  []
  (str (Files/createTempDirectory "xia-checkpoint-test"
                                  (into-array FileAttribute []))))

(deftest create-online-checkpoint-stages-safe-copy-without-secret-files
  (let [support-dir      (paths/support-dir-path (db/current-db-path))
        checkpoint-root  (temp-dir)
        manifest-before? (.exists (io/file support-dir "master.salt"))]
    (spit (io/file support-dir "master.key") "should-not-be-copied")
    (spit (io/file support-dir "master.passphrase") "should-not-be-copied")
    (db/set-config! :user/name "Checkpoint Test")
    (let [{:keys [checkpoint_id staged_path staged_db_path manifest_path ready_marker_path
                  source_db_path support_files status key_source restore_requires
                  includes_secret_material]} (checkpoint/create-online-checkpoint!
                                               {:staging-root checkpoint-root})
          manifest (edn/read-string (slurp manifest_path))]
      (is (= :ready status))
      (is (string? checkpoint_id))
      (is (= (.getAbsolutePath (io/file (db/current-db-path))) source_db_path))
      (is (= staged_path (:staged_path manifest)))
      (is (= staged_db_path (:staged_db_path manifest)))
      (is (.exists (File. ^String staged_path)))
      (is (.exists (File. ^String staged_db_path)))
      (is (.exists (File. ^String manifest_path)))
      (is (.exists (File. ^String ready_marker_path)))
      (is (.exists (io/file staged_db_path "data.mdb")))
      (is (false? (.exists (io/file staged_db_path ".xia" "master.key"))))
      (is (false? (.exists (io/file staged_db_path ".xia" "master.passphrase"))))
      (is (= manifest-before?
             (.exists (io/file staged_db_path ".xia" "master.salt"))))
      (is (= (if manifest-before? ["master.salt"] [])
             support_files))
      (is (= false includes_secret_material))
      (is (= "prompt-passphrase" key_source))
      (is (= ["Provide the same master passphrase before opening the checkpoint DB."]
             restore_requires))
      (is (= {:kind :datalevin-copy :version 1}
             (:format manifest))))))
