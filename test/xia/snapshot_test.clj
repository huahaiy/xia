(ns xia.snapshot-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.db :as db]
            [xia.paths :as paths]
            [xia.snapshot :as snapshot]
            [xia.test-helpers :as th])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-dir
  []
  (str (Files/createTempDirectory "xia-snapshot-test"
                                  (into-array FileAttribute []))))

(defn- write-file!
  [path content]
  (let [file (io/file path)]
    (.mkdirs (.getParentFile file))
    (spit file content)))

(defn- connect-options
  []
  (th/test-connect-options
   {:passphrase-provider (constantly "xia-test-passphrase")}))

(use-fixtures :each
  (fn [f]
    (db/clear-runtime!)
    (try
      (f)
      (finally
        (when (db/connected?)
          (db/close!))
        (db/clear-runtime!)))))

(deftest safety-snapshot-restores-db-and-shared-workspace
  (let [root           (temp-dir)
        db-path        (str root "/db")
        snapshot-root  (str root "/snapshots")
        workspace-root (str root "/workspace-root")
        workspace-file (str workspace-root "/default/items/item-1/note.md")]
    (with-redefs-fn {#'xia.paths/env-value
                     (fn [k]
                       (when (= "XIA_WORKSPACE_ROOT" k)
                         workspace-root))}
      (fn []
      (write-file! workspace-file "known-good workspace")
      (db/connect! db-path (connect-options))
      (db/set-config! :user/name "known-good")
      (let [manifest (snapshot/create-snapshot! :db-path db-path
                                                :snapshot-root snapshot-root
                                                :label "before risky work")]
        (is (some? (:snapshot/id manifest)))
        (is (= "before risky work" (:snapshot/label manifest)))
        (is (true? (get-in manifest [:workspace :included?])))
        (is (.isFile (io/file (:snapshot/path manifest) "db.xia")))
        (is (.isFile (io/file (:snapshot/path manifest)
                              "workspace/default/items/item-1/note.md")))

        (db/set-config! :user/name "polluted")
        (write-file! workspace-file "polluted workspace")
        (db/close!)

        (let [result (snapshot/restore-snapshot! (:snapshot/id manifest)
                                                 :db-path db-path
                                                 :snapshot-root snapshot-root
                                                 :force? true)]
          (is (= :restored (:status result)))
          (is (string? (get-in result [:db :moved-aside])))
          (is (string? (get-in result [:workspace :moved-aside]))))

        (db/connect! db-path (connect-options))
        (is (= "known-good" (db/get-config :user/name)))
        (is (= "known-good workspace" (slurp workspace-file))))))))

(deftest safety-snapshot-can-skip-shared-workspace
  (let [root           (temp-dir)
        db-path        (str root "/db")
        snapshot-root  (str root "/snapshots")
        workspace-root (str root "/workspace-root")
        workspace-file (str workspace-root "/default/items/item-1/note.md")]
    (with-redefs-fn {#'xia.paths/env-value
                     (fn [k]
                       (when (= "XIA_WORKSPACE_ROOT" k)
                         workspace-root))}
      (fn []
      (write-file! workspace-file "not snapshotted")
      (db/connect! db-path (connect-options))
      (db/set-config! :user/name "db-only")
      (let [manifest (snapshot/create-snapshot! :db-path db-path
                                                :snapshot-root snapshot-root
                                                :label "db only"
                                                :include-workspace? false)]
        (is (= "db only" (:snapshot/label manifest)))
        (is (false? (get-in manifest [:workspace :included?])))
        (is (not (.exists (io/file (:snapshot/path manifest) "workspace")))))))))
