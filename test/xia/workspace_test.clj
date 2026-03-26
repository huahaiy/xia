(ns xia.workspace-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.artifact :as artifact]
            [xia.db :as db]
            [xia.local-doc :as local-doc]
            [xia.paths :as paths]
            [xia.test-helpers :refer [minimal-pdf-base64 with-test-db]]
            [xia.working-memory :as wm]
            [xia.workspace :as workspace])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(use-fixtures :each with-test-db)

(defn- with-temp-workspace-root*
  [f]
  (let [root (str (Files/createTempDirectory "xia-workspace-test"
                                             (into-array FileAttribute [])))]
    (with-redefs [paths/shared-workspace-root (constantly root)]
      (f root))))

(deftest notes-can-be-written-listed-and-read-from-shared-workspaces
  (with-temp-workspace-root*
    (fn [root]
      (let [sid    (db/create-session! :terminal)
            item   (binding [wm/*session-id* sid]
                     (workspace/write-note! "alpha\nbeta\ngamma"
                                            :title "Shared plan"))
            listed (workspace/list-items)
            slice  (workspace/read-item (:id item) :max-chars 5)]
        (is (= (workspace/workspace-dir) (paths/path-str root "default")))
        (is (= :note (:source-type item)))
        (is (= "default" (:workspace-id item)))
        (is (= "default" (:producer-instance-id item)))
        (is (= (str sid) (:producer-session-id item)))
        (is (str/ends-with? (:name item) ".md"))
        (is (= [(:id item)] (mapv :id listed)))
        (is (= "alpha" (:text slice)))
        (is (true? (:truncated? slice)))))))

(deftest published-artifacts-can-be-imported-into-another-session
  (with-temp-workspace-root*
    (fn [_root]
      (let [sid-a    (db/create-session! :terminal)
            sid-b    (db/create-session! :terminal)
            created  (artifact/create-artifact! {:session-id sid-a
                                                 :name "report.md"
                                                 :kind :markdown
                                                 :content "# Findings\n\nAlpha"})
            shared   (binding [wm/*session-id* sid-b]
                       (workspace/publish-artifact! (:id created)))
            imported (binding [wm/*session-id* sid-b]
                       (workspace/import-item-as-artifact! (:id shared)))]
        (is (= :artifact (:source-type shared)))
        (is (= (str (:id created)) (:source-id shared)))
        (is (= "report.md" (:name shared)))
        (is (= "report.md" (:name imported)))
        (is (= "# Findings\n\nAlpha" (:text (artifact/get-session-artifact sid-b (:id imported)))))
        (is (= {:workspace-item-id (str (:id shared))
                :workspace-id "default"
                :workspace-source-type :artifact
                :workspace-source-id (:source-id shared)}
               (:meta imported)))))))

(deftest published-local-docs-preserve-derived-text-provenance
  (with-temp-workspace-root*
    (fn [_root]
      (let [sid-a    (db/create-session! :terminal)
            sid-b    (db/create-session! :terminal)
            saved    (local-doc/save-upload! {:session-id sid-a
                                              :name "paper.pdf"
                                              :media-type "application/pdf"
                                              :bytes-base64 (minimal-pdf-base64 "Hello workspace PDF")})
            shared   (binding [wm/*session-id* sid-b]
                       (workspace/publish-local-doc! (:id saved)))
            imported (binding [wm/*session-id* sid-b]
                       (workspace/import-item-as-local-doc! (:id shared)))]
        (is (= :local-doc (:source-type shared)))
        (is (= "text/plain" (:media-type shared)))
        (is (true? (:text-derived? shared)))
        (is (= "paper.pdf" (:original-name shared)))
        (is (= "application/pdf" (:original-media-type shared)))
        (is (str/ends-with? (:name shared) ".txt"))
        (is (= "text/plain" (:media-type imported)))
        (is (.contains ^String (:text imported) "Hello workspace PDF"))))))
