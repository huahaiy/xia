(ns xia.local-doc-test
  (:require [clojure.test :refer :all]
            [datalevin.core :as d]
            [xia.db :as db]
            [xia.local-doc :as local-doc]
            [xia.memory :as memory]
            [xia.test-helpers :refer [minimal-pdf-base64 office-fixture-base64 with-test-db]]
            [xia.working-memory :as wm])
  (:import [java.nio.charset StandardCharsets]
           [java.util Base64]))

(use-fixtures :each with-test-db)

(deftest local-documents-are-searchable-at-rest-and-session-scoped
  (let [sid-a  (db/create-session! :http)
        sid-b  (db/create-session! :http)
        saved  (local-doc/save-upload! {:session-id sid-a
                                        :name "notes.md"
                                        :media-type "text/markdown"
                                        :size-bytes 11
                                        :text "# secret"})
        eid    (ffirst (db/q '[:find ?e :in $ ?id :where [?e :local.doc/id ?id]]
                              (:id saved)))
        raw    (into {} (d/entity (d/db (db/conn)) eid))]
    (is (= "notes.md" (:local.doc/name raw)))
    (is (= "# secret" (:local.doc/text raw)))
    (is (= "# secret" (:local.doc/preview raw)))
    (is (= [(:id saved)]
           (mapv :id (local-doc/list-docs sid-a))))
    (is (= [] (local-doc/list-docs sid-b)))
    (is (= "# secret" (:text (local-doc/get-session-doc sid-a (:id saved)))))
    (is (nil? (local-doc/get-session-doc sid-b (:id saved))))
    (let [episodes (memory/recent-episodes 5)
          upload   (first episodes)]
      (is (= 1 (count episodes)))
      (is (= :event (:type upload)))
      (is (= "Uploaded local document notes.md" (:summary upload)))
      (is (.contains ^String (:context upload) "Media type: text/markdown"))
      (is (.contains ^String (:context upload) "Preview: # secret")))))

(deftest local-document-reupload-dedupes-by-session-and-content
  (let [sid    (db/create-session! :http)
        first  (local-doc/save-upload! {:session-id sid
                                        :name "first.txt"
                                        :media-type "text/plain"
                                        :text "same content"})
        second (local-doc/save-upload! {:session-id sid
                                        :name "second.txt"
                                        :media-type "text/plain"
                                        :text "same content"})]
    (is (= (:id first) (:id second)))
    (is (= 1 (count (local-doc/list-docs sid))))
    (is (= "second.txt" (:name (local-doc/get-session-doc sid (:id first)))))
    (let [episodes (memory/recent-episodes 10)]
      (is (= 2 (count episodes)))
      (is (= #{"Uploaded local document first.txt"
               "Uploaded local document second.txt"}
             (set (map :summary episodes)))))))

(deftest pdf-uploads-are-extracted-server-side
  (let [sid (db/create-session! :http)]
    (let [saved (local-doc/save-upload! {:session-id sid
                                         :name "paper.pdf"
                                         :media-type "application/pdf"
                                         :bytes-base64 (minimal-pdf-base64 "Hello PDF world")})]
      (is (= "application/pdf" (:media-type saved)))
      (is (.contains ^String (:text saved) "Hello PDF world"))
      (is (.contains ^String (:preview saved) "Hello PDF world")))))

(deftest docx-uploads-are-extracted-and-searchable
  (let [sid (db/create-session! :http)
        saved (local-doc/save-upload! {:session-id sid
                                       :name "paper.docx"
                                       :media-type "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                                       :bytes-base64 (office-fixture-base64 :docx)})]
    (is (= "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
           (:media-type saved)))
    (is (.contains ^String (:text saved) "Automobile research brief"))
    (binding [wm/*session-id* sid]
      (is (= "paper.docx" (:name (first (local-doc/search-docs "car"))))))))

(deftest xlsx-uploads-are-extracted-and-searchable
  (let [sid (db/create-session! :http)
        saved (local-doc/save-upload! {:session-id sid
                                       :name "table.xlsx"
                                       :media-type "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                       :bytes-base64 (office-fixture-base64 :xlsx)})]
    (is (= "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
           (:media-type saved)))
    (is (.contains ^String (:text saved) "## Sheet: Revenue"))
    (is (.contains ^String (:text saved) "Quarter\tAmount"))
    (is (.contains ^String (:text saved) "Q2\t55"))
    (binding [wm/*session-id* sid]
      (is (= "table.xlsx" (:name (first (local-doc/search-docs "Revenue"))))))))

(deftest pptx-uploads-are-extracted-and-searchable
  (let [sid (db/create-session! :http)
        saved (local-doc/save-upload! {:session-id sid
                                       :name "deck.pptx"
                                       :media-type "application/vnd.openxmlformats-officedocument.presentationml.presentation"
                                       :bytes-base64 (office-fixture-base64 :pptx)})]
    (is (= "application/vnd.openxmlformats-officedocument.presentationml.presentation"
           (:media-type saved)))
    (is (.contains ^String (:text saved) "## Slide 1"))
    (is (.contains ^String (:text saved) "Project Update"))
    (is (.contains ^String (:text saved) "vehicle testing"))
    (binding [wm/*session-id* sid]
      (is (= "deck.pptx" (:name (first (local-doc/search-docs "roadmap"))))))))

(deftest invalid-pdf-uploads-fail-clearly
  (let [sid (db/create-session! :http)]
    (try
      (local-doc/save-upload! {:session-id sid
                               :name "broken.pdf"
                               :media-type "application/pdf"
                               :bytes-base64 (.encodeToString (Base64/getEncoder)
                                                              (.getBytes "not a real pdf"
                                                                         StandardCharsets/UTF_8))})
      (is false "Expected invalid PDF rejection")
      (catch clojure.lang.ExceptionInfo e
        (is (= :local-doc/pdf-extraction-failed
               (:type (ex-data e))))))))

(deftest failed-local-document-uploads-can-be-persisted
  (let [sid (db/create-session! :http)
        err (ex-info "Unsupported local document format"
                     {:type :local-doc/unsupported-format})
        doc (local-doc/save-failed-upload! {:session-id sid
                                            :name "bad.bin"
                                            :media-type "application/octet-stream"
                                            :bytes (.getBytes "raw" StandardCharsets/UTF_8)}
                                           err)]
    (is (= :failed (:status doc)))
    (is (= "bad.bin" (:name doc)))
    (is (= "Unsupported local document format" (:error doc)))
    (is (= [(:id doc)] (mapv :id (local-doc/list-docs sid))))
    (is (= #{"Failed to upload local document bad.bin"}
           (set (map :summary (memory/recent-episodes 5)))))))

(deftest local-document-note-and-delete-actions-create-event-episodes
  (let [sid   (db/create-session! :http)
        saved (local-doc/save-upload! {:session-id sid
                                       :name "research.txt"
                                       :media-type "text/plain"
                                       :text "interesting notes"})
        {:keys [pad]} (local-doc/create-scratch-pad-from-doc! sid (:id saved))]
    (is (= "research.txt" (:title pad)))
    (is (= "interesting notes" (:content pad)))
    (local-doc/delete-doc! sid (:id saved))
    (is (nil? (local-doc/get-session-doc sid (:id saved))))
    (is (= #{"Uploaded local document research.txt"
             "Created note from local document research.txt"
             "Deleted local document research.txt"}
           (set (map :summary (memory/recent-episodes 10)))))))
