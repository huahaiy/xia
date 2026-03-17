(ns xia.local-doc-test
  (:require [clojure.test :refer :all]
            [datalevin.core :as d]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.local-doc :as local-doc]
            [xia.memory :as memory]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(deftest local-documents-are-encrypted-at-rest-and-session-scoped
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
    (is (crypto/encrypted? (:local.doc/name raw)))
    (is (crypto/encrypted? (:local.doc/text raw)))
    (is (crypto/encrypted? (:local.doc/preview raw)))
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

(deftest pdf-uploads-are-rejected-clearly
  (let [sid (db/create-session! :http)]
    (try
      (local-doc/save-upload! {:session-id sid
                               :name "paper.pdf"
                               :media-type "application/pdf"
                               :text "ignored"})
      (is false "Expected PDF upload rejection")
      (catch clojure.lang.ExceptionInfo e
        (is (= :local-doc/pdf-not-supported (:type (ex-data e))))))))

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
