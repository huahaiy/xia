(ns xia.local-doc-smoke-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.local-doc :as local-doc]
            [xia.local-ocr :as local-ocr]
            [xia.memory :as memory]
            [xia.test-helpers :refer [minimal-pdf-base64 with-test-db]])
  (:import [java.nio.charset StandardCharsets]))

(use-fixtures :each with-test-db)

(deftest local-documents-are-searchable-at-rest-and-session-scoped
  (let [sid-a  (db/create-session! :http)
        sid-b  (db/create-session! :http)
        saved  (local-doc/save-upload! {:session-id sid-a
                                        :name "notes.md"
                                        :media-type "text/markdown"
                                        :size-bytes 11
                                        :text "# secret"})]
    (is (= [(:id saved)]
           (mapv :id (local-doc/list-docs sid-a))))
    (is (= [] (local-doc/list-docs sid-b)))
    (is (= "# secret" (:text (local-doc/get-session-doc sid-a (:id saved)))))
    (is (nil? (local-doc/get-session-doc sid-b (:id saved))))
    (let [episodes (memory/recent-episodes 5)
          upload   (first episodes)]
      (is (= 1 (count episodes)))
      (is (= :event (:type upload)))
      (is (= "Uploaded local document notes.md" (:summary upload))))))

(deftest pdf-uploads-are-extracted-server-side
  (let [sid   (db/create-session! :http)
        saved (local-doc/save-upload! {:session-id sid
                                       :name "paper.pdf"
                                       :media-type "application/pdf"
                                       :bytes-base64 (minimal-pdf-base64 "Hello PDF world")})]
    (is (= "application/pdf" (:media-type saved)))
    (is (.contains ^String (:text saved) "Hello PDF world"))
    (is (.contains ^String (:preview saved) "Hello PDF world"))))

(deftest image-uploads-use-local-ocr
  (let [sid   (db/create-session! :http)
        calls (atom [])]
    (with-redefs [local-ocr/ocr-image-bytes (fn [_bytes opts]
                                              (swap! calls conj opts)
                                              "Invoice 42\nTotal due")]
      (let [saved (local-doc/save-upload! {:session-id sid
                                           :name "invoice.png"
                                           :media-type "image/png"
                                           :bytes (.getBytes "fake-image" StandardCharsets/UTF_8)})]
        (is (= "image/png" (:media-type saved)))
        (is (= "Invoice 42\nTotal due" (:text saved)))
        (is (.contains ^String (:summary saved) "Invoice 42"))
        (is (.contains ^String (:preview saved) "Total due"))
        (is (= :ocr (:ocr-mode (first @calls))))
        (is (= "image/png" (:media-type (first @calls))))))))
