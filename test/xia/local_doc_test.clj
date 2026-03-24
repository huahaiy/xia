(ns xia.local-doc-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [datalevin.core :as d]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.local-doc :as local-doc]
            [xia.local-ocr :as local-ocr]
            [xia.memory :as memory]
            [xia.test-helpers :refer [minimal-pdf-base64 office-fixture-base64 test-llm-provider with-test-db]]
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
    (is (= "# secret" (:local.doc/summary raw)))
    (is (= "# secret" (:local.doc/preview raw)))
    (is (= 1 (:local.doc/chunk-count raw)))
    (let [chunk-eids (->> (db/q '[:find ?chunk
                                  :in $ ?doc
                                  :where [?doc :local.doc/chunks ?chunk]]
                                eid)
                          (map first)
                          vec)]
      (is (= 1 (count chunk-eids)))
      (is (= #{[eid]}
             (db/q '[:find ?doc
                     :in $ ?chunk
                     :where [?chunk :local.doc.chunk/doc ?doc]]
                   (first chunk-eids)))))
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
      (is (.contains ^String (:context upload) "Summary: # secret"))
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

(deftest image-uploads-support-explicit-ocr-modes
  (let [sid   (db/create-session! :http)
        calls (atom [])]
    (with-redefs [local-ocr/ocr-image-bytes (fn [_bytes opts]
                                              (swap! calls conj opts)
                                              "x = y + z")]
      (let [saved (local-doc/save-upload! {:session-id sid
                                           :name "formula.jpg"
                                           :media-type "image/jpeg"
                                           :ocr-mode "formula"
                                           :bytes (.getBytes "formula-image" StandardCharsets/UTF_8)})]
        (is (= "x = y + z" (:text saved)))
        (is (= :formula (:ocr-mode (first @calls))))))))

(deftest image-uploads-do-not-dedupe-by-source-bytes
  (let [sid   (db/create-session! :http)
        bytes (.getBytes "shared-image" StandardCharsets/UTF_8)
        calls (atom 0)]
    (with-redefs [local-ocr/ocr-image-bytes (fn [_ opts]
                                              (swap! calls inc)
                                              (str "OCR run " @calls " " (name (:ocr-mode opts))))]
      (let [first-doc  (local-doc/save-upload! {:session-id sid
                                                :name "scan-a.png"
                                                :media-type "image/png"
                                                :bytes bytes})
            second-doc (local-doc/save-upload! {:session-id sid
                                                :name "scan-b.png"
                                                :media-type "image/png"
                                                :ocr-mode :table
                                                :bytes bytes})]
        (is (not= (:id first-doc) (:id second-doc)))
        (is (= 2 (count (local-doc/list-docs sid))))
        (is (= 2 @calls))))))

(deftest large-local-docs-are-chunked-and-return-matched-chunks
  (let [sid   (db/create-session! :http)
        para1 (str "Section One\n\n" (apply str (repeat 260 "alpha retrieval ")))
        para2 (str "Section Two\n\n" (apply str (repeat 260 "beta grounding ")))
        para3 (str "Section Three\n\n" (apply str (repeat 260 "hyperdrive evidence ")))
        text  (str para1 "\n\n" para2 "\n\n" para3)
        saved (local-doc/save-upload! {:session-id sid
                                       :name "long-notes.txt"
                                       :media-type "text/plain"
                                       :text text})
        eid   (ffirst (db/q '[:find ?e :in $ ?id :where [?e :local.doc/id ?id]]
                             (:id saved)))
        chunk-eids (->> (db/q '[:find ?chunk
                                :in $ ?doc
                                :where [?doc :local.doc/chunks ?chunk]]
                              eid)
                        (map first)
                        vec)
        chunk-texts (map (fn [chunk-eid]
                           (:local.doc.chunk/text (into {} (d/entity (d/db (db/conn)) chunk-eid))))
                         chunk-eids)]
    (is (> (:chunk-count saved) 1))
    (is (= (:chunk-count saved) (count chunk-eids)))
    (is (some #(str/starts-with? % "Section One") chunk-texts))
    (is (some #(str/starts-with? % "Section Three") chunk-texts))
    (binding [wm/*session-id* sid]
      (let [result (first (local-doc/search-docs "hyperdrive"))]
        (is (= (:id saved) (:id result)))
        (is (seq (:matched-chunks result)))
        (is (some #(re-find #"hyperdrive" (or (:summary %) (:preview % "")))
                  (:matched-chunks result)))))))

(deftest chunking-merges-heading-blocks-in-a-single-pass
  (let [sid   (db/create-session! :http)
        text  (str "Launch Plan\r\n\r\n"
                   "Priya owns the launch checklist and the June 12 milestone review.\r\n\r\n"
                   "Evidence\r\n\r\n"
                   (apply str (repeat 160 "Atlas telemetry confirms the readiness gate. ")))
        saved (local-doc/save-upload! {:session-id sid
                                       :name "headings.txt"
                                       :media-type "text/plain"
                                       :text text})
        eid   (ffirst (db/q '[:find ?e :in $ ?id :where [?e :local.doc/id ?id]]
                             (:id saved)))
        chunk-texts (->> (db/q '[:find ?chunk
                                 :in $ ?doc
                                 :where [?doc :local.doc/chunks ?chunk]]
                               eid)
                         (map first)
                         (map #(into {} (d/entity (d/db (db/conn)) %)))
                         (map :local.doc.chunk/text)
                         vec)]
    (is (some #(str/starts-with? % "Launch Plan Priya owns the launch checklist")
              chunk-texts))
    (is (some #(str/starts-with? % "Evidence Atlas telemetry confirms the readiness gate")
              chunk-texts))))

(deftest local-documents-default-to-extractive-summaries-even-with-a-local-llm
  (let [sid (db/create-session! :http)]
    (with-redefs [db/current-llm-provider (constantly (test-llm-provider))]
      (let [saved (local-doc/save-upload! {:session-id sid
                                           :name "brief.txt"
                                           :media-type "text/plain"
                                           :text "Alpha systems provide detailed evidence about the vehicle launch plan and milestone checkpoints."})]
        (is (= :extractive (:summary-source saved)))
        (is (not (str/starts-with? (:summary saved) "model-summary:")))))))

(deftest local-documents-use-model-summaries-only-when-enabled
  (let [sid (db/create-session! :http)]
    (db/set-config! :local-doc/model-summaries-enabled? true)
    (with-redefs [db/current-llm-provider (constantly (test-llm-provider))]
      (let [saved (local-doc/save-upload! {:session-id sid
                                           :name "brief.txt"
                                           :media-type "text/plain"
                                           :text "Alpha systems provide detailed evidence about the vehicle launch plan and milestone checkpoints."})
            eid   (ffirst (db/q '[:find ?e :in $ ?id :where [?e :local.doc/id ?id]]
                                 (:id saved)))
            raw   (into {} (d/entity (d/db (db/conn)) eid))
            chunk-eid (ffirst (db/q '[:find ?chunk
                                      :in $ ?doc
                                      :where [?doc :local.doc/chunks ?chunk]]
                                    eid))
            chunk (into {} (d/entity (d/db (db/conn)) chunk-eid))]
        (is (= :model (:summary-source saved)))
        (is (= :model (:local.doc/summary-source raw)))
        (is (= :model (:local.doc.chunk/summary-source chunk)))
        (is (str/starts-with? (:summary saved) "model-summary:"))
        (is (str/starts-with? (:local.doc.chunk/summary chunk) "model-summary:"))))))

(deftest local-documents-can-use-external-llm-summaries
  (let [sid   (db/create-session! :http)
        calls (atom [])]
    (db/set-config! :local-doc/model-summaries-enabled? true)
    (db/set-config! :local-doc/model-summary-backend "external")
    (db/set-config! :local-doc/model-summary-provider-id "openai")
    (with-redefs [db/current-llm-provider (constantly nil)
                  llm/chat-simple (fn [_messages & opts]
                                    (swap! calls conj (apply hash-map opts))
                                    "external-summary: atlas launch priya june 12")]
      (let [saved (local-doc/save-upload! {:session-id sid
                                           :name "brief.txt"
                                           :media-type "text/plain"
                                           :text "Atlas launch is scheduled for June 12. Priya owns readiness."})
            eid   (ffirst (db/q '[:find ?e :in $ ?id :where [?e :local.doc/id ?id]]
                                 (:id saved)))
            raw   (into {} (d/entity (d/db (db/conn)) eid))
            chunk-eid (ffirst (db/q '[:find ?chunk
                                      :in $ ?doc
                                      :where [?doc :local.doc/chunks ?chunk]]
                                    eid))
            chunk (into {} (d/entity (d/db (db/conn)) chunk-eid))]
        (is (= :model (:summary-source saved)))
        (is (= :model (:local.doc/summary-source raw)))
        (is (= :model (:local.doc.chunk/summary-source chunk)))
        (is (str/starts-with? (:summary saved) "external-summary:"))
        (is (every? #(= :openai (:provider-id %)) @calls))
        (is (every? #(zero? (long (:temperature %))) @calls))))))

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

(deftest visible-local-doc-retrieval-can-span-sessions
  (let [sid-a   (db/create-session! :http)
        sid-b   (db/create-session! :http)
        older   (local-doc/save-upload! {:session-id sid-a
                                         :name "older.md"
                                         :media-type "text/markdown"
                                         :text "# Older\n\nAlpha notes"})
        current (local-doc/save-upload! {:session-id sid-b
                                         :name "current.md"
                                         :media-type "text/markdown"
                                         :text "# Current\n\nBeta notes"})]
    (binding [wm/*session-id* sid-b]
      (is (= [(:id current) (:id older)]
             (mapv :id (local-doc/list-visible-docs :top 5))))
      (is (= (:id older)
             (:id (first (local-doc/search-visible-docs "Alpha")))))
      (let [slice (local-doc/read-visible-doc (:id older) :max-chars 8)]
        (is (= "older.md" (:name slice)))
        (is (= (str sid-a) (:session-id slice)))
        (is (= "# Older\n" (:text slice)))))))
