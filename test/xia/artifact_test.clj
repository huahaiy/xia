(ns xia.artifact-test
  (:require [clojure.test :refer :all]
            [xia.artifact :as artifact]
            [xia.db :as db]
            [xia.memory :as memory]
            [xia.test-helpers :refer [with-test-db]]
            [xia.working-memory :as wm])
  (:import [java.nio.charset StandardCharsets]
           [java.util Arrays Base64]))

(use-fixtures :each with-test-db)

(deftest artifacts-are-session-scoped-readable-and-episodic
  (let [sid-a   (db/create-session! :http)
        sid-b   (db/create-session! :http)
        created (artifact/create-artifact! {:session-id sid-a
                                            :name "report.md"
                                            :title "Research Brief"
                                            :kind :markdown
                                            :content "# Findings\n\nAlpha"})]
    (is (= "report.md" (:name created)))
    (is (= "Research Brief" (:title created)))
    (is (= :markdown (:kind created)))
    (is (= "text/markdown" (:media-type created)))
    (is (= [(:id created)] (mapv :id (artifact/list-artifacts sid-a))))
    (is (= [] (artifact/list-artifacts sid-b)))
    (is (= "# Findings\n\nAlpha" (:text (artifact/get-session-artifact sid-a (:id created)))))
    (is (nil? (artifact/get-session-artifact sid-b (:id created))))
    (binding [wm/*session-id* sid-a]
      (let [slice (artifact/read-artifact (:id created) :max-chars 11)]
        (is (= "report.md" (:name slice)))
        (is (= "# Findings\n" (:text slice)))
        (is (= 11 (:end-offset slice)))
        (is (true? (:truncated? slice)))))
    (artifact/delete-artifact! sid-a (:id created))
    (is (nil? (artifact/get-session-artifact sid-a (:id created))))
    (is (= #{"Created artifact report.md"
             "Deleted artifact report.md"}
           (set (map :summary (memory/recent-episodes 10)))))))

(deftest structured-artifacts-normalize-json-and-csv
  (let [sid   (db/create-session! :http)
        json* (artifact/create-artifact! {:session-id sid
                                          :title "Metrics"
                                          :kind :json
                                          :data {"wins" 3
                                                 "losses" 1}})
        csv*  (artifact/create-artifact! {:session-id sid
                                          :name "scores.csv"
                                          :kind :csv
                                          :rows [{"name" "Ada" "score" 3}
                                                 {"name" "Lin" "score" 4}]})]
    (is (= "Metrics" (:title json*)))
    (is (= "application/json" (:media-type json*)))
    (is (= "json" (:extension json*)))
    (is (.contains ^String (:text json*) "\"wins\": 3"))
    (is (= "scores.csv" (:name csv*)))
    (is (= "text/csv" (:media-type csv*)))
    (is (= "csv" (:extension csv*)))
    (is (= "name,score\nAda,3\nLin,4" (:text csv*)))))

(deftest binary-artifacts-use-compressed-blobs
  (let [sid     (db/create-session! :http)
        payload (.getBytes "%PDF-1.7\nartifact\n" StandardCharsets/UTF_8)
        encoded (.encodeToString (Base64/getEncoder) payload)
        created (artifact/create-artifact! {:session-id sid
                                            :name "report.pdf"
                                            :kind :pdf
                                            :bytes-base64 encoded})]
    (is (= "report.pdf" (:name created)))
    (is (= :pdf (:kind created)))
    (is (= "application/pdf" (:media-type created)))
    (is (nil? (:text created)))
    (is (false? (:text-available? created)))
    (is (true? (:has-blob? created)))
    (is (= :zstd (:blob-codec created)))
    (is (pos? (:compressed-size-bytes created)))
    (is (.contains ^String (:preview created) "Binary artifact"))
    (binding [wm/*session-id* sid]
      (let [slice (artifact/read-artifact (:id created))]
        (is (nil? (:text slice)))
        (is (false? (:text-available? slice)))
        (is (true? (:has-blob? slice)))))
    (let [download (artifact/artifact-download-data sid (:id created))]
      (is (= "report.pdf" (:name download)))
      (is (= "application/pdf" (:media-type download)))
      (is (Arrays/equals ^bytes payload ^bytes (:bytes download))))
    (is (some? (#'xia.artifact/load-blob-bytes (:blob-id created) (:blob-codec created))))
    (artifact/delete-artifact! sid (:id created))
    (is (nil? (#'xia.artifact/load-blob-bytes (:blob-id created) (:blob-codec created))))))

(deftest artifacts-can-create-scratch-pads-and-record-events
  (let [sid     (db/create-session! :http)
        created (artifact/create-artifact! {:session-id sid
                                            :name "brief.md"
                                            :title "Research Brief"
                                            :kind :markdown
                                            :content "# Brief\n\nAlpha"})
        {:keys [artifact pad]} (artifact/create-scratch-pad-from-artifact! sid (:id created))]
    (is (= (:id created) (:id artifact)))
    (is (= "Research Brief" (:title pad)))
    (is (= "# Brief\n\nAlpha" (:content pad)))
    (is (contains? (set (map :summary (memory/recent-episodes 10)))
                   "Created note from artifact brief.md"))))
