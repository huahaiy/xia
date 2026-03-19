(ns xia.summarizer-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [xia.db :as db]
            [xia.summarizer :as summarizer]))

(deftest model-summaries-are-disabled-by-default
  (with-redefs [db/get-config (constantly nil)]
    (is (false? (summarizer/enabled?)))))

(deftest chunk-prompt-is-xia-specific
  (let [prompt (summarizer/build-chunk-prompt
                 "Atlas launch is scheduled for June 12. Priya owns readiness.")]
    (is (str/includes? prompt "retrieval summary for Xia"))
    (is (str/includes? prompt "names, headings, dates, numbers, owners, statuses, decisions, risks"))
    (is (str/includes? prompt "Atlas launch is scheduled for June 12"))
    (is (str/ends-with? prompt "Retrieval summary:"))))

(deftest document-prompt-uses-document-purpose
  (let [prompt (summarizer/build-document-prompt
                 "Chunk 1: Atlas launch is scheduled for June 12."
                 :chunk-summaries)]
    (is (str/includes? prompt "document-level retrieval summary for Xia"))
    (is (str/includes? prompt "Evidence from document sections:"))
    (is (str/includes? prompt "Chunk 1: Atlas launch is scheduled for June 12."))
    (is (str/ends-with? prompt "Document retrieval summary:"))))
