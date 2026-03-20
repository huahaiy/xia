(ns xia.summarizer-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.summarizer :as summarizer]))

(deftest model-summaries-are-disabled-by-default
  (with-redefs [db/get-config (constantly nil)]
    (is (false? (summarizer/enabled?)))))

(deftest model-summary-backend-defaults-to-local
  (with-redefs [db/get-config (constantly nil)]
    (is (= :local (summarizer/summary-backend)))))

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

(deftest external-summaries-use-chat-provider-when-configured
  (let [seen (atom nil)]
    (with-redefs [db/get-config (fn [k]
                                  (case k
                                    :local-doc/model-summaries-enabled? "true"
                                    :local-doc/model-summary-backend "external"
                                    :local-doc/model-summary-provider-id "openai"
                                    nil))
                  db/current-llm-provider (constantly nil)
                  llm/chat-simple (fn [messages & opts]
                                    (reset! seen {:messages messages
                                                  :opts (apply hash-map opts)})
                                    "external summary")]
      (is (= "external summary"
             (summarizer/summarize-chunk "Atlas launch is scheduled for June 12." 200)))
      (is (= 1 (count (:messages @seen))))
      (is (= "user" (get-in @seen [:messages 0 "role"])))
      (is (str/includes? (get-in @seen [:messages 0 "content"])
                         "retrieval summary for Xia"))
      (is (str/includes? (get-in @seen [:messages 0 "content"])
                         "Atlas launch is scheduled for June 12."))
      (is (= {:provider-id :openai
              :max-tokens 96
              :temperature 0}
             (:opts @seen))))))
