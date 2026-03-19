(ns xia.extractive-summary-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [xia.extractive-summary :as extractive-summary]))

(deftest summarize-text-keeps-heading-and-salient-facts
  (let [text "Deployment Readiness\n\nInitial setup is complete. Cutover is scheduled for April 18 at 02:00 UTC, and Dana owns rollback readiness. Background notes follow for the staging audit."
        summary (extractive-summary/summarize-text text 180)]
    (is (string? summary))
    (is (str/includes? summary "Deployment Readiness"))
    (is (str/includes? summary "April 18"))
    (is (str/includes? summary "Dana"))))

(deftest summarize-text-falls-back-to-leading-fragment-when-plain
  (let [text "Alpha systems provide detailed evidence about the vehicle launch plan and milestone checkpoints without paragraph structure"
        summary (extractive-summary/summarize-text text 120)]
    (is (str/starts-with? summary "Alpha systems provide detailed evidence"))
    (is (<= (count summary) 120))))

(deftest summarize-document-prefers-first-and-salient-later-chunks
  (let [chunk-summaries ["Migration overview and sequence for the tenant cutover."
                         "Cutover is scheduled for April 18 at 02:00 UTC, and Dana owns rollback readiness."
                         "Appendix notes and glossary entries."]
        summary (extractive-summary/summarize-document chunk-summaries
                                                       ""
                                                       220)]
    (testing "keeps the opening subject"
      (is (str/includes? summary "Migration overview")))
    (testing "pulls in the later chunk with the strongest retrieval facts"
      (is (str/includes? summary "April 18"))
      (is (str/includes? summary "Dana")))))
