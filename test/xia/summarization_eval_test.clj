(ns xia.summarization-eval-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [datalevin.llm :as llm]
            [xia.summarization-eval :as eval]))

(defn- fixture-summary
  [prompt]
  (cond
    (str/includes? prompt "Atlas vehicle launch planning")
    "Atlas vehicle launch stays on track for June, with Priya owning readiness and battery shipment risk remaining."

    (str/includes? prompt "If a token is exposed")
    "Revoke exposed tokens immediately, rotate dependent secrets, audit recent access, and notify the incident lead after rotation."

    (str/includes? prompt "Northwind's support lead")
    "Northwind's renewal depends on fixing CSV validation, admin-role clarity, and migration guidance; pricing was not the main issue."

    (str/includes? prompt "Revenue reached $12M in Q2")
    "Q2 revenue reached $12M while churn fell and support satisfaction improved after onboarding changes."

    (str/includes? prompt "A Redis failover at 02:14 UTC")
    "A Redis failover caused stale billing reads; mitigation restored consistency, no payment data was lost, and follow-up work targets detection and alarms."

    (str/includes? prompt "The evaluation compared three retrieval prompts")
    "Prompt B should ship by default because it improved retrieval recall, while Prompt C hallucinated service intervals despite better explanations."

    (str/includes? prompt "Chunk 1: Q2 revenue reached $12M")
    "Q2 revenue reached $12M while churn fell and support satisfaction improved after onboarding changes."

    (str/includes? prompt "Chunk 1: A Redis failover caused stale billing reads")
    "A Redis failover caused stale billing reads; mitigation restored consistency, no payment data was lost, and follow-up work targets detection and alarms."

    (str/includes? prompt "Chunk 1: Prompt B should ship by default")
    "Prompt B should ship by default because it improved retrieval recall, while Prompt C hallucinated service intervals despite better explanations."

    :else
    "fallback summary"))

(defn- fixture-provider
  []
  (reify
    llm/ILLMProvider
    (generate-text* [_ prompt _max-tokens _opts]
      (fixture-summary prompt))
    (summarize-text* [_ text max-tokens opts]
      (fixture-summary text))
    (llm-metadata [_]
      {:llm/provider {:kind :test
                      :id :fixture-provider}})
    (llm-context-size [_]
      4096)
    (close-llm-provider [_]
      nil)

    java.lang.AutoCloseable
    (close [_]
      nil)))

(deftest load-dataset-test
  (let [dataset (eval/load-dataset {:id :xia-local-doc-fixtures
                                    :label "fixture dataset"
                                    :layout :edn
                                    :path "eval/summarization-fixtures.edn"})]
    (is (= :xia-local-doc-fixtures (:id dataset)))
    (is (= 6 (count (:samples dataset))))
    (is (= #{:chunk :document}
           (set (map :kind (:samples dataset)))))))

(deftest evaluate-benchmark-test
  (let [report (eval/evaluate-benchmark
                 {:dataset {:id :tiny-summaries
                            :label "tiny summaries"
                            :layout :edn
                            :path "eval/summarization-fixtures.edn"}
                  :models [{:id :extractive-baseline
                            :label "Extractive baseline"
                            :mode :extractive-baseline}
                           {:id :fixture-model
                            :label "Fixture provider"
                            :provider (fixture-provider)}]
                  :include-samples? true})]
    (testing "returns one result block per model"
      (is (= 2 (count (:results report)))))
    (testing "fixture provider produces high-quality summaries on the suite"
      (let [result (second (:results report))
            metrics (:metrics result)]
        (is (= 6 (:sample-count result)))
        (is (> (double (:required-coverage metrics)) 0.95))
        (is (> (double (:reference-token-f1 metrics)) 0.95))
        (is (= 0.0 (double (:forbidden-violation-rate metrics))))
        (is (= 1.0 (double (:budget-pass-rate metrics))))
        (is (= 6 (count (:samples result))))))
    (testing "extractive baseline still emits aggregate metrics"
      (let [result (first (:results report))]
        (is (= :extractive-baseline (:model-id result)))
        (is (map? (:metrics result)))
        (is (= #{:chunk :document}
               (set (keys (:by-kind result)))))))))
