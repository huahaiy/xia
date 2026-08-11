(ns xia.pipeline-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.pipeline :as pipeline]
            [xia.test-helpers :as th]
            [xia.tool :as tool]))

(use-fixtures :each th/with-test-db)

(deftest restricted-pipeline-runs-tools-and-returns-final-output
  (let [calls (atom [])]
    (is (= {:titles ["Alpha" "Beta"]}
           (pipeline/run-pipeline!
            {:code "(let [r (call-tool :web-search {\"query\" (:query input)})]
                      {:titles (mapv #(get % \"title\") (get r \"results\"))})"
             :input {:query "xia"}
             :invoke-tool (fn [tool-id arguments context]
                            (swap! calls conj {:tool-id tool-id
                                               :arguments arguments
                                               :context context})
                            {"results" [{"title" "Alpha"
                                         "snippet" "Hidden intermediate text"}
                                        {"title" "Beta"
                                         "snippet" "More hidden text"}]})})))
    (is (= [{:tool-id :web-search
             :arguments {"query" "xia"}
             :context {}}]
           @calls))))

(deftest restricted-pipeline-blocks-non-whitelisted-tools
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"not allowed in restricted pipelines"
       (pipeline/run-pipeline!
        {:code "(call-tool :email-send {\"to\" \"person@example.com\"})"
         :invoke-tool (fn [& _]
                        (throw (ex-info "should not be called" {})))}))))

(deftest restricted-pipeline-enforces-call-limit
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"tool call limit"
       (pipeline/run-pipeline!
        {:code "(do
                  (call-tool :web-search {\"query\" \"one\"})
                  (call-tool :web-search {\"query\" \"two\"}))"
         :max-calls 1
         :invoke-tool (fn [_ _ _]
                        {})}))))

(deftest pipeline-run-tool-returns-only-the-final-structured-value
  (db/install-tool! {:id          :web-search
                     :name        "web-search"
                     :description "Stub search"
                     :approval    :auto
                     :handler     "(fn [_]
                                     {\"results\" [{\"title\" \"Alpha\"
                                                    \"snippet\" \"intermediate\"}]
                                      \"summary\" \"intermediate summary\"})"})
  (db/install-tool! {:id          :pipeline-run
                     :name        "pipeline-run"
                     :description "Pipeline runner"
                     :approval    :auto
                     :handler     "(fn [args] (xia.pipeline/run! args))"})
  (tool/load-tool! :web-search)
  (tool/load-tool! :pipeline-run)
  (is (= {:count 1
          :titles ["Alpha"]}
         (tool/execute-tool
          :pipeline-run
          {"code" "(let [r (call-tool :web-search {\"query\" \"xia\"})]
                     {:count (count (get r \"results\"))
                      :titles (mapv #(get % \"title\") (get r \"results\"))})"}
          {:channel :terminal}))))
