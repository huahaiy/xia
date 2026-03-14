(ns xia.llm-test
  (:require [clojure.test :refer :all]
            [charred.api :as json]
            [xia.llm :as llm]))

(deftest build-request-sets-request-timeout
  (let [req  (#'llm/build-request {:base-url "https://api.example.com/v1"
                                   :api-key  "sk-test"
                                   :model    "gpt-test"}
                                  [{"role" "user" "content" "hello"}]
                                  {:temperature 0.2
                                   :max-tokens  128})
        body (json/read-json (:body req))]
    (is (= "https://api.example.com/v1/chat/completions" (:url req)))
    (is (= :post (:method req)))
    (is (= 120000 (:timeout req)))
    (is (= true (:retry-enabled? req)))
    (is (= "LLM request" (:request-label req)))
    (is (= "Bearer sk-test" (get-in req [:headers "Authorization"])))
    (is (= "application/json" (get-in req [:headers "Content-Type"])))
    (is (= "gpt-test" (get body "model")))
    (is (= 128 (get body "max_tokens")))))

(deftest resolve-provider-selection-round-robins-workload
  (with-redefs [xia.db/list-providers
                (constantly [{:llm.provider/id :openai-a
                              :llm.provider/workloads #{:assistant}}
                             {:llm.provider/id :openai-b
                              :llm.provider/workloads #{:assistant}}])
                xia.db/get-default-provider
                (constantly {:llm.provider/id :fallback})
                xia.llm/workload-counters
                (atom {})]
    (let [first-selection  (llm/resolve-provider-selection {:workload :assistant})
          second-selection (llm/resolve-provider-selection {:workload :assistant})
          third-selection  (llm/resolve-provider-selection {:workload :assistant})]
      (is (= :openai-a (:provider-id first-selection)))
      (is (= :openai-b (:provider-id second-selection)))
      (is (= :openai-a (:provider-id third-selection))))))

(deftest resolve-provider-selection-prefers-healthy-providers
  (let [future-ms (+ (System/currentTimeMillis) 60000)]
    (with-redefs [xia.db/list-providers
                  (constantly [{:llm.provider/id :openai-a
                                :llm.provider/workloads #{:assistant}}
                               {:llm.provider/id :openai-b
                                :llm.provider/workloads #{:assistant}}])
                  xia.db/get-default-provider
                  (constantly {:llm.provider/id :fallback})
                  xia.llm/workload-counters
                  (atom {})
                  xia.llm/provider-health
                  (atom {:openai-a {:consecutive-failures 2
                                    :cooldown-until-ms future-ms
                                    :last-error "rate limited"}})]
      (is (= :openai-b
             (:provider-id (llm/resolve-provider-selection {:workload :assistant})))))))

(deftest resolve-provider-selection-falls-back-to-default-when-workload-unassigned
  (with-redefs [xia.db/list-providers (constantly [])
                xia.db/get-default-provider (constantly {:llm.provider/id :default})]
    (is (= :default
           (:provider-id (llm/resolve-provider-selection {:workload :memory-summary}))))))

(deftest provider-health-summary-falls-back-to-configured-default-provider
  (let [future-ms (+ (System/currentTimeMillis) 60000)]
    (with-redefs [xia.db/get-default-provider (constantly {:llm.provider/id :fallback})
                  xia.llm/provider-health
                  (atom {:fallback {:consecutive-failures 2
                                    :cooldown-until-ms future-ms
                                    :last-error "rate limited"}})]
      (let [health (llm/provider-health-summary nil)]
        (is (= :fallback (:provider-id health)))
        (is (= :cooling-down (:status health)))
        (is (= 2 (:consecutive-failures health)))
        (is (= "rate limited" (:last-error health)))))))

(deftest provider-cooldown-ms-saturates-at-max-with-large-failure-counts
  (is (= (var-get #'llm/provider-health-base-cooldown-ms)
         (#'llm/provider-cooldown-ms 1)))
  (is (= (var-get #'llm/provider-health-max-cooldown-ms)
         (#'llm/provider-cooldown-ms 64)))
  (is (= (var-get #'llm/provider-health-max-cooldown-ms)
         (#'llm/provider-cooldown-ms 1000))))

(deftest chat-uses-workload-routed-provider
  (with-redefs [xia.db/list-providers
                (constantly [{:llm.provider/id :router-a
                              :llm.provider/base-url "https://api.example.com/v1"
                              :llm.provider/api-key "sk-test"
                              :llm.provider/model "gpt-route"
                              :llm.provider/workloads #{:assistant}}])
                xia.db/get-default-provider
                (constantly {:llm.provider/id :fallback})
                xia.llm/workload-counters
                (atom {})
                xia.llm/provider-health
                (atom {})
                xia.http-client/request
                (fn [req]
                  (is (= "https://api.example.com/v1/chat/completions" (:url req)))
                  {:status 200
                   :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"})]
    (is (= "ok"
           (llm/chat-simple [{"role" "user" "content" "hello"}]
                            :workload :assistant)))))

(deftest chat-fails-over-to-next-provider-and-records-health
  (let [requests (atom [])]
    (with-redefs [xia.db/list-providers
                  (constantly [{:llm.provider/id :openai-a
                                :llm.provider/base-url "https://a.example.com/v1"
                                :llm.provider/api-key "sk-a"
                                :llm.provider/model "gpt-a"
                                :llm.provider/workloads #{:assistant}}
                               {:llm.provider/id :openai-b
                                :llm.provider/base-url "https://b.example.com/v1"
                                :llm.provider/api-key "sk-b"
                                :llm.provider/model "gpt-b"
                                :llm.provider/workloads #{:assistant}}])
                  xia.db/get-default-provider
                  (constantly {:llm.provider/id :fallback})
                  xia.llm/workload-counters
                  (atom {})
                  xia.llm/provider-health
                  (atom {})
                  xia.http-client/request
                  (fn [req]
                    (swap! requests conj (:url req))
                    (if (= "https://a.example.com/v1/chat/completions" (:url req))
                      {:status 429
                       :body "rate limited"}
                      {:status 200
                       :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"}))]
      (is (= "ok"
             (llm/chat-simple [{"role" "user" "content" "hello"}]
                              :workload :assistant)))
      (is (= ["https://a.example.com/v1/chat/completions"
              "https://b.example.com/v1/chat/completions"]
             @requests))
      (let [health-a (llm/provider-health-summary :openai-a)
            health-b (llm/provider-health-summary :openai-b)]
        (is (= :cooling-down (:status health-a)))
        (is (= 1 (:consecutive-failures health-a)))
        (is (= :healthy (:status health-b)))))))
