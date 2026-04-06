(ns xia.llm-smoke-test
  (:require [clojure.test :refer :all]
            [xia.llm :as llm]))

(deftest provider-access-mode-normalizes-runtime-model
  (is (= :api
         (llm/provider-access-mode {:llm.provider/access-mode :account
                                    :llm.provider/credential-source :oauth-account})))
  (is (= :api
         (llm/provider-access-mode {:llm.provider/access-mode :api
                                    :llm.provider/credential-source :browser-session}))))

(deftest fetch-provider-model-metadata-infers-context-window-and-recommended-budgets
  (with-redefs [xia.http-client/request
                (fn [_req]
                  {:status 200
                   :body (str "{\"id\":\"test-model\","
                              "\"context_length\":200000,"
                              "\"max_input_tokens\":128000,"
                              "\"capabilities\":{\"vision\":false}}")})]
    (is (= {:id "test-model"
            :vision? false
            :vision-source :metadata
            :context-window 128000
            :context-window-source :metadata
            :recommended-system-prompt-budget 24000
            :recommended-history-budget 72000
            :recommended-input-budget-cap 96000}
           (llm/fetch-provider-model-metadata {:base-url "https://api.example.com/v1"
                                               :model "test-model"})))))

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

(deftest chat-enforces-provider-rate-limit-before-http-request
  (let [request-count    (atom 0)
        provider-health* (atom {})
        decisions        (atom [])]
    (with-redefs [xia.db/get-default-provider
                  (constantly {:llm.provider/id :default
                               :llm.provider/base-url "https://api.example.com/v1"
                               :llm.provider/api-key "sk-test"
                               :llm.provider/model "gpt-test"
                               :llm.provider/rate-limit-per-minute 2})
                  xia.llm/provider-rate-limits
                  (java.util.concurrent.ConcurrentHashMap.)
                  xia.llm/provider-rate-limit-cleanup
                  (java.util.concurrent.atomic.AtomicLong. 0)
                  xia.llm/provider-health
                  provider-health*
                  xia.llm/now-ms
                  (constantly 1000)
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
                  xia.prompt/policy-decision!
                  (fn [decision]
                    (swap! decisions conj decision))
                  xia.http-client/request
                  (fn [_req]
                    (swap! request-count inc)
                    {:status 200
                     :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"})]
      (is (= "ok"
             (llm/chat-simple [{"role" "user" "content" "one"}])))
      (is (= "ok"
             (llm/chat-simple [{"role" "user" "content" "two"}])))
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"Rate limit exceeded for provider default"
            (llm/chat-simple [{"role" "user" "content" "three"}])))
      (is (= [{:decision-type :provider-rate-limit-policy
               :allowed? false
               :mode :rate-limit
               :provider-id :default
               :workload nil
               :limit 2}]
             (mapv #(select-keys %
                                  [:decision-type :allowed? :mode :provider-id :workload :limit])
                   @decisions)))
      (is (= 2 @request-count))
      (is (= 0 (get-in @provider-health* [:default :consecutive-failures])))
      (is (nil? (get-in @provider-health* [:default :last-error]))))))
