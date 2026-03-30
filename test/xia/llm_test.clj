(ns xia.llm-test
  (:require [clojure.test :refer :all]
            [charred.api :as json]
            [xia.async :as async]
            [xia.llm :as llm]))

(deftest provider-access-mode-normalizes-runtime-model
  (is (= :api
         (llm/provider-access-mode {:llm.provider/access-mode :account
                                    :llm.provider/credential-source :oauth-account})))
  (is (= :api
         (llm/provider-access-mode {:llm.provider/access-mode :api
                                    :llm.provider/credential-source :browser-session}))))

(deftest vision-capable-checks-provider-flag
  (is (true? (llm/vision-capable? {:llm.provider/id :vision
                                   :llm.provider/vision? true})))
  (is (false? (llm/vision-capable? {:llm.provider/id :text
                                    :llm.provider/vision? false}))))

(deftest fetch-provider-model-metadata-infers-vision-from-metadata
  (with-redefs [xia.http-client/request
                (fn [req]
                  (is (= "https://api.example.com/v1/models/anthropic%2Fclaude-sonnet-4"
                         (:url req)))
                  {:status 200
                   :body (json/write-json-str {"id" "anthropic/claude-sonnet-4"
                                               "architecture" {"input_modalities" ["text" "image"]}})})]
    (is (= {:id "anthropic/claude-sonnet-4"
            :vision? true
            :vision-source :metadata}
           (llm/fetch-provider-model-metadata {:base-url "https://api.example.com/v1"
                                               :model "anthropic/claude-sonnet-4"})))))

(deftest fetch-provider-model-metadata-falls-back-to-model-id-inference
  (with-redefs [xia.http-client/request
                (fn [_req]
                  {:status 404
                   :body ""})]
    (is (= {:id "gpt-4o"
            :vision? true
            :vision-source :model-id}
           (llm/fetch-provider-model-metadata {:base-url "https://api.example.com/v1"
                                               :model "gpt-4o"})))))

(deftest fetch-provider-model-metadata-infers-context-window-and-recommended-budgets
  (with-redefs [xia.http-client/request
                (fn [_req]
                  {:status 200
                   :body (json/write-json-str {"id" "test-model"
                                               "context_length" 200000
                                               "max_input_tokens" 128000
                                               "capabilities" {"vision" false}})})]
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

(deftest fetch-provider-models-uses-anthropic-native-headers
  (with-redefs [xia.http-client/request
                (fn [req]
                  (is (= "https://api.anthropic.com/v1/models" (:url req)))
                  (is (= "sk-ant" (get-in req [:headers "x-api-key"])))
                  (is (= "2023-06-01" (get-in req [:headers "anthropic-version"])))
                  {:status 200
                   :body (json/write-json-str {"data" [{"id" "claude-sonnet-4-5"}
                                                       {"id" "claude-haiku-4-5"}]})})]
    (is (= ["claude-haiku-4-5" "claude-sonnet-4-5"]
           (llm/fetch-provider-models {:base-url "https://api.anthropic.com/v1"
                                       :api-key "sk-ant"})))))

(deftest fetch-provider-model-metadata-uses-anthropic-native-headers
  (with-redefs [xia.http-client/request
                (fn [req]
                  (is (= "https://api.anthropic.com/v1/models/claude-sonnet-4-5" (:url req)))
                  (is (= "sk-ant" (get-in req [:headers "x-api-key"])))
                  (is (= "2023-06-01" (get-in req [:headers "anthropic-version"])))
                  {:status 200
                   :body (json/write-json-str {"id" "claude-sonnet-4-5"
                                               "display_name" "Claude Sonnet 4.5"})})]
    (is (= {:id "claude-sonnet-4-5"
            :vision? true
            :vision-source :model-id}
           (llm/fetch-provider-model-metadata {:base-url "https://api.anthropic.com/v1"
                                               :api-key "sk-ant"
                                               :model "claude-sonnet-4-5"})))))

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

(deftest resolve-provider-selection-prunes-stale-workload-counters
  (let [counters (atom {:assistant 7
                        :removed-workload 42})]
    (with-redefs [xia.db/list-providers
                  (constantly [{:llm.provider/id :openai-a
                                :llm.provider/workloads #{:assistant}}
                               {:llm.provider/id :openai-b
                                :llm.provider/workloads #{:assistant}}])
                  xia.db/get-default-provider
                  (constantly {:llm.provider/id :fallback})
                  xia.llm/workload-counters
                  counters]
      (llm/resolve-provider-selection {:workload :assistant})
      (is (= #{:assistant}
             (set (keys @counters))))
      (is (= 8
             (:assistant @counters))))))

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

(deftest workload-routes-expose-async-classification
  (let [routes-by-id (into {} (map (juxt :id identity)) (llm/workload-routes))]
    (is (false? (:async? (get routes-by-id :assistant))))
    (is (false? (:async? (get routes-by-id :history-compaction))))
    (is (true? (:async? (get routes-by-id :topic-summary))))
    (is (true? (:async? (get routes-by-id :memory-summary))))
    (is (true? (:async? (get routes-by-id :memory-importance))))
    (is (true? (:async? (get routes-by-id :memory-extraction))))
    (is (true? (:async? (get routes-by-id :fact-utility))))))

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

(deftest chat-enforces-provider-rate-limit-before-http-request
  (let [request-count (atom 0)
        provider-health* (atom {})]
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
      (is (= 2 @request-count))
      (is (= 0 (get-in @provider-health* [:default :consecutive-failures])))
      (is (nil? (get-in @provider-health* [:default :last-error]))))))

(deftest chat-enforces-provider-rate-limit-concurrently
  (let [request-count (atom 0)
        provider-health* (atom {})
        start-latch     (java.util.concurrent.CountDownLatch. 1)]
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
                  xia.http-client/request
                  (fn [_req]
                    (swap! request-count inc)
                    {:status 200
                     :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"})]
      (let [calls   (doall
                      (repeatedly 4
                        (fn []
                          (future
                            (.await start-latch)
                            (try
                              (llm/chat-simple [{"role" "user" "content" "hello"}])
                              (catch clojure.lang.ExceptionInfo e
                                (if (= :llm/rate-limit (:type (ex-data e)))
                                  :rate-limited
                                  (throw e))))))))
            _       (.countDown start-latch)
            results (mapv deref calls)]
        (is (= 2 (count (filter #(= "ok" %) results))))
        (is (= 2 (count (filter #(= :rate-limited %) results))))
        (is (= 2 @request-count))
        (is (= 0 (get-in @provider-health* [:default :consecutive-failures])))
        (is (nil? (get-in @provider-health* [:default :last-error])))))))

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
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [req]
                  (is (= "https://api.example.com/v1/chat/completions" (:url req)))
                  {:status 200
                   :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"})]
    (is (= "ok"
           (llm/chat-simple [{"role" "user" "content" "hello"}]
                            :workload :assistant)))))

(deftest chat-skips-async-log-write-during-shutdown
  (let [logged? (atom false)]
    (llm/reset-runtime!)
    (try
      (llm/prepare-shutdown!)
      (with-redefs [xia.db/get-default-provider
                    (constantly {:llm.provider/id :default
                                 :llm.provider/base-url "https://api.example.com/v1"
                                 :llm.provider/api-key "sk-test"
                                 :llm.provider/model "gpt-test"})
                    xia.llm/provider-health
                    (atom {})
                    xia.llm/max-provider-retry-rounds
                    (constantly 4)
                    xia.llm/max-provider-retry-wait-ms
                    (constantly 300000)
                    xia.async/submit-background!
                    (fn [_description f]
                      (future (f)))
                    xia.http-client/request
                    (fn [_]
                      {:status 200
                       :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"})
                    xia.db/log-llm-call!
                    (fn [_]
                      (reset! logged? true))]
        (is (= "ok"
               (llm/chat-simple [{"role" "user" "content" "hello"}])))
        (llm/await-background-tasks!)
        (is (false? @logged?)))
      (finally
        (llm/reset-runtime!)))))

(deftest chat-message-stamps-session-provenance-into-llm-log
  (let [logged (atom nil)
        session-id (random-uuid)]
    (llm/reset-runtime!)
    (try
      (with-redefs [xia.db/get-default-provider
                    (constantly {:llm.provider/id :default
                                 :llm.provider/base-url "https://api.example.com/v1"
                                 :llm.provider/api-key "sk-test"
                                 :llm.provider/model "gpt-test"})
                    xia.llm/provider-health
                    (atom {})
                    xia.llm/max-provider-retry-rounds
                    (constantly 4)
                    xia.llm/max-provider-retry-wait-ms
                    (constantly 300000)
                    xia.async/submit-background!
                    (fn [_description f]
                      (future (f)))
                    xia.http-client/request
                    (fn [_]
                      {:status 200
                       :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"})
                    xia.db/log-llm-call!
                    (fn [entry]
                      (reset! logged entry))]
        (let [message (llm/chat-message [{"role" "user" "content" "hello"}]
                                        :session-id session-id)]
          (llm/await-background-tasks!)
          (is (= "ok" (get message "content")))
          (is (= session-id (:session-id @logged)))
          (is (= :default (:provider-id @logged)))
          (is (= "gpt-test" (:model @logged)))
          (is (uuid? (:id @logged)))
          (is (= (:id @logged) (:llm-call-id (meta message))))))
      (finally
        (llm/reset-runtime!)))))

(deftest chat-simple-rejects-malformed-provider-response
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :default
                             :llm.provider/base-url "https://api.example.com/v1"
                             :llm.provider/api-key "sk-test"
                             :llm.provider/model "gpt-test"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [_req]
                  {:status 200
                   :body "{\"choices\":[{}]}"})]
    (let [err (try
                (llm/chat-simple [{"role" "user" "content" "hello"}])
                nil
                (catch clojure.lang.ExceptionInfo e
                  e))]
      (is (some? err))
      (is (re-find #"missing choices\[0\]\.message"
                   (.getMessage ^Throwable err)))
      (is (= {:type :llm/malformed-response
              :provider-id :default}
             (select-keys (ex-data err) [:type :provider-id])))
      (is (string? (:response-preview (ex-data err)))))))

(deftest chat-simple-rejects-missing-string-content
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :default
                             :llm.provider/base-url "https://api.example.com/v1"
                             :llm.provider/api-key "sk-test"
                             :llm.provider/model "gpt-test"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [_req]
                  {:status 200
                   :body "{\"choices\":[{\"message\":{\"tool_calls\":[]}}]}"})]
    (let [err (try
                (llm/chat-simple [{"role" "user" "content" "hello"}])
                nil
                (catch clojure.lang.ExceptionInfo e
                  e))]
      (is (some? err))
      (is (re-find #"missing text choices\[0\]\.message\.content"
                   (.getMessage ^Throwable err)))
      (is (= :llm/malformed-response
             (:type (ex-data err)))))))

(deftest chat-simple-accepts-content-part-arrays
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :default
                             :llm.provider/base-url "https://api.example.com/v1"
                             :llm.provider/api-key "sk-test"
                             :llm.provider/model "gpt-test"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [_req]
                  {:status 200
                   :body "{\"choices\":[{\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"Hello\"},{\"type\":\"text\",\"text\":\" there.\"}]}}]}"})]
    (is (= "Hello there."
           (llm/chat-simple [{"role" "user" "content" "hello"}])))))

(deftest chat-with-tools-accepts-content-part-arrays
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :default
                             :llm.provider/base-url "https://api.example.com/v1"
                             :llm.provider/api-key "sk-test"
                             :llm.provider/model "gpt-test"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [_req]
                  {:status 200
                   :body "{\"choices\":[{\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"Searching...\"}],\"tool_calls\":[{\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"web-search\",\"arguments\":\"{}\"}}]}}]}"})]
    (is (= {"content" "Searching..."
            "tool_calls" [{"id" "call_1"
                           "type" "function"
                           "function" {"name" "web-search"
                                       "arguments" "{}"}}]}
           (llm/chat-with-tools [{"role" "user" "content" "hello"}]
                                [{:type "function"
                                  :function {:name "web-search"}}])))))

(deftest chat-with-tools-normalizes-nil-content-but-rejects-malformed-message
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :default
                             :llm.provider/base-url "https://api.example.com/v1"
                             :llm.provider/api-key "sk-test"
                             :llm.provider/model "gpt-test"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [_req]
                  {:status 200
                   :body "{\"choices\":[{\"message\":{\"content\":null,\"tool_calls\":[{\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"web-search\",\"arguments\":\"{}\"}}]}}]}"})]
    (is (= {"content" ""
            "tool_calls" [{"id" "call_1"
                           "type" "function"
                           "function" {"name" "web-search"
                                       "arguments" "{}"}}]}
           (llm/chat-with-tools [{"role" "user" "content" "hello"}]
                                [{:type "function"
                                  :function {:name "web-search"}}]))))
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :default
                             :llm.provider/base-url "https://api.example.com/v1"
                             :llm.provider/api-key "sk-test"
                             :llm.provider/model "gpt-test"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [_req]
                  {:status 200
                   :body "{\"choices\":[{\"message\":{\"content\":null}}]}"})]
    (let [err (try
                (llm/chat-with-tools [{"role" "user" "content" "hello"}]
                                     [{:type "function"
                                       :function {:name "web-search"}}])
                nil
                (catch clojure.lang.ExceptionInfo e
                  e))]
      (is (some? err))
      (is (re-find #"neither content nor tool_calls"
                   (.getMessage ^Throwable err)))
      (is (= :llm/malformed-response
             (:type (ex-data err)))))))

(deftest chat-streams-deltas-and-reconstructs-final-message
  (let [deltas (atom [])]
    (with-redefs [xia.db/get-default-provider
                  (constantly {:llm.provider/id :default
                               :llm.provider/base-url "https://api.example.com/v1"
                               :llm.provider/api-key "sk-test"
                               :llm.provider/model "gpt-test"})
                  xia.llm/provider-health
                  (atom {})
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
                  xia.http-client/request-events
                  (fn [req]
                    (is (= true (get (json/read-json (:body req)) "stream")))
                    ((:on-event req) {:event "message"
                                      :data "{\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}"})
                    ((:on-event req) {:event "message"
                                      :data "{\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}"})
                    ((:on-event req) {:event "message"
                                      :data "{\"choices\":[{\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}"})
                    ((:on-event req) {:event "message"
                                      :data "[DONE]"})
                    {:status 200
                     :headers {"content-type" "text/event-stream"}
                     :streamed? true})]
      (is (= "Hello"
             (llm/chat-simple [{"role" "user" "content" "hello"}]
                              :on-delta (fn [{:keys [content]}]
                                          (swap! deltas conj content)))))
      (is (= ["Hel" "Hello"] @deltas)))))

(deftest chat-streams-tool-call-deltas
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :default
                             :llm.provider/base-url "https://api.example.com/v1"
                             :llm.provider/api-key "sk-test"
                             :llm.provider/model "gpt-test"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request-events
                (fn [req]
                  ((:on-event req) {:event "message"
                                    :data "{\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"web-\",\"arguments\":\"{\\\"q\\\":\"}}]}}]}"})
                  ((:on-event req) {:event "message"
                                    :data "{\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"search\",\"arguments\":\"\\\"hi\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}"})
                  {:status 200
                   :headers {"content-type" "text/event-stream"}
                   :streamed? true})]
    (is (= {"content" ""
            "role" "assistant"
            "tool_calls" [{"id" "call_1"
                           "type" "function"
                           "function" {"name" "web-search"
                                       "arguments" "{\"q\":\"hi\"}"}}]}
           (llm/chat-with-tools [{"role" "user" "content" "hello"}]
                                [{:type "function"
                                  :function {:name "web-search"}}]
                                :on-delta (fn [_] nil))))))

(deftest chat-recovers-dropped-stream-with-non-streaming-fallback
  (let [deltas           (atom [])
        stream-calls     (atom 0)
        fallback-calls   (atom 0)
        provider-health* (atom {})]
    (with-redefs [xia.db/get-default-provider
                  (constantly {:llm.provider/id :default
                               :llm.provider/base-url "https://api.example.com/v1"
                               :llm.provider/api-key "sk-test"
                               :llm.provider/model "gpt-test"})
                  xia.llm/provider-health
                  provider-health*
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
                  xia.http-client/request-events
                  (fn [req]
                    (swap! stream-calls inc)
                    (is (= true (get (json/read-json (:body req)) "stream")))
                    ((:on-event req) {:event "message"
                                      :data "{\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}"})
                    (throw (java.io.IOException. "stream dropped")))
                  xia.http-client/request
                  (fn [req]
                    (swap! fallback-calls inc)
                    (is (nil? (get (json/read-json (:body req)) "stream")))
                    {:status 200
                     :body "{\"choices\":[{\"message\":{\"content\":\"Hello there.\"}}]}"})]
      (is (= "Hello there."
             (llm/chat-simple [{"role" "user" "content" "hello"}]
                              :on-delta (fn [{:keys [content]}]
                                          (swap! deltas conj content)))))
      (is (= 1 @stream-calls))
      (is (= 1 @fallback-calls))
      (is (= ["Hel" "Hello there."] @deltas))
      (is (= 0 (get-in @provider-health* [:default :consecutive-failures])))
      (is (nil? (get-in @provider-health* [:default :last-error]))))))

(deftest chat-recovers-incomplete-stream-with-non-streaming-fallback
  (let [deltas         (atom [])
        fallback-calls (atom 0)]
    (with-redefs [xia.db/get-default-provider
                  (constantly {:llm.provider/id :default
                               :llm.provider/base-url "https://api.example.com/v1"
                               :llm.provider/api-key "sk-test"
                               :llm.provider/model "gpt-test"})
                  xia.llm/provider-health
                  (atom {})
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
                  xia.http-client/request-events
                  (fn [req]
                    ((:on-event req) {:event "message"
                                      :data "{\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}"})
                    {:status 200
                     :headers {"content-type" "text/event-stream"}
                     :streamed? true})
                  xia.http-client/request
                  (fn [_req]
                    (swap! fallback-calls inc)
                    {:status 200
                     :body "{\"choices\":[{\"message\":{\"content\":\"Hello\"}}]}"})]
      (is (= "Hello"
             (llm/chat-simple [{"role" "user" "content" "hello"}]
                              :on-delta (fn [{:keys [content]}]
                                          (swap! deltas conj content)))))
      (is (= 1 @fallback-calls))
      (is (= ["Hel" "Hello"] @deltas)))))

(deftest chat-with-tools-recovers-dropped-stream-with-non-streaming-fallback
  (let [fallback-calls (atom 0)]
    (with-redefs [xia.db/get-default-provider
                  (constantly {:llm.provider/id :default
                               :llm.provider/base-url "https://api.example.com/v1"
                               :llm.provider/api-key "sk-test"
                               :llm.provider/model "gpt-test"})
                  xia.llm/provider-health
                  (atom {})
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
                  xia.http-client/request-events
                  (fn [req]
                    ((:on-event req) {:event "message"
                                      :data "{\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"web-\",\"arguments\":\"{\\\"q\\\":\"}}]}}]}"})
                    (throw (java.io.IOException. "stream dropped")))
                  xia.http-client/request
                  (fn [req]
                    (swap! fallback-calls inc)
                    (is (nil? (get (json/read-json (:body req)) "stream")))
                    {:status 200
                     :body "{\"choices\":[{\"message\":{\"content\":\"\",\"tool_calls\":[{\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"web-search\",\"arguments\":\"{\\\"q\\\":\\\"hi\\\"}\"}}]}}]}"})]
      (is (= {"content" ""
              "tool_calls" [{"id" "call_1"
                             "type" "function"
                             "function" {"name" "web-search"
                                         "arguments" "{\"q\":\"hi\"}"}}]}
             (llm/chat-with-tools [{"role" "user" "content" "hello"}]
                                  [{:type "function"
                                  :function {:name "web-search"}}]
                                  :on-delta (fn [_] nil))))
      (is (= 1 @fallback-calls)))))

(deftest chat-supports-anthropic-native-text-response
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :anthropic
                             :llm.provider/base-url "https://api.anthropic.com/v1"
                             :llm.provider/api-key "sk-ant"
                             :llm.provider/model "claude-sonnet-4-5"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [req]
                  (is (= "https://api.anthropic.com/v1/messages" (:url req)))
                  (is (= "sk-ant" (get-in req [:headers "x-api-key"])))
                  {:status 200
                   :body (json/write-json-str {"type" "message"
                                               "role" "assistant"
                                               "content" [{"type" "text"
                                                           "text" "Hello there."}]
                                               "stop_reason" "end_turn"
                                               "usage" {"input_tokens" 12
                                                        "output_tokens" 4}})})]
    (let [response (llm/chat [{"role" "user" "content" "hello"}])]
      (is (= "Hello there."
             (get-in response ["choices" 0 "message" "content"])))
      (is (= "stop"
             (get-in response ["choices" 0 "finish_reason"])))
      (is (= 16
             (get-in response ["usage" "total_tokens"]))))
    (is (= "Hello there."
           (llm/chat-simple [{"role" "user" "content" "hello"}])))))

(deftest chat-with-tools-supports-anthropic-native-tool-use-response
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :anthropic
                             :llm.provider/base-url "https://api.anthropic.com/v1"
                             :llm.provider/api-key "sk-ant"
                             :llm.provider/model "claude-sonnet-4-5"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [_req]
                  {:status 200
                   :body (json/write-json-str {"type" "message"
                                               "role" "assistant"
                                               "content" [{"type" "text"
                                                           "text" "Searching..."}
                                                          {"type" "tool_use"
                                                           "id" "toolu_1"
                                                           "name" "web-search"
                                                           "input" {"q" "hi"}}]
                                               "stop_reason" "tool_use"})})]
    (is (= {"role" "assistant"
            "content" "Searching..."
            "tool_calls" [{"id" "toolu_1"
                           "type" "function"
                           "function" {"name" "web-search"
                                       "arguments" "{\"q\":\"hi\"}"}}]}
           (llm/chat-with-tools [{"role" "user" "content" "hello"}]
                                [{:type "function"
                                  :function {:name "web-search"}}])))))

(deftest chat-streams-anthropic-text-deltas
  (let [deltas (atom [])]
    (with-redefs [xia.db/get-default-provider
                  (constantly {:llm.provider/id :anthropic
                               :llm.provider/base-url "https://api.anthropic.com/v1"
                               :llm.provider/api-key "sk-ant"
                               :llm.provider/model "claude-sonnet-4-5"})
                  xia.llm/provider-health
                  (atom {})
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
                  xia.http-client/request-events
                  (fn [req]
                    (is (= true (get (json/read-json (:body req)) "stream")))
                    ((:on-event req) {:event "message_start"
                                      :data (json/write-json-str {"type" "message_start"
                                                                  "message" {"type" "message"
                                                                             "role" "assistant"
                                                                             "content" []
                                                                             "usage" {"input_tokens" 10
                                                                                      "output_tokens" 0}}})})
                    ((:on-event req) {:event "content_block_start"
                                      :data (json/write-json-str {"type" "content_block_start"
                                                                  "index" 0
                                                                  "content_block" {"type" "text"
                                                                                   "text" ""}})})
                    ((:on-event req) {:event "content_block_delta"
                                      :data (json/write-json-str {"type" "content_block_delta"
                                                                  "index" 0
                                                                  "delta" {"type" "text_delta"
                                                                           "text" "Hel"}})})
                    ((:on-event req) {:event "content_block_delta"
                                      :data (json/write-json-str {"type" "content_block_delta"
                                                                  "index" 0
                                                                  "delta" {"type" "text_delta"
                                                                           "text" "lo"}})})
                    ((:on-event req) {:event "message_delta"
                                      :data (json/write-json-str {"type" "message_delta"
                                                                  "delta" {"stop_reason" "end_turn"
                                                                           "stop_sequence" nil}
                                                                  "usage" {"input_tokens" 10
                                                                           "output_tokens" 2}})})
                    ((:on-event req) {:event "message_stop"
                                      :data (json/write-json-str {"type" "message_stop"})})
                    {:status 200
                     :headers {"content-type" "text/event-stream"}
                     :streamed? true})]
      (is (= "Hello"
             (llm/chat-simple [{"role" "user" "content" "hello"}]
                              :on-delta (fn [{:keys [content]}]
                                          (swap! deltas conj content)))))
      (is (= ["Hel" "Hello"] @deltas)))))

(deftest chat-streams-anthropic-tool-call-deltas
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :anthropic
                             :llm.provider/base-url "https://api.anthropic.com/v1"
                             :llm.provider/api-key "sk-ant"
                             :llm.provider/model "claude-sonnet-4-5"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request-events
                (fn [req]
                  ((:on-event req) {:event "message_start"
                                    :data (json/write-json-str {"type" "message_start"
                                                                "message" {"type" "message"
                                                                           "role" "assistant"
                                                                           "content" []}})})
                  ((:on-event req) {:event "content_block_start"
                                    :data (json/write-json-str {"type" "content_block_start"
                                                                "index" 0
                                                                "content_block" {"type" "tool_use"
                                                                                 "id" "toolu_1"
                                                                                 "name" "web-search"
                                                                                 "input" {}}})})
                  ((:on-event req) {:event "content_block_delta"
                                    :data (json/write-json-str {"type" "content_block_delta"
                                                                "index" 0
                                                                "delta" {"type" "input_json_delta"
                                                                         "partial_json" "{\"q\":\""}})})
                  ((:on-event req) {:event "content_block_delta"
                                    :data (json/write-json-str {"type" "content_block_delta"
                                                                "index" 0
                                                                "delta" {"type" "input_json_delta"
                                                                         "partial_json" "hi\"}"}})})
                  ((:on-event req) {:event "message_delta"
                                    :data (json/write-json-str {"type" "message_delta"
                                                                "delta" {"stop_reason" "tool_use"
                                                                         "stop_sequence" nil}})})
                  ((:on-event req) {:event "message_stop"
                                    :data (json/write-json-str {"type" "message_stop"})})
                  {:status 200
                   :headers {"content-type" "text/event-stream"}
                   :streamed? true})]
    (is (= {"content" ""
            "role" "assistant"
            "tool_calls" [{"id" "toolu_1"
                           "type" "function"
                           "function" {"name" "web-search"
                                       "arguments" "{\"q\":\"hi\"}"}}]}
           (llm/chat-with-tools [{"role" "user" "content" "hello"}]
                                [{:type "function"
                                  :function {:name "web-search"}}]
                                :on-delta (fn [_] nil))))))

(deftest chat-allows-loopback-provider-base-url
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :default
                             :llm.provider/base-url "http://127.0.0.1:11434/v1"
                             :llm.provider/api-key ""
                             :llm.provider/model "qwen"})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [req]
                  (is (true? (:allow-private-network? req)))
                  {:status 200
                   :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"})]
    (is (= "ok"
           (llm/chat-simple [{"role" "user" "content" "hello"}])))))

(deftest chat-propagates-explicit-private-network-opt-in
  (with-redefs [xia.db/get-default-provider
                (constantly {:llm.provider/id :default
                             :llm.provider/base-url "http://192.168.1.10:11434/v1"
                             :llm.provider/api-key ""
                             :llm.provider/model "qwen"
                             :llm.provider/allow-private-network? true})
                xia.llm/provider-health
                (atom {})
                xia.llm/max-provider-retry-rounds
                (constantly 4)
                xia.llm/max-provider-retry-wait-ms
                (constantly 300000)
                xia.http-client/request
                (fn [req]
                  (is (true? (:allow-private-network? req)))
                  {:status 200
                   :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"})]
    (is (= "ok"
           (llm/chat-simple [{"role" "user" "content" "hello"}])))))

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
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
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

(deftest chat-retries-single-provider-after-rate-limit-backoff
  (let [clock-ms (atom 1000)
        sleeps   (atom [])
        calls    (atom 0)]
    (with-redefs [xia.db/get-default-provider
                  (constantly {:llm.provider/id :default
                               :llm.provider/base-url "https://api.example.com/v1"
                               :llm.provider/api-key "sk-test"
                               :llm.provider/model "gpt-test"})
                  xia.llm/provider-health
                  (atom {})
                  xia.llm/now-ms
                  (fn [] @clock-ms)
                  xia.llm/sleep-ms!
                  (fn [delay-ms]
                    (swap! sleeps conj delay-ms)
                    (swap! clock-ms + delay-ms))
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
                  xia.http-client/request
                  (fn [_req]
                    (if (= 1 (swap! calls inc))
                      {:status 429
                       :headers {}
                       :body "rate limited"}
                      {:status 200
                       :headers {}
                       :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"}))]
      (is (= "ok"
             (llm/chat-simple [{"role" "user" "content" "hello"}])))
      (is (= 2 @calls))
      (is (= [30000] @sleeps))
      (is (= :healthy
             (:status (llm/provider-health-summary :default)))))))

(deftest chat-honors-retry-after-header-when-backing-off
  (let [clock-ms (atom 1000)
        sleeps   (atom [])
        calls    (atom 0)]
    (with-redefs [xia.db/get-default-provider
                  (constantly {:llm.provider/id :default
                               :llm.provider/base-url "https://api.example.com/v1"
                               :llm.provider/api-key "sk-test"
                               :llm.provider/model "gpt-test"})
                  xia.llm/provider-health
                  (atom {})
                  xia.llm/now-ms
                  (fn [] @clock-ms)
                  xia.llm/sleep-ms!
                  (fn [delay-ms]
                    (swap! sleeps conj delay-ms)
                    (swap! clock-ms + delay-ms))
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
                  xia.http-client/request
                  (fn [_req]
                    (if (= 1 (swap! calls inc))
                      {:status 429
                       :headers {"Retry-After" "120"}
                       :body "rate limited"}
                      {:status 200
                       :headers {}
                       :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"}))]
      (is (= "ok"
             (llm/chat-simple [{"role" "user" "content" "hello"}])))
      (is (= [120000] @sleeps)))))

(deftest chat-does-not-back-off-on-non-retryable-provider-error
  (let [sleeps (atom [])]
    (with-redefs [xia.db/get-default-provider
                  (constantly {:llm.provider/id :default
                               :llm.provider/base-url "https://api.example.com/v1"
                               :llm.provider/api-key "sk-test"
                               :llm.provider/model "gpt-test"})
                  xia.llm/provider-health
                  (atom {})
                  xia.llm/sleep-ms!
                  (fn [delay-ms]
                    (swap! sleeps conj delay-ms))
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
                  xia.http-client/request
                  (fn [_req]
                    {:status 401
                     :headers {}
                     :body "unauthorized"})]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"status 401"
                            (llm/chat-simple [{"role" "user" "content" "hello"}])))
      (is (empty? @sleeps)))))

(deftest chat-waits-for-cooling-provider-before-first-request
  (let [clock-ms (atom 1000)
        sleeps   (atom [])
        calls    (atom 0)]
    (with-redefs [xia.db/get-default-provider
                  (constantly {:llm.provider/id :default
                               :llm.provider/base-url "https://api.example.com/v1"
                               :llm.provider/api-key "sk-test"
                               :llm.provider/model "gpt-test"})
                  xia.llm/provider-health
                  (atom {:default {:consecutive-failures 1
                                   :cooldown-until-ms 4000
                                   :last-error "rate limited"}})
                  xia.llm/now-ms
                  (fn [] @clock-ms)
                  xia.llm/sleep-ms!
                  (fn [delay-ms]
                    (swap! sleeps conj delay-ms)
                    (swap! clock-ms + delay-ms))
                  xia.llm/max-provider-retry-rounds
                  (constantly 4)
                  xia.llm/max-provider-retry-wait-ms
                  (constantly 300000)
                  xia.http-client/request
                  (fn [_req]
                    (swap! calls inc)
                    {:status 200
                     :headers {}
                     :body "{\"choices\":[{\"message\":{\"content\":\"ok\"}}]}"})]
      (is (= "ok"
             (llm/chat-simple [{"role" "user" "content" "hello"}])))
      (is (= [3000] @sleeps))
      (is (= 1 @calls)))))
