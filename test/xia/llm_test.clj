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

(deftest build-request-enables-streaming-when-delta-callback-present
  (let [req  (#'llm/build-request {:base-url "https://api.example.com/v1"
                                   :api-key  "sk-test"
                                   :model    "gpt-test"}
                                  [{"role" "user" "content" "hello"}]
                                  {:on-delta (fn [_] nil)})
        body (json/read-json (:body req))]
    (is (= true (get body "stream")))))

(deftest build-request-preserves-multimodal-message-content
  (let [req  (#'llm/build-request {:base-url "https://api.example.com/v1"
                                   :api-key  "sk-test"
                                   :model    "gpt-4o"}
                                  [{"role" "user"
                                    "content" [{"type" "text" "text" "interpret this"}
                                               {"type" "image_url"
                                                "image_url" {"url" "https://cdn.example.com/chart.png"
                                                             "detail" "high"}}]}]
                                  {})
        body (json/read-json (:body req))]
    (is (= "interpret this"
           (get-in body ["messages" 0 "content" 0 "text"])))
    (is (= "https://cdn.example.com/chart.png"
           (get-in body ["messages" 0 "content" 1 "image_url" "url"])))
    (is (= "high"
           (get-in body ["messages" 0 "content" 1 "image_url" "detail"])))))

(deftest build-request-can-allow-private-network-targets
  (let [req (#'llm/build-request {:base-url "http://127.0.0.1:11434/v1"
                                  :api-key "sk-test"
                                  :model "qwen"
                                  :allow-private-network? true}
                                 [{"role" "user" "content" "hello"}]
                                 {})]
    (is (true? (:allow-private-network? req)))))

(deftest build-request-omits-authorization-header-when-not-needed
  (let [req (#'llm/build-request {:base-url "http://127.0.0.1:11434/v1"
                                  :model "qwen"}
                                 [{"role" "user" "content" "hello"}]
                                 {})]
    (is (nil? (get-in req [:headers "Authorization"])))))

(deftest provider-auth-header-can-use-linked-oauth-account
  (with-redefs [xia.oauth/ensure-account-ready! (constantly {:oauth.account/access-token "oauth-token"
                                                             :oauth.account/token-type "Bearer"})
                xia.oauth/oauth-header (constantly "Bearer oauth-token")]
    (is (= "Bearer oauth-token"
           (#'llm/provider-auth-header {:llm.provider/auth-type :oauth-account
                                        :llm.provider/oauth-account :openai-login})))))

(deftest vision-capable-checks-provider-flag
  (is (true? (llm/vision-capable? {:llm.provider/id :vision
                                   :llm.provider/vision? true})))
  (is (false? (llm/vision-capable? {:llm.provider/id :text
                                    :llm.provider/vision? false}))))

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
      (is (re-find #"missing string choices\[0\]\.message\.content"
                   (.getMessage ^Throwable err)))
      (is (= :llm/malformed-response
             (:type (ex-data err)))))))

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
