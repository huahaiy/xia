(ns xia.task-policy-test
  (:require [clojure.test :refer :all]
            [xia.config :as cfg]
            [xia.task-policy :as task-policy]))

(deftest configured-task-policy-values-come-from-config
  (with-redefs [cfg/positive-long (fn [key-name default-value]
                                    (case key-name
                                      :agent/supervisor-max-identical-iterations 7
                                      :agent/supervisor-max-restarts 5
                                      :agent/supervisor-restart-backoff-ms 250
                                      :agent/supervisor-restart-grace-ms 900
                                      :llm/max-provider-retry-rounds 9
                                      :llm/max-provider-retry-wait-ms 777000
                                      :agent/max-turn-llm-calls 42
                                      :agent/max-turn-total-tokens 123456
                                      :agent/max-turn-wall-clock-ms 654321
                                      default-value))
                cfg/positive-double (fn [key-name default-value]
                                      (case key-name
                                        :agent/supervisor-semantic-loop-threshold 0.93
                                        default-value))]
    (is (= 7 (task-policy/supervisor-max-identical-iterations)))
    (is (= 0.93 (task-policy/supervisor-semantic-loop-threshold)))
    (is (= 5 (task-policy/supervisor-max-restarts)))
    (is (= 250 (task-policy/supervisor-restart-backoff-ms)))
    (is (= 900 (task-policy/supervisor-restart-grace-ms)))
    (is (= 9 (task-policy/llm-max-provider-retry-rounds)))
    (is (= 777000 (task-policy/llm-max-provider-retry-wait-ms)))
    (is (= 42 (task-policy/max-turn-llm-calls)))
    (is (= 123456 (task-policy/max-turn-total-tokens)))
    (is (= 654321 (task-policy/max-turn-wall-clock-ms)))))

(deftest turn-llm-budget-records-usage-and-exhaustion
  (with-redefs [task-policy/max-turn-llm-calls (constantly 2)
                task-policy/max-turn-total-tokens (constantly 100)
                task-policy/max-turn-wall-clock-ms (constantly 10000)]
    (let [budget-state (atom (task-policy/new-turn-llm-budget "session-1" :terminal))]
      (task-policy/record-turn-llm-request!
       budget-state
       {:usage {"prompt_tokens" "25"
                "completion_tokens" "15"
                "total_tokens" "40"}
        :duration-ms 150})
      (is (= {:llm-call-count 1
              :prompt-tokens 25
              :completion-tokens 15
              :total-tokens 40
              :llm-total-duration-ms 150}
             (select-keys @budget-state
                          [:llm-call-count
                           :prompt-tokens
                           :completion-tokens
                           :total-tokens
                           :llm-total-duration-ms])))
      (is (nil? (task-policy/turn-llm-budget-status budget-state)))
      (task-policy/record-turn-llm-request!
       budget-state
       {:usage {:prompt_tokens 30
                :output_tokens 35}
        :duration-ms 200
        :error (ex-info "provider unavailable" {})})
      (let [status (task-policy/turn-llm-budget-status budget-state)]
        (is (= :llm-calls (:kind status)))
        (is (= 2 (:llm-call-count status)))
        (is (= 105 (:total-tokens status)))
        (is (= "cumulative LLM call budget (2/2)"
               (task-policy/turn-llm-budget-summary status)))
        (is (= "provider unavailable" (:last-llm-error @budget-state)))
        (is (= 1 (:llm-error-count @budget-state)))
        (is (= :turn-budget-exhausted
               (:type (ex-data (task-policy/turn-llm-budget-ex budget-state)))))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"Reached the cumulative LLM call budget"
                              (task-policy/throw-if-turn-llm-budget-exhausted!
                               budget-state)))))))

(deftest turn-llm-budget-supports-token-and-wall-clock-limits
  (let [token-budget (atom {:session-id "session-2"
                            :channel :terminal
                            :started-at-ms (System/currentTimeMillis)
                            :llm-call-count 1
                            :total-tokens 250
                            :prompt-tokens 100
                            :completion-tokens 150
                            :max-llm-calls 10
                            :max-total-tokens 200
                            :max-wall-clock-ms 60000})
        wall-clock-budget (atom {:session-id "session-3"
                                 :channel :terminal
                                 :started-at-ms (- (System/currentTimeMillis) 5000)
                                 :llm-call-count 1
                                 :total-tokens 10
                                 :prompt-tokens 5
                                 :completion-tokens 5
                                 :max-llm-calls 10
                                 :max-total-tokens 1000
                                 :max-wall-clock-ms 1000})]
    (is (= :tokens (:kind (task-policy/turn-llm-budget-status token-budget))))
    (is (= "cumulative token budget (250/200)"
           (task-policy/turn-llm-budget-summary
            (task-policy/turn-llm-budget-status token-budget))))
    (is (= :wall-clock (:kind (task-policy/turn-llm-budget-status wall-clock-budget))))
    (is (re-find #"wall-clock budget"
                 (task-policy/turn-llm-budget-summary
                  (task-policy/turn-llm-budget-status wall-clock-budget))))))

(deftest restart-policy-decision-captures-restart-permission
  (with-redefs [task-policy/supervisor-max-restarts (constantly 2)
                task-policy/supervisor-restart-backoff-ms (constantly 75)
                task-policy/supervisor-restart-grace-ms (constantly 500)]
    (let [allowed (task-policy/restart-policy-decision
                   (ex-info "transient model failure" {:type :agent-stalled :phase :llm})
                   {:phase :llm}
                   0)
          limited (task-policy/restart-policy-decision
                   (ex-info "still failing" {:type :agent-stalled :phase :llm})
                   {:phase :llm}
                   2)
          tool-risk (task-policy/restart-policy-decision
                     (RuntimeException. "crashed after tool")
                     {:phase :tool
                      :tool-risk? true
                      :tool-id :web-search}
                     0)]
      (is (= {:allowed? true
              :mode :restarting
              :attempt 1
              :max-restarts 2
              :backoff-ms 75
              :grace-ms 500
              :failure-type :agent-stalled
              :failure-phase :llm
              :worker-phase :llm}
             (select-keys allowed
                          [:allowed? :mode :attempt :max-restarts :backoff-ms
                           :grace-ms :failure-type :failure-phase :worker-phase])))
      (is (= {:allowed? false
              :mode :restart-limit
              :attempt 3
              :max-restarts 2}
             (select-keys limited
                          [:allowed? :mode :attempt :max-restarts])))
      (is (= {:allowed? false
              :mode :tool-risk
              :tool-risk? true
              :tool-id :web-search}
             (select-keys tool-risk
                          [:allowed? :mode :tool-risk? :tool-id]))))))

(deftest http-request-retry-decision-captures-allowed-and-terminal-outcomes
  (let [allowed (task-policy/http-request-retry-decision
                 {:method :get
                  :max-attempts 3
                  :initial-backoff-ms 50
                  :max-backoff-ms 500}
                 1
                 {:status 503
                  :reason "busy"})
        limited (task-policy/http-request-retry-decision
                 {:method :get
                  :max-attempts 2}
                 2
                 {:status 503
                  :reason "still busy"})
        disabled (task-policy/http-request-retry-decision
                  {:method :post}
                  1
                  {:status 503
                   :reason "post busy"})]
    (is (= {:allowed? true
            :mode :transient-status
            :attempt 1
            :max-attempts 3
            :status 503
            :delay-ms 50}
           (select-keys allowed
                        [:allowed? :mode :attempt :max-attempts :status :delay-ms])))
    (is (= {:allowed? false
            :mode :attempt-limit
            :attempt 2
            :max-attempts 2
            :status 503}
           (select-keys limited
                        [:allowed? :mode :attempt :max-attempts :status])))
    (is (= {:allowed? false
            :mode :retry-disabled
            :attempt 1
            :status 503}
           (select-keys disabled
                        [:allowed? :mode :attempt :status])))))

(deftest llm-retry-helpers-capture-delay-and-retryability
  (let [now-ms (constantly 1000)
        retry-after (task-policy/llm-retry-after-ms {"retry-after" "120"} now-ms)
        sleep-ms (task-policy/llm-retry-sleep-ms 1000 1 4 300000 5000 (constantly 2000))
        non-retry-sleep (task-policy/llm-retry-sleep-ms 1000 4 4 300000 5000 (constantly 2000))
        retryable (ex-info "rate limited" {:status 429})
        permanent (ex-info "unauthorized" {:status 401})]
    (is (= 120000 retry-after))
    (is (= 5000 sleep-ms))
    (is (nil? non-retry-sleep))
    (is (true? (task-policy/llm-retryable-error? retryable)))
    (is (false? (task-policy/llm-retryable-error? permanent)))))
