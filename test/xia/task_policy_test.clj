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
                                      :agent/max-tool-rounds 14
                                      :agent/max-tool-calls-per-round 6
                                      :agent/parallel-tool-timeout-ms 3210
                                      :agent/branch-task-timeout-ms 6543
                                      :agent/supervisor-phase-timeout-ms 111
                                      :agent/supervisor-llm-timeout-ms 222
                                      :agent/supervisor-tool-timeout-ms 333
                                      :schedule/failure-backoff-minutes 20
                                      :schedule/max-failure-backoff-minutes 600
                                      :schedule/pause-after-repeated-failures 4
                                      :llm/max-provider-retry-rounds 9
                                      :llm/max-provider-retry-wait-ms 777000
                                      :agent/max-turn-llm-calls 42
                                      :agent/max-turn-total-tokens 123456
                                      :agent/max-turn-wall-clock-ms 654321
                                      :agent/max-user-message-chars 999
                                      :agent/max-user-message-tokens 77
                                      :agent/max-branch-tasks 8
                                      :agent/max-parallel-branches 4
                                      :agent/max-branch-tool-rounds 6
                                      :agent/branch-error-stack-frames 15
                                      :agent/llm-status-preview-chars 210
                                      :agent/llm-status-update-interval-ms 750
                                      :agent/supervisor-tick-ms 12
                                      :agent/task-control-wait-ms 4321
                                      :schedule/max-schedules 55
                                      :schedule/min-interval-minutes 7
                                      :scheduler/max-concurrent-runs 9
                                      :async/background-max-threads 11
                                      :async/background-queue-capacity 300
                                      :async/parallel-max-threads 12
                                      :async/parallel-queue-capacity 400
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
    (is (= 14 (task-policy/max-tool-rounds)))
    (is (= 6 (task-policy/max-tool-calls-per-round)))
    (is (= 3210 (task-policy/parallel-tool-timeout-ms)))
    (is (= 6543 (task-policy/branch-task-timeout-ms)))
    (is (= 111 (task-policy/supervisor-phase-timeout-ms)))
    (is (= 222 (task-policy/supervisor-llm-timeout-ms)))
    (is (= 333 (task-policy/supervisor-tool-timeout-ms)))
    (is (= 222 (task-policy/supervisor-worker-timeout-ms :llm)))
    (is (= 333 (task-policy/supervisor-worker-timeout-ms :tool)))
    (is (= 111 (task-policy/supervisor-worker-timeout-ms :planning)))
    (is (= 20 (task-policy/schedule-failure-backoff-minutes)))
    (is (= 600 (task-policy/schedule-max-failure-backoff-minutes)))
    (is (= 4 (task-policy/schedule-pause-after-repeated-failures)))
    (is (= 9 (task-policy/llm-max-provider-retry-rounds)))
    (is (= 777000 (task-policy/llm-max-provider-retry-wait-ms)))
    (is (= 42 (task-policy/max-turn-llm-calls)))
    (is (= 123456 (task-policy/max-turn-total-tokens)))
    (is (= 654321 (task-policy/max-turn-wall-clock-ms)))
    (is (= 999 (task-policy/max-user-message-chars)))
    (is (= 77 (task-policy/max-user-message-tokens)))
    (is (= 8 (task-policy/max-branch-tasks)))
    (is (= 4 (task-policy/max-parallel-branches)))
    (is (= 6 (task-policy/max-branch-tool-rounds)))
    (is (= 15 (task-policy/branch-error-stack-frames)))
    (is (= 210 (task-policy/llm-status-preview-chars)))
    (is (= 750 (task-policy/llm-status-update-interval-ms)))
    (is (= 12 (task-policy/supervisor-tick-ms)))
    (is (= 4321 (task-policy/task-control-wait-ms)))
    (is (= 55 (task-policy/max-schedules)))
    (is (= 7 (task-policy/min-schedule-interval-minutes)))
    (is (= 9 (task-policy/scheduler-max-concurrent-runs)))
    (is (= 11 (task-policy/async-background-max-threads)))
    (is (= 300 (task-policy/async-background-queue-capacity)))
    (is (= 12 (task-policy/async-parallel-max-threads)))
    (is (= 400 (task-policy/async-parallel-queue-capacity)))))

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

(deftest tool-limit-decisions-capture-round-and-call-caps
  (with-redefs [task-policy/max-tool-rounds (constantly 3)
                task-policy/max-tool-calls-per-round (constantly 4)]
    (let [allowed-round (task-policy/tool-round-limit-decision 1 3)
          blocked-round (task-policy/tool-round-limit-decision 3 3)
          allowed-calls (task-policy/tool-call-limit-decision 4)
          blocked-calls (task-policy/tool-call-limit-decision 5)]
      (is (= {:allowed? true
              :mode :within-limit
              :rounds 1
              :max-tool-rounds 3}
             (select-keys allowed-round
                          [:allowed? :mode :rounds :max-tool-rounds])))
      (is (= {:allowed? false
              :mode :round-limit
              :rounds 3
              :max-tool-rounds 3}
             (select-keys blocked-round
                          [:allowed? :mode :rounds :max-tool-rounds])))
      (is (= {:allowed? true
              :mode :within-limit
              :tool-count 4
              :max-tool-calls-per-round 4}
             (select-keys allowed-calls
                          [:allowed? :mode :tool-count :max-tool-calls-per-round])))
      (is (= {:allowed? false
              :mode :round-call-limit
              :tool-count 5
              :max-tool-calls-per-round 4}
             (select-keys blocked-calls
                          [:allowed? :mode :tool-count :max-tool-calls-per-round]))))))

(deftest schedule-failure-policy-captures-backoff-and-pause
  (let [now (java.util.Date. 1000)]
    (with-redefs [task-policy/schedule-failure-backoff-minutes (constantly 15)
                  task-policy/schedule-max-failure-backoff-minutes (constantly 720)
                  task-policy/schedule-pause-after-repeated-failures (constantly 3)]
      (let [backoff (task-policy/schedule-failure-policy {:same-failure? false
                                                          :previous-failures 0
                                                          :now now})
            paused  (task-policy/schedule-failure-policy {:same-failure? true
                                                          :previous-failures 2
                                                          :now now})]
        (is (= {:decision-type :schedule-failure-policy
                :mode :backoff
                :same-failure? false
                :consecutive-failures 1
                :pause-threshold 3
                :backoff-minutes 15
                :max-backoff-minutes 720}
               (select-keys backoff
                            [:decision-type :mode :same-failure? :consecutive-failures
                             :pause-threshold :backoff-minutes :max-backoff-minutes])))
        (is (instance? java.util.Date (:backoff-until backoff)))
        (is (= {:decision-type :schedule-failure-policy
                :mode :pause
                :same-failure? true
                :consecutive-failures 3
                :pause-threshold 3
                :backoff-ms nil}
               (select-keys paused
                            [:decision-type :mode :same-failure? :consecutive-failures
                             :pause-threshold :backoff-ms])))
        (is (nil? (:backoff-until paused)))))))

(deftest rate-limit-policies-capture-provider-and-service-blocks
  (is (= {:decision-type :provider-rate-limit-policy
          :allowed? false
          :mode :rate-limit
          :provider-id :default
          :workload :assistant
          :limit 2}
         (select-keys (task-policy/provider-rate-limit-policy :default :assistant 2)
                      [:decision-type :allowed? :mode :provider-id :workload :limit])))
  (is (= {:decision-type :service-rate-limit-policy
          :allowed? false
          :mode :rate-limit
          :service-id :limited
          :limit 3}
         (select-keys (task-policy/service-rate-limit-policy :limited 3)
                      [:decision-type :allowed? :mode :service-id :limit]))))

(deftest timeout-policies-capture-parallel-tool-and-branch-task-blocks
  (is (= {:decision-type :parallel-tool-timeout-policy
          :allowed? false
          :mode :timeout
          :tool-id :web-fetch
          :tool-name "web-fetch"
          :timeout-ms 50}
         (select-keys (task-policy/parallel-tool-timeout-policy :web-fetch "web-fetch" 50)
                      [:decision-type :allowed? :mode :tool-id :tool-name :timeout-ms])))
  (is (= {:decision-type :branch-task-timeout-policy
          :allowed? false
          :mode :timeout
          :task "slow"
          :timeout-ms 75}
         (select-keys (task-policy/branch-task-timeout-policy "slow" "slow branch" 75)
                      [:decision-type :allowed? :mode :task :timeout-ms]))))

(deftest user-message-and-branch-count-policies-capture-blocked-inputs
  (with-redefs [task-policy/max-user-message-chars (constantly 5)
                task-policy/max-user-message-tokens (constantly 2)]
    (is (= {:decision-type :user-message-size-policy
            :allowed? false
            :mode :char-limit
            :char-count 6
            :max-chars 5}
           (select-keys (task-policy/user-message-size-decision 6 2)
                        [:decision-type :allowed? :mode :char-count :max-chars])))
    (is (= {:decision-type :user-message-size-policy
            :allowed? false
            :mode :token-limit
            :token-estimate 3
            :max-tokens 2}
           (select-keys (task-policy/user-message-size-decision 5 3)
                        [:decision-type :allowed? :mode :token-estimate :max-tokens]))))
  (is (= {:decision-type :branch-task-count-policy
          :allowed? false
          :mode :task-limit
          :task-count 3
          :max-tasks 2}
         (select-keys (task-policy/branch-task-count-policy 3 2)
                      [:decision-type :allowed? :mode :task-count :max-tasks]))))

(deftest schedule-admission-policies-capture-frequency-and-count-blocks
  (with-redefs [task-policy/min-schedule-interval-minutes (constantly 5)
                task-policy/max-schedules (constantly 50)]
    (is (= {:decision-type :schedule-frequency-policy
            :allowed? false
            :mode :interval-limit
            :interval-minutes 2
            :min-interval-minutes 5}
           (select-keys (task-policy/schedule-frequency-policy {:interval-minutes 2})
                        [:decision-type :allowed? :mode :interval-minutes :min-interval-minutes])))
    (is (= {:decision-type :schedule-frequency-policy
            :allowed? false
            :mode :calendar-frequency
            :min-interval-minutes 5}
           (select-keys (task-policy/schedule-frequency-policy {:spec {}})
                        [:decision-type :allowed? :mode :min-interval-minutes])))
    (is (= {:decision-type :schedule-count-policy
            :allowed? false
            :mode :schedule-limit
            :current-count 50
            :max-schedules 50}
           (select-keys (task-policy/schedule-count-policy 50)
                        [:decision-type :allowed? :mode :current-count :max-schedules])))))
