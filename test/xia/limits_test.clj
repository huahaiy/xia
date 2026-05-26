(ns xia.limits-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.db :as db]
            [xia.limits :as limits]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest request-usage-entry-normalizes-provider-usage-and-response-metadata
  (let [response (with-meta {"usage" {"prompt_tokens" "12"
                                      "completion_tokens" 3
                                      "total_tokens" 15}}
                   {:provider-id :openai
                    :model "gpt-test"
                    :workload :assistant
                    :llm-call-id #uuid "00000000-0000-0000-0000-000000000001"})
        entry (limits/request-usage-entry
               {:kind :chat-message
                :response response
                :usage (get response "usage")
                :duration-ms 42})]
    (is (= {:kind :chat-message
            :provider-id :openai
            :model "gpt-test"
            :workload :assistant
            :llm-call-id #uuid "00000000-0000-0000-0000-000000000001"
            :prompt-tokens 12
            :completion-tokens 3
            :total-tokens 15
            :duration-ms 42}
           (select-keys entry [:kind :provider-id :model :workload :llm-call-id
                               :prompt-tokens :completion-tokens :total-tokens
                               :duration-ms])))))

(deftest budget-status-and-exceptions-are-scope-based
  (let [budget-state (atom (assoc (limits/new-turn-budget
                                   #uuid "00000000-0000-0000-0000-000000000002"
                                   :web)
                                  :max-llm-calls 1))]
    (is (nil? (limits/budget-status budget-state)))
    (limits/record-turn-request! budget-state
                                 {:kind :chat-message
                                  :usage {"prompt_tokens" 4
                                          "completion_tokens" 5}
                                  :duration-ms 10})
    (let [status (limits/budget-status budget-state)]
      (is (= :turn (:scope status)))
      (is (= :llm-calls (:kind status)))
      (is (= 9 (:total-tokens status)))
      (is (= "cumulative LLM call budget (1/1)"
             (limits/budget-summary status))))
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Reached the cumulative LLM call budget"
          (limits/throw-if-exhausted! budget-state)))
    (try
      (limits/throw-if-exhausted! budget-state)
      (is false "Expected limit exhaustion")
      (catch clojure.lang.ExceptionInfo e
        (is (limits/exhausted-exception? e))
        (is (= :limit-exhausted (:type (ex-data e))))
        (is (= :turn (:scope (ex-data e))))))))

(deftest log-usage-persists-sanitized-ledger-entry
  (db/set-config! :limits/model-prices
                  (pr-str {[:openai "gpt-test"] {:input-usd-per-1m 1.0
                                                  :output-usd-per-1m 2.0}}))
  (let [session-id #uuid "00000000-0000-0000-0000-000000000003"
        task-id #uuid "00000000-0000-0000-0000-000000000004"
        response (with-meta {"usage" {"prompt_tokens" 100
                                      "completion_tokens" 50}}
                   {:provider-id :openai
                    :model "gpt-test"
                    :workload :assistant
                    :llm-call-id #uuid "00000000-0000-0000-0000-000000000005"})]
    (limits/log-usage! {:session-id session-id
                        :task-id task-id
                        :schedule-id :nightly
                        :persistent-goal-id "goal-1"}
                       {:kind :chat-message
                        :response response
                        :usage (get response "usage")
                        :duration-ms 25})
    (is (= {:scope :session
            :llm-call-count 1
            :prompt-tokens 100
            :completion-tokens 50
            :total-tokens 150
            :llm-total-duration-ms 25
            :cost-micros 200}
           (db/limit-usage-totals :session {:session-id session-id})))
    (is (= 1 (:llm-call-count
              (db/limit-usage-totals :schedule {:schedule-id :nightly}))))))

(deftest goal-policy-budget-uses-goal-ledger-and-contract-budget
  (limits/log-usage! {:persistent-goal-id "goal-1"}
                     {:kind :chat-message
                      :usage {"prompt_tokens" 10
                              "completion_tokens" 5}
                      :duration-ms 20})
  (is (= 1 (:llm-call-count
            (db/limit-usage-totals :goal {:goal-id "goal-1"}))))
  (let [status (limits/policy-status
                {:persistent-goal-id "goal-1"
                 :operating-envelope {:effective {:goal {:budget {:max-llm-calls 1}}}}})]
    (is (= :goal (:scope status)))
    (is (= "goal-1" (:goal-id status)))
    (is (= :llm-calls (:kind status)))
    (is (= 1 (:limit status)))))

(deftest policy-ceilings-use-persistent-ledger-totals
  (let [session-id #uuid "00000000-0000-0000-0000-000000000006"]
    (db/set-config! :limits/session-max-llm-calls 1)
    (limits/log-usage! {:session-id session-id}
                       {:kind :chat-message
                        :usage {"prompt_tokens" 1
                                "completion_tokens" 1}
                        :duration-ms 5})
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"session LLM call ceiling"
          (limits/throw-if-policy-exhausted! {:session-id session-id})))
    (try
      (limits/throw-if-policy-exhausted! {:session-id session-id})
      (is false "Expected persistent session limit exhaustion")
      (catch clojure.lang.ExceptionInfo e
        (is (limits/exhausted-exception? e))
        (is (= :session (:scope (ex-data e))))
        (is (true? (:policy? (ex-data e))))))))

(deftest near-ceilings-produce-routing-decisions
  (let [session-id #uuid "00000000-0000-0000-0000-000000000007"]
    (db/set-config! :limits/session-max-llm-calls 10)
    (db/set-config! :limits/session-warn-ratio 0.5)
    (db/set-config! :limits/session-near-action :prefer-local)
    (db/set-config! :limits/prefer-local-provider-id :local-llm)
    (dotimes [_ 5]
      (limits/log-usage! {:session-id session-id}
                         {:kind :chat-message
                          :usage {"prompt_tokens" 1
                                  "completion_tokens" 1}
                          :duration-ms 1}))
    (let [decision (limits/routing-decision {:session-id session-id})]
      (is (= :near (:state decision)))
      (is (= :prefer-local (:action decision)))
      (is (= :local-llm (:target-provider-id decision)))
      (is (= {:workload :assistant
              :provider-id :local-llm}
             (limits/apply-routing-decision {:workload :assistant}
                                            decision)))
      (is (= {:workload :assistant
              :provider-id :explicit}
             (limits/apply-routing-decision {:workload :assistant
                                             :provider-id :explicit}
                                            decision))))))

(deftest exhausted-ceilings-carry-configured-actions
  (let [session-id #uuid "00000000-0000-0000-0000-000000000008"]
    (db/set-config! :limits/session-max-total-tokens 2)
    (db/set-config! :limits/session-action :require-approval)
    (limits/log-usage! {:session-id session-id}
                       {:kind :chat-message
                        :usage {"prompt_tokens" 1
                                "completion_tokens" 1}
                        :duration-ms 1})
    (let [decision (limits/policy-decision {:session-id session-id})
          event (limits/policy-decision-event decision)]
      (is (= :exhausted (:state decision)))
      (is (= :require-approval (:action decision)))
      (is (= :tokens (:kind decision)))
      (is (= :limit-policy (:decision-type event)))
      (is (= :require-approval (:mode event)))
      (is (= "session token ceiling (2/2)" (:reason event))))))
