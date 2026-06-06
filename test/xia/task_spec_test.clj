(ns xia.task-spec-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.agent.task-runtime :as task-runtime]
            [xia.async :as async]
            [xia.bridge :as bridge]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.prompt :as prompt]
            [xia.task-spec :as task-spec]
            [xia.test-helpers :as th]))

(defn- with-clear-executors
  [f]
  (task-spec/clear-registered-executors!)
  (try
    (f)
    (finally
      (task-spec/clear-registered-executors!))))

(use-fixtures :each th/with-test-db with-clear-executors)

(deftest task-spec-runs-deterministic-steps-through-task-runtime
  (let [task-id (task-spec/create-task!
                 {:goal "Prepare report"
                  :inputs {:rows []}
                  :steps [{:id :load
                           :kind :value
                           :value [:input :rows]}
                          {:id :has-rows
                           :kind :condition
                           :expr [:>= [:count [:output :load]] 1]}
                          {:id :render
                           :kind :value
                           :when [:step-ok? :has-rows]
                           :value {:body [:str "Rows: " [:count [:output :load]]]}}]})
        result  (task-spec/run-task! task-id
                                     :context {:inputs {:rows [{:id 1} {:id 2}]}})
        task    (db/get-task task-id)
        turns   (db/task-turns task-id)
        items   (mapcat #(db/turn-items (:id %)) turns)
        events  (:events (bridge/task-event-history task-id))]
    (is (= :completed (:status result)))
    (is (= :task (:type task)))
    (is (= :completed (:state task)))
    (is (= :task (get-in task [:contract :kind])))
    (is (= :task (get-in task [:contract :spec :kind])))
    (is (= :hybrid (get-in task [:meta :execution :mode])))
    (is (= "Rows: 2"
           (get-in task [:meta :task-spec :outputs :render :body])))
    (is (= [:success :success :success]
           (mapv :status (filter #(= :task-step (:type %)) items))))
    (is (some #(= :item.task-step (:type %)) events))
    (is (some #(= :task.completed (:type %)) events))))

(deftest task-spec-runs-ready-dag-steps-before-earlier-blocked-steps
  (let [task-id (task-spec/create-task!
                 {:goal "Run DAG out of vector order"
                  :steps [{:id :join
                           :kind :value
                           :depends-on [:left :right]
                           :value {:body [:str [:output :left] "+"
                                          [:output :right]]}}
                          {:id :left
                           :kind :value
                           :depends-on :seed
                           :value [:str [:output :seed] "-left"]}
                          {:id :done
                           :kind :value
                           :depends-on :join
                           :value [:output :join :body]}
                          {:id :right
                           :kind :value
                           :depends-on :seed
                           :value [:str [:output :seed] "-right"]}
                          {:id :seed
                           :kind :value
                           :value "root"}]})
        result  (task-spec/run-task! task-id)
        task    (db/get-task task-id)
        items   (filter #(= :task-step (:type %))
                        (mapcat #(db/turn-items (:id %))
                                (db/task-turns task-id)))]
    (is (= :completed (:status result)))
    (is (= "root-left+root-right"
           (get-in task [:meta :task-spec :outputs :done])))
    (is (= ["seed" "left" "right" "join" "done"]
           (mapv #(get-in % [:data :step-id]) items)))))

(deftest task-spec-validates-dependency-graph
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"unknown step"
       (task-spec/create-task!
        {:goal "Bad dependency"
         :steps [{:id :run
                  :kind :value
                  :depends-on :missing
                  :value "never"}]})))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"cycle"
       (task-spec/create-task!
        {:goal "Cyclic dependency"
         :steps [{:id :a
                  :kind :value
                  :depends-on :b
                  :value "a"}
                 {:id :b
                  :kind :value
                  :depends-on :a
                  :value "b"}]}))))

(deftest task-spec-pauses-at-unsupported-step-and-resumes-with-executor
  (let [task-id (task-spec/create-task!
                 {:goal "Hybrid task"
                  :steps [{:id :seed
                           :kind :value
                           :value 1}
                          {:id :judge
                           :kind :external-judge
                           :prompt "Decide what to do"}
                          {:id :done
                           :kind :value
                           :when [:step-ok? :judge]
                           :value [:output :judge]}]})
        paused  (task-spec/run-task! task-id)
        task*   (db/get-task task-id)
        resumed (task-spec/run-task!
                 task-id
                 :executors {:external-judge (fn [_]
                                                {:status :success
                                                 :summary "Judged"
                                                 :output {:decision "ok"}})})
        task**  (db/get-task task-id)]
    (is (= :paused (:status paused)))
    (is (= :resumable (:state task*)))
    (is (= :task-spec-paused (:stop-reason task*)))
    (is (= :paused (get-in task* [:meta :task-spec :steps :judge :status])))
    (is (= :completed (:status resumed)))
    (is (= :completed (:state task**)))
    (is (= {:decision "ok"}
           (get-in task** [:meta :task-spec :outputs :done])))))

(deftest task-spec-pause-payload-persists-and-resumes-with-input
  (let [calls    (atom [])
        deadline "2026-06-02T18:00:00Z"
        task-id  (task-spec/create-task!
                  {:goal "Wait externally"
                   :steps [{:id :wait
                            :kind :custom}
                           {:id :done
                            :kind :value
                            :value [:output :wait :value]}]})
        executor (fn [{:keys [pause resume-token resume-input
                              resume-input-provided? context]}]
                   (swap! calls conj {:pause pause
                                      :resume-token resume-token
                                      :resume-input resume-input
                                      :resume-input-provided? resume-input-provided?
                                      :context-pause (:pause context)
                                      :context-resume-input (:resume-input context)})
                   (if resume-input-provided?
                     {:status :success
                      :summary "External wait resumed"
                      :output {:value (:value resume-input)}}
                     {:status :paused
                      :pause-reason :external-wait
                      :waiting-for :webhook
                      :resume-token "token-1"
                      :deadline deadline
                      :summary "Waiting for webhook"
                      :output {:request-id "req-1"}}))]
    (let [paused (task-spec/run-task! task-id
                                      :executors {:custom executor})
          task*  (db/get-task task-id)
          pause  {:reason :external-wait
                  :waiting-for :webhook
                  :resume-token "token-1"
                  :deadline deadline}]
      (is (= :paused (:status paused)))
      (is (= pause (:pause paused)))
      (is (= pause (get-in task* [:meta :task-spec :pause])))
      (is (= pause (get-in task* [:meta :task-spec :steps :wait :pause])))
      (is (= :external-wait
             (get-in task* [:meta :task-spec :pause-reason])))
      (is (= {:request-id "req-1"}
             (get-in task* [:meta :task-spec :outputs :wait])))
      (is (= [{:pause nil
               :resume-token nil
               :resume-input nil
               :resume-input-provided? nil
               :context-pause nil
               :context-resume-input nil}]
             @calls)))
    (let [resumed (task-spec/run-task!
                   task-id
                   :context {:resume-token "token-1"
                             :resume-input {:value "ready"}}
                   :executors {:custom executor})
          task**  (db/get-task task-id)
          second-call (second @calls)]
      (is (= :completed (:status resumed)))
      (is (= "ready"
             (get-in task** [:meta :task-spec :outputs :done])))
      (is (= {:reason :external-wait
              :waiting-for :webhook
              :resume-token "token-1"
              :deadline deadline}
             (:pause second-call)))
      (is (= "token-1" (:resume-token second-call)))
      (is (= {:value "ready"} (:resume-input second-call)))
      (is (true? (:resume-input-provided? second-call)))
      (is (= (:pause second-call) (:context-pause second-call)))
      (is (= {:value "ready"} (:context-resume-input second-call)))
      (is (nil? (get-in task** [:meta :task-spec :pause])))
      (is (nil? (get-in task** [:meta :task-spec :steps :wait :pause]))))))

(deftest task-spec-llm-step-evaluates-inputs-and-structured-output
  (let [calls   (atom [])
        call-id (random-uuid)
        task-id (task-spec/create-task!
                 {:goal "Render report"
                  :inputs {:topic "Xia"}
                  :steps [{:id :draft
                           :kind :llm
                           :mode :transform
                           :prompt "Write a compact report about the topic."
                           :inputs {:topic [:input :topic]}
                           :output-schema {:type :object
                                           :required [:body]
                                           :properties {:body {:type :string}}}
                           :provider-id :test-provider
                           :workload :assistant
                           :temperature 0
                           :max-tokens 128}
                          {:id :publish
                           :kind :value
                           :value [:output :draft :body]}]})]
    (with-redefs [llm/chat-message
                  (fn [messages & opts]
                    (swap! calls conj {:messages messages
                                       :opts (apply hash-map opts)})
                    (with-meta {"role" "assistant"
                                "content" "{\"body\":\"About Xia\"}"}
                      {:provider-id :test-provider
                       :model "test-model"
                       :workload :assistant
                       :llm-call-id call-id}))]
      (let [result (task-spec/run-task! task-id)
            task   (db/get-task task-id)
            turns  (db/task-turns task-id)
            items  (mapcat #(db/turn-items (:id %)) turns)
            call   (first @calls)
            user-content (get-in call [:messages 1 "content"])]
        (is (= :completed (:status result)))
        (is (= "About Xia"
               (get-in task [:meta :task-spec :outputs :publish])))
        (is (= {:body "About Xia"}
               (get-in task [:meta :task-spec :outputs :draft])))
        (is (= :test-provider (get-in call [:opts :provider-id])))
        (is (= :assistant (get-in call [:opts :workload])))
        (is (= 0.0 (get-in call [:opts :temperature])))
        (is (= 128 (get-in call [:opts :max-tokens])))
        (is (str/includes? user-content "Xia"))
        (is (str/includes? user-content "Return only valid JSON"))
        (is (some #(and (= :assistant-message (:type %))
                        (= call-id (:llm-call-id %))
                        (= {:body "About Xia"} (get-in % [:data :output])))
                  items))))))

(deftest task-spec-llm-agent-mode-pauses-without-agent-executor
  (let [task-id (task-spec/create-task!
                 {:goal "Open-ended work"
                  :steps [{:id :work
                           :kind :llm
                           :mode :agent
                           :prompt "Work with the user."}]})
        result  (task-spec/run-task! task-id)
        task    (db/get-task task-id)]
    (is (= :paused (:status result)))
    (is (= :resumable (:state task)))
    (is (= :missing-executor
           (get-in task [:meta :task-spec :pause-reason])))
    (is (= :paused
           (get-in task [:meta :task-spec :steps :work :status])))))

(deftest task-spec-llm-step-validates-output-schema
  (let [task-id (task-spec/create-task!
                 {:goal "Validate report"
                  :steps [{:id :draft
                           :kind :llm
                           :prompt "Return the report."
                           :output-schema {:type :object
                                           :required [:body]
                                           :properties {:body {:type :string}}}}]})]
    (with-redefs [llm/chat-message
                  (fn [_messages & _opts]
                    {"role" "assistant"
                     "content" "{}"})]
      (let [result (task-spec/run-task! task-id)
            task   (db/get-task task-id)]
        (is (= :failed (:status result)))
        (is (= :failed (:state task)))
        (is (str/includes? (get-in task [:meta :task-spec :steps :draft :error])
                           "missing required field"))))))

(deftest registered-executor-runs-custom-step-kind
  (let [calls (atom [])]
    (is (= :custom
           (task-spec/register-executor!
            :custom
            (fn [{:keys [task-id step context]}]
              (swap! calls conj {:task-id task-id
                                 :step-id (:id step)
                                 :context context})
              {:status :success
               :summary "Custom step completed"
               :output {:source (:source context)}}))))
    (let [task-id (task-spec/create-task!
                   {:goal "Run custom step"
                    :steps [{:id :run
                             :kind :custom}
                            {:id :done
                             :kind :value
                             :value [:output :run]}]})
          result  (task-spec/run-task! task-id
                                       :context {:source "registry"})
          task    (db/get-task task-id)]
      (is (= :completed (:status result)))
      (is (= {:source "registry"}
             (get-in task [:meta :task-spec :outputs :done])))
      (is (= [{:task-id task-id
               :step-id :run
               :context {:source "registry"}}]
             @calls)))))

(deftest task-spec-retries-failed-step-before-continuing
  (let [attempts (atom 0)
        task-id  (task-spec/create-task!
                  {:goal "Retry flaky step"
                   :steps [{:id :flaky
                            :kind :custom
                            :retry {:max-attempts 3}}
                           {:id :done
                            :kind :value
                            :value [:output :flaky :attempt]}]})]
    (let [result (task-spec/run-task!
                  task-id
                  :executors {:custom (fn [_]
                                        (let [attempt (swap! attempts inc)]
                                          (if (< attempt 3)
                                            {:status :failed
                                             :error (str "failed attempt " attempt)}
                                            {:status :success
                                             :output {:attempt attempt}})))})
          task   (db/get-task task-id)
          turns  (db/task-turns task-id)
          items  (mapcat #(db/turn-items (:id %)) turns)
          retry-items (filter #(= "task-step-retry"
                                  (get-in % [:data :kind]))
                              items)]
      (is (= :completed (:status result)))
      (is (= 3 @attempts))
      (is (= 3 (get-in task [:meta :task-spec :outputs :done])))
      (is (= 3 (get-in task [:meta :task-spec :steps :flaky :attempts])))
      (is (= 3 (get-in task [:meta :task-spec :steps :flaky :max-attempts])))
      (is (= 2 (count retry-items)))
      (is (some #(and (= :task-step (:type %))
                      (= :success (:status %))
                      (= 3 (get-in % [:data :attempts])))
                items)))))

(deftest task-spec-times-out-step-at-runner-level
  (let [task-id (task-spec/create-task!
                 {:goal "Timeout slow step"
                  :steps [{:id :slow
                           :kind :custom
                           :timeout-ms 25}]})]
    (let [result (task-spec/run-task!
                  task-id
                  :executors {:custom (fn [_]
                                        (Thread/sleep 250)
                                        {:status :success
                                         :output "too late"})})
          task   (db/get-task task-id)]
      (is (= :failed (:status result)))
      (is (= :failed (:state task)))
      (is (= 25 (get-in task [:meta :task-spec :steps :slow :timeout-ms])))
      (is (str/includes? (get-in task [:meta :task-spec :steps :slow :error])
                         "timed out after 25 ms"))
      (is (nil? (get-in task [:meta :task-spec :outputs :slow]))))))

(deftest per-run-executor-overrides-registered-executor
  (task-spec/register-executor!
   :custom
   (fn [_]
     {:status :success
      :output "registered"}))
  (let [task-id (task-spec/create-task!
                 {:goal "Override custom step"
                  :steps [{:id :run
                           :kind :custom}]})
        result  (task-spec/run-task!
                 task-id
                 :executors {:custom (fn [_]
                                       {:status :success
                                        :output "per-run"})})
        task    (db/get-task task-id)]
    (is (= :completed (:status result)))
    (is (= "per-run"
           (get-in task [:meta :task-spec :outputs :run])))))

(deftest task-spec-input-step-uses-channel-prompt
  (let [session-id (db/create-session! :terminal)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :task
                                     :state :resumable
                                     :title "Input spec"
                                     :summary "Input spec"
                                     :contract (task-spec/task-contract
                                                {:goal "Input spec"
                                                 :steps [{:id :ask
                                                          :kind :input
                                                          :label "Access code"}
                                                         {:id :echo
                                                          :kind :value
                                                          :value [:output :ask]}]})
                                     :meta {:execution {:mode :interactive}}})
        prompts    (atom [])]
    (prompt/register-prompt!
     :terminal
     (fn [label & {:keys [mask?]}]
       (swap! prompts conj {:label label :mask? mask?})
       "123456"))
    (let [result (task-spec/run-task! task-id)
          task   (db/get-task task-id)
          items  (mapcat #(db/turn-items (:id %))
                         (db/task-turns task-id))]
      (is (= :completed (:status result)))
      (is (= "123456"
             (get-in task [:meta :task-spec :outputs :echo])))
      (is (= [{:label "Access code" :mask? false}] @prompts))
      (is (some #(= :input-request (:type %)) items))
      (is (some #(= "input-response" (get-in % [:data :kind])) items)))))

(deftest task-spec-approval-step-uses-channel-approval
  (let [session-id (db/create-session! :terminal)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :task
                                     :state :resumable
                                     :title "Approval spec"
                                     :summary "Approval spec"
                                     :contract (task-spec/task-contract
                                                {:goal "Approval spec"
                                                 :steps [{:id :confirm-delete
                                                          :kind :approval
                                                          :tool-id :dangerous-action
                                                          :tool-name "Dangerous action"
                                                          :description "Confirm the action"
                                                          :args {:id 42}}
                                                         {:id :done
                                                          :kind :value
                                                          :when [:step-ok? :confirm-delete]
                                                          :value "approved"}]})
                                     :meta {:execution {:mode :interactive}}})
        approvals (atom [])]
    (prompt/register-approval!
     :terminal
     (fn [request]
       (swap! approvals conj request)
       true))
    (let [result (task-spec/run-task! task-id)
          task   (db/get-task task-id)
          items  (mapcat #(db/turn-items (:id %))
                         (db/task-turns task-id))]
      (is (= :completed (:status result)))
      (is (= "approved"
             (get-in task [:meta :task-spec :outputs :done])))
      (is (= :dangerous-action
             (:tool-id (first @approvals))))
      (is (= {:id 42}
             (:arguments (first @approvals))))
      (is (some #(= :approval-request (:type %)) items))
      (is (some #(= "approval-decision" (get-in % [:data :kind])) items)))))

(deftest task-spec-runs-subtask-inline
  (let [task-id (task-spec/create-task!
                 {:goal "Run parent task"
                  :inputs {:name "Xia"}
                  :steps [{:id :prepare
                           :kind :subtask
                           :title "Prepare greeting"
                           :inputs {:name [:input :name]}
                           :spec {:goal "Prepare greeting"
                                  :steps [{:id :render
                                           :kind :value
                                           :value {:body [:str "Hello " [:input :name]]}}
                                          {:id :done
                                           :kind :value
                                           :value [:output :render]}]}}
                          {:id :publish
                           :kind :value
                           :value [:output :prepare [:outputs :done :body]]}]})
        result  (task-spec/run-task! task-id)
        task    (db/get-task task-id)
        output  (get-in task [:meta :task-spec :outputs :prepare])
        child   (db/get-task (:task-id output))]
    (is (= :completed (:status result)))
    (is (= "Hello Xia"
           (get-in task [:meta :task-spec :outputs :publish])))
    (is (= :completed (:state child)))
    (is (= task-id (:parent-id child)))
    (is (= :subtask (get-in child [:meta :trigger :kind])))
    (is (= :prepare (get-in child [:meta :trigger :parent-step-id])))
    (is (= "Hello Xia"
           (get-in child [:meta :task-spec :outputs :done :body])))))

(deftest task-spec-subtask-pauses-and-resumes-same-child
  (let [task-id (task-spec/create-task!
                 {:goal "Run parent task with child executor"
                  :steps [{:id :delegate
                           :kind :subtask
                           :spec {:goal "Judge in child"
                                  :steps [{:id :judge
                                           :kind :external-judge
                                           :prompt "Judge this"}
                                          {:id :done
                                           :kind :value
                                           :value [:output :judge]}]}}
                          {:id :publish
                           :kind :value
                           :value [:output :delegate [:outputs :done :decision]]}]})
        paused  (task-spec/run-task! task-id)
        task*   (db/get-task task-id)
        child-id (get-in task* [:meta :task-spec :outputs :delegate :task-id])
        child*  (db/get-task child-id)
        resumed (task-spec/run-task!
                 task-id
                 :executors {:external-judge (fn [_]
                                                {:status :success
                                                 :summary "Child judged"
                                                 :output {:decision "ship"}})})
        task**  (db/get-task task-id)
        child-id* (get-in task** [:meta :task-spec :outputs :delegate :task-id])
        child** (db/get-task child-id*)]
    (is (= :paused (:status paused)))
    (is (= :resumable (:state task*)))
    (is (= :resumable (:state child*)))
    (is (= :paused (get-in child* [:meta :task-spec :steps :judge :status])))
    (is (= :completed (:status resumed)))
    (is (= child-id child-id*))
    (is (= :completed (:state child**)))
    (is (= "ship"
           (get-in task** [:meta :task-spec :outputs :publish])))))

(deftest task-spec-branch-join-runs-independent-child-and-collects-outputs
  (let [task-id (task-spec/create-task!
                 {:goal "Run parent task with joined branch"
                  :inputs {:topic "Xia"}
                  :steps [{:id :research
                           :kind :branch
                           :mode :join
                           :title "Research branch"
                           :inputs {:topic [:input :topic]}
                           :spec {:goal "Research branch"
                                  :steps [{:id :findings
                                           :kind :value
                                           :value {:body [:str "Findings for " [:input :topic]]}}]}}
                          {:id :publish
                           :kind :value
                           :value [:output :research [:outputs :findings :body]]}]})
        result  (task-spec/run-task! task-id)
        task    (db/get-task task-id)
        output  (get-in task [:meta :task-spec :outputs :research])
        child   (db/get-task (:task-id output))]
    (is (= :completed (:status result)))
    (is (= "Findings for Xia"
           (get-in task [:meta :task-spec :outputs :publish])))
    (is (= :completed (:state child)))
    (is (= :branch (:channel child)))
    (is (= task-id (:parent-id child)))
    (is (= :branch (get-in child [:meta :trigger :kind])))
    (is (= :research (get-in child [:meta :trigger :parent-step-id])))
    (is (true? (get-in child [:meta :branch-worker])))
    (is (= "Findings for Xia"
           (get-in child [:meta :task-spec :outputs :findings :body])))))

(deftest task-spec-branch-async-spawns-child-and-continues
  (let [submitted (atom nil)
        task-id   (task-spec/create-task!
                   {:goal "Run parent task with async branch"
                    :steps [{:id :research
                             :kind :branch
                             :mode :async
                             :spec {:goal "Async research"
                                    :steps [{:id :findings
                                             :kind :value
                                             :value {:body "async findings"}}]}}
                            {:id :record-child
                             :kind :value
                             :value [:output :research :task-id]}]})]
    (with-redefs [async/submit-background! (fn [_ f]
                                             (reset! submitted f)
                                             ::future)]
      (let [result (task-spec/run-task! task-id)
            task   (db/get-task task-id)
            child-id (get-in task [:meta :task-spec :outputs :research :task-id])
            child  (db/get-task child-id)]
        (is (= :completed (:status result)))
        (is (= child-id
               (get-in task [:meta :task-spec :outputs :record-child])))
        (is (= :running
               (get-in task [:meta :task-spec :outputs :research :status])))
        (is (true? (get-in task [:meta :task-spec :outputs :research :async])))
        (is (= :resumable (:state child)))
        (is (= :branch (:channel child)))
        (is (= :branch (get-in child [:meta :trigger :kind])))
        (is (fn? @submitted))
        (@submitted)
        (let [child* (db/get-task child-id)]
          (is (= :completed (:state child*)))
          (is (= "async findings"
                 (get-in child* [:meta :task-spec :outputs :findings :body]))))))))

(deftest task-spec-parallel-runs-child-specs-and-collects-outputs
  (let [task-id (task-spec/create-task!
                 {:goal "Run parallel children"
                  :inputs {:base "root"}
                  :steps [{:id :fanout
                           :kind :parallel
                           :inputs {:base [:input :base]}
                           :output-step :done
                           :branches [{:id :left
                                       :inputs {:side "L"}
                                       :spec {:goal "Left child"
                                              :steps [{:id :done
                                                       :kind :value
                                                       :value [:str [:input :base] "-"
                                                               [:input :side]]}]}}
                                      {:id :right
                                       :inputs {:side "R"}
                                       :spec {:goal "Right child"
                                              :steps [{:id :done
                                                       :kind :value
                                                       :value [:str [:input :base] "-"
                                                               [:input :side]]}]}}]}
                          {:id :join
                           :kind :value
                           :value [:str [:output :fanout [:outputs :left]]
                                   "+"
                                   [:output :fanout [:outputs :right]]]}]})
        result  (task-spec/run-task! task-id)
        task    (db/get-task task-id)
        output  (get-in task [:meta :task-spec :outputs :fanout])]
    (is (= :completed (:status result)))
    (is (= "root-L+root-R"
           (get-in task [:meta :task-spec :outputs :join])))
    (is (= {:left "root-L"
            :right "root-R"}
           (:outputs output)))
    (is (every? uuid? (map :task-id (vals (:branches output)))))))

(deftest task-spec-map-runs-child-spec-for-each-item
  (let [task-id (task-spec/create-task!
                 {:goal "Map items"
                  :inputs {:items ["a" "b" "c"]}
                  :steps [{:id :mapped
                           :kind :map
                           :items [:input :items]
                           :as :letter
                           :index-as :idx
                           :output-step :done
                           :spec {:goal "Render item"
                                  :steps [{:id :done
                                           :kind :value
                                           :value [:str [:input :letter] ":"
                                                   [:input :idx]]}]}}
                          {:id :joined
                           :kind :value
                           :value [:output :mapped :outputs]}]})
        result  (task-spec/run-task! task-id)
        task    (db/get-task task-id)
        output  (get-in task [:meta :task-spec :outputs :mapped])]
    (is (= :completed (:status result)))
    (is (= ["a:0" "b:1" "c:2"] (:outputs output)))
    (is (= ["a:0" "b:1" "c:2"]
           (get-in task [:meta :task-spec :outputs :joined])))
    (is (= [0 1 2] (mapv :index (:results output))))
    (is (every? uuid? (map :task-id (:results output))))))

(deftest task-spec-loop-repeats-child-spec-until-condition
  (let [task-id (task-spec/create-task!
                 {:goal "Loop accumulator"
                  :steps [{:id :repeat
                           :kind :loop
                           :initial {:n 0}
                           :while [:< [:input [:acc :n]] 3]
                           :max-iterations 10
                           :output-step :next
                           :spec {:goal "Increment"
                                  :steps [{:id :next
                                           :kind :bump}]}}
                          {:id :done
                           :kind :value
                           :value [:output :repeat [:value :n]]}]})
        result  (task-spec/run-task!
                 task-id
                 :executors {:bump (fn [{:keys [context]}]
                                     (let [n (long (get-in context [:inputs :acc :n]))]
                                       {:status :success
                                        :output {:n (inc n)}}))})
        task    (db/get-task task-id)
        output  (get-in task [:meta :task-spec :outputs :repeat])]
    (is (= :completed (:status result)))
    (is (= 3 (get-in task [:meta :task-spec :outputs :done])))
    (is (= {:n 3} (:value output)))
    (is (= [{:n 1} {:n 2} {:n 3}] (:outputs output)))
    (is (= :condition (:stopped output)))))

(deftest task-spec-loop-supports-child-directed-break
  (let [task-id (task-spec/create-task!
                 {:goal "Loop break"
                  :steps [{:id :repeat
                           :kind :loop
                           :initial {:n 0}
                           :max-iterations 10
                           :output-step :next
                           :spec {:goal "Increment until done"
                                  :steps [{:id :next
                                           :kind :maybe-stop}]}}
                          {:id :done
                           :kind :value
                           :value [:output :repeat [:value :n]]}]})
        result  (task-spec/run-task!
                 task-id
                 :executors {:maybe-stop (fn [{:keys [context]}]
                                           (let [n (long (get-in context [:inputs :acc :n]))]
                                             {:status :success
                                              :output (if (>= n 2)
                                                        {:control :break
                                                         :value {:n n}}
                                                        {:control :continue
                                                         :value {:n (inc n)}})}))})
        task    (db/get-task task-id)
        output  (get-in task [:meta :task-spec :outputs :repeat])]
    (is (= :completed (:status result)))
    (is (= 2 (get-in task [:meta :task-spec :outputs :done])))
    (is (= :break (:stopped output)))
    (is (= {:n 2} (:value output)))
    (is (= [{:n 1} {:n 2} {:n 2}] (:outputs output)))
    (is (= [:continue :continue :break]
           (mapv :control (:iterations output))))))

(deftest task-control-resume-routes-task-spec-through-runner
  (let [session-id (db/create-session! :terminal)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :task
                                     :state :resumable
                                     :title "Resume spec"
                                     :summary "Resume spec"
                                     :contract {:kind :task
                                                :version 1
                                                :goal "Resume spec"
                                                :spec {:kind :task
                                                       :version 1
                                                       :goal "Resume spec"
                                                       :steps [{:id :continue
                                                                :kind :llm
                                                                :prompt "Continue"}]}}
                                     :meta {:execution {:mode :agent}}})
        calls      (atom [])]
    (with-redefs [async/submit-background! (fn [_ f]
                                             (f)
                                             true)]
      (let [result (task-runtime/resume-task!
                    {:task-run-entry (constantly nil)
                     :session-run-entry (constantly nil)
                     :reserve-next-session-turn! (fn [& _] ::reservation)
                     :clear-session-turn-reservation! (fn [& args]
                                                        (swap! calls conj [:clear (vec args)]))
                     :run-task-spec! (fn [& args]
                                       (swap! calls conj [:runner (vec args)])
                                       {:status :completed})}
                    task-id
                    :message "Continue now")]
        (is (= :running (:status result)))
        (is (= [:runner
                [task-id
                 :message "Continue now"
                 :channel :terminal
                 :runtime-op :resume
                 :turn-reservation-token ::reservation]]
               (first @calls)))
        (is (= :clear (ffirst (rest @calls))))))))
