(ns xia.task-spec-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.agent.task-runtime :as task-runtime]
            [xia.async :as async]
            [xia.bridge :as bridge]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.task-spec :as task-spec]
            [xia.test-helpers :as th]
            [xia.tool :as tool]))

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

(deftest task-spec-authoring-produces-valid-spec-from-goal-and-tool-catalog
  (let [calls (atom [])
        content "{\"spec\":{\"goal\":\"Summarize the inbox\",\"inputs\":{\"topic\":\"inbox\"},\"steps\":[{\"id\":\"search\",\"kind\":\"tool\",\"tool\":\"email-search\",\"args\":{\"query\":[\"input\",\"topic\"]}},{\"id\":\"done\",\"kind\":\"value\",\"depends-on\":\"search\",\"value\":[\"output\",\"search\",\"content\"]}]}}"]
    (with-redefs [llm/chat-message
                  (fn [messages & opts]
                    (swap! calls conj {:messages messages
                                       :opts (apply hash-map opts)})
                    (with-meta {"role" "assistant"
                                "content" content}
                      {:provider-id :planner
                       :workload :task-planning
                       :llm-call-id (random-uuid)}))]
      (let [result (task-spec/author-spec!
                    "Summarize my inbox"
                    :tools [{:id :email-search
                             :name "Email search"
                             :description "Search email"
                             :parameters {"type" "object"
                                          "properties" {"query" {"type" "string"}}}}]
                    :provider-id :planner
                    :workload :task-planning
                    :repair-attempts 0)
            spec   (:spec result)
            call   (first @calls)
            prompt (get-in call [:messages 1 "content"])]
        (is (= :success (:status result)))
        (is (= :task-spec-authoring-result (:kind result)))
        (is (= :task (get-in result [:contract :kind])))
        (is (= [:input :topic]
               (get-in spec [:steps 0 :args :query])))
        (is (= [:output :search :content]
               (get-in spec [:steps 1 :value])))
        (is (str/includes? prompt "Summarize my inbox"))
        (is (str/includes? prompt "email-search"))
        (is (= :planner (get-in call [:opts :provider-id])))
        (is (= :task-planning (get-in call [:opts :workload])))))))

(deftest task-spec-authored-tool-args-use-json-keys-at-execution
  (let [calls   (atom [])
        content "{\"spec\":{\"goal\":\"Search\",\"inputs\":{\"topic\":\"inbox\"},\"steps\":[{\"id\":\"search\",\"kind\":\"tool\",\"tool\":\"email-search\",\"args\":{\"query\":[\"input\",\"topic\"],\"max_results\":3,\"filters\":{\"unread_only\":true}}}]}}"]
    (with-redefs [llm/chat-message
                  (fn [_messages & _opts]
                    {"role" "assistant"
                     "content" content})
                  tool/execute-tool
                  (fn [tool-id args _context]
                    (swap! calls conj {:tool-id tool-id
                                       :args args})
                    {:content "ok"})]
      (let [auth-result (task-spec/author-spec!
                         "Search"
                         :tools [{:id :email-search
                                  :name "Email search"
                                  :description "Search email"
                                  :parameters {"type" "object"
                                               "properties" {"query" {"type" "string"}}}}]
                         :repair-attempts 0)
            task-id     (task-spec/create-task! (:spec auth-result))
            run-result  (task-spec/run-task! task-id)]
        (is (= :success (:status auth-result)))
        (is (= :completed (:status run-result)))
        (is (= [{:tool-id :email-search
                 :args {"query" "inbox"
                        "max_results" 3
                        "filters" {"unread_only" true}}}]
               @calls))))))

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
