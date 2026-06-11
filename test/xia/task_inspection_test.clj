(ns xia.task-inspection-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.task-inspection :as task-inspection]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest task-spec-progress-drives-current-state
  (let [task-id        (random-uuid)
        task           {:id task-id
                        :state :running
                        :title "Ship report"
                        :contract {:kind :task
                                   :spec {:kind :task
                                          :version 1
                                          :goal "Ship report"
                                          :steps [{:id :load
                                                   :kind :tool
                                                   :summary "Load source"}
                                                  {:id :draft
                                                   :kind :llm
                                                   :summary "Draft report"}
                                                  {:id :publish
                                                   :kind :tool
                                                   :summary "Publish report"}]}}
                        :meta {:runtime {:state :running
                                         :current-focus "Runtime focus"
                                         :next-step "Runtime next"
                                         :progress-status :runtime-progress}
                               :checkpoint {:summary "Checkpoint focus"
                                            :next-step "Checkpoint next"
                                            :progress-status :checkpoint-progress}
                               :task-spec {:status :running
                                           :current-step-id :draft
                                           :steps {:load {:id :load
                                                          :kind :tool
                                                          :status :success
                                                          :summary "Loaded source"}
                                                   :draft {:id :draft
                                                           :kind :llm
                                                           :status :running}
                                                   :publish {:id :publish
                                                             :kind :tool
                                                             :status :pending}}}}}
        autonomy-state {:stack [{:title "Autonomous root"}
                                {:title "Autonomous tip"
                                 :next-step "Autonomous next"
                                 :progress-status :in-progress}]}
        inspection     (task-inspection/task-inspection {}
                                                        task
                                                        autonomy-state
                                                        false
                                                        {:turns []
                                                         :items []})
        current-state  (:current_state inspection)]
    (is (= "task_spec" (:progress_source current-state)))
    (is (= "running" (:task_spec_status current-state)))
    (is (= "draft" (:current_step_id current-state)))
    (is (= "llm" (:current_step_kind current-state)))
    (is (= "Draft report" (:current_focus current-state)))
    (is (= "Publish report" (:next_step current-state)))
    (is (= "in-progress" (:progress_status current-state)))
    (is (= 3 (:step_count current-state)))
    (is (= 1 (:completed_count current-state)))
    (is (= "Autonomous tip"
           (get-in inspection [:executor_details :autonomous :current_tip :title])))
    (is (= "Autonomous tip"
           (get-in inspection [:executor_details :autonomous :stack_summary :tip_title])))
    (is (not (contains? inspection :current_tip)))
    (is (not (contains? inspection :stack_summary)))
    (is (= "running" (get-in inspection [:task_spec :status])))))
