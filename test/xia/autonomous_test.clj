(ns xia.autonomous-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.autonomous :as autonomous]
            [xia.prompt :as prompt]))

(deftest parse-controller-response-normalizes-progress-and-agenda
  (let [{:keys [assistant-text intent control]}
        (autonomous/parse-controller-response
          (str "ACTION_INTENT_JSON:"
               "{\"focus\":\"Reply to billing emails\",\"agenda_item\":\"Send reply\",\"plan_step\":\"Draft the reply\",\"why\":\"The inbox review is already done\",\"tool\":\"gmail-search\",\"tool_args_summary\":\"label:billing unread\"}\n\n"
               "Worked the plan.\n\n"
               "AUTONOMOUS_STATUS_JSON:"
               "{\"status\":\"continue\",\"summary\":\"Worked the plan\",\"next_step\":\"Send the reply\",\"reason\":\"One step remains\",\"goal_complete\":false,"
               "\"current_focus\":\"Reply to billing emails\","
               "\"stack_action\":\"stay\","
               "\"progress_status\":\"in_progress\","
               "\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"done\"},"
               "{\"item\":\"Send reply\",\"status\":\"pending\"}]}"))]
    (is (= "Worked the plan." assistant-text))
    (is (= {:focus "Reply to billing emails"
            :agenda-item "Send reply"
            :plan-step "Draft the reply"
            :why "The inbox review is already done"
            :tool-name "gmail-search"
            :tool-args-summary "label:billing unread"}
           intent))
    (is (= :continue (:status control)))
    (is (= "Reply to billing emails" (:current-focus control)))
    (is (= :stay (:stack-action control)))
    (is (= :in-progress (:progress-status control)))
    (is (= [{:item "Check inbox" :status :completed}
            {:item "Send reply" :status :pending}]
           (:agenda control)))))

(deftest parse-controller-response-supports-paused-resumable-and-diverged
  (let [{:keys [control]}
        (autonomous/parse-controller-response
          (str "Paused after the workflow branched.\n\n"
               "AUTONOMOUS_STATUS_JSON:"
               "{\"status\":\"continue\",\"summary\":\"Workflow branched\",\"next_step\":\"Resume from alternate branch\",\"reason\":\"Original path diverged\",\"goal_complete\":false,"
               "\"agenda\":[{\"item\":\"Wait for callback\",\"status\":\"paused\"},"
               "{\"item\":\"Resume alternate path\",\"status\":\"resumable\"},"
               "{\"item\":\"Follow new branch\",\"status\":\"diverged\"}]}"))]
    (is (= :diverged (:progress-status control)))
    (is (= [{:item "Wait for callback" :status :paused}
            {:item "Resume alternate path" :status :resumable}
            {:item "Follow new branch" :status :diverged}]
           (:agenda control)))))

(deftest parse-controller-response-distinguishes-missing-and-malformed-control
  (let [missing   (autonomous/parse-controller-response
                    "Plain reply without a control envelope.")
        malformed (autonomous/parse-controller-response
                    (str "Worked the plan.\n\n"
                         "AUTONOMOUS_STATUS_JSON:{\"status\":\"continue\""))]
    (is (= :missing (:control-status missing)))
    (is (nil? (:control missing)))
    (is (= "Plain reply without a control envelope."
           (:assistant-text missing)))
    (is (= :malformed (:control-status malformed)))
    (is (nil? (:control malformed)))
    (is (= "Worked the plan."
           (:assistant-text malformed)))))

(deftest controller-state-message-renders-progress-and-agenda
  (let [message (autonomous/controller-state-message
                  {:goal "Handle billing emails"
                   :iteration 2
                   :max-iterations 6
                   :stack [{:title "Handle billing emails"
                            :summary "Checked inbox"
                            :next-step "Draft replies"
                            :reason "Unread messages remain"
                            :progress-status :in-progress
                            :agenda [{:item "Check inbox" :status :completed}
                                     {:item "Draft replies" :status :in-progress}]}]})
        content (:content message)]
    (is (str/includes? content "Current execution stack"))
    (is (str/includes? content "Tip summary: Checked inbox"))
    (is (str/includes? content "[completed] Check inbox"))
    (is (str/includes? content "[in_progress] Draft replies"))))

(deftest controller-state-message-renders-resumable-and-diverged-statuses
  (let [message (autonomous/controller-state-message
                  {:goal "Recover failed browser flow"
                   :iteration 3
                   :max-iterations 6
                   :stack [{:title "Recover failed browser flow"
                            :summary "Original path diverged"
                            :next-step "Resume alternate branch"
                            :reason "The page structure changed"
                            :progress-status :resumable
                            :agenda [{:item "Re-open page" :status :completed}
                                     {:item "Wait for callback" :status :paused}
                                     {:item "Resume alternate branch" :status :resumable}
                                     {:item "Investigate new branch" :status :diverged}]}]})
        content (:content message)]
    (is (str/includes? content "[resumable] Recover failed browser flow"))
    (is (str/includes? content "[paused] Wait for callback"))
    (is (str/includes? content "[resumable] Resume alternate branch"))
    (is (str/includes? content "[diverged] Investigate new branch"))))

(deftest controller-system-message-enforces-tip-driven-execution
  (binding [prompt/*interaction-context* {:channel :terminal}]
    (let [content (:content (autonomous/controller-system-message))]
      (is (str/includes? content
                         "Always work on the current stack tip. Do not choose among stack frames."))
      (is (str/includes? content
                         "Use stay when continuing the current tip"))
      (is (str/includes? content
                         "ACTION_INTENT_JSON:"))
      (is (str/includes? content
                         "At the start of the first assistant response in every iteration")))))

(deftest controller-state-message-requires-intent-before-work
  (let [content (:content (autonomous/controller-state-message
                           {:goal "Handle billing emails"
                            :iteration 1
                            :max-iterations 6
                            :stack [{:title "Handle billing emails"
                                     :progress-status :in-progress
                                     :agenda [{:item "Draft reply" :status :in-progress}]}]}))]
    (is (str/includes? content
                       "Start the first assistant response in this iteration with ACTION_INTENT_JSON"))))

(deftest apply-control-pushes-and-pops-stack-frames
  (let [initial (autonomous/initial-state "Handle billing emails")
        pushed  (autonomous/apply-control
                  initial
                  {:status :continue
                   :summary "Need invoice ids first"
                   :next-step "Look up invoice ids"
                   :reason "A subroutine is required"
                   :current-focus "Find invoice ids"
                   :stack-action :push
                   :progress-status :pending
                   :agenda [{:item "Look up invoice ids" :status :pending}]})
        popped  (autonomous/apply-control
                  pushed
                  {:status :continue
                   :summary "Invoice ids found"
                   :next-step "Draft the billing reply"
                   :reason "Return to parent task"
                   :current-focus "Handle billing emails"
                   :stack-action :pop
                   :progress-status :in-progress
                   :agenda [{:item "Check inbox" :status :completed}
                            {:item "Draft billing reply" :status :in-progress}]})]
    (is (= ["Handle billing emails" "Find invoice ids"]
           (mapv :title (:stack pushed))))
    (is (= :resumable
           (get-in pushed [:stack 0 :progress-status])))
    (is (= "Find invoice ids"
           (:title (autonomous/current-frame pushed))))
    (is (= ["Handle billing emails"]
           (mapv :title (:stack popped))))
    (is (= :in-progress
           (get-in popped [:stack 0 :progress-status])))
    (is (= [{:item "Check inbox" :status :completed}
            {:item "Draft billing reply" :status :in-progress}]
           (get-in popped [:stack 0 :agenda])))))

(deftest apply-control-stay-preserves-existing-frame-fields
  (let [initial {:goal "Handle billing emails"
                 :stack [{:title "Handle billing emails"
                          :summary "Checked inbox"
                          :next-step "Draft replies"
                          :reason "Unread messages remain"
                          :progress-status :in-progress
                          :agenda [{:item "Check inbox" :status :completed}
                                   {:item "Draft replies" :status :in-progress}]}]}
        updated (autonomous/apply-control
                 initial
                 {:status :continue
                  :summary nil
                  :next-step "Send replies"
                  :reason nil
                  :current-focus "Handle billing emails"
                  :stack-action :stay
                  :progress-status :in-progress})]
    (is (= "Checked inbox"
           (get-in updated [:stack 0 :summary])))
    (is (= "Unread messages remain"
           (get-in updated [:stack 0 :reason])))
    (is (= "Send replies"
           (get-in updated [:stack 0 :next-step])))
    (is (= [{:item "Check inbox" :status :completed}
            {:item "Draft replies" :status :in-progress}]
           (get-in updated [:stack 0 :agenda])))))

(deftest apply-control-clear-resets-or-empties-the-stack
  (let [initial {:goal "Handle billing emails"
                 :stack [{:title "Handle billing emails"
                          :progress-status :in-progress}
                         {:title "Find invoice ids"
                          :progress-status :in-progress}]}
        reset-state (autonomous/apply-control
                     initial
                     {:status :continue
                      :summary "Switch to a fresh task"
                      :next-step "Handle the refund request"
                      :reason "Discard the prior stack"
                      :current-focus "Handle refund request"
                      :stack-action :clear
                      :progress-status :pending})
        cleared-state (autonomous/apply-control
                       initial
                       {:status :complete
                        :summary "Done"
                        :next-step ""
                        :reason "The prior stack is no longer needed"
                        :current-focus "Handle refund request"
                        :stack-action :clear
                        :progress-status :complete})]
    (is (= ["Handle refund request"]
           (mapv :title (:stack reset-state))))
    (is (= []
           (:stack cleared-state)))))

(deftest normalize-state-handles-string-keyed-snapshots
  (let [state (autonomous/normalize-state
               {"goal" "Handle billing emails"
                "stack" [{"title" "Handle billing emails"
                          "progress_status" "resumable"
                          "agenda" [{"item" "Wait for invoice ids"
                                     "status" "resumable"}]}]})]
    (is (= "Handle billing emails" (:goal state)))
    (is (= "Handle billing emails"
           (get-in state [:stack 0 :title])))
    (is (= :resumable
           (get-in state [:stack 0 :progress-status])))
    (is (= [{:item "Wait for invoice ids" :status :resumable}]
           (get-in state [:stack 0 :agenda])))))
