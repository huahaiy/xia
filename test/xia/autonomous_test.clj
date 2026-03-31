(ns xia.autonomous-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.autonomous :as autonomous]))

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

(deftest parse-controller-response-tolerates-filler-between-markers-and-json
  (let [{:keys [assistant-text intent-status intent control-status control]}
        (autonomous/parse-controller-response
         (str "ACTION_INTENT_JSON: Here is my intent:\n"
              "{\"focus\":\"Reply to billing emails\",\"agenda_item\":\"Send reply\",\"plan_step\":\"Draft the reply\",\"why\":\"The inbox review is already done\"}\n\n"
              "Worked the plan.\n\n"
              "AUTONOMOUS_STATUS_JSON: Here is the updated status:\n"
              "{\"status\":\"continue\",\"summary\":\"Worked the plan\",\"next_step\":\"Send the reply\",\"reason\":\"One step remains\",\"goal_complete\":false,"
              "\"current_focus\":\"Reply to billing emails\","
              "\"stack_action\":\"stay\","
              "\"progress_status\":\"in_progress\","
              "\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"done\"},"
              "{\"item\":\"Send reply\",\"status\":\"pending\"}]}"))]
    (is (= :parsed intent-status))
    (is (= :parsed control-status))
    (is (= "Worked the plan." assistant-text))
    (is (= "Reply to billing emails" (:focus intent)))
    (is (= :continue (:status control)))
    (is (= "Send the reply" (:next-step control)))))

(deftest parse-controller-response-supports-fenced-json-protocol-blocks
  (let [{:keys [assistant-text intent-status intent control-status control]}
        (autonomous/parse-controller-response
         (str "ACTION_INTENT_JSON:\n"
              "```json\n"
              "Here is my intent:\n"
              "{\"focus\":\"Reply to billing emails\",\"agenda_item\":\"Send reply\",\"plan_step\":\"Draft the reply\",\"why\":\"The inbox review is already done\"}\n"
              "```\n\n"
              "Worked the plan.\n\n"
              "AUTONOMOUS_STATUS_JSON:\n"
              "```json\n"
              "Here is the updated status:\n"
              "{\"status\":\"continue\",\"summary\":\"Worked the plan\",\"next_step\":\"Send the reply\",\"reason\":\"One step remains\",\"goal_complete\":false,"
              "\"current_focus\":\"Reply to billing emails\","
              "\"stack_action\":\"stay\","
              "\"progress_status\":\"in_progress\","
              "\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"done\"},"
              "{\"item\":\"Send reply\",\"status\":\"pending\"}]}\n"
              "```"))]
    (is (= :parsed intent-status))
    (is (= :parsed control-status))
    (is (= "Worked the plan." assistant-text))
    (is (= "Reply to billing emails" (:focus intent)))
    (is (= :continue (:status control)))
    (is (= "Send the reply" (:next-step control)))))

(deftest parse-controller-response-does-not-parse-control-json-as-intent
  (let [{:keys [assistant-text intent-status intent control-status control]}
        (autonomous/parse-controller-response
         (str "ACTION_INTENT_JSON: Here is my intent, I will think first.\n\n"
              "Worked the plan.\n\n"
              "AUTONOMOUS_STATUS_JSON:{\"status\":\"continue\",\"summary\":\"Worked the plan\",\"next_step\":\"Send the reply\",\"reason\":\"One step remains\",\"goal_complete\":false}"))]
    (is (= :malformed intent-status))
    (is (nil? intent))
    (is (= :parsed control-status))
    (is (str/includes? assistant-text "Worked the plan."))
    (is (not (str/includes? assistant-text "ACTION_INTENT_JSON:")))
    (is (= :continue (:status control)))))

(deftest parse-controller-response-preserves-long-summary-and-next-step
  (let [summary   (str "Completed the data gathering pass across the subscription migration checklist, including payment-state verification, account-ownership confirmation, retry-window analysis, and the unresolved edge cases for invoices 17 through 24. "
                       "Captured the discrepancies, identified the missing confirmations, and wrote down the exact remaining follow-up actions for support, billing, and the browser automation retry path.")
        next-step (str "Draft the customer reply using the verified invoice ownership details, then queue the follow-up browser check for the payment retry state, and finally update the internal billing note with the unresolved invoice discrepancies.")
        {:keys [control]}
        (autonomous/parse-controller-response
         (str "Worked the plan.\n\n"
              "AUTONOMOUS_STATUS_JSON:"
              "{\"status\":\"continue\","
              "\"summary\":" (pr-str summary) ","
              "\"next_step\":" (pr-str next-step) ","
              "\"reason\":\"More work remains\",\"goal_complete\":false}"))]
    (is (= summary (:summary control)))
    (is (= next-step (:next-step control)))))

(deftest controller-system-message-reserves-control-envelope-for-final-non-tool-response
  (let [content (:content (autonomous/controller-system-message))]
    (is (str/includes? content
                       "If you request any tool calls in a response, do not append AUTONOMOUS_STATUS_JSON: yet."))
    (is (str/includes? content
                       "Append AUTONOMOUS_STATUS_JSON: only on the final assistant response of the iteration"))
    (is (str/includes? content
                       "Raw JSON is preferred; fenced ```json blocks are also accepted."))
    (is (not (str/includes? content
                            "with no markdown fencing")))))

(deftest initial-state-preserves-a-long-goal
  (let [goal  (str "Handle the multi-account billing remediation workflow for the March support backlog, including invoice verification, refund eligibility review, payment retry checks, customer reply drafting, and the follow-up notes needed for finance escalation when ownership records disagree across systems.")
        state (autonomous/initial-state goal)]
    (is (= goal (autonomous/root-goal state)))))

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

(deftest apply-control-pop-derives-parent-state-from-child-when-parent-update-is-omitted
  (let [state {:stack [{:title "Handle billing emails"
                        :summary "Need invoice ids before replying"
                        :next-step "Find invoice ids"
                        :reason "Blocked on invoice lookup"
                        :progress-status :resumable
                        :agenda [{:item "Find invoice ids" :status :resumable}
                                 {:item "Draft billing reply" :status :pending}]}
                       {:title "Find invoice ids"
                        :summary "Searching invoices"
                        :next-step "Return to the reply"
                        :reason "This child subtask is almost done"
                        :progress-status :in-progress
                        :agenda [{:item "Look up invoice ids" :status :completed}]}]}
        popped (autonomous/apply-control
                state
                {:status :continue
                 :summary "Invoice ids found"
                 :reason "Return to parent task"
                 :current-focus "Handle billing emails"
                 :stack-action :pop})]
    (is (= ["Handle billing emails"]
           (mapv :title (:stack popped))))
    (is (= :in-progress
           (get-in popped [:stack 0 :progress-status])))
    (is (= "Draft billing reply"
           (get-in popped [:stack 0 :next-step])))
    (is (= [{:item "Find invoice ids" :status :completed}
            {:item "Draft billing reply" :status :pending}]
           (get-in popped [:stack 0 :agenda])))))

(deftest apply-control-pop-does-not-complete-a-similarly-named-sibling-agenda-item
  (let [state {:stack [{:title "Handle billing emails"
                        :summary "Need the draft and the follow-up"
                        :next-step "Draft billing reply"
                        :reason "Working through similar subtasks"
                        :progress-status :resumable
                        :agenda [{:item "Draft billing reply follow up" :status :pending}
                                 {:item "Draft billing reply" :status :resumable}]}
                       {:title "Draft billing reply"
                        :summary "Writing the reply"
                        :next-step "Return to parent task"
                        :reason "This child subtask is almost done"
                        :progress-status :in-progress
                        :agenda [{:item "Write the reply" :status :completed}]}]}
        popped (autonomous/apply-control
                state
                {:status :continue
                 :summary "Reply drafted"
                 :reason "Return to parent task"
                 :current-focus "Handle billing emails"
                 :stack-action :pop})]
    (is (= [{:item "Draft billing reply follow up" :status :pending}
            {:item "Draft billing reply" :status :completed}]
           (get-in popped [:stack 0 :agenda])))))

(deftest apply-control-compresses-older-middle-stack-frames-at-max-depth
  (let [state (reduce (fn [current idx]
                        (autonomous/apply-control
                         current
                         {:status :continue
                          :summary (str "Working task " idx)
                          :next-step (str "Continue task " idx)
                          :reason "Descending into a subtask"
                          :current-focus (str "Task " idx)
                          :stack-action :push
                          :progress-status :pending
                          :agenda [{:item (str "Task " idx) :status :pending}]}))
                      (autonomous/initial-state "Task 0")
                      (range 1 41))]
    (is (= 32 (count (:stack state))))
    (is (= "Task 0" (get-in state [:stack 0 :title])))
    (is (true? (get-in state [:stack 1 :compressed?])))
    (is (= 10 (get-in state [:stack 1 :compressed-count])))
    (is (= "Task 10" (get-in state [:stack 1 :title])))
    (is (= "Task 11" (get-in state [:stack 2 :title])))
    (is (= "Task 40" (get-in state [:stack 31 :title])))
    (is (= "Task 0" (autonomous/root-goal state)))))

(deftest apply-control-stay-preserves-existing-frame-fields
  (let [initial {:stack [{:title "Handle billing emails"
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

(deftest apply-control-preserves-existing-progress-status-when-parser-derived-it
  (let [initial {:stack [{:title "Handle billing emails"
                          :summary "Checked inbox"
                          :next-step "Draft replies"
                          :reason "Unread messages remain"
                          :progress-status :in-progress
                          :agenda [{:item "Check inbox" :status :completed}
                                   {:item "Draft replies" :status :in-progress}]}]}
        control (:control
                 (autonomous/parse-controller-response
                  (str "Worked the plan.\n\n"
                       "AUTONOMOUS_STATUS_JSON:"
                       "{\"status\":\"continue\",\"summary\":\"Worked the plan\",\"next_step\":\"Send replies\",\"reason\":\"One step remains\",\"current_focus\":\"Handle billing emails\",\"stack_action\":\"stay\"}")))
        updated (autonomous/apply-control initial control)]
    (is (= :in-progress
           (get-in updated [:stack 0 :progress-status])))
    (is (= "Send replies"
           (get-in updated [:stack 0 :next-step])))))

(deftest apply-control-clear-resets-or-empties-the-stack
  (let [initial {:stack [{:title "Handle billing emails"
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

(deftest apply-control-replace-commits-a-new-top-level-goal
  (let [initial {:stack [{:title "Handle billing emails"
                          :summary "Working the inbox"
                          :next-step "Draft replies"
                          :reason "Unread messages remain"
                          :progress-status :in-progress}]}
        updated (autonomous/apply-control
                 initial
                 {:status :continue
                  :summary "Switched to refund follow-up"
                  :next-step "Review the refund thread"
                  :reason "The user changed the task"
                  :current-focus "Handle refund follow-up"
                  :stack-action :replace
                  :progress-status :pending})]
    (is (= "Handle refund follow-up"
           (autonomous/root-goal updated)))
    (is (= ["Handle refund follow-up"]
           (mapv :title (:stack updated))))))

(deftest reconcile-invalid-goal-complete-keeps-the-stack-incomplete
  (let [state {:stack [{:title "Handle billing emails"
                        :summary "Drafted the reply"
                        :next-step "Send the reply"
                        :reason "One step remains"
                        :progress-status :complete
                        :agenda [{:item "Draft the reply" :status :completed}
                                 {:item "Send the reply" :status :pending}]}]}
        updated (autonomous/reconcile-invalid-goal-complete state)]
    (is (false? (autonomous/structurally-complete? state)))
    (is (= :in-progress
           (get-in updated [:stack 0 :progress-status])))
    (is (= [{:item "Draft the reply" :status :completed}
            {:item "Send the reply" :status :pending}]
           (get-in updated [:stack 0 :agenda])))))

(deftest normalize-state-handles-string-keyed-snapshots
  (let [state (autonomous/normalize-state
               {"stack" [{"title" "Handle billing emails"
                          "progress_status" "resumable"
                          "agenda" [{"item" "Wait for invoice ids"
                                     "status" "resumable"}]}]})]
    (is (= "Handle billing emails" (autonomous/root-goal state)))
    (is (= "Handle billing emails"
           (get-in state [:stack 0 :title])))
    (is (= :resumable
           (get-in state [:stack 0 :progress-status])))
    (is (= [{:item "Wait for invoice ids" :status :resumable}]
           (get-in state [:stack 0 :agenda])))))

(deftest prepare-turn-state-preserves-completed-stack-for-follow-ups
  (let [state {:stack [{:title "Handle billing emails"
                        :summary "Sent the reply"
                        :next-step nil
                        :reason "Goal satisfied"
                        :progress-status :complete
                        :agenda [{:item "Send reply" :status :completed}]}]}]
    (is (= state
           (autonomous/prepare-turn-state state
                                          "Can you also send the tracking link?")))))
