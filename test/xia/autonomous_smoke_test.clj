(ns xia.autonomous-smoke-test
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

(deftest controller-system-message-reserves-control-envelope-for-final-non-tool-response
  (let [content (:content (autonomous/controller-system-message))]
    (is (str/includes? content "ACTION_INTENT_JSON:"))
    (is (str/includes? content "AUTONOMOUS_STATUS_JSON:"))
    (is (str/includes? content "\"used_facts\""))
    (is (str/includes? content "\"stack_action\""))
    (is (str/includes? content "\"progress_status\""))
    (is (re-find #"final" content))))

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
