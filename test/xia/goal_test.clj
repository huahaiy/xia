(ns xia.goal-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.agent :as agent]
            [xia.agent.task-runtime :as task-runtime]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.goal :as goal]
            [xia.test-helpers :as th]
            [xia.working-memory :as wm]))

(use-fixtures :each th/with-test-db)

(defn- completed-autonomy-state
  [text]
  (autonomous/apply-control
   (autonomous/initial-state text)
   {:status :complete
    :summary "Done"
    :next-step "None"
    :reason "All requested work is done"
    :current-focus text
    :stack-action :stay
    :progress-status :complete
    :goal-complete? true
    :agenda []}))

(deftest persistent-goal-lifecycle-is-session-state
  (let [session-id (db/create-session! :http)
        goal*      (goal/set-goal! session-id "Keep working until the report ships" :max-turns 3)]
    (is (= :active (:status goal*)))
    (is (= "Keep working until the report ships" (:text goal*)))
    (is (= 3 (:max-turns goal*)))
    (is (= :paused (:status (goal/pause-goal! session-id))))
    (is (= :active (:status (goal/resume-goal! session-id))))
    (goal/clear-goal! session-id)
    (is (nil? (goal/current-goal session-id)))))

(deftest persistent-goal-has-explicit-contract
  (let [session-id (db/create-session! :http)
        goal*      (goal/set-goal! session-id
                                   "Ship the report"
                                   :success-criteria ["Draft approved" "Published"]
                                   :constraints {:model {:tier :goal}
                                                 :tools {:network false}}
                                   :preferences {:style :brief}
                                   :budget {:max-llm-calls 4}
                                   :resume-policy {:mode :auto})
        task-id    (db/create-task! {:session-id session-id
                                     :channel :http
                                     :type :interactive
                                     :state :running
                                     :title "Report"})]
    (is (= "Ship the report" (get-in goal* [:contract :goal/intent])))
    (is (= ["Draft approved" "Published"]
           (get-in goal* [:contract :goal/success-criteria])))
    (is (= :goal (get-in (goal/operating-envelope-source goal*)
                         [:model :tier])))
    (is (= :brief (get-in (goal/operating-envelope-source goal*)
                          [:style])))
    (is (= 4 (get-in (goal/operating-envelope-source goal*)
                     [:goal :budget :max-llm-calls])))
    (is (re-find #"Draft approved"
                 (goal/working-memory-input goal* "continue")))
    (goal/attach-task! session-id task-id)
    (is (= ["Draft approved" "Published"]
           (get-in (db/get-task task-id)
                   [:meta :persistent-goal :contract :goal/success-criteria])))))

(deftest setting-a-new-persistent-goal-resets-session-autonomy
  (let [session-id (db/create-session! :http)]
    (wm/ensure-wm! session-id)
    (wm/set-autonomy-state! session-id (autonomous/initial-state "Old root"))
    (is (= "Old root" (autonomous/root-goal (wm/autonomy-state session-id))))
    (goal/set-goal! session-id "New root")
    (is (nil? (wm/autonomy-state session-id)))))

(deftest runtime-task-keeps-user-turn-input-separate-from-goal-title
  (let [session-id (db/create-session! :http)
        state      (autonomous/initial-state "Ship the report")
        {:keys [task-id task-turn-id]}
        (task-runtime/ensure-runtime-task! {:truncate-summary (fn [text _limit] text)}
                                           session-id
                                           :http
                                           "Ship the report"
                                           state
                                           nil
                                           nil
                                           nil
                                           :turn-input "continue")
        task       (db/get-task task-id)
        turn       (db/get-task-turn task-turn-id)]
    (is (= "Ship the report" (:title task)))
    (is (= "Ship the report" (get-in task [:contract :goal])))
    (is (= "continue" (:input turn)))))

(deftest persistent-goal-is-mirrored-to-task-meta-and-judged-complete
  (let [session-id (db/create-session! :http)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :http
                                     :type :interactive
                                     :state :running
                                     :title "Report"
                                     :summary "Report"})
        _          (goal/set-goal! session-id "Ship report")
        _          (goal/attach-task! session-id task-id)
        task       (db/get-task task-id)
        state      (completed-autonomy-state "Ship report")]
    (is (= "Ship report" (get-in task [:meta :persistent-goal :text])))
    (goal/judge-after-turn! session-id
                            {:task-id task-id
                             :task-state :completed
                             :control {:status :complete
                                       :summary "Done"
                                       :goal-complete? true}
                             :autonomy-state state
                             :summary "Done"})
    (let [goal* (goal/current-goal session-id)]
      (is (= :completed (:status goal*)))
      (is (= :complete (:last-judge-status goal*)))
      (is (= 1 (:turn-count goal*)))
      (is (= :completed (get-in (db/get-task task-id)
                                [:meta :persistent-goal :status])))
      (is (= 1 (get-in (db/get-task task-id)
                       [:meta :persistent-goal :turn-count]))))))

(deftest persistent-goal-max-turn-guard-pauses-active-goal
  (let [session-id (db/create-session! :http)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :http
                                     :type :interactive
                                     :state :resumable
                                     :title "Investigate"
                                     :summary "Investigate"})
        state      (autonomous/initial-state "Investigate")]
    (goal/set-goal! session-id "Investigate until resolved" :max-turns 1)
    (goal/judge-after-turn! session-id
                            {:task-id task-id
                             :task-state :resumable
                             :control {:status :continue
                                       :summary "More work remains"
                                       :next-step "Continue"
                                       :goal-complete? false}
                             :autonomy-state state
                             :guardrail :iteration-limit
                             :summary "More work remains"})
    (let [goal* (goal/current-goal session-id)]
      (is (= :paused (:status goal*)))
      (is (= :max-turns (:last-judge-status goal*)))
      (is (= 1 (:turn-count goal*))))))

(deftest persistent-goal-resume-respects-terminal-and-turn-guard-states
  (let [session-id (db/create-session! :http)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :http
                                     :type :interactive
                                     :state :completed
                                     :title "Done"
                                     :summary "Done"})]
    (goal/set-goal! session-id "Ship report")
    (goal/judge-after-turn! session-id
                            {:task-id task-id
                             :task-state :completed
                             :control {:status :complete
                                       :summary "Done"
                                       :goal-complete? true}
                             :autonomy-state (completed-autonomy-state "Ship report")
                             :summary "Done"})
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"complete"
                          (goal/resume-goal! session-id)))
    (is (= :not-resumable (:status (agent/resume-task! task-id))))))

(deftest max-turn-goal-cannot-resume-without-new-goal
  (let [session-id (db/create-session! :http)]
    (goal/set-goal! session-id "Investigate" :max-turns 1)
    (goal/judge-after-turn! session-id
                            {:task-state :resumable
                             :control {:status :continue
                                       :summary "More work remains"
                                       :goal-complete? false}
                             :autonomy-state (autonomous/initial-state "Investigate")
                             :guardrail :iteration-limit
                             :summary "More work remains"})
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"guardrail"
                          (goal/resume-goal! session-id)))))
