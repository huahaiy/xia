(ns xia.scheduler-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.agent]
            [xia.autonomous]
            [xia.db :as db]
            [xia.hippocampus]
            [xia.oauth]
            [xia.schedule]
            [xia.scheduler :as scheduler]
            [xia.test-helpers :refer [with-test-db]]
            [xia.working-memory]))

(use-fixtures :each with-test-db)

(deftest execute-prompt-schedule-records-conversation-on-success
  (let [sid (random-uuid)
        lifecycle (atom [])
        run* (atom nil)]
    (with-redefs [xia.db/create-session! (fn [channel]
                                           (is (= :scheduler channel))
                                           sid)
                  xia.working-memory/ensure-wm! (fn [session-id]
                                                  (swap! lifecycle conj [:ensure session-id]))
                  xia.schedule/augment-prompt-with-recovery-context (fn [_schedule-id prompt] prompt)
                  xia.schedule/save-task-checkpoint! (fn [schedule-id checkpoint]
                                                       (swap! lifecycle conj [:checkpoint schedule-id (:phase checkpoint)]))
                  xia.autonomous/max-iterations (constantly 4)
                  xia.schedule/record-task-success! (fn [schedule-id result]
                                                      (swap! lifecycle conj [:task-success schedule-id result]))
                  xia.agent/process-message (fn [session-id prompt & {:keys [channel tool-context]}]
                                              (swap! lifecycle conj [:process session-id channel prompt
                                                                     (:schedule-id tool-context)
                                                                     (:approval-bypass? tool-context)])
                                              "done")
                  xia.schedule/record-run! (fn [schedule-id run]
                                             (reset! run* {:schedule-id schedule-id
                                                           :run run}))
                  xia.working-memory/get-wm (fn [session-id]
                                              (swap! lifecycle conj [:get-wm session-id])
                                              {:topics "nightly summary"})
                  xia.working-memory/snapshot! (fn [session-id]
                                                 (swap! lifecycle conj [:snapshot session-id]))
                  xia.hippocampus/record-conversation! (fn [session-id channel & {:keys [topics]}]
                                                         (swap! lifecycle conj [:record session-id channel topics]))
                  xia.working-memory/clear-wm! (fn [session-id]
                                                 (swap! lifecycle conj [:clear session-id]))]
      (#'scheduler/execute-prompt-schedule {:id :nightly-review
                                            :prompt "summarize the week"
                                            :trusted? true}))
    (is (= {:schedule-id :nightly-review
            :run {:status :success
                  :actions []
                  :result "done"}}
           (update @run* :run select-keys [:status :actions :result])))
    (is (= [[:ensure sid]
            [:checkpoint :nightly-review :planning]
            [:checkpoint :nightly-review :understanding]
            [:process sid :scheduler "summarize the week" :nightly-review true]
            [:checkpoint :nightly-review :observing]
            [:task-success :nightly-review "done"]
            [:get-wm sid]
            [:snapshot sid]
            [:record sid :scheduler "nightly summary"]
            [:clear sid]]
           @lifecycle))))

(deftest execute-prompt-schedule-records-conversation-on-error
  (let [sid (random-uuid)
        lifecycle (atom [])
        run* (atom nil)]
    (with-redefs [xia.db/create-session! (fn [_channel] sid)
                  xia.working-memory/ensure-wm! (fn [session-id]
                                                  (swap! lifecycle conj [:ensure session-id]))
                  xia.schedule/augment-prompt-with-recovery-context (fn [_schedule-id prompt] prompt)
                  xia.schedule/save-task-checkpoint! (fn [schedule-id checkpoint]
                                                       (swap! lifecycle conj [:checkpoint schedule-id (:phase checkpoint)]))
                  xia.autonomous/max-iterations (constantly 4)
                  xia.schedule/record-task-failure! (fn [schedule-id error]
                                                      (swap! lifecycle conj [:task-failure schedule-id error]))
                  xia.agent/process-message (fn [session-id _prompt & _]
                                              (swap! lifecycle conj [:process session-id])
                                              (throw (ex-info "boom" {:type :test})))
                  xia.schedule/record-run! (fn [schedule-id run]
                                             (reset! run* {:schedule-id schedule-id
                                                           :run run}))
                  xia.working-memory/get-wm (fn [session-id]
                                              (swap! lifecycle conj [:get-wm session-id])
                                              {:topics "failed run"})
                  xia.working-memory/snapshot! (fn [session-id]
                                                 (swap! lifecycle conj [:snapshot session-id]))
                  xia.hippocampus/record-conversation! (fn [session-id channel & {:keys [topics]}]
                                                         (swap! lifecycle conj [:record session-id channel topics]))
                  xia.working-memory/clear-wm! (fn [session-id]
                                                 (swap! lifecycle conj [:clear session-id]))]
      (#'scheduler/execute-prompt-schedule {:id :nightly-review
                                            :prompt "summarize the week"
                                            :trusted? false}))
    (is (= {:schedule-id :nightly-review
            :run {:status :error
                  :actions []
                  :error "boom"}}
           (update @run* :run select-keys [:status :actions :error])))
    (is (= [[:ensure sid]
            [:checkpoint :nightly-review :planning]
            [:checkpoint :nightly-review :understanding]
            [:process sid]
            [:task-failure :nightly-review "boom"]
            [:get-wm sid]
            [:snapshot sid]
            [:record sid :scheduler "failed run"]
            [:clear sid]]
           @lifecycle))))

(deftest execute-prompt-schedule-injects-recovery-context
  (let [sid (random-uuid)
        seen-prompt (atom nil)]
    (with-redefs [xia.db/create-session! (fn [_channel] sid)
                  xia.working-memory/ensure-wm! (fn [_session-id] nil)
                  xia.schedule/augment-prompt-with-recovery-context
                  (fn [schedule-id prompt]
                    (is (= :nightly-review schedule-id))
                    (str prompt "\n\nRecovery context"))
                  xia.schedule/save-task-checkpoint! (fn [& _] nil)
                  xia.autonomous/max-iterations (constantly 4)
                  xia.schedule/record-task-success! (fn [& _] nil)
                  xia.agent/process-message
                  (fn [_session-id prompt & _]
                    (reset! seen-prompt prompt)
                    "done")
                  xia.schedule/record-run! (fn [& _] nil)
                  xia.working-memory/get-wm (fn [_session-id] {:topics "summary"})
                  xia.working-memory/snapshot! (fn [_session-id] nil)
                  xia.hippocampus/record-conversation! (fn [& _] nil)
                  xia.working-memory/clear-wm! (fn [_session-id] nil)]
      (#'scheduler/execute-prompt-schedule {:id :nightly-review
                                            :prompt "summarize the week"
                                            :trusted? true}))
    (is (= "summarize the week\n\nRecovery context" @seen-prompt))))

(deftest execute-prompt-schedule-reuses-resumable-session
  (let [sid (random-uuid)
        created? (atom false)
        activated? (atom [])
        checkpoints (atom [])]
    (with-redefs [xia.schedule/resumable-session-id (fn [schedule-id]
                                                      (is (= :nightly-review schedule-id))
                                                      sid)
                  xia.db/create-session! (fn [_channel]
                                           (reset! created? true)
                                           (random-uuid))
                  xia.db/set-session-active! (fn [session-id active?]
                                               (swap! activated? conj [session-id active?]))
                  xia.working-memory/ensure-wm! (fn [_session-id] nil)
                  xia.schedule/augment-prompt-with-recovery-context (fn [_schedule-id prompt] prompt)
                  xia.schedule/save-task-checkpoint! (fn [_schedule-id checkpoint]
                                                       (swap! checkpoints conj checkpoint))
                  xia.autonomous/max-iterations (constantly 4)
                  xia.schedule/record-task-success! (fn [& _] nil)
                  xia.agent/process-message (fn [session-id _prompt & _]
                                              (is (= sid session-id))
                                              "done")
                  xia.schedule/record-run! (fn [& _] nil)
                  xia.working-memory/get-wm (fn [_session-id] {:topics "summary"})
                  xia.working-memory/snapshot! (fn [_session-id] nil)
                  xia.hippocampus/record-conversation! (fn [& _] nil)
                  xia.working-memory/clear-wm! (fn [_session-id] nil)]
      (#'scheduler/execute-prompt-schedule {:id :nightly-review
                                            :prompt "summarize the week"
                                            :trusted? true}))
    (is (false? @created?))
    (is (= [[sid true]] @activated?))
    (is (some #(true? (:resumed? %)) @checkpoints))
    (is (some #(= sid (:session-id %)) @checkpoints))))

(deftest execute-prompt-schedule-repeats-until-autonomous-controller-completes
  (let [sid (random-uuid)
        calls (atom [])
        task-result* (atom nil)
        run* (atom nil)
        replies (atom ["Checked inbox.\n\nAUTONOMOUS_STATUS_JSON:{\"status\":\"continue\",\"summary\":\"Checked inbox\",\"next_step\":\"Draft replies\",\"reason\":\"Unread messages remain\",\"goal_complete\":false}"
                       "Sent the replies.\n\nAUTONOMOUS_STATUS_JSON:{\"status\":\"complete\",\"summary\":\"Sent replies\",\"next_step\":\"\",\"reason\":\"Goal satisfied\",\"goal_complete\":true}"])]
    (with-redefs [xia.db/create-session! (fn [_channel] sid)
                  xia.working-memory/ensure-wm! (fn [_session-id] nil)
                  xia.schedule/augment-prompt-with-recovery-context (fn [_schedule-id prompt] prompt)
                  xia.schedule/save-task-checkpoint! (fn [& _] nil)
                  xia.autonomous/max-iterations (constantly 4)
                  xia.agent/process-message
                  (fn [session-id prompt & {:keys [channel tool-context persist-message?
                                                   transient-messages working-memory-message]}]
                    (swap! calls conj {:session-id session-id
                                       :prompt prompt
                                       :channel channel
                                       :schedule-id (:schedule-id tool-context)
                                       :persist-message? persist-message?
                                       :transient-messages transient-messages
                                       :working-memory-message working-memory-message})
                    (let [response (first @replies)]
                      (swap! replies subvec 1)
                      response))
                  xia.schedule/record-task-success! (fn [_schedule-id result]
                                                      (reset! task-result* result))
                  xia.schedule/record-run! (fn [schedule-id run]
                                             (reset! run* {:schedule-id schedule-id
                                                           :run run}))
                  xia.working-memory/get-wm (fn [_session-id] {:topics "summary"})
                  xia.working-memory/snapshot! (fn [_session-id] nil)
                  xia.hippocampus/record-conversation! (fn [& _] nil)
                  xia.working-memory/clear-wm! (fn [_session-id] nil)]
      (#'scheduler/execute-prompt-schedule {:id :nightly-review
                                            :prompt "summarize the week"
                                            :trusted? true}))
    (is (= 2 (count @calls)))
    (is (= {:schedule-id :nightly-review
            :run {:status :success
                  :actions []
                  :result "Sent the replies."}}
           (update @run* :run select-keys [:status :actions :result])))
    (is (= "Sent the replies." @task-result*))
    (is (= "summarize the week" (:prompt (first @calls))))
    (is (true? (:persist-message? (first @calls))))
    (is (= "" (:prompt (second @calls))))
    (is (false? (:persist-message? (second @calls))))
    (is (str/includes? (:working-memory-message (second @calls))
                       "Draft replies"))))

(deftest execute-prompt-schedule-completes-when-controller-state-is-missing
  (let [sid (random-uuid)
        task-result* (atom nil)
        calls (atom 0)]
    (with-redefs [xia.db/create-session! (fn [_channel] sid)
                  xia.working-memory/ensure-wm! (fn [_session-id] nil)
                  xia.schedule/augment-prompt-with-recovery-context (fn [_schedule-id prompt] prompt)
                  xia.schedule/save-task-checkpoint! (fn [& _] nil)
                  xia.autonomous/max-iterations (constantly 4)
                  xia.agent/process-message (fn [_session-id _prompt & _]
                                              (swap! calls inc)
                                              "done without controller state")
                  xia.schedule/record-task-success! (fn [_schedule-id result]
                                                      (reset! task-result* result))
                  xia.schedule/record-run! (fn [& _] nil)
                  xia.working-memory/get-wm (fn [_session-id] {:topics "summary"})
                  xia.working-memory/snapshot! (fn [_session-id] nil)
                  xia.hippocampus/record-conversation! (fn [& _] nil)
                  xia.working-memory/clear-wm! (fn [_session-id] nil)]
      (#'scheduler/execute-prompt-schedule {:id :nightly-review
                                            :prompt "summarize the week"
                                            :trusted? true}))
    (is (= 1 @calls))
    (is (= "done without controller state" @task-result*))))

(deftest execute-prompt-schedule-fails-when-autonomous-loop-hits-iteration-cap
  (let [sid (random-uuid)
        calls (atom 0)
        task-error* (atom nil)
        run* (atom nil)]
    (with-redefs [xia.db/create-session! (fn [_channel] sid)
                  xia.working-memory/ensure-wm! (fn [_session-id] nil)
                  xia.schedule/augment-prompt-with-recovery-context (fn [_schedule-id prompt] prompt)
                  xia.schedule/save-task-checkpoint! (fn [& _] nil)
                  xia.autonomous/max-iterations (constantly 2)
                  xia.agent/process-message
                  (fn [_session-id _prompt & _]
                    (swap! calls inc)
                    "Still working.\n\nAUTONOMOUS_STATUS_JSON:{\"status\":\"continue\",\"summary\":\"Still working\",\"next_step\":\"Keep going\",\"reason\":\"More work remains\",\"goal_complete\":false}")
                  xia.schedule/record-task-failure! (fn [_schedule-id error]
                                                      (reset! task-error* error))
                  xia.schedule/record-run! (fn [schedule-id run]
                                             (reset! run* {:schedule-id schedule-id
                                                           :run run}))
                  xia.working-memory/get-wm (fn [_session-id] {:topics "summary"})
                  xia.working-memory/snapshot! (fn [_session-id] nil)
                  xia.hippocampus/record-conversation! (fn [& _] nil)
                  xia.working-memory/clear-wm! (fn [_session-id] nil)]
      (#'scheduler/execute-prompt-schedule {:id :nightly-review
                                            :prompt "summarize the week"
                                            :trusted? true}))
    (is (= 2 @calls))
    (is (str/includes? @task-error* "iteration limit of 2"))
    (is (str/includes? (get-in @run* [:run :error]) "iteration limit of 2"))))

(deftest scheduler-work-executor-is-bounded
  (let [work-executor-atom @#'scheduler/work-executor]
    (when-let [exec @work-executor-atom]
      (.shutdownNow ^java.util.concurrent.ExecutorService exec))
    (reset! work-executor-atom nil)
    (db/set-config! :scheduler/max-concurrent-runs 3)
    (let [^java.util.concurrent.ThreadPoolExecutor exec (#'scheduler/ensure-work-executor!)]
      (try
        (is (= 3 (.getCorePoolSize exec)))
        (is (= 3 (.getMaximumPoolSize exec)))
        (finally
          (.shutdownNow exec)
          (reset! work-executor-atom nil))))))

(deftest tick-submits-due-schedules-through-worker-pool
  (let [submitted (atom [])
        maintenance-atom @#'scheduler/last-maintenance-at
        original-last @maintenance-atom]
    (reset! maintenance-atom (java.util.Date.))
    (try
      (with-redefs [xia.schedule/due-schedules (fn [_now]
                                                 [{:id :alpha}
                                                  {:id :beta}])
                    xia.backup/backup-due? (fn [] false)
                    xia.hippocampus/consolidate-if-pending! (fn [] nil)
                    xia.hippocampus/maintain-knowledge! (fn [_now] nil)
                    xia.scheduler/submit-work! (fn [kind _f]
                                                 (swap! submitted conj kind)
                                                 true)]
        (#'scheduler/tick!)
        (is (= ["schedule alpha" "schedule beta"] @submitted)))
      (finally
        (reset! maintenance-atom original-last)))))

(deftest tick-starts-scheduled-backup-when-due
  (let [called (promise)
        maintenance-atom @#'scheduler/last-maintenance-at
        original-last @maintenance-atom]
    (reset! maintenance-atom (java.util.Date.))
    (try
      (with-redefs [xia.schedule/due-schedules (fn [_now] [])
                    xia.backup/backup-due? (fn [] true)
                    xia.scheduler/submit-work! (fn [kind f]
                                                 (is (= "automatic backup" kind))
                                                 (f)
                                                 true)
                    xia.backup/run-scheduled-backup! (fn []
                                                       (deliver called :ran)
                                                       {:status :success})
                    xia.hippocampus/consolidate-if-pending! (fn [] nil)
                    xia.hippocampus/maintain-knowledge! (fn [_now] nil)]
        (#'scheduler/tick!)
        (is (= :ran (deref called 1000 nil))))
      (finally
        (reset! maintenance-atom original-last)))))

(deftest execute-schedule-proactively-refreshes-oauth-before-run
  (let [calls (atom [])
        running-atom @#'scheduler/running-schedules
        original @running-atom]
    (reset! running-atom #{})
    (try
      (with-redefs [xia.oauth/refresh-autonomous-accounts! (fn []
                                                             (swap! calls conj :refresh)
                                                             {:status :ok
                                                              :checked 1
                                                              :refreshed [:github]
                                                              :errors []})
                    xia.scheduler/execute-tool-schedule (fn [_sched]
                                                          (swap! calls conj :execute))
                    xia.schedule/trim-history! (fn [& _] nil)]
        (#'scheduler/execute-schedule! {:id :nightly :type :tool})
        (is (= [:refresh :execute] @calls)))
      (finally
        (reset! running-atom original)))))

(deftest execute-schedule-continues-when-proactive-oauth-refresh-fails
  (let [executed? (atom false)
        running-atom @#'scheduler/running-schedules
        original @running-atom]
    (reset! running-atom #{})
    (try
      (with-redefs [xia.oauth/refresh-autonomous-accounts! (fn []
                                                             (throw (ex-info "refresh boom" {})))
                    xia.scheduler/execute-tool-schedule (fn [_sched]
                                                          (reset! executed? true))
                    xia.schedule/trim-history! (fn [& _] nil)]
        (#'scheduler/execute-schedule! {:id :nightly :type :tool})
        (is (true? @executed?)))
      (finally
        (reset! running-atom original)))))
