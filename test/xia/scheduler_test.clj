(ns xia.scheduler-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.agent]
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
            [:process sid :scheduler "summarize the week" :nightly-review true]
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
