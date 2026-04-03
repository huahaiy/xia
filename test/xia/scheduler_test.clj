(ns xia.scheduler-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.agent]
            [xia.db :as db]
            [xia.hippocampus]
            [xia.llm :as llm]
            [xia.oauth]
            [xia.schedule :as schedule]
            [xia.scheduler :as scheduler]
            [xia.test-helpers :refer [with-test-db]]
            [xia.working-memory]))

(defn- with-clean-scheduler-runtime
  [f]
  (scheduler/reset-runtime!)
  (try
    (f)
    (finally
      (scheduler/reset-runtime!))))

(use-fixtures :each with-test-db with-clean-scheduler-runtime)

(deftest execute-prompt-schedule-records-conversation-on-success
  (let [sid (db/create-session! :scheduler)
        lifecycle (atom [])
        run* (atom nil)
        process-call (atom nil)]
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
                  xia.agent/process-message (fn [session-id prompt & {:keys [channel tool-context task-id runtime-op]}]
                                              (reset! process-call {:session-id session-id
                                                                    :task-id task-id
                                                                    :runtime-op runtime-op})
                                              (llm/*request-observer* {:kind :chat-message
                                                                       :usage {"prompt_tokens" 2
                                                                               "completion_tokens" 3
                                                                               "total_tokens" 5}
                                                                       :duration-ms 25})
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
      (scheduler/run-prompt-schedule! {:id :nightly-review
                                       :prompt "summarize the week"
                                       :trusted? true}))
    (is (= {:schedule-id :nightly-review
            :run {:status :success
                  :actions []
                  :meta {:task-id (:task-id @process-call)
                         :llm-budget {:scope :schedule-run
                                      :schedule-id :nightly-review
                                      :llm-call-count 1
                                      :prompt-tokens 2
                                      :completion-tokens 3
                                      :total-tokens 5
                                      :llm-total-duration-ms 25}}
                  :result "done"}}
           (-> @run*
               (update :run #(select-keys % [:status :actions :meta :result]))
               (update-in [:run :meta :llm-budget]
                          #(select-keys %
                                        [:scope :schedule-id :llm-call-count :prompt-tokens
                                         :completion-tokens :total-tokens :llm-total-duration-ms])))))
    (let [task (db/get-task (:task-id @process-call))]
      (is (= sid (:session-id task)))
      (is (= :scheduler (:channel task)))
      (is (= :schedule (:type task)))
      (is (= :start (:runtime-op @process-call)))
      (is (= :nightly-review (get-in task [:meta :schedule-id]))))
    (is (= [[:ensure sid]
            [:checkpoint :nightly-review :planning]
            [:process sid :scheduler "summarize the week" :nightly-review true]
            [:task-success :nightly-review "done"]
            [:get-wm sid]
            [:snapshot sid]
            [:record sid :scheduler "nightly summary"]
            [:clear sid]]
           @lifecycle))))

(deftest execute-prompt-schedule-records-budget-exhaustion
  (let [sid (db/create-session! :scheduler)
        lifecycle (atom [])
        run* (atom nil)]
    (db/set-config! :schedule/max-run-llm-calls 1)
    (with-redefs [xia.db/create-session! (fn [_channel] sid)
                  xia.working-memory/ensure-wm! (fn [session-id]
                                                  (swap! lifecycle conj [:ensure session-id]))
                  xia.schedule/augment-prompt-with-recovery-context (fn [_schedule-id prompt] prompt)
                  xia.schedule/save-task-checkpoint! (fn [schedule-id checkpoint]
                                                       (swap! lifecycle conj [:checkpoint schedule-id (:phase checkpoint)]))
                  xia.schedule/record-task-failure! (fn [schedule-id error]
                                                      (swap! lifecycle conj [:task-failure schedule-id error]))
                  xia.agent/process-message (fn [session-id prompt & {:keys [channel]}]
                                              (llm/*request-observer* {:kind :chat-message
                                                                       :usage {"prompt_tokens" 3
                                                                               "completion_tokens" 4
                                                                               "total_tokens" 7}
                                                                       :duration-ms 40})
                                              (swap! lifecycle conj [:process session-id channel prompt])
                                              (llm/*request-budget-guard* {:kind :chat-message})
                                              "unreachable")
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
      (scheduler/run-prompt-schedule! {:id :nightly-review
                                       :prompt "summarize the week"
                                       :trusted? false}))
    (is (= {:schedule-id :nightly-review
            :run {:status :budget-exhausted}}
           (update @run* :run #(select-keys % [:status]))))
    (is (= {:scope :schedule-run
            :schedule-id :nightly-review
            :llm-call-count 1
            :total-tokens 7}
           (select-keys (get-in @run* [:run :meta :llm-budget])
                        [:scope :schedule-id :llm-call-count :total-tokens])))
    (is (= [[:ensure sid]
            [:checkpoint :nightly-review :planning]
            [:process sid :scheduler "summarize the week"]
            [:task-failure :nightly-review "Reached the scheduled run LLM call budget (1/1)"]
            [:get-wm sid]
            [:snapshot sid]
            [:record sid :scheduler "failed run"]
            [:clear sid]]
           @lifecycle))))

(deftest execute-prompt-schedule-records-conversation-on-error
  (let [sid (db/create-session! :scheduler)
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
      (scheduler/run-prompt-schedule! {:id :nightly-review
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
  (let [sid (db/create-session! :scheduler)
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
      (scheduler/run-prompt-schedule! {:id :nightly-review
                                       :prompt "summarize the week"
                                       :trusted? true}))
    (is (= "summarize the week\n\nRecovery context" @seen-prompt))))

(deftest execute-prompt-schedule-reuses-resumable-session
  (let [sid (db/create-session! :scheduler)
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
      (scheduler/run-prompt-schedule! {:id :nightly-review
                                       :prompt "summarize the week"
                                       :trusted? true}))
    (is (false? @created?))
    (is (= [[sid true]] @activated?))
    (is (some #(true? (:resumed? %)) @checkpoints))
    (is (some #(= sid (:session-id %)) @checkpoints))))

(deftest execute-prompt-schedule-reuses-existing-schedule-task
  (let [sid      (db/create-session! :scheduler)
        task-id  (db/create-task! {:session-id sid
                                   :channel :scheduler
                                   :type :schedule
                                   :state :paused
                                   :title "Nightly review"
                                   :meta {:schedule-id :nightly-review}})
        seen     (atom nil)]
    (xia.schedule/bind-task! :nightly-review task-id)
    (with-redefs [xia.schedule/resumable-session-id (fn [_schedule-id] sid)
                  xia.db/create-session! (fn [_channel]
                                           (throw (ex-info "unexpected create-session!" {})))
                  xia.db/set-session-active! (fn [_session-id _active?] true)
                  xia.working-memory/ensure-wm! (fn [_session-id] nil)
                  xia.schedule/augment-prompt-with-recovery-context (fn [_schedule-id prompt] prompt)
                  xia.schedule/save-task-checkpoint! (fn [& _] nil)
                  xia.schedule/record-task-success! (fn [& _] nil)
                  xia.agent/process-message (fn [session-id _prompt & {:keys [task-id runtime-op channel]}]
                                              (reset! seen {:session-id session-id
                                                            :task-id task-id
                                                            :runtime-op runtime-op
                                                            :channel channel})
                                              "done")
                  xia.schedule/record-run! (fn [& _] nil)
                  xia.working-memory/get-wm (fn [_session-id] {:topics "summary"})
                  xia.working-memory/snapshot! (fn [_session-id] nil)
                  xia.hippocampus/record-conversation! (fn [& _] nil)
                  xia.working-memory/clear-wm! (fn [_session-id] nil)]
      (scheduler/run-prompt-schedule! {:id :nightly-review
                                       :name "Nightly review"
                                       :prompt "summarize the week"
                                       :trusted? true}))
    (is (= {:session-id sid
            :task-id task-id
            :runtime-op :resume
            :channel :scheduler}
           @seen))))

(deftest execute-prompt-schedule-keeps-the-same-task-across-session-change
  (let [sid-a    (db/create-session! :scheduler)
        sid-b    (db/create-session! :scheduler)
        task-id  (db/create-task! {:session-id sid-a
                                   :channel :scheduler
                                   :type :schedule
                                   :state :paused
                                   :title "Nightly review"
                                   :meta {:schedule-id :nightly-review}})
        seen     (atom nil)]
    (xia.schedule/bind-task! :nightly-review task-id)
    (with-redefs [xia.schedule/resumable-session-id (fn [_schedule-id] nil)
                  xia.db/create-session! (fn [_channel] sid-b)
                  xia.db/set-session-active! (fn [_session-id _active?] true)
                  xia.working-memory/ensure-wm! (fn [_session-id] nil)
                  xia.schedule/augment-prompt-with-recovery-context (fn [_schedule-id prompt] prompt)
                  xia.schedule/save-task-checkpoint! (fn [& _] nil)
                  xia.schedule/record-task-success! (fn [& _] nil)
                  xia.agent/process-message (fn [session-id _prompt & {:keys [task-id runtime-op channel]}]
                                              (reset! seen {:session-id session-id
                                                            :task-id task-id
                                                            :runtime-op runtime-op
                                                            :channel channel})
                                              "done")
                  xia.schedule/record-run! (fn [& _] nil)
                  xia.working-memory/get-wm (fn [_session-id] {:topics "summary"})
                  xia.working-memory/snapshot! (fn [_session-id] nil)
                  xia.hippocampus/record-conversation! (fn [& _] nil)
                  xia.working-memory/clear-wm! (fn [_session-id] nil)]
      (scheduler/run-prompt-schedule! {:id :nightly-review
                                       :name "Nightly review"
                                       :prompt "summarize the week"
                                       :trusted? true}))
    (let [task (db/get-task task-id)]
      (is (= {:session-id sid-b
              :task-id task-id
              :runtime-op :resume
              :channel :scheduler}
             @seen))
      (is (= sid-b (:session-id task)))
      (is (= #{sid-a sid-b}
             (set (map :session-id (:session-links task))))))))

(deftest execute-tool-schedule-creates-and-reuses-explicit-schedule-task
  (schedule/create-schedule!
    {:id :nightly-tool
     :name "Nightly tool"
     :spec {:minute #{0} :hour #{9}}
     :type :tool
     :tool-id :web-fetch
     :tool-args {:url "https://example.com"}})
  (let [seen-contexts (atom [])]
    (with-redefs [xia.tool/execute-tool
                  (fn [tool-id args context]
                    (swap! seen-contexts conj {:tool-id tool-id
                                               :args args
                                               :context context})
                    {:summary "Fetched the nightly page."
                     :content "Fetched the nightly page."})]
      (scheduler/run-tool-schedule! (schedule/get-schedule :nightly-tool))
      (let [first-task-id (schedule/schedule-task-id :nightly-tool)
            first-task    (db/get-task first-task-id)
            first-turns   (db/task-turns first-task-id)
            first-items   (db/turn-items (:id (last first-turns)))
            first-run     (first (schedule/schedule-history :nightly-tool 1))]
        (is (= :scheduler (:channel first-task)))
        (is (= :schedule (:type first-task)))
        (is (= :nightly-tool (get-in first-task [:meta :schedule-id])))
        (is (= :tool (get-in first-task [:meta :schedule-type])))
        (is (= :completed (:state first-task)))
        (is (= :start (:operation (last first-turns))))
        (is (= [:tool-call :tool-result]
               (mapv :type first-items)))
        (is (= first-task-id (get-in first-run [:meta :task-id])))
        (is (= first-task-id (get-in (first @seen-contexts) [:context :task-id]))))
      (scheduler/run-tool-schedule! (schedule/get-schedule :nightly-tool))
      (let [task-id (schedule/schedule-task-id :nightly-tool)
            turns   (db/task-turns task-id)
            last-run (first (schedule/schedule-history :nightly-tool 1))]
        (is (= 2 (count turns)))
        (is (= :resume (:operation (last turns))))
        (is (= task-id (get-in last-run [:meta :task-id])))
        (is (= [task-id task-id]
               (mapv #(get-in % [:context :task-id]) @seen-contexts)))))))

(deftest tick-submits-due-schedules-through-worker-pool
  (let [submitted (atom [])]
    (with-redefs [xia.schedule/due-schedules (fn [_now]
                                               [{:id :alpha}
                                                {:id :beta}])
                  xia.backup/backup-due? (fn [] false)
                  xia.hippocampus/consolidate-if-pending! (fn [] nil)
                  xia.hippocampus/maintain-knowledge! (fn [_now] nil)
                  xia.scheduler/submit-work! (fn [kind _f]
                                               (swap! submitted conj kind)
                                               true)]
      (scheduler/tick-once!)
      (is (every? (set @submitted) ["schedule alpha" "schedule beta"])))))

(deftest tick-starts-scheduled-backup-when-due
  (let [called (promise)]
    (with-redefs [xia.schedule/due-schedules (fn [_now] [])
                  xia.backup/backup-due? (fn [] true)
                  xia.scheduler/submit-work! (fn [kind f]
                                               (when (= "automatic backup" kind)
                                                 (f))
                                               true)
                  xia.backup/run-scheduled-backup! (fn []
                                                     (deliver called :ran)
                                                     {:status :success})
                  xia.hippocampus/consolidate-if-pending! (fn [] nil)
                  xia.hippocampus/maintain-knowledge! (fn [_now] nil)]
      (scheduler/tick-once!)
      (is (= :ran (deref called 1000 nil))))))

(deftest execute-schedule-proactively-refreshes-oauth-before-run
  (let [calls (atom [])]
    (with-redefs [xia.oauth/refresh-autonomous-accounts! (fn []
                                                           (swap! calls conj :refresh)
                                                           {:status :ok
                                                            :checked 1
                                                            :refreshed [:github]
                                                            :errors []})
                  xia.scheduler/execute-tool-schedule (fn [_sched]
                                                        (swap! calls conj :execute))
                  xia.schedule/trim-history! (fn [& _] nil)]
      (scheduler/run-schedule! {:id :nightly :type :tool})
      (is (= [:refresh :execute] @calls)))))

(deftest execute-schedule-continues-when-proactive-oauth-refresh-fails
  (let [executed? (atom false)]
    (with-redefs [xia.oauth/refresh-autonomous-accounts! (fn []
                                                           (throw (ex-info "refresh boom" {})))
                  xia.scheduler/execute-tool-schedule (fn [_sched]
                                                        (reset! executed? true))
                  xia.schedule/trim-history! (fn [& _] nil)]
      (scheduler/run-schedule! {:id :nightly :type :tool})
      (is (true? @executed?)))))
