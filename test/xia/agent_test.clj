(ns xia.agent-test
  (:require [clojure.test :refer :all]
            [charred.api :as json]
            [clojure.string :as str]
            [datalevin.embedding :as emb]
            [xia.agent :as agent]
            [xia.agent.task-runtime :as task-runtime]
            [xia.agent.tools :as agent-tools]
            [xia.async :as async]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.prompt :as prompt]
            [xia.retrieval-state :as retrieval-state]
            [xia.tool :as tool]
            [xia.test-helpers :refer [with-test-db]]
            [xia.working-memory :as wm]))

(use-fixtures :each with-test-db)

(defn- semantic-loop-test-provider
  []
  (reify emb/IEmbeddingProvider
    (embedding [_ items _opts]
      (mapv (fn [item]
              (let [text (-> item str str/lower-case)]
                (cond
                  (or (str/includes? text "inspect config file")
                      (str/includes? text "review configuration document"))
                  [1.0 0.0 0.0]

                  (or (str/includes? text "message billing customer")
                      (str/includes? text "follow up with the billing customer"))
                  [0.0 1.0 0.0]

                  :else
                  [0.0 0.0 1.0])))
            items))
    (embedding-metadata [_]
      {:provider :semantic-loop-test})
    (embedding-dimensions [_]
      3)
    (close-provider [_]
      nil)
    java.lang.AutoCloseable
    (close [_]
      nil)))

(deftest process-message-reports-progress
  (let [session-id (db/create-session! :terminal)
        statuses   (atom [])]
    (wm/ensure-wm! session-id)
    (prompt/register-status! :terminal
                             (fn [status]
                               (swap! statuses conj (select-keys status
                                                                 [:state :phase :message]))))
    (try
      (with-redefs [xia.tool/tool-definitions (constantly [])
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.llm/chat-message      (fn [_messages & _opts] {"content" "All set."})]
        (is (= "All set."
               (agent/process-message session-id "hello" :channel :terminal))))
      (is (= [:understanding :working-memory :planning :llm :finalizing :observing :complete]
             (mapv :phase @statuses)))
      (is (str/includes? (:message (first @statuses))
                         "Iteration 1/6: Understanding hello"))
      (is (str/includes? (:message (nth @statuses 5))
                         "Iteration 1/6: Observed hello"))
      (is (= {:state :done
              :phase :complete
              :message "Ready"}
             (last @statuses)))
      (finally
        (prompt/register-status! :terminal nil)))))

(deftest process-message-creates-task-turn-and-items
  (let [session-id (db/create-session! :terminal)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       {"content" "All set."})]
      (is (= "All set."
             (agent/process-message session-id "hello" :channel :terminal))))
    (let [tasks (db/list-tasks {:session-id session-id})
          task-id (:id (first tasks))
          task (db/get-task task-id)
          turns (db/task-turns task-id)
          items (db/turn-items (:id (first turns)))
          item-types (set (map :type items))]
      (is (= 1 (count tasks)))
      (is (= :completed (:state task)))
      (is (= :interactive (:type task)))
      (is (= session-id (:session-id task)))
      (is (nil? (:current-turn-id task)))
      (is (= 1 (count turns)))
      (is (= :completed (:state (first turns))))
      (is (= :start (:operation (first turns))))
      (is (contains? item-types :user-message))
      (is (contains? item-types :assistant-message))
      (is (contains? item-types :status))
      (is (contains? item-types :checkpoint))
      (is (= :user (:role (first (filter #(= :user-message (:type %)) items)))))
      (is (= :assistant (:role (first (filter #(= :assistant-message (:type %)) items))))))))

(deftest process-message-attaches-to-an-existing-task
  (let [session-id (db/create-session! :terminal)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       {"content" "All set."})]
      (is (= "All set."
             (agent/process-message session-id "first pass" :channel :terminal)))
      (let [task-id (:id (first (db/list-tasks {:session-id session-id})))]
        (is (= "All set."
               (agent/process-message session-id
                                      "follow up"
                                      :channel :terminal
                                      :task-id task-id)))
        (let [tasks (db/list-tasks {:session-id session-id})
              turns (db/task-turns task-id)
              turn-ops (mapv :operation turns)]
          (is (= 1 (count tasks)))
          (is (= 2 (count turns)))
          (is (= [:start :resume] turn-ops)))))))

(deftest process-message-prefers-task-autonomy-state-when-attaching
  (let [session-id           (db/create-session! :terminal)
        task-autonomy-state  (autonomous/initial-state "Investigate the billing discrepancy")
        wm-autonomy-state    (autonomous/initial-state "Stale working memory goal")
        task-id              (db/create-task! {:session-id session-id
                                               :channel :terminal
                                               :type :interactive
                                               :state :paused
                                               :title "Investigate the billing discrepancy"
                                               :summary "Waiting for the next pass"
                                               :stop-reason :paused
                                               :autonomy-state task-autonomy-state})
        seen-messages        (atom nil)]
    (wm/create-wm! session-id)
    (wm/set-autonomy-state! session-id wm-autonomy-state)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "base"}]
                                                        :used-fact-eids []})
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.llm/chat-message               (fn [messages & _opts]
                                                       (reset! seen-messages messages)
                                                       {"content" "Continuing.\nAUTONOMOUS_STATUS_JSON:{\"status\":\"complete\",\"summary\":\"done\",\"goal_complete\":false}"})]
      (is (= "Continuing."
             (agent/process-message session-id
                                    "Continue the investigation"
                                    :channel :terminal
                                    :task-id task-id))))
    (let [system-content (some-> @seen-messages first :content)]
      (is (str/includes? system-content "Investigate the billing discrepancy"))
      (is (not (str/includes? system-content "Stale working memory goal"))))))

(deftest pause-task-updates-task-and-records-a-control-turn
  (let [session-id (db/create-session! :terminal)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :interactive
                                     :state :running
                                     :title "Reply to the billing emails"})]
    (let [result       (agent/pause-task! task-id)
          task         (db/get-task task-id)
          turns        (db/task-turns task-id)
          control-turn (last turns)
          items        (db/turn-items (:id control-turn))]
      (is (= :paused (:status result)))
      (is (= :paused (:state task)))
      (is (= :paused (:stop-reason task)))
      (is (= "Task paused by user" (:summary task)))
      (is (= :pause (:operation control-turn)))
      (is (= :completed (:state control-turn)))
      (is (= [:system-note] (mapv :type items)))
      (is (= "Task paused by user" (:summary (first items)))))))

(deftest stop-task-updates-task-and-records-a-control-turn
  (let [session-id (db/create-session! :terminal)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :interactive
                                     :state :paused
                                     :title "Reply to the billing emails"})]
    (let [result       (agent/stop-task! task-id)
          task         (db/get-task task-id)
          turns        (db/task-turns task-id)
          control-turn (last turns)
          items        (db/turn-items (:id control-turn))]
      (is (= :stopped (:status result)))
      (is (= :cancelled (:state task)))
      (is (= :stopped (:stop-reason task)))
      (is (= "Task stopped by user" (:summary task)))
      (is (instance? java.util.Date (:finished-at task)))
      (is (= :stop (:operation control-turn)))
      (is (= :completed (:state control-turn)))
      (is (= [:system-note] (mapv :type items)))
      (is (= "Task stopped by user" (:summary (first items)))))))

(deftest interrupt-task-updates-task-and-records-a-control-turn
  (let [session-id (db/create-session! :terminal)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :interactive
                                     :state :running
                                     :title "Reply to the billing emails"})]
    (let [result       (agent/interrupt-task! task-id)
          task         (db/get-task task-id)
          turns        (db/task-turns task-id)
          control-turn (last turns)
          items        (db/turn-items (:id control-turn))]
      (is (= :paused (:status result)))
      (is (= :paused (:state task)))
      (is (= :interrupted (:stop-reason task)))
      (is (= "Task interrupted by user" (:summary task)))
      (is (= :interrupt (:operation control-turn)))
      (is (= :completed (:state control-turn)))
      (is (= [:system-note] (mapv :type items)))
      (is (= "Task interrupted by user" (:summary (first items)))))))

(deftest pause-task-prefers-live-task-run-over-session-lookup
  (let [session-id   (db/create-session! :terminal)
        task-id      (db/create-task! {:session-id session-id
                                       :channel :terminal
                                       :type :interactive
                                       :state :running
                                       :title "Review the invoice"})
        cancel-calls (atom [])]
    (let [result (task-runtime/pause-task! {:task-run-entry (fn [id]
                                                              (when (= task-id id)
                                                                {:task-id task-id
                                                                 :session-id session-id
                                                                 :task-turn-id (random-uuid)}))
                                            :session-run-entry (constantly nil)
                                            :cancel-session! (fn [sid reason]
                                                               (swap! cancel-calls conj [sid reason])
                                                               true)}
                                           task-id)]
      (is (= :pausing (:status result)))
      (is (= task-id (:task-id result)))
      (is (= session-id (:session-id result)))
      (is (= [[session-id "task pause requested"]] @cancel-calls)))))

(deftest resume-task-restarts-a-paused-task
  (let [session-id     (db/create-session! :terminal)
        task-id        (db/create-task! {:session-id session-id
                                         :channel :terminal
                                         :type :interactive
                                         :state :paused
                                         :title "Review the invoice"
                                         :summary "Waiting for the next pass"
                                         :stop-reason :paused
                                         :finished-at (java.util.Date.)})
        submitted-work (atom nil)
        process-calls  (atom [])]
    (with-redefs [xia.async/submit-background! (fn [_label f]
                                                 (reset! submitted-work f)
                                                 ::submitted)
                  xia.agent/process-message     (fn [sid message & {:as opts}]
                                                 (swap! process-calls conj {:session-id sid
                                                                            :message message
                                                                            :opts opts})
                                                 "ok")]
      (let [result (agent/resume-task! task-id :message "Continue reviewing the invoice")
            task   (db/get-task task-id)]
        (is (= :running (:status result)))
        (is (= :running (:state task)))
        (is (nil? (:stop-reason task)))
        (is (nil? (:finished-at task)))
        (is (= "Task resumed" (:summary task)))
        (is (fn? @submitted-work))
        (@submitted-work)
        (is (= [{:session-id session-id
                 :message "Continue reviewing the invoice"
                 :opts {:channel :terminal
                        :task-id task-id
                        :runtime-op :resume
                        :persist-message? false}}]
               @process-calls))))))

(deftest steer-task-restarts-a-task-with-a-new-instruction
  (let [session-id     (db/create-session! :terminal)
        task-id        (db/create-task! {:session-id session-id
                                         :channel :terminal
                                         :type :interactive
                                         :state :paused
                                         :title "Review the invoice"
                                         :summary "Waiting for the next pass"
                                         :stop-reason :paused
                                         :finished-at (java.util.Date.)})
        submitted-work (atom nil)
        process-calls  (atom [])]
    (with-redefs [xia.async/submit-background! (fn [_label f]
                                                 (reset! submitted-work f)
                                                 ::submitted)
                  xia.agent/process-message     (fn [sid message & {:as opts}]
                                                 (swap! process-calls conj {:session-id sid
                                                                            :message message
                                                                            :opts opts})
                                                 "ok")]
      (let [result (agent/steer-task! task-id "Focus on the disputed invoice line item")
            task   (db/get-task task-id)]
        (is (= :steering (:status result)))
        (is (= :running (:state task)))
        (is (nil? (:stop-reason task)))
        (is (nil? (:finished-at task)))
        (is (= "Focus on the disputed invoice line item" (:summary task)))
        (is (fn? @submitted-work))
        (@submitted-work)
        (is (= [{:session-id session-id
                 :message "Focus on the disputed invoice line item"
                 :opts {:channel :terminal
                        :task-id task-id
                        :runtime-op :steer
                        :interrupting-turn-id nil}}]
               @process-calls))))))

(deftest steer-task-prefers-live-task-turn-and-task-idle-wait
  (let [session-id     (db/create-session! :terminal)
        task-id        (db/create-task! {:session-id session-id
                                         :channel :terminal
                                         :type :interactive
                                         :state :running
                                         :title "Reply to the billing emails"})
        persisted-turn (db/start-task-turn! task-id
                                            {:operation :start
                                             :state :running
                                             :input "reply to the billing emails"
                                             :summary "Persisted turn"})
        live-turn      (random-uuid)
        submitted-work (atom nil)
        process-calls  (atom [])
        cancel-calls   (atom [])
        idle-waits     (atom [])]
    (db/update-task! task-id {:current-turn-id persisted-turn})
    (with-redefs [xia.async/submit-background! (fn [_label f]
                                                 (reset! submitted-work f)
                                                 ::submitted)]
      (let [result (task-runtime/steer-task!
                    {:task-run-entry (fn [id]
                                       (when (= task-id id)
                                         {:task-id task-id
                                          :task-turn-id live-turn
                                          :session-id session-id}))
                     :session-run-entry (constantly nil)
                     :cancel-session! (fn [sid reason]
                                        (swap! cancel-calls conj [sid reason])
                                        true)
                     :process-message (fn [sid message & {:as opts}]
                                        (swap! process-calls conj {:session-id sid
                                                                   :message message
                                                                   :opts opts})
                                        "ok")
                     :task-control-wait-ms (constantly 1234)
                     :truncate-summary (fn [text max-chars]
                                         (subs text 0 (min (count text) (int max-chars))))
                     :wait-for-task-idle! (fn [id timeout-ms]
                                            (swap! idle-waits conj [id timeout-ms])
                                            true)
                     :wait-for-session-idle! (fn [& _]
                                               (throw (ex-info "session wait should not be used"
                                                               {:type ::unexpected-session-wait})))}
                    task-id
                    "Switch to the escalated billing thread")]
        (is (= :steering (:status result)))
        (is (= [[session-id "task steer requested"]] @cancel-calls))
        (is (fn? @submitted-work))
        (@submitted-work)
        (is (= [[task-id 1234]] @idle-waits))
        (is (= [{:session-id session-id
                 :message "Switch to the escalated billing thread"
                 :opts {:channel :terminal
                        :task-id task-id
                        :runtime-op :steer
                        :interrupting-turn-id live-turn}}]
               @process-calls))))))

(deftest steer-task-cancels-an-active-run-before-restarting
  (let [session-id     (db/create-session! :terminal)
        task-id        (db/create-task! {:session-id session-id
                                         :channel :terminal
                                         :type :interactive
                                         :state :running
                                         :title "Reply to the billing emails"})
        current-turn-id (db/start-task-turn! task-id
                                             {:operation :start
                                              :state :running
                                              :input "reply to the billing emails"
                                              :summary "Working on the draft"})
        submitted-work (atom nil)
        cancelled      (atom [])
        process-calls  (atom [])]
    (#'xia.agent/with-session-run
     session-id
     (fn []
       (with-redefs [xia.agent/cancel-session!         (fn [sid reason]
                                                         (swap! cancelled conj [sid reason])
                                                         true)
                     xia.async/submit-background!      (fn [_label f]
                                                         (reset! submitted-work f)
                                                         ::submitted)
                     xia.agent/wait-for-session-idle! (fn [_sid _timeout-ms]
                                                        true)
                     xia.agent/process-message        (fn [sid message & {:as opts}]
                                                        (swap! process-calls conj {:session-id sid
                                                                                   :message message
                                                                                   :opts opts})
                                                        "ok")]
         (let [result (agent/steer-task! task-id "Switch to the escalated billing thread")]
           (is (= :steering (:status result)))
           (is (= [[session-id "task steer requested"]] @cancelled))
           (is (fn? @submitted-work))
           (@submitted-work)))))
    (is (= [{:session-id session-id
             :message "Switch to the escalated billing thread"
             :opts {:channel :terminal
                    :task-id task-id
                    :runtime-op :steer
                    :interrupting-turn-id current-turn-id}}]
           @process-calls))))

(deftest steer-task-prefers-task-current-turn-id-over-latest-turn
  (let [session-id      (db/create-session! :terminal)
        task-id         (db/create-task! {:session-id session-id
                                          :channel :terminal
                                          :type :interactive
                                          :state :running
                                          :title "Reply to the billing emails"})
        current-turn-id (db/start-task-turn! task-id
                                             {:operation :start
                                              :state :running
                                              :input "reply to the billing emails"
                                              :summary "Working on the first draft"})
        _               (db/start-task-turn! task-id
                                             {:operation :resume
                                              :state :completed
                                              :input "old follow up"
                                              :summary "Completed a stale resume"})
        _               (db/update-task! task-id {:current-turn-id current-turn-id})
        submitted-work  (atom nil)
        cancelled       (atom [])
        process-calls   (atom [])]
    (#'xia.agent/with-session-run
     session-id
     (fn []
       (with-redefs [xia.agent/cancel-session!         (fn [sid reason]
                                                         (swap! cancelled conj [sid reason])
                                                         true)
                     xia.async/submit-background!      (fn [_label f]
                                                         (reset! submitted-work f)
                                                         ::submitted)
                     xia.agent/wait-for-session-idle!  (fn [_sid _timeout-ms]
                                                         true)
                     xia.agent/process-message         (fn [sid message & {:as opts}]
                                                         (swap! process-calls conj {:session-id sid
                                                                                    :message message
                                                                                    :opts opts})
                                                         "ok")]
         (let [result (agent/steer-task! task-id "Switch to the escalated billing thread")]
           (is (= :steering (:status result)))
           (is (= [[session-id "task steer requested"]] @cancelled))
           (is (fn? @submitted-work))
           (@submitted-work)))))
    (is (= [{:session-id session-id
             :message "Switch to the escalated billing thread"
             :opts {:channel :terminal
                    :task-id task-id
                    :runtime-op :steer
                    :interrupting-turn-id current-turn-id}}]
           @process-calls))))

(deftest fork-task-creates-a-child-task-and-starts-it-in-a-worker-session
  (let [parent-session-id (db/create-session! :terminal)
        parent-task-id    (db/create-task! {:session-id parent-session-id
                                            :channel :terminal
                                            :type :interactive
                                            :state :running
                                            :title "Review the invoice"})
        submitted-work    (atom nil)
        process-calls     (atom [])
        deactivated       (atom [])
        cleared           (atom [])]
    (with-redefs [xia.async/submit-background! (fn [_label f]
                                                 (reset! submitted-work f)
                                                 ::submitted)
                  xia.agent/process-message     (fn [sid message & {:as opts}]
                                                 (swap! process-calls conj {:session-id sid
                                                                            :message message
                                                                            :opts opts})
                                                 "ok")
                  xia.db/set-session-active!    (fn [sid active?]
                                                 (swap! deactivated conj [sid active?])
                                                 true)
                  xia.working-memory/clear-wm!  (fn [sid]
                                                 (swap! cleared conj sid)
                                                 true)]
      (let [result       (agent/fork-task! parent-task-id "Investigate the disputed invoice line item")
            child-task   (:task result)
            child-task-id (:id child-task)
            parent-task   (db/get-task parent-task-id)
            parent-tip    (some-> parent-task :autonomy-state autonomous/current-frame)
            child-turns  (db/task-turns child-task-id)
            parent-turns (db/task-turns parent-task-id)]
        (is (= :forking (:status result)))
        (is (= parent-task-id (:parent-id child-task)))
        (is (= :branch (:channel child-task)))
        (is (= :branch (:type child-task)))
        (is (= :running (:state child-task)))
        (is (= "Investigate the disputed invoice line item" (:title child-task)))
        (is (= :child-task (:kind parent-tip)))
        (is (= child-task-id (:child-task-id parent-tip)))
        (is (= "Investigate the disputed invoice line item" (:title parent-tip)))
        (is (uuid? (:session-id child-task)))
        (is (empty? child-turns))
        (is (= :fork (:operation (last parent-turns))))
        (is (fn? @submitted-work))
        (@submitted-work)
        (is (= [{:session-id (:session-id child-task)
                 :message "Investigate the disputed invoice line item"
                 :opts {:channel :branch
                        :task-id child-task-id
                        :runtime-op :fork
                        :resource-session-id parent-session-id
                        :tool-context {:branch-worker? true
                                       :parent-session-id parent-session-id
                                       :resource-session-id parent-session-id}}}]
               @process-calls))
        (is (= [[(:session-id child-task) false]] @deactivated))
        (is (= [(:session-id child-task)] @cleared))
        (db/update-task! child-task-id
                         {:state :completed
                          :summary "Invoice evidence collected"})
        (let [reconciled (task-runtime/runtime-autonomy-state parent-session-id parent-task-id)
              refreshed-parent (db/get-task parent-task-id)]
          (is (= 1 (count (:stack reconciled))))
          (is (= "Review the invoice"
                 (some-> reconciled autonomous/current-frame :title)))
          (is (= reconciled (:autonomy-state refreshed-parent))))))))

(deftest process-message-records-tool-call-items
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "workspace-read"
                                                                              :parameters {}}}])
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                       {:summary "Read the billing note"
                                                        :path "notes/billing.md"})
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "ACTION_INTENT_JSON:{"
                                                                           "\"focus\":\"Review the billing note\","
                                                                           "\"agenda_item\":\"Read the note\","
                                                                           "\"plan_step\":\"Use workspace-read\","
                                                                           "\"why\":\"Need the file contents first\","
                                                                           "\"tool\":\"workspace-read\","
                                                                           "\"tool_args_summary\":\"{\\\"path\\\":\\\"notes/billing.md\\\"}\""
                                                                           "}\n\nReading the note.")
                                                            "tool_calls" [{"id" "call-1"
                                                                           "function" {"name" "workspace-read"
                                                                                       "arguments" "{\"path\":\"notes/billing.md\"}"}}]}
                                                         {"content" (str "Finished reviewing the note.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:{"
                                                                         "\"status\":\"complete\","
                                                                         "\"summary\":\"Reviewed the billing note\","
                                                                         "\"next_step\":\"\","
                                                                         "\"reason\":\"The note has been read\","
                                                                         "\"goal_complete\":true,"
                                                                         "\"current_focus\":\"Review the billing note\","
                                                                         "\"stack_action\":\"stay\","
                                                                         "\"progress_status\":\"complete\","
                                                                         "\"agenda\":[{\"item\":\"Read the note\",\"status\":\"completed\"}]"
                                                                         "}")}))]
      (is (= "Finished reviewing the note."
             (agent/process-message session-id "Review the billing note" :channel :terminal))))
    (let [task-id    (:id (first (db/list-tasks {:session-id session-id})))
          turn-id    (:id (first (db/task-turns task-id)))
          items      (db/turn-items turn-id)
          tool-call  (first (filter #(= :tool-call (:type %)) items))
          tool-result (first (filter #(= :tool-result (:type %)) items))]
      (is tool-call)
      (is (= :requested (:status tool-call)))
      (is (= "workspace-read" (:tool-id tool-call)))
      (is (= "call-1" (:tool-call-id tool-call)))
      (is (= "{\"path\":\"notes/billing.md\"}"
             (get-in tool-call [:data :arguments])))
      (is tool-result)
      (is (= :success (:status tool-result)))
      (is (= "workspace-read" (:tool-id tool-result))))))

(deftest process-message-records-input-request-items-and-waiting-state
  (let [session-id      (db/create-session! :terminal)
        llm-calls       (atom 0)
        prompt-started  (promise)
        prompt-response (promise)]
    (prompt/register-prompt! :terminal
                             (fn [label & _]
                               (deliver prompt-started label)
                               (deref prompt-response 3000 "123456")))
    (try
      (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                    xia.tool/tool-definitions          (constantly [{:type "function"
                                                                     :function {:name "ask-user"
                                                                                :parameters {}}}])
                    xia.tool/parallel-safe?            (constantly false)
                    xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                         {:summary (str "Captured code "
                                                                        (prompt/prompt! "OTP Code" :mask? true))})
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.context/build-messages-data    (fn [_session-id _opts]
                                                         {:messages [{:role "system" :content "test"}]
                                                          :used-fact-eids []})
                    xia.llm/chat-message               (fn [_messages & _opts]
                                                         (case (swap! llm-calls inc)
                                                           1 {"content" (str "ACTION_INTENT_JSON:{"
                                                                             "\"focus\":\"Complete the login\","
                                                                             "\"agenda_item\":\"Collect OTP\","
                                                                             "\"plan_step\":\"Ask for the OTP code\","
                                                                             "\"why\":\"The tool needs the user code before continuing\","
                                                                             "\"tool\":\"ask-user\","
                                                                             "\"tool_args_summary\":\"{}\""
                                                                             "}\n\nRequesting the OTP.")
                                                              "tool_calls" [{"id" "call-1"
                                                                             "function" {"name" "ask-user"
                                                                                         "arguments" "{}"}}]}
                                                           {"content" (str "Captured the OTP.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:{"
                                                                           "\"status\":\"complete\","
                                                                           "\"summary\":\"Captured the OTP\","
                                                                           "\"next_step\":\"\","
                                                                           "\"reason\":\"The login code is available\","
                                                                           "\"goal_complete\":true,"
                                                                           "\"current_focus\":\"Complete the login\","
                                                                           "\"stack_action\":\"stay\","
                                                                           "\"progress_status\":\"complete\","
                                                                           "\"agenda\":[{\"item\":\"Collect OTP\",\"status\":\"completed\"}]"
                                                                           "}")}))]
        (let [result (future
                       (agent/process-message session-id "Complete the login" :channel :terminal))]
          (is (= "OTP Code" (deref prompt-started 3000 ::timeout)))
          (let [task-id (:id (first (db/list-tasks {:session-id session-id})))
                task    (db/get-task task-id)
                turn    (first (db/task-turns task-id))
                items   (db/turn-items (:id turn))]
            (is (= :waiting_input (:state task)))
            (is (= (:id turn) (:current-turn-id task)))
            (is (= :waiting_input (get-in task [:meta :runtime :state])))
            (is (= :waiting_input (get-in task [:meta :runtime :phase])))
            (is (= "Waiting for input: OTP Code" (get-in task [:meta :runtime :message])))
            (is (= :waiting_input (:state turn)))
            (is (some #(and (= :input-request (:type %))
                            (= :waiting (:status %))
                            (= "OTP Code" (get-in % [:data :label])))
                      items)))
          (deliver prompt-response "123456")
          (is (= "Captured the OTP."
                 (deref result 3000 ::timeout)))
          (let [task-id (:id (first (db/list-tasks {:session-id session-id})))]
            (is (nil? (:current-turn-id (db/get-task task-id)))))))
      (finally
        (prompt/register-prompt! :terminal nil)))))

(deftest process-message-records-approval-request-items-and-waiting-state
  (let [session-id         (db/create-session! :terminal)
        llm-calls          (atom 0)
        approval-started   (promise)
        approval-decision  (promise)]
    (prompt/register-approval! :terminal
                               (fn [request]
                                 (deliver approval-started (:tool-name request))
                                 (deref approval-decision 3000 true)))
    (try
      (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                    xia.tool/tool-definitions          (constantly [{:type "function"
                                                                     :function {:name "send-email"
                                                                                :parameters {}}}])
                    xia.tool/parallel-safe?            (constantly false)
                    xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                         {:approved? (prompt/approve! {:tool-id :email-send
                                                                                       :tool-name "email-send"
                                                                                       :description "Send the billing reply"
                                                                                       :arguments {:to "billing@example.com"}
                                                                                       :policy :always})
                                                          :summary "Approval decision captured"})
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.context/build-messages-data    (fn [_session-id _opts]
                                                         {:messages [{:role "system" :content "test"}]
                                                          :used-fact-eids []})
                    xia.llm/chat-message               (fn [_messages & _opts]
                                                         (case (swap! llm-calls inc)
                                                           1 {"content" (str "ACTION_INTENT_JSON:{"
                                                                             "\"focus\":\"Send the billing reply\","
                                                                             "\"agenda_item\":\"Get approval\","
                                                                             "\"plan_step\":\"Ask for approval before sending\","
                                                                             "\"why\":\"The email tool is privileged\","
                                                                             "\"tool\":\"send-email\","
                                                                             "\"tool_args_summary\":\"{\\\"to\\\":\\\"billing@example.com\\\"}\""
                                                                             "}\n\nRequesting approval.")
                                                              "tool_calls" [{"id" "call-1"
                                                                             "function" {"name" "send-email"
                                                                                         "arguments" "{\"to\":\"billing@example.com\"}"}}]}
                                                           {"content" (str "Approval was handled.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:{"
                                                                           "\"status\":\"complete\","
                                                                           "\"summary\":\"Handled the approval\","
                                                                           "\"next_step\":\"\","
                                                                           "\"reason\":\"The approval flow finished\","
                                                                           "\"goal_complete\":true,"
                                                                           "\"current_focus\":\"Send the billing reply\","
                                                                           "\"stack_action\":\"stay\","
                                                                           "\"progress_status\":\"complete\","
                                                                           "\"agenda\":[{\"item\":\"Get approval\",\"status\":\"completed\"}]"
                                                                           "}")}))]
        (let [result (future
                       (agent/process-message session-id "Send the billing reply" :channel :terminal))]
          (is (= "email-send" (deref approval-started 3000 ::timeout)))
          (let [task-id (:id (first (db/list-tasks {:session-id session-id})))
                task    (db/get-task task-id)
                turn    (first (db/task-turns task-id))
                items   (db/turn-items (:id turn))]
            (is (= :waiting_approval (:state task)))
            (is (= (:id turn) (:current-turn-id task)))
            (is (= :waiting_approval (get-in task [:meta :runtime :state])))
            (is (= :waiting_approval (get-in task [:meta :runtime :phase])))
            (is (= "Waiting for approval for email-send" (get-in task [:meta :runtime :message])))
            (is (= :waiting_approval (:state turn)))
            (is (some #(and (= :approval-request (:type %))
                            (= :waiting (:status %))
                            (= "email-send" (get-in % [:data :tool-name])))
                      items)))
          (deliver approval-decision true)
          (is (= "Approval was handled."
                 (deref result 3000 ::timeout)))
          (let [task-id (:id (first (db/list-tasks {:session-id session-id})))]
            (is (nil? (:current-turn-id (db/get-task task-id)))))))
      (finally
        (prompt/register-approval! :terminal nil)))))

(deftest process-message-persists-llm-provenance-and-audit-events
  (let [session-id (db/create-session! :terminal)
        call-id    (random-uuid)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :openrouter}
                                                                  :provider-id :openrouter})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (with-meta {"content" "All set."}
                                                         {:provider-id :openrouter
                                                          :model "moonshotai/kimi-k2.5"
                                                          :workload :assistant
                                                          :llm-call-id call-id}))]
      (is (= "All set."
             (agent/process-message session-id "hello" :channel :terminal))))
    (let [messages (db/session-messages session-id)
          assistant (last messages)
          events (db/session-audit-events session-id)]
      (is (= call-id (:llm-call-id assistant)))
      (is (= :openrouter (:provider-id assistant)))
      (is (= "moonshotai/kimi-k2.5" (:model assistant)))
      (is (= :assistant (:workload assistant)))
      (is (= [:user-message :llm-response]
             (mapv :type events)))
      (is (= call-id (:llm-call-id (last events)))))))

(deftest tool-round-signature-preserves-sanitized-result-details
  (let [signature (agent-tools/tool-round-signature
                   [{"id" "call-1"
                     "function" {"name" "workspace-read"
                                 "arguments" "{\"path\":\"notes/billing.md\"}"}}]
                   [{:tool_call_id "call-1"
                     :tool_name "workspace-read"
                     :result {:summary "Read the billing note"
                              :path "notes/billing.md"
                              :line-count 42
                              :image_data_url "data:image/png;base64,xxx"}
                     :content "Read the billing note"}])]
    (is (= [{:name "workspace-read"
             :arguments "{\"path\":\"notes/billing.md\"}"}]
           (:calls signature)))
    (is (= [{:tool-name "workspace-read"
             :status "success"
             :summary "Read the billing note"
             :result {:summary "Read the billing note"
                      :path "notes/billing.md"
                      :line-count 42}
             :content "Read the billing note"}]
           (:results signature)))))

(deftest semantic-loop-equivalent-does-not-treat-missing-fallbacks-as-a-match
  (let [result (#'xia.agent/semantic-loop-equivalent?
                {}
                {:semantic-fallback nil
                 :semantic-text nil}
                {:semantic-fallback nil
                 :semantic-text nil})]
    (is (false? (:same-semantic? result)))
    (is (nil? (:semantic-similarity result)))
    (is (nil? (:semantic-match-source result)))))

(deftest process-message-audits-tool-result-completions
  (let [session-id     (db/create-session! :terminal)
        tool-call-id   (random-uuid)
        final-call-id  (random-uuid)
        llm-calls      (atom 0)]
    (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}
                                                                  {:type "function"
                                                                   :function {:name "email-send"
                                                                              :parameters {}}}])
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [tool-id _args _context]
                                                       (case tool-id
                                                         :web-search {:summary "Found the billing thread."}
                                                         :email-send {:error "SMTP rejected the recipient"}
                                                         {:summary "ok"}))
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :openrouter}
                                                                  :provider-id :openrouter})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (if (= 1 (swap! llm-calls inc))
                                                         (with-meta
                                                           {"content" (str "ACTION_INTENT_JSON:"
                                                                           "{\"focus\":\"Reply to the billing emails\",\"agenda_item\":\"Check inbox\",\"plan_step\":\"Search billing and send reply\",\"why\":\"Need tool results before replying\",\"tool\":\"web-search\",\"tool_args_summary\":\"{}\"}\n\n"
                                                                           "Checking tools.")
                                                            "tool_calls" [{"id" "call-1"
                                                                           "function" {"name" "web-search"
                                                                                       "arguments" "{}"}}
                                                                          {"id" "call-2"
                                                                           "function" {"name" "email-send"
                                                                                       "arguments" "{}"}}]}
                                                           {:provider-id :openrouter
                                                            :model "moonshotai/kimi-k2.5"
                                                            :workload :assistant
                                                            :llm-call-id tool-call-id})
                                                         (with-meta
                                                           {"content" "Done."}
                                                           {:provider-id :openrouter
                                                            :model "moonshotai/kimi-k2.5"
                                                            :workload :assistant
                                                            :llm-call-id final-call-id})))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Done."
             (agent/process-message session-id "reply to the billing emails" :channel :terminal))))
    (let [events              (db/session-audit-events session-id)
          tool-result-events  (filterv #(= :tool-result (:type %)) events)
          success-event       (some #(when (= "web-search" (:tool-id %)) %) tool-result-events)
          error-event         (some #(when (= "email-send" (:tool-id %)) %) tool-result-events)]
      (is (= 2 (count tool-result-events)))
      (is (= tool-call-id (:llm-call-id success-event)))
      (is (= "call-1" (:tool-call-id success-event)))
      (is (= {"tool-name" "web-search"
              "status" "success"
              "summary" "Found the billing thread."}
             (:data success-event)))
      (is (:message-id success-event))
      (is (= tool-call-id (:llm-call-id error-event)))
      (is (= "call-2" (:tool-call-id error-event)))
      (is (= {"tool-name" "email-send"
              "status" "error"
              "summary" "{\"error\":\"SMTP rejected the recipient\"}"
              "error" "SMTP rejected the recipient"}
             (:data error-event)))
      (is (:message-id error-event)))))

(deftest process-message-reports-llm-partial-content
  (let [session-id (db/create-session! :terminal)
        statuses   (atom [])]
    (wm/ensure-wm! session-id)
    (prompt/register-status! :terminal
                             (fn [status]
                               (swap! statuses conj (select-keys status
                                                                 [:state :phase :message :partial-content]))))
    (try
      (with-redefs [xia.tool/tool-definitions (constantly [])
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.llm/chat-message (fn [_messages & {:keys [on-delta]}]
                                           (when on-delta
                                             (on-delta {:delta "Hello"
                                                        :content "Hello"}))
                                           {"content" "Hello there."})]
        (is (= "Hello there."
               (agent/process-message session-id "hello" :channel :terminal))))
      (is (= {:state :running
              :phase :llm
              :message "Calling model"
              :partial-content "Hello"}
             (some #(when (:partial-content %) %) @statuses)))
      (finally
        (prompt/register-status! :terminal nil)))))

(deftest process-message-surfaces-action-intent-before-tools
  (let [session-id (db/create-session! :terminal)
        statuses   (atom [])
        llm-calls  (atom 0)]
    (prompt/register-status! :terminal
                             (fn [status]
                               (swap! statuses conj (select-keys status
                                                                 [:state :phase :message
                                                                  :intent-focus :intent-agenda-item
                                                                  :intent-plan-step :intent-why
                                                                  :intent-tool-name
                                                                  :intent-tool-args-summary]))))
    (try
      (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                    xia.tool/tool-definitions          (constantly [{:type "function"
                                                                     :function {:name "web-search"
                                                                                :parameters {}}}])
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.context/build-messages-data    (fn [_session-id _opts]
                                                         {:messages [{:role "system" :content "test"}]
                                                          :used-fact-eids []})
                    xia.tool/parallel-safe?            (constantly false)
                    xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                         {:summary "Found the message."})
                    xia.llm/chat-message               (fn [_messages & _opts]
                                                         (if (= 1 (swap! llm-calls inc))
                                                           {"content" (str "ACTION_INTENT_JSON:"
                                                                           "{\"focus\":\"Handle billing emails\",\"agenda_item\":\"Check inbox\",\"plan_step\":\"Search unread billing email\",\"why\":\"Need the latest unread items\",\"tool\":\"web-search\",\"tool_args_summary\":\"label:billing unread\"}\n\n"
                                                                           "Searching now.")
                                                            "tool_calls" [{"id" "call-1"
                                                                           "function" {"name" "web-search"
                                                                                       "arguments" "{}"}}]}
                                                           {"content" (str "Sent the reply.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"complete\",\"summary\":\"Sent the reply\",\"next_step\":\"\",\"reason\":\"Goal satisfied\",\"goal_complete\":true,\"current_focus\":\"Handle billing emails\",\"stack_action\":\"stay\",\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Send reply\",\"status\":\"completed\"}]}")}))
                    xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
        (is (= "Sent the reply."
               (agent/process-message session-id "reply to the billing emails" :channel :terminal))))
      (is (some #(= {:state :running
                     :phase :intent
                     :message "Intent: Search unread billing email via web-search (label:billing unread)"
                     :intent-focus "Handle billing emails"
                     :intent-agenda-item "Check inbox"
                     :intent-plan-step "Search unread billing email"
                     :intent-why "Need the latest unread items"
                     :intent-tool-name "web-search"
                     :intent-tool-args-summary "label:billing unread"}
                    %)
                @statuses))
      (is (= "Searching now."
             (:content (first (filter #(and (= :assistant (:role %))
                                            (seq (:tool-calls %)))
                                      (db/session-messages session-id))))))
      (finally
        (prompt/register-status! :terminal nil)))))

(deftest process-message-rejects-tool-rounds-without-a-valid-intent-envelope
  (let [session-id (db/create-session! :terminal)]
    (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}])
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [& _]
                                                       {:summary "Found the message."})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       {"content" "Searching now."
                                                        "tool_calls" [{"id" "call-1"
                                                                       "function" {"name" "web-search"
                                                                                   "arguments" "{}"}}]})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [err (try
                  (agent/process-message session-id "reply to the billing emails" :channel :terminal)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (= :autonomous-protocol-invalid
               (:type (ex-data err))))
        (is (= :missing
               (:intent-status (ex-data err))))
        (is (= :missing
               (:control-status (ex-data err))))))))

(deftest process-message-rejects-tool-rounds-with-malformed-control-envelopes
  (let [session-id (db/create-session! :terminal)]
    (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}])
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [& _]
                                                       {:summary "Found the message."})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       {"content" (str "ACTION_INTENT_JSON:"
                                                                       "{\"focus\":\"Handle billing emails\",\"agenda_item\":\"Check inbox\",\"plan_step\":\"Search unread billing email\",\"why\":\"Need the latest unread items\",\"tool\":\"web-search\",\"tool_args_summary\":\"label:billing unread\"}\n\n"
                                                                       "AUTONOMOUS_STATUS_JSON:{\"status\":\"continue\"")
                                                        "tool_calls" [{"id" "call-1"
                                                                       "function" {"name" "web-search"
                                                                                   "arguments" "{}"}}]})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [err (try
                  (agent/process-message session-id "reply to the billing emails" :channel :terminal)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (= :autonomous-protocol-invalid
               (:type (ex-data err))))
        (is (= :parsed
               (:intent-status (ex-data err))))
        (is (= :malformed
               (:control-status (ex-data err))))))))

(deftest process-message-can-be-cancelled
  (let [session-id (db/create-session! :terminal)
        started    (promise)
        stopped    (promise)
        statuses   (atom [])]
    (prompt/register-status! :terminal
                             (fn [status]
                               (swap! statuses conj (select-keys status
                                                                 [:state :phase :message]))))
    (try
      (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                    xia.tool/tool-definitions          (constantly [])
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.context/build-messages-data    (fn [_session-id _opts]
                                                         {:messages [{:role "system" :content "test"}]
                                                          :used-fact-eids []})
                    xia.llm/chat-message               (fn [_messages & _opts]
                                                         (try
                                                           (deliver started true)
                                                           (Thread/sleep 10000)
                                                           {"content" "done"}
                                                           (catch InterruptedException e
                                                             (.interrupt (Thread/currentThread))
                                                             (throw e))
                                                           (finally
                                                             (deliver stopped true))))]
        (let [result (future
                       (try
                         (agent/process-message session-id "cancel me" :channel :terminal)
                         (catch Exception e
                           e)))]
          (is (= true (deref started 1000 ::timeout)))
          (is (true? (agent/cancel-session! session-id "user requested cancel")))
          (let [err (deref result 1000 ::timeout)]
            (is (instance? clojure.lang.ExceptionInfo err))
            (is (= {:type :request-cancelled
                    :status 499
                    :error "request cancelled"
                    :session-id session-id
                    :reason "user requested cancel"}
                   (select-keys (ex-data err)
                                [:type :status :error :session-id :reason]))))
          (is (= true (deref stopped 1000 ::timeout)))))
      (is (= {:state :cancelled
              :phase :cancelled
              :message "Request cancelled"}
             (last @statuses)))
      (finally
        (prompt/register-status! :terminal nil)))))

(deftest cancel-session-does-not-self-interrupt-the-supervisor-thread
  (let [session-id (db/create-session! :terminal)]
    (#'xia.agent/with-session-run
     session-id
     (fn []
       (is (true? (agent/cancel-session! session-id "self cancel")))
       (is (= "self cancel"
              (-> (#'xia.agent/session-run-entry session-id)
                  :cancel-reason)))
       (is (false? (Thread/interrupted)))))))

(deftest process-message-propagates-request-trace-to-status-and-checkpoints
  (let [session-id   (db/create-session! :scheduler)
        statuses     (atom [])
        checkpoints  (atom [])]
    (prompt/register-status! :scheduler
                             (fn [status]
                               (swap! statuses conj (select-keys status
                                                                 [:state :phase :message
                                                                  :request-id :correlation-id
                                                                  :parent-request-id :schedule-id
                                                                  :session-id]))))
    (try
      (binding [prompt/*interaction-context* {:channel :scheduler
                                              :schedule-id :nightly-review
                                              :request-id "req-parent"
                                              :correlation-id "corr-root"}]
        (with-redefs [xia.tool/tool-definitions          (constantly [])
                      xia.working-memory/update-wm!      (fn [& _] nil)
                      xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                      :provider-id :default})
                      xia.llm/chat-message               (fn [_messages & _opts] {"content" "All set."})
                      xia.schedule/save-task-checkpoint! (fn [schedule-id checkpoint]
                                                           (swap! checkpoints conj [schedule-id checkpoint]))]
          (is (= "All set."
                 (agent/process-message session-id "hello" :channel :scheduler)))))
      (is (every? #(= "corr-root" (:correlation-id %)) @statuses))
      (is (every? #(= "req-parent" (:parent-request-id %)) @statuses))
      (is (every? #(= :nightly-review (:schedule-id %)) @statuses))
      (is (every? #(= session-id (:session-id %)) @statuses))
      (is (every? string? (keep :request-id @statuses)))
      (is (every? #(not= "req-parent" %) (keep :request-id @statuses)))
      (is (= 1 (count (distinct (keep :request-id @statuses)))))
      (is (= :nightly-review (ffirst @checkpoints)))
      (is (every? #(= "corr-root" (get-in % [1 :correlation-id])) @checkpoints))
      (is (every? #(= "req-parent" (get-in % [1 :parent-request-id])) @checkpoints))
      (is (every? string? (map #(get-in % [1 :request-id]) @checkpoints)))
      (finally
        (prompt/register-status! :scheduler nil)))))

(deftest process-message-resolves-assistant-provider-once
  (let [session-id        (db/create-session! :terminal)
        selection-calls   (atom 0)
        build-messages-opts (atom nil)
        llm-opts          (atom nil)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (fn [_]
                                                       (swap! selection-calls inc)
                                                       {:provider {:llm.provider/id :router-a}
                                                        :provider-id :router-a})
                  xia.context/build-messages-data    (fn [_session-id opts]
                                                       (reset! build-messages-opts opts)
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & opts]
                                                       (reset! llm-opts (apply hash-map opts))
                                                       {"content" "All set."})]
      (is (= "All set."
             (agent/process-message session-id "hello" :channel :terminal))))
    (is (= 1 @selection-calls))
    (is (= :router-a (:provider-id @build-messages-opts)))
    (is (= :router-a (get-in @build-messages-opts [:provider :llm.provider/id])))
    (is (= :history-compaction (:compaction-workload @build-messages-opts)))
    (is (= :router-a (:provider-id @llm-opts)))
    (is (fn? (:on-delta @llm-opts)))))

(deftest process-message-passes-execution-context-to-tool-definitions
  (let [session-id    (db/create-session! :scheduler)
        context-seen  (atom nil)]
    (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.tool/tool-definitions          (fn [context]
                                                       (reset! context-seen context)
                                                       [])
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts] {"content" "All set."})]
      (is (= "All set."
             (agent/process-message session-id
                                    "hello"
                                    :channel :scheduler
                                    :tool-context {:schedule-id      :nightly
                                                   :autonomous-run?  true
                                                   :approval-bypass? true}))))
    (is (= {:session-id        session-id
            :channel           :scheduler
            :user-message      "hello"
            :schedule-id       :nightly
            :autonomous-run?   true
            :approval-bypass?  true}
           (select-keys @context-seen
                        [:session-id :channel :user-message :schedule-id
                         :autonomous-run? :approval-bypass?])))))

(deftest process-message-persists-schedule-checkpoints-around-tool-rounds
  (let [session-id   (db/create-session! :scheduler)
        checkpoints  (atom [])
        llm-calls    (atom 0)]
    (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}])
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                       {:summary "Found the page."})
                  xia.schedule/save-task-checkpoint! (fn [schedule-id checkpoint]
                                                       (swap! checkpoints conj [schedule-id checkpoint]))
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (if (= 1 (swap! llm-calls inc))
                                                         {"content" (str "ACTION_INTENT_JSON:"
                                                                         "{\"focus\":\"Research this\",\"agenda_item\":\"Research this\",\"plan_step\":\"Search the page\",\"why\":\"Need the first search result before continuing\",\"tool\":\"web-search\",\"tool_args_summary\":\"{}\"}")
                                                          "tool_calls" [{"id" "call-1"
                                                                         "function" {"name" "web-search"
                                                                                     "arguments" "{}"}}]}
                                                         {"content" "done"}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "done"
             (agent/process-message session-id
                                    "research this"
                                    :channel :scheduler
                                    :tool-context {:schedule-id :nightly-review}))))
    (is (= :nightly-review (ffirst @checkpoints)))
    (is (= :understanding (get-in @checkpoints [0 1 :phase])))
    (is (= :planning (get-in @checkpoints [1 1 :phase])))
    (is (= :intent (get-in @checkpoints [2 1 :phase])))
    (is (= :tool (get-in @checkpoints [3 1 :phase])))
    (is (= "web-search" (get-in @checkpoints [3 1 :tool-name])))
    (is (= :tool (get-in @checkpoints [4 1 :phase])))
    (is (= ["web-search"] (get-in @checkpoints [4 1 :tool-ids])))
    (is (= :observing (get-in @checkpoints [5 1 :phase])))))

(deftest process-message-repeats-autonomous-loop-until-controller-completes
  (let [session-id        (db/create-session! :terminal)
        llm-messages      (atom [])
        build-calls       (atom 0)
        selection-calls   (atom 0)
        reviewed          (atom nil)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (fn [_]
                                                       (swap! selection-calls inc)
                                                       {:provider {:llm.provider/id :default}
                                                        :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids [(swap! build-calls inc)]})
                  xia.llm/chat-message               (fn [messages & _opts]
                                                       (swap! llm-messages conj messages)
                                                       (case (count @llm-messages)
                                                         1 {"content" (str "Checked inbox.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Checked inbox\",\"next_step\":\"Draft replies\",\"reason\":\"Unread messages remain\",\"goal_complete\":false,\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft replies\",\"status\":\"in_progress\"},{\"item\":\"Send replies\",\"status\":\"pending\"}]}")}
                                                         {"content" (str "Sent the replies.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"complete\",\"summary\":\"Sent replies\",\"next_step\":\"\",\"reason\":\"Goal satisfied\",\"goal_complete\":true,\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft replies\",\"status\":\"completed\"},{\"item\":\"Send replies\",\"status\":\"completed\"}]}")} ))
                  xia.agent/schedule-fact-utility-review!
                  (fn [review-session-id fact-eids user-message assistant-response]
                    (reset! reviewed {:session-id review-session-id
                                      :fact-eids fact-eids
                                      :user-message user-message
                                      :assistant-response assistant-response}))]
      (is (= "Sent the replies."
             (agent/process-message session-id "reply to the billing emails" :channel :terminal))))
    (is (= 1 @selection-calls))
    (is (= 2 (count @llm-messages)))
    (is (str/includes? (get-in @llm-messages [0 0 :content])
                       "Current iteration: 1 of 6."))
    (is (str/includes? (get-in @llm-messages [1 0 :content])
                       "Current iteration: 2 of 6."))
    (is (str/includes? (get-in @llm-messages [1 0 :content])
                       "Tip summary: Checked inbox"))
    (is (str/includes? (get-in @llm-messages [1 0 :content])
                       "[in_progress] Draft replies"))
    (is (= [[:user "reply to the billing emails"]
            [:assistant "Checked inbox."]
            [:assistant "Sent the replies."]]
           (->> (db/session-messages session-id)
                (filter #(contains? #{:user :assistant} (:role %)))
                (mapv (fn [{:keys [role content]}]
                        [role content])))))
    (is (= {:session-id session-id
            :fact-eids [1 2]
            :user-message "reply to the billing emails"
            :assistant-response "Sent the replies."}
           @reviewed))))

(deftest process-message-emits-intermediate-assistant-messages-for-continue-iterations
  (let [session-id         (db/create-session! :terminal)
        assistant-messages (atom [])
        llm-calls          (atom 0)]
    (prompt/register-assistant-message! :terminal
                                        (fn [message]
                                          (swap! assistant-messages conj
                                                 (select-keys message
                                                              [:channel :session-id :text
                                                               :iteration :max-iterations
                                                               :status :progress-status]))))
    (try
      (with-redefs [xia.tool/tool-definitions          (constantly [])
                    xia.working-memory/update-wm!      (fn [& _] nil)
                    xia.working-memory/refresh-wm!     (fn [& _] nil)
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.context/build-messages-data    (fn [_session-id _opts]
                                                         {:messages [{:role "system" :content "test"}]
                                                          :used-fact-eids []})
                    xia.llm/chat-message               (fn [_messages & _opts]
                                                         (case (swap! llm-calls inc)
                                                           1 {"content" (str "Checked inbox.\n\n"
                                                                             "AUTONOMOUS_STATUS_JSON:"
                                                                             "{\"status\":\"continue\",\"summary\":\"Checked inbox\",\"next_step\":\"Draft replies\",\"reason\":\"Unread messages remain\",\"goal_complete\":false,\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft replies\",\"status\":\"in_progress\"}]}")}
                                                           {"content" (str "Sent the replies.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"complete\",\"summary\":\"Sent replies\",\"next_step\":\"\",\"reason\":\"Goal satisfied\",\"goal_complete\":true,\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft replies\",\"status\":\"completed\"}]}")} ))
                    xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
        (is (= "Sent the replies."
               (agent/process-message session-id
                                      "reply to the billing emails"
                                      :channel :terminal))))
      (is (= [{:channel :terminal
               :session-id session-id
               :text "Checked inbox."
               :iteration 1
               :max-iterations 6
               :status :continue
               :progress-status :in-progress}]
             @assistant-messages))
      (finally
        (prompt/register-assistant-message! :terminal nil)))))

(deftest process-message-refreshes-working-memory-when-autonomy-query-changes-between-text-only-iterations
  (let [session-id (db/create-session! :terminal)
        wm-updates (atom [])
        wm-refreshes (atom [])
        llm-calls  (atom 0)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [message _session-id _channel _opts]
                                                       (swap! wm-updates conj message))
                  xia.working-memory/refresh-wm!     (fn [message _session-id _channel _opts]
                                                       (swap! wm-refreshes conj message))
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Checked inbox.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Checked inbox\",\"next_step\":\"Draft replies\",\"reason\":\"Unread messages remain\",\"goal_complete\":false,\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft replies\",\"status\":\"in_progress\"}]}")}
                                                         {"content" (str "Sent the replies.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"complete\",\"summary\":\"Sent replies\",\"next_step\":\"\",\"reason\":\"Goal satisfied\",\"goal_complete\":true,\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft replies\",\"status\":\"completed\"}]}")} ))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Sent the replies."
             (agent/process-message session-id
                                    "reply to the billing emails"
                                    :channel :terminal))))
    (is (= ["reply to the billing emails"] @wm-updates))
    (is (= 1 (count @wm-refreshes)))
    (is (str/includes? (first @wm-refreshes) "Draft replies"))))

(deftest process-message-refreshes-working-memory-after-non-mutating-tool-iterations-when-the-query-evolves
  (let [session-id    (db/create-session! :terminal)
        wm-updates    (atom [])
        wm-refreshes  (atom [])
        llm-calls     (atom 0)]
    (with-redefs [xia.working-memory/update-wm!      (fn [message _session-id _channel _opts]
                                                       (swap! wm-updates conj message))
                  xia.working-memory/refresh-wm!     (fn [message _session-id _channel _opts]
                                                       (swap! wm-refreshes conj message))
                  xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}])
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                       {:summary "Found the invoices."})
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Checking the inbox.\n\n"
                                                                           "ACTION_INTENT_JSON:{\"focus\":\"Reply to the billing emails\",\"agenda_item\":\"Check inbox\",\"plan_step\":\"Search unread billing email\",\"why\":\"Need the latest unread items\",\"tool\":\"web-search\",\"tool_args_summary\":\"label:billing unread\"}")
                                                            "tool_calls" [{"id" "call-1"
                                                                           "function" {"name" "web-search"
                                                                                       "arguments" "{}"}}]}
                                                         2 {"content" (str "Checked the inbox.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Checked the inbox\",\"next_step\":\"Draft the reply\",\"reason\":\"Need one more pass\",\"goal_complete\":false,\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft reply\",\"status\":\"in_progress\"}]}")}
                                                         {"content" (str "Sent the reply.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"complete\",\"summary\":\"Sent the reply\",\"next_step\":\"\",\"reason\":\"Done\",\"goal_complete\":true,\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft reply\",\"status\":\"completed\"}]}")} ))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Sent the reply."
             (agent/process-message session-id
                                    "reply to the billing emails"
                                    :channel :terminal))))
    (is (= ["reply to the billing emails"] @wm-updates))
    (is (= 1 (count @wm-refreshes)))
    (is (str/includes? (first @wm-refreshes) "Draft the reply"))))

(deftest process-message-refreshes-working-memory-after-retrieval-state-changes
  (let [session-id    (db/create-session! :terminal)
        wm-updates    (atom [])
        wm-refreshes  (atom [])
        llm-calls     (atom 0)]
    (with-redefs [xia.working-memory/update-wm!      (fn [message _session-id _channel _opts]
                                                       (swap! wm-updates conj message))
                  xia.working-memory/refresh-wm!     (fn [message _session-id _channel _opts]
                                                       (swap! wm-refreshes conj message))
                  xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}])
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                       (retrieval-state/bump-knowledge!)
                                                       {:summary "Found the invoices."})
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Checking the inbox.\n\n"
                                                                           "ACTION_INTENT_JSON:{\"focus\":\"Reply to the billing emails\",\"agenda_item\":\"Check inbox\",\"plan_step\":\"Search unread billing email\",\"why\":\"Need the latest unread items\",\"tool\":\"web-search\",\"tool_args_summary\":\"label:billing unread\"}")
                                                            "tool_calls" [{"id" "call-1"
                                                                           "function" {"name" "web-search"
                                                                                       "arguments" "{}"}}]}
                                                         2 {"content" (str "Checked the inbox.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Checked the inbox\",\"next_step\":\"Draft the reply\",\"reason\":\"Need one more pass\",\"goal_complete\":false,\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft reply\",\"status\":\"in_progress\"}]}")}
                                                         {"content" (str "Sent the reply.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"complete\",\"summary\":\"Sent the reply\",\"next_step\":\"\",\"reason\":\"Done\",\"goal_complete\":true,\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft reply\",\"status\":\"completed\"}]}")} ))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Sent the reply."
             (agent/process-message session-id
                                    "reply to the billing emails"
                                    :channel :terminal))))
    (is (= ["reply to the billing emails"] @wm-updates))
    (is (= 1 (count @wm-refreshes)))
    (is (str/includes? (first @wm-refreshes) "Draft the reply"))))

(deftest process-message-rebuilds-next-iteration-context-from-persisted-continue-messages
  (let [session-id      (db/create-session! :terminal)
        llm-calls       (atom 0)
        built-messages  (atom [])
        original-build  @#'xia.context/build-messages-data]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.working-memory/refresh-wm!     (fn [& _] nil)
                  xia.context/assemble-system-prompt-data
                  (fn [_session-id _opts]
                    {:prompt "system"
                     :used-fact-eids []})
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data
                  (fn [sid opts]
                    (let [result (original-build sid opts)]
                      (swap! built-messages conj (:messages result))
                      result))
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Checked inbox.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Checked inbox\",\"next_step\":\"Draft replies\",\"reason\":\"Unread messages remain\",\"goal_complete\":false,\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft replies\",\"status\":\"in_progress\"}]}")}
                                                         {"content" (str "Sent the replies.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"complete\",\"summary\":\"Sent replies\",\"next_step\":\"\",\"reason\":\"Goal satisfied\",\"goal_complete\":true,\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft replies\",\"status\":\"completed\"}]}")} ))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Sent the replies."
             (agent/process-message session-id
                                    "reply to the billing emails"
                                    :channel :terminal))))
    (let [second-build (second @built-messages)]
      (is (= 2 (count @built-messages)))
      (is (some #(and (= "assistant" (:role %))
                      (= "Checked inbox." (:content %)))
                second-build)))))

(deftest process-message-reuses-system-prompt-cache-entry-across-autonomous-iterations
  (let [session-id    (db/create-session! :terminal)
        build-opts    (atom [])
        llm-calls     (atom 0)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.working-memory/refresh-wm!     (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id opts]
                                                       (swap! build-opts conj (select-keys opts [:system-prompt-cache-entry]))
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []
                                                        :system-prompt-cache-entry {:key (count @build-opts)
                                                                                   :data {:prompt "cached"
                                                                                          :used-fact-eids []}}})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Checked inbox.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Checked inbox\",\"next_step\":\"Draft replies\",\"reason\":\"Unread messages remain\",\"goal_complete\":false,\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft replies\",\"status\":\"in_progress\"}]}")}
                                                         {"content" (str "Sent the replies.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"complete\",\"summary\":\"Sent replies\",\"next_step\":\"\",\"reason\":\"Goal satisfied\",\"goal_complete\":true,\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft replies\",\"status\":\"completed\"}]}")} ))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Sent the replies."
             (agent/process-message session-id
                                    "reply to the billing emails"
                                    :channel :terminal))))
    (is (nil? (get-in @build-opts [0 :system-prompt-cache-entry])))
    (is (= {:key 1
            :data {:prompt "cached"
                   :used-fact-eids []}}
           (get-in @build-opts [1 :system-prompt-cache-entry])))))

(deftest process-message-preserves-autonomy-state-after-completed-goal
  (let [session-id (db/create-session! :terminal)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       {"content" (str "Sent the replies.\n\n"
                                                                       "AUTONOMOUS_STATUS_JSON:"
                                                                       "{\"status\":\"complete\",\"summary\":\"Sent replies\",\"next_step\":\"\",\"reason\":\"Goal satisfied\",\"goal_complete\":true,\"stack_action\":\"stay\",\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Send replies\",\"status\":\"completed\"}]}")})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Sent the replies."
             (agent/process-message session-id
                                    "reply to the billing emails"
                                    :channel :terminal))))
    (is (= {:stack [{:title "reply to the billing emails"
                     :summary "Sent replies"
                     :reason "Goal satisfied"
                     :progress-status :complete
                     :agenda [{:item "Send replies" :status :completed}]}]}
           (wm/autonomy-state session-id)))
    (wm/clear-wm! session-id)
    (wm/ensure-wm! session-id)
    (is (= {:stack [{:title "reply to the billing emails"
                     :summary "Sent replies"
                     :reason "Goal satisfied"
                     :progress-status :complete
                     :agenda [{:item "Send replies" :status :completed}]}]}
           (wm/autonomy-state session-id)))))

(deftest process-message-does-not-trust-goal-complete-when-agenda-remains
  (let [session-id (db/create-session! :terminal)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       {"content" (str "Almost done.\n\n"
                                                                       "AUTONOMOUS_STATUS_JSON:"
                                                                       "{\"status\":\"complete\",\"summary\":\"Drafted the reply\",\"next_step\":\"Send the reply\",\"reason\":\"One step remains\",\"goal_complete\":true,\"stack_action\":\"stay\",\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Draft the reply\",\"status\":\"completed\"},{\"item\":\"Send the reply\",\"status\":\"pending\"}]}")})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Almost done."
             (agent/process-message session-id
                                    "reply to the billing emails"
                                    :channel :terminal))))
    (is (= {:stack [{:title "reply to the billing emails"
                     :summary "Drafted the reply"
                     :next-step "Send the reply"
                     :reason "One step remains"
                     :progress-status :in-progress
                     :agenda [{:item "Draft the reply" :status :completed}
                              {:item "Send the reply" :status :pending}]}]}
           (wm/autonomy-state session-id)))))

(deftest process-message-restores-completed-stack-across-top-level-follow-up-turns
  (let [session-id   (db/create-session! :terminal)
        llm-messages (atom [])
        llm-calls    (atom 0)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [messages & _opts]
                                                       (swap! llm-messages conj messages)
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Sent the replies.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"complete\",\"summary\":\"Sent replies\",\"next_step\":\"\",\"reason\":\"Goal satisfied\",\"goal_complete\":true,\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Send replies\",\"status\":\"completed\"}]}")}
                                                         {"content" (str "Added the tracking link.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"complete\",\"summary\":\"Added the tracking link\",\"next_step\":\"\",\"reason\":\"The follow-up is done\",\"goal_complete\":true,\"stack_action\":\"replace\",\"current_focus\":\"Send the tracking link\",\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Send the tracking link\",\"status\":\"completed\"}]}")} ))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Sent the replies."
             (agent/process-message session-id
                                    "reply to the billing emails"
                                    :channel :terminal)))
      (wm/clear-wm! session-id)
      (is (= "Added the tracking link."
             (agent/process-message session-id
                                    "also send the tracking link"
                                    :channel :terminal))))
    (is (= 2 (count @llm-messages)))
    (is (str/includes? (get-in @llm-messages [1 0 :content])
                       "Current execution stack"))
    (is (str/includes? (get-in @llm-messages [1 0 :content])
                       "[complete] reply to the billing emails"))
    (is (str/includes? (get-in @llm-messages [1 0 :content])
                       "Latest turn input:\nalso send the tracking link"))))

(deftest process-message-does-not-snapshot-before-first-iteration
  (let [session-id      (db/create-session! :terminal)
        snapshot-calls  (atom 0)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       {"content" (str "Sent the replies.\n\n"
                                                                       "AUTONOMOUS_STATUS_JSON:"
                                                                       "{\"status\":\"complete\",\"summary\":\"Sent replies\",\"next_step\":\"\",\"reason\":\"Goal satisfied\",\"goal_complete\":true,\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Send replies\",\"status\":\"completed\"}]}")})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)
                  xia.working-memory/snapshot!       (fn [_session-id]
                                                       (swap! snapshot-calls inc))]
      (is (= "Sent the replies."
             (agent/process-message session-id
                                    "reply to the billing emails"
                                    :channel :terminal))))
    (is (= 1 @snapshot-calls)
        "one snapshot after the iteration update")))

(deftest process-message-preserves-autonomy-state-when-control-envelope-is-missing
  (let [session-id (db/create-session! :terminal)]
    (wm/ensure-wm! session-id)
    (wm/set-autonomy-state! session-id
                            {:stack [{:title "Reply to the billing emails"
                                      :summary "Need invoice ids from the user"
                                      :next-step "Wait for invoice ids"
                                      :reason "Blocked on user input"
                                      :progress-status :resumable
                                      :agenda [{:item "Wait for invoice ids"
                                                :status :resumable}]}]})
    (wm/snapshot! session-id)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       {"content" "Plain reply without a control envelope."})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Plain reply without a control envelope."
             (agent/process-message session-id
                                    "thanks"
                                    :channel :terminal))))
    (is (= {:stack [{:title "Reply to the billing emails"
                     :summary "Need invoice ids from the user"
                     :next-step "Wait for invoice ids"
                     :reason "Blocked on user input"
                     :progress-status :resumable
                     :agenda [{:item "Wait for invoice ids"
                               :status :resumable}]}]}
           (wm/autonomy-state session-id)))
    (wm/clear-wm! session-id)
    (wm/ensure-wm! session-id)
    (is (= {:stack [{:title "Reply to the billing emails"
                     :summary "Need invoice ids from the user"
                     :next-step "Wait for invoice ids"
                     :reason "Blocked on user input"
                     :progress-status :resumable
                     :agenda [{:item "Wait for invoice ids"
                               :status :resumable}]}]}
           (wm/autonomy-state session-id)))))

(deftest process-message-preserves-autonomy-state-and-strips-malformed-control-envelope
  (let [session-id (db/create-session! :terminal)]
    (wm/ensure-wm! session-id)
    (wm/set-autonomy-state! session-id
                            {:stack [{:title "Reply to the billing emails"
                                      :summary "Drafted the reply"
                                      :next-step "Send it"
                                      :reason "One step remains"
                                      :progress-status :in-progress
                                      :agenda [{:item "Send it"
                                                :status :in-progress}]}]})
    (wm/snapshot! session-id)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       {"content" (str "Worked the plan.\n\n"
                                                                       "AUTONOMOUS_STATUS_JSON:{\"status\":\"continue\"")})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Worked the plan."
             (agent/process-message session-id
                                    "continue"
                                    :channel :terminal))))
    (is (= {:stack [{:title "Reply to the billing emails"
                     :summary "Drafted the reply"
                     :next-step "Send it"
                     :reason "One step remains"
                     :progress-status :in-progress
                     :agenda [{:item "Send it"
                               :status :in-progress}]}]}
           (wm/autonomy-state session-id)))))

(deftest process-message-restores-resumable-stack-across-top-level-turns
  (let [session-id   (db/create-session! :terminal)
        llm-messages (atom [])
        llm-calls    (atom 0)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [messages & _opts]
                                                       (swap! llm-messages conj messages)
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Paused for invoice ids.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"complete\",\"summary\":\"Need invoice ids from the user\",\"next_step\":\"Wait for invoice ids\",\"reason\":\"Need user input before continuing\",\"goal_complete\":false,\"current_focus\":\"Find invoice ids\",\"stack_action\":\"push\",\"progress_status\":\"resumable\",\"agenda\":[{\"item\":\"Look up invoice ids\",\"status\":\"completed\"},{\"item\":\"Wait for user invoice ids\",\"status\":\"resumable\"}]}")}
                                                         {"content" (str "Resumed with invoice ids.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"complete\",\"summary\":\"Resumed with invoice ids\",\"next_step\":\"\",\"reason\":\"Continuing the same task\",\"goal_complete\":false,\"current_focus\":\"Find invoice ids\",\"stack_action\":\"stay\",\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Use supplied invoice ids\",\"status\":\"completed\"},{\"item\":\"Draft reply\",\"status\":\"in_progress\"}]}")} ))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Paused for invoice ids."
             (agent/process-message session-id
                                    "reply to the billing emails"
                                    :channel :terminal)))
      (wm/clear-wm! session-id)
    (is (= "Resumed with invoice ids."
             (agent/process-message session-id
                                    "Here are the invoice ids: 12 and 13"
                                    :channel :terminal))))
    (is (= 2 (count @llm-messages)))
    (is (str/includes? (get-in @llm-messages [1 0 :content])
                       "Current execution stack"))
    (is (str/includes? (get-in @llm-messages [1 0 :content])
                       "[resumable] Find invoice ids"))
    (is (str/includes? (get-in @llm-messages [1 0 :content])
                       "New input for this turn:"))
    (is (str/includes? (get-in @llm-messages [1 0 :content])
                       "Here are the invoice ids: 12 and 13"))))

(deftest process-message-surfaces-turn-input-in-goal-block-and-commits-pivoted-goal
  (let [session-id   (db/create-session! :terminal)
        llm-messages (atom [])]
    (wm/ensure-wm! session-id)
    (wm/set-autonomy-state! session-id
                            {:stack [{:title "Reply to the billing emails"
                                      :summary "Draft the reply"
                                      :next-step "Send the reply"
                                      :reason "Still in progress"
                                      :progress-status :in-progress
                                      :agenda [{:item "Send the reply"
                                                :status :in-progress}]}]})
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [messages & _opts]
                                                       (swap! llm-messages conj messages)
                                                       {"content" (str "Switched to the refund task.\n\n"
                                                                       "AUTONOMOUS_STATUS_JSON:"
                                                                       "{\"status\":\"complete\",\"summary\":\"Switched to the refund task\",\"next_step\":\"Review the refund thread\",\"reason\":\"The user changed the task\",\"goal_complete\":false,\"current_focus\":\"Handle the refund follow-up\",\"stack_action\":\"replace\",\"progress_status\":\"pending\",\"agenda\":[{\"item\":\"Review the refund thread\",\"status\":\"pending\"}]}")})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
    (is (= "Switched to the refund task."
             (agent/process-message session-id
                                    "Stop the billing work and handle the refund follow-up instead."
                                    :channel :terminal))))
    (is (str/includes? (get-in @llm-messages [0 0 :content])
                       "Current root goal:\nReply to the billing emails"))
    (is (str/includes? (get-in @llm-messages [0 0 :content])
                       "Latest turn input:\nStop the billing work and handle the refund follow-up instead."))
    (is (= "Handle the refund follow-up"
           (autonomous/root-goal (wm/autonomy-state session-id))))
    (is (= "Handle the refund follow-up"
           (some-> (wm/autonomy-state session-id)
                   autonomous/current-frame
                   :title)))))

(deftest process-message-returns-final-message-when-autonomous-loop-hits-iteration-cap
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)
        reviewed   (atom nil)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.autonomous/max-iterations      (constantly 2)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (swap! llm-calls inc)
                                                       {"content" (str "Still working.\n\n"
                                                                       "AUTONOMOUS_STATUS_JSON:"
                                                                       "{\"status\":\"continue\",\"summary\":\"Still working\",\"next_step\":\"Keep going\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Retry task\",\"status\":\"blocked\"}]}")})
                  xia.agent/schedule-fact-utility-review!
                  (fn [review-session-id _fact-eids _user-message assistant-response]
                    (reset! reviewed {:session-id review-session-id
                                      :assistant-response assistant-response}))]
      (let [result (agent/process-message session-id "keep going" :channel :terminal)]
        (is (str/includes? result "Still working."))
        (is (str/includes? result "Note: I stopped after reaching the autonomous iteration limit for this turn (2)."))
        (is (str/includes? result "Suggested next step: Keep going"))))
    (is (= 2 @llm-calls))
    (is (= [[:user "keep going"]
            [:assistant "Still working."]
            [:assistant (str "Still working.\n\n"
                             "Note: I stopped after reaching the autonomous iteration limit for this turn (2). Suggested next step: Keep going Reply to continue from the current agenda.")]]
           (->> (db/session-messages session-id)
                (filter #(contains? #{:user :assistant} (:role %)))
                (mapv (fn [{:keys [role content]}]
                        [role content])))))
    (is (= {:session-id session-id
            :assistant-response "Still working."}
           @reviewed))
    (is (= :blocked (some-> (wm/autonomy-state session-id)
                            autonomous/current-frame
                            :progress-status)))))

(deftest process-message-stops-when-turn-llm-call-budget-is-reached
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/max-turn-llm-calls 2)
    (db/set-config! :agent/max-turn-total-tokens 100000)
    (db/set-config! :agent/max-turn-wall-clock-ms 300000)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.autonomous/max-iterations      (constantly 6)
                  xia.llm/chat                       (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"choices" [{"message" {"content" (str "First step done.\n\n"
                                                                                                   "AUTONOMOUS_STATUS_JSON:"
                                                                                                   "{\"status\":\"continue\",\"summary\":\"First step done\",\"next_step\":\"Take the second step\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Take the second step\",\"status\":\"in_progress\"}]}")}}]
                                                            "usage" {"prompt_tokens" 3
                                                                     "completion_tokens" 4
                                                                     "total_tokens" 7}}
                                                         2 {"choices" [{"message" {"content" (str "Second step done.\n\n"
                                                                                                   "AUTONOMOUS_STATUS_JSON:"
                                                                                                   "{\"status\":\"continue\",\"summary\":\"Second step done\",\"next_step\":\"Take the third step\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Take the third step\",\"status\":\"pending\"}]}")}}]
                                                            "usage" {"prompt_tokens" 3
                                                                     "completion_tokens" 4
                                                                     "total_tokens" 7}}
                                                         (throw (ex-info "unexpected third LLM call" {}))))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [result (agent/process-message session-id "keep going" :channel :terminal)]
        (is (str/includes? result "Second step done."))
        (is (str/includes? result "Note: I stopped this turn after reaching the cumulative LLM call budget (2/2)."))
        (is (str/includes? result "Suggested next step: Take the third step"))))
    (is (= 2 @llm-calls))
    (is (= [[:user "keep going"]
            [:assistant "First step done."]
            [:assistant (str "Second step done.\n\n"
                             "Note: I stopped this turn after reaching the cumulative LLM call budget (2/2). Suggested next step: Take the third step Reply to continue from the current agenda.")]]
           (->> (db/session-messages session-id)
                (filter #(contains? #{:user :assistant} (:role %)))
                (mapv (fn [{:keys [role content]}]
                        [role content])))))))

(deftest process-message-stops-when-turn-token-budget-is-reached
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/max-turn-llm-calls 10)
    (db/set-config! :agent/max-turn-total-tokens 10)
    (db/set-config! :agent/max-turn-wall-clock-ms 300000)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.autonomous/max-iterations      (constantly 6)
                  xia.llm/chat                       (fn [_messages & _opts]
                                                       (swap! llm-calls inc)
                                                       {"choices" [{"message" {"content" (str "Spent the budget.\n\n"
                                                                                              "AUTONOMOUS_STATUS_JSON:"
                                                                                              "{\"status\":\"continue\",\"summary\":\"Spent the budget\",\"next_step\":\"Resume once more tokens are available\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Resume once more tokens are available\",\"status\":\"blocked\"}]}")}}]
                                                        "usage" {"prompt_tokens" 4
                                                                 "completion_tokens" 6
                                                                 "total_tokens" 10}})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [result (agent/process-message session-id "keep going" :channel :terminal)]
        (is (str/includes? result "Spent the budget."))
        (is (str/includes? result "Note: I stopped this turn after reaching the cumulative token budget (10/10)."))))
    (is (= 1 @llm-calls))))

(deftest process-message-counts-context-llm-calls-against-the-turn-budget
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/max-turn-llm-calls 1)
    (db/set-config! :agent/max-turn-total-tokens 100000)
    (db/set-config! :agent/max-turn-wall-clock-ms 300000)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       (llm/chat-simple [{"role" "user"
                                                                          "content" "summarize the earlier history"}])
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.autonomous/max-iterations      (constantly 6)
                  xia.llm/chat                       (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"choices" [{"message" {"content" "history recap"}}]
                                                            "usage" {"prompt_tokens" 2
                                                                     "completion_tokens" 3
                                                                     "total_tokens" 5}}
                                                         (throw (ex-info "unexpected assistant LLM call" {}))))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [result (agent/process-message session-id "keep going" :channel :terminal)]
        (is (str/includes? result "Note: I stopped this turn after reaching the cumulative LLM call budget (1/1)."))))
    (is (= 1 @llm-calls))
    (is (= [[:user "keep going"]
            [:assistant "Note: I stopped this turn after reaching the cumulative LLM call budget (1/1). Reply to continue from the current agenda."]]
           (->> (db/session-messages session-id)
                (filter #(contains? #{:user :assistant} (:role %)))
                (mapv (fn [{:keys [role content]}]
                        [role content])))))))

(deftest process-message-supervisor-stops-a-stalled-llm-worker
  (let [session-id (db/create-session! :terminal)
        statuses   (atom [])
        stopped    (promise)]
    (db/set-config! :agent/supervisor-tick-ms 10)
    (db/set-config! :agent/supervisor-max-restarts 0)
    (db/set-config! :agent/supervisor-llm-timeout-ms 50)
    (prompt/register-status! :terminal
                             (fn [status]
                               (swap! statuses conj (select-keys status
                                                                 [:state :phase :message]))))
    (try
      (with-redefs [xia.tool/tool-definitions          (constantly [])
                    xia.working-memory/update-wm!      (fn [& _] nil)
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.context/build-messages-data    (fn [_session-id _opts]
                                                         {:messages [{:role "system" :content "test"}]
                                                          :used-fact-eids []})
                    xia.llm/chat-message               (fn [_messages & _opts]
                                                         (try
                                                           (Thread/sleep 10000)
                                                           {"content" "done"}
                                                           (catch InterruptedException e
                                                             (.interrupt (Thread/currentThread))
                                                             (throw e))
                                                           (finally
                                                             (deliver stopped true))))
                    xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
        (let [err (try
                    (agent/process-message session-id "do the thing" :channel :terminal)
                    (catch clojure.lang.ExceptionInfo e
                      e))]
          (is (instance? clojure.lang.ExceptionInfo err))
          (is (= {:type :agent-stalled
                  :session-id session-id
                  :channel :terminal
                  :phase :llm
                  :timeout-ms 50}
                 (select-keys (ex-data err)
                              [:type :session-id :channel :phase :timeout-ms])))))
      (is (= true (deref stopped 1000 ::timeout)))
      (is (= {:state :error
              :phase :stalled
              :message "Supervisor stopped the run: Agent supervisor stopped a stalled worker during llm phase"}
             (last @statuses)))
      (is (= [[:user "do the thing"]]
             (->> (db/session-messages session-id)
                  (mapv (fn [{:keys [role content]}]
                          [role content])))))
      (finally
        (prompt/register-status! :terminal nil)))))

(deftest process-message-supervisor-restarts-a-stalled-llm-worker
  (let [session-id (db/create-session! :terminal)
        statuses   (atom [])
        llm-calls  (atom 0)
        stopped    (promise)]
    (db/set-config! :agent/supervisor-tick-ms 10)
    (db/set-config! :agent/supervisor-max-restarts 1)
    (db/set-config! :agent/supervisor-restart-backoff-ms 1)
    (db/set-config! :agent/supervisor-llm-timeout-ms 50)
    (prompt/register-status! :terminal
                             (fn [status]
                               (swap! statuses conj (select-keys status
                                                                 [:state :phase :message]))))
    (try
      (with-redefs [xia.tool/tool-definitions          (constantly [])
                    xia.working-memory/update-wm!      (fn [& _] nil)
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.context/build-messages-data    (fn [_session-id _opts]
                                                         {:messages [{:role "system" :content "test"}]
                                                          :used-fact-eids []})
                    xia.llm/chat-message               (fn [_messages & _opts]
                                                         (case (swap! llm-calls inc)
                                                           1 (try
                                                               (Thread/sleep 10000)
                                                               {"content" "unexpected"}
                                                               (catch InterruptedException e
                                                                 (.interrupt (Thread/currentThread))
                                                                 (throw e))
                                                               (finally
                                                                 (deliver stopped true)))
                                                           {"content" "Recovered reply."}))
                    xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
        (is (= "Recovered reply."
               (agent/process-message session-id "do the thing" :channel :terminal))))
      (is (= true (deref stopped 1000 ::timeout)))
      (is (= 2 @llm-calls))
      (is (some #(= {:state :running
                     :phase :restarting
                     :message "Restarting iteration after Agent supervisor stopped a stalled worker during llm phase (attempt 1/1)"}
                    %)
                @statuses))
      (is (= {:state :done
              :phase :complete
              :message "Ready"}
             (last @statuses)))
      (is (= [[:user "do the thing"]
              [:assistant "Recovered reply."]]
             (->> (db/session-messages session-id)
                  (filter #(contains? #{:user :assistant} (:role %)))
                  (mapv (fn [{:keys [role content]}]
                          [role content])))))
      (finally
        (prompt/register-status! :terminal nil)))))

(deftest process-message-does-not-restart-after-tool-execution-begins
  (let [session-id (db/create-session! :terminal)
        statuses   (atom [])
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-restarts 1)
    (db/set-config! :agent/supervisor-restart-backoff-ms 1)
    (prompt/register-status! :terminal
                             (fn [status]
                               (swap! statuses conj (select-keys status
                                                                 [:state :phase :message]))))
    (try
      (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                    xia.tool/tool-definitions          (constantly [{:type "function"
                                                                     :function {:name "web-search"
                                                                                :parameters {}}}])
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.context/build-messages-data    (fn [_session-id _opts]
                                                         {:messages [{:role "system" :content "test"}]
                                                          :used-fact-eids []})
                    xia.tool/parallel-safe?            (constantly false)
                    xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                         {:summary "Found the page."})
                    xia.llm/chat-message               (fn [_messages & _opts]
                                                         (case (swap! llm-calls inc)
                                                           1 {"content" (str "ACTION_INTENT_JSON:"
                                                                             "{\"focus\":\"Research this\",\"agenda_item\":\"Research this\",\"plan_step\":\"Search the page\",\"why\":\"Need the first search result before continuing\",\"tool\":\"web-search\",\"tool_args_summary\":\"{}\"}")
                                                              "tool_calls" [{"id" "call-1"
                                                                             "function" {"name" "web-search"
                                                                                         "arguments" "{}"}}]}
                                                           2 (throw (RuntimeException. "crashed after tool"))
                                                           {"content" "unexpected restart"}))
                    xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
        (let [err (try
                    (agent/process-message session-id
                                           "research this"
                                           :channel :terminal)
                    (catch Exception e
                      e))]
          (is (instance? RuntimeException err))
          (is (= "crashed after tool" (.getMessage ^RuntimeException err)))))
      (is (= 2 @llm-calls))
      (is (not-any? #(= :restarting (:phase %)) @statuses))
      (finally
        (prompt/register-status! :terminal nil)))))

(deftest process-message-supervisor-restarts-after-transient-worker-crash
  (let [session-id (db/create-session! :terminal)
        statuses   (atom [])
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-restarts 1)
    (db/set-config! :agent/supervisor-restart-backoff-ms 1)
    (prompt/register-status! :terminal
                             (fn [status]
                               (swap! statuses conj (select-keys status
                                                                 [:state :phase :message]))))
    (try
      (with-redefs [xia.tool/tool-definitions          (constantly [])
                    xia.working-memory/update-wm!      (fn [& _] nil)
                    xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                    :provider-id :default})
                    xia.context/build-messages-data    (fn [_session-id _opts]
                                                         {:messages [{:role "system" :content "test"}]
                                                          :used-fact-eids []})
                    xia.llm/chat-message               (fn [_messages & _opts]
                                                         (case (swap! llm-calls inc)
                                                           1 (throw (RuntimeException. "transient model failure"))
                                                           {"content" "Recovered after crash."}))
                    xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
        (is (= "Recovered after crash."
               (agent/process-message session-id "do the thing" :channel :terminal))))
      (is (= 2 @llm-calls))
      (is (some #(= {:state :running
                     :phase :restarting
                     :message "Restarting iteration after transient model failure (attempt 1/1)"}
                    %)
                @statuses))
      (is (= {:state :done
              :phase :complete
              :message "Ready"}
             (last @statuses)))
      (finally
        (prompt/register-status! :terminal nil)))))

(deftest process-message-supervisor-detects-identical-autonomous-iterations
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-identical-iterations 2)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.autonomous/max-iterations      (constantly 6)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (swap! llm-calls inc)
                                                       {"content" (str "Still working.\n\n"
                                                                       "AUTONOMOUS_STATUS_JSON:"
                                                                       "{\"status\":\"continue\",\"summary\":\"Still working\",\"next_step\":\"Keep going\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Retry task\",\"status\":\"blocked\"}]}")})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [err (try
                  (agent/process-message session-id "keep going" :channel :terminal)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (re-find #"Autonomous loop made no progress after 2 identical iterations"
                     (.getMessage ^Exception err)))
        (is (= {:type :autonomous-loop-stalled
                :session-id session-id
                :channel :terminal
                :iteration 2
                :max-iterations 6
                :progress-status :blocked
                :agenda [{:item "Retry task" :status :blocked}]}
               (select-keys (ex-data err)
                            [:type :session-id :channel :iteration :max-iterations
                             :progress-status :agenda])))))
    (is (= 2 @llm-calls))
    (is (= [[:user "keep going"]
            [:assistant "Still working."]
            [:assistant "Still working."]]
           (->> (db/session-messages session-id)
                (mapv (fn [{:keys [role content]}]
                        [role content])))))))

(deftest process-message-treats-summary-rephrasing-alone-as-an-identical-loop
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-identical-iterations 2)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.autonomous/max-iterations      (constantly 6)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Step one finished.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Step one finished\",\"next_step\":\"Keep going\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Retry task\",\"status\":\"blocked\"}]}")}
                                                         2 {"content" (str "Step two finished.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Step two finished\",\"next_step\":\"Keep going\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Retry task\",\"status\":\"blocked\"}]}")}
                                                         {"content" (str "Step three finished.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"continue\",\"summary\":\"Step three finished\",\"next_step\":\"Keep going\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Retry task\",\"status\":\"blocked\"}]}")}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [err (try
                  (agent/process-message session-id "keep going" :channel :terminal)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (= :autonomous-loop-stalled
               (:type (ex-data err))))))
    (is (= 2 @llm-calls))
    (is (= [[:user "keep going"]
            [:assistant "Step one finished."]
            [:assistant "Step two finished."]]
           (->> (db/session-messages session-id)
                (mapv (fn [{:keys [role content]}]
                        [role content])))))))

(deftest process-message-detects-stalled-loops-even-when-tool-choices-vary
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-identical-iterations 2)
    (with-redefs [xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}
                                                                  {:type "function"
                                                                   :function {:name "workspace-read"
                                                                              :parameters {}}}
                                                                  {:type "function"
                                                                   :function {:name "email-read"
                                                                              :parameters {}}}])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [tool-id _args _context]
                                                       (case tool-id
                                                         :web-search {:summary "Searched the web."}
                                                         :workspace-read {:summary "Read the workspace note."}
                                                         :email-read {:summary "Read the latest email."}
                                                         {:summary "done"}))
                  xia.autonomous/max-iterations      (constantly 3)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "ACTION_INTENT_JSON:"
                                                                           "{\"focus\":\"Retry task\",\"agenda_item\":\"Retry task\",\"plan_step\":\"Search billing invoices\",\"why\":\"Need more context before retrying the task\",\"tool\":\"web-search\",\"tool_args_summary\":\"q=billing invoices\"}")
                                                            "tool_calls" [{"id" "call-1"
                                                                           "function" {"name" "web-search"
                                                                                       "arguments" "{\"q\":\"billing invoices\"}"}}]}
                                                         2 {"content" (str "Still working.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Still working\",\"next_step\":\"Keep going\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Retry task\",\"status\":\"blocked\"}]}")}
                                                         3 {"content" (str "ACTION_INTENT_JSON:"
                                                                           "{\"focus\":\"Retry task\",\"agenda_item\":\"Retry task\",\"plan_step\":\"Read the billing note\",\"why\":\"Need more context before retrying the task\",\"tool\":\"workspace-read\",\"tool_args_summary\":\"path=notes/billing.md\"}")
                                                            "tool_calls" [{"id" "call-2"
                                                                           "function" {"name" "workspace-read"
                                                                                       "arguments" "{\"path\":\"notes/billing.md\"}"}}]}
                                                         4 {"content" (str "Still working.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Still working\",\"next_step\":\"Keep going\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Retry task\",\"status\":\"blocked\"}]}")}
                                                         5 {"content" ""
                                                            "tool_calls" [{"id" "call-3"
                                                                           "function" {"name" "email-read"
                                                                                       "arguments" "{\"message_id\":\"latest\"}"}}]}
                                                         {"content" (str "Still working.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"continue\",\"summary\":\"Still working\",\"next_step\":\"Keep going\",\"reason\":\"More work remains\",\"goal_complete\":false,\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Retry task\",\"status\":\"blocked\"}]}")}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [err (try
                  (agent/process-message session-id "keep going" :channel :terminal)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (= :autonomous-loop-stalled
               (:type (ex-data err)))))
      (is (= 4 @llm-calls)))))

(deftest process-message-does-not-stall-on-repeated-tool-failures-when-focus-shifts
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-identical-iterations 2)
    (with-redefs [xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}
                                                                  {:type "function"
                                                                   :function {:name "workspace-read"
                                                                              :parameters {}}}])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [tool-id _args _context]
                                                       (case tool-id
                                                         :web-search {:error "Search backend timed out"}
                                                         :workspace-read {:error "Workspace file could not be opened"}
                                                         {:error "Tool failed"}))
                  xia.autonomous/max-iterations      (constantly 2)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "ACTION_INTENT_JSON:"
                                                                           "{\"focus\":\"Search the invoices\",\"agenda_item\":\"Search the invoices\",\"plan_step\":\"Search the invoices\",\"why\":\"Need the billing details\",\"tool\":\"web-search\",\"tool_args_summary\":\"q=billing invoices\"}")
                                                            "tool_calls" [{"id" "call-1"
                                                                           "function" {"name" "web-search"
                                                                                       "arguments" "{\"q\":\"billing invoices\"}"}}]}
                                                         2 {"content" (str "The search failed.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"The search failed\",\"next_step\":\"Search the invoices again\",\"reason\":\"Still need the billing details\",\"goal_complete\":false,\"current_focus\":\"Search the invoices\",\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Search the invoices\",\"status\":\"blocked\"}]}")}
                                                         3 {"content" (str "ACTION_INTENT_JSON:"
                                                                           "{\"focus\":\"Review the billing notes\",\"agenda_item\":\"Review the billing notes\",\"plan_step\":\"Review the billing notes\",\"why\":\"Still need the billing details\",\"tool\":\"workspace-read\",\"tool_args_summary\":\"path=notes/billing.md\"}")
                                                            "tool_calls" [{"id" "call-2"
                                                                           "function" {"name" "workspace-read"
                                                                                       "arguments" "{\"path\":\"notes/billing.md\"}"}}]}
                                                         {"content" (str "The notes lookup also failed.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"continue\",\"summary\":\"The notes lookup also failed\",\"next_step\":\"Review the billing notes\",\"reason\":\"Still need the billing details\",\"goal_complete\":false,\"current_focus\":\"Review the billing notes\",\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Review the billing notes\",\"status\":\"blocked\"}]}")}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (str/includes? (agent/process-message session-id
                                                "handle the billing issue"
                                                :channel :terminal)
                         "iteration limit")))
    (is (= 4 @llm-calls))))

(deftest process-message-detects-repeated-tool-failure-loops-when-focus-stays-the-same
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-identical-iterations 2)
    (with-redefs [xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                       {:error "Search backend timed out"})
                  xia.autonomous/max-iterations      (constantly 6)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "ACTION_INTENT_JSON:"
                                                                           "{\"focus\":\"Search the invoices\",\"agenda_item\":\"Search the invoices\",\"plan_step\":\"Search the invoices\",\"why\":\"Need the billing details\",\"tool\":\"web-search\",\"tool_args_summary\":\"q=billing invoices\"}")
                                                            "tool_calls" [{"id" "call-1"
                                                                           "function" {"name" "web-search"
                                                                                       "arguments" "{\"q\":\"billing invoices\"}"}}]}
                                                         2 {"content" (str "The search failed.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"The search failed\",\"next_step\":\"Search the invoices again\",\"reason\":\"Still need the billing details\",\"goal_complete\":false,\"current_focus\":\"Search the invoices\",\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Search the invoices\",\"status\":\"blocked\"}]}")}
                                                         3 {"content" (str "ACTION_INTENT_JSON:"
                                                                           "{\"focus\":\"Search the invoices\",\"agenda_item\":\"Search the invoices\",\"plan_step\":\"Search the invoices again\",\"why\":\"Still need the billing details\",\"tool\":\"web-search\",\"tool_args_summary\":\"q=billing invoices retry\"}")
                                                            "tool_calls" [{"id" "call-2"
                                                                           "function" {"name" "web-search"
                                                                                       "arguments" "{\"q\":\"billing invoices retry\"}"}}]}
                                                         {"content" (str "The retry failed too.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"continue\",\"summary\":\"The retry failed too\",\"next_step\":\"Search the invoices again\",\"reason\":\"Still need the billing details\",\"goal_complete\":false,\"current_focus\":\"Search the invoices\",\"progress_status\":\"blocked\",\"agenda\":[{\"item\":\"Search the invoices\",\"status\":\"blocked\"}]}")}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [err (try
                  (agent/process-message session-id "handle the billing issue" :channel :terminal)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (= :autonomous-loop-stalled
               (:type (ex-data err))))
        (is (= :tool-failure
               (:semantic-match-source (ex-data err))))))
    (is (= 4 @llm-calls))))

(deftest process-message-treats-semantic-focus-rephrasing-as-the-same-stall
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-identical-iterations 2)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.autonomous/max-iterations      (constantly 6)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Reading the file.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Reading the file\",\"next_step\":\"Read config file\",\"reason\":\"Need more details\",\"goal_complete\":false,\"current_focus\":\"Read config file\",\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Read config file\",\"status\":\"in_progress\"}]}")}
                                                         {"content" (str "Opening the file.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"continue\",\"summary\":\"Opening the file\",\"next_step\":\"Open config file\",\"reason\":\"Need more details\",\"goal_complete\":false,\"current_focus\":\"Open config file\",\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Open config file\",\"status\":\"in_progress\"}]}")}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [err (try
                  (agent/process-message session-id "inspect config file" :channel :terminal)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (= :autonomous-loop-stalled
               (:type (ex-data err)))))
      (is (= 2 @llm-calls)))))

(deftest process-message-uses-embeddings-to-detect-semantic-looping
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-identical-iterations 2)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.db/current-embedding-provider  (constantly (semantic-loop-test-provider))
                  xia.autonomous/max-iterations      (constantly 6)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Inspecting the config file.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Inspecting the config file\",\"next_step\":\"Inspect config file\",\"reason\":\"Need more details\",\"goal_complete\":false,\"current_focus\":\"Inspect config file\",\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Inspect config file\",\"status\":\"in_progress\"}]}")}
                                                         {"content" (str "Reviewing the configuration document.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"continue\",\"summary\":\"Reviewing the configuration document\",\"next_step\":\"Review configuration document\",\"reason\":\"Need more details\",\"goal_complete\":false,\"current_focus\":\"Review configuration document\",\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Review configuration document\",\"status\":\"in_progress\"}]}")}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [err (try
                  (agent/process-message session-id "inspect config file" :channel :terminal)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (= :autonomous-loop-stalled
               (:type (ex-data err))))
        (is (= :embedding
               (:semantic-match-source (ex-data err))))))
    (is (= 2 @llm-calls))))

(deftest process-message-resets-the-stall-counter-when-embedding-focus-shifts
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-identical-iterations 2)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.db/current-embedding-provider  (constantly (semantic-loop-test-provider))
                  xia.autonomous/max-iterations      (constantly 6)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Inspecting the config file.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Inspecting the config file\",\"next_step\":\"Inspect config file\",\"reason\":\"Need more details\",\"goal_complete\":false,\"current_focus\":\"Inspect config file\",\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Inspect config file\",\"status\":\"in_progress\"}]}")}
                                                         2 {"content" (str "Following up with the billing customer.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Following up with the billing customer\",\"next_step\":\"Follow up with the billing customer\",\"reason\":\"Need more details\",\"goal_complete\":false,\"current_focus\":\"Follow up with the billing customer\",\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Follow up with the billing customer\",\"status\":\"in_progress\"}]}")}
                                                         {"content" "Finished.\n\nAUTONOMOUS_STATUS_JSON:{\"status\":\"complete\",\"summary\":\"Finished\",\"next_step\":\"\",\"reason\":\"Done\",\"goal_complete\":true,\"current_focus\":\"Follow up with the billing customer\",\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Follow up with the billing customer\",\"status\":\"completed\"}]}"}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Finished."
             (agent/process-message session-id "inspect config file" :channel :terminal))))
    (is (= 3 @llm-calls))))

(deftest process-message-does-not-stall-when-agenda-completion-advances
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (db/set-config! :agent/supervisor-max-identical-iterations 2)
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.autonomous/max-iterations      (constantly 3)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "Checked inbox.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Checked inbox\",\"next_step\":\"Draft reply\",\"reason\":\"Need to draft\",\"goal_complete\":false,\"current_focus\":\"Handle billing\",\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft reply\",\"status\":\"pending\"}]}")}
                                                         2 {"content" (str "Drafted reply.\n\n"
                                                                           "AUTONOMOUS_STATUS_JSON:"
                                                                           "{\"status\":\"continue\",\"summary\":\"Drafted reply\",\"next_step\":\"Send reply\",\"reason\":\"Need to send\",\"goal_complete\":false,\"current_focus\":\"Handle billing\",\"progress_status\":\"in_progress\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft reply\",\"status\":\"completed\"},{\"item\":\"Send reply\",\"status\":\"pending\"}]}")}
                                                         {"content" (str "Sent reply.\n\n"
                                                                         "AUTONOMOUS_STATUS_JSON:"
                                                                         "{\"status\":\"complete\",\"summary\":\"Sent reply\",\"next_step\":\"\",\"reason\":\"Done\",\"goal_complete\":true,\"current_focus\":\"Handle billing\",\"progress_status\":\"complete\",\"agenda\":[{\"item\":\"Check inbox\",\"status\":\"completed\"},{\"item\":\"Draft reply\",\"status\":\"completed\"},{\"item\":\"Send reply\",\"status\":\"completed\"}]}")}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "Sent reply."
             (agent/process-message session-id "handle billing" :channel :terminal))))
    (is (= 3 @llm-calls))))

(deftest process-message-allows-final-llm-call-after-last-tool-round
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}])
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                       {:summary "Found the page."})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (if (= 1 (swap! llm-calls inc))
                                                         {"content" (str "ACTION_INTENT_JSON:"
                                                                         "{\"focus\":\"Research this\",\"agenda_item\":\"Research this\",\"plan_step\":\"Search the page\",\"why\":\"Need the first search result before continuing\",\"tool\":\"web-search\",\"tool_args_summary\":\"{}\"}")
                                                          "tool_calls" [{"id" "call-1"
                                                                         "function" {"name" "web-search"
                                                                                     "arguments" "{}"}}]}
                                                         {"content" "done"}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "done"
             (agent/process-message session-id
                                    "research this"
                                    :channel :terminal
                                    :max-tool-rounds 1))))
    (is (= 2 @llm-calls))))

(deftest process-message-rejects-second-tool-round-when-max-tool-rounds-is-one
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom 0)]
    (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "web-search"
                                                                              :parameters {}}}])
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                       {:summary "Found the page."})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-calls inc)
                                                         1 {"content" (str "ACTION_INTENT_JSON:"
                                                                           "{\"focus\":\"Research this\",\"agenda_item\":\"Research this\",\"plan_step\":\"Search the page\",\"why\":\"Need the first search result before continuing\",\"tool\":\"web-search\",\"tool_args_summary\":\"{}\"}")
                                                            "tool_calls" [{"id" "call-1"
                                                                           "function" {"name" "web-search"
                                                                                       "arguments" "{}"}}]}
                                                         {"content" ""
                                                          "tool_calls" [{"id" "call-2"
                                                                         "function" {"name" "web-search"
                                                                                     "arguments" "{}"}}]}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (let [err (try
                  (agent/process-message session-id
                                         "research this"
                                         :channel :terminal
                                         :max-tool-rounds 1)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (re-find #"Too many tool-calling rounds" (.getMessage ^Exception err)))
        (is (= {:rounds 1
                :max-tool-rounds 1}
               (select-keys (ex-data err) [:rounds :max-tool-rounds])))))
    (is (= 2 @llm-calls))))

(deftest process-message-schedules-fact-utility-review
  (let [session-id (db/create-session! :terminal)
        reviewed   (atom nil)]
    (with-redefs [xia.tool/tool-definitions             (constantly [])
                  xia.working-memory/update-wm!         (fn [& _] nil)
                  xia.llm/resolve-provider-selection    (constantly {:provider {:llm.provider/id :default}
                                                                     :provider-id :default})
                  xia.context/build-messages-data       (fn [_session-id _opts]
                                                          {:messages [{:role "system" :content "test"}]
                                                           :used-fact-eids [11 22]})
                  xia.llm/chat-message                  (fn [_messages & _opts] {"content" "All set."})
                  xia.agent/schedule-fact-utility-review!
                  (fn [review-session-id fact-eids user-message assistant-response]
                    (reset! reviewed {:session-id review-session-id
                                      :fact-eids fact-eids
                                      :user-message user-message
                                      :assistant-response assistant-response}))]
      (is (= "All set."
             (agent/process-message session-id "hello" :channel :terminal))))
    (is (= {:session-id session-id
            :fact-eids [11 22]
            :user-message "hello"
            :assistant-response "All set."}
           @reviewed))))

(deftest schedule-fact-utility-review-batches-turns-per-session
  (reset! @#'xia.agent/fact-utility-review-state {})
  (try
    (let [session-id       (random-uuid)
          reviewed         (promise)
          submitted        (atom [])
          heuristic-calls  (atom [])
          review-call-count (atom 0)]
      (with-redefs [xia.agent/fact-utility-review-debounce-ms 25
                    xia.async/submit-background!
                    (fn [description f]
                      (swap! submitted conj description)
                      (future (f)))
                    xia.working-memory/apply-fact-utility-heuristic!
                    (fn [fact-eids assistant-response]
                      (swap! heuristic-calls conj {:fact-eids fact-eids
                                                   :assistant-response assistant-response})
                      0)
                    xia.working-memory/review-fact-utility-observations!
                    (fn [observations]
                      (swap! review-call-count inc)
                      (deliver reviewed observations)
                      (count observations))]
        (agent/schedule-fact-utility-review! session-id [11] "hello" "first response")
        (agent/schedule-fact-utility-review! session-id [22] "follow up" "second response")
        (is (= [{:fact-eids [11] :assistant-response "first response"}
                {:fact-eids [22] :assistant-response "second response"}]
               @heuristic-calls))
        (is (= [{:fact-eid 11
                 :user-message "hello"
                 :assistant-response "first response"}
                {:fact-eid 22
                 :user-message "follow up"
                 :assistant-response "second response"}]
               (deref reviewed 1000 ::timeout)))
        (is (= ["fact-utility-review"] @submitted))
        (is (= 1 @review-call-count))))
    (finally
      (reset! @#'xia.agent/fact-utility-review-state {}))))

(deftest process-message-continues-when-working-memory-update-fails
  (let [session-id (db/create-session! :terminal)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _]
                                                       (throw (ex-info "embedding unavailable"
                                                                       {:type :embedding-service-failure})))
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts] {"content" "All set."})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "All set."
             (agent/process-message session-id "hello" :channel :terminal))))
    (is (= [[:user "hello"]
            [:assistant "All set."]]
           (->> (db/session-messages session-id)
                (mapv (fn [{:keys [role content]}]
                        [role content])))))))

(deftest process-message-continues-when-fact-utility-review-scheduling-fails
  (let [session-id (db/create-session! :terminal)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids [11]})
                  xia.llm/chat-message               (fn [_messages & _opts] {"content" "All set."})
                  xia.agent/schedule-fact-utility-review!
                  (fn [& _]
                    (throw (ex-info "fact review queue unavailable" {:type :fact-review-failure})))]
      (is (= "All set."
             (agent/process-message session-id "hello" :channel :terminal))))
    (is (= [[:user "hello"]
            [:assistant "All set."]]
           (->> (db/session-messages session-id)
                (mapv (fn [{:keys [role content]}]
                        [role content])))))))

(deftest process-message-injects-visual-tool-results-as-follow-up-user-input
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom [])]
    (with-redefs [xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "browser-screenshot"
                                                                              :parameters {}}}])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :vision
                                                                              :llm.provider/vision? true}
                                                                  :provider-id :vision})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                       {:summary "Captured chart from the browser."
                                                        :detail "high"
                                                        :image_data_url "data:image/png;base64,AAAA"})
                  xia.llm/chat-message               (fn [messages & _opts]
                                                       (swap! llm-calls conj messages)
                                                       (if (= 1 (count @llm-calls))
                                                         {"content" (str "ACTION_INTENT_JSON:"
                                                                         "{\"focus\":\"Interpret the chart\",\"agenda_item\":\"Capture chart\",\"plan_step\":\"Take a browser screenshot\",\"why\":\"Need the visual state to interpret the chart\",\"tool\":\"browser-screenshot\",\"tool_args_summary\":\"{}\"}")
                                                          "tool_calls" [{"id" "call-1"
                                                                         "function" {"name" "browser-screenshot"
                                                                                     "arguments" "{}"}}]}
                                                         {"content" "done"}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "done"
             (agent/process-message session-id "interpret the chart" :channel :terminal))))
    (let [second-call (second @llm-calls)
          tool-msg    (some #(when (= "tool" (:role %)) %) second-call)
          image-msg   (last second-call)
          persisted-tool (some #(when (= :tool (:role %)) %) (db/session-messages session-id))]
      (is (= "Captured chart from the browser." (:content tool-msg)))
      (is (= "user" (:role image-msg)))
      (is (= "text" (get-in image-msg [:content 0 "type"])))
      (is (= "image_url" (get-in image-msg [:content 1 "type"])))
      (is (= "data:image/png;base64,AAAA"
             (get-in image-msg [:content 1 "image_url" "url"])))
      (is (= "high" (get-in image-msg [:content 1 "image_url" "detail"])))
      (is (= {:summary "Captured chart from the browser."
              :detail "high"}
             (:tool-result persisted-tool))))))

(deftest multimodal-follow-up-messages-accept-public-image-urls
  (with-redefs [xia.ssrf/validate-url! (fn [_] nil)]
    (let [messages (#'agent/multimodal-follow-up-messages
                    {:summary "Read this remote chart."
                     :image_url "https://cdn.example.com/chart.png"}
                    {:assistant-provider {:llm.provider/id :vision
                                          :llm.provider/vision? true}})]
      (is (= "user" (get-in messages [0 :role])))
      (is (= "https://cdn.example.com/chart.png"
             (get-in messages [0 :content 1 "image_url" "url"])))))) 

(deftest multimodal-follow-up-messages-skip-non-vision-providers
  (with-redefs [xia.ssrf/validate-url! (fn [_] nil)]
    (is (nil? (#'agent/multimodal-follow-up-messages
                {:summary "Read this remote chart."
                 :image_url "https://cdn.example.com/chart.png"}
                {:assistant-provider {:llm.provider/id :text-only
                                      :llm.provider/vision? false}
                 :assistant-provider-id :text-only})))))

(deftest process-message-does-not-inject-visual-follow-up-for-non-vision-models
  (let [session-id (db/create-session! :terminal)
        llm-calls  (atom [])]
    (with-redefs [xia.tool/tool-definitions          (constantly [{:type "function"
                                                                   :function {:name "browser-screenshot"
                                                                              :parameters {}}}])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :text-only
                                                                              :llm.provider/vision? false}
                                                                  :provider-id :text-only})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.tool/parallel-safe?            (constantly false)
                  xia.tool/execute-tool              (fn [_tool-id _args _context]
                                                       {:summary "Captured chart from the browser."
                                                        :detail "high"
                                                        :image_data_url "data:image/png;base64,AAAA"})
                  xia.llm/chat-message               (fn [messages & _opts]
                                                       (swap! llm-calls conj messages)
                                                       (if (= 1 (count @llm-calls))
                                                         {"content" (str "ACTION_INTENT_JSON:"
                                                                         "{\"focus\":\"Interpret the chart\",\"agenda_item\":\"Capture chart\",\"plan_step\":\"Take a browser screenshot\",\"why\":\"Need the visual state to interpret the chart\",\"tool\":\"browser-screenshot\",\"tool_args_summary\":\"{}\"}")
                                                          "tool_calls" [{"id" "call-1"
                                                                         "function" {"name" "browser-screenshot"
                                                                                     "arguments" "{}"}}]}
                                                         {"content" "done"}))
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "done"
             (agent/process-message session-id "interpret the chart" :channel :terminal))))
    (let [second-call (second @llm-calls)]
      (is (not-any? vector? (keep :content second-call)))
      (is (some #(and (= "tool" (:role %))
                      (= "Captured chart from the browser." (:content %)))
                second-call)))))

(deftest process-message-rejects-oversized-user-message-by-char-count
  (let [session-id  (db/create-session! :terminal)
        wm-calls    (atom 0)
        llm-calls   (atom 0)]
    (db/set-config! :agent/max-user-message-chars 5)
    (with-redefs [xia.working-memory/update-wm!      (fn [& _]
                                                       (swap! wm-calls inc))
                  xia.tool/tool-definitions          (constantly [])
                  xia.llm/resolve-provider-selection (fn [& _]
                                                       (swap! llm-calls inc)
                                                       {:provider {:llm.provider/id :default}
                                                        :provider-id :default})
                  xia.context/build-messages-data    (fn [& _]
                                                       (swap! llm-calls inc)
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [& _]
                                                       (swap! llm-calls inc)
                                                       {"content" "unreachable"})]
      (let [err (try
                  (agent/process-message session-id "123456" :channel :terminal)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (re-find #"User message too large: 6 chars \(max 5\)"
                     (.getMessage ^Exception err)))
        (is (= {:type       :user-message-too-large
                :status     413
                :error      "user message too large"
                :char-count 6
                :max-chars  5}
               (select-keys (ex-data err)
                            [:type :status :error :char-count :max-chars])))))
    (is (zero? @wm-calls))
    (is (zero? @llm-calls))
    (is (empty? (db/session-messages session-id)))))

(deftest process-message-rejects-oversized-user-message-by-token-estimate
  (let [session-id (db/create-session! :terminal)
        text       "hello world!"
        wm-calls   (atom 0)
        llm-calls  (atom 0)]
    (db/set-config! :agent/max-user-message-chars 1000)
    (db/set-config! :agent/max-user-message-tokens 1)
    (with-redefs [xia.working-memory/update-wm!      (fn [& _]
                                                       (swap! wm-calls inc))
                  xia.tool/tool-definitions          (constantly [])
                  xia.llm/resolve-provider-selection (fn [& _]
                                                       (swap! llm-calls inc)
                                                       {:provider {:llm.provider/id :default}
                                                        :provider-id :default})
                  xia.context/build-messages-data    (fn [& _]
                                                       (swap! llm-calls inc)
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [& _]
                                                       (swap! llm-calls inc)
                                                       {"content" "unreachable"})]
        (let [err (try
                  (agent/process-message session-id text :channel :terminal)
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (re-find #"User message too large: ~2 tokens \(max 1\)"
                     (.getMessage ^Exception err)))
        (is (= {:type           :user-message-too-large
                :status         413
                :error          "user message too large"
                :token-estimate 2
                :max-tokens     1}
               (select-keys (ex-data err)
                            [:type :status :error :token-estimate :max-tokens])))))
    (is (zero? @wm-calls))
    (is (zero? @llm-calls))
    (is (empty? (db/session-messages session-id)))))

(deftest process-message-rejects-concurrent-turns-per-session
  (let [session-id         (db/create-session! :http)
        first-llm-started  (promise)
        release-first-llm  (promise)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (deliver first-llm-started true)
                                                       @release-first-llm
                                                       {"content" "reply-1"})]
      (let [first-turn  (future (agent/process-message session-id "first" :channel :http))
            _           (is (= true (deref first-llm-started 1000 ::timeout)))
            second-turn (future
                          (try
                            (agent/process-message session-id "second" :channel :http)
                            (catch clojure.lang.ExceptionInfo e
                              e)))]
        (is (= [[:user "first"]]
               (->> (db/session-messages session-id)
                    (mapv (fn [{:keys [role content]}]
                            [role content])))))
        (let [busy-error @second-turn]
          (is (instance? clojure.lang.ExceptionInfo busy-error))
          (is (= {:type :session-busy
                  :status 409
                  :error "session is busy"
                  :session-id session-id}
                 (select-keys (ex-data busy-error)
                              [:type :status :error :session-id]))))
        (deliver release-first-llm true)
        (is (= "reply-1" @first-turn))))
    (is (= [[:user "first"]
            [:assistant "reply-1"]]
           (->> (db/session-messages session-id)
                (mapv (fn [{:keys [role content]}]
                        [role content])))))))

(deftest process-message-allows-concurrent-turns-for-different-sessions
  (let [session-a  (db/create-session! :http)
        session-b  (db/create-session! :http)
        llm-call    (atom 0)
        started-a  (promise)
        started-b  (promise)
        release-a  (promise)
        release-b  (promise)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (case (swap! llm-call inc)
                                                         1
                                                         (do
                                                           (deliver started-a true)
                                                           @release-a
                                                           {"content" "reply-a"})
                                                         2
                                                         (do
                                                           (deliver started-b true)
                                                           @release-b
                                                           {"content" "reply-b"})))]
      (let [turn-a (future (agent/process-message session-a "first" :channel :http))
            _      (is (= true (deref started-a 1000 ::timeout)))
            turn-b (future (agent/process-message session-b "second" :channel :http))]
        (is (= true (deref started-b 1000 ::timeout)))
        (deliver release-a true)
        (deliver release-b true)
        (is (= "reply-a" @turn-a))
        (is (= "reply-b" @turn-b))))
    (is (= [[:user "first"]
            [:assistant "reply-a"]]
           (->> (db/session-messages session-a)
                (mapv (fn [{:keys [role content]}]
                        [role content])))))
    (is (= [[:user "second"]
            [:assistant "reply-b"]]
           (->> (db/session-messages session-b)
                (mapv (fn [{:keys [role content]}]
                        [role content])))))))

(deftest run-branch-tasks-creates-worker-sessions-with_isolated_context
  (let [parent-session-id (db/create-session! :terminal)
        seen              (atom [])]
    (binding [prompt/*interaction-context* {:channel :terminal
                                            :request-id "req-parent"
                                            :correlation-id "corr-root"}]
      (with-redefs [xia.agent/process-message
                    (fn [session-id message & {:keys [channel provider-id resource-session-id
                                                     max-tool-rounds tool-context]}]
                      (swap! seen conj {:session-id session-id
                                        :message message
                                        :channel channel
                                        :provider-id provider-id
                                        :resource-session-id resource-session-id
                                        :tool-context tool-context})
                      (str "branch result for " session-id))]
        (let [result (agent/run-branch-tasks
                       [{:task "site a" :prompt "inspect site a"}
                        {:task "site b" :prompt "inspect site b"}]
                       :session-id parent-session-id
                       :provider-id :default
                       :objective "compare two sites"
                       :max-parallel 2
                       :max-tool-rounds 1)]
          (is (= 2 (:branch_count result)))
          (is (= 2 (:completed_count result)))
          (is (= 0 (:failed_count result)))
          (is (= "req-parent" (:request_id result)))
          (is (= "corr-root" (:correlation_id result)))
          (is (= 2 (count (:results result))))
          (is (every? #(= "completed" (:status %)) (:results result)))
          (is (every? #(not= parent-session-id (:session-id %)) @seen))
          (is (every? #(= :branch (:channel %)) @seen))
          (is (every? #(= :default (:provider-id %)) @seen))
          (is (every? #(= parent-session-id (:resource-session-id %)) @seen))
          (is (every? true? (map #(get-in % [:tool-context :branch-worker?]) @seen)))
          (is (every? #(= parent-session-id (get-in % [:tool-context :parent-session-id])) @seen))
          (is (every? #(= "corr-root" (get-in % [:tool-context :correlation-id])) @seen))
          (is (every? #(= "req-parent" (get-in % [:tool-context :parent-request-id])) @seen))
          (is (every? string? (map #(get-in % [:tool-context :request-id]) @seen)))
          (is (every? #(not= "req-parent" %) (map #(get-in % [:tool-context :request-id]) @seen)))
          (let [worker-sessions (db/list-sessions {:include-workers? true})]
            (is (= 1 (count (db/list-sessions))))
            (is (= 3 (count worker-sessions)))
            (is (= 2 (count (filter :worker? worker-sessions))))
            (is (every? false? (map :active? (filter :worker? worker-sessions))))))))))

(deftest run-branch-tasks-captures-throwable-detail-and-trace
  (let [parent-session-id (db/create-session! :terminal)]
    (binding [prompt/*interaction-context* {:channel :terminal
                                            :request-id "req-parent"
                                            :correlation-id "corr-root"}]
      (with-redefs [xia.agent/process-message
                    (fn [_session-id _message & _]
                      (throw (ex-info "branch exploded"
                                      {:kind :boom}
                                      (IllegalStateException. "disk full"))))]
        (let [result  (agent/run-branch-tasks
                        [{:task "site a" :prompt "inspect site a"}]
                        :session-id parent-session-id
                        :provider-id :default
                        :max-parallel 1)
              branch  (first (:results result))
              detail  (:error-detail branch)]
          (is (= 1 (:failed_count result)))
          (is (= "failed" (:status branch)))
          (is (= "req-parent" (:parent-request-id branch)))
          (is (= "corr-root" (:correlation-id branch)))
          (is (string? (:request-id branch)))
          (is (= "branch exploded" (:error branch)))
          (is (= "branch exploded" (:message detail)))
          (is (= "clojure.lang.ExceptionInfo" (:class detail)))
          (is (= "disk full" (get-in detail [:root-cause :message])))
          (is (= :boom (get-in detail [:causes 0 :data :kind])))
          (is (seq (:stack-trace detail))))))))

(deftest run-branch-tasks-times-out-hung-batches
  (let [parent-session-id (db/create-session! :terminal)
        started           (atom [])]
    (db/set-config! :agent/branch-task-timeout-ms 50)
    (with-redefs [xia.agent/process-message
                  (fn [_session-id message & _]
                    (swap! started conj message)
                    (if (str/includes? message "What to do:\nslow branch")
                      (Thread/sleep 10000)
                      "done"))]
      (let [started-at (System/nanoTime)
            err        (try
                         (agent/run-branch-tasks
                          [{:task "slow" :prompt "slow branch"}
                           {:task "fast" :prompt "fast branch"}]
                          :session-id parent-session-id
                          :max-parallel 2)
                         (catch clojure.lang.ExceptionInfo e
                           e))
            elapsed-ms (/ (double (- (System/nanoTime) started-at)) 1000000.0)]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (re-find #"Branch task timed out: slow"
                     (.getMessage ^Exception err)))
        (is (= {:type :branch-task-timeout
                :timeout-ms 50
                :task "slow"}
               (select-keys (ex-data err)
                            [:type :timeout-ms :task])))
        (is (< elapsed-ms 1000.0))
        (is (= 2 (count @started)))
        (is (some #(str/includes? % "What to do:\nslow branch") @started))
        (is (some #(str/includes? % "What to do:\nfast branch") @started))))))

(deftest run-branch-tasks-timeout-interrupts-child-process-message
  (let [parent-session-id (db/create-session! :terminal)
        started           (promise)
        stopped           (promise)]
    (db/set-config! :agent/branch-task-timeout-ms 50)
    (with-redefs [xia.agent/process-message
                  (fn [_child-session-id _message & _opts]
                    (deliver started true)
                    (try
                      (Thread/sleep 10000)
                      "done"
                      (finally
                        (deliver stopped true))))]
      (let [result (future
                     (try
                       (agent/run-branch-tasks
                        [{:task "slow" :prompt "slow branch"}]
                        :session-id parent-session-id
                        :max-parallel 1)
                       (catch Exception e
                         e)))]
        (is (= true (deref started 3000 ::timeout)))
        (let [err (deref result 3000 ::timeout)]
          (is (instance? clojure.lang.ExceptionInfo err))
          (is (re-find #"Branch task timed out: slow"
                       (.getMessage ^Exception err)))
          (is (= {:type :branch-task-timeout
                  :timeout-ms 50
                  :task "slow"}
                 (select-keys (ex-data err)
                              [:type :timeout-ms :task]))))
        (is (= true (deref stopped 3000 ::timeout)))))))

(deftest cancel-session-cancels-active-branch-child-sessions
  (let [parent-session-id (db/create-session! :terminal)
        child-started     (promise)
        child-cancelled   (promise)
        child-stopped     (promise)]
    (binding [prompt/*interaction-context* {:channel :terminal}]
      (with-redefs [xia.agent/process-message
                    (fn [child-session-id _message & _opts]
                      (#'xia.agent/with-session-run
                       child-session-id
                       (fn []
                         (deliver child-started child-session-id)
                         (try
                           (loop []
                             (#'xia.agent/throw-if-cancelled! child-session-id)
                             (try
                               (Thread/sleep 20)
                               (catch InterruptedException _
                                 (when (true? (:cancelled? (#'xia.agent/session-run-entry child-session-id)))
                                   (deliver child-cancelled child-session-id))
                                 (.interrupt (Thread/currentThread))))
                             (when (true? (:cancelled? (#'xia.agent/session-run-entry child-session-id)))
                               (deliver child-cancelled child-session-id))
                             (recur))
                           (catch clojure.lang.ExceptionInfo e
                             (when (= :request-cancelled (:type (ex-data e)))
                               (deliver child-cancelled child-session-id))
                             (throw e))
                           (finally
                             (deliver child-stopped true))))))]
        (let [result (future
                       (try
                         (#'xia.agent/with-session-run
                          parent-session-id
                          (fn []
                            (agent/run-branch-tasks
                             [{:task "slow" :prompt "slow branch"}]
                             :session-id parent-session-id
                             :max-parallel 1)))
                         (catch Throwable t
                           t)))
              child-session-id (deref child-started 1000 ::timeout)]
          (is (not= ::timeout child-session-id))
          (is (contains? (:child-session-ids (#'xia.agent/session-run-entry parent-session-id))
                         child-session-id))
          (is (true? (agent/cancel-session! parent-session-id "user requested cancel")))
          (is (= child-session-id (deref child-cancelled 1000 ::timeout)))
          (is (= true (deref child-stopped 1000 ::timeout)))
          (is (instance? Throwable (deref result 1000 ::timeout))))))))

(deftest run-branch-task-cleans-up-when-working-memory-creation-fails
  (let [parent-session-id (db/create-session! :terminal)
        deactivated       (atom [])
        cleared           (atom [])]
    (binding [prompt/*interaction-context* {:channel :terminal}]
      (with-redefs [xia.working-memory/create-wm! (fn [_session-id]
                                                    (throw (ex-info "wm boom" {})))
                    xia.db/set-session-active!  (fn [session-id active?]
                                                  (swap! deactivated conj [session-id active?]))
                    xia.working-memory/clear-wm! (fn [session-id]
                                                   (swap! cleared conj session-id))]
        (let [result (#'xia.agent/run-branch-task*
                      parent-session-id
                      {:task "site a" :prompt "inspect site a"}
                      {:channel :terminal})]
          (is (= "failed" (:status result)))
          (is (= "wm boom" (:error result)))
          (is (= [[(:session-id result) false]] @deactivated))
          (is (= [(:session-id result)] @cleared)))))))

(deftest run-branch-task-clears-wm-even-when-session-deactivation-fails
  (let [parent-session-id (db/create-session! :terminal)
        deactivated       (atom [])
        cleared           (atom [])]
    (binding [prompt/*interaction-context* {:channel :terminal}]
      (with-redefs [xia.agent/process-message     (fn [_session-id _message & _]
                                                    "branch result")
                    xia.db/set-session-active!  (fn [session-id active?]
                                                  (swap! deactivated conj [session-id active?])
                                                  (throw (ex-info "deactivate boom" {})))
                    xia.working-memory/clear-wm! (fn [session-id]
                                                   (swap! cleared conj session-id))]
        (let [result (#'xia.agent/run-branch-task*
                      parent-session-id
                      {:task "site a" :prompt "inspect site a"}
                      {:channel :terminal})]
          (is (= "completed" (:status result)))
          (is (= [[(:session-id result) false]] @deactivated))
          (is (= [(:session-id result)] @cleared)))))))

(deftest execute-tool-calls-runs-independent-batches-in-parallel
  (let [active     (atom 0)
        max-active (atom 0)
        tool-calls [{"id"       "call-1"
                     "function" {"name"      "web-fetch"
                                 "arguments" "{}"}}
                    {"id"       "call-2"
                     "function" {"name"      "web-search"
                                 "arguments" "{}"}}]]
    (with-redefs [tool/parallel-safe? (constantly true)
                  tool/execute-tool   (fn [tool-id _args _context]
                                        (let [sleep-ms (if (= tool-id :web-fetch) 200 50)
                                              active-now (swap! active inc)]
                                          (swap! max-active max active-now)
                                          (Thread/sleep sleep-ms)
                                          (swap! active dec)
                                          {:tool (name tool-id)}))]
      (let [results (#'agent/execute-tool-calls tool-calls {:channel :terminal})]
        (is (= 2 @max-active))
        (is (= ["call-1" "call-2"] (mapv :tool_call_id results)))
        (is (= ["web-fetch" "web-search"]
               (mapv #(get (json/read-json (:content %)) "tool") results)))))))

(deftest execute-tool-calls-keeps-unsafe-tools-sequential
  (let [active     (atom 0)
        max-active (atom 0)
        tool-calls [{"id"       "call-1"
                     "function" {"name"      "browser-open"
                                 "arguments" "{}"}}
                    {"id"       "call-2"
                     "function" {"name"      "browser-navigate"
                                 "arguments" "{}"}}]]
    (with-redefs [tool/parallel-safe? (constantly false)
                  tool/execute-tool   (fn [tool-id _args _context]
                                        (let [active-now (swap! active inc)]
                                          (swap! max-active max active-now)
                                          (Thread/sleep 75)
                                          (swap! active dec)
                                          {:tool (name tool-id)}))]
      (let [results (#'agent/execute-tool-calls tool-calls {:channel :terminal})]
        (is (= 1 @max-active))
        (is (= ["call-1" "call-2"] (mapv :tool_call_id results)))))))

(deftest execute-tool-calls-waits-for-all-parallel-futures-before-rethrowing
  (let [started       (atom [])
        release-slow  (promise)
        slow-finished (promise)
        tool-calls    [{"id"       "call-1"
                        "function" {"name"      "web-fetch"
                                    "arguments" "{}"}}
                       {"id"       "call-2"
                        "function" {"name"      "web-search"
                                    "arguments" "{}"}}]]
    (with-redefs [tool/parallel-safe? (constantly true)
                  tool/execute-tool   (fn [tool-id _args _context]
                                        (swap! started conj tool-id)
                                        (case tool-id
                                          :web-fetch
                                          (throw (ex-info "boom" {:tool-id tool-id}))

                                          :web-search
                                          (do
                                            @release-slow
                                            (deliver slow-finished true)
                                            {:tool (name tool-id)})))]
      (let [batch-future (future
                           (try
                             (#'agent/execute-tool-calls tool-calls {:channel :terminal})
                             (catch Exception e
                               e)))]
        (loop [attempt 0]
          (when (and (< attempt 20)
                     (not= #{:web-fetch :web-search} (set @started)))
            (Thread/sleep 10)
            (recur (inc attempt))))
        (is (= #{:web-fetch :web-search} (set @started)))
        (is (= ::pending (deref batch-future 100 ::pending)))
        (deliver release-slow true)
        (is (= true (deref slow-finished 1000 ::timeout)))
        (let [err (deref batch-future 1000 ::timeout)]
          (is (instance? clojure.lang.ExceptionInfo err))
          (is (re-find #"Parallel tool execution failed: web-fetch"
                       (.getMessage ^Exception err))))))))

(deftest execute-tool-calls-times-out-hung-parallel-futures
  (let [started    (atom [])
        tool-calls [{"id"       "call-1"
                     "function" {"name"      "web-fetch"
                                 "arguments" "{}"}}
                    {"id"       "call-2"
                     "function" {"name"      "web-search"
                                 "arguments" "{}"}}]]
    (db/set-config! :agent/parallel-tool-timeout-ms 50)
    (with-redefs [tool/parallel-safe? (constantly true)
                  tool/execute-tool   (fn [tool-id _args _context]
                                        (swap! started conj tool-id)
                                        (if (= :web-fetch tool-id)
                                          (Thread/sleep 10000)
                                          {:tool (name tool-id)}))]
      (let [started-at (System/nanoTime)
            err        (try
                         (#'agent/execute-tool-calls tool-calls {:channel :terminal})
                         (catch clojure.lang.ExceptionInfo e
                           e))
            elapsed-ms (/ (double (- (System/nanoTime) started-at)) 1000000.0)]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (re-find #"Parallel tool execution timed out: web-fetch"
                     (.getMessage ^Exception err)))
        (is (= {:type :parallel-tool-timeout
                :timeout-ms 50
                :tool-id :web-fetch
                :func-name "web-fetch"}
               (select-keys (ex-data err)
                            [:type :timeout-ms :tool-id :func-name])))
        (is (< elapsed-ms 1000.0))
        (is (= #{:web-fetch :web-search} (set @started)))))))

(deftest execute-tool-calls-rejects-oversized-rounds-before-execution
  (let [executed   (atom 0)
        tool-calls (vec (for [i (range 13)]
                          {"id"       (str "call-" i)
                           "function" {"name"      "web-search"
                                       "arguments" "{}"}}))]
    (with-redefs [tool/parallel-safe? (constantly true)
                  tool/execute-tool   (fn [& _]
                                        (swap! executed inc)
                                        {:ok true})]
      (let [err (try
                  (#'agent/execute-tool-calls tool-calls {:channel :terminal})
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (re-find #"Too many tool calls in one round: 13 \(max 12\)"
                     (.getMessage ^Exception err)))
        (is (= {:tool-count 13
                :max-tool-calls-per-round 12}
               (select-keys (ex-data err)
                            [:tool-count :max-tool-calls-per-round])))
        (is (zero? @executed))))))

(deftest execute-tool-calls-rejects-oversized-sequential-rounds-before-execution
  (let [executed   (atom 0)
        tool-calls (vec (for [i (range 13)]
                          {"id"       (str "call-" i)
                           "function" {"name"      "web-search"
                                       "arguments" "{}"}}))]
    (with-redefs [tool/parallel-safe? (constantly false)
                  tool/execute-tool   (fn [& _]
                                        (swap! executed inc)
                                        {:ok true})]
      (let [err (try
                  (#'agent/execute-tool-calls tool-calls {:channel :terminal})
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (re-find #"Too many tool calls in one round: 13 \(max 12\)"
                     (.getMessage ^Exception err)))
        (is (= {:tool-count 13
                :max-tool-calls-per-round 12}
               (select-keys (ex-data err)
                            [:tool-count :max-tool-calls-per-round])))
        (is (zero? @executed))))))

(deftest execute-tool-calls-honors-configured-round-cap
  (let [executed   (atom 0)
        tool-calls (vec (for [i (range 5)]
                          {"id"       (str "call-" i)
                           "function" {"name"      "web-search"
                                       "arguments" "{}"}}))]
    (db/set-config! :agent/max-tool-calls-per-round 4)
    (with-redefs [tool/parallel-safe? (constantly true)
                  tool/execute-tool   (fn [& _]
                                        (swap! executed inc)
                                        {:ok true})]
      (let [err (try
                  (#'agent/execute-tool-calls tool-calls {:channel :terminal})
                  (catch clojure.lang.ExceptionInfo e
                    e))]
        (is (instance? clojure.lang.ExceptionInfo err))
        (is (re-find #"Too many tool calls in one round: 5 \(max 4\)"
                     (.getMessage ^Exception err)))
        (is (= {:tool-count 5
                :max-tool-calls-per-round 4}
               (select-keys (ex-data err)
                            [:tool-count :max-tool-calls-per-round])))
        (is (zero? @executed))))))
