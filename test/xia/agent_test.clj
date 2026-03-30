(ns xia.agent-test
  (:require [clojure.test :refer :all]
            [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.agent :as agent]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.tool :as tool]
            [xia.test-helpers :refer [with-test-db]]
            [xia.working-memory :as wm]))

(use-fixtures :each with-test-db)

(deftest configured-max-tool-rounds-defaults-to-one-hundred
  (db/delete-config! :agent/max-tool-rounds)
  (is (= 100 (#'xia.agent/configured-max-tool-rounds))))

(deftest worker-run-cleanup-is-worker-scoped
  (let [session-id (db/create-session! :terminal)
        token-a    (Object.)
        token-b    (Object.)
        worker-a   (future :worker-a)
        worker-b   (future :worker-b)]
    (#'xia.agent/with-session-run
     session-id
     (fn []
       (#'xia.agent/begin-worker-run! session-id token-a)
       (#'xia.agent/register-worker-future! session-id token-a worker-a)
       (#'xia.agent/register-worker-thread! session-id token-a)
       (#'xia.agent/begin-worker-run! session-id token-b)
       (#'xia.agent/register-worker-future! session-id token-b worker-b)
       (#'xia.agent/register-worker-thread! session-id token-b)
       (#'xia.agent/clear-worker-run! session-id token-a)
       (let [entry (#'xia.agent/session-run-entry session-id)]
         (is (= token-b (:worker-token entry)))
         (is (= worker-b (:worker-future entry)))
         (is (= (Thread/currentThread) (:worker-thread entry))))
       (#'xia.agent/clear-worker-run! session-id token-b)
       (let [entry (#'xia.agent/session-run-entry session-id)]
         (is (nil? (:worker-token entry)))
         (is (nil? (:worker-future entry)))
         (is (nil? (:worker-thread entry))))))))

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

(deftest process-message-parses-the-final-controller-response-once
  (let [session-id   (db/create-session! :terminal)
        parse-calls  (atom 0)
        parse-fn     autonomous/parse-controller-response]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.autonomous/parse-controller-response
                  (fn [response]
                    (swap! parse-calls inc)
                    (parse-fn response))
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       {"content" "All set."})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)]
      (is (= "All set."
             (agent/process-message session-id "hello" :channel :terminal))))
    (is (= 1 @parse-calls))))

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
                                                         {"content" ""
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
    (is (= :tool (get-in @checkpoints [2 1 :phase])))
    (is (= "web-search" (get-in @checkpoints [2 1 :tool-name])))
    (is (= :tool (get-in @checkpoints [3 1 :phase])))
    (is (= ["web-search"] (get-in @checkpoints [3 1 :tool-ids])))
    (is (= :observing (get-in @checkpoints [4 1 :phase])))))

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
    (is (str/includes? (get-in @llm-messages [0 2 :content])
                       "Current iteration: 1 of 6."))
    (is (str/includes? (get-in @llm-messages [1 2 :content])
                       "Current iteration: 2 of 6."))
    (is (str/includes? (get-in @llm-messages [1 2 :content])
                       "Tip summary: Checked inbox"))
    (is (str/includes? (get-in @llm-messages [1 2 :content])
                       "[in_progress] Draft replies"))
    (is (= [[:user "reply to the billing emails"]
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

(deftest process-message-refreshes-working-memory-between-autonomous-iterations
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
    (is (= ["reply to the billing emails"] @wm-refreshes))))

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

(deftest process-message-clears-autonomy-state-after-completed-goal
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
    (is (nil? (wm/autonomy-state session-id)))
    (wm/clear-wm! session-id)
    (wm/ensure-wm! session-id)
    (is (nil? (wm/autonomy-state session-id)))))

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
    (is (= 2 @snapshot-calls)
        "one snapshot after the iteration update, one after clearing autonomy state")))

(deftest process-message-clears-autonomy-state-when-control-envelope-is-missing
  (let [session-id (db/create-session! :terminal)]
    (wm/ensure-wm! session-id)
    (wm/set-autonomy-state! session-id
                            {:goal "Reply to the billing emails"
                             :stack [{:title "Reply to the billing emails"
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
    (is (nil? (wm/autonomy-state session-id)))
    (wm/clear-wm! session-id)
    (wm/ensure-wm! session-id)
    (is (nil? (wm/autonomy-state session-id)))))

(deftest process-message-clears-autonomy-state-and-strips-malformed-control-envelope
  (let [session-id (db/create-session! :terminal)]
    (wm/ensure-wm! session-id)
    (wm/set-autonomy-state! session-id
                            {:goal "Reply to the billing emails"
                             :stack [{:title "Reply to the billing emails"
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
    (is (nil? (wm/autonomy-state session-id)))))

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
    (is (str/includes? (get-in @llm-messages [1 2 :content])
                       "Current execution stack"))
    (is (str/includes? (get-in @llm-messages [1 2 :content])
                       "[resumable] Find invoice ids"))
    (is (str/includes? (get-in @llm-messages [1 2 :content])
                       "New input for this turn:"))
    (is (str/includes? (get-in @llm-messages [1 2 :content])
                       "Here are the invoice ids: 12 and 13"))))

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
                                                           1 {"content" ""
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
    (is (= [[:user "keep going"]]
           (->> (db/session-messages session-id)
                (mapv (fn [{:keys [role content]}]
                        [role content])))))))

(deftest process-message-does-not-treat-changing-summaries-as-identical-iterations
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
      (let [result (agent/process-message session-id "keep going" :channel :terminal)]
        (is (str/includes? result "Step three finished."))
        (is (str/includes? result "Note: I stopped after reaching the autonomous iteration limit for this turn (3)."))))
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
                                                         {"content" ""
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
                                                       (swap! llm-calls inc)
                                                       {"content" ""
                                                        "tool_calls" [{"id" "call-1"
                                                                       "function" {"name" "web-search"
                                                                                   "arguments" "{}"}}]})
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
          heuristic-calls  (atom [])
          review-call-count (atom 0)]
      (with-redefs [xia.agent/fact-utility-review-debounce-ms 25
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
        (is (= 1 @review-call-count))))
    (finally
      (reset! @#'xia.agent/fact-utility-review-state {}))))

(deftest fact-utility-review-state-update-returns-committed-enqueue-result-after-retry
  (let [session-id  (random-uuid)
        state-atom  (atom {})
        cas-calls   (atom 0)
        observations [{:fact-eid 11
                        :user-message "hello"
                        :assistant-response "reply"}]
        cas-fn      (fn [atm old new]
                      (if (= 1 (swap! cas-calls inc))
                        (do
                          (reset! atm {session-id {:pending [{:fact-eid 99
                                                              :user-message "prior"
                                                              :assistant-response "prior"}]
                                                   :worker-token :external-token}})
                          false)
                        (compare-and-set! atm old new)))]
    (with-redefs [clojure.core/random-uuid (let [ids (atom [:stale-token :fresh-token])]
                                             (fn []
                                               (let [id (first @ids)]
                                                 (swap! ids rest)
                                                 id)))]
      (is (nil? (#'xia.agent/update-fact-utility-review-state!
                 state-atom
                 cas-fn
                 #(#'xia.agent/enqueue-fact-utility-review-state % session-id observations))))
      (is (= :external-token
             (get-in @state-atom [session-id :worker-token])))
      (is (= [{:fact-eid 99
               :user-message "prior"
               :assistant-response "prior"}
              {:fact-eid 11
               :user-message "hello"
               :assistant-response "reply"}]
             (get-in @state-atom [session-id :pending]))))))

(deftest fact-utility-review-state-update-returns-committed-claim-result-after-retry
  (let [session-id (random-uuid)
        state-atom (atom {session-id {:pending [{:fact-eid 1} {:fact-eid 2}]
                                      :worker-token :worker-a}})
        cas-calls  (atom 0)
        cas-fn     (fn [atm old new]
                     (if (= 1 (swap! cas-calls inc))
                       (do
                         (reset! atm {session-id {:pending [{:fact-eid 3}]
                                                  :worker-token :worker-b}})
                         false)
                       (compare-and-set! atm old new)))]
    (is (nil? (#'xia.agent/update-fact-utility-review-state!
               state-atom
               cas-fn
               #(#'xia.agent/claim-fact-utility-review-batch-state % session-id :worker-a))))
    (is (= :worker-b
           (get-in @state-atom [session-id :worker-token])))))

(deftest fact-utility-review-state-update-returns-committed-finish-result-after-retry
  (let [session-id (random-uuid)
        state-atom (atom {session-id {:pending [{:fact-eid 1}]
                                      :worker-token :worker-a}})
        cas-calls  (atom 0)
        cas-fn     (fn [atm old new]
                     (if (= 1 (swap! cas-calls inc))
                       (do
                         (reset! atm {})
                         false)
                       (compare-and-set! atm old new)))]
    (with-redefs [clojure.core/random-uuid (constantly :stale-token)]
      (is (nil? (#'xia.agent/update-fact-utility-review-state!
                 state-atom
                 cas-fn
                 #(#'xia.agent/finish-fact-utility-review-worker-state % session-id :worker-a))))
      (is (= {} @state-atom)))))

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
                                                         {"content" ""
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
                                                         {"content" ""
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
    (with-redefs [xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.tool/tool-definitions          (constantly [])
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.llm/chat-message               (fn [_messages & _opts]
                                                       (deliver started true)
                                                       (try
                                                         (Thread/sleep 10000)
                                                         {"content" "done"}
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
        (is (= true (deref started 1000 ::timeout)))
        (let [err (deref result 1000 ::timeout)]
          (is (instance? clojure.lang.ExceptionInfo err))
          (is (re-find #"Branch task timed out: slow"
                       (.getMessage ^Exception err)))
          (is (= {:type :branch-task-timeout
                  :timeout-ms 50
                  :task "slow"}
                 (select-keys (ex-data err)
                              [:type :timeout-ms :task]))))
        (is (= true (deref stopped 1000 ::timeout)))))))

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

(deftest tool-call-batches-only-group-consecutive-parallel-safe-calls
  (let [prepared-calls [{:func-name "web-search-1" :parallel? true}
                        {:func-name "browser-click" :parallel? false}
                        {:func-name "web-search-2" :parallel? true}
                        {:func-name "web-fetch" :parallel? true}
                        {:func-name "browser-type" :parallel? false}]]
    (is (= [{:parallel? true
             :calls [{:func-name "web-search-1" :parallel? true}]}
            {:parallel? false
             :calls [{:func-name "browser-click" :parallel? false}]}
            {:parallel? true
             :calls [{:func-name "web-search-2" :parallel? true}
                     {:func-name "web-fetch" :parallel? true}]}
            {:parallel? false
             :calls [{:func-name "browser-type" :parallel? false}]}]
           (#'agent/tool-call-batches prepared-calls)))))

(deftest stop-worker-cancels-registered-parallel-tool-futures
  (let [session-id    (db/create-session! :terminal)
        worker-token  (Object.)
        started-tools (atom #{})
        interrupted   {:web-fetch (promise)
                       :web-search (promise)}
        tool-calls    [{"id"       "call-1"
                        "function" {"name"      "web-fetch"
                                    "arguments" "{}"}}
                       {"id"       "call-2"
                        "function" {"name"      "web-search"
                                    "arguments" "{}"}}]]
    (#'xia.agent/with-session-run
     session-id
     (fn []
       (#'xia.agent/begin-worker-run! session-id worker-token)
       (with-redefs [tool/parallel-safe? (constantly true)
                     tool/execute-tool   (fn [tool-id _args _context]
                                           (swap! started-tools conj tool-id)
                                           (try
                                             (Thread/sleep 10000)
                                             {:tool (name tool-id)}
                                             (catch InterruptedException e
                                               (deliver (get interrupted tool-id) true)
                                               (.interrupt (Thread/currentThread))
                                               (throw e))))]
         (let [batch-future (future
                              (try
                                (#'xia.agent/register-worker-thread! session-id worker-token)
                                (#'agent/execute-tool-calls tool-calls
                                                            {:channel :terminal
                                                             :session-id session-id
                                                             :worker-token worker-token})
                                (catch Exception e
                                  e)
                                (finally
                                  (#'xia.agent/clear-worker-run! session-id worker-token))))]
           (#'xia.agent/register-worker-future! session-id worker-token batch-future)
           (loop [attempt 0]
             (when (and (< attempt 50)
                        (or (not= #{:web-fetch :web-search} @started-tools)
                            (not= 2 (count (:parallel-tool-futures (#'xia.agent/session-run-entry session-id))))))
               (Thread/sleep 10)
               (recur (inc attempt))))
           (is (= #{:web-fetch :web-search} @started-tools))
           (is (= 2 (count (:parallel-tool-futures (#'xia.agent/session-run-entry session-id)))))
           (is (true? (#'xia.agent/stop-worker! session-id batch-future)))
           (is (= true (deref (:web-fetch interrupted) 1000 ::timeout)))
           (is (= true (deref (:web-search interrupted) 1000 ::timeout)))
           (let [result (try
                          (deref batch-future 1000 ::timeout)
                          (catch java.util.concurrent.CancellationException e
                            e))]
             (is (or (instance? InterruptedException result)
                     (instance? clojure.lang.ExceptionInfo result)
                     (instance? java.util.concurrent.CancellationException result))))
           (is (= [] (:parallel-tool-futures (#'xia.agent/session-run-entry session-id))))))))))

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

(deftest bind-original-tool-call-ids-preserves-llm-tool-call-id
  (let [prepared-calls [{:tool-call {"id" "call-1"
                                     "function" {"name" "web-search"
                                                 "arguments" "{}"}}
                         :func-name "web-search"
                         :tool-id :web-search}]
        results        [{:role "tool"
                         :tool_call_id "wrong-id"
                         :content "{\"ok\":true}"
                         :result {"ok" true}}]]
    (is (= [{:role "tool"
             :tool_call_id "call-1"
             :content "{\"ok\":true}"
             :result {"ok" true}}]
           (#'agent/bind-original-tool-call-ids prepared-calls results)))))

(deftest prepare-tool-call-warns-on-malformed-json-arguments
  (let [logged   (atom nil)
        tool-call {"id"       "call-1"
                   "function" {"name"      "web-search"
                               "arguments" "{not-json"}}]
    (log/with-min-level :trace
      (with-redefs [log/-log!
                    (fn [_config level _ns-str _file _line _column _msg-type _auto-err vargs_ _base-data _callsite-id _spying?]
                      (let [vargs     @vargs_
                            throwable (when (instance? Throwable (first vargs))
                                        (first vargs))
                            msg-args   (if throwable (rest vargs) vargs)]
                        (reset! logged {:level level
                                        :throwable throwable
                                        :message (str/join " " msg-args)})))]
        (let [prepared (#'agent/prepare-tool-call tool-call)]
          (is (= {} (:args prepared)))
          (is (= :web-search (:tool-id prepared)))
          (is (= :warn (:level @logged)))
          (is (instance? Exception (:throwable @logged)))
          (is (re-find #"Failed to parse tool arguments for web-search"
                       (:message @logged))))))))

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
