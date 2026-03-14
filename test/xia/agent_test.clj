(ns xia.agent-test
  (:require [clojure.test :refer :all]
            [charred.api :as json]
            [xia.agent :as agent]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.tool :as tool]
            [xia.test-helpers :refer [with-test-db]]
            [xia.working-memory :as wm]))

(use-fixtures :each with-test-db)

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
                    xia.llm/chat-simple       (fn [_messages & _opts] "All set.")]
        (is (= "All set."
               (agent/process-message session-id "hello" :channel :terminal))))
      (is (= [{:state :running
               :phase :working-memory
               :message "Updating working memory"}
              {:state :running
               :phase :llm
               :message "Calling model"}
              {:state :running
               :phase :finalizing
               :message "Preparing response"}
              {:state :done
               :phase :complete
               :message "Ready"}]
             @statuses))
      (finally
        (prompt/register-status! :terminal nil)))))

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
                  xia.llm/chat-simple                (fn [_messages & opts]
                                                       (reset! llm-opts opts)
                                                       "All set.")]
      (is (= "All set."
             (agent/process-message session-id "hello" :channel :terminal))))
    (is (= 1 @selection-calls))
    (is (= :router-a (:provider-id @build-messages-opts)))
    (is (= :router-a (get-in @build-messages-opts [:provider :llm.provider/id])))
    (is (= :history-compaction (:compaction-workload @build-messages-opts)))
    (is (= [:provider-id :router-a] @llm-opts))))

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
                  xia.llm/chat-simple                (fn [_messages & _opts] "All set.")]
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
                  xia.llm/chat-simple                   (fn [_messages & _opts] "All set.")
                  xia.agent/schedule-fact-utility-review!
                  (fn [fact-eids user-message assistant-response]
                    (reset! reviewed {:fact-eids fact-eids
                                      :user-message user-message
                                      :assistant-response assistant-response}))]
      (is (= "All set."
             (agent/process-message session-id "hello" :channel :terminal))))
    (is (= {:fact-eids [11 22]
            :user-message "hello"
            :assistant-response "All set."}
           @reviewed))))

(deftest process-message-serializes-concurrent-turns-per-session
  (let [session-id         (db/create-session! :http)
        first-llm-started  (promise)
        release-first-llm  (promise)
        llm-call-count     (atom 0)]
    (with-redefs [xia.tool/tool-definitions          (constantly [])
                  xia.working-memory/update-wm!      (fn [& _] nil)
                  xia.llm/resolve-provider-selection (constantly {:provider {:llm.provider/id :default}
                                                                  :provider-id :default})
                  xia.context/build-messages-data    (fn [_session-id _opts]
                                                       {:messages [{:role "system" :content "test"}]
                                                        :used-fact-eids []})
                  xia.agent/schedule-fact-utility-review! (fn [& _] nil)
                  xia.llm/chat-simple                (fn [_messages & _opts]
                                                       (case (swap! llm-call-count inc)
                                                         1 (do
                                                             (deliver first-llm-started true)
                                                             @release-first-llm
                                                             "reply-1")
                                                         2 "reply-2"))]
      (let [first-turn  (future (agent/process-message session-id "first" :channel :http))
            _           (is (= true (deref first-llm-started 1000 ::timeout)))
            second-turn (future (agent/process-message session-id "second" :channel :http))]
        (Thread/sleep 50)
        (is (= [[:user "first"]]
               (->> (db/session-messages session-id)
                    (mapv (fn [{:keys [role content]}]
                            [role content])))))
        (deliver release-first-llm true)
        (is (= "reply-1" @first-turn))
        (is (= "reply-2" @second-turn))))
    (is (= [[:user "first"]
            [:assistant "reply-1"]
            [:user "second"]
            [:assistant "reply-2"]]
           (->> (db/session-messages session-id)
                (mapv (fn [{:keys [role content]}]
                        [role content])))))))

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

(deftest prepare-tool-call-warns-on-malformed-json-arguments
  (let [logged   (atom nil)
        tool-call {"id"       "call-1"
                   "function" {"name"      "web-search"
                               "arguments" "{not-json"}}]
    (with-redefs [clojure.tools.logging/log*
                  (fn [_logger level throwable message]
                    (reset! logged {:level level
                                    :throwable throwable
                                    :message message}))]
      (let [prepared (#'agent/prepare-tool-call tool-call)]
        (is (= {} (:args prepared)))
        (is (= :web-search (:tool-id prepared)))
        (is (= :warn (:level @logged)))
        (is (instance? Exception (:throwable @logged)))
        (is (re-find #"Failed to parse tool arguments for web-search"
                     (:message @logged)))))))

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
