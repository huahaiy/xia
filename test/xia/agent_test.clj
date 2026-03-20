(ns xia.agent-test
  (:require [clojure.test :refer :all]
            [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
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
                  xia.llm/chat-with-tools            (fn [_messages _tools & _opts]
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
    (is (= :planning (get-in @checkpoints [0 1 :phase])))
    (is (= :tool (get-in @checkpoints [1 1 :phase])))
    (is (= ["web-search"] (get-in @checkpoints [1 1 :tool-ids])))))

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
                  xia.llm/chat-with-tools            (fn [messages _tools & _opts]
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
                     :image_url "https://cdn.example.com/chart.png"})]
      (is (= "user" (get-in messages [0 :role])))
      (is (= "https://cdn.example.com/chart.png"
             (get-in messages [0 :content 1 "image_url" "url"])))))) 

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
                  xia.llm/chat-simple                (fn [& _]
                                                       (swap! llm-calls inc)
                                                       "unreachable")]
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
                  xia.llm/chat-simple                (fn [& _]
                                                       (swap! llm-calls inc)
                                                       "unreachable")]
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

(deftest run-branch-tasks-creates-worker-sessions-with_isolated_context
  (let [parent-session-id (db/create-session! :terminal)
        seen              (atom [])]
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
        (is (= 2 (count (:results result))))
        (is (every? #(= "completed" (:status %)) (:results result)))
        (is (every? #(not= parent-session-id (:session-id %)) @seen))
        (is (every? #(= :branch (:channel %)) @seen))
        (is (every? #(= :default (:provider-id %)) @seen))
        (is (every? #(= parent-session-id (:resource-session-id %)) @seen))
        (is (every? true? (map #(get-in % [:tool-context :branch-worker?]) @seen)))
        (is (every? #(= parent-session-id (get-in % [:tool-context :parent-session-id])) @seen))
        (let [worker-sessions (db/list-sessions {:include-workers? true})]
          (is (= 1 (count (db/list-sessions))))
          (is (= 3 (count worker-sessions)))
          (is (= 2 (count (filter :worker? worker-sessions))))
          (is (every? false? (map :active? (filter :worker? worker-sessions)))))))))

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
