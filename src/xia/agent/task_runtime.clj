(ns xia.agent.task-runtime
  "Task runtime persistence and control operations."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.autonomous :as autonomous]
            [xia.async :as async]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.task-event :as task-event]
            [xia.working-memory :as wm]))

(defn- truncate-summary*
  [deps text max-chars]
  (when-let [f (:truncate-summary deps)]
    (f text max-chars)))

(defn- sanitized-tool-result*
  [deps result]
  (if-let [f (:sanitized-tool-result deps)]
    (f result)
    result))

(defn- merge-task-meta!
  [task-id f]
  (when-let [task (db/get-task task-id)]
    (db/update-task! task-id
                     {:meta (f (or (:meta task) {}))})))

(defn- clean-runtime-status
  [status]
  (->> status
       (keep (fn [[k v]]
               (when (some? v)
                 [k v])))
       (into {})))

(defn- set-task-runtime-status!
  [task-id status]
  (when task-id
    (merge-task-meta! task-id
                      (fn [meta]
                        (assoc meta
                               :runtime
                               (clean-runtime-status
                                (assoc status :updated-at (java.util.Date.))))))))

(defn- emit-runtime-event!
  [event]
  (when event
    (prompt/runtime-event! event)))

(defn- emit-task-started-event!
  [task-id]
  (some-> task-id db/get-task task-event/task-started-event emit-runtime-event!))

(defn- emit-task-updated-event!
  [task-id]
  (when-let [task (some-> task-id db/get-task)]
    (emit-runtime-event! (task-event/task-updated-event task))
    (some-> (task-event/task-state-event task)
            emit-runtime-event!)))

(defn- emit-turn-open-event!
  [task-turn-id]
  (when-let [turn (some-> task-turn-id db/get-task-turn)]
    (when-let [task (db/get-task (:task-id turn))]
      (some-> (task-event/turn-open-event task turn)
              emit-runtime-event!))))

(defn- emit-turn-close-event!
  [task-turn-id]
  (when-let [turn (some-> task-turn-id db/get-task-turn)]
    (when-let [task (db/get-task (:task-id turn))]
      (some-> (task-event/turn-close-event task turn)
              emit-runtime-event!))))

(defn- emit-item-event!
  [task-turn-id item-id]
  (when-let [turn (some-> task-turn-id db/get-task-turn)]
    (when-let [task (db/get-task (:task-id turn))]
      (when-let [item (db/get-task-item item-id)]
        (emit-runtime-event! (task-event/item-event task turn item))))))

(defn- task-title
  [deps user-message]
  (or (some-> user-message str str/trim not-empty (#(truncate-summary* deps % 240)))
      "Autonomous task"))

(defn- resolve-task-operation
  [task-id runtime-op]
  (or runtime-op
      (if task-id
        :resume
        :start)))

(defn runtime-autonomy-state
  [session-id task-id]
  (let [task-state (some-> task-id db/get-task :autonomy-state)
        wm-state   (wm/autonomy-state session-id)
        base-state (or task-state wm-state)
        reconciled (some-> base-state autonomous/reconcile-child-task-state)]
    (when (and task-id reconciled (not= reconciled task-state))
      (db/update-task! task-id {:autonomy-state reconciled}))
    (when (and session-id reconciled (not= reconciled wm-state))
      (wm/set-autonomy-state! session-id reconciled))
    reconciled))

(defn ensure-runtime-task!
  [deps session-id channel user-message autonomy-state task-id runtime-op interrupting-turn-id]
  (let [operation        (resolve-task-operation task-id runtime-op)
        title            (task-title deps user-message)
        new-task?        (nil? task-id)
        resolved-task-id (or task-id
                             (db/create-task! {:session-id session-id
                                               :channel channel
                                               :type :interactive
                                               :state :running
                                               :title title
                                               :summary title
                                               :autonomy-state autonomy-state
                                               :started-at (java.util.Date.)}))]
    (when task-id
      (db/update-task! resolved-task-id
                       {:state :running
                        :summary title
                        :autonomy-state autonomy-state}))
    (let [task-turn-id (db/start-task-turn! resolved-task-id
                                            {:operation operation
                                             :state :running
                                             :input user-message
                                             :summary title
                                             :interrupting-turn-id interrupting-turn-id})]
      (when new-task?
        (emit-task-started-event! resolved-task-id))
      (when-not new-task?
        (emit-task-updated-event! resolved-task-id))
      (emit-turn-open-event! task-turn-id)
      {:task-id resolved-task-id
       :task-turn-id task-turn-id
       :task-operation operation})))

(defn record-task-item!
  [task-turn-id attrs]
  (when task-turn-id
    (let [item-id (db/add-task-item! task-turn-id attrs)]
      (emit-item-event! task-turn-id item-id)
      item-id)))

(defn record-task-message-item!
  [deps task-turn-id item-type role text & {:keys [message-id llm-call-id data status]}]
  (when task-turn-id
    (let [data* (cond
                  (and text (map? data)) (assoc data :text text)
                  text {:text text}
                  (map? data) data
                  (some? data) {:value data}
                  :else nil)]
      (record-task-item! task-turn-id
                         (cond-> {:type item-type
                                  :role role
                                  :summary (or (some-> text (#(truncate-summary* deps % 240)))
                                               (some-> data* pr-str (#(truncate-summary* deps % 240))))}
                           status (assoc :status status)
                           message-id (assoc :message-id message-id)
                           llm-call-id (assoc :llm-call-id llm-call-id)
                           data* (assoc :data data*))))))

(defn record-task-tool-call-items!
  [task-turn-id assistant-message-id llm-call-id tool-calls]
  (doseq [tool-call tool-calls]
    (let [tool-name    (or (get-in tool-call ["function" "name"])
                           (get tool-call "name"))
          tool-call-id (get tool-call "id")
          arguments    (or (get-in tool-call ["function" "arguments"])
                           (get tool-call "arguments"))]
      (record-task-item! task-turn-id
                         (cond-> {:type :tool-call
                                  :status :requested
                                  :summary (str "Requested tool " tool-name)
                                  :message-id assistant-message-id
                                  :llm-call-id llm-call-id
                                  :tool-id tool-name
                                  :tool-call-id tool-call-id
                                  :data {:tool-name tool-name}}
                           arguments (assoc-in [:data :arguments] arguments))))))

(defn record-task-tool-result-item!
  [deps task-turn-id tool-message-id llm-call-id tool-result]
  (when task-turn-id
    (let [tool-name    (:tool_name tool-result)
          tool-call-id (:tool_call_id tool-result)
          result       (sanitized-tool-result* deps (:result tool-result))
          status       (if (:error tool-result) :error :success)
          summary      (or (truncate-summary* deps (:content tool-result) 240)
                           (truncate-summary* deps (:summary tool-result) 240)
                           (truncate-summary* deps (:error tool-result) 240)
                           (truncate-summary* deps (some-> result pr-str) 240)
                           (str (name status) " " tool-name))]
      (record-task-item! task-turn-id
                         {:type :tool-result
                          :status status
                          :summary summary
                          :message-id tool-message-id
                          :llm-call-id llm-call-id
                          :tool-id tool-name
                          :tool-call-id tool-call-id
                          :data (cond-> {:tool-name tool-name
                                         :status (name status)}
                                  result (assoc :result result)
                                  (:content tool-result) (assoc :content (:content tool-result))
                                  (:summary tool-result) (assoc :summary (:summary tool-result))
                                  (:error tool-result) (assoc :error (:error tool-result)))}))))

(defn sync-runtime-task!
  [task-id attrs]
  (when task-id
    (db/update-task! task-id attrs)
    (emit-task-updated-event! task-id)))

(defn sync-runtime-task-turn!
  [task-turn-id attrs]
  (when task-turn-id
    (db/update-task-turn! task-turn-id attrs)
    (when (contains? #{:completed :failed :cancelled} (:state attrs))
      (emit-turn-close-event! task-turn-id))))

(defn- live-task-run
  [deps task-id]
  (when-let [f (:task-run-entry deps)]
    (when task-id
      (f task-id))))

(defn- task-active?
  [deps task]
  (boolean
   (or (live-task-run deps (:id task))
       (when-let [f (:session-run-entry deps)]
         (let [entry (some-> task :session-id f)]
           (and (or (nil? (:task-id entry))
                    (= (:id task) (:task-id entry)))
                entry))))))

(defn- active-task-turn-id
  [deps task-id]
  (or (some-> (live-task-run deps task-id) :task-turn-id)
      (some-> task-id db/get-task :current-turn-id)
      (some->> (db/task-turns task-id) last :id)))

(defn- record-task-control-turn!
  [task-id operation summary]
  (let [turn-id (db/start-task-turn! task-id
                                     {:operation operation
                                      :state :completed
                                      :summary summary})]
    (record-task-item! turn-id
                       {:type :system-note
                        :status :success
                        :summary summary
                        :data {:operation (name operation)}})
    (db/update-task-turn! turn-id {:state :completed
                                   :summary summary})
    (emit-turn-close-event! turn-id)
    turn-id))

(defn- attach-child-task-to-parent!
  [task child-task-id title]
  (when-let [task-id (:id task)]
    (let [session-id   (:session-id task)
          base-state   (or (when session-id
                             (wm/autonomy-state session-id))
                           (:autonomy-state task)
                           (autonomous/initial-state (:title task)))
          summary      (str "Delegated child task: " title)
          next-state   (autonomous/attach-child-task
                        base-state
                        child-task-id
                        :title title
                        :summary summary
                        :reason "Delegated work to a child task."
                        :progress-status :in-progress)]
      (db/update-task! task-id
                       {:autonomy-state next-state
                        :summary summary})
      (when session-id
        (wm/set-autonomy-state! session-id next-state))
      next-state)))

(defn- sync-runtime-waiting-state!
  [runtime-task state summary]
  (when-let [{:keys [task-id task-turn-id]} @runtime-task]
    (sync-runtime-task-turn! task-turn-id
                             {:state state
                              :summary summary})
    (sync-runtime-task! task-id
                        {:state state
                         :summary summary})
    (set-task-runtime-status! task-id
                              {:state state
                               :phase state
                               :message summary})))

(defn- restore-runtime-running-state!
  [runtime-task summary]
  (when-let [{:keys [task-id task-turn-id]} @runtime-task]
    (sync-runtime-task-turn! task-turn-id
                             {:state :running
                              :summary summary})
    (sync-runtime-task! task-id
                        {:state :running
                         :summary summary})
    (set-task-runtime-status! task-id
                              {:state :running
                               :phase :running
                               :message summary})))

(defn task-runtime-callbacks
  [deps runtime-task]
  {:task-runtime/on-status
   (fn [status]
     (when-let [{:keys [task-id task-turn-id]} @runtime-task]
       (set-task-runtime-status! task-id
                                 {:state (:state status)
                                  :phase (:phase status)
                                  :message (:message status)
                                  :iteration (:iteration status)
                                  :max-iterations (:max-iterations status)
                                  :attempt (:attempt status)
                                  :max-restarts (:max-restarts status)
                                  :failure-phase (:failure-phase status)
                                  :worker-phase (:worker-phase status)
                                  :round (:round status)
                                  :tool-count (:tool-count status)
                                  :tool-id (:tool-id status)
                                  :tool-name (:tool-name status)
                                  :parallel (:parallel status)
                                  :partial-content (:partial-content status)
                                  :timeout-ms (:timeout-ms status)
                                  :grace-ms (:grace-ms status)
                                  :cancel-reason (:cancel-reason status)
                                  :current-focus (:current-focus status)
                                  :progress-status (:progress-status status)
                                  :stack-depth (:stack-depth status)
                                  :agenda (:agenda status)
                                  :stack (:stack status)
                                  :stack-action (:stack-action status)
                                  :intent-focus (:intent-focus status)
                                  :intent-agenda-item (:intent-agenda-item status)
                                  :intent-plan-step (:intent-plan-step status)
                                  :intent-why (:intent-why status)
                                  :intent-tool-name (:intent-tool-name status)
                                  :intent-tool-args-summary (:intent-tool-args-summary status)})
       (record-task-item! task-turn-id
                          {:type :status
                           :status (let [state (:state status)]
                                     (cond
                                       (keyword? state) state
                                       (string? state) (keyword state)
                                       :else nil))
                           :summary (or (:message status)
                                        (some-> (:phase status) name)
                                        "Status update")
                           :data (into {}
                                       (keep (fn [[k v]]
                                               (when (some? v)
                                                 [k (if (keyword? v) (name v) v)])))
                                       status)})))

   :task-runtime/on-input-request
   (fn [{:keys [label mask?]}]
     (let [summary (str "Waiting for input: " label)]
       (sync-runtime-waiting-state! runtime-task :waiting_input summary)
       (when-let [{:keys [task-turn-id]} @runtime-task]
         (record-task-item! task-turn-id
                            {:type :input-request
                             :status :waiting
                             :summary summary
                             :data {:label label
                                    :masked (boolean mask?)}}))))

   :task-runtime/on-input-response
   (fn [{:keys [label mask? provided]}]
     (restore-runtime-running-state! runtime-task
                                     (str "Received input for " label))
     (when-let [{:keys [task-turn-id]} @runtime-task]
       (record-task-item! task-turn-id
                          {:type :system-note
                           :status :success
                           :summary (str "Received input for " label)
                           :data {:kind "input-response"
                                  :label label
                                  :masked (boolean mask?)
                                  :provided (boolean provided)}})))

   :task-runtime/on-approval-request
   (fn [{:keys [tool-id tool-name description arguments policy reason]}]
     (let [tool-label (or tool-name (some-> tool-id name) "tool")
           summary    (str "Waiting for approval for " tool-label)]
       (sync-runtime-waiting-state! runtime-task :waiting_approval summary)
       (when-let [{:keys [task-turn-id]} @runtime-task]
         (record-task-item! task-turn-id
                            {:type :approval-request
                             :status :waiting
                             :tool-id tool-label
                             :summary summary
                             :data (cond-> {:tool-name tool-label}
                                     tool-id (assoc :tool-id (name tool-id))
                                     description (assoc :description description)
                                     arguments (assoc :arguments arguments)
                                     policy (assoc :policy (name policy))
                                     reason (assoc :reason reason))}))))

   :task-runtime/on-approval-decision
   (fn [{:keys [tool-id tool-name approved? policy]}]
     (let [tool-label (or tool-name (some-> tool-id name) "tool")
           summary    (str "Approval "
                           (if approved? "granted" "denied")
                           " for "
                           tool-label)]
       (restore-runtime-running-state! runtime-task summary)
       (when-let [{:keys [task-turn-id]} @runtime-task]
         (record-task-item! task-turn-id
                            {:type :system-note
                             :status (if approved? :success :error)
                             :tool-id tool-label
                             :summary summary
                             :data (cond-> {:kind "approval-decision"
                                            :tool-name tool-label
                                            :approved (boolean approved?)}
                                     tool-id (assoc :tool-id (name tool-id))
                                     policy (assoc :policy (name policy)))}))))

   :task-runtime/on-policy-decision
   (fn [{:keys [tool-id tool-name task decision-type allowed? policy mode reason error
                attempt max-restarts max-attempts max-retry-rounds max-retry-wait-ms
                backoff-ms delay-ms grace-ms failure-type failure-phase worker-phase
                tool-risk? round rounds tool-count max-tool-rounds max-tool-calls-per-round
                timeout-ms
                request-label url status providers workload provider-id service-id limit]}]
     (let [target-label (or request-label
                            (some-> provider-id name)
                            (some-> service-id name)
                            task
                            tool-name
                            (some-> tool-id name)
                            "request")
           summary (str (case decision-type
                          :approval-policy "Approval policy"
                          :execution-policy "Execution policy"
                          :restart-policy "Restart policy"
                          :http-retry-policy "HTTP retry policy"
                          :provider-retry-policy "Provider retry policy"
                          :provider-rate-limit-policy "Provider rate limit policy"
                          :service-rate-limit-policy "Service rate limit policy"
                          :parallel-tool-timeout-policy "Parallel tool timeout policy"
                          :branch-task-timeout-policy "Branch task timeout policy"
                          :tool-round-policy "Tool round policy"
                          :tool-call-policy "Tool call policy"
                          "Policy")
                        " "
                        (if allowed? "allowed" "blocked")
                        " for "
                        target-label)]
       (when-let [{:keys [task-turn-id]} @runtime-task]
         (record-task-item! task-turn-id
                            {:type :system-note
                             :status (if allowed? :success :error)
                             :tool-id target-label
                             :summary summary
                             :data (cond-> {:kind "policy-decision"
                                            :tool-name target-label
                                            :allowed (boolean allowed?)}
                                     tool-id (assoc :tool-id (name tool-id))
                                     decision-type (assoc :decision-type (name decision-type))
                                     policy (assoc :policy (name policy))
                                     request-label (assoc :request-label request-label)
                                     provider-id (assoc :provider-id (name provider-id))
                                     service-id (assoc :service-id (name service-id))
                                     workload (assoc :workload (name workload))
                                     limit (assoc :limit limit)
                                     timeout-ms (assoc :timeout-ms timeout-ms)
                                     task (assoc :task task)
                                     url (assoc :url url)
                                     status (assoc :status-code status)
                                     mode (assoc :mode (name mode))
                                     reason (assoc :reason reason)
                                     attempt (assoc :attempt attempt)
                                     rounds (assoc :rounds rounds)
                                     tool-count (assoc :tool-count tool-count)
                                     max-attempts (assoc :max-attempts max-attempts)
                                     max-tool-rounds (assoc :max-tool-rounds max-tool-rounds)
                                     max-tool-calls-per-round (assoc :max-tool-calls-per-round max-tool-calls-per-round)
                                     max-retry-rounds (assoc :max-retry-rounds max-retry-rounds)
                                     max-retry-wait-ms (assoc :max-retry-wait-ms max-retry-wait-ms)
                                     max-restarts (assoc :max-restarts max-restarts)
                                     delay-ms (assoc :delay-ms delay-ms)
                                     backoff-ms (assoc :backoff-ms backoff-ms)
                                     grace-ms (assoc :grace-ms grace-ms)
                                     failure-type (assoc :failure-type (name failure-type))
                                     failure-phase (assoc :failure-phase (name failure-phase))
                                     worker-phase (assoc :worker-phase (name worker-phase))
                                     tool-risk? (assoc :tool-risk tool-risk?)
                                     (seq providers) (assoc :providers (mapv name providers))
                                     round (assoc :round round)
                                     error (assoc :error error))}))))})

(defn pause-task!
  [deps task-id]
  (if-let [task (db/get-task task-id)]
    (let [session-id (or (some-> (live-task-run deps task-id) :session-id)
                         (:session-id task))]
      (cond
        (nil? session-id)
        {:status :invalid
         :error "task has no session"}

        (task-active? deps task)
        (if ((:cancel-session! deps) session-id "task pause requested")
          {:status :pausing
           :task-id task-id
           :session-id session-id}
          {:status :busy
           :error "task is still processing a request"})

        (= :paused (:state task))
        {:status :already-paused
         :task-id task-id
         :session-id session-id}

        :else
        (do
          (record-task-control-turn! task-id :pause "Task paused by user")
          (db/update-task! task-id
                           {:state :paused
                            :stop-reason :paused
                            :summary "Task paused by user"})
          {:status :paused
           :task-id task-id
           :session-id session-id
           :task (db/get-task task-id)})))
    {:status :not-found
     :error "task not found"}))

(defn stop-task!
  [deps task-id]
  (if-let [task (db/get-task task-id)]
    (let [session-id (or (some-> (live-task-run deps task-id) :session-id)
                         (:session-id task))]
      (cond
        (nil? session-id)
        {:status :invalid
         :error "task has no session"}

        (task-active? deps task)
        (if ((:cancel-session! deps) session-id "task stop requested")
          {:status :stopping
           :task-id task-id
           :session-id session-id}
          {:status :busy
           :error "task is still processing a request"})

        (= :cancelled (:state task))
        {:status :already-stopped
         :task-id task-id
         :session-id session-id}

        :else
        (do
          (record-task-control-turn! task-id :stop "Task stopped by user")
          (db/update-task! task-id
                           {:state :cancelled
                            :stop-reason :stopped
                            :summary "Task stopped by user"
                            :finished-at (java.util.Date.)})
          {:status :stopped
           :task-id task-id
           :session-id session-id
           :task (db/get-task task-id)})))
    {:status :not-found
     :error "task not found"}))

(defn resume-task!
  [deps task-id & {:keys [message]}]
  (if-let [task (db/get-task task-id)]
    (let [session-id (or (some-> (live-task-run deps task-id) :session-id)
                         (:session-id task))
          channel    (or (:channel task) :terminal)
          message*   (or (some-> message str str/trim not-empty)
                         "Continue from the current agenda.")]
      (cond
        (nil? session-id)
        {:status :invalid
         :error "task has no session"}

        (task-active? deps task)
        {:status :already-running
         :task-id task-id
         :session-id session-id}

        (contains? #{:cancelled :failed} (:state task))
        {:status :not-resumable
         :task-id task-id
         :session-id session-id
         :error "task is not resumable"}

        :else
        (if-let [_future
                 (async/submit-background!
                  (str "task-resume:" task-id)
                  #((:process-message deps) session-id
                    message*
                    :channel channel
                    :task-id task-id
                    :runtime-op :resume
                    :persist-message? false))]
          (do
            (db/update-task! task-id
                             {:state :running
                              :stop-reason nil
                              :error nil
                              :finished-at nil
                              :summary "Task resumed"})
            {:status :running
             :task-id task-id
             :session-id session-id})
          {:status :unavailable
           :task-id task-id
           :session-id session-id
           :error "resume worker unavailable"})))
    {:status :not-found
     :error "task not found"}))

(defn interrupt-task!
  [deps task-id]
  (if-let [task (db/get-task task-id)]
    (let [session-id (or (some-> (live-task-run deps task-id) :session-id)
                         (:session-id task))]
      (cond
        (nil? session-id)
        {:status :invalid
         :error "task has no session"}

        (task-active? deps task)
        (if ((:cancel-session! deps) session-id "task interrupt requested")
          {:status :interrupting
           :task-id task-id
           :session-id session-id}
          {:status :busy
           :error "task is still processing a request"})

        (= :paused (:state task))
        {:status :already-paused
         :task-id task-id
         :session-id session-id}

        :else
        (do
          (record-task-control-turn! task-id :interrupt "Task interrupted by user")
          (db/update-task! task-id
                           {:state :paused
                            :stop-reason :interrupted
                            :summary "Task interrupted by user"})
          {:status :paused
           :task-id task-id
           :session-id session-id
           :task (db/get-task task-id)})))
    {:status :not-found
     :error "task not found"}))

(defn steer-task!
  [deps task-id message]
  (if-let [task (db/get-task task-id)]
    (let [live-run              (live-task-run deps task-id)
          session-id            (or (:session-id live-run)
                                    (:session-id task))
          channel               (or (:channel task) :terminal)
          message*              (some-> message str str/trim not-empty)
          interrupted-turn-id   (or (:task-turn-id live-run)
                                    (active-task-turn-id deps task-id))
          submit-steer!
          (fn [interrupting-turn-id]
            (async/submit-background!
             (str "task-steer:" task-id)
             #(do
                (when interrupting-turn-id
                  (let [timeout-ms ((:task-control-wait-ms deps))
                        task-wait-fn (:wait-for-task-idle! deps)
                        idle? (cond
                                task-wait-fn (task-wait-fn task-id timeout-ms)
                                (:wait-for-session-idle! deps)
                                ((:wait-for-session-idle! deps) session-id timeout-ms)
                                :else true)]
                    (when-not idle?
                    (db/update-task! task-id
                                     {:state :paused
                                      :stop-reason :interrupted
                                      :summary "Timed out waiting to apply the new instruction"
                                      :error "steer request timed out waiting for the current turn to stop"})
                    (throw (ex-info "Timed out waiting for the current task turn to stop"
                                    {:type :task-steer-timeout
                                     :task-id task-id
                                     :session-id session-id
                                     :timeout-ms timeout-ms})))))
                ((:process-message deps) session-id
                 message*
                 :channel channel
                 :task-id task-id
                 :runtime-op :steer
                 :interrupting-turn-id interrupting-turn-id))))]
      (cond
        (nil? session-id)
        {:status :invalid
         :error "task has no session"}

        (nil? message*)
        {:status :invalid
         :error "steer message is required"}

        (task-active? deps task)
        (if ((:cancel-session! deps) session-id "task steer requested")
          (if-let [_future (submit-steer! interrupted-turn-id)]
            (do
              (db/update-task! task-id
                               {:state :running
                                :stop-reason nil
                                :error nil
                                :finished-at nil
                                :summary (task-title deps message*)})
              {:status :steering
               :task-id task-id
               :session-id session-id})
            (do
              (db/update-task! task-id
                               {:state :paused
                                :stop-reason :interrupted
                                :summary "Task interrupted by new instruction"})
              {:status :unavailable
               :task-id task-id
               :session-id session-id
               :error "steer worker unavailable"}))
          {:status :busy
           :error "task is still processing a request"})

        :else
        (if-let [_future (submit-steer! nil)]
          (do
            (db/update-task! task-id
                             {:state :running
                              :stop-reason nil
                              :error nil
                              :finished-at nil
                              :summary (task-title deps message*)})
            {:status :steering
             :task-id task-id
             :session-id session-id})
          {:status :unavailable
           :task-id task-id
           :session-id session-id
           :error "steer worker unavailable"})))
    {:status :not-found
     :error "task not found"}))

(defn fork-task!
  [deps task-id message]
  (if-let [task (db/get-task task-id)]
    (let [parent-session-id (:session-id task)
          message*          (some-> message str str/trim not-empty)
          title             (task-title deps message*)]
      (cond
        (nil? parent-session-id)
        {:status :invalid
         :error "task has no session"}

        (nil? message*)
        {:status :invalid
         :error "fork message is required"}

        :else
        (let [child-session-id (db/create-session! :branch
                                                   {:parent-session-id parent-session-id
                                                    :worker? true
                                                    :label title})
              child-task-id    (db/create-task! {:session-id child-session-id
                                                 :parent-id task-id
                                                 :channel :branch
                                                 :type :branch
                                                 :state :running
                                                 :title title
                                                 :summary title
                                                 :started-at (java.util.Date.)})
              _                (attach-child-task-to-parent! task child-task-id title)
              submit-fork!     (fn []
                                 (async/submit-background!
                                  (str "task-fork:" child-task-id)
                                  #(do
                                     ((:register-child-session! deps) parent-session-id child-session-id)
                                     (try
                                       ((:process-message deps) child-session-id
                                        message*
                                        :channel :branch
                                        :task-id child-task-id
                                        :runtime-op :fork
                                        :resource-session-id parent-session-id
                                        :tool-context {:branch-worker? true
                                                       :parent-session-id parent-session-id
                                                       :resource-session-id parent-session-id})
                                       (finally
                                         ((:unregister-child-session! deps) parent-session-id child-session-id)
                                         (try
                                           (db/set-session-active! child-session-id false)
                                           (catch Throwable t
                                             (log/warn t "Failed to deactivate forked task session"
                                                       {:task-id child-task-id
                                                        :session-id child-session-id
                                                        :parent-task-id task-id
                                                        :parent-session-id parent-session-id})))
                                         (try
                                           (wm/clear-wm! child-session-id)
                                           (catch Throwable t
                                             (log/warn t "Failed to clear forked task working memory"
                                                       {:task-id child-task-id
                                                        :session-id child-session-id
                                                        :parent-task-id task-id
                                                        :parent-session-id parent-session-id}))))))))]
          (record-task-control-turn! task-id
                                     :fork
                                     (str "Forked child task: " title))
          (if-let [_future (submit-fork!)]
            {:status :forking
             :task-id child-task-id
             :session-id child-session-id
             :task (db/get-task child-task-id)}
            (do
              (db/update-task! child-task-id
                               {:state :failed
                                :summary "Fork worker unavailable"
                                :error "fork worker unavailable"
                                :finished-at (java.util.Date.)})
              (try
                (db/set-session-active! child-session-id false)
                (catch Throwable t
                  (log/warn t "Failed to deactivate forked task session after worker rejection"
                            {:task-id child-task-id
                             :session-id child-session-id
                             :parent-task-id task-id
                             :parent-session-id parent-session-id})))
              {:status :unavailable
               :task-id child-task-id
               :session-id child-session-id
               :task (db/get-task child-task-id)
               :error "fork worker unavailable"})))))
    {:status :not-found
     :error "task not found"}))
