(ns xia.task-inspection
  "Compact task-inspection views derived from task state, turns, and items."
  (:require [clojure.string :as str]
            [xia.agent.task-runtime :as task-runtime]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.task-policy :as task-policy]))

(def ^:private detail-output-limit 5)
(def ^:private detail-tool-limit 8)
(def ^:private detail-policy-limit 8)
(def ^:private detail-status-limit 8)
(def ^:private detail-checkpoint-limit 5)
(def ^:private detail-activity-limit 12)

(defn- instant->str*
  [opts value]
  (when-let [f (:instant->str opts)]
    (f value)))

(defn- truncate-text*
  [opts value max-chars]
  (let [text (some-> value str str/trim not-empty)]
    (cond
      (nil? text)
      nil

      (:truncate-text opts)
      ((:truncate-text opts) text max-chars)

      (<= (count text) max-chars)
      text

      :else
      (str (subs text 0 (max 0 (- max-chars 3))) "..."))))

(defn- keyword->str
  [value]
  (cond
    (keyword? value) (name value)
    (some? value) (str value)
    :else nil))

(defn- uuid->str
  [value]
  (when value
    (str value)))

(defn- latest-item
  [items pred]
  (some pred (reverse items)))

(defn- all-task-items
  [task-id]
  (let [turns (db/task-turns task-id)]
    {:turns turns
     :items (->> turns
                 (mapcat #(db/turn-items (:id %)))
                 (sort-by (juxt :created-at :turn-id :index) compare)
                 vec)}))

(defn- ensure-task-history-data
  [task history-data]
  (or history-data
      (all-task-items (:id task))))

(defn- current-tip-body
  [opts autonomy-state]
  (when-let [state (some-> autonomy-state autonomous/normalize-state)]
    (when-let [tip (peek (:stack state))]
      (let [agenda (vec (or (:agenda tip) []))]
        (cond-> {:title (or (:title tip) "Task")
                 :stack_depth (count (:stack state))
                 :compressed_frame_count (count (filter :compressed? (:stack state)))
                 :child_task_frame_count (count (filter #(= :child-task (:kind %)) (:stack state)))
                 :agenda (mapv (fn [{:keys [item status]}]
                                 (cond-> {:item item}
                                   status (assoc :status (keyword->str status))))
                               agenda)}
          (:kind tip) (assoc :kind (keyword->str (:kind tip)))
          (:summary tip) (assoc :summary (truncate-text* opts (:summary tip) 240))
          (:next-step tip) (assoc :next_step (truncate-text* opts (:next-step tip) 200))
          (:progress-status tip) (assoc :progress_status (keyword->str (:progress-status tip))))))))

(defn- stack-frame-summary
  [opts frame]
  (cond-> {:title (:title frame)}
    (:kind frame) (assoc :kind (keyword->str (:kind frame)))
    (:progress-status frame) (assoc :progress_status (keyword->str (:progress-status frame)))
    (:summary frame) (assoc :summary (truncate-text* opts (:summary frame) 160))
    (:next-step frame) (assoc :next_step (truncate-text* opts (:next-step frame) 120))
    (:compressed? frame) (assoc :compressed true)
    (:compressed-count frame) (assoc :compressed_count (:compressed-count frame))
    (:child-task-id frame) (assoc :child_task_id (uuid->str (:child-task-id frame)))))

(defn- stack-summary
  [opts autonomy-state compact?]
  (when-let [state (some-> autonomy-state autonomous/normalize-state)]
    (let [stack* (vec (:stack state))
          root   (first stack*)
          tip    (peek stack*)
          base   {:depth (count stack*)
                  :root_title (:title root)
                  :tip_title (:title tip)
                  :compressed_frame_count (count (filter :compressed? stack*))
                  :child_task_frame_count (count (filter #(= :child-task (:kind %)) stack*))}]
      (if compact?
        base
        (assoc base :frames (mapv #(stack-frame-summary opts %) stack*))))))

(defn- checkpoint-body
  [opts task]
  (when-let [checkpoint (get-in task [:meta :checkpoint])]
    (cond-> {:summary (truncate-text* opts (:summary checkpoint) 240)}
      (:phase checkpoint) (assoc :phase (keyword->str (:phase checkpoint)))
      (:current-focus checkpoint) (assoc :current_focus (:current-focus checkpoint))
      (:next-step checkpoint) (assoc :next_step (truncate-text* opts (:next-step checkpoint) 200))
      (get-in task [:meta :checkpoint-at]) (assoc :at (instant->str* opts (get-in task [:meta :checkpoint-at]))))))

(defn- assistant-output-entry
  [opts item]
  (let [text (or (get-in item [:data :text])
                 (:summary item))]
    (cond-> {:turn_id (uuid->str (:turn-id item))
             :created_at (instant->str* opts (:created-at item))
             :summary (truncate-text* opts (:summary item) 240)}
      (:message-id item) (assoc :message_id (uuid->str (:message-id item)))
      text (assoc :text (truncate-text* opts text 320)))))

(defn- recent-output
  [opts items limit]
  (->> items
       reverse
       (filter #(= :assistant-message (:type %)))
       (take limit)
       (mapv #(assistant-output-entry opts %))))

(defn- tool-activity-detail
  [opts item]
  (or (get-in item [:data :error])
      (get-in item [:data :content])
      (get-in item [:data :summary])
      (get-in item [:data :arguments])
      (some-> (get-in item [:data :result]) pr-str)))

(defn- tool-activity-entry
  [opts item]
  (cond-> {:turn_id (uuid->str (:turn-id item))
           :created_at (instant->str* opts (:created-at item))
           :event (case (:type item)
                    :tool-call "requested"
                    :tool-result "completed"
                    (keyword->str (:type item)))
           :summary (truncate-text* opts (:summary item) 240)}
    (:tool-id item) (assoc :tool_id (:tool-id item))
    (:tool-call-id item) (assoc :tool_call_id (:tool-call-id item))
    (:status item) (assoc :status (keyword->str (:status item)))
    (tool-activity-detail opts item) (assoc :detail (truncate-text* opts (tool-activity-detail opts item) 240))))

(defn- recent-tool-activity
  [opts items limit]
  (->> items
       reverse
       (filter #(contains? #{:tool-call :tool-result} (:type %)))
       (take limit)
       (mapv #(tool-activity-entry opts %))))

(defn- policy-decision-entry
  [opts item]
  (let [data (:data item)
        target (or (:request-label data)
                   (:task data)
                   (:tool-name data)
                   (:tool-id data)
                   (:provider-id data)
                   (:service-id data))]
    (cond-> {:turn_id (uuid->str (:turn-id item))
             :created_at (instant->str* opts (:created-at item))
             :summary (truncate-text* opts (:summary item) 240)}
      (:decision-type data) (assoc :decision_type (:decision-type data))
      (contains? data :allowed) (assoc :allowed (boolean (:allowed data)))
      (:mode data) (assoc :mode (:mode data))
      target (assoc :target target)
      (:reason data) (assoc :reason (truncate-text* opts (:reason data) 200))
      (:error data) (assoc :error (truncate-text* opts (:error data) 200)))))

(defn- recent-policy-decisions
  [opts items limit]
  (->> items
       reverse
       (filter #(and (= :system-note (:type %))
                     (= "policy-decision" (get-in % [:data :kind]))))
       (take limit)
       (mapv #(policy-decision-entry opts %))))

(defn- status-update-entry
  [opts item]
  (let [data (:data item)]
    (cond-> {:turn_id (uuid->str (:turn-id item))
             :created_at (instant->str* opts (:created-at item))
             :summary (truncate-text* opts (:summary item) 240)}
      (:status item) (assoc :status (keyword->str (:status item)))
      (:phase data) (assoc :phase (keyword->str (:phase data)))
      (:state data) (assoc :state (keyword->str (:state data)))
      (:current-focus data) (assoc :current_focus (truncate-text* opts (:current-focus data) 160))
      (:next-step data) (assoc :next_step (truncate-text* opts (:next-step data) 160))
      (:progress-status data) (assoc :progress_status (keyword->str (:progress-status data)))
      (:tool-name data) (assoc :tool_name (:tool-name data))
      (:tool-id data) (assoc :tool_id (:tool-id data))
      (:iteration data) (assoc :iteration (:iteration data))
      (:round data) (assoc :round (:round data)))))

(defn- recent-status-updates
  [opts items limit]
  (->> items
       reverse
       (filter #(= :status (:type %)))
       (take limit)
       (mapv #(status-update-entry opts %))))

(defn- checkpoint-entry
  [opts item]
  (let [data (:data item)]
    (cond-> {:turn_id (uuid->str (:turn-id item))
             :created_at (instant->str* opts (:created-at item))
             :summary (truncate-text* opts (:summary item) 240)}
      (:phase data) (assoc :phase (keyword->str (:phase data)))
      (:current-focus data) (assoc :current_focus (truncate-text* opts (:current-focus data) 160))
      (:next-step data) (assoc :next_step (truncate-text* opts (:next-step data) 160))
      (:progress-status data) (assoc :progress_status (keyword->str (:progress-status data)))
      (:stack-action data) (assoc :stack_action (keyword->str (:stack-action data))))))

(defn- recent-checkpoints
  [opts items limit]
  (->> items
       reverse
       (filter #(= :checkpoint (:type %)))
       (take limit)
       (mapv #(checkpoint-entry opts %))))

(defn- interaction-entry
  [opts kind item]
  (let [data (:data item)]
    (cond-> {:kind kind
             :turn_id (uuid->str (:turn-id item))
             :created_at (instant->str* opts (:created-at item))
             :summary (truncate-text* opts (:summary item) 240)}
      (:status item) (assoc :status (keyword->str (:status item)))
      (:label data) (assoc :label (:label data))
      (contains? data :masked) (assoc :masked (boolean (:masked data)))
      (:tool-name data) (assoc :tool_name (:tool-name data))
      (:tool-id data) (assoc :tool_id (:tool-id data))
      (:policy data) (assoc :policy (:policy data))
      (:description data) (assoc :description (truncate-text* opts (:description data) 200))
      (:reason data) (assoc :reason (truncate-text* opts (:reason data) 200))
      (contains? data :approved) (assoc :approved (boolean (:approved data)))
      (contains? data :provided) (assoc :provided (boolean (:provided data))))))

(defn- note-entry
  [opts item]
  (let [kind (get-in item [:data :kind])]
    (case kind
      "policy-decision" (assoc (policy-decision-entry opts item) :kind "task.policy-decision")
      "input-response" (interaction-entry opts "input.received" item)
      "approval-decision" (interaction-entry opts "approval.decided" item)
      "budget-exhausted" {:kind "task.budget-exhausted"
                          :turn_id (uuid->str (:turn-id item))
                          :created_at (instant->str* opts (:created-at item))
                          :summary (truncate-text* opts (:summary item) 240)}
      {:kind "task.note"
       :turn_id (uuid->str (:turn-id item))
       :created_at (instant->str* opts (:created-at item))
       :summary (truncate-text* opts (:summary item) 240)})))

(defn- activity-entry
  [opts item]
  (case (:type item)
    :assistant-message (assoc (assistant-output-entry opts item) :kind "message.assistant")
    :tool-call (assoc (tool-activity-entry opts item) :kind "tool.requested")
    :tool-result (assoc (tool-activity-entry opts item) :kind "tool.completed")
    :status (assoc (status-update-entry opts item) :kind "task.status")
    :checkpoint (assoc (checkpoint-entry opts item) :kind "task.checkpoint")
    :input-request (interaction-entry opts "input.requested" item)
    :approval-request (interaction-entry opts "approval.requested" item)
    :system-note (note-entry opts item)
    nil))

(defn- recent-activity
  [opts items limit]
  (->> items
       reverse
       (keep #(activity-entry opts %))
       (take limit)
       vec))

(defn- current-state-body
  [opts task runtime current-tip checkpoint]
  (let [task-state (or (:state runtime) (:state task))
        current-focus (or (:current-focus runtime)
                          (:title current-tip)
                          (:title task))
        next-step (or (:next-step runtime)
                      (:next_step current-tip)
                      (:next-step checkpoint))
        boundary-summary (some-> (task-runtime/task-boundary-summary task) :summary)
        resume-hint      (task-runtime/task-resume-hint task)
        progress-status (or (some-> (:progress-status runtime) keyword->str)
                            (:progress_status current-tip)
                            (some-> (:progress-status checkpoint) keyword->str))]
    (cond-> {:task_state (keyword->str task-state)}
      (:phase runtime) (assoc :phase (keyword->str (:phase runtime)))
      (:message runtime) (assoc :message (truncate-text* opts (:message runtime) 240))
      current-focus (assoc :current_focus (truncate-text* opts current-focus 180))
      next-step (assoc :next_step (truncate-text* opts next-step 180))
      boundary-summary (assoc :boundary_summary (truncate-text* opts boundary-summary 240))
      resume-hint (assoc :resume_hint (truncate-text* opts resume-hint 240))
      progress-status (assoc :progress_status progress-status)
      (:stack_depth current-tip) (assoc :stack_depth (:stack_depth current-tip))
      (:compressed_frame_count current-tip) (assoc :compressed_frame_count (:compressed_frame_count current-tip))
      (:child_task_frame_count current-tip) (assoc :child_task_frame_count (:child_task_frame_count current-tip)))))

(defn- waiting-input-attention
  [opts items task runtime]
  (when-let [request (latest-item items #(when (= :input-request (:type %)) %))]
    (cond-> {:kind "waiting_input"
             :summary (truncate-text* opts (or (:summary request)
                                               (:message runtime)
                                               (:summary task))
                                     240)}
      (get-in request [:data :label]) (assoc :label (get-in request [:data :label]))
      (contains? (:data request) :masked) (assoc :masked (boolean (get-in request [:data :masked])))
      (:created-at request) (assoc :requested_at (instant->str* opts (:created-at request))))))

(defn- waiting-approval-attention
  [opts items task runtime]
  (when-let [request (latest-item items #(when (= :approval-request (:type %)) %))]
    (let [data (:data request)]
      (cond-> {:kind "waiting_approval"
               :summary (truncate-text* opts (or (:summary request)
                                                 (:message runtime)
                                                 (:summary task))
                                       240)}
        (:tool-name data) (assoc :tool_name (:tool-name data))
        (:tool-id data) (assoc :tool_id (:tool-id data))
        (:policy data) (assoc :policy (:policy data))
        (:description data) (assoc :description (truncate-text* opts (:description data) 200))
        (:reason data) (assoc :reason (truncate-text* opts (:reason data) 200))
        (:created-at request) (assoc :requested_at (instant->str* opts (:created-at request)))))))

(defn- attention-body
  [opts task runtime items budget]
  (let [task-state  (or (:state runtime) (:state task))
        resume-hint (task-runtime/task-resume-hint task)]
    (cond
      (= :waiting_input task-state)
      (cond-> (waiting-input-attention opts items task runtime)
        resume-hint (assoc :resume_hint (truncate-text* opts resume-hint 240)))

      (= :waiting_approval task-state)
      (cond-> (waiting-approval-attention opts items task runtime)
        resume-hint (assoc :resume_hint (truncate-text* opts resume-hint 240)))

      (= :failed task-state)
      (cond-> {:kind "failed"
               :summary (truncate-text* opts (or (:error task)
                                                 (:summary task)
                                                 "Task failed")
                                       240)}
        (:error task) (assoc :error (truncate-text* opts (:error task) 240)))

      (= :paused task-state)
      (cond-> {:kind "paused"
               :summary (truncate-text* opts (or (:summary task)
                                                 "Task paused")
                                       240)}
        resume-hint (assoc :resume_hint (truncate-text* opts resume-hint 240))
        (:stop-reason task) (assoc :stop_reason (keyword->str (:stop-reason task))))

      (= :resumable task-state)
      (cond-> {:kind "resumable"
               :summary (truncate-text* opts (or (:summary task)
                                                 "Task can be resumed")
                                       240)}
        resume-hint (assoc :resume_hint (truncate-text* opts resume-hint 240)))

      :else
      (when-let [budget-status (:status budget)]
        {:kind "budget_exhausted"
         :summary (:summary budget-status)}))))

(defn- counts-body
  [turns items]
  {:turn_count (count turns)
   :item_count (count items)
   :assistant_message_count (count (filter #(= :assistant-message (:type %)) items))
   :tool_call_count (count (filter #(= :tool-call (:type %)) items))
   :tool_result_count (count (filter #(= :tool-result (:type %)) items))
   :policy_decision_count (count (filter #(and (= :system-note (:type %))
                                               (= "policy-decision" (get-in % [:data :kind])))
                                         items))
   :checkpoint_count (count (filter #(= :checkpoint (:type %)) items))})

(defn- budget-summary
  [budget]
  (str (:llm-call-count budget 0)
       " calls, "
       (:total-tokens budget 0)
       " tokens, "
       (task-policy/format-duration-ms (:llm-total-duration-ms budget 0))
       " runtime"))

(defn- budget-body
  [task]
  (when-let [persisted (get-in task [:meta :llm-budget])]
    (let [budget-state (atom (task-policy/restore-task-llm-budget
                              (:id task)
                              (:channel task)
                              (or (:started-at task) (:created-at task))
                              persisted))
          status       (task-policy/task-llm-budget-status budget-state)]
      (cond-> {:summary (budget-summary @budget-state)
               :llm_call_count (:llm-call-count @budget-state)
               :total_tokens (:total-tokens @budget-state)
               :prompt_tokens (:prompt-tokens @budget-state)
               :completion_tokens (:completion-tokens @budget-state)
               :llm_total_duration_ms (:llm-total-duration-ms @budget-state)
               :llm_error_count (:llm-error-count @budget-state)
               :limits {:max_llm_calls (:max-llm-calls @budget-state)
                        :max_total_tokens (:max-total-tokens @budget-state)
                        :max_llm_duration_ms (:max-llm-duration-ms @budget-state)}}
        (:last-llm-duration-ms @budget-state)
        (assoc :last_llm_duration_ms (:last-llm-duration-ms @budget-state))

        (:last-llm-error @budget-state)
        (assoc :last_llm_error (:last-llm-error @budget-state))

        status
        (assoc :status {:kind (keyword->str (:kind status))
                        :summary (task-policy/task-llm-budget-summary status)})))))

(defn task-inspection
  ([opts task autonomy-state]
   (task-inspection opts task autonomy-state false))
  ([opts task autonomy-state compact?]
   (task-inspection opts task autonomy-state compact? nil))
  ([opts task autonomy-state compact? history-data]
   (let [{:keys [turns items]} (ensure-task-history-data task history-data)
         runtime               (get-in task [:meta :runtime])
         checkpoint            (get-in task [:meta :checkpoint])
         output-limit          (if compact? 1 detail-output-limit)
         tool-limit            (if compact? 1 detail-tool-limit)
         policy-limit          (if compact? 1 detail-policy-limit)
         status-limit          (if compact? 1 detail-status-limit)
         checkpoint-limit      (if compact? 1 detail-checkpoint-limit)
         activity-limit        (if compact? 1 detail-activity-limit)
         current-tip           (current-tip-body opts autonomy-state)
         budget                (budget-body task)
         output                (recent-output opts items output-limit)
         tool-activity         (recent-tool-activity opts items tool-limit)
         policy-decisions      (recent-policy-decisions opts items policy-limit)
         status-updates        (recent-status-updates opts items status-limit)
         checkpoints           (recent-checkpoints opts items checkpoint-limit)
         activity              (recent-activity opts items activity-limit)
         base                  {:current_tip current-tip
                                :stack_summary (stack-summary opts autonomy-state compact?)
                                :last_checkpoint (checkpoint-body opts task)
                                :current_state (current-state-body opts task runtime current-tip checkpoint)
                                :attention (attention-body opts task runtime items budget)
                                :budget budget
                                :counts (counts-body turns items)}]
     (if compact?
       (cond-> base
         (first output) (assoc :last_output (first output))
         (first tool-activity) (assoc :last_tool_activity (first tool-activity))
         (first policy-decisions) (assoc :last_policy_decision (first policy-decisions))
         (first status-updates) (assoc :last_status_update (first status-updates))
         (first checkpoints) (assoc :last_checkpoint_event (first checkpoints))
         (first activity) (assoc :last_activity (first activity)))
       (assoc base
              :recent_output output
              :recent_tool_activity tool-activity
              :recent_policy_decisions policy-decisions
              :recent_status_updates status-updates
              :recent_checkpoints checkpoints
              :recent_activity activity)))))
