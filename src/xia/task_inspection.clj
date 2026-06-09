(ns xia.task-inspection
  "Compact task-inspection views derived from task state, turns, and items."
  (:require [clojure.string :as str]
            [xia.agent.task-runtime :as task-runtime]
            [xia.autonomous :as autonomous]
            [xia.constraints :as constraints]
            [xia.db :as db]
            [xia.limits :as limits]))

(def ^:private detail-output-limit 5)
(def ^:private detail-tool-limit 8)
(def ^:private detail-policy-limit 8)
(def ^:private detail-status-limit 8)
(def ^:private detail-checkpoint-limit 5)
(def ^:private detail-activity-limit 12)

(declare truncate-envelope-value)

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

(defn- executor-details-body
  [current-tip stack-summary]
  (when (or current-tip stack-summary)
    {:autonomous (cond-> {}
                   current-tip (assoc :current_tip current-tip)
                   stack-summary (assoc :stack_summary stack-summary))}))

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

(defn- task-step-entry
  [opts item]
  (let [data (:data item)]
    (cond-> {:turn_id (uuid->str (:turn-id item))
             :created_at (instant->str* opts (:created-at item))
             :summary (truncate-text* opts (:summary item) 240)}
      (:step-id data) (assoc :step_id (:step-id data))
      (:step-kind data) (assoc :step_kind (:step-kind data))
      (:status item) (assoc :status (keyword->str (:status item)))
      (:status data) (assoc :task_status (:status data))
      (contains? data :output) (assoc :output (truncate-envelope-value opts (:output data)))
      (:error data) (assoc :error (truncate-text* opts (:error data) 240)))))

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
    :task-step (assoc (task-step-entry opts item) :kind "task.step")
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
  [opts task runtime current-tip checkpoint task-spec-progress]
  (let [task-state (or (:state runtime) (:state task))
        current-focus (or (:current_focus task-spec-progress)
                          (:current-focus runtime)
                          (:title current-tip)
                          (:title task))
        next-step (or (:next_step task-spec-progress)
                      (:next-step runtime)
                      (:next_step current-tip)
                      (:next-step checkpoint))
        boundary-summary (some-> (task-runtime/task-boundary-summary task) :summary)
        resume-hint      (task-runtime/task-resume-hint task)
        progress-status (or (:progress_status task-spec-progress)
                            (some-> (:progress-status runtime) keyword->str)
                            (:progress_status current-tip)
                            (some-> (:progress-status checkpoint) keyword->str))
        spec-fields     (select-keys task-spec-progress
                                      [:progress_source
                                       :task_spec_status
                                       :current_step_id
                                       :current_step_kind
                                       :step_count
                                       :completed_count
                                       :failed_count
                                       :pause_reason
                                       :waiting_for])]
    (cond-> (merge {:task_state (keyword->str task-state)}
                   spec-fields)
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
      (limits/format-duration-ms (:llm-total-duration-ms budget 0))
      " runtime"))

(defn- budget-body
  [task]
  (when-let [persisted (get-in task [:meta :llm-budget])]
    (let [budget-state (atom (limits/restore-task-budget
                              (:id task)
                              (:channel task)
                              (or (:started-at task) (:created-at task))
                              persisted))
          status       (limits/budget-status budget-state)]
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
                        :summary (limits/budget-summary status)})))))

(def ^:private completed-step-statuses
  #{:success :skipped})

(def ^:private active-step-statuses
  #{:running :paused})

(def ^:private pending-step-statuses
  #{:pending :ready})

(defn- task-spec-status->progress-status
  [status]
  (case status
    :ready "pending"
    :running "in-progress"
    :paused "paused"
    :failed "failed"
    :completed "completed"
    (keyword->str status)))

(defn- ordered-task-spec-steps
  [task task-spec]
  (let [step-map (:steps task-spec)
        spec     (get-in task [:contract :spec])
        steps    (if-let [spec-steps (seq (:steps spec))]
                   (mapv (fn [{:keys [id kind] :as spec-step}]
                           (assoc (merge {:id id
                                          :kind kind
                                          :status :pending}
                                         (get step-map id))
                                  ::definition spec-step))
                         spec-steps)
                   (mapv #(assoc % ::definition nil)
                         (vals step-map)))]
    steps))

(defn- step-id->str
  [step-id]
  (some-> step-id keyword->str))

(defn- task-spec-step-title
  [step]
  (let [definition (::definition step)
        tool-id    (or (:tool definition)
                       (:tool-id definition)
                       (:tool_id definition))]
    (or (:summary step)
        (:summary definition)
        (:title definition)
        (when tool-id
          (str "Run " (keyword->str tool-id)))
        (:prompt definition)
        (when-let [step-id (step-id->str (:id step))]
          (str "Step " step-id)))))

(defn- task-spec-current-step
  [task-spec steps]
  (let [current-id (:current-step-id task-spec)]
    (or (some #(when (= current-id (:id %)) %) steps)
        (some #(when (contains? active-step-statuses (:status %)) %) steps)
        (some #(when (= :failed (:status %)) %) (reverse steps))
        (some #(when (contains? pending-step-statuses (:status %)) %) steps)
        (last steps))))

(defn- task-spec-next-step
  [current-step steps]
  (some #(when (and (not= (:id current-step) (:id %))
                    (contains? pending-step-statuses (:status %)))
           %)
        steps))

(defn- task-spec-progress-body
  [opts task task-spec steps]
  (when task-spec
    (let [current-step    (task-spec-current-step task-spec steps)
          next-step       (task-spec-next-step current-step steps)
          current-title   (when-let [title (some-> current-step
                                                   task-spec-step-title)]
                            (truncate-text* opts title 180))
          next-title      (when-let [title (some-> next-step
                                                   task-spec-step-title)]
                            (truncate-text* opts title 180))
          current-status  (:status current-step)
          status          (:status task-spec)]
      (cond-> {:progress_source "task_spec"
               :task_spec_status (keyword->str status)
               :progress_status (or (task-spec-status->progress-status status)
                                    (some-> current-status keyword->str))
               :step_count (count steps)
               :completed_count (count (filter #(contains? completed-step-statuses
                                                           (:status %))
                                               steps))
               :failed_count (count (filter #(= :failed (:status %)) steps))}
        current-step (assoc :current_step_id (step-id->str (:id current-step)))
        (:kind current-step) (assoc :current_step_kind (keyword->str (:kind current-step)))
        current-title (assoc :current_focus current-title)
        next-title (assoc :next_step next-title)
        (:pause-reason task-spec) (assoc :pause_reason (keyword->str (:pause-reason task-spec)))
        (:waiting-for task-spec) (assoc :waiting_for (keyword->str (:waiting-for task-spec)))))))

(defn- task-spec-step-summary
  [step]
  (when (map? step)
    (cond-> {:id (some-> (:id step) name)
             :kind (keyword->str (:kind step))
             :status (keyword->str (:status step))}
      (:summary step) (assoc :summary (:summary step))
      (:error step) (assoc :error (:error step)))))

(defn- task-spec-body
  [task steps]
  (when-let [task-spec (get-in task [:meta :task-spec])]
    (let [steps (or steps (ordered-task-spec-steps task task-spec))]
      (cond-> {:status (keyword->str (:status task-spec))
               :step_count (count steps)
               :completed_count (count (filter #(contains? #{:success :skipped}
                                                            (:status %))
                                                steps))
               :failed_count (count (filter #(= :failed (:status %)) steps))}
        (:current-step-id task-spec) (assoc :current_step_id (name (:current-step-id task-spec)))
        (:pause-reason task-spec) (assoc :pause_reason (keyword->str (:pause-reason task-spec)))
        (seq steps) (assoc :steps (mapv task-spec-step-summary steps))))))

(defn- truncate-envelope-value
  [opts value]
  (cond
    (string? value)
    (truncate-text* opts value 480)

    (map? value)
    (into (empty value)
          (keep (fn [[k v]]
                  (when-let [v* (truncate-envelope-value opts v)]
                    [k v*])))
          value)

    (vector? value)
    (mapv #(truncate-envelope-value opts %) value)

    (sequential? value)
    (mapv #(truncate-envelope-value opts %) value)

    :else
    value))

(defn- operating-envelope-body
  [opts task]
  (let [envelope (constraints/operating-envelope {:session-id (:session-id task)
                                                  :task-id (:id task)})
        sources* (:sources envelope)]
    {:precedence (mapv name (:precedence envelope))
     :resolved   (reduce (fn [acc key-name]
                            (update acc key-name #(some-> % str)))
                          (:resolved sources*)
                          [:session-id :task-id :user-profile-id])
     :effective  (truncate-envelope-value opts (:effective envelope))}))

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
         stack-summary*        (stack-summary opts autonomy-state compact?)
         budget                (budget-body task)
         task-spec-state       (get-in task [:meta :task-spec])
         task-spec-steps       (when task-spec-state
                                 (ordered-task-spec-steps task task-spec-state))
         task-spec-progress    (task-spec-progress-body opts
                                                        task
                                                        task-spec-state
                                                        task-spec-steps)
         task-spec             (task-spec-body task task-spec-steps)
         output                (recent-output opts items output-limit)
         tool-activity         (recent-tool-activity opts items tool-limit)
         policy-decisions      (recent-policy-decisions opts items policy-limit)
         status-updates        (recent-status-updates opts items status-limit)
         checkpoints           (recent-checkpoints opts items checkpoint-limit)
         activity              (recent-activity opts items activity-limit)
         operating-envelope    (operating-envelope-body opts task)
         executor-details      (executor-details-body current-tip stack-summary*)
         base                  (cond-> {:current_tip current-tip
                                         :stack_summary stack-summary*
                                         :executor_details executor-details
                                         :last_checkpoint (checkpoint-body opts task)
                                         :current_state (current-state-body opts
                                                                            task
                                                                            runtime
                                                                            current-tip
                                                                            checkpoint
                                                                            task-spec-progress)
                                         :attention (attention-body opts task runtime items budget)
                                         :budget budget
                                         :operating_envelope operating-envelope
                                         :counts (counts-body turns items)}
                                 task-spec (assoc :task_spec task-spec))]
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
