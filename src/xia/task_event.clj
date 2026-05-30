(ns xia.task-event
  "Canonical task runtime events and channel projections."
  (:require [clojure.string :as str]))

(def ^:private terminal-turn-states
  #{:completed :failed :cancelled})

(def ^:private live-task-states
  #{:running :waiting_input :waiting_approval})

(def ^:private terminal-status-states
  #{:completed :done :error :cancelled})

(def ^:private run-start-operations
  #{:start :resume :steer :fork})

(def ^:private runtime-status-key-aliases
  {:partial_content :partial-content
   :tool_id :tool-id
   :tool_name :tool-name
   :max_iterations :max-iterations
   :current_focus :current-focus
   :progress_status :progress-status
   :intent_focus :intent-focus
   :intent_agenda_item :intent-agenda-item
   :intent_plan_step :intent-plan-step
   :intent_why :intent-why
   :intent_tool_name :intent-tool-name
   :intent_tool_args_summary :intent-tool-args-summary
   :stack_depth :stack-depth
   :tool_count :tool-count
   :updated_at :updated-at})

(defn- event-id
  [& parts]
  (str/join ":" (map str parts)))

(defn- event-keyword
  [value]
  (cond
    (keyword? value) value
    (string? value) (keyword value)
    :else nil))

(defn- runtime-status-value
  [value]
  (cond
    (= value :done)
    :completed

    (and (string? value) (#{"running" "completed" "done" "error" "cancelled"} value))
    (if (= "done" value)
      :completed
      (keyword value))

    (and (string? value) (#{"understanding" "working-memory" "planning" "llm" "tool-plan"
                            "tool" "approval" "finalizing" "observing" "updating"
                            "restarting" "paused" "cancelled" "error"} value))
    (keyword value)

    :else value))

(defn- update-present
  [m k f]
  (if (contains? m k)
    (update m k f)
    m))

(defn normalize-runtime-status
  "Normalize status maps to Xia's canonical runtime status shape.
   Accepts both kebab-case and transport-style snake_case keys."
  [status]
  (when (map? status)
    (-> (reduce-kv (fn [m k v]
                     (assoc m (get runtime-status-key-aliases k k) v))
                   {}
                   status)
        (update-present :state runtime-status-value)
        (update-present :phase runtime-status-value)
        (update-present :tool-id runtime-status-value))))

(defn terminal-status?
  [status]
  (contains? terminal-status-states
             (:state (normalize-runtime-status status))))

(defn runtime-event
  "Normalize a live runtime event emitted by prompt/task-runtime."
  [event]
  (when (map? event)
    (let [event* (cond-> event
                   (:type event) (update :type event-keyword))]
      (if (= :task.status (:type event*))
        (update event* :data normalize-runtime-status)
        event*))))

(defn event-runtime-status
  "Project a canonical task.status event into a runtime status map."
  [event]
  (let [event* (runtime-event event)
        data   (some-> (:data event*) normalize-runtime-status)]
    (when (map? data)
      (assoc data
             :updated-at (or (:received-at event*)
                             (:created-at event*))
             :message (or (:message data) (:summary event*))))))

(defn task-runtime-status
  "Return the best current runtime status for a live task.
   `latest-status-event` should be the newest live :task.status event for task."
  [task latest-status-event]
  (when (contains? live-task-states (:state task))
    (or (event-runtime-status latest-status-event)
        (normalize-runtime-status (get-in task [:meta :runtime])))))

(defn assistant-message-text
  [event]
  (let [event* (runtime-event event)]
    (when (= :message.assistant (:type event*))
      (or (some-> (get-in event* [:data :text]) str not-empty)
          (some-> (:summary event*) str not-empty)))))

(defn terminal-status-projection
  [event fallback-session-id]
  (let [event* (runtime-event event)]
    (when (= :task.status (:type event*))
      (let [status (event-runtime-status event*)]
        (cond-> {:session-id (or (:session-id event*) fallback-session-id)
                 :state (:state status)
                 :phase (:phase status)
                 :message (:message status)
                 :partial-content (:partial-content status)}
          (:updated-at status) (assoc :updated-at (:updated-at status)))))))

(defn- name-value
  [value]
  (cond
    (keyword? value) (name value)
    (some? value) value
    :else nil))

(defn runtime-status->wire-body
  [status & {:keys [instant->str]}]
  (when-let [status* (normalize-runtime-status status)]
    {:state      (some-> (:state status*) name)
     :phase      (some-> (:phase status*) name)
     :message    (:message status*)
     :error      (:error status*)
     :finalization_step (some-> (:finalization-step status*) name)
     :partial_content (:partial-content status*)
     :tool_id    (some-> (:tool-id status*) name-value)
     :tool_name  (:tool-name status*)
     :iteration  (:iteration status*)
     :max_iterations (:max-iterations status*)
     :current_focus (:current-focus status*)
     :progress_status (:progress-status status*)
     :intent_focus (:intent-focus status*)
     :intent_agenda_item (:intent-agenda-item status*)
     :intent_plan_step (:intent-plan-step status*)
     :intent_why (:intent-why status*)
     :intent_tool_name (:intent-tool-name status*)
     :intent_tool_args_summary (:intent-tool-args-summary status*)
     :stack_depth (:stack-depth status*)
     :agenda     (:agenda status*)
     :stack      (:stack status*)
     :round      (:round status*)
     :tool_count (:tool-count status*)
     :updated_at (if instant->str
                   (instant->str (:updated-at status*))
                   (:updated-at status*))}))

(defn event->wire-body
  [event & {:keys [instant->str]}]
  (let [event* (runtime-event event)
        instant->str* (or instant->str identity)]
    (cond-> {:id         (:id event*)
             :index      (:index event*)
             :type       (some-> (:type event*) name)
             :task_id    (some-> (:task-id event*) str)
             :created_at (instant->str* (:created-at event*))}
      (:stream-index event*) (assoc :stream_index (:stream-index event*))
      (:received-at event*) (assoc :received_at (instant->str* (:received-at event*)))
      (:turn-id event*) (assoc :turn_id (str (:turn-id event*)))
      (:item-id event*) (assoc :item_id (str (:item-id event*)))
      (:summary event*) (assoc :summary (:summary event*))
      (:status event*) (assoc :status (name (:status event*)))
      (:role event*) (assoc :role (name (:role event*)))
      (:tool-id event*) (assoc :tool_id (:tool-id event*))
      (:tool-call-id event*) (assoc :tool_call_id (:tool-call-id event*))
      (:llm-call-id event*) (assoc :llm_call_id (str (:llm-call-id event*)))
      (:message-id event*) (assoc :message_id (str (:message-id event*)))
      (:data event*) (assoc :data (:data event*)))))

(defn- item-event-type
  [item]
  (case (:type item)
    :user-message :message.user
    :assistant-message :message.assistant
    :tool-call :tool.requested
    :tool-result :tool.completed
    :input-request :input.requested
    :approval-request :approval.requested
    :status :task.status
    :checkpoint :task.checkpoint
    :system-note (case (get-in item [:data :kind])
                   "input-response" :input.received
                   "approval-decision" :approval.decided
                   "budget-exhausted" :task.budget-exhausted
                   "policy-decision" :task.policy-decision
                   :task.note)
    (keyword (str "item." (name (:type item))))))

(defn turn-open-event
  [task turn]
  (when (contains? run-start-operations (:operation turn))
    {:id         (event-id "turn" (:id turn) "started")
     :type       (case (:operation turn)
                   :resume :task.resumed
                   :steer :task.steered
                   :fork :task.forked
                   :turn.started)
     :task-id    (:id task)
     :turn-id    (:id turn)
     :created-at (:created-at turn)
     :summary    (:summary turn)
     :data       (cond-> {:operation (:operation turn)
                          :state (:state turn)}
                   (:input turn) (assoc :input (:input turn))
                   (:interrupting-turn-id turn) (assoc :interrupting-turn-id (:interrupting-turn-id turn)))}))

(defn turn-close-event
  [task turn]
  (when (and (:finished-at turn)
             (contains? terminal-turn-states (:state turn)))
    {:id         (event-id "turn" (:id turn) (name (:state turn)))
     :type       (case (:operation turn)
                   :pause :task.paused
                   :interrupt :task.interrupted
                   :stop :task.stopped
                   (case (:state turn)
                     :completed :turn.completed
                     :failed :turn.failed
                     :cancelled :turn.cancelled))
     :task-id    (:id task)
     :turn-id    (:id turn)
     :created-at (:finished-at turn)
     :summary    (:summary turn)
     :data       (cond-> {:operation (:operation turn)
                          :state (:state turn)}
                   (:error turn) (assoc :error (:error turn)))}))

(defn item-event
  [task turn item]
  {:id         (event-id "item" (:id item))
   :type       (item-event-type item)
   :task-id    (:id task)
   :turn-id    (:id turn)
   :item-id    (:id item)
   :created-at (:created-at item)
   :summary    (:summary item)
   :status     (:status item)
   :role       (:role item)
   :tool-id    (:tool-id item)
   :tool-call-id (:tool-call-id item)
   :llm-call-id  (:llm-call-id item)
   :message-id   (:message-id item)
   :data       (:data item)})

(defn task-started-event
  [task]
  {:id         (event-id "task" (:id task) "started")
   :type       :task.started
   :task-id    (:id task)
   :created-at (:created-at task)
   :summary    (:title task)
   :data       {:channel (:channel task)
                :task-type (:type task)}})

(defn task-state-event
  [task]
  (when-let [event-type (case (:state task)
                          :resumable :task.resumable
                          :completed :task.completed
                          :failed :task.failed
                          :cancelled :task.cancelled
                          nil)]
    {:id         (event-id "task" (:id task) (name event-type)
                           (or (some-> (:finished-at task) .getTime)
                               (some-> (:updated-at task) .getTime)
                               0))
     :type       event-type
     :task-id    (:id task)
     :created-at (or (:finished-at task)
                     (:updated-at task)
                     (:created-at task))
     :summary    (or (:summary task) (:title task))
     :data       (cond-> {:state (:state task)}
                   (:channel task) (assoc :channel (:channel task))
                   (:type task) (assoc :task-type (:type task))
                   (:stop-reason task) (assoc :stop-reason (:stop-reason task))
                   (:current-turn-id task) (assoc :current-turn-id (:current-turn-id task))
                   (:error task) (assoc :error (:error task)))}))

(defn task-events
  [task turns turn-items]
  (let [base-events (concat
                     [(task-started-event task)]
                     (mapcat (fn [turn]
                               (let [items (get turn-items (:id turn) [])]
                                 (concat
                                  (when-let [event (turn-open-event task turn)]
                                    [event])
                                  (map #(item-event task turn %) items)
                                  (when-let [event (turn-close-event task turn)]
                                    [event]))))
                             turns)
                     (when-let [event (task-state-event task)]
                       [event]))]
    (->> base-events
         (map-indexed (fn [idx event]
                        (assoc event :index (inc idx))))
         vec)))
