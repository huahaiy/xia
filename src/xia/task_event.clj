(ns xia.task-event
  "Normalized runtime event projection over persisted task/turn/item records."
  (:require [clojure.string :as str]))

(def ^:private terminal-turn-states
  #{:completed :failed :cancelled})

(def ^:private run-start-operations
  #{:start :resume :steer :fork})

(defn- event-id
  [& parts]
  (str/join ":" (map str parts)))

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

(defn task-updated-event
  [task]
  {:id         (event-id "task" (:id task) "updated"
                         (or (some-> (:updated-at task) .getTime)
                             (some-> (:finished-at task) .getTime)
                             0))
   :type       :task.updated
   :task-id    (:id task)
   :created-at (or (:updated-at task)
                   (:finished-at task)
                   (:created-at task))
   :summary    (or (:summary task) (:title task))
   :data       (cond-> {:state (:state task)}
                 (:channel task) (assoc :channel (:channel task))
                 (:type task) (assoc :task-type (:type task))
                 (:stop-reason task) (assoc :stop-reason (:stop-reason task))
                 (:current-turn-id task) (assoc :current-turn-id (:current-turn-id task))
                 (:error task) (assoc :error (:error task)))})

(defn task-state-event
  [task]
  (when-let [event-type (case (:state task)
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
