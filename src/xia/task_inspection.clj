(ns xia.task-inspection
  "Compact task-inspection views derived from task state, turns, and items."
  (:require [clojure.string :as str]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.task-policy :as task-policy]))

(def ^:private detail-output-limit 5)
(def ^:private detail-tool-limit 8)

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

(defn- all-task-items
  [task-id]
  (let [turns (db/task-turns task-id)]
    {:turns turns
     :items (->> turns
                 (mapcat #(db/turn-items (:id %)))
                 (sort-by :created-at compare)
                 vec)}))

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

(defn- counts-body
  [turns items]
  {:turn_count (count turns)
   :item_count (count items)
   :assistant_message_count (count (filter #(= :assistant-message (:type %)) items))
   :tool_call_count (count (filter #(= :tool-call (:type %)) items))
   :tool_result_count (count (filter #(= :tool-result (:type %)) items))})

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
   (let [{:keys [turns items]} (all-task-items (:id task))
         output-limit          (if compact? 1 detail-output-limit)
         tool-limit            (if compact? 1 detail-tool-limit)
         output                (recent-output opts items output-limit)
         tool-activity         (recent-tool-activity opts items tool-limit)
         base                  {:current_tip (current-tip-body opts autonomy-state)
                                :last_checkpoint (checkpoint-body opts task)
                                :budget (budget-body task)
                                :counts (counts-body turns items)}]
     (if compact?
       (cond-> base
         (first output) (assoc :last_output (first output))
         (first tool-activity) (assoc :last_tool_activity (first tool-activity)))
       (assoc base
              :recent_output output
              :recent_tool_activity tool-activity)))))
