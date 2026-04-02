(ns xia.channel.http.session
  "Session/chat/history/LLM-call HTTP handlers."
  (:require [charred.api :as json]
            [org.httpkit.server :as http]
            [taoensso.timbre :as log]
            [xia.agent :as agent]
            [xia.agent.task-runtime :as task-runtime]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.schedule :as schedule]
            [xia.task-event :as task-event]
            [xia.task-inspection :as task-inspection]
            [xia.working-memory :as wm]))

(def ^:private history-session-channels #{:http :websocket :terminal :slack :telegram :imessage})

(defn- json-response*
  [deps status body]
  ((:json-response deps) status body))

(defn- exception-response*
  [deps throwable]
  ((:exception-response deps) throwable))

(defn- instant->str*
  [deps value]
  ((:instant->str deps) value))

(defn- touch-rest-session!*
  [deps session-id]
  ((:touch-rest-session! deps) session-id))

(defn- parse-session-id*
  [deps session-id]
  ((:parse-session-id deps) session-id))

(defn- read-body*
  [deps req]
  ((:read-body deps) req))

(defn- session-accessible?*
  [deps session-id expected-channel]
  ((:session-accessible? deps) session-id expected-channel))

(defn- session-active?*
  [deps session-id]
  ((:session-active? deps) session-id))

(defn- maybe-resume-http-session!*
  [deps session-id expected-channel]
  ((:maybe-resume-http-session! deps) session-id expected-channel))

(defn- cancel-rest-session-finalizer!*
  [deps session-id]
  ((:cancel-rest-session-finalizer! deps) session-id))

(defn- approval->body*
  [deps approval]
  ((:approval->body deps) approval))

(defn- prompt->body*
  [deps prompt]
  ((:prompt->body deps) prompt))

(defn- session-busy?*
  [deps session-id]
  ((:session-busy? deps) session-id))

(defn- finalize-rest-session!*
  [deps session-id reason]
  ((:finalize-rest-session! deps) session-id reason))

(defn- parse-keyword-id*
  [deps value field-name]
  ((:parse-keyword-id deps) value field-name))

(defn- parse-query-string*
  [deps query-string]
  ((:parse-query-string deps) query-string))

(defn- register-task-runtime-stream-subscriber!*
  [deps task-id subscriber-id callback]
  ((:register-task-runtime-stream-subscriber! deps) task-id subscriber-id callback))

(defn- unregister-task-runtime-stream-subscriber!*
  [deps task-id subscriber-id]
  ((:unregister-task-runtime-stream-subscriber! deps) task-id subscriber-id))

(defn- date->millis*
  [deps value]
  ((:date->millis deps) value))

(defn- truncate-text*
  [deps value limit]
  ((:truncate-text deps) value limit))

(defn- throwable-message*
  [deps throwable]
  ((:throwable-message deps) throwable))

(defn- session-statuses-atom
  [deps]
  (:session-statuses-atom deps))

(defn- task-runtime-events-atom
  [deps]
  (:task-runtime-events-atom deps))

(defn- live-task?
  [task]
  (contains? #{:running :waiting_input :waiting_approval} (:state task)))

(def ^:private live-task-states
  #{:running :waiting_input :waiting_approval})

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

(defn- normalize-runtime-status-map
  [status]
  (reduce-kv (fn [m k v]
               (assoc m (get runtime-status-key-aliases k k) v))
             {}
             status))

(defn- runtime-status-value
  [value]
  (cond
    (and (string? value) (#{"running" "done" "error" "cancelled"} value))
    (keyword value)

    (and (string? value) (#{"understanding" "working-memory" "planning" "llm" "tool-plan"
                            "tool" "approval" "finalizing" "observing" "updating"
                            "restarting" "paused" "cancelled" "error"} value))
    (keyword value)

    :else value))

(defn- latest-task-status-event
  [deps task-id]
  (some->> (get @(task-runtime-events-atom deps) (str task-id))
           :events
           reverse
           (some #(when (= :task.status (:type %)) %))))

(defn- event-status->runtime-status
  [event]
  (let [data (some-> (:data event) normalize-runtime-status-map)]
    (when (map? data)
      (-> data
          (update :state runtime-status-value)
          (update :phase runtime-status-value)
          (update :tool-id runtime-status-value)
          (assoc :updated-at (or (:received-at event)
                                 (:created-at event)))
          (update :message #(or % (:summary event)))))))

(defn- task-runtime-status
  [deps task]
  (when (contains? live-task-states (:state task))
    (or (some-> (latest-task-status-event deps (:id task))
                event-status->runtime-status)
        (some-> (get-in task [:meta :runtime])
                normalize-runtime-status-map
                (update :state runtime-status-value)
                (update :phase runtime-status-value)
                (update :tool-id runtime-status-value)))))

(defn- local-doc-ref->body
  [doc]
  {:id     (some-> (:id doc) str)
   :name   (:name doc)
   :status (some-> (:status doc) name)})

(defn- artifact-ref->body
  [artifact]
  {:id     (some-> (:id artifact) str)
   :name   (:name artifact)
   :title  (:title artifact)
   :status (some-> (:status artifact) name)})

(defn- tool-call->body
  [tool-call]
  (cond-> {:id        (get tool-call "id")
           :name      (or (get-in tool-call ["function" "name"])
                          (get tool-call "name"))}
    (or (get-in tool-call ["function" "arguments"])
        (get tool-call "arguments"))
    (assoc :arguments (or (get-in tool-call ["function" "arguments"])
                          (get tool-call "arguments")))))

(defn- session-message->body
  [deps {:keys [id role content created-at local-docs artifacts tool-calls llm-call-id provider-id model workload]}]
  (cond-> {:id         (some-> id str)
           :role       (name role)
           :content    content
           :created_at (instant->str* deps created-at)
           :local_docs (into [] (map local-doc-ref->body) (or local-docs []))
           :artifacts  (into [] (map artifact-ref->body) (or artifacts []))}
    llm-call-id (assoc :llm_call_id (str llm-call-id))
    provider-id (assoc :provider_id (name provider-id))
    model (assoc :model model)
    workload (assoc :workload (name workload))
    (seq tool-calls) (assoc :tool_calls (mapv tool-call->body tool-calls))))

(defn- audit-event->body
  [deps event]
  (cond-> {:id         (some-> (:id event) str)
           :session_id (some-> (:session-id event) str)
           :channel    (some-> (:channel event) name)
           :actor      (some-> (:actor event) name)
           :type       (some-> (:type event) name)
           :created_at (instant->str* deps (:created-at event))}
    (:message-id event) (assoc :message_id (str (:message-id event)))
    (:llm-call-id event) (assoc :llm_call_id (str (:llm-call-id event)))
    (:tool-id event) (assoc :tool_id (:tool-id event))
    (:tool-call-id event) (assoc :tool_call_id (:tool-call-id event))
    (:data event) (assoc :data (:data event))))

(defn- status->body
  [deps status]
  (when status
    {:state      (some-> (:state status) name)
     :phase      (some-> (:phase status) name)
     :message    (:message status)
     :partial_content (:partial-content status)
     :tool_id    (some-> (:tool-id status) name)
     :tool_name  (:tool-name status)
     :iteration  (:iteration status)
     :max_iterations (:max-iterations status)
     :current_focus (:current-focus status)
     :progress_status (:progress-status status)
     :intent_focus (:intent-focus status)
     :intent_agenda_item (:intent-agenda-item status)
     :intent_plan_step (:intent-plan-step status)
     :intent_why (:intent-why status)
     :intent_tool_name (:intent-tool-name status)
     :intent_tool_args_summary (:intent-tool-args-summary status)
     :stack_depth (:stack-depth status)
     :agenda     (:agenda status)
     :stack      (:stack status)
     :round      (:round status)
     :tool_count (:tool-count status)
     :updated_at (instant->str* deps (:updated-at status))}))

(defn- history-run->body
  [deps run]
  {:id          (some-> (:id run) str)
   :schedule_id (some-> (:schedule-id run) name)
   :started_at  (instant->str* deps (:started-at run))
   :finished_at (instant->str* deps (:finished-at run))
   :status      (some-> (:status run) name)
   :actions     (:actions run)
   :result      (:result run)
   :error       (:error run)})

(defn- history-schedule->body
  [deps sched]
  (let [latest-run (first (schedule/schedule-history (:id sched) 1))]
    {:id            (some-> (:id sched) name)
     :name          (:name sched)
     :type          (some-> (:type sched) name)
     :trusted       (boolean (:trusted? sched))
     :enabled       (boolean (:enabled? sched))
     :last_run      (instant->str* deps (:last-run sched))
     :next_run      (instant->str* deps (:next-run sched))
     :latest_status (some-> (:status latest-run) name)
     :latest_error  (truncate-text* deps (:error latest-run) 160)}))

(defn- history-session->body
  [deps session]
  (let [messages     (->> (db/session-messages (:id session))
                          (filter #(#{:user :assistant} (:role %)))
                          vec)
        last-message (last messages)]
    {:id              (some-> (:id session) str)
     :channel         (some-> (:channel session) name)
     :created_at      (instant->str* deps (:created-at session))
     :active          (boolean (:active? session))
     :message_count   (count messages)
     :last_message_at (instant->str* deps (:created-at last-message))
     :preview         (truncate-text* deps (:content last-message) 160)}))

(defn- task-item->body
  [deps item]
  (cond-> {:id         (some-> (:id item) str)
           :turn_id    (some-> (:turn-id item) str)
           :index      (:index item)
           :type       (some-> (:type item) name)
           :created_at (instant->str* deps (:created-at item))}
    (:status item) (assoc :status (name (:status item)))
    (:role item) (assoc :role (name (:role item)))
    (:summary item) (assoc :summary (:summary item))
    (:data item) (assoc :data (:data item))
    (:message-id item) (assoc :message_id (str (:message-id item)))
    (:llm-call-id item) (assoc :llm_call_id (str (:llm-call-id item)))
    (:tool-id item) (assoc :tool_id (:tool-id item))
    (:tool-call-id item) (assoc :tool_call_id (:tool-call-id item))))

(defn- task-event->body
  [deps event]
  (cond-> {:id         (:id event)
           :index      (:index event)
           :type       (some-> (:type event) name)
           :task_id    (some-> (:task-id event) str)
           :created_at (instant->str* deps (:created-at event))}
    (:stream-index event) (assoc :stream_index (:stream-index event))
    (:received-at event) (assoc :received_at (instant->str* deps (:received-at event)))
    (:turn-id event) (assoc :turn_id (str (:turn-id event)))
    (:item-id event) (assoc :item_id (str (:item-id event)))
    (:summary event) (assoc :summary (:summary event))
    (:status event) (assoc :status (name (:status event)))
    (:role event) (assoc :role (name (:role event)))
    (:tool-id event) (assoc :tool_id (:tool-id event))
    (:tool-call-id event) (assoc :tool_call_id (:tool-call-id event))
    (:llm-call-id event) (assoc :llm_call_id (str (:llm-call-id event)))
    (:message-id event) (assoc :message_id (str (:message-id event)))
    (:data event) (assoc :data (:data event))))

(defn- task-turn->body
  [deps turn items]
  (cond-> {:id         (some-> (:id turn) str)
           :task_id    (some-> (:task-id turn) str)
           :index      (:index turn)
           :operation  (some-> (:operation turn) name)
           :state      (some-> (:state turn) name)
           :created_at (instant->str* deps (:created-at turn))
           :updated_at (instant->str* deps (:updated-at turn))
           :items      (mapv #(task-item->body deps %) items)}
    (:input turn) (assoc :input (:input turn))
    (:summary turn) (assoc :summary (:summary turn))
    (:error turn) (assoc :error (:error turn))
    (:meta turn) (assoc :meta (:meta turn))
    (:interrupting-turn-id turn) (assoc :interrupting_turn_id (str (:interrupting-turn-id turn)))
    (:started-at turn) (assoc :started_at (instant->str* deps (:started-at turn)))
    (:finished-at turn) (assoc :finished_at (instant->str* deps (:finished-at turn)))))

(declare stack->body)

(defn- task->body
  ([deps task]
   (task->body deps
               task
               (task-runtime/runtime-autonomy-state (:session-id task) (:id task))))
  ([deps task autonomy-state]
   (let [runtime         (get-in task [:meta :runtime])
         recovery        (task-runtime/task-recovery task)
         checkpoint      (task-runtime/task-checkpoint task)
         checkpoint-at   (task-runtime/task-checkpoint-at task)
         recovery-brief  (task-runtime/task-recovery-brief task)
         inspection      (task-inspection/task-inspection
                          {:instant->str #(instant->str* deps %)
                           :truncate-text #(truncate-text* deps %1 %2)}
                          task
                          autonomy-state)
         state           (or (:state runtime) (:state task))
         stack           (stack->body deps autonomy-state)]
     (cond-> {:id         (some-> (:id task) str)
              :session_id (some-> (:session-id task) str)
              :channel    (some-> (:channel task) name)
              :type       (some-> (:type task) name)
              :state      (some-> state name)
              :created_at (instant->str* deps (:created-at task))
              :updated_at (instant->str* deps (:updated-at task))}
       (:parent-id task) (assoc :parent_id (str (:parent-id task)))
       (:current-turn-id task) (assoc :current_turn_id (str (:current-turn-id task)))
       (:title task) (assoc :title (:title task))
       (:summary task) (assoc :summary (:summary task))
       (:stop-reason task) (assoc :stop_reason (name (:stop-reason task)))
       (:error task) (assoc :error (:error task))
       runtime (assoc :runtime runtime)
       recovery (assoc :recovery recovery)
       checkpoint (assoc :checkpoint checkpoint)
       checkpoint-at (assoc :checkpoint_at (instant->str* deps checkpoint-at))
       recovery-brief (assoc :recovery_brief recovery-brief)
       inspection (assoc :inspection inspection)
       (:meta task) (assoc :meta (:meta task))
       autonomy-state (assoc :autonomy_state autonomy-state)
       stack (assoc :stack stack)
       (:started-at task) (assoc :started_at (instant->str* deps (:started-at task)))
       (:finished-at task) (assoc :finished_at (instant->str* deps (:finished-at task)))))))

(defn- stack-frame->body
  [deps frame]
  (cond-> {:title (:title frame)}
    (:kind frame) (assoc :kind (name (:kind frame)))
    (:child-task-id frame) (assoc :child_task_id (str (:child-task-id frame)))
    (:progress-status frame) (assoc :progress_status (name (:progress-status frame)))
    (:summary frame) (assoc :summary (truncate-text* deps (:summary frame) 240))
    (:next-step frame) (assoc :next_step (truncate-text* deps (:next-step frame) 160))
    (:compressed? frame) (assoc :compressed true)
    (:compressed-count frame) (assoc :compressed_count (:compressed-count frame))))

(defn- stack->body
  [deps autonomy-state]
  (when autonomy-state
    (let [stack* (vec (:stack (autonomous/normalize-state autonomy-state)))
          tip    (peek stack*)
          root   (first stack*)]
      {:depth (count stack*)
       :current_focus (:title tip)
       :root_goal (:title root)
       :frames (mapv #(stack-frame->body deps %) stack*)})))

(defn- history-task->body
  ([deps task]
   (history-task->body deps
                       task
                       (task-runtime/runtime-autonomy-state (:session-id task) (:id task))))
  ([deps task autonomy-state]
   (let [turns       (db/task-turns (:id task))
         latest-turn (last turns)
         runtime     (get-in task [:meta :runtime])
         recovery    (task-runtime/task-recovery task)
         checkpoint  (task-runtime/task-checkpoint task)
         checkpoint-at (task-runtime/task-checkpoint-at task)
         recovery-brief (task-runtime/task-recovery-brief task)
         inspection  (task-inspection/task-inspection
                      {:instant->str #(instant->str* deps %)
                       :truncate-text #(truncate-text* deps %1 %2)}
                      task
                      autonomy-state
                      true)
         state       (or (:state runtime) (:state task))
         stack       (stack->body deps autonomy-state)]
     (cond-> {:id          (some-> (:id task) str)
              :session_id  (some-> (:session-id task) str)
              :channel     (some-> (:channel task) name)
              :type        (some-> (:type task) name)
              :state       (some-> state name)
              :turn_count  (count turns)
              :created_at  (instant->str* deps (:created-at task))
              :updated_at  (instant->str* deps (:updated-at task))}
       (:current-turn-id task) (assoc :current_turn_id (str (:current-turn-id task)))
       (:title task) (assoc :title (:title task))
       (:summary task) (assoc :summary (:summary task))
       runtime (assoc :runtime runtime)
       recovery (assoc :recovery recovery)
       checkpoint (assoc :checkpoint checkpoint)
       checkpoint-at (assoc :checkpoint_at (instant->str* deps checkpoint-at))
       recovery-brief (assoc :recovery_brief recovery-brief)
       inspection (assoc :inspection inspection)
       stack (assoc :stack stack)
       latest-turn (assoc :latest_turn_state (some-> (:state latest-turn) name))
       latest-turn (assoc :latest_turn_summary (truncate-text* deps (:summary latest-turn) 160))
       (:started-at task) (assoc :started_at (instant->str* deps (:started-at task)))
       (:finished-at task) (assoc :finished_at (instant->str* deps (:finished-at task)))))))

(defn- llm-call-summary->body
  [deps entry]
  (cond-> {:id          (str (:id entry))
           :session_id  (some-> (:session-id entry) str)
           :provider_id (some-> (:provider-id entry) name)
           :model       (:model entry)
           :workload    (some-> (:workload entry) name)
           :status      (some-> (:status entry) name)
           :duration_ms (:duration-ms entry)
           :created_at  (instant->str* deps (:created-at entry))}
    (:prompt-tokens entry)     (assoc :prompt_tokens (:prompt-tokens entry))
    (:completion-tokens entry) (assoc :completion_tokens (:completion-tokens entry))
    (:error entry)             (assoc :error (:error entry))))

(defn- llm-call-detail->body
  [deps entry]
  (cond-> (llm-call-summary->body deps entry)
    (:messages entry) (assoc :messages (:messages entry))
    (:tools entry)    (assoc :tools (:tools entry))
    (:response entry) (assoc :response (:response entry))
    (seq (:related-messages entry))
    (assoc :related_messages
           (mapv (fn [{:keys [id role provider-id model workload created-at]}]
                   (cond-> {:id         (str id)
                            :role       (name role)
                            :created_at (instant->str* deps created-at)}
                     provider-id (assoc :provider_id (name provider-id))
                     model (assoc :model model)
                     workload (assoc :workload (name workload))))
                 (:related-messages entry)))))

(defn handle-create-session
  ([deps]
   (handle-create-session deps :http))
  ([deps channel]
   (let [sid (db/create-session! channel)]
     (wm/ensure-wm! sid)
     (touch-rest-session!* deps sid)
     (json-response* deps 200 {:session_id (str sid)}))))

(defn- internal-server-error-response
  [deps ^Throwable throwable]
  (json-response* deps 500 {:error (or (throwable-message* deps throwable)
                                       "internal server error")}))

(defn- chat-request
  ([deps req]
   (chat-request deps req :http))
  ([deps req channel]
   (let [data          (read-body* deps req)
         message       (get data "message")
         session-id    (get data "session_id")
         local-doc-ids (when (sequential? (get data "local_doc_ids"))
                         (vec (keep #(when (some? %) (str %))
                                    (get data "local_doc_ids"))))
         artifact-ids  (when (sequential? (get data "artifact_ids"))
                         (vec (keep #(when (some? %) (str %))
                                    (get data "artifact_ids"))))]
     (when (and session-id (= channel :http))
       (maybe-resume-http-session!* deps session-id channel))
     (cond
       (not message)
       {:response (json-response* deps 400 {:error "missing 'message' field"})}

       (and session-id (not (session-accessible?* deps session-id channel)))
       {:response (json-response* deps 404 {:error "unknown session id"})}

       (and session-id (not (session-active?* deps session-id)))
       {:response (json-response* deps 409 {:error "session closed"})}

       :else
       (let [sid (if session-id
                   (java.util.UUID/fromString session-id)
                   (db/create-session! channel))]
         (wm/ensure-wm! sid)
         (cancel-rest-session-finalizer!* deps sid)
         {:session-id    sid
          :channel       channel
          :message       message
          :local-doc-ids local-doc-ids
          :artifact-ids  artifact-ids})))))

(defn- process-chat!
  [deps {:keys [session-id channel message local-doc-ids artifact-ids]}]
  (try
    (let [response (agent/process-message session-id
                                          message
                                          :channel channel
                                          :local-doc-ids local-doc-ids
                                          :artifact-ids artifact-ids)
          assistant-message (db/latest-session-message session-id #{:assistant})
          task              (db/current-session-task session-id)
          body              (cond-> {:session_id (str session-id)
                                     :role       "assistant"
                                     :content    response
                                     :message    (when assistant-message
                                                   (session-message->body deps assistant-message))}
                               task (assoc :task (task->body deps task))
                               task (assoc :task_id (some-> (:id task) str))
                               (:current-turn-id task) (assoc :current_turn_id
                                                              (str (:current-turn-id task))))]
      (touch-rest-session!* deps session-id)
      (json-response* deps 200 body))
    (catch clojure.lang.ExceptionInfo e
      (exception-response* deps e))
    (catch Exception e
      (log/error e "HTTP chat request failed")
      (internal-server-error-response deps e))))

(defn- handle-chat-sync
  [deps chat]
  (process-chat! deps chat))

(defn- handle-chat-async
  [deps req chat]
  (http/as-channel
   req
   {:on-open
    (fn [ch]
      (future
        (let [response (try
                         (process-chat! deps chat)
                         (catch clojure.lang.ExceptionInfo e
                           (exception-response* deps e))
                         (catch Exception e
                           (log/error e "Async HTTP chat request failed")
                           (internal-server-error-response deps e)))]
          (http/send! ch response))))}))

(defn handle-chat
  ([deps req]
   (handle-chat deps req :http))
  ([deps req channel]
   (let [{:keys [response] :as chat} (chat-request deps req channel)]
     (cond
       response
       response

       (:async-channel req)
       (handle-chat-async deps req chat)

       :else
       (handle-chat-sync deps chat)))))

(defn handle-get-status
  ([deps session-id]
   (handle-get-status deps session-id nil))
  ([deps session-id expected-channel]
   (let [sid (parse-session-id* deps session-id)]
     (when (and sid (= expected-channel :http))
       (maybe-resume-http-session!* deps sid expected-channel))
     (cond
       (nil? sid)
       (json-response* deps 400 {:error "invalid session id"})

       (not (session-accessible?* deps sid expected-channel))
       (json-response* deps 404 {:error "session not found"})

       (not (session-active?* deps sid))
       (json-response* deps 409 {:error "session closed"})

       :else
       (do
         (touch-rest-session!* deps sid)
         (let [task   (db/current-session-task (java.util.UUID/fromString sid))
               status (or (when task
                            (task-runtime-status deps task))
                          (get @(session-statuses-atom deps) sid))]
           (json-response* deps 200
                           (cond-> {:session_id sid
                                    :status     (status->body deps status)}
                             task (assoc :task_id (some-> (:id task) str))
                             (:current-turn-id task) (assoc :current_turn_id
                                                            (str (:current-turn-id task)))))))))))

(defn handle-get-current-task
  ([deps session-id]
   (handle-get-current-task deps session-id nil))
  ([deps session-id expected-channel]
   (let [sid (parse-session-id* deps session-id)]
     (when (and sid (= expected-channel :http))
       (maybe-resume-http-session!* deps sid expected-channel))
     (cond
       (nil? sid)
       (json-response* deps 400 {:error "invalid session id"})

       (not (session-accessible?* deps sid expected-channel))
       (json-response* deps 404 {:error "session not found"})

       (not (session-active?* deps sid))
       (json-response* deps 409 {:error "session closed"})

       :else
       (do
         (touch-rest-session!* deps sid)
         (let [task (db/current-session-task (java.util.UUID/fromString sid))]
           (json-response* deps 200
                           (cond-> {:session_id sid
                                    :task       (when task
                                                  (task->body deps task))}
                             task (assoc :task_id (some-> (:id task) str))
                             task (assoc :task_live (boolean (live-task? task)))
                             (:current-turn-id task) (assoc :current_turn_id
                                                            (str (:current-turn-id task)))))))))))

(defn handle-get-approval
  ([deps session-id]
   (handle-get-approval deps session-id nil))
  ([deps session-id expected-channel]
   (when (and session-id (= expected-channel :http))
     (maybe-resume-http-session!* deps session-id expected-channel))
   (cond
     (nil? (parse-session-id* deps session-id))
     (json-response* deps 400 {:error "invalid session id"})

     (not (session-accessible?* deps session-id expected-channel))
     (json-response* deps 404 {:error "session not found"})

     (not (session-active?* deps session-id))
     (json-response* deps 409 {:error "session closed"})

     :else
     (do
       (touch-rest-session!* deps session-id)
       (if-let [approval (prompt/pending-interaction {:session-id session-id
                                                      :kind :approval})]
         (json-response* deps 200 {:pending true
                                   :approval (approval->body* deps approval)})
         (json-response* deps 200 {:pending false}))))))

(defn handle-get-prompt
  ([deps session-id]
   (handle-get-prompt deps session-id nil))
  ([deps session-id expected-channel]
   (when (and session-id (= expected-channel :http))
     (maybe-resume-http-session!* deps session-id expected-channel))
   (cond
     (nil? (parse-session-id* deps session-id))
     (json-response* deps 400 {:error "invalid session id"})

     (not (session-accessible?* deps session-id expected-channel))
     (json-response* deps 404 {:error "session not found"})

     (not (session-active?* deps session-id))
     (json-response* deps 409 {:error "session closed"})

     :else
     (do
       (touch-rest-session!* deps session-id)
       (if-let [interaction (prompt/pending-interaction {:session-id session-id
                                                         :kind :prompt})]
         (json-response* deps 200 {:pending true
                                   :prompt (prompt->body* deps interaction)})
         (json-response* deps 200 {:pending false}))))))

(defn handle-submit-prompt
  ([deps session-id req]
   (handle-submit-prompt deps session-id req nil))
  ([deps session-id req expected-channel]
   (if-not (parse-session-id* deps session-id)
     (json-response* deps 400 {:error "invalid session id"})
     (if-not (session-accessible?* deps session-id expected-channel)
       (json-response* deps 404 {:error "session not found"})
       (let [data      (read-body* deps req)
             prompt-id (get data "prompt_id")
             has-value? (contains? data "value")
             value     (get data "value")]
         (cond
           (not has-value?)
           (json-response* deps 400 {:error "missing value"})

           :else
           (let [{:keys [status]}
                 (prompt/deliver-validated-interaction! {:session-id session-id
                                                         :kind :prompt}
                                                        prompt-id
                                                        (str (or value "")))]
             (case status
               :missing (json-response* deps 404 {:error "no pending prompt"})
               :stale (json-response* deps 409 {:error "stale prompt id"})
               (do
                 (touch-rest-session!* deps session-id)
                 (json-response* deps 200 {:status "recorded"}))))))))))

(defn handle-submit-approval
  ([deps session-id req]
   (handle-submit-approval deps session-id req nil))
  ([deps session-id req expected-channel]
   (if-not (parse-session-id* deps session-id)
     (json-response* deps 400 {:error "invalid session id"})
     (if-not (session-accessible?* deps session-id expected-channel)
       (json-response* deps 404 {:error "session not found"})
       (let [data        (read-body* deps req)
             approval-id (get data "approval_id")
             decision    (get data "decision")
             decision*   (case decision
                           "allow" :allow
                           "deny"  :deny
                           nil)]
         (cond
           (nil? decision*)
           (json-response* deps 400 {:error "invalid decision"})

           :else
           (let [{:keys [status]}
                 (prompt/deliver-validated-interaction! {:session-id session-id
                                                         :kind :approval}
                                                        approval-id
                                                        decision*)]
             (case status
               :missing (json-response* deps 404 {:error "no pending approval"})
               :stale (json-response* deps 409 {:error "stale approval id"})
               (do
                 (touch-rest-session!* deps session-id)
                 (json-response* deps 200 {:status "recorded"}))))))))))

(defn handle-get-task-prompt
  [deps task-id]
  (try
    (let [uuid (java.util.UUID/fromString task-id)
          task (db/get-task uuid)]
      (if-not task
        (json-response* deps 404 {:error "task not found"})
        (if-let [interaction (prompt/resolve-pending-interaction {:task-id uuid
                                                                  :session-id (:session-id task)
                                                                  :kind :prompt})]
          (json-response* deps 200 {:pending true
                                    :prompt (prompt->body* deps interaction)})
          (json-response* deps 200 {:pending false}))))
    (catch IllegalArgumentException _
      (json-response* deps 400 {:error "invalid task id"}))))

(defn handle-submit-task-prompt
  [deps task-id req]
  (try
    (let [uuid (java.util.UUID/fromString task-id)
          task (db/get-task uuid)]
      (if-not task
        (json-response* deps 404 {:error "task not found"})
        (let [data      (read-body* deps req)
              prompt-id (get data "prompt_id")
              has-value? (contains? data "value")
              value     (get data "value")]
          (cond
            (not has-value?)
            (json-response* deps 400 {:error "missing value"})

            :else
            (let [{:keys [status]}
                  (prompt/deliver-validated-interaction! {:task-id uuid
                                                          :session-id (:session-id task)
                                                          :kind :prompt}
                                                         prompt-id
                                                         (str (or value "")))]
              (case status
                :missing (json-response* deps 404 {:error "no pending prompt"})
                :stale (json-response* deps 409 {:error "stale prompt id"})
                (json-response* deps 200 {:status "recorded"})))))))
    (catch IllegalArgumentException _
      (json-response* deps 400 {:error "invalid task id"}))))

(defn handle-get-task-approval
  [deps task-id]
  (try
    (let [uuid (java.util.UUID/fromString task-id)
          task (db/get-task uuid)]
      (if-not task
        (json-response* deps 404 {:error "task not found"})
        (if-let [interaction (prompt/resolve-pending-interaction {:task-id uuid
                                                                  :session-id (:session-id task)
                                                                  :kind :approval})]
          (json-response* deps 200 {:pending true
                                    :approval (approval->body* deps interaction)})
          (json-response* deps 200 {:pending false}))))
    (catch IllegalArgumentException _
      (json-response* deps 400 {:error "invalid task id"}))))

(defn handle-submit-task-approval
  [deps task-id req]
  (try
    (let [uuid (java.util.UUID/fromString task-id)
          task (db/get-task uuid)]
      (if-not task
        (json-response* deps 404 {:error "task not found"})
        (let [data        (read-body* deps req)
              approval-id (get data "approval_id")
              decision    (get data "decision")
              decision*   (case decision
                            "allow" :allow
                            "deny"  :deny
                            nil)]
          (cond
            (nil? decision*)
            (json-response* deps 400 {:error "invalid decision"})

            :else
            (let [{:keys [status]}
                  (prompt/deliver-validated-interaction! {:task-id uuid
                                                          :session-id (:session-id task)
                                                          :kind :approval}
                                                         approval-id
                                                         decision*)]
              (case status
                :missing (json-response* deps 404 {:error "no pending approval"})
                :stale (json-response* deps 409 {:error "stale approval id"})
                (json-response* deps 200 {:status "recorded"})))))))
    (catch IllegalArgumentException _
      (json-response* deps 400 {:error "invalid task id"}))))

(defn handle-session-messages
  ([deps session-id]
   (handle-session-messages deps session-id nil))
  ([deps session-id expected-channel]
   (try
     (let [sid (java.util.UUID/fromString session-id)]
       (if-not (session-accessible?* deps sid expected-channel)
         (json-response* deps 404 {:error "session not found"})
         (let [messages (->> (db/session-messages sid)
                             (into [] (comp
                                       (filter #(#{:user :assistant} (:role %)))
                                       (map #(session-message->body deps %)))))]
           (touch-rest-session!* deps session-id)
           (json-response* deps 200 {:session_id session-id
                                     :messages   messages}))))
     (catch IllegalArgumentException _
       (json-response* deps 400 {:error "invalid session id"})))))

(defn handle-close-session
  ([deps session-id]
   (handle-close-session deps session-id nil))
  ([deps session-id expected-channel]
   (cond
     (nil? (parse-session-id* deps session-id))
     (json-response* deps 400 {:error "invalid session id"})

     (not (session-accessible?* deps session-id expected-channel))
     (json-response* deps 404 {:error "session not found"})

     :else
     (let [sid    (parse-session-id* deps session-id)
           result (prompt/apply-session-control-intent!
                   {:busy? (fn [session-id]
                             (session-busy?* deps session-id))
                    :cancel-session! (fn [session-id reason]
                                       (agent/cancel-session! session-id reason))
                    :finalize-session! (fn [session-id]
                                         (finalize-rest-session!* deps session-id :explicit))}
                   sid
                   :close
                   :reason "session close requested")
           {:keys [response-kind status status-key message]} (prompt/session-control-result-view :close result)]
       (case response-kind
         :accepted
         (json-response* deps 202 {:session_id sid
                                   :status status-key
                                   :closing true})

         :completed
         (json-response* deps 200 {:session_id sid
                                   :status status-key
                                   :already_closed (= :already-closed status)})

         :conflict
         (json-response* deps 409 {:error message})

         (json-response* deps 500 {:error "unknown session control result"}))))))

(defn handle-history-sessions
  [deps]
  (json-response* deps 200
                  {:sessions (->> (db/list-sessions)
                                  (into [] (comp (filter #(contains? history-session-channels
                                                                    (:channel %)))
                                                 (map #(history-session->body deps %)))))}))

(defn handle-history-tasks
  [deps]
  (json-response* deps 200
                  {:tasks (->> (db/list-tasks)
                               (into [] (map #(history-task->body deps %))))}))

(defn handle-get-task
  [deps task-id]
  (try
    (let [uuid (java.util.UUID/fromString task-id)
          task (db/get-task uuid)]
      (if-not task
        (json-response* deps 404 {:error "task not found"})
        (let [turns (db/task-turns uuid)]
          (json-response* deps 200
                          {:task  (task->body deps task)
                           :turns (mapv (fn [turn]
                                          (task-turn->body deps
                                                           turn
                                                           (db/turn-items (:id turn))))
                                        turns)}))))
    (catch IllegalArgumentException _
      (json-response* deps 400 {:error "invalid task id"}))))

(defn handle-get-task-events
  [deps task-id]
  (try
    (let [uuid (java.util.UUID/fromString task-id)
          task (db/get-task uuid)]
      (if-not task
        (json-response* deps 404 {:error "task not found"})
        (let [turns      (db/task-turns uuid)
              turn-items (into {}
                               (map (fn [turn]
                                      [(:id turn) (db/turn-items (:id turn))]))
                               turns)
              events     (task-event/task-events task turns turn-items)]
          (json-response* deps 200
                          {:task_id (str uuid)
                           :events  (mapv #(task-event->body deps %) events)}))))
    (catch IllegalArgumentException _
      (json-response* deps 400 {:error "invalid task id"}))))

(defn handle-get-live-task-events
  [deps task-id req]
  (try
    (let [uuid   (java.util.UUID/fromString task-id)
          task   (db/get-task uuid)
          params (parse-query-string* deps (:query-string req))
          after  (or (some-> (get params "after") parse-long) 0)]
      (if-not task
        (json-response* deps 404 {:error "task not found"})
        (let [{:keys [next-index events]} (get @(task-runtime-events-atom deps)
                                               (str uuid))
              events* (->> (or events [])
                           (filter #(> (long (or (:stream-index %) 0))
                                       (long after)))
                           (mapv #(task-event->body deps %)))]
          (json-response* deps 200
                          {:task_id (str uuid)
                           :after after
                           :next_stream_index (long (or next-index 0))
                           :events events*}))))
    (catch IllegalArgumentException _
      (json-response* deps 400 {:error "invalid task id"}))))

(defn- task-live-events-after
  [deps task-id after]
  (let [{:keys [next-index events]} (get @(task-runtime-events-atom deps)
                                         (str task-id))]
    {:next-index (long (or next-index 0))
     :events (->> (or events [])
                  (filter #(> (long (or (:stream-index %) 0))
                              (long after)))
                  vec)}))

(defn- task-event-stream-after
  [deps req]
  (let [params         (parse-query-string* deps (:query-string req))
        query-after    (some-> (get params "after") parse-long)
        last-event-id  (some-> (get-in req [:headers "last-event-id"]) parse-long)]
    (long (max (or query-after 0)
               (or last-event-id 0)))))

(defn- task-event-sse-chunk
  [deps event]
  (let [body (task-event->body deps event)
        data (json/write-json-str body)
        id   (long (or (:stream-index event) 0))]
    (str "id: " id "\n"
         "data: " data "\n\n")))

(defn handle-get-task-event-stream
  [deps task-id req]
  (try
    (let [uuid  (java.util.UUID/fromString task-id)
          task  (db/get-task uuid)
          after (task-event-stream-after deps req)]
      (if-not task
        (json-response* deps 404 {:error "task not found"})
        (let [subscriber-id* (atom nil)]
          (http/as-channel
           req
           {:on-open
            (fn [ch]
              (let [subscriber-id (str (random-uuid))
                    last-sent     (atom (long after))
                    send-event!   (fn [event]
                                    (let [stream-index (long (or (:stream-index event) 0))]
                                      (when (pos? stream-index)
                                        (loop []
                                          (let [previous @last-sent]
                                            (when (> stream-index previous)
                                              (if (compare-and-set! last-sent previous stream-index)
                                                (http/send! ch (task-event-sse-chunk deps event) false)
                                                (recur))))))))]
                (reset! subscriber-id* subscriber-id)
                (http/send! ch {:status  200
                                :headers {"content-type" "text/event-stream; charset=utf-8"
                                          "cache-control" "no-store"
                                          "connection" "keep-alive"}
                                :body    ""} false)
                (register-task-runtime-stream-subscriber!*
                 deps uuid subscriber-id send-event!)
                (doseq [event (:events (task-live-events-after deps uuid @last-sent))]
                  (send-event! event))
                (http/send! ch ": connected\n\n" false)))

            :on-close
            (fn [_ch _status]
              (when-let [subscriber-id @subscriber-id*]
                (unregister-task-runtime-stream-subscriber!* deps uuid subscriber-id)))}))))
    (catch IllegalArgumentException _
      (json-response* deps 400 {:error "invalid task id"}))))

(defn- task-control-response
  [deps intent result]
  (let [{:keys [status response-kind status-key message]} (prompt/control-result-view intent result)]
    (case response-kind
      :missing
      (json-response* deps 404 {:error "task not found"})

      :conflict
      (json-response* deps 409 {:error (or (:error result) message)
                                :task_id (some-> (:task-id result) str)
                                :session_id (some-> (:session-id result) str)})

      :unavailable
      (json-response* deps 503 {:error (or (:error result) message)
                                :task_id (some-> (:task-id result) str)
                                :session_id (some-> (:session-id result) str)})

      :accepted
      (json-response* deps 202
                      (cond-> {:status status-key
                               :task_id (some-> (:task-id result) str)
                               :session_id (some-> (:session-id result) str)}
                        (= status :forking)
                        (assoc :task (when-let [task (:task result)]
                                       (task->body deps task)))))

      :completed
      (json-response* deps 200
                      (cond-> {:status status-key}
                        (contains? #{:already-paused :already-stopped} status)
                        (assoc :task_id (some-> (:task-id result) str)
                               :session_id (some-> (:session-id result) str))
                        (contains? #{:paused :stopped} status)
                        (assoc :task (when-let [task (:task result)]
                                       (task->body deps task)))))

      (json-response* deps 500 {:error "unknown task control result"}))))

(def ^:private task-control-handlers
  {:pause-task! agent/pause-task!
   :resume-task! agent/resume-task!
   :stop-task! agent/stop-task!
   :interrupt-task! agent/interrupt-task!
   :steer-task! agent/steer-task!
   :fork-task! agent/fork-task!})

(defn- handle-task-control-intent
  [deps task-id intent & {:keys [message]}]
  (try
    (task-control-response deps
                           intent
                           (prompt/apply-task-control-intent! task-control-handlers
                                                              (java.util.UUID/fromString task-id)
                                                              intent
                                                              :message message))
    (catch IllegalArgumentException _
      (json-response* deps 400 {:error "invalid task id"}))))

(defn handle-pause-task
  [deps task-id]
  (handle-task-control-intent deps task-id :pause))

(defn handle-stop-task
  [deps task-id]
  (handle-task-control-intent deps task-id :stop))

(defn handle-interrupt-task
  [deps task-id]
  (handle-task-control-intent deps task-id :interrupt))

(defn handle-steer-task
  [deps task-id req]
  (let [data    (read-body* deps req)
        message (get data "message")]
    (handle-task-control-intent deps task-id :steer :message message)))

(defn handle-fork-task
  [deps task-id req]
  (let [data    (read-body* deps req)
        message (get data "message")]
    (handle-task-control-intent deps task-id :fork :message message)))

(defn handle-resume-task
  [deps task-id req]
  (let [data    (read-body* deps req)
        message (get data "message")]
    (handle-task-control-intent deps task-id :resume :message message)))

(defn handle-history-schedules
  [deps]
  (json-response* deps 200
                  {:schedules (->> (schedule/list-schedules)
                                   (sort-by (fn [sched]
                                              (or (date->millis* deps (:last-run sched))
                                                  (date->millis* deps (:next-run sched))
                                                  Long/MIN_VALUE))
                                            >)
                                   (into [] (map #(history-schedule->body deps %))))}))

(defn handle-history-schedule-runs
  [deps schedule-id]
  (try
    (let [sid   (parse-keyword-id* deps schedule-id "schedule_id")
          sched (schedule/get-schedule sid)]
      (if-not sched
        (json-response* deps 404 {:error "schedule not found"})
        (json-response* deps 200
                        {:schedule (history-schedule->body deps sched)
                         :runs     (into [] (map #(history-run->body deps %))
                                         (schedule/schedule-history sid 20))})))
    (catch clojure.lang.ExceptionInfo e
      (exception-response* deps e))))

(defn handle-list-llm-calls
  [deps req]
  (let [params         (parse-query-string* deps (:query-string req))
        limit          (or (some-> (get params "limit") parse-long) 50)
        raw-session-id (get params "session_id")
        session-id     (some-> raw-session-id (parse-session-id* deps))]
    (if (and raw-session-id (nil? session-id))
      (json-response* deps 400 {:error "invalid session id"})
      (json-response* deps 200
                      {:calls (into [] (map #(llm-call-summary->body deps %))
                                    (db/list-llm-calls (min limit 200) session-id))}))))

(defn handle-get-llm-call
  [deps call-id]
  (try
    (let [uuid  (java.util.UUID/fromString call-id)
          entry (db/get-llm-call uuid)]
      (if entry
        (json-response* deps 200 {:call (llm-call-detail->body deps entry)})
        (json-response* deps 404 {:error "call not found"})))
    (catch IllegalArgumentException _
      (json-response* deps 400 {:error "invalid call id"}))))

(defn handle-session-audit
  ([deps session-id]
   (handle-session-audit deps session-id nil))
  ([deps session-id expected-channel]
   (try
     (let [sid (java.util.UUID/fromString session-id)]
       (if-not (session-accessible?* deps sid expected-channel)
         (json-response* deps 404 {:error "session not found"})
         (let [events (mapv #(audit-event->body deps %)
                            (db/session-audit-events sid 1000))]
           (touch-rest-session!* deps session-id)
           (json-response* deps 200 {:session_id session-id
                                     :events     events}))))
     (catch IllegalArgumentException _
       (json-response* deps 400 {:error "invalid session id"})))))
