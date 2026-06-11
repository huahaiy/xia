(ns xia.channel.http.session
  "Session/chat/history/LLM-call HTTP handlers."
  (:require [charred.api :as json]
            [org.httpkit.server :as http]
            [taoensso.timbre :as log]
            [xia.bridge :as bridge]
            [xia.channel.http.common :as http-common]
            [xia.db :as db]
            [xia.goal :as goal]
            [xia.runtime-state :as runtime-state]
            [xia.schedule :as schedule]
            [xia.task-event :as task-event]))

(def ^:private history-session-channels #{:http :websocket :terminal :slack :telegram :imessage})

(defn- runtime-draining-response
  [deps work-kind]
  (let [{:keys [error reason]} (runtime-state/reject-new-work-data work-kind)]
    (http-common/json-response deps
                               409
                               {:error error
                                :reason (some-> reason name)})))

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

(defn- register-task-runtime-stream-subscriber!*
  [deps task-id subscriber-id callback]
  ((:register-task-runtime-stream-subscriber! deps) task-id subscriber-id callback))

(defn- unregister-task-runtime-stream-subscriber!*
  [deps task-id subscriber-id]
  ((:unregister-task-runtime-stream-subscriber! deps) task-id subscriber-id))

(defn- session-statuses-atom
  [deps]
  (:session-statuses-atom deps))

(defn- live-task?
  [task]
  (contains? #{:running :waiting_input :waiting_approval} (:state task)))

(defn- latest-task-status-event
  [deps task-id]
  ((:latest-task-status-event deps) task-id))

(defn- task-runtime-status
  [deps task]
  (task-event/task-runtime-status task (latest-task-status-event deps (:id task))))

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
  [deps {:keys [id role content created-at local-docs artifacts tool-calls llm-call-id provider-id model workload external-sender]}]
  (cond-> {:id         (some-> id str)
           :role       (name role)
           :content    content
           :created_at (http-common/instant->str deps created-at)
           :local_docs (into [] (map local-doc-ref->body) (or local-docs []))
           :artifacts  (into [] (map artifact-ref->body) (or artifacts []))}
    llm-call-id (assoc :llm_call_id (str llm-call-id))
    provider-id (assoc :provider_id (name provider-id))
    model (assoc :model model)
    workload (assoc :workload (name workload))
    external-sender (assoc :external_sender external-sender)
    (seq tool-calls) (assoc :tool_calls (mapv tool-call->body tool-calls))))

(defn- audit-event->body
  [deps event]
  (cond-> {:id         (some-> (:id event) str)
           :session_id (some-> (:session-id event) str)
           :channel    (some-> (:channel event) name)
           :actor      (some-> (:actor event) name)
           :type       (some-> (:type event) name)
           :created_at (http-common/instant->str deps (:created-at event))}
    (:message-id event) (assoc :message_id (str (:message-id event)))
    (:llm-call-id event) (assoc :llm_call_id (str (:llm-call-id event)))
    (:tool-id event) (assoc :tool_id (:tool-id event))
    (:tool-call-id event) (assoc :tool_call_id (:tool-call-id event))
    (:data event) (assoc :data (:data event))))

(defn- status->body
  [deps status]
  (task-event/runtime-status->wire-body status
                                        :instant->str #(http-common/instant->str deps %)))

(defn- history-run->body
  [deps run]
  {:id          (some-> (:id run) str)
   :schedule_id (some-> (:schedule-id run) name)
   :started_at  (http-common/instant->str deps (:started-at run))
   :finished_at (http-common/instant->str deps (:finished-at run))
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
     :last_run      (http-common/instant->str deps (:last-run sched))
     :next_run      (http-common/instant->str deps (:next-run sched))
     :latest_status (some-> (:status latest-run) name)
     :latest_error  (http-common/truncate-text deps (:error latest-run) 160)}))

(declare user-profile->body history-user-profile->body workspace->body history-workspace->body stack-view->body)

(defn- history-session->body
  ([deps session]
   (history-session->body deps session nil))
  ([deps session history-data]
   (let [{:keys [message-count last-message]}
         (or history-data
             (let [messages (->> (bridge/session-messages (:id session))
                                 (filter #(#{:user :assistant} (:role %)))
                                 vec)]
               {:message-count (count messages)
                :last-message  (last messages)}))
         user-profile (:user-profile session)
         workspace    (:workspace session)]
     {:id              (some-> (:id session) str)
      :channel         (some-> (:channel session) name)
      :created_at      (http-common/instant->str deps (:created-at session))
      :active          (boolean (:active? session))
      :message_count   (long (or message-count 0))
      :last_message_at (http-common/instant->str deps (:created-at last-message))
      :preview         (http-common/truncate-text deps (:content last-message) 160)
      :user_profile    (history-user-profile->body deps user-profile)
      :workspace       (history-workspace->body deps workspace)})))

(defn- task-item->body
  [deps item]
  (cond-> {:id         (some-> (:id item) str)
           :turn_id    (some-> (:turn-id item) str)
           :index      (:index item)
           :type       (some-> (:type item) name)
           :created_at (http-common/instant->str deps (:created-at item))}
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
  (task-event/event->wire-body event
                               :instant->str #(http-common/instant->str deps %)))

(defn- task-turn->body
  [deps turn items]
  (cond-> {:id         (some-> (:id turn) str)
           :task_id    (some-> (:task-id turn) str)
           :index      (:index turn)
           :operation  (some-> (:operation turn) name)
           :state      (some-> (:state turn) name)
           :created_at (http-common/instant->str deps (:created-at turn))
           :updated_at (http-common/instant->str deps (:updated-at turn))
           :items      (mapv #(task-item->body deps %) items)}
    (:input turn) (assoc :input (:input turn))
    (:summary turn) (assoc :summary (:summary turn))
    (:error turn) (assoc :error (:error turn))
    (:meta turn) (assoc :meta (:meta turn))
    (:interrupting-turn-id turn) (assoc :interrupting_turn_id (str (:interrupting-turn-id turn)))
    (:started-at turn) (assoc :started_at (http-common/instant->str deps (:started-at turn)))
    (:finished-at turn) (assoc :finished_at (http-common/instant->str deps (:finished-at turn)))))

(declare stack-view->body user-profile->body)

(defn- session-link->body
  [deps {:keys [session-id role created-at updated-at current? execution-current?]}]
  (let [current?           (boolean current?)
        execution-current? (boolean execution-current?)]
    (cond-> {:session_id        (some-> session-id str)
             :current           current?
             :execution_current execution-current?}
      role (assoc :role (name role))
      created-at (assoc :created_at (http-common/instant->str deps created-at))
      updated-at (assoc :updated_at (http-common/instant->str deps updated-at)))))

(defn- user-profile->body
  [deps user-profile]
  (when user-profile
    (cond-> {:id         (some-> (:id user-profile) str)
             :created_at (http-common/instant->str deps (:created-at user-profile))
             :updated_at (http-common/instant->str deps (:updated-at user-profile))}
      (:key user-profile) (assoc :key (:key user-profile))
      (:name user-profile) (assoc :name (:name user-profile))
      (:summary user-profile) (assoc :summary (:summary user-profile))
      (:preferences user-profile) (assoc :preferences (:preferences user-profile)))))

(defn- history-user-profile->body
  [deps user-profile]
  (when user-profile
    (cond-> {:id (some-> (:id user-profile) str)}
      (:key user-profile) (assoc :key (:key user-profile))
      (:name user-profile) (assoc :name (:name user-profile))
      (:summary user-profile) (assoc :summary (:summary user-profile)))))

(defn- workspace->body
  [deps workspace]
  (when workspace
    (cond-> {:id         (:id workspace)
             :created_at (http-common/instant->str deps (:created-at workspace))
             :updated_at (http-common/instant->str deps (:updated-at workspace))}
      (:name workspace) (assoc :name (:name workspace))
      (:preferences workspace) (assoc :preferences (:preferences workspace))
      (:constraints workspace) (assoc :constraints (:constraints workspace)))))

(defn- history-workspace->body
  [deps workspace]
  (when workspace
    (cond-> {:id (:id workspace)}
      (:name workspace) (assoc :name (:name workspace)))))

(defn- history-contract->body
  [contract]
  (when (map? contract)
    (let [kind (:kind contract)
          task-spec (:spec contract)]
      (cond-> {}
        kind (assoc :kind kind)
        (:goal contract) (assoc :goal (:goal contract))
        (:objective contract) (assoc :objective (:objective contract))
        (:parent-task-id contract) (assoc :parent-task-id (:parent-task-id contract))
        (:schedule-id contract) (assoc :schedule-id (:schedule-id contract))
        (:schedule-type contract) (assoc :schedule-type (:schedule-type contract))
        (contains? contract :trusted?) (assoc :trusted? (:trusted? contract))
        (:tool-id contract) (assoc :tool-id (:tool-id contract))
        task-spec (assoc :step-count (count (:steps task-spec)))))))

(defn- persistent-goal->body
  [deps goal]
  (when goal
    (let [contract (goal/goal-contract goal)]
      (cond-> {:id (:id goal)
               :text (:text goal)
               :status (some-> (:status goal) name)
               :source (some-> (:source goal) name)
               :turn_count (long (or (:turn-count goal) 0))
               :max_turns (:max-turns goal)}
        contract (assoc :contract {:intent (:goal/intent contract)
                                   :success_criteria (:goal/success-criteria contract)
                                   :constraints (:goal/constraints contract)
                                   :preferences (:goal/preferences contract)
                                   :budget (:goal/budget contract)
                                   :resume_policy (:goal/resume-policy contract)})
        (:goal/success-criteria contract) (assoc :success_criteria (:goal/success-criteria contract))
        (:goal/constraints contract) (assoc :constraints (:goal/constraints contract))
        (:goal/preferences contract) (assoc :preferences (:goal/preferences contract))
        (:goal/budget contract) (assoc :budget (:goal/budget contract))
        (:goal/resume-policy contract) (assoc :resume_policy (:goal/resume-policy contract))
        (:last-task-id goal) (assoc :last_task_id (str (:last-task-id goal)))
        (:last-task-state goal) (assoc :last_task_state (some-> (:last-task-state goal) name))
        (:last-judge-status goal) (assoc :last_judge_status (some-> (:last-judge-status goal) name))
        (:last-judge-reason goal) (assoc :last_judge_reason (:last-judge-reason goal))
        (:last-guardrail goal) (assoc :last_guardrail (some-> (:last-guardrail goal) name))
        (:last-summary goal) (assoc :last_summary (:last-summary goal))
        (:next-step goal) (assoc :next_step (:next-step goal))
        (:last-budget-status goal) (assoc :last_budget_status (:last-budget-status goal))
        (:created-at goal) (assoc :created_at (http-common/instant->str deps (:created-at goal)))
        (:updated-at goal) (assoc :updated_at (http-common/instant->str deps (:updated-at goal)))
        (:last-used-at goal) (assoc :last_used_at (http-common/instant->str deps (:last-used-at goal)))
        (:last-judged-at goal) (assoc :last_judged_at (http-common/instant->str deps (:last-judged-at goal)))
        (:paused-at goal) (assoc :paused_at (http-common/instant->str deps (:paused-at goal)))
        (:resumed-at goal) (assoc :resumed_at (http-common/instant->str deps (:resumed-at goal)))
        (:completed-at goal) (assoc :completed_at (http-common/instant->str deps (:completed-at goal)))))))

(defn- task->body
  ([deps task]
   (task->body deps task nil))
  ([deps task autonomy-state]
   (let [{:keys [state execution-session-role runtime-view inspection session-links stack]}
         (bridge/task-view
          {:instant->str #(http-common/instant->str deps %)
           :truncate-text #(http-common/truncate-text deps %1 %2)}
          task
          (cond-> {}
            autonomy-state (assoc :autonomy-state autonomy-state)))
         {:keys [recovery checkpoint checkpoint-at resume-hint recovery-brief]
          boundary :boundary-summary} runtime-view
         persistent-goal (get-in task [:meta :persistent-goal])
         contract        (:contract task)
         constraints     (:constraints task)
         session-links   (not-empty (mapv #(session-link->body deps %) session-links))
         stack           (stack-view->body deps stack)]
     (cond-> {:id         (some-> (:id task) str)
              :session_id (some-> (:session-id task) str)
              :execution_session_id (some-> (:session-id task) str)
              :channel    (some-> (:channel task) name)
              :type       (some-> (:type task) name)
              :state      (some-> state name)
              :created_at (http-common/instant->str deps (:created-at task))
              :updated_at (http-common/instant->str deps (:updated-at task))}
       execution-session-role (assoc :execution_session_role (name execution-session-role))
       (:parent-id task) (assoc :parent_id (str (:parent-id task)))
       (:current-turn-id task) (assoc :current_turn_id (str (:current-turn-id task)))
       (:title task) (assoc :title (:title task))
       (:summary task) (assoc :summary (:summary task))
       contract (assoc :contract contract)
       constraints (assoc :constraints constraints)
       (:stop-reason task) (assoc :stop_reason (name (:stop-reason task)))
       (:error task) (assoc :error (:error task))
       recovery (assoc :recovery recovery)
       boundary (assoc :boundary_summary boundary)
       checkpoint (assoc :checkpoint checkpoint)
       checkpoint-at (assoc :checkpoint_at (http-common/instant->str deps checkpoint-at))
       resume-hint (assoc :resume_hint resume-hint)
       recovery-brief (assoc :recovery_brief recovery-brief)
       persistent-goal (assoc :persistent_goal (persistent-goal->body deps persistent-goal))
       inspection (assoc :inspection inspection)
       session-links (assoc :session_links session-links)
       stack (assoc :stack stack)
       (:started-at task) (assoc :started_at (http-common/instant->str deps (:started-at task)))
       (:finished-at task) (assoc :finished_at (http-common/instant->str deps (:finished-at task)))))))

(defn- stack-frame->body
  [deps frame]
  (cond-> {:title (:title frame)}
    (:kind frame) (assoc :kind (name (:kind frame)))
    (:child-task-id frame) (assoc :child_task_id (str (:child-task-id frame)))
    (:progress-status frame) (assoc :progress_status (name (:progress-status frame)))
    (:summary frame) (assoc :summary (http-common/truncate-text deps (:summary frame) 240))
    (:next-step frame) (assoc :next_step (http-common/truncate-text deps (:next-step frame) 160))
    (:compressed? frame) (assoc :compressed true)
    (:compressed-count frame) (assoc :compressed_count (:compressed-count frame))))

(defn- stack-view->body
  [deps stack-view]
  (when stack-view
    {:depth (:depth stack-view)
     :current_focus (:current-focus stack-view)
     :root_goal (:root-goal stack-view)
     :frames (mapv #(stack-frame->body deps %) (:frames stack-view))}))

(defn- history-task->body
  ([deps task]
   (history-task->body deps task nil))
  ([deps task history-data]
   (let [{:keys [turns]} (or history-data {})
         turns       (or turns [])
         latest-turn (last turns)
         {:keys [state execution-session-role runtime-view inspection session-links stack]}
         (bridge/task-view
          {:instant->str #(http-common/instant->str deps %)
           :truncate-text #(http-common/truncate-text deps %1 %2)}
          task
          {:compact? true
           :history-data history-data})
         {:keys [recovery checkpoint checkpoint-at resume-hint recovery-brief]
          boundary :boundary-summary} runtime-view
         contract    (history-contract->body (:contract task))
         constraints (:constraints task)
         session-links (not-empty (mapv #(session-link->body deps %) session-links))
         stack       (stack-view->body deps stack)]
     (cond-> {:id          (some-> (:id task) str)
              :session_id  (some-> (:session-id task) str)
              :execution_session_id (some-> (:session-id task) str)
              :channel     (some-> (:channel task) name)
              :type        (some-> (:type task) name)
              :state       (some-> state name)
              :turn_count  (count turns)
              :created_at  (http-common/instant->str deps (:created-at task))
              :updated_at  (http-common/instant->str deps (:updated-at task))}
       execution-session-role (assoc :execution_session_role (name execution-session-role))
       (:current-turn-id task) (assoc :current_turn_id (str (:current-turn-id task)))
       (:title task) (assoc :title (:title task))
       (:summary task) (assoc :summary (:summary task))
       contract (assoc :contract contract)
       constraints (assoc :constraints constraints)
       recovery (assoc :recovery recovery)
       boundary (assoc :boundary_summary boundary)
       checkpoint (assoc :checkpoint checkpoint)
       checkpoint-at (assoc :checkpoint_at (http-common/instant->str deps checkpoint-at))
       resume-hint (assoc :resume_hint resume-hint)
       recovery-brief (assoc :recovery_brief recovery-brief)
       inspection (assoc :inspection inspection)
       session-links (assoc :session_links session-links)
       stack (assoc :stack stack)
       latest-turn (assoc :latest_turn_state (some-> (:state latest-turn) name))
       latest-turn (assoc :latest_turn_summary (http-common/truncate-text deps (:summary latest-turn) 160))
       (:started-at task) (assoc :started_at (http-common/instant->str deps (:started-at task)))
       (:finished-at task) (assoc :finished_at (http-common/instant->str deps (:finished-at task)))))))

(defn- llm-call-summary->body
  [deps entry]
  (cond-> {:id          (str (:id entry))
           :session_id  (some-> (:session-id entry) str)
           :provider_id (some-> (:provider-id entry) name)
           :model       (:model entry)
           :workload    (some-> (:workload entry) name)
           :status      (some-> (:status entry) name)
           :duration_ms (:duration-ms entry)
           :created_at  (http-common/instant->str deps (:created-at entry))}
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
                            :created_at (http-common/instant->str deps created-at)}
                     provider-id (assoc :provider_id (name provider-id))
                     model (assoc :model model)
                     workload (assoc :workload (name workload))))
                 (:related-messages entry)))))

(defn handle-create-session
  ([deps]
   (handle-create-session deps :http))
  ([deps channel]
   (let [{:keys [session-id]} (bridge/create-session! channel)
         sid session-id]
     (http-common/touch-rest-session! deps sid)
     (http-common/json-response deps 200 {:session_id (str sid)}))))

(defn- internal-server-error-response
  [deps ^Throwable throwable]
  (http-common/json-response deps 500 {:error (or (http-common/throwable-message deps throwable)
                                       "internal server error")}))

(defn- chat-request
  ([deps req]
   (chat-request deps req :http))
  ([deps req channel]
   (let [data          (http-common/read-body deps req)
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
       {:response (http-common/json-response deps 400 {:error "missing 'message' field"})}

       (and session-id (not (session-accessible?* deps session-id channel)))
       {:response (http-common/json-response deps 404 {:error "unknown session id"})}

       (and session-id (not (session-active?* deps session-id)))
       {:response (http-common/json-response deps 409 {:error "session closed"})}

       :else
       (let [sid (if session-id
                   (java.util.UUID/fromString session-id)
                   (:session-id (bridge/create-session! channel)))]
         (cancel-rest-session-finalizer!* deps sid)
         {:session-id    sid
          :channel       channel
          :message       message
          :local-doc-ids local-doc-ids
          :artifact-ids  artifact-ids})))))

(defn- process-chat!
  [deps {:keys [session-id channel message local-doc-ids artifact-ids]}]
  (try
    (let [response (bridge/send-message! session-id
                                         message
                                         :channel channel
                                         :local-doc-ids local-doc-ids
                                         :artifact-ids artifact-ids)
          assistant-message (bridge/latest-session-message session-id #{:assistant})
          task              (bridge/current-session-task session-id)
          persistent-goal   (goal/current-goal session-id)
          body              (cond-> {:session_id (str session-id)
                                     :role       "assistant"
                                     :content    response
                                     :message    (when assistant-message
                                                   (session-message->body deps assistant-message))}
                               task (assoc :task (task->body deps task))
                               task (assoc :task_id (some-> (:id task) str))
                               persistent-goal (assoc :goal (persistent-goal->body deps persistent-goal))
                               (:current-turn-id task) (assoc :current_turn_id
                                                              (str (:current-turn-id task))))]
      (http-common/touch-rest-session! deps session-id)
      (http-common/json-response deps 200 body))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))
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
                           (http-common/exception-response deps e))
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

(defn- with-active-session
  [deps session-id expected-channel f]
  (let [sid (http-common/parse-session-id deps session-id)]
    (when (and sid (= expected-channel :http))
      (maybe-resume-http-session!* deps sid expected-channel))
    (cond
      (nil? sid)
      (http-common/json-response deps 400 {:error "invalid session id"})

      (not (session-accessible?* deps sid expected-channel))
      (http-common/json-response deps 404 {:error "session not found"})

      (not (session-active?* deps sid))
      (http-common/json-response deps 409 {:error "session closed"})

      :else
      (do
        (http-common/touch-rest-session! deps sid)
        (f sid (java.util.UUID/fromString sid))))))

(defn handle-get-status
  ([deps session-id]
   (handle-get-status deps session-id nil))
  ([deps session-id expected-channel]
   (with-active-session
    deps
    session-id
    expected-channel
    (fn [sid session-uuid]
      (let [task            (bridge/current-session-task sid)
            persistent-goal (goal/current-goal session-uuid)
            status          (or (when task
                                  (task-runtime-status deps task))
                                (get @(session-statuses-atom deps) sid))]
        (http-common/json-response deps 200
                                   (cond-> {:session_id sid
                                            :status     (status->body deps status)}
                                     task (assoc :task_id (some-> (:id task) str))
                                     persistent-goal (assoc :goal (persistent-goal->body deps persistent-goal))
                                     (:current-turn-id task) (assoc :current_turn_id
                                                                    (str (:current-turn-id task))))))))))

(defn handle-get-current-task
  ([deps session-id]
   (handle-get-current-task deps session-id nil))
  ([deps session-id expected-channel]
   (with-active-session
    deps
    session-id
    expected-channel
    (fn [sid session-uuid]
      (let [task            (bridge/current-session-task sid)
            persistent-goal (goal/current-goal session-uuid)]
        (http-common/json-response deps 200
                                   (cond-> {:session_id sid
                                            :task       (when task
                                                          (task->body deps task))}
                                     task (assoc :task_id (some-> (:id task) str))
                                     task (assoc :task_live (boolean (live-task? task)))
                                     persistent-goal (assoc :goal (persistent-goal->body deps persistent-goal))
                                     (:current-turn-id task) (assoc :current_turn_id
                                                                    (str (:current-turn-id task))))))))))

(defn- with-goal-session
  [deps session-id expected-channel f]
  (with-active-session
   deps
   session-id
   expected-channel
   (fn [_sid session-uuid]
     (f session-uuid))))

(defn- goal-task-id
  [goal]
  (let [value (:last-task-id goal)]
    (cond
      (uuid? value) value
      (string? value) (try
                        (java.util.UUID/fromString value)
                        (catch IllegalArgumentException _ nil))
      :else nil)))

(defn- goal-task-control->body
  [deps result]
  (when result
    (cond-> {:status (some-> (:status result) name)}
      (:error result) (assoc :error (:error result))
      (:task-id result) (assoc :task_id (str (:task-id result)))
      (:session-id result) (assoc :session_id (str (:session-id result)))
      (:task result) (assoc :task (task->body deps (:task result))))))

(defn- goal-ex-response
  [deps ^clojure.lang.ExceptionInfo e]
  (let [data (ex-data e)]
    (http-common/json-response deps
                    (long (or (:status data) 500))
                    {:error (or (:error data) (.getMessage e))})))

(defn- pause-goal-task!
  [goal]
  (when-let [task-id (goal-task-id goal)]
    (when-let [task (db/get-task task-id)]
      (when (contains? #{:running :waiting_input :waiting_approval :resumable :paused}
                       (:state task))
        (bridge/control-task! task-id
                              :pause
                              :context {:session-id (:session-id task)
                                        :channel :http})))))

(defn- resume-goal-task!
  [goal]
  (when-let [task-id (goal-task-id goal)]
    (when-let [task (db/get-task task-id)]
      (when-not (contains? #{:cancelled :failed} (:state task))
        (bridge/control-task! task-id
                              :resume
                              :message (str "Continue working on persistent goal: "
                                            (:text goal))
                              :context {:session-id (:session-id task)
                                        :channel :http})))))

(defn handle-get-goal
  ([deps session-id]
   (handle-get-goal deps session-id nil))
  ([deps session-id expected-channel]
   (with-goal-session
     deps
     session-id
     expected-channel
     (fn [sid]
       (http-common/json-response deps 200
                       {:session_id (str sid)
                        :goal (persistent-goal->body deps (goal/current-goal sid))})))))

(defn handle-set-goal
  ([deps session-id req]
   (handle-set-goal deps session-id req nil))
  ([deps session-id req expected-channel]
   (with-goal-session
     deps
     session-id
     expected-channel
     (fn [sid]
       (try
         (if (session-busy?* deps sid)
           (http-common/json-response deps 409 {:error "session is busy"})
           (let [data (http-common/read-body deps req)
                 text (or (get data "goal")
                          (get data "text"))
                 max-turns (or (get data "max_turns")
                               (get data "max-turns"))
                 success-criteria (or (get data "success_criteria")
                                      (get data "success-criteria"))
                 constraints (get data "constraints")
                 preferences (get data "preferences")
                 budget (get data "budget")
                 resume-policy (or (get data "resume_policy")
                                   (get data "resume-policy"))
                 goal* (goal/set-goal! sid
                                        text
                                        :max-turns max-turns
                                        :success-criteria success-criteria
                                        :constraints constraints
                                        :preferences preferences
                                        :budget budget
                                        :resume-policy resume-policy)]
             (http-common/json-response deps 200
                             {:session_id (str sid)
                              :goal (persistent-goal->body deps goal*)})))
         (catch clojure.lang.ExceptionInfo e
           (goal-ex-response deps e)))))))

(defn handle-pause-goal
  ([deps session-id]
   (handle-pause-goal deps session-id nil))
  ([deps session-id expected-channel]
   (with-goal-session
     deps
     session-id
     expected-channel
     (fn [sid]
       (try
         (let [goal* (goal/pause-goal! sid)
               control (pause-goal-task! goal*)]
           (http-common/json-response deps 200
                           (cond-> {:session_id (str sid)
                                    :goal (persistent-goal->body deps goal*)}
                             control (assoc :task_control
                                            (goal-task-control->body deps control)))))
         (catch clojure.lang.ExceptionInfo e
           (goal-ex-response deps e)))))))

(defn handle-resume-goal
  ([deps session-id]
   (handle-resume-goal deps session-id nil))
  ([deps session-id expected-channel]
   (with-goal-session
     deps
     session-id
     expected-channel
     (fn [sid]
       (try
         (let [goal* (goal/resume-goal! sid)
               control (resume-goal-task! goal*)]
           (http-common/json-response deps 200
                           (cond-> {:session_id (str sid)
                                    :goal (persistent-goal->body deps goal*)}
                             control (assoc :task_control
                                            (goal-task-control->body deps control)))))
         (catch clojure.lang.ExceptionInfo e
           (goal-ex-response deps e)))))))

(defn handle-clear-goal
  ([deps session-id]
   (handle-clear-goal deps session-id nil))
  ([deps session-id expected-channel]
   (with-goal-session
     deps
     session-id
     expected-channel
     (fn [sid]
       (if (session-busy?* deps sid)
         (http-common/json-response deps 409 {:error "session is busy"})
         (do
           (goal/clear-goal! sid)
           (http-common/json-response deps 200
                           {:session_id (str sid)
                            :goal nil})))))))

(defn- interaction-body-key
  [kind]
  (case kind
    :prompt :prompt
    :approval :approval))

(defn- interaction->body
  [deps kind interaction]
  (case kind
    :prompt (prompt->body* deps interaction)
    :approval (approval->body* deps interaction)))

(defn- interaction-submit-work-kind
  [kind]
  (case kind
    :prompt :prompt-reply
    :approval :approval-reply))

(defn- interaction-missing-error
  [kind]
  (str "no pending " (name kind)))

(defn- interaction-stale-error
  [kind]
  (str "stale " (name kind) " id"))

(defn- session-interaction-target
  [deps session-id expected-channel & {:keys [resume? require-active?]}]
  (when (and resume? session-id (= expected-channel :http))
    (maybe-resume-http-session!* deps session-id expected-channel))
  (cond
    (nil? (http-common/parse-session-id deps session-id))
    {:response (http-common/json-response deps 400 {:error "invalid session id"})}

    (not (session-accessible?* deps session-id expected-channel))
    {:response (http-common/json-response deps 404 {:error "session not found"})}

    (and require-active? (not (session-active?* deps session-id)))
    {:response (http-common/json-response deps 409 {:error "session closed"})}

    :else
    {:selector {:session-id session-id}
     :touch-session-id session-id}))

(defn- task-interaction-target
  [deps task-id]
  (try
    (let [uuid (java.util.UUID/fromString task-id)
          task (db/get-task uuid)]
      (if-not task
        {:response (http-common/json-response deps 404 {:error "task not found"})}
        {:selector {:task-id uuid
                    :session-id (:session-id task)}}))
    (catch IllegalArgumentException _
      {:response (http-common/json-response deps 400 {:error "invalid task id"})})))

(defn- render-pending-interaction
  [deps kind target]
  (if-let [response (:response target)]
    response
    (do
      (when-let [session-id (:touch-session-id target)]
        (http-common/touch-rest-session! deps session-id))
      (let [selector (assoc (:selector target) :kind kind)]
        (if-let [interaction (bridge/resolve-pending-interaction selector)]
          (http-common/json-response deps 200
                          {:pending true
                           (interaction-body-key kind)
                           (interaction->body deps kind interaction)})
          (http-common/json-response deps 200 {:pending false}))))))

(defn- prompt-submission
  [deps data]
  (if-not (contains? data "value")
    {:response (http-common/json-response deps 400 {:error "missing value"})}
    {:public-id (get data "prompt_id")
     :value (str (or (get data "value") ""))}))

(defn- approval-submission
  [deps data]
  (let [decision (get data "decision")
        decision* (case decision
                    "allow" :allow
                    "deny" :deny
                    nil)]
    (if-not decision*
      {:response (http-common/json-response deps 400 {:error "invalid decision"})}
      {:public-id (get data "approval_id")
       :value decision*})))

(defn- interaction-submission
  [deps kind data]
  (case kind
    :prompt (prompt-submission deps data)
    :approval (approval-submission deps data)))

(defn- submit-interaction-response
  [deps kind target req]
  (if-let [response (:response target)]
    response
    (let [data (http-common/read-body deps req)
          {:keys [response public-id value]} (interaction-submission deps kind data)]
      (cond
        response
        response

        (not (runtime-state/accepting-new-work?))
        (runtime-draining-response deps (interaction-submit-work-kind kind))

        :else
        (let [{:keys [status]}
              (bridge/submit-interaction! (assoc (:selector target) :kind kind)
                                          public-id
                                          value)]
          (case status
            :missing
            (http-common/json-response deps 404
                            {:error (interaction-missing-error kind)})

            :stale
            (http-common/json-response deps 409
                            {:error (interaction-stale-error kind)})

            (do
              (when-let [session-id (:touch-session-id target)]
                (http-common/touch-rest-session! deps session-id))
              (http-common/json-response deps 200 {:status "recorded"}))))))))

(defn- handle-get-session-interaction
  [deps session-id expected-channel kind]
  (render-pending-interaction
   deps
   kind
   (session-interaction-target deps
                               session-id
                               expected-channel
                               :resume? true
                               :require-active? true)))

(defn- handle-submit-session-interaction
  [deps session-id req expected-channel kind]
  (submit-interaction-response
   deps
   kind
   (session-interaction-target deps session-id expected-channel)
   req))

(defn- handle-get-task-interaction
  [deps task-id kind]
  (render-pending-interaction deps kind (task-interaction-target deps task-id)))

(defn- handle-submit-task-interaction
  [deps task-id req kind]
  (submit-interaction-response
   deps
   kind
   (task-interaction-target deps task-id)
   req))

(defn handle-get-approval
  ([deps session-id]
   (handle-get-approval deps session-id nil))
  ([deps session-id expected-channel]
   (handle-get-session-interaction deps session-id expected-channel :approval)))

(defn handle-get-prompt
  ([deps session-id]
   (handle-get-prompt deps session-id nil))
  ([deps session-id expected-channel]
   (handle-get-session-interaction deps session-id expected-channel :prompt)))

(defn handle-submit-prompt
  ([deps session-id req]
   (handle-submit-prompt deps session-id req nil))
  ([deps session-id req expected-channel]
   (handle-submit-session-interaction
    deps session-id req expected-channel :prompt)))

(defn handle-submit-approval
  ([deps session-id req]
   (handle-submit-approval deps session-id req nil))
  ([deps session-id req expected-channel]
   (handle-submit-session-interaction
    deps session-id req expected-channel :approval)))

(defn handle-get-task-prompt
  [deps task-id]
  (handle-get-task-interaction deps task-id :prompt))

(defn handle-submit-task-prompt
  [deps task-id req]
  (handle-submit-task-interaction deps task-id req :prompt))

(defn handle-get-task-approval
  [deps task-id]
  (handle-get-task-interaction deps task-id :approval))

(defn handle-submit-task-approval
  [deps task-id req]
  (handle-submit-task-interaction deps task-id req :approval))

(defn handle-session-messages
  ([deps session-id]
   (handle-session-messages deps session-id nil))
  ([deps session-id expected-channel]
   (try
     (let [sid (java.util.UUID/fromString session-id)]
       (if-not (session-accessible?* deps sid expected-channel)
         (http-common/json-response deps 404 {:error "session not found"})
         (let [messages (->> (bridge/session-messages sid)
                             (into [] (comp
                                       (filter #(#{:user :assistant} (:role %)))
                                       (map #(session-message->body deps %)))))]
           (http-common/touch-rest-session! deps session-id)
           (http-common/json-response deps 200 {:session_id session-id
                                     :messages   messages}))))
     (catch IllegalArgumentException _
       (http-common/json-response deps 400 {:error "invalid session id"})))))

(defn handle-close-session
  ([deps session-id]
   (handle-close-session deps session-id nil))
  ([deps session-id expected-channel]
   (cond
     (nil? (http-common/parse-session-id deps session-id))
     (http-common/json-response deps 400 {:error "invalid session id"})

     (not (session-accessible?* deps session-id expected-channel))
     (http-common/json-response deps 404 {:error "session not found"})

     :else
     (let [sid    (http-common/parse-session-id deps session-id)
           result (bridge/control-session! sid
                                           :close
                                           :reason "session close requested"
                                           :context {:session-id sid
                                                     :channel (or expected-channel :http)}
                                           :busy? (fn [session-id]
                                                    (session-busy?* deps session-id))
                                           :finalize-session! (fn [session-id]
                                                               (finalize-rest-session!* deps session-id :explicit)))
           {:keys [response-kind status status-key message]} (bridge/session-control-result-view :close result)]
       (case response-kind
         :accepted
         (http-common/json-response deps 202 {:session_id sid
                                   :status status-key
                                   :closing true})

         :completed
         (http-common/json-response deps 200 {:session_id sid
                                   :status status-key
                                   :already_closed (= :already-closed status)})

         :conflict
         (http-common/json-response deps 409 {:error message})

         (http-common/json-response deps 500 {:error "unknown session control result"}))))))

(defn handle-history-sessions
  [deps]
  (let [sessions      (->> (db/list-sessions)
                           (into [] (filter #(contains? history-session-channels
                                                        (:channel %)))))
        history-data  (db/session-history-data (map :id sessions))]
    (http-common/json-response deps 200
                    {:sessions (->> sessions
                                    (into [] (map #(history-session->body deps
                                                                          %
                                                                          (get history-data (:id %))))))})))

(defn handle-history-tasks
  [deps]
  (let [tasks         (db/list-tasks)
        history-data  (db/task-history-data (map :id tasks))]
    (http-common/json-response deps 200
                    {:tasks (->> tasks
                                 (into [] (map #(history-task->body deps
                                                                   %
                                                                   (get history-data (:id %))))))})))

(defn handle-get-task
  [deps task-id]
  (try
    (let [uuid (java.util.UUID/fromString task-id)]
      (if-let [{:keys [task turns]} (bridge/task-detail-view uuid)]
        (http-common/json-response deps 200
                        {:task  (task->body deps task)
                         :turns (mapv (fn [{:keys [turn items]}]
                                        (task-turn->body deps turn items))
                                     turns)})
        (http-common/json-response deps 404 {:error "task not found"})))
    (catch IllegalArgumentException _
      (http-common/json-response deps 400 {:error "invalid task id"}))))

(defn handle-get-task-events
  [deps task-id]
  (try
    (let [uuid (java.util.UUID/fromString task-id)]
      (if-let [{:keys [events]} (bridge/task-event-history uuid)]
        (http-common/json-response deps 200
                        {:task_id (str uuid)
                         :events  (mapv #(task-event->body deps %) events)})
        (http-common/json-response deps 404 {:error "task not found"})))
    (catch IllegalArgumentException _
      (http-common/json-response deps 400 {:error "invalid task id"}))))

(defn handle-get-live-task-events
  [deps task-id req]
  (try
    (let [uuid   (java.util.UUID/fromString task-id)
          task   (db/get-task uuid)
          params (http-common/parse-query-string deps (:query-string req))
          after  (or (some-> (get params "after") parse-long) 0)]
      (if-not task
        (http-common/json-response deps 404 {:error "task not found"})
        (let [{:keys [next-index events]} ((:task-runtime-events-after deps) uuid after)
              events* (mapv #(task-event->body deps %) events)]
          (http-common/json-response deps 200
                          {:task_id (str uuid)
                           :after after
                           :next_stream_index (long (or next-index 0))
                           :events events*}))))
    (catch IllegalArgumentException _
      (http-common/json-response deps 400 {:error "invalid task id"}))))

(defn- task-live-events-after
  [deps task-id after]
  ((:task-runtime-events-after deps) task-id after))

(defn- task-event-stream-after
  [deps req]
  (let [params         (http-common/parse-query-string deps (:query-string req))
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
        (http-common/json-response deps 404 {:error "task not found"})
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
      (http-common/json-response deps 400 {:error "invalid task id"}))))

(defn- task-control-response
  [deps intent result]
  (let [{:keys [status response-kind status-key message]} (bridge/control-result-view intent result)]
    (case response-kind
      :missing
      (http-common/json-response deps 404 {:error "task not found"})

      :conflict
      (http-common/json-response deps 409 {:error (or (:error result) message)
                                :task_id (some-> (:task-id result) str)
                                :session_id (some-> (:session-id result) str)
                                :execution_session_id (some-> (:session-id result) str)})

      :unavailable
      (http-common/json-response deps 503 {:error (or (:error result) message)
                                :task_id (some-> (:task-id result) str)
                                :session_id (some-> (:session-id result) str)
                                :execution_session_id (some-> (:session-id result) str)})

      :accepted
      (http-common/json-response deps 202
                      (cond-> {:status status-key
                               :task_id (some-> (:task-id result) str)
                               :session_id (some-> (:session-id result) str)
                               :execution_session_id (some-> (:session-id result) str)}
                        (= status :forking)
                        (assoc :task (when-let [task (:task result)]
                                       (task->body deps task)))))

      :completed
      (http-common/json-response deps 200
                      (cond-> {:status status-key}
                        (:session-id result)
                        (assoc :execution_session_id (some-> (:session-id result) str))

                        (contains? #{:already-paused :already-stopped} status)
                        (assoc :task_id (some-> (:task-id result) str)
                               :session_id (some-> (:session-id result) str))
                        (contains? #{:paused :stopped} status)
                        (assoc :task (when-let [task (:task result)]
                                       (task->body deps task)))))

      (http-common/json-response deps 500 {:error "unknown task control result"}))))

(defn- handle-task-control-intent
  [deps task-id intent & {:keys [message]}]
  (try
    (let [uuid (java.util.UUID/fromString task-id)
          task (db/get-task uuid)]
      (task-control-response deps
                             intent
                             (bridge/control-task! uuid
                                                   intent
                                                   :message message
                                                   :context {:session-id (:session-id task)
                                                             :channel :http})))
    (catch IllegalArgumentException _
      (http-common/json-response deps 400 {:error "invalid task id"}))))

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
  (let [data    (http-common/read-body deps req)
        message (get data "message")]
    (handle-task-control-intent deps task-id :steer :message message)))

(defn handle-fork-task
  [deps task-id req]
  (let [data    (http-common/read-body deps req)
        message (get data "message")]
    (handle-task-control-intent deps task-id :fork :message message)))

(defn handle-resume-task
  [deps task-id req]
  (let [data    (http-common/read-body deps req)
        message (get data "message")]
    (handle-task-control-intent deps task-id :resume :message message)))

(defn handle-history-schedules
  [deps]
  (http-common/json-response deps 200
                  {:schedules (->> (schedule/list-schedules)
                                   (sort-by (fn [sched]
                                              (or (http-common/date->millis deps (:last-run sched))
                                                  (http-common/date->millis deps (:next-run sched))
                                                  Long/MIN_VALUE))
                                            >)
                                   (into [] (map #(history-schedule->body deps %))))}))

(defn handle-history-schedule-runs
  [deps schedule-id]
  (try
    (let [sid   (http-common/parse-keyword-id deps schedule-id "schedule_id")
          sched (schedule/get-schedule sid)]
      (if-not sched
        (http-common/json-response deps 404 {:error "schedule not found"})
        (http-common/json-response deps 200
                        {:schedule (history-schedule->body deps sched)
                         :runs     (into [] (map #(history-run->body deps %))
                                         (schedule/schedule-history sid 20))})))
    (catch clojure.lang.ExceptionInfo e
      (http-common/exception-response deps e))))

(defn handle-list-llm-calls
  [deps req]
  (let [params         (http-common/parse-query-string deps (:query-string req))
        limit          (or (some-> (get params "limit") parse-long) 50)
        raw-session-id (get params "session_id")
        session-id     (some-> raw-session-id (http-common/parse-session-id deps))]
    (if (and raw-session-id (nil? session-id))
      (http-common/json-response deps 400 {:error "invalid session id"})
      (http-common/json-response deps 200
                      {:calls (into [] (map #(llm-call-summary->body deps %))
                                    (db/list-llm-calls (min limit 200) session-id))}))))

(defn handle-get-llm-call
  [deps call-id]
  (try
    (let [uuid  (java.util.UUID/fromString call-id)
          entry (db/get-llm-call uuid)]
      (if entry
        (http-common/json-response deps 200 {:call (llm-call-detail->body deps entry)})
        (http-common/json-response deps 404 {:error "call not found"})))
    (catch IllegalArgumentException _
      (http-common/json-response deps 400 {:error "invalid call id"}))))

(defn handle-session-audit
  ([deps session-id]
   (handle-session-audit deps session-id nil))
  ([deps session-id expected-channel]
   (try
     (let [sid (java.util.UUID/fromString session-id)]
       (if-not (session-accessible?* deps sid expected-channel)
         (http-common/json-response deps 404 {:error "session not found"})
         (let [events (mapv #(audit-event->body deps %)
                            (db/session-audit-events sid 1000))]
           (http-common/touch-rest-session! deps session-id)
           (http-common/json-response deps 200 {:session_id session-id
                                     :events     events}))))
     (catch IllegalArgumentException _
       (http-common/json-response deps 400 {:error "invalid session id"})))))
