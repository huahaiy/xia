(ns xia.bridge
  "Bridge/session-runner facade between channel surfaces and Xia core runtime.

  Channels should enter the autonomous runtime through this namespace so terminal,
  HTTP/WebSocket, command, IDE, and messaging integrations share one shape for
  message dispatch, interaction replies, controls, and runtime adapter wiring."
  (:require [taoensso.timbre :as log]
            [xia.agent :as agent]
            [xia.agent.task-runtime :as task-runtime]
            [xia.agent.tools :as agent-tools]
            [xia.audit :as audit]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.hippocampus :as hippo]
            [xia.llm :as llm]
            [xia.prompt :as prompt]
            [xia.session-lifecycle :as session-life]
            [xia.task-event :as task-event]
            [xia.task-inspection :as task-inspection]
            [xia.working-memory :as wm])
  (:import [java.util Date]))

(def ^:private max-live-task-runtime-events 200)
(defonce ^:private installed-runtime-atom (atom nil))

(defn make-runtime
  []
  {:task-runtime-events-atom (atom {})
   :task-runtime-stream-subscribers-atom (atom {})})

(declare clear-runtime!)

(defn install-runtime!
  [runtime]
  (when-let [current @installed-runtime-atom]
    (when-not (identical? current runtime)
      (clear-runtime! current)))
  (reset! installed-runtime-atom runtime)
  runtime)

(defn- current-runtime
  []
  (or @installed-runtime-atom
      (throw (ex-info "Bridge runtime is not installed"
                      {:component :xia/bridge-runtime}))))

(defn clear-runtime!
  ([]
   (when-let [runtime @installed-runtime-atom]
     (clear-runtime! runtime))
   nil)
  ([runtime]
   (when runtime
     (reset! (:task-runtime-events-atom runtime) {})
     (reset! (:task-runtime-stream-subscribers-atom runtime) {})
     (when (identical? runtime @installed-runtime-atom)
       (reset! installed-runtime-atom nil)))
   nil))

(defn register-channel-adapter!
  "Register the interaction adapter for a user-facing channel.

  Adapter keys match `xia.prompt/register-channel-adapter!`:
  `:prompt`, `:approval`, `:status`, `:assistant-message`, and `:runtime-event`."
  [channel adapter]
  (prompt/register-channel-adapter! channel adapter))

(defn clear-channel-adapter!
  "Remove all registered bridge handlers for a channel."
  [channel]
  (prompt/clear-channel-adapter! channel))

(defn create-session!
  "Create a user-facing session for `channel` and initialize working memory.

  Returns a small runner descriptor that channel code can keep or expose."
  ([channel]
   (session-life/create! channel))
  ([channel opts]
   (session-life/create! channel opts)))

(defn resume-session!
  "Resume an inactive session and ensure working memory is ready."
  [session-id & {:as opts}]
  (apply session-life/resume! session-id (mapcat identity opts)))

(defn finalize-session!
  "Finalize a session through the shared lifecycle path."
  [session-id & {:as opts}]
  (apply session-life/finalize! session-id (mapcat identity opts)))

(defn finalize-channel-session!
  "Finalize a channel-owned session through the shared lifecycle path."
  [session-id channel & {:keys [reason consolidation-mode mark-inactive?]
                         :or   {reason :channel-close
                                mark-inactive? true}}]
  (finalize-session! session-id
                     :reason reason
                     :default-channel channel
                     :clear-state! session-life/clear-session-state!
                     :mark-inactive? mark-inactive?
                     :consolidation-mode consolidation-mode))

(defn active-channel-sessions
  "Return active sessions for the supplied channel set."
  [channels]
  (let [channels* (set channels)]
    (->> (db/list-sessions {:include-workers? true})
         (filter (fn [{:keys [channel active?]}]
                   (and active?
                        (contains? channels* channel))))
         vec)))

(defn finalize-active-channel-sessions!
  "Finalize active sessions belonging to the supplied channel set."
  [channels finalize! & {:keys [reason]
                         :or   {reason :server-stop}}]
  (let [sessions (active-channel-sessions channels)]
    (doseq [{:keys [id]} sessions]
      (finalize! id reason))
    (count sessions)))

(defn runtime-event-store
  "Return a bridge runtime-event store.
   With no args, uses the installed bridge runtime. The two-atom arity remains
   for isolated tests and temporary adapters."
  ([]
   (runtime-event-store (current-runtime)))
  ([runtime]
   {:events-atom (:task-runtime-events-atom runtime)
    :subscribers-atom (:task-runtime-stream-subscribers-atom runtime)})
  ([events-atom subscribers-atom]
   {:events-atom events-atom
    :subscribers-atom subscribers-atom}))

(defn append-task-runtime-event!
  "Append a task runtime event to a bounded per-task live event buffer."
  [store event]
  (when-let [task-id (some-> (:task-id event) str)]
    (let [received-at (Date.)]
      (-> (swap! (:events-atom store)
                 (fn [state]
                   (let [{:keys [next-index events]} (get state task-id)
                         next-index* (inc (long (or next-index 0)))
                         event* (assoc event
                                       :stream-index next-index*
                                       :received-at received-at)
                         events* (conj (vec (or events [])) event*)
                         trimmed (if (> (count events*) max-live-task-runtime-events)
                                   (subvec events* (- (count events*) max-live-task-runtime-events))
                                   events*)]
                     (assoc state task-id {:next-index next-index*
                                           :events trimmed}))))
          (get task-id)
          :events
          last))))

(defn task-runtime-events-after
  "Return buffered task runtime events after `stream-index`."
  [store task-id stream-index]
  (let [{:keys [next-index events]} (get @(:events-atom store) (str task-id))
        after (long (or stream-index 0))]
    {:next-index (long (or next-index 0))
     :events (->> (or events [])
                  (filter #(> (long (or (:stream-index %) 0)) after))
                  vec)}))

(defn latest-task-runtime-status-event
  "Return the latest buffered `:task.status` runtime event for a task."
  [store task-id]
  (some->> (get @(:events-atom store) (str task-id))
           :events
           reverse
           (some #(when (= :task.status (:type %)) %))))

(defn register-task-runtime-event-subscriber!
  [store task-id subscriber-id callback]
  (when (and task-id subscriber-id callback)
    (swap! (:subscribers-atom store)
           update
           (str task-id)
           (fnil assoc {})
           subscriber-id
           callback)))

(defn unregister-task-runtime-event-subscriber!
  [store task-id subscriber-id]
  (when (and task-id subscriber-id)
    (swap! (:subscribers-atom store)
           (fn [state]
             (let [task-key (str task-id)
                   subscribers (dissoc (get state task-key {}) subscriber-id)]
               (if (seq subscribers)
                 (assoc state task-key subscribers)
                 (dissoc state task-key)))))))

(defn notify-task-runtime-event-subscribers!
  [store event]
  (when-let [task-id (some-> (:task-id event) str)]
    (doseq [[subscriber-id callback] (get @(:subscribers-atom store) task-id)]
      (try
        (callback event)
        (catch Exception e
          (log/warn e "Failed to deliver runtime event to task stream subscriber"
                    "task" task-id
                    "subscriber" subscriber-id)
          (unregister-task-runtime-event-subscriber! store task-id subscriber-id))))))

(defn handle-task-runtime-event!
  [store event]
  (when-let [event* (append-task-runtime-event! store event)]
    (notify-task-runtime-event-subscribers! store event*)
    event*))

(defn send-message!
  "Run one user message through Xia for an existing session.

  Options are passed through to `xia.agent/process-message` so callers can provide
  channel, persistence, local doc, artifact, and future runner-scoped options
  without depending on agent internals.

  Runner-scoped `:request-budget-guard` and `:request-observer` options are
  installed around the run instead of being forwarded to the agent."
  [session-id user-message & {:keys [request-budget-guard request-observer]
                              :as opts}]
  (let [agent-opts (dissoc opts :request-budget-guard :request-observer)]
    (binding [llm/*request-budget-guard* (or request-budget-guard
                                            llm/*request-budget-guard*)
              llm/*request-observer* (or request-observer
                                         llm/*request-observer*)]
      (apply agent/process-message session-id user-message (mapcat identity agent-opts)))))

(defn interaction-context
  "Return the current bridge interaction context."
  []
  prompt/*interaction-context*)

(defn working-memory-context
  "Return the channel-facing working-memory context for a session."
  [session-id]
  (wm/wm->context session-id))

(defn session-topics
  "Return the current working-memory topic summary for a session."
  [session-id]
  (:topics (wm/get-wm session-id)))

(defn clear-session-autonomy-state!
  "Clear autonomy state and snapshot working memory for a session."
  [session-id]
  (wm/clear-autonomy-state! session-id)
  (wm/snapshot! session-id))

(defn record-session-conversation!
  "Persist a session conversation into long-term memory."
  [session-id channel & {:as opts}]
  (apply hippo/record-conversation! session-id channel (mapcat identity opts)))

(defn installed-tools
  "Return installed tools for channel presentation."
  []
  (db/list-tools))

(defn session-messages
  "Return persisted messages for a channel session."
  [session-id]
  (db/session-messages session-id))

(defn latest-session-message
  "Return the latest persisted message for a channel session."
  ([session-id]
   (db/latest-session-message session-id))
  ([session-id roles]
   (db/latest-session-message session-id roles)))

(defn session-external-meta
  "Return transport routing metadata for an externally addressed session."
  [session-id]
  (or (db/session-external-meta session-id) {}))

(defn ensure-external-session!
  "Find or create a channel session keyed by external transport identity."
  [channel external-key external-meta & {:keys [label]}]
  (let [existing (db/find-session-by-external-key external-key)]
    (if-let [session-id (:id existing)]
      (do
        (resume-session! session-id :expected-channel channel)
        (db/save-session-external-meta! session-id external-meta)
        session-id)
      (:session-id (create-session! channel
                                    {:label label
                                     :external-key external-key
                                     :external-meta external-meta})))))

(defn record-external-user-message!
  "Persist and audit a user message received from an external transport."
  [session-id channel user-message external-message-id external-sender]
  (let [message-id (db/add-message! session-id :user user-message
                                    :external-sender external-sender)]
    (audit/log! {:session-id session-id
                 :channel channel}
                {:actor :user
                 :type :user-message
                 :message-id message-id
                 :data (cond-> {:external_message_id external-message-id}
                         external-sender (assoc :external_sender external-sender)
                         true (assoc :messaging true))})
    message-id))

(defn memory-consolidation-summary
  "Return current long-term memory consolidation status."
  []
  (hippo/consolidation-summary))

(defn knowledge-decay-settings
  "Return resolved knowledge decay settings."
  []
  (hippo/knowledge-decay-settings))

(defn knowledge-decay-config-resolutions
  "Return config-resolution metadata for knowledge decay settings."
  []
  (hippo/knowledge-decay-config-resolutions))

(defn run-memory-maintenance!
  "Run pending memory consolidation and knowledge maintenance."
  [now]
  (hippo/consolidate-if-pending!)
  (hippo/maintain-knowledge! now))

(defn clear-working-memory!
  "Clear installed working memory for a session."
  [session-id]
  (wm/clear-wm! session-id))

(defn current-session-task
  "Return the currently active task for a channel session."
  [session-id]
  (when-let [sid (session-life/session-uuid session-id)]
    (db/current-session-task sid)))

(defn current-session-task-id
  "Return the current task id for a channel session, if one exists."
  [session-id]
  (some-> (current-session-task session-id) :id))

(defn current-task-context
  "Return the current task, autonomy state, and compact inspection for a session."
  ([session-id]
   (current-task-context session-id true))
  ([session-id compact?]
   (when-let [task (current-session-task session-id)]
     (let [session-id*     (:session-id task)
           autonomy-state (task-runtime/inspect-runtime-autonomy-state session-id* (:id task))
           inspection (task-inspection/task-inspection
                       {:truncate-text agent-tools/truncate-summary}
                       task
                       autonomy-state
                       compact?)]
       {:task task
        :autonomy-state autonomy-state
        :inspection inspection}))))

(defn task-autonomy-state
  "Return the runtime autonomy state for `task` or the supplied session/task ids."
  ([task]
   (task-autonomy-state (:session-id task) (:id task)))
  ([session-id task-id]
   (task-runtime/inspect-runtime-autonomy-state session-id task-id)))

(defn task-runtime-view
  "Return channel-facing runtime metadata derived from a persisted task."
  [task]
  {:recovery (task-runtime/task-recovery task)
   :boundary-summary (task-runtime/task-boundary-summary task)
   :checkpoint (task-runtime/task-checkpoint task)
   :checkpoint-at (task-runtime/task-checkpoint-at task)
   :resume-hint (task-runtime/task-resume-hint task)
   :recovery-brief (task-runtime/task-recovery-brief task)})

(defn task-inspection
  "Return a task inspection view for channel presentation."
  ([opts task autonomy-state]
   (task-inspection/task-inspection opts task autonomy-state))
  ([opts task autonomy-state compact?]
   (task-inspection/task-inspection opts task autonomy-state compact?))
  ([opts task autonomy-state compact? history-data]
   (task-inspection/task-inspection opts task autonomy-state compact? history-data)))

(defn- task-execution-session-role
  [task]
  (some (fn [{:keys [session-id role]}]
          (when (= session-id (:session-id task))
            role))
        (:session-links task)))

(defn- task-session-link-views
  [task]
  (let [execution-session-id (:session-id task)]
    (not-empty
     (mapv (fn [{:keys [session-id] :as link}]
             (let [execution-current? (= session-id execution-session-id)]
               (assoc link
                      :current? execution-current?
                      :execution-current? execution-current?)))
           (:session-links task)))))

(defn- task-stack-view
  [autonomy-state]
  (when autonomy-state
    (let [stack* (vec (:stack (autonomous/normalize-state autonomy-state)))
          tip    (peek stack*)
          root   (first stack*)]
      {:depth (count stack*)
       :current-focus (:title tip)
       :root-goal (:title root)
       :frames stack*})))

(defn- task-inspection-view
  [opts task autonomy-state options]
  (let [compact-provided? (contains? options :compact?)
        history-provided? (contains? options :history-data)
        compact?          (:compact? options)
        history-data      (:history-data options)]
    (cond
      history-provided?
      (task-inspection/task-inspection opts task autonomy-state compact? history-data)

      compact-provided?
      (task-inspection/task-inspection opts task autonomy-state compact?)

      :else
      (task-inspection/task-inspection opts task autonomy-state))))

(defn task-view
  "Return the channel-facing task projection inputs for HTTP, terminal, and
   other adapters. Wire-format rendering stays in the channel."
  ([opts task]
   (task-view opts task {}))
  ([opts task options]
   (let [autonomy-state (or (:autonomy-state options)
                            (task-autonomy-state task))
         runtime        (get-in task [:meta :runtime])]
     {:task task
      :autonomy-state autonomy-state
      :runtime runtime
      :state (or (:state runtime) (:state task))
      :execution-session-role (task-execution-session-role task)
      :runtime-view (task-runtime-view task)
      :inspection (task-inspection-view opts task autonomy-state options)
      :session-links (task-session-link-views task)
      :stack (task-stack-view autonomy-state)})))

(defn task-detail-view
  "Return a persisted task plus its turns and turn items."
  [task-id]
  (when-let [task (db/get-task task-id)]
    (let [turns (db/task-turns task-id)]
      {:task task
       :turns (mapv (fn [turn]
                      {:turn turn
                       :items (db/turn-items (:id turn))})
                    turns)})))

(defn task-event-history
  "Return persisted task events derived from task turns and items."
  [task-id]
  (when-let [task (db/get-task task-id)]
    (let [turns      (db/task-turns task-id)
          turn-items (into {}
                           (map (fn [turn]
                                  [(:id turn) (db/turn-items (:id turn))]))
                           turns)]
      {:task-id task-id
       :task task
       :events (task-event/task-events task turns turn-items)})))

(defn session-cancelled?
  "True when the session has been cancelled or interrupted."
  [session-id]
  (agent/session-cancelled? session-id))

(defn cancel-session!
  "Request cancellation for an active session run."
  ([session-id]
   (agent/cancel-session! session-id))
  ([session-id reason]
   (agent/cancel-session! session-id reason)))

(defn pending-interaction
  "Return a pending prompt or approval by session/task/channel selector."
  [selector]
  (prompt/pending-interaction selector))

(defn register-interaction!
  "Register a pending prompt or approval interaction."
  [interaction]
  (prompt/register-interaction! interaction))

(defn clear-pending-interaction!
  "Clear a pending prompt or approval interaction."
  [selector]
  (prompt/clear-pending-interaction! selector))

(defn request-channel-interaction!
  "Register a channel interaction, notify the transport, wait for a result, then clear it."
  [interaction notify! await!]
  (register-interaction! interaction)
  (try
    (when notify!
      (notify! interaction))
    (when await!
      (await! interaction))
    (finally
      (clear-pending-interaction! {:interaction-id (:interaction-id interaction)}))))

(defn resolve-pending-interaction
  "Resolve the best pending interaction for a selector."
  [selector]
  (prompt/resolve-pending-interaction selector))

(defn submit-interaction!
  "Validate a public prompt/approval id and deliver `value` to the pending run."
  [selector expected-public-id value]
  (prompt/deliver-validated-interaction! selector expected-public-id value))

(defn submit-freeform-reply!
  "Coerce and deliver a free-form reply such as YES/NO/CANCEL."
  [selector raw-reply]
  (prompt/submit-freeform-interaction-reply! selector raw-reply))

(defn interaction-retry-text
  "Return a user-facing retry hint for an invalid interaction reply."
  [interaction]
  (prompt/interaction-retry-text interaction))

(defn parse-control-intent
  "Parse a transport message into a control intent."
  [raw-message]
  (prompt/parse-control-intent raw-message))

(defn control-result-text
  "Render task-control result text."
  [intent result]
  (prompt/control-result-text intent result))

(defn control-result-view
  "Return normalized task-control presentation."
  [intent result]
  (prompt/control-result-view intent result))

(defn session-control-result-text
  "Render session-control result text."
  [intent result]
  (prompt/session-control-result-text intent result))

(defn session-control-result-view
  "Return normalized session-control presentation."
  [intent result]
  (prompt/session-control-result-view intent result))

(defn- task-control-handlers
  []
  {:pause-task! agent/pause-task!
   :resume-task! agent/resume-task!
   :stop-task! agent/stop-task!
   :interrupt-task! agent/interrupt-task!
   :steer-task! agent/steer-task!
   :fork-task! agent/fork-task!})

(defn- default-finalize-session!
  [session-id]
  (session-life/finalize! session-id
                          :clear-state! session-life/clear-session-state!))

(defn control-task!
  "Apply a task control intent through the shared bridge.

  `intent` is typically one of `:pause`, `:resume`, `:stop`, `:interrupt`,
  `:steer`, or `:fork`."
  [task-id intent & {:keys [message context]}]
  (prompt/apply-task-control-intent! (task-control-handlers)
                                     task-id
                                     intent
                                     :message message
                                     :context context))

(defn control-session!
  "Apply a session control intent through the shared bridge.

  Optional handlers:
  - `:busy?` checks whether a close should cancel instead of finalize.
  - `:finalize-session!` performs channel-specific finalization for close."
  [session-id intent & {:keys [reason context busy? finalize-session!]}]
  (prompt/apply-session-control-intent!
   (cond-> {:cancel-session! agent/cancel-session!
            :finalize-session! default-finalize-session!}
     busy? (assoc :busy? busy?)
     finalize-session! (assoc :finalize-session! finalize-session!))
   session-id
   intent
   :reason reason
   :context context))

(defn apply-control-message!
  "Parse and apply a free-form control message for a session.

  Returns nil when `raw-message` is not a control request. Otherwise returns a map
  with `:intent`, `:scope`, `:result`, and user-facing `:text`."
  [session-id channel raw-message]
  (when-let [intent (parse-control-intent raw-message)]
    (if-let [task (db/current-session-task session-id)]
      (let [result (control-task! (:id task)
                                  intent
                                  :context {:session-id session-id
                                            :channel channel})]
        {:intent intent
         :scope :task
         :task task
         :result result
         :text (control-result-text intent result)})
      (if (= :interrupt intent)
        (let [result (control-session! session-id
                                       :interrupt
                                       :reason "session cancel requested"
                                       :context {:session-id session-id
                                                 :channel channel})]
          {:intent intent
           :scope :session
           :result result
           :text (session-control-result-text :interrupt result)})
        (if (= :close intent)
          (let [result (control-session! session-id
                                         :close
                                         :reason "session close requested"
                                         :context {:session-id session-id
                                                   :channel channel})]
            {:intent intent
             :scope :session
             :result result
             :text (session-control-result-text :close result)})
          (let [result {:status :missing}]
            {:intent intent
             :scope :task
             :result result
             :text (control-result-text intent result)}))))))

(defn status!
  "Publish a runtime status update for the current bridge interaction context."
  [status]
  (prompt/status! status))

(defn assistant-message!
  "Publish an assistant message for the current bridge interaction context."
  [message]
  (prompt/assistant-message! message))

(defn runtime-event!
  "Publish a typed runtime event for the current bridge interaction context."
  [event]
  (prompt/runtime-event! event))
