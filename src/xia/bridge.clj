(ns xia.bridge
  "Bridge/session-runner facade between channel surfaces and Xia core runtime.

  Channels should enter the autonomous runtime through this namespace so terminal,
  HTTP/WebSocket, command, IDE, and messaging integrations share one shape for
  message dispatch, interaction replies, controls, and runtime adapter wiring."
  (:require [xia.agent :as agent]
            [xia.agent.task-runtime :as task-runtime]
            [xia.agent.tools :as agent-tools]
            [xia.db :as db]
            [xia.hippocampus :as hippo]
            [xia.prompt :as prompt]
            [xia.session-lifecycle :as session-life]
            [xia.task-inspection :as task-inspection]
            [xia.working-memory :as wm]))

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

(defn send-message!
  "Run one user message through Xia for an existing session.

  Options are passed through to `xia.agent/process-message` so callers can provide
  channel, persistence, local doc, artifact, and future runner-scoped options
  without depending on agent internals."
  [session-id user-message & {:as opts}]
  (apply agent/process-message session-id user-message (mapcat identity opts)))

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

(defn clear-working-memory!
  "Clear installed working memory for a session."
  [session-id]
  (wm/clear-wm! session-id))

(defn current-task-context
  "Return the current task, autonomy state, and compact inspection for a session."
  ([session-id]
   (current-task-context session-id true))
  ([session-id compact?]
   (when-let [task (db/current-session-task session-id)]
     (let [autonomy-state (task-runtime/inspect-runtime-autonomy-state session-id (:id task))
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
