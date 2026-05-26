(ns xia.bridge
  "Bridge/session-runner facade between channel surfaces and Xia core runtime.

  Channels should enter the autonomous runtime through this namespace so terminal,
  HTTP/WebSocket, command, IDE, and messaging integrations share one shape for
  message dispatch, interaction replies, controls, and runtime adapter wiring."
  (:require [xia.agent :as agent]
            [xia.db :as db]
            [xia.prompt :as prompt]
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
   (let [session-id (db/create-session! channel)]
     (wm/ensure-wm! session-id)
     {:session-id session-id
      :channel channel}))
  ([channel opts]
   (let [session-id (if (some? opts)
                      (db/create-session! channel opts)
                      (db/create-session! channel))]
     (wm/ensure-wm! session-id)
     {:session-id session-id
      :channel channel})))

(defn send-message!
  "Run one user message through Xia for an existing session.

  Options are passed through to `xia.agent/process-message` so callers can provide
  channel, persistence, local doc, artifact, and future runner-scoped options
  without depending on agent internals."
  [session-id user-message & {:as opts}]
  (apply agent/process-message session-id user-message (mapcat identity opts)))

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
   (cond-> {:cancel-session! agent/cancel-session!}
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
        (let [result {:status :missing}]
          {:intent intent
           :scope :task
           :result result
           :text (control-result-text intent result)})))))

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
