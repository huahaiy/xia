(ns xia.prompt
  "Interactive user prompt — allows tool execution to pause and ask
   the user for input (e.g., credentials for a one-time login).

   Channels register prompt/approval handlers keyed by channel.
   Tool code calls (prompt! ...) or privileged tool execution calls
   (approve! ...) which delegates to the current channel handler."
  (:require [clojure.string :as str]
            [xia.audit :as audit]))

;; ---------------------------------------------------------------------------
;; Prompt callback registry
;; ---------------------------------------------------------------------------

(defonce ^:private prompt-handlers (atom {}))
(defonce ^:private approval-handlers (atom {}))
(defonce ^:private status-handlers (atom {}))
(defonce ^:private assistant-message-handlers (atom {}))
(defonce ^:private runtime-event-handlers (atom {}))
(defonce ^:private pending-interactions
  (atom {:by-id {}
         :by-session {}
         :by-task {}}))

(def ^:private handler-registry
  {:prompt prompt-handlers
   :approval approval-handlers
   :status status-handlers
   :assistant-message assistant-message-handlers
   :runtime-event runtime-event-handlers})

(def ^:dynamic *interaction-context*
  "Dynamic execution context for tool interactions, e.g. {:channel :terminal :session-id ...}."
  nil)

(declare deliver-pending-interaction!)

(defn- current-channel []
  (or (:channel *interaction-context*) :default))

(defn- resolve-handler
  [handlers]
  (let [channel (current-channel)
        m       @handlers]
    (or (get m channel)
        (get m :default))))

(defn- invoke-runtime-hook!
  [hook-key payload]
  (when-let [f (get *interaction-context* hook-key)]
    (try
      (f payload)
      (catch Throwable _
        nil))))

(defn- register-handler!
  [handlers channel f]
  (swap! handlers
         (fn [m]
           (if f
             (assoc m channel f)
             (dissoc m channel)))))

(defn register-channel-adapter!
  "Register the shared interaction adapter for a channel.
   Supported keys are:
   - `:prompt`
   - `:approval`
   - `:status`
   - `:assistant-message`
   - `:runtime-event`

   Any omitted key is cleared for that channel, so the adapter map acts as the
   full interaction surface for that channel."
  [channel adapter]
  (doseq [[k handlers] handler-registry]
    (register-handler! handlers channel (get adapter k)))
  nil)

(defn clear-channel-adapter!
  "Remove all registered interaction handlers for a channel."
  [channel]
  (register-channel-adapter! channel {})
  nil)

(defn- normalize-interaction-selector
  [{:keys [interaction-id session-id task-id] :as selector}]
  (cond-> selector
    interaction-id (assoc :interaction-id (str interaction-id))
    session-id (assoc :session-id (str session-id))
    task-id (assoc :task-id (str task-id))))

(defn- interaction-id-for-selector
  [state selector]
  (let [{:keys [interaction-id task-id session-id]} (normalize-interaction-selector selector)]
    (or interaction-id
        (when task-id
          (get-in state [:by-task task-id]))
        (when session-id
          (get-in state [:by-session session-id])))))

(defn- dissoc-interaction*
  [state interaction-id]
  (if-let [{:keys [session-id task-id]} (get-in state [:by-id interaction-id])]
    (cond-> (update state :by-id dissoc interaction-id)
      session-id (update :by-session
                         (fn [m]
                           (if (= interaction-id (get m session-id))
                             (dissoc m session-id)
                             m)))
      task-id (update :by-task
                      (fn [m]
                        (if (= interaction-id (get m task-id))
                          (dissoc m task-id)
                          m))))
    state))

(defn register-interaction!
  "Register a pending channel interaction. Interactions are correlated by
   `:task-id` when present, otherwise by `:session-id`.

   Required/recognized keys:
   - `:kind`
   - `:response`
   - `:interaction-id` (optional, generated when absent)
   - `:session-id` (optional, defaults from `*interaction-context*`)
   - `:task-id` (optional, defaults from `*interaction-context*`)
   - `:channel` (optional, defaults from `*interaction-context*`)
   - `:created-at` (optional, defaults to now)"
  [{:keys [interaction-id session-id task-id channel created-at] :as interaction}]
  (let [context      *interaction-context*
        interaction* (cond-> interaction
                       (nil? interaction-id) (assoc :interaction-id (str (random-uuid)))
                       (nil? session-id) (assoc :session-id (:session-id context))
                       (nil? task-id) (assoc :task-id (:task-id context))
                       (nil? channel) (assoc :channel (or (:channel context) (current-channel)))
                       (nil? created-at) (assoc :created-at (java.util.Date.)))
        interaction* (-> interaction*
                         (update :session-id #(some-> % str))
                         (update :task-id #(some-> % str))
                         (update :interaction-id str))]
    (swap! pending-interactions
           (fn [state]
             (let [state* (cond-> state
                            (:task-id interaction*)
                            (dissoc-interaction* (get-in state [:by-task (:task-id interaction*)]))
                            (and (nil? (:task-id interaction*))
                                 (:session-id interaction*))
                            (dissoc-interaction* (get-in state [:by-session (:session-id interaction*)])))]
               (cond-> (assoc-in state* [:by-id (:interaction-id interaction*)] interaction*)
                 (:session-id interaction*) (assoc-in [:by-session (:session-id interaction*)] (:interaction-id interaction*))
                 (:task-id interaction*) (assoc-in [:by-task (:task-id interaction*)] (:interaction-id interaction*))))))
    interaction*))

(defn pending-interaction
  "Look up a pending interaction by `:interaction-id`, `:task-id`, or
   `:session-id`. Task lookup is preferred over session lookup.

   Optional selector keys:
   - `:kind`
   - `:channel`"
  [selector]
  (let [state       @pending-interactions
        selector*   (normalize-interaction-selector selector)
        interaction-id (interaction-id-for-selector state selector*)
        interaction (when interaction-id
                      (get-in state [:by-id interaction-id]))]
    (when (and interaction
               (or (nil? (:kind selector*)) (= (:kind selector*) (:kind interaction)))
               (or (nil? (:channel selector*)) (= (:channel selector*) (:channel interaction))))
      interaction)))

(defn resolve-pending-interaction
  "Resolve a pending interaction by the most specific selector available.
   Lookup order is `:interaction-id`, then `:task-id`, then `:session-id`."
  [selector]
  (let [selector* (normalize-interaction-selector selector)
        options   (cond-> []
                    (:interaction-id selector*) (conj {:interaction-id (:interaction-id selector*)})
                    (:task-id selector*) (conj {:task-id (:task-id selector*)})
                    (:session-id selector*) (conj {:session-id (:session-id selector*)}))]
    (some (fn [base-selector]
            (pending-interaction (merge base-selector
                                        (select-keys selector* [:kind :channel]))))
          options)))

(def ^:private cancel-replies
  #{"cancel" "/cancel" "stop" "abort" "nevermind" "never mind"})

(def ^:private allow-replies
  #{"yes" "y" "allow" "approve" "approved" "ok"})

(def ^:private deny-replies
  #{"no" "n" "deny" "reject" "denied"})

(def ^:private pause-replies
  #{"pause" "/pause"})

(def ^:private resume-replies
  #{"resume" "/resume" "continue" "/continue"})

(def ^:private stop-replies
  #{"stop-task" "/stop-task" "stop task" "stop"})

(def ^:private interrupt-replies
  #{"interrupt" "/interrupt" "cancel task" "cancel run"})

(defn coerce-interaction-reply
  "Coerce a free-form channel reply for a pending interaction into the value
   that should be delivered to the waiting runtime.

   Returns one of:
   - `:cancel`
   - `:allow`
   - `:deny`
   - normalized input text for prompt interactions
   - `nil` when the reply is not actionable"
  [interaction raw-reply]
  (let [text  (some-> raw-reply str str/trim)
        value (some-> text str/lower-case)]
    (cond
      (str/blank? text)
      nil

      (contains? cancel-replies value)
      :cancel

      (= :approval (:kind interaction))
      (cond
        (contains? allow-replies value) :allow
        (contains? deny-replies value) :deny
        :else nil)

      :else
      text)))

(defn interaction-public-id
  "Return the public correlation id for an interaction."
  [interaction]
  (case (:kind interaction)
    :prompt (:prompt-id interaction)
    :approval (:approval-id interaction)
    (:interaction-id interaction)))

(defn interaction-retry-text
  "Return a standard retry hint for a pending interaction."
  [interaction]
  (case (:kind interaction)
    :approval "Still waiting for approval. Reply YES, NO, or CANCEL."
    "Still waiting for input. Reply with the requested value or CANCEL."))

(defn deliver-validated-interaction!
  "Resolve a pending interaction, validate its public id, and deliver a value.

   Returns a result map with `:status`:
   - `:missing`
   - `:stale`
   - `:delivered`"
  [selector expected-public-id value]
  (if-let [interaction (resolve-pending-interaction selector)]
    (if (not= expected-public-id (interaction-public-id interaction))
      {:status :stale
       :interaction interaction}
      (do
        (deliver-pending-interaction! {:interaction-id (:interaction-id interaction)} value)
        {:status :delivered
         :interaction interaction
         :value value}))
    {:status :missing}))

(defn submit-freeform-interaction-reply!
  "Resolve a pending interaction, coerce a free-form reply, and deliver it.

   Returns a result map with `:status`:
   - `:missing`
   - `:invalid`
   - `:delivered`"
  [selector raw-reply]
  (if-let [interaction (resolve-pending-interaction selector)]
    (if-let [value (coerce-interaction-reply interaction raw-reply)]
      (do
        (deliver-pending-interaction! {:interaction-id (:interaction-id interaction)} value)
        {:status :delivered
         :interaction interaction
         :value value})
      {:status :invalid
       :interaction interaction})
    {:status :missing}))

(defn parse-control-intent
  "Parse a free-form transport message into a task/session control intent.

   Returns one of:
   - `:interrupt`
   - `:pause`
   - `:resume`
   - `:stop`
   - `nil`"
  [raw-reply]
  (let [value (some-> raw-reply str str/trim str/lower-case)]
    (cond
      (str/blank? value) nil
      (contains? stop-replies value) :stop
      (contains? pause-replies value) :pause
      (contains? resume-replies value) :resume
      (or (contains? interrupt-replies value)
          (contains? cancel-replies value)) :interrupt
      :else nil)))

(defn control-result-text
  "Render a user-facing acknowledgement for a control intent result."
  [intent result]
  (case (:status result)
    :paused (case intent
              :interrupt "Pausing the current task."
              "Task paused.")
    :pausing "Pausing the current task."
    :interrupting "Interrupting the current task."
    :stopped "Task stopped."
    :stopping "Stopping the current task."
    :running (case intent
               :resume "Resuming the current task."
               "Task running.")
    :already-running "Task is already running."
    :already-paused "Task is already paused."
    :already-stopped "Task is already stopped."
    :not-found "No current task to control."
    :missing "No current task to control."
    :invalid (or (:error result) "That control request is not valid right now.")
    :busy (or (:error result) "The current task is busy.")
    :unavailable (or (:error result) "Task control is temporarily unavailable.")
    :not-resumable (or (:error result) "This task cannot be resumed.")
    (or (:error result) "I couldn't apply that control request.")))

(defn control-result-view
  "Return a normalized presentation for a task control result."
  [intent result]
  (let [status (:status result)]
    {:status status
     :status-key (case status
                   :already-paused "already_paused"
                   :already-stopped "already_stopped"
                   (when (keyword? status)
                     (name status)))
     :response-kind (case status
                      (:missing :not-found) :missing
                      (:invalid :busy :already-running :not-resumable) :conflict
                      :unavailable :unavailable
                      (:pausing :stopping :interrupting :steering :forking :running) :accepted
                      (:already-paused :already-stopped :paused :stopped) :completed
                      :unknown)
     :message (control-result-text intent result)}))

(defn session-control-result-text
  "Render a user-facing acknowledgement for a session-level control result."
  [intent result]
  (case (:status result)
    :cancelling (case intent
                  :close "Closing the current session."
                  "Cancelling the current session.")
    :closed "Session closed."
    :already-closed "Session is already closed."
    :busy (or (:error result) "The current session is still processing a request.")
    :missing "No current session to control."
    :invalid (or (:error result) "That session control request is not valid.")
    (or (:error result) "I couldn't apply that session control request.")))

(defn session-control-result-view
  "Return a normalized presentation for a session control result."
  [intent result]
  (let [status (:status result)]
    {:status status
     :status-key (case status
                   :already-closed "already_closed"
                   (when (keyword? status)
                     (name status)))
     :response-kind (case status
                      :missing :missing
                      :busy :conflict
                      :cancelling :accepted
                      (:closed :already-closed) :completed
                      :invalid :conflict
                      :unknown)
     :message (session-control-result-text intent result)}))

(defn apply-session-control-intent!
  "Dispatch a session-level control intent to the supplied handlers.

   `handlers` may provide:
   - `:cancel-session!`
   - `:busy?`
   - `:finalize-session!`"
  [handlers session-id intent & {:keys [reason context]}]
  (let [audit-ctx (merge (select-keys *interaction-context* [:session-id :channel])
                         (select-keys context [:session-id :channel]))
        result
        (if-not session-id
          {:status :missing}
          (case intent
            :interrupt
            (if ((:cancel-session! handlers) session-id (or reason "session cancel requested"))
              {:status :cancelling
               :session-id session-id}
              {:status :busy
               :session-id session-id
               :error "session is still processing a request"})

            :close
            (let [busy? (boolean (when-let [f (:busy? handlers)]
                                   (f session-id)))
                  finalize! (:finalize-session! handlers)]
              (cond
                busy?
                (if ((:cancel-session! handlers) session-id (or reason "session close requested"))
                  {:status :cancelling
                   :session-id session-id
                   :closing true}
                  {:status :busy
                   :session-id session-id
                   :error "session is still processing a request"})

                (nil? finalize!)
                {:status :invalid
                 :session-id session-id
                 :error "Session finalization is unavailable"}

                :else
                {:status (if (finalize! session-id) :closed :already-closed)
                 :session-id session-id}))

            {:status :invalid
             :session-id session-id
             :error (str "Unsupported session control intent: " intent)}))]
    (audit/log! audit-ctx
                {:actor :user
                 :type :session-control
                 :data (cond-> {:kind "session-control"
                                :intent (name intent)
                                :session-id (some-> session-id str)
                                :status (some-> (:status result) name)}
                         reason (assoc :reason reason)
                         (:closing result) (assoc :closing true))})
    result))

(defn apply-task-control-intent!
  "Dispatch a control intent to the supplied task-control handlers.

   `handlers` should provide:
   - `:pause-task!`
   - `:resume-task!`
   - `:stop-task!`
   - `:interrupt-task!`
   - `:steer-task!`
   - `:fork-task!`"
  [handlers task-id intent & {:keys [message context]}]
  (let [audit-ctx (merge (select-keys *interaction-context* [:session-id :channel])
                         (select-keys context [:session-id :channel]))
        result
        (if-not task-id
          {:status :missing}
          (case intent
            :pause ((:pause-task! handlers) task-id)
            :resume ((:resume-task! handlers) task-id :message message)
            :stop ((:stop-task! handlers) task-id)
            :interrupt ((:interrupt-task! handlers) task-id)
            :steer ((:steer-task! handlers) task-id message)
            :fork ((:fork-task! handlers) task-id message)
            {:status :invalid
             :error (str "Unsupported control intent: " intent)}))]
    (audit/log! audit-ctx
                {:actor :user
                 :type :task-control
                 :data (cond-> {:kind "task-control"
                                :intent (name intent)
                                :task-id (some-> task-id str)
                                :status (some-> (:status result) name)}
                         (:session-id result) (assoc :task-session-id (str (:session-id result)))
                         (some-> message str not-empty) (assoc :message-provided true))})
    result))

(defn clear-pending-interaction!
  "Remove a pending interaction by selector."
  [selector]
  (let [selector* (normalize-interaction-selector selector)]
    (swap! pending-interactions
           (fn [state]
             (if-let [interaction-id (interaction-id-for-selector state selector*)]
               (dissoc-interaction* state interaction-id)
               state)))
    nil))

(defn deliver-pending-interaction!
  "Deliver a value to a pending interaction response promise and clear it.
   Returns true when an interaction was found."
  [selector value]
  (if-let [interaction (pending-interaction selector)]
    (do
      (when-let [response (:response interaction)]
        (deliver response value))
      (clear-pending-interaction! {:interaction-id (:interaction-id interaction)})
      true)
    false))

(defn cancel-pending-interaction!
  "Cancel a pending interaction and clear it."
  [selector]
  (deliver-pending-interaction! selector :cancel))

(defn register-prompt!
  "Register a prompt function for a channel. Called by channels at startup.
   The function signature is: (fn [label & {:keys [mask?]}] => string)
     label — what to show the user (e.g. \"Password\")
     mask? — true to hide input (for passwords)"
  ([f]
   (register-prompt! :default f))
  ([channel f]
   (register-handler! prompt-handlers channel f)))

(defn prompt!
  "Prompt the user for input. Blocks until the user responds.
   Throws if no channel has registered a prompt handler."
  [label & {:keys [mask?] :or {mask? false}}]
  (let [f (resolve-handler prompt-handlers)]
    (when-not f
      (throw (ex-info "No interactive prompt available for current channel"
                      {:label label :channel (current-channel)})))
    (invoke-runtime-hook! :task-runtime/on-input-request
                          {:label label
                           :mask? (boolean mask?)})
    (audit/log! *interaction-context*
                {:actor :user
                 :type  :input-request
                 :data  {:label label
                         :masked (boolean mask?)}})
    (let [value (f label :mask? mask?)]
      (invoke-runtime-hook! :task-runtime/on-input-response
                            {:label label
                             :mask? (boolean mask?)
                             :provided (not (clojure.string/blank? value))})
      (audit/log! *interaction-context*
                  {:actor :user
                   :type  :input-response
                   :data  {:label label
                           :masked (boolean mask?)
                           :provided (not (clojure.string/blank? value))}})
      value)))

(defn prompt-available?
  "True if a prompt handler is registered."
  []
  (some? (resolve-handler prompt-handlers)))

(defn register-approval!
  "Register a privileged-tool approval handler for a channel.
   The handler signature is: (fn [request-map] => boolean)."
  ([f]
   (register-approval! :default f))
  ([channel f]
   (register-handler! approval-handlers channel f)))

(defn approve!
  "Request approval for a privileged tool in the current interaction context.
   Returns true if approved, false if denied."
  [request]
  (let [f (resolve-handler approval-handlers)
        req (merge {:channel (current-channel)} *interaction-context* request)]
    (when-not f
      (throw (ex-info "No approval handler available for current channel"
                      {:channel (current-channel)
                       :tool-id (:tool-id request)})))
    (invoke-runtime-hook! :task-runtime/on-approval-request req)
    (audit/log! *interaction-context*
                {:actor :user
                 :type  :approval-request
                 :tool-id (some-> (:tool-id req) name)
                 :data  {:tool-name   (:tool-name req)
                         :description (:description req)
                         :arguments   (:arguments req)
                         :policy      (some-> (:policy req) name)
                         :reason      (:reason req)}})
    (let [approved? (boolean (f req))]
      (invoke-runtime-hook! :task-runtime/on-approval-decision
                            (assoc req :approved? approved?))
      (audit/log! *interaction-context*
                  {:actor :user
                   :type  :approval-decision
                   :tool-id (some-> (:tool-id req) name)
                   :data  {:tool-name (:tool-name req)
                           :approved approved?
                           :policy   (some-> (:policy req) name)}})
      approved?)))

(defn policy-decision!
  "Record a policy decision for the current interaction context.
   Used to persist explicit approval-policy and execution-policy decisions."
  [decision]
  (invoke-runtime-hook! :task-runtime/on-policy-decision decision)
  (audit/log! *interaction-context*
              {:actor :assistant
               :type :policy-decision
               :tool-id (some-> (:tool-id decision) name)
               :data (into {}
                           (keep (fn [[k v]]
                                   (when (some? v)
                                     [k (if (keyword? v) (name v) v)])))
                           decision)})
  nil)

(defn approval-available?
  "True if an approval handler is registered."
  []
  (some? (resolve-handler approval-handlers)))

(defn register-status!
  "Register a status callback for a channel.
   The handler signature is: (fn [status-map] => any)."
  ([f]
   (register-status! :default f))
  ([channel f]
   (register-handler! status-handlers channel f)))

(defn status!
  "Report an execution status update for the current interaction context.
   Returns nil if no status handler is registered for the current channel."
  [status]
  (invoke-runtime-hook! :task-runtime/on-status status)
  (when-let [f (resolve-handler status-handlers)]
    (f (merge {:channel (current-channel)} *interaction-context* status))))

(defn status-available?
  "True if a status handler is registered."
  []
  (some? (resolve-handler status-handlers)))

(defn register-assistant-message!
  "Register an assistant message callback for a channel.
   The handler signature is: (fn [message-map] => any)."
  ([f]
   (register-assistant-message! :default f))
  ([channel f]
   (register-handler! assistant-message-handlers channel f)))

(defn assistant-message!
  "Report an assistant message for the current interaction context.
   Returns nil if no assistant message handler is registered for the current channel."
  [message]
  (when-let [f (resolve-handler assistant-message-handlers)]
    (f (merge {:channel (current-channel)} *interaction-context* message))))

(defn assistant-message-available?
  "True if an assistant message handler is registered."
  []
  (some? (resolve-handler assistant-message-handlers)))

(defn register-runtime-event!
  "Register a runtime event callback for a channel.
   The handler signature is: (fn [event-map] => any)."
  ([f]
   (register-runtime-event! :default f))
  ([channel f]
   (register-handler! runtime-event-handlers channel f)))

(defn runtime-event!
  "Report a typed runtime event for the current interaction context.
   Returns nil if no runtime event handler is registered for the current channel."
  [event]
  (when-let [f (resolve-handler runtime-event-handlers)]
    (f (merge {:channel (current-channel)} *interaction-context* event))))

(defn runtime-event-available?
  "True if a runtime event handler is registered."
  []
  (some? (resolve-handler runtime-event-handlers)))
