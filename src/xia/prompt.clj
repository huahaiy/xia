(ns xia.prompt
  "Interactive user prompt — allows tool execution to pause and ask
   the user for input (e.g., credentials for a one-time login).

   Channels register prompt/approval handlers keyed by channel.
   Tool code calls (prompt! ...) or privileged tool execution calls
   (approve! ...) which delegates to the current channel handler."
  (:require [xia.audit :as audit]))

;; ---------------------------------------------------------------------------
;; Prompt callback registry
;; ---------------------------------------------------------------------------

(defonce ^:private prompt-handlers (atom {}))
(defonce ^:private approval-handlers (atom {}))
(defonce ^:private status-handlers (atom {}))
(defonce ^:private assistant-message-handlers (atom {}))
(defonce ^:private runtime-event-handlers (atom {}))

(def ^:dynamic *interaction-context*
  "Dynamic execution context for tool interactions, e.g. {:channel :terminal :session-id ...}."
  nil)

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
