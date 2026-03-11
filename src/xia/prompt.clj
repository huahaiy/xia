(ns xia.prompt
  "Interactive user prompt — allows tool execution to pause and ask
   the user for input (e.g., credentials for a one-time login).

   Channels register prompt/approval handlers keyed by channel.
   Tool code calls (prompt! ...) or privileged tool execution calls
   (approve! ...) which delegates to the current channel handler.")

;; ---------------------------------------------------------------------------
;; Prompt callback registry
;; ---------------------------------------------------------------------------

(defonce ^:private prompt-handlers (atom {}))
(defonce ^:private approval-handlers (atom {}))

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
    (f label :mask? mask?)))

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
    (boolean (f req))))

(defn approval-available?
  "True if an approval handler is registered."
  []
  (some? (resolve-handler approval-handlers)))
