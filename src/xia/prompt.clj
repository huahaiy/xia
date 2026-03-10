(ns xia.prompt
  "Interactive user prompt — allows tool execution to pause and ask
   the user for input (e.g., credentials for a one-time login).

   Channels (terminal, HTTP) register a prompt function at startup.
   Tool code calls (prompt! ...) which delegates to the registered handler.")

;; ---------------------------------------------------------------------------
;; Prompt callback registry
;; ---------------------------------------------------------------------------

(defonce ^:private prompt-fn (atom nil))

(defn register-prompt!
  "Register a prompt function. Called by channels at startup.
   The function signature is: (fn [label & {:keys [mask?]}] => string)
     label — what to show the user (e.g. \"Password\")
     mask? — true to hide input (for passwords)"
  [f]
  (reset! prompt-fn f))

(defn prompt!
  "Prompt the user for input. Blocks until the user responds.
   Throws if no channel has registered a prompt handler."
  [label & {:keys [mask?] :or {mask? false}}]
  (let [f @prompt-fn]
    (when-not f
      (throw (ex-info "No interactive prompt available (not running in terminal?)"
                      {:label label})))
    (f label :mask? mask?)))

(defn prompt-available?
  "True if a prompt handler is registered."
  []
  (some? @prompt-fn))
