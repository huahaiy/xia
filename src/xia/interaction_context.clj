(ns xia.interaction-context
  "Dynamic context and lightweight predicates for user/tool interactions.")

(def ^:dynamic *interaction-context*
  "Dynamic execution context for tool interactions, e.g. {:channel :terminal :session-id ...}."
  nil)

(defn- legacy-prompt-context
  []
  (when-let [prompt-ns (find-ns 'xia.prompt)]
    (when-let [context-var (ns-resolve prompt-ns '*interaction-context*)]
      @context-var)))

(defn context
  []
  (or *interaction-context*
      (legacy-prompt-context)))

(defn autonomous-run?
  ([] (autonomous-run? (context)))
  ([ctx]
   (true? (:autonomous-run? ctx))))

(defn trusted?
  ([] (trusted? (context)))
  ([ctx]
   (and (autonomous-run? ctx)
        (true? (:approval-bypass? ctx)))))

(defn audit!
  ([event]
   (audit! (context) event))
  ([ctx event]
   (when-let [audit-log (:audit-log ctx)]
     (swap! audit-log conj
            (merge {:at (str (java.time.Instant/now))}
                   event)))))
