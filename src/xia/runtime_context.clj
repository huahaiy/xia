(ns xia.runtime-context
  "Explicit runtime context for Integrant-owned component runtimes.

  Code entered through an Integrant component can bind this context so
  current-runtime lookups resolve structurally.")

(def ^:dynamic *runtime-context* nil)

(defn component-runtime
  "Return the concrete runtime map carried by an Integrant component value."
  [component]
  (cond
    (and (map? component) (contains? component :runtime))
    (:runtime component)

    :else
    nil))

(defn component-runtimes
  [component-key component]
  (when-let [runtime (component-runtime component)]
    [[component-key runtime]]))

(defn make
  "Build a runtime context from a map of Integrant component keys to values."
  [components]
  {:components components
   :runtimes   (into {}
                     (mapcat (fn [[component-key component]]
                               (or (component-runtimes component-key component)
                                   [])))
                     components)})

(defn current
  []
  *runtime-context*)

(defn runtime
  ([component-key]
   (runtime *runtime-context* component-key))
  ([context component-key]
   (get-in context [:runtimes component-key])))

(defn component
  ([component-key]
   (component *runtime-context* component-key))
  ([context component-key]
   (get-in context [:components component-key])))

(defn assoc-component
  [context component-key component-value]
  (let [context* (or context (make {}))
        context** (-> context*
                      (assoc-in [:components component-key] component-value)
                      (update :runtimes dissoc component-key))]
    (if-let [runtimes (seq (component-runtimes component-key component-value))]
      (update context** :runtimes into runtimes)
      context**)))

(defn merge-contexts
  "Merge runtime contexts from left to right. Later contexts win for duplicate keys."
  [& contexts]
  (when-let [contexts* (seq (remove nil? contexts))]
    {:components (apply merge (map :components contexts*))
     :runtimes   (apply merge (map :runtimes contexts*))}))

(defn with-runtime-context
  [context f]
  (if context
    (binding [*runtime-context* context]
      (f))
    (f)))

(defn without-runtime-context
  [f]
  (binding [*runtime-context* nil]
    (f)))

(defn convey-bindings
  [f]
  (let [bindings (get-thread-bindings)]
    (fn []
      (with-bindings* bindings f))))
