(ns xia.runtime-context
  "Explicit runtime context for Integrant-owned component runtimes.

  Runtime namespaces still keep installed-runtime atoms as a compatibility
  fallback, but code entered through an Integrant component can bind this
  context so current-runtime lookups resolve structurally first.")

(def ^:dynamic *runtime-context* nil)

(defn component-runtime
  "Return the concrete runtime map carried by an Integrant component value."
  [component]
  (cond
    (and (map? component) (contains? component :runtime))
    (:runtime component)

    :else
    nil))

(def ^:private nested-runtime-keys-by-component
  {:xia/agent-runtime [:fact-review-runtime]
   :xia/tool-runtime  [:permission-runtime]})

(defn- nested-runtimes
  [component-key runtime]
  (select-keys runtime (get nested-runtime-keys-by-component component-key [])))

(defn- nested-runtime-key
  [runtime-key]
  (keyword "xia" (name runtime-key)))

(defn- component-runtime-keys
  [component-key]
  (cons component-key
        (map nested-runtime-key
             (get nested-runtime-keys-by-component component-key []))))

(defn component-runtimes
  [component-key component]
  (when-let [runtime (component-runtime component)]
    (into [[component-key runtime]]
          (keep (fn [[runtime-key runtime-value]]
                  (when runtime-value
                    [(nested-runtime-key runtime-key) runtime-value])))
          (nested-runtimes component-key runtime))))

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
                      (update :runtimes #(apply dissoc (or %) (component-runtime-keys component-key))))]
    (if-let [runtimes (seq (component-runtimes component-key component-value))]
      (update context** :runtimes into runtimes)
      context**)))

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
