(ns xia.plugin
  "Sandboxed plugin manifests and lifecycle hook execution."
  (:refer-clojure :exclude [run!])
  (:require [clojure.string :as str]
            [sci.core :as sci]
            [taoensso.timbre :as log]
            [xia.audit :as audit]
            [xia.db :as db]
            [xia.task-policy :as task-policy]))

(def hook-events
  #{:pre-tool
    :post-tool
    :post-llm
    :task-state-change
    :schedule-run})

(defn hook-capability
  [event]
  (keyword "hook" (name event)))

(def ^:private denied-symbols
  '[alter-var-root
    all-ns
    binding
    create-ns
    eval
    find-ns
    find-var
    future
    future-call
    in-ns
    intern
    load
    load-file
    load-reader
    load-string
    ns
    ns-aliases
    ns-interns
    ns-map
    ns-publics
    ns-refers
    ns-resolve
    pmap
    read
    read-line
    read-string
    refer
    require
    requiring-resolve
    resolve
    send
    send-off
    slurp
    spit
    the-ns
    clojure.core/alter-var-root
    clojure.core/all-ns
    clojure.core/binding
    clojure.core/create-ns
    clojure.core/eval
    clojure.core/find-ns
    clojure.core/find-var
    clojure.core/future
    clojure.core/future-call
    clojure.core/in-ns
    clojure.core/intern
    clojure.core/load
    clojure.core/load-file
    clojure.core/load-reader
    clojure.core/load-string
    clojure.core/ns
    clojure.core/ns-aliases
    clojure.core/ns-interns
    clojure.core/ns-map
    clojure.core/ns-publics
    clojure.core/ns-refers
    clojure.core/ns-resolve
    clojure.core/pmap
    clojure.core/read
    clojure.core/read-line
    clojure.core/read-string
    clojure.core/refer
    clojure.core/require
    clojure.core/requiring-resolve
    clojure.core/resolve
    clojure.core/send
    clojure.core/send-off
    clojure.core/slurp
    clojure.core/spit
    clojure.core/the-ns])

(def ^:private max-output-depth 8)
(def ^:private max-output-items 200)

(declare normalize-output-value)

(defn- nonblank-str
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn- read-key
  [m k]
  (let [snake (str/replace (name k) "-" "_")]
    (first (for [k* [k (name k) snake (keyword snake)]
                 :when (contains? m k*)]
             (get m k*)))))

(defn- normalize-id
  [value field]
  (cond
    (keyword? value)
    value

    (symbol? value)
    (keyword (namespace value) (name value))

    :else
    (let [text (nonblank-str value)
          text (when text
                 (if (str/starts-with? text ":") (subs text 1) text))]
      (cond
        (nil? text)
        (throw (ex-info (str "plugin manifest missing " field)
                        {:type :plugin/invalid-manifest
                         :field field}))

        (re-find #"\s" text)
      (throw (ex-info (str "plugin manifest " field " must not contain whitespace")
                      {:type :plugin/invalid-manifest
                       :field field
                       :value value}))

        :else
        (keyword text)))))

(defn- normalize-keyword
  [value field]
  (cond
    (keyword? value)
    value

    (symbol? value)
    (keyword (namespace value) (name value))

    (string? value)
    (let [text (str/trim value)
          text (if (str/starts-with? text ":") (subs text 1) text)]
      (when-not (str/blank? text)
        (if (str/includes? text "/")
          (let [[ns n] (str/split text #"/" 2)]
            (keyword ns n))
          (keyword text))))

    :else
    (throw (ex-info (str "plugin manifest " field " must be a keyword or string")
                    {:type :plugin/invalid-manifest
                     :field field
                     :value value}))))

(defn- normalize-capabilities
  [value]
  (let [items (cond
                (nil? value) []
                (set? value) value
                (sequential? value) value
                :else
                (throw (ex-info "plugin manifest capabilities must be a collection"
                                {:type :plugin/invalid-manifest
                                 :field :capabilities
                                 :value value})))]
    (set (map #(normalize-keyword % :capabilities) items))))

(defn- normalize-hook
  [hook]
  (when-not (map? hook)
    (throw (ex-info "plugin hook must be a map"
                    {:type :plugin/invalid-manifest
                     :hook hook})))
  (let [event   (normalize-keyword (read-key hook :event) :event)
        hook-id (or (when-some [id (read-key hook :id)]
                      (normalize-id id "hook id"))
                    (keyword (str (name event) "-" (hash hook))))
        handler (nonblank-str (read-key hook :handler))]
    (when-not (contains? hook-events event)
      (throw (ex-info "plugin hook event is not supported"
                      {:type :plugin/invalid-manifest
                       :event event
                       :supported-events (sort (map name hook-events))})))
    (when-not handler
      (throw (ex-info "plugin hook requires a handler string"
                      {:type :plugin/invalid-manifest
                       :hook-id hook-id})))
    {:id hook-id
     :event event
     :handler handler}))

(defn normalize-manifest
  [manifest]
  (when-not (map? manifest)
    (throw (ex-info "plugin manifest must be a map"
                    {:type :plugin/invalid-manifest})))
  (let [hooks        (mapv normalize-hook (or (read-key manifest :hooks) []))
        capabilities (normalize-capabilities (read-key manifest :capabilities))
        missing      (->> hooks
                          (map (comp hook-capability :event))
                          (remove capabilities)
                          set)]
    (when (seq missing)
      (throw (ex-info "plugin manifest hooks require explicit hook capabilities"
                      {:type :plugin/missing-capability
                       :missing-capabilities (sort (map str missing))})))
    {:id (normalize-id (read-key manifest :id) "id")
     :name (or (nonblank-str (read-key manifest :name))
               (name (normalize-id (read-key manifest :id) "id")))
     :description (or (nonblank-str (read-key manifest :description)) "")
     :version (or (nonblank-str (read-key manifest :version)) "")
     :enabled? (if (some? (read-key manifest :enabled?))
                 (boolean (read-key manifest :enabled?))
                 true)
     :capabilities capabilities
     :hooks hooks}))

(defn install-plugin!
  [manifest]
  (let [plugin (normalize-manifest manifest)]
    (db/save-plugin! {:id (:id plugin)
                      :name (:name plugin)
                      :description (:description plugin)
                      :version (:version plugin)
                      :enabled? (:enabled? plugin)
                      :capabilities (:capabilities plugin)
                      :manifest plugin})))

(defn enable-plugin!
  [plugin-id enabled?]
  (let [plugin-id* (normalize-id plugin-id "id")]
    (when (db/get-plugin plugin-id*)
      (db/enable-plugin! plugin-id* enabled?))))

(defn- plugin-manifest
  [plugin]
  (or (:plugin/manifest plugin) {}))

(defn- enabled-plugin-hooks
  [event]
  (let [capability (hook-capability event)]
    (for [plugin (db/list-plugins)
          :when (:plugin/enabled? plugin)
          :let [manifest (plugin-manifest plugin)
                capabilities (set (:capabilities manifest))]
          :when (contains? capabilities capability)
          hook (:hooks manifest)
          :when (= event (:event hook))]
      {:plugin plugin
       :manifest manifest
       :hook hook})))

(defn- unsupported-output-ex
  [value reason]
  (ex-info (str "plugin hook returned unsupported output: " reason)
           {:type :plugin/invalid-output
            :reason reason
            :output-class (some-> value class .getName)}))

(defn- normalize-output-key
  [key]
  (cond
    (string? key) key
    (keyword? key) key
    (symbol? key) (str key)
    (number? key) (str key)
    (boolean? key) (str key)
    (nil? key) "null"
    (instance? java.util.UUID key) (str key)
    (instance? java.time.Instant key) (str key)
    (instance? java.util.Date key) (str (.toInstant ^java.util.Date key))
    :else (throw (unsupported-output-ex key "unsupported map key type"))))

(defn- normalize-output-coll
  [coll depth]
  (let [items (doall (take (inc max-output-items) coll))]
    (when (> (count items) max-output-items)
      (throw (unsupported-output-ex coll "collection exceeds size limit")))
    (mapv #(normalize-output-value % (inc depth)) items)))

(defn- normalize-output-value
  [value depth]
  (when (> depth max-output-depth)
    (throw (unsupported-output-ex value "output nesting is too deep")))
  (cond
    (or (nil? value)
        (string? value)
        (boolean? value)
        (number? value)
        (keyword? value))
    value

    (symbol? value)
    (str value)

    (instance? java.util.UUID value)
    value

    (instance? java.time.Instant value)
    value

    (instance? java.util.Date value)
    value

    (map? value)
    (do
      (when (> (count value) max-output-items)
        (throw (unsupported-output-ex value "map exceeds size limit")))
      (reduce-kv (fn [m k v]
                   (assoc m
                          (normalize-output-key k)
                          (normalize-output-value v (inc depth))))
                 {}
                 value))

    (vector? value)
    (normalize-output-coll value depth)

    (set? value)
    (normalize-output-coll (seq value) depth)

    (sequential? value)
    (normalize-output-coll value depth)

    :else
    (throw (unsupported-output-ex value "unsupported value type"))))

(defn- normalize-output
  [value]
  (normalize-output-value value 0))

(defn- call-with-timeout
  [timeout-ms f]
  (let [result* (promise)
        runner  (bound-fn*
                 (fn []
                   (try
                     (deliver result* {:status :ok
                                       :value (f)})
                     (catch Throwable t
                       (deliver result* {:status :error
                                         :throwable t})))))
        thread  (doto (Thread. ^Runnable
                               (reify Runnable
                                 (run [_]
                                   (runner)))
                               ^String (str "xia-plugin-hook-" (System/nanoTime)))
                  (.setDaemon true))
        timeout (Object.)]
    (.start thread)
    (let [result (deref result* timeout-ms timeout)]
      (if (identical? timeout result)
        (do
          (.interrupt thread)
          (throw (ex-info (str "plugin hook timed out after " timeout-ms "ms")
                          {:type :plugin/hook-timeout
                           :timeout-ms timeout-ms})))
        (case (:status result)
          :ok (:value result)
          :error (throw (:throwable result)))))))

(defn- make-ctx
  [event]
  (sci/init
   {:namespaces {'user       {'event event}
                 'xia.plugin {'event event}}
    :deny denied-symbols
    :classes {}}))

(defn- eval-hook-handler
  [handler event]
  (let [ctx (make-ctx event)
        f   (sci/eval-string* ctx handler)]
    (when-not (fn? f)
      (throw (ex-info "plugin hook handler must evaluate to a function"
                      {:type :plugin/invalid-handler})))
    (f event)))

(defn- summarize-result
  [value]
  (cond
    (nil? value) nil
    (string? value) (subs value 0 (min 240 (count value)))
    :else (let [text (pr-str value)]
            (subs text 0 (min 240 (count text))))))

(defn- audit-hook!
  [context event plugin hook status details]
  (let [entry {:actor :system
               :type :plugin-hook
               :tool-id (some-> (:tool-id context) name)
               :tool-call-id (:tool-call-id context)
               :llm-call-id (:llm-call-id context)
               :data (merge {:plugin-id (name (:plugin/id plugin))
                             :plugin-name (:plugin/name plugin)
                             :hook-id (name (:id hook))
                             :hook-event (name event)
                             :status (name status)}
                            details)}]
    (when-let [audit-log (:audit-log context)]
      (swap! audit-log conj (:data entry)))
    (audit/log! context entry)))

(defn run-hooks!
  "Run enabled plugin hooks for an event. Hook failures are audited and isolated."
  [event context]
  (let [event* (normalize-keyword event :event)]
    (when-not (contains? hook-events event*)
      (throw (ex-info "unsupported plugin hook event"
                      {:type :plugin/unsupported-hook-event
                       :event event*})))
    (mapv
     (fn [{:keys [plugin hook]}]
       (let [payload (assoc (or context {})
                            :hook-event event*
                            :plugin-id (:plugin/id plugin)
                            :hook-id (:id hook))]
         (try
           (let [result (call-with-timeout
                         (task-policy/plugin-hook-timeout-ms)
                         #(normalize-output
                           (eval-hook-handler (:handler hook) payload)))]
             (audit-hook! context event* plugin hook :success
                          (cond-> {}
                            (some? result) (assoc :result-summary (summarize-result result))))
             {:plugin-id (:plugin/id plugin)
              :hook-id (:id hook)
              :event event*
              :status :success
              :result result})
           (catch Throwable t
             (log/warn t "Plugin hook failed"
                       {:plugin-id (:plugin/id plugin)
                        :hook-id (:id hook)
                        :event event*})
             (audit-hook! context event* plugin hook :error
                          {:error (.getMessage t)})
             {:plugin-id (:plugin/id plugin)
              :hook-id (:id hook)
              :event event*
              :status :error
              :error (.getMessage t)}))))
     (enabled-plugin-hooks event*))))

(defn blocked-by-pre-tool-hook
  [results]
  (letfn [(allow-value [result]
            (reduce (fn [_ k]
                      (when (contains? result k)
                        (reduced (get result k))))
                    nil
                    [:allow? :allow "allow?" "allow"]))]
    (some (fn [{:keys [result]}]
            (when (and (map? result)
                       (false? (allow-value result)))
              {:allowed? false
               :reason (or (:reason result)
                           (get result "reason")
                           "blocked by plugin pre-tool hook")}))
          results)))
