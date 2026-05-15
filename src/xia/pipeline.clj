(ns xia.pipeline
  "Restricted SCI pipelines for repetitive tool workflows."
  (:refer-clojure :exclude [run!])
  (:require [clojure.string :as str]
            [clojure.tools.reader :as tr]
            [clojure.tools.reader.reader-types :as rt]
            [sci.core :as sci]
            [xia.task-policy :as task-policy]))

(def ^:private reader-eof (Object.))

(def ^:dynamic *tool-context* {})
(def ^:dynamic *timeout-state* nil)

(def allowed-tool-ids
  "Tool ids that restricted pipelines may call. Keep this list read-oriented;
   pipeline-run itself must not be included."
  #{:artifact-list
    :artifact-read
    :artifact-search
    :board-list
    :local-doc-read
    :local-doc-search
    :recent-work
    :web-extract
    :web-fetch
    :web-search
    :workspace-list
    :workspace-read})

(def ^:private max-result-depth 12)
(def ^:private max-result-items 1000)
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

(declare instrument-timeouts)
(declare normalize-structured-value)

(defn normalize-tool-id
  [tool-id]
  (cond
    (keyword? tool-id)
    tool-id

    (symbol? tool-id)
    (keyword (name tool-id))

    (string? tool-id)
    (let [s (str/trim tool-id)
          s (if (str/starts-with? s ":") (subs s 1) s)]
      (when-not (str/blank? s)
        (keyword s)))

    :else
    nil))

(defn tool-allowed?
  [tool-id]
  (contains? allowed-tool-ids (normalize-tool-id tool-id)))

(defn check-timeout!
  []
  (when-let [{:keys [deadline-nanos timeout-ms]} *timeout-state*]
    (when (>= (System/nanoTime) (long deadline-nanos))
      (throw (ex-info (str "Restricted pipeline exceeded "
                           timeout-ms
                           "ms execution budget")
                      {:type :pipeline/timeout
                       :timeout-ms timeout-ms}))))
  nil)

(defn- instrument-body
  [body]
  (let [body* (map instrument-timeouts body)]
    (cons '(xia.pipeline/check-timeout!) body*)))

(defn- instrument-binding-vector
  [bindings]
  (into []
        (map-indexed (fn [idx form]
                       (if (even? idx)
                         form
                         (instrument-timeouts form))))
        bindings))

(defn- instrument-fn-clause
  [[params & body]]
  (list* params (instrument-body body)))

(defn- instrument-fn-form
  [[op & more]]
  (let [[name more] (if (symbol? (first more))
                      [(first more) (rest more)]
                      [nil more])]
    (if (vector? (first more))
      (let [[params & body] more]
        (list* op
               (concat (when name [name])
                       [params]
                       (instrument-body body))))
      (list* op
             (concat (when name [name])
                     (map instrument-fn-clause more))))))

(defn- instrument-loop-form
  [[op bindings & body]]
  (list* op
         (instrument-binding-vector bindings)
         (instrument-body body)))

(defn- instrument-letfn-form
  [[op bindings & body]]
  (list* op
         (into []
               (map (fn [[fname params & fbody]]
                      (list* fname params (instrument-body fbody))))
               bindings)
         (map instrument-timeouts body)))

(defn- instrument-while-form
  [[op test & body]]
  (list* op
         (instrument-timeouts test)
         (instrument-body body)))

(defn- instrument-timeouts
  [form]
  (cond
    (list? form)
    (let [op (first form)]
      (case op
        fn     (instrument-fn-form form)
        fn*    (instrument-fn-form form)
        loop   (instrument-loop-form form)
        loop*  (instrument-loop-form form)
        let    (list* op
                      (instrument-binding-vector (second form))
                      (map instrument-timeouts (nnext form)))
        let*   (list* op
                      (instrument-binding-vector (second form))
                      (map instrument-timeouts (nnext form)))
        letfn  (instrument-letfn-form form)
        while  (instrument-while-form form)
        doseq  (instrument-loop-form form)
        dotimes (instrument-loop-form form)
        for    (instrument-loop-form form)
        (apply list (map instrument-timeouts form))))

    (vector? form)
    (mapv instrument-timeouts form)

    (map? form)
    (reduce-kv (fn [m k v]
                 (assoc m
                        (instrument-timeouts k)
                        (instrument-timeouts v)))
               (empty form)
               form)

    (set? form)
    (into (empty form) (map instrument-timeouts) form)

    :else
    form))

(defn- instrument-code-string
  [code-str]
  (let [reader (rt/indexing-push-back-reader
                (rt/string-push-back-reader code-str))]
    (binding [*print-meta* true]
      (loop [forms []]
        (let [form (tr/read {:eof reader-eof
                             :read-cond :allow
                             :features #{:clj}}
                            reader)]
          (if (identical? reader-eof form)
            (str/join "\n"
                      (map (fn [form*]
                             (pr-str (list 'do
                                           '(xia.pipeline/check-timeout!)
                                           (instrument-timeouts form*))))
                           forms))
            (recur (conj forms form))))))))

(defn- arg-value
  [m k]
  (let [snake (-> (name k)
                  (str/replace "-" "_"))
        ks    [k (name k) snake (keyword snake)]]
    (first (for [k* ks
                 :when (contains? m k*)]
             (get m k*)))))

(defn- require-code
  [args]
  (let [code (arg-value args :code)]
    (when-not (and (string? code)
                   (not (str/blank? code)))
      (throw (ex-info "Restricted pipeline requires a non-empty code string"
                      {:type :pipeline/missing-code})))
    (let [max-chars (task-policy/tool-pipeline-max-code-chars)]
      (when (> (count code) max-chars)
        (throw (ex-info (str "Restricted pipeline code exceeds "
                             max-chars
                             " characters")
                        {:type :pipeline/code-too-large
                         :max-code-chars max-chars
                         :code-chars (count code)}))))
    code))

(defn- parse-positive-long
  [value field]
  (cond
    (nil? value)
    nil

    (integer? value)
    (long value)

    (and (number? value)
         (== value (long value)))
    (long value)

    (number? value)
    (throw (ex-info (str "Restricted pipeline " field " must be an integer")
                    {:type :pipeline/invalid-number
                     :field field
                     :value value}))

    (string? value)
    (try
      (Long/parseLong (str/trim value))
      (catch NumberFormatException _
        (throw (ex-info (str "Restricted pipeline " field " must be an integer")
                        {:type :pipeline/invalid-number
                         :field field
                         :value value}))))

    :else
    (throw (ex-info (str "Restricted pipeline " field " must be an integer")
                    {:type :pipeline/invalid-number
                     :field field
                     :value value}))))

(defn- requested-max-calls
  [args]
  (let [limit (task-policy/tool-pipeline-max-calls)
        n     (parse-positive-long (arg-value args :max-calls) "max_calls")]
    (cond
      (nil? n)
      limit

      (pos? n)
      (min limit n)

      :else
      (throw (ex-info "Restricted pipeline max_calls must be positive"
                      {:type :pipeline/invalid-max-calls
                       :max-calls n})))))

(defn- unsupported-result-ex
  [value reason]
  (ex-info (str "Restricted pipeline returned unsupported structured output: "
                reason)
           {:type :pipeline/invalid-result
            :reason reason
            :result-class (some-> value class .getName)}))

(defn- normalize-result-key
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
    :else (throw (unsupported-result-ex key "unsupported map key type"))))

(defn- normalize-structured-coll
  [coll depth]
  (let [items (doall (take (inc max-result-items) coll))]
    (when (> (count items) max-result-items)
      (throw (unsupported-result-ex coll "collection exceeds size limit")))
    (mapv #(normalize-structured-value % (inc depth)) items)))

(defn- normalize-structured-value
  [value depth]
  (when (> depth max-result-depth)
    (throw (unsupported-result-ex value "result nesting is too deep")))
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
      (when (> (count value) max-result-items)
        (throw (unsupported-result-ex value "map exceeds size limit")))
      (reduce-kv (fn [m k v]
                   (assoc m
                          (normalize-result-key k)
                          (normalize-structured-value v (inc depth))))
                 {}
                 value))

    (vector? value)
    (normalize-structured-coll value depth)

    (set? value)
    (normalize-structured-coll (seq value) depth)

    (sequential? value)
    (normalize-structured-coll value depth)

    :else
    (throw (unsupported-result-ex value "unsupported value type"))))

(defn- normalize-structured-output
  [value]
  (normalize-structured-value value 0))

(defn- execute-tool!
  [tool-id arguments context invoke-tool]
  (let [tool-id* (normalize-tool-id tool-id)]
    (when-not tool-id*
      (throw (ex-info "Restricted pipeline tool id must be a keyword, symbol, or string"
                      {:type :pipeline/invalid-tool-id
                       :tool-id tool-id})))
    (when-not (contains? allowed-tool-ids tool-id*)
      (throw (ex-info (str "Tool " (name tool-id*) " is not allowed in restricted pipelines")
                      {:type :pipeline/tool-not-allowed
                       :tool-id tool-id*
                       :allowed-tools (sort (map name allowed-tool-ids))})))
    (when-not (or (nil? arguments) (map? arguments))
      (throw (ex-info "Restricted pipeline tool arguments must be a map"
                      {:type :pipeline/invalid-tool-arguments
                       :tool-id tool-id*
                       :arguments-class (some-> arguments class .getName)})))
    (invoke-tool tool-id* (or arguments {}) context)))

(defn- default-tool-invoker
  [tool-id arguments context]
  ((requiring-resolve 'xia.tool/execute-pipeline-tool)
   tool-id
   arguments
   context))

(defn- make-call-tool
  [{:keys [context invoke-tool max-calls call-count]}]
  (fn [tool-id arguments]
    (check-timeout!)
    (let [n (swap! call-count inc)]
      (when (> n max-calls)
        (throw (ex-info (str "Restricted pipeline exceeded "
                             max-calls
                             " tool call limit")
                        {:type :pipeline/max-calls-exceeded
                         :max-calls max-calls
                         :attempted-calls n}))))
    (normalize-structured-output
     (execute-tool! tool-id arguments context invoke-tool))))

(defn- make-ctx
  [{:keys [input call-tool]}]
  (let [allowed (vec (sort (map name allowed-tool-ids)))]
    (sci/init
     {:namespaces {'user         {'input input
                                  'call-tool call-tool
                                  'allowed-tools allowed}
                   'xia.pipeline {'input input
                                  'call-tool call-tool
                                  'allowed-tools allowed
                                  'check-timeout! check-timeout!}}
      :deny       denied-symbols
      :classes    {}})))

(defn run-pipeline!
  "Run restricted SCI pipeline opts. Expects :code and returns only the final
   structured value produced by the pipeline."
  [{:keys [code input context invoke-tool max-calls]}]
  (let [args        {:code code
                     :max-calls max-calls}
        code*       (require-code args)
        max-calls*  (requested-max-calls args)
        timeout-ms  (task-policy/tool-pipeline-timeout-ms)
        call-count  (atom 0)
        call-tool   (make-call-tool {:context (or context {})
                                     :invoke-tool (or invoke-tool default-tool-invoker)
                                     :max-calls max-calls*
                                     :call-count call-count})
        ctx         (make-ctx {:input input
                               :call-tool call-tool})
        code-ready  (instrument-code-string code*)]
    (binding [*timeout-state* {:timeout-ms timeout-ms
                               :deadline-nanos (+ (System/nanoTime)
                                                  (* 1000000
                                                     (long timeout-ms)))}]
      (normalize-structured-output
       (sci/eval-string* ctx code-ready)))))

(defn run!
  "Run a restricted pipeline from tool arguments."
  [args]
  (run-pipeline! {:code      (require-code args)
                  :input     (arg-value args :input)
                  :max-calls (arg-value args :max-calls)
                  :context   *tool-context*}))
