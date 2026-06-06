(ns xia.task-spec
  "Declarative task specs on top of the durable task runtime."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [xia.agent.task-runtime :as task-runtime]
            [xia.async :as async]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.prompt :as prompt]
            [xia.tool :as tool]))

(def ^:private task-spec-version 1)
(def ^:private runtime-key :task-spec)
(def ^:private default-max-steps 100)
(def ^:private default-loop-max-iterations 100)
(def ^:private default-retry-max-attempts 2)
(def ^:private default-retry-delay-ms 0)
(def ^:private default-retry-backoff-factor 1.0)
(def ^:private pause-payload-keys
  [:reason :pause-reason :waiting-for :resume-token :deadline :deadline-at
   :resume-input :resume-input-schema :data])
(def ^:private step-pause-state-keys
  [:pause :pause-reason :waiting-for :resume-token :deadline :deadline-at])
(def ^:private terminal-step-statuses #{:success :skipped :failed})
(def ^:private success-step-statuses #{:success})
(def ^:private dependency-satisfied-statuses #{:success :skipped})
(def ^:private supported-task-spec-versions #{task-spec-version})
(def ^:private builtin-step-kinds
  #{:value :emit :condition :tool :input :approval :llm :branch :subtask
    :parallel :map :loop})
(def ^:private llm-step-modes #{:transform :judge :judgment :agent :interactive})
(def ^:private branch-modes #{:async :join})
(def ^:private schema-primitive-types
  #{:object :array :string :number :integer :boolean :null})
(defonce ^:private executor-registry-atom (atom {}))

(defn- now []
  (java.util.Date.))

(defn- nonblank-string
  [value]
  (some-> value str str/trim not-empty))

(defn- normalize-id
  [field value]
  (cond
    (keyword? value) value
    (symbol? value) (keyword (name value))
    (string? value) (some-> value str/trim not-empty keyword)
    :else (throw (ex-info (str "Task spec " (name field) " is required")
                          {:type :task-spec/invalid
                           :field field
                           :value value}))))

(defn- normalize-kind
  [value]
  (cond
    (keyword? value) value
    (symbol? value) (keyword (name value))
    (string? value) (some-> value str/trim not-empty keyword)
    :else nil))

(defn- normalize-dependency-values
  [step-id value]
  (cond
    (or (nil? value) (false? value))
    []

    (or (keyword? value)
        (symbol? value)
        (string? value))
    [(normalize-id :depends-on value)]

    (sequential? value)
    (mapv #(normalize-id :depends-on %) value)

    :else
    (throw (ex-info "Task spec :depends-on must be a step id or collection of step ids"
                    {:type :task-spec/invalid
                     :field :depends-on
                     :step-id step-id
                     :value value}))))

(defn- normalize-executor-kind
  [kind]
  (or (normalize-kind kind)
      (throw (ex-info "Task spec executor kind is required"
                      {:type :task-spec/invalid
                       :field :executor-kind
                       :value kind}))))

(defn registered-executors
  "Return the globally registered task step executors.

   Executor functions receive:
   `{:task-id ... :turn-id ... :state ... :context ... :step ...
     :pause ... :resume-token ... :resume-input ...}`

   They return a step result map:
   `{:status :success|:skipped|:failed|:paused :output ... :summary ...}`."
  []
  @executor-registry-atom)

(defn register-executor!
  "Register a global executor for a task step kind.

   Registered executors override built-in defaults. Per-run `:executors`
   supplied to `run-task!` override registered executors."
  [kind executor]
  (when-not (fn? executor)
    (throw (ex-info "Task spec executor must be a function"
                    {:type :task-spec/invalid
                     :field :executor
                     :kind kind
                     :value executor})))
  (let [kind* (normalize-executor-kind kind)]
    (swap! executor-registry-atom assoc kind* executor)
    kind*))

(defn unregister-executor!
  "Remove a globally registered executor for a task step kind."
  [kind]
  (let [kind* (normalize-executor-kind kind)]
    (swap! executor-registry-atom dissoc kind*)
    kind*))

(defn clear-registered-executors!
  "Clear all globally registered task step executors."
  []
  (reset! executor-registry-atom {})
  nil)

(defn- normalize-step
  [step]
  (when-not (map? step)
    (throw (ex-info "Task spec step must be a map"
                    {:type :task-spec/invalid
                     :step step})))
  (let [id   (normalize-id :step-id (:id step))
        kind (or (normalize-kind (:kind step)) :value)
        deps (normalize-dependency-values
              id
              (or (:depends-on step)
                  (:depends step)
                  (:dependencies step)))]
    (cond-> (assoc step
                   :id id
                   :kind kind)
      (seq deps) (assoc :depends-on deps))))

(defn- validate-step-dependencies!
  [steps]
  (let [ids      (set (map :id steps))
        step-map (into {} (map (juxt :id identity)) steps)]
    (doseq [{:keys [id depends-on]} steps
            dep-id depends-on]
      (when (= id dep-id)
        (throw (ex-info "Task spec step cannot depend on itself"
                        {:type :task-spec/invalid
                         :field :depends-on
                         :step-id id
                         :depends-on dep-id})))
      (when-not (contains? ids dep-id)
        (throw (ex-info "Task spec step depends on an unknown step"
                        {:type :task-spec/invalid
                         :field :depends-on
                         :step-id id
                         :depends-on dep-id}))))
    (let [visiting (atom #{})
          visited  (atom #{})]
      (letfn [(visit! [path step-id]
                (when (contains? @visiting step-id)
                  (throw (ex-info "Task spec dependencies contain a cycle"
                                  {:type :task-spec/invalid
                                   :field :depends-on
                                   :cycle (conj (vec path) step-id)})))
                (when-not (contains? @visited step-id)
                  (swap! visiting conj step-id)
                  (doseq [dep-id (:depends-on (get step-map step-id))]
                    (visit! (conj (vec path) step-id) dep-id))
                  (swap! visiting disj step-id)
                  (swap! visited conj step-id)))]
        (doseq [{:keys [id]} steps]
          (visit! [] id)))))
  nil)

(declare normalize-spec
         normalize-tool-id)

(defn- validation-issue
  [severity message data]
  (merge {:severity severity
          :message message}
         data))

(defn- validation-error
  [message data]
  (validation-issue :error message data))

(defn- validation-warning
  [message data]
  (validation-issue :warning message data))

(defn- throw-validation-errors!
  [errors]
  (when (seq errors)
    (throw (ex-info "Task spec validation failed"
                    {:type :task-spec/invalid
                     :errors (vec errors)}))))

(defn- supported-version-errors
  [spec]
  (let [version (or (:version spec) task-spec-version)]
    (cond
      (not (integer? version))
      [(validation-error "Task spec :version must be an integer"
                         {:type :task-spec/invalid
                          :field :version
                          :path [:version]
                          :value version})]

      (not (contains? supported-task-spec-versions version))
      [(validation-error (str "Unsupported task spec version " version)
                         {:type :task-spec/invalid
                          :field :version
                          :path [:version]
                          :value version
                          :supported-versions (sort supported-task-spec-versions)})]

      :else
      [])))

(defn- step-path
  [step & path]
  (into [:steps (:id step)] path))

(defn- field-present?
  [m field]
  (contains? m field))

(defn- any-field-present?
  [m fields]
  (boolean (some #(field-present? m %) fields)))

(defn- first-present-field
  [m fields]
  (some #(when (field-present? m %) %) fields))

(defn- missing-field-error
  [step field message]
  (validation-error message
                    {:type :task-spec/invalid
                     :step-id (:id step)
                     :field field
                     :path (step-path step field)}))

(defn- normalize-ref-id
  [value]
  (try
    (normalize-id :step-id value)
    (catch Exception _
      nil)))

(defn- expression-form?
  [value]
  (and (vector? value)
       (seq value)
       (or (keyword? (first value))
           (symbol? (first value)))))

(def ^:private expression-arities
  {:literal {:min 1 :max 1}
   :input {:min 1 :max 1}
   :output {:min 1 :max 2}
   :step-status {:min 1 :max 1}
   :step-ok? {:min 1 :max 1}
   :step-skipped? {:min 1 :max 1}
   :step-failed? {:min 1 :max 1}
   :get {:min 2 :max 3}
   :get-in {:min 2 :max 2}
   :count {:min 1 :max 1}
   := {:min 2}
   :not= {:min 2}
   :> {:min 2}
   :>= {:min 2}
   :< {:min 2}
   :<= {:min 2}
   :and {:min 0}
   :or {:min 0}
   :not {:min 1 :max 1}
   :empty? {:min 1 :max 1}
   :present? {:min 1 :max 1}
   :contains? {:min 2 :max 2}
   :if {:min 2 :max 3}
   :merge {:min 0}
   :str {:min 0}
   :keyword {:min 1 :max 1}})

(defn- arity-errors
  [step path op arg-count arity]
  (let [{:keys [min max]} arity]
    (cond-> []
      (and min (< arg-count (long min)))
      (conj (validation-error
             (str "Task spec expression " (name op) " expects at least " min
                  " argument" (when (not= 1 min) "s"))
             {:type :task-spec/invalid-expression
              :step-id (:id step)
              :operator op
              :path path
              :argument-count arg-count
              :min-arguments min}))

      (and max (> arg-count (long max)))
      (conj (validation-error
             (str "Task spec expression " (name op) " expects at most " max
                  " argument" (when (not= 1 max) "s"))
             {:type :task-spec/invalid-expression
              :step-id (:id step)
              :operator op
              :path path
              :argument-count arg-count
              :max-arguments max})))))

(declare expression-errors)

(defn- step-reference-errors
  [step-ids step path op value]
  (let [step-id (normalize-ref-id value)]
    (cond
      (nil? step-id)
      [(validation-error (str "Task spec expression " (name op)
                              " requires a step id")
                         {:type :task-spec/invalid-expression
                          :step-id (:id step)
                          :operator op
                          :path path
                          :value value})]

      (not (contains? step-ids step-id))
      [(validation-error "Task spec expression references an unknown step"
                         {:type :task-spec/invalid-expression
                          :step-id (:id step)
                          :operator op
                          :path path
                          :references step-id})]

      :else
      [])))

(defn- expression-arg-errors
  [step-ids step path args indexes]
  (into []
        (mapcat (fn [idx]
                  (if (< idx (count args))
                    (expression-errors step-ids
                                       step
                                       (conj path idx)
                                       (nth args idx))
                    [])))
        indexes))

(defn- vector-expression-errors
  [step-ids step path [op & args :as expr]]
  (let [op*   (normalize-kind op)
        arity (get expression-arities op*)]
    (if-not arity
      [(validation-error "Unknown task spec expression operator"
                         {:type :task-spec/unknown-expression
                          :step-id (:id step)
                          :operator op
                          :path path
                          :expr expr})]
      (let [base-errors (arity-errors step path op* (count args) arity)]
        (into base-errors
              (case op*
                :literal
                []

                :input
                []

                :output
                (if (seq args)
                  (step-reference-errors step-ids step path op* (first args))
                  [])

                (:step-status :step-ok? :step-skipped? :step-failed?)
                (if (seq args)
                  (step-reference-errors step-ids step path op* (first args))
                  [])

                :get
                (expression-arg-errors step-ids step path args [0])

                :get-in
                (concat
                 (expression-arg-errors step-ids step path args [0])
                 (when (and (<= 2 (count args))
                            (not (sequential? (second args))))
                   [(validation-error "Task spec :get-in path must be sequential"
                                      {:type :task-spec/invalid-expression
                                       :step-id (:id step)
                                       :operator op*
                                       :path (conj path 1)
                                       :value (second args)})]))

                (:count :not :empty? :present? :keyword)
                (expression-arg-errors step-ids step path args [0])

                (:contains?)
                (expression-arg-errors step-ids step path args [0 1])

                (:if)
                (expression-arg-errors step-ids step path args (range (count args)))

                (:= :not= :> :>= :< :<= :and :or :merge :str)
                (expression-arg-errors step-ids step path args (range (count args)))

                []))))))

(defn- expression-errors
  [step-ids step path value]
  (cond
    (expression-form? value)
    (vector-expression-errors step-ids step path value)

    (map? value)
    (into []
          (mapcat (fn [[k v]]
                    (expression-errors step-ids step (conj path k) v)))
          value)

    (vector? value)
    (into []
          (mapcat (fn [[idx item]]
                    (expression-errors step-ids step (conj path idx) item)))
          (map-indexed vector value))

    :else
    []))

(defn- schema-field-value
  [schema field]
  (let [field-name (name field)]
    (cond
      (contains? schema field) (get schema field)
      (contains? schema field-name) (get schema field-name)
      :else nil)))

(defn- schema-field-present?
  [schema field]
  (let [field-name (name field)]
    (or (contains? schema field)
        (contains? schema field-name))))

(defn- schema-type-values
  [type*]
  (cond
    (nil? type*) []
    (sequential? type*) type*
    :else [type*]))

(declare output-schema-errors)

(defn- schema-type-errors
  [step path schema]
  (when (schema-field-present? schema :type)
    (into []
          (keep (fn [type*]
                  (let [type-id (normalize-kind type*)]
                    (when-not (contains? schema-primitive-types type-id)
                      (validation-error
                       "Task spec output schema has an unsupported :type"
                       {:type :task-spec/invalid-schema
                        :step-id (:id step)
                        :field :type
                        :path (conj path :type)
                        :value type*
                        :supported-types (sort schema-primitive-types)})))))
          (schema-type-values (schema-field-value schema :type)))))

(defn- schema-required-errors
  [step path schema]
  (when (schema-field-present? schema :required)
    (let [required (schema-field-value schema :required)]
      (if-not (sequential? required)
        [(validation-error "Task spec output schema :required must be sequential"
                           {:type :task-spec/invalid-schema
                            :step-id (:id step)
                            :field :required
                            :path (conj path :required)
                            :value required})]
        (into []
              (keep-indexed (fn [idx key]
                              (when-not (or (keyword? key)
                                            (symbol? key)
                                            (string? key))
                                (validation-error
                                 "Task spec output schema :required entries must be names"
                                 {:type :task-spec/invalid-schema
                                  :step-id (:id step)
                                  :field :required
                                  :path (conj path :required idx)
                                  :value key}))))
              required)))))

(defn- schema-properties-errors
  [step path schema]
  (when (schema-field-present? schema :properties)
    (let [properties (schema-field-value schema :properties)]
      (if-not (map? properties)
        [(validation-error "Task spec output schema :properties must be a map"
                           {:type :task-spec/invalid-schema
                            :step-id (:id step)
                            :field :properties
                            :path (conj path :properties)
                            :value properties})]
        (into []
              (mapcat (fn [[k child-schema]]
                        (let [child-path (conj path :properties k)]
                          (if (map? child-schema)
                            (output-schema-errors step child-path child-schema)
                            [(validation-error
                              "Task spec output schema property must be a schema map"
                              {:type :task-spec/invalid-schema
                               :step-id (:id step)
                               :field :properties
                               :path child-path
                               :value child-schema})]))))
              properties)))))

(defn- schema-items-errors
  [step path schema]
  (when (schema-field-present? schema :items)
    (let [items (schema-field-value schema :items)
          item-path (conj path :items)]
      (cond
        (map? items)
        (output-schema-errors step item-path items)

        (sequential? items)
        (into []
              (mapcat (fn [[idx item-schema]]
                        (if (map? item-schema)
                          (output-schema-errors step (conj item-path idx) item-schema)
                          [(validation-error
                            "Task spec output schema item must be a schema map"
                            {:type :task-spec/invalid-schema
                             :step-id (:id step)
                             :field :items
                             :path (conj item-path idx)
                             :value item-schema})])))
              (map-indexed vector items))

        :else
        [(validation-error "Task spec output schema :items must be a schema map or sequence"
                           {:type :task-spec/invalid-schema
                            :step-id (:id step)
                            :field :items
                            :path item-path
                            :value items})]))))

(defn- output-schema-errors
  [step path schema]
  (if-not (map? schema)
    [(validation-error "Task spec llm :output-schema must be a map"
                       {:type :task-spec/invalid-schema
                        :step-id (:id step)
                        :field :output-schema
                        :path path
                        :value schema})]
    (vec (concat (schema-type-errors step path schema)
                 (schema-required-errors step path schema)
                 (schema-properties-errors step path schema)
                 (schema-items-errors step path schema)))))

(defn- validation-errors-from-exception
  [e path]
  (let [data (ex-data e)]
    (if-let [errors (:errors data)]
      (mapv #(update % :path (fn [issue-path]
                               (into path (or issue-path []))))
            errors)
      [(validation-error (or (ex-message e) "Task spec validation failed")
                         (merge {:type (or (:type data) :task-spec/invalid)
                                 :path path}
                                (dissoc data :errors)))])))

(defn- nested-spec-errors
  [step path spec]
  (try
    (normalize-spec spec)
    []
    (catch clojure.lang.ExceptionInfo e
      (validation-errors-from-exception e path))))

(defn- validate-nested-spec-field
  [step owner field path]
  (when (contains? owner field)
    (nested-spec-errors step path (get owner field))))

(defn- validate-contract-field
  [step owner path]
  (when (contains? owner :contract)
    (let [contract (:contract owner)]
      (cond
        (not (map? contract))
        [(validation-error "Task spec nested :contract must be a map"
                           {:type :task-spec/invalid
                            :step-id (:id step)
                            :field :contract
                            :path path
                            :value contract})]

        (not= :task (:kind contract))
        [(validation-error "Task spec nested :contract must have :kind :task"
                           {:type :task-spec/invalid
                            :step-id (:id step)
                            :field :contract
                            :path path
                            :value (:kind contract)})]

        (not (contains? contract :spec))
        [(validation-error "Task spec nested :contract requires :spec"
                           {:type :task-spec/invalid
                            :step-id (:id step)
                            :field :contract
                            :path path})]

        :else
        (nested-spec-errors step (conj path :spec) (:spec contract))))))

(defn- nested-owner-spec-errors
  [step owner path]
  (vec (concat
        (validate-contract-field step owner (conj path :contract))
        (validate-nested-spec-field step owner :spec (conj path :spec))
        (when (and (not (contains? owner :contract))
                   (not (contains? owner :spec))
                   (contains? owner :steps))
          (nested-spec-errors step path owner)))))

(defn- literal-positive-integer-errors
  [step field path value]
  (when (and (some? value)
             (not (expression-form? value))
             (not (and (integer? value)
                       (pos? (long value)))))
    [(validation-error (str "Task spec " (name field) " must be a positive integer")
                       {:type :task-spec/invalid
                        :step-id (:id step)
                        :field field
                        :path path
                        :value value})]))

(defn- literal-retry-errors
  [step step-ids]
  (when (contains? step :retry)
    (let [retry* (:retry step)
          path   (step-path step :retry)]
      (vec
       (concat
        (expression-errors step-ids step path retry*)
        (when (and (not (expression-form? retry*))
                   (not (or (nil? retry*)
                            (boolean? retry*)
                            (number? retry*)
                            (map? retry*))))
          [(validation-error "Task spec :retry must be a boolean, number, or map"
                             {:type :task-spec/invalid
                              :step-id (:id step)
                              :field :retry
                              :path path
                              :value retry*})])
        (when (map? retry*)
          (into []
                (mapcat (fn [[field value]]
                          (case field
                            (:max-attempts :attempts :max-retries :retries
                             :delay-ms :initial-delay-ms :max-delay-ms)
                            (literal-positive-integer-errors step
                                                             field
                                                             (conj path field)
                                                             value)

                            :backoff-factor
                            (when (and (some? value)
                                       (not (expression-form? value))
                                       (not (and (number? value)
                                                 (pos? (double value)))))
                              [(validation-error
                                "Task spec retry :backoff-factor must be a positive number"
                                {:type :task-spec/invalid
                                 :step-id (:id step)
                                 :field :backoff-factor
                                 :path (conj path field)
                                 :value value})])

                            [])))
                retry*)))))))

(defn- step-common-expression-errors
  [step-ids step]
  (vec
   (concat
    (when (contains? step :when)
      (expression-errors step-ids step (step-path step :when) (:when step)))
    (when (contains? step :timeout-ms)
      (concat
       (expression-errors step-ids step (step-path step :timeout-ms) (:timeout-ms step))
       (literal-positive-integer-errors step
                                        :timeout-ms
                                        (step-path step :timeout-ms)
                                        (:timeout-ms step))))
    (literal-retry-errors step step-ids))))

(defn- step-input-expression-errors
  [step-ids step owner path]
  (when (contains? owner :inputs)
    (expression-errors step-ids step (conj path :inputs) (:inputs owner))))

(defn- text-option-expression-errors
  [step-ids step fields]
  (into []
        (mapcat (fn [field]
                  (when (contains? step field)
                    (expression-errors step-ids step (step-path step field) (get step field)))))
        fields))

(defn- llm-output-schema-validation-errors
  [step-ids step]
  (when (contains? step :output-schema)
    (let [schema (:output-schema step)
          path   (step-path step :output-schema)]
      (if (expression-form? schema)
        (expression-errors step-ids step path schema)
        (output-schema-errors step path schema)))))

(defn- llm-mode-errors
  [step]
  (when (contains? step :mode)
    (let [mode (normalize-kind (:mode step))]
      (when-not (contains? llm-step-modes mode)
        [(validation-error "Task spec llm :mode is not supported"
                           {:type :task-spec/invalid
                            :step-id (:id step)
                            :field :mode
                            :path (step-path step :mode)
                            :value (:mode step)
                            :supported-modes (sort llm-step-modes)})]))))

(defn- branch-mode-errors
  [step]
  (when (contains? step :mode)
    (let [mode (normalize-kind (:mode step))]
      (when-not (contains? branch-modes mode)
        [(validation-error "Task spec branch :mode must be :async or :join"
                           {:type :task-spec/invalid
                            :step-id (:id step)
                            :field :mode
                            :path (step-path step :mode)
                            :value (:mode step)
                            :supported-modes (sort branch-modes)})]))))

(defn- parallel-branch-errors
  [step-ids step]
  (let [branches (or (:branches step)
                     (:tasks step)
                     (:children step))]
    (cond
      (nil? branches)
      [(missing-field-error step :branches
                            "Task spec parallel step requires :branches")]

      (map? branches)
      (into []
            (mapcat (fn [[branch-id entry]]
                      (let [entry* (if (map? entry)
                                     (assoc entry :id (normalize-id :parallel-entry-id branch-id))
                                     {:id (normalize-id :parallel-entry-id branch-id)
                                      :spec entry})
                            path   (step-path step :branches branch-id)]
                        (vec
                         (concat
                          (step-input-expression-errors step-ids step entry* path)
                          (if (or (contains? entry* :contract)
                                  (contains? entry* :spec)
                                  (contains? entry* :steps))
                            (nested-owner-spec-errors step entry* path)
                            [(validation-error
                              "Task spec parallel entry requires :spec"
                              {:type :task-spec/invalid
                               :step-id (:id step)
                               :field :spec
                               :path path})])))))
            branches)

      (sequential? branches)
      (into []
            (mapcat (fn [[idx entry]]
                      (let [path (step-path step :branches idx)]
                        (if-not (map? entry)
                          [(validation-error "Task spec parallel entries must be maps"
                                             {:type :task-spec/invalid
                                              :step-id (:id step)
                                              :field :branches
                                              :path path
                                              :value entry})]
                          (vec
                           (concat
                            (when-not (contains? entry :id)
                              [(validation-error
                                "Task spec parallel entries require :id"
                                {:type :task-spec/invalid
                                 :step-id (:id step)
                                 :field :id
                                 :path (conj path :id)})])
                            (step-input-expression-errors step-ids step entry path)
                            (if (or (contains? entry :contract)
                                    (contains? entry :spec)
                                    (contains? entry :steps))
                              (nested-owner-spec-errors step entry path)
                              [(validation-error
                                "Task spec parallel entry requires :spec"
                                {:type :task-spec/invalid
                                 :step-id (:id step)
                                 :field :spec
                                 :path path})])))))))
            (map-indexed vector branches))

      :else
      [(validation-error "Task spec parallel :branches must be a map or sequence"
                         {:type :task-spec/invalid
                          :step-id (:id step)
                          :field :branches
                          :path (step-path step :branches)
                          :value branches})]))))

(defn- step-kind-errors
  [step-ids step]
  (let [kind (:kind step)]
    (case kind
      (:value :emit)
      (concat
       (when-not (contains? step :value)
         [(missing-field-error step :value
                               "Task spec value step requires :value")])
       (when (contains? step :value)
         (expression-errors step-ids step (step-path step :value) (:value step))))

      :condition
      (concat
       (when-not (contains? step :expr)
         [(missing-field-error step :expr
                               "Task spec condition step requires :expr")])
       (when (contains? step :expr)
         (expression-errors step-ids step (step-path step :expr) (:expr step))))

      :tool
      (concat
       (when-not (any-field-present? step [:tool :tool-id])
         [(missing-field-error step :tool
                               "Task spec tool step requires :tool or :tool-id")])
       (when (contains? step :args)
         (expression-errors step-ids step (step-path step :args) (:args step))))

      :approval
      (concat
       (when (contains? step :args)
         (expression-errors step-ids step (step-path step :args) (:args step)))
       (when (contains? step :arguments)
         (expression-errors step-ids step (step-path step :arguments) (:arguments step))))

      :input
      []

      :llm
      (concat
       (llm-mode-errors step)
       (step-input-expression-errors step-ids step step (step-path step))
       (text-option-expression-errors step-ids step [:prompt :message :goal :task])
       (llm-output-schema-validation-errors step-ids step)
       (text-option-expression-errors step-ids step
                                      [:output-format :format :json?
                                       :structured-output? :provider-id :provider
                                       :workload :model :temperature :max-tokens
                                       :max-output-tokens :budget]))

      :subtask
      (concat
       (step-input-expression-errors step-ids step step (step-path step))
       (if (or (contains? step :contract)
               (contains? step :spec))
         (nested-owner-spec-errors step step (step-path step))
         [(missing-field-error step :spec
                               "Task spec subtask step requires :spec")]))

      :branch
      (concat
       (branch-mode-errors step)
       (step-input-expression-errors step-ids step step (step-path step))
       (text-option-expression-errors step-ids step [:prompt :message :goal :task])
       (when (or (contains? step :contract)
                 (contains? step :spec))
         (nested-owner-spec-errors step step (step-path step)))
       (when-not (or (contains? step :contract)
                     (contains? step :spec)
                     (some nonblank-string
                           [(:prompt step) (:message step) (:goal step) (:task step)]))
         [(missing-field-error step :spec
                               "Task spec branch step requires :spec or :prompt")]))

      :parallel
      (concat
       (step-input-expression-errors step-ids step step (step-path step))
       (parallel-branch-errors step-ids step))

      :map
      (let [items-field (first-present-field step [:items :collection :coll :each])]
        (concat
         (step-input-expression-errors step-ids step step (step-path step))
         (when-not items-field
           [(missing-field-error step :items
                                 "Task spec map step requires :items")])
         (when items-field
           (expression-errors step-ids
                              step
                              (step-path step items-field)
                              (get step items-field)))
         (if (contains? step :spec)
           (nested-spec-errors step (step-path step :spec) (:spec step))
           [(missing-field-error step :spec
                                 "Task spec map step requires :spec")])))

      :loop
      (concat
       (step-input-expression-errors step-ids step step (step-path step))
       (when (contains? step :initial)
         (expression-errors step-ids step (step-path step :initial) (:initial step)))
       (when (contains? step :while)
         (expression-errors step-ids step (step-path step :while) (:while step)))
       (when (contains? step :until)
         (expression-errors step-ids step (step-path step :until) (:until step)))
       (when (contains? step :max-iterations)
         (concat
          (expression-errors step-ids step
                             (step-path step :max-iterations)
                             (:max-iterations step))
          (literal-positive-integer-errors step
                                           :max-iterations
                                           (step-path step :max-iterations)
                                           (:max-iterations step))))
       (if (contains? step :spec)
         (nested-spec-errors step (step-path step :spec) (:spec step))
         [(missing-field-error step :spec
                               "Task spec loop step requires :spec")]))

      [])))

(defn- normalized-spec-validation-errors
  [spec]
  (let [steps    (:steps spec)
        step-ids (set (map :id steps))]
    (into []
          (mapcat (fn [step]
                    (concat
                     (step-common-expression-errors step-ids step)
                     (step-kind-errors step-ids step))))
          steps)))

(defn- normalized-spec-warnings
  [spec]
  (let [registered (registered-executors)]
    (into []
          (mapcat
           (fn [step]
             (concat
              (when (and (not (contains? builtin-step-kinds (:kind step)))
                         (not (contains? registered (:kind step))))
                [(validation-warning
                  "Task spec step kind has no built-in or registered executor"
                  {:type :task-spec/missing-executor
                   :step-id (:id step)
                   :field :kind
                   :path (step-path step :kind)
                   :kind (:kind step)})])
              (when (and (= :tool (:kind step))
                         (any-field-present? step [:tool :tool-id]))
                (let [tool-id (normalize-tool-id (or (:tool step) (:tool-id step)))]
                  (when (and tool-id
                             (try
                               (nil? (db/get-tool tool-id))
                               (catch Exception _
                                 false)))
                    [(validation-warning
                      "Task spec tool is not registered"
                      {:type :task-spec/missing-tool
                       :step-id (:id step)
                       :field :tool
                       :path (step-path step :tool)
                       :tool-id tool-id})])))
              (when (and (= :llm (:kind step))
                         (not (some nonblank-string
                                    [(:prompt step) (:message step)
                                     (:goal step) (:task step)])))
                [(validation-warning
                  "Task spec llm step has no prompt/message/goal/task"
                  {:type :task-spec/missing-prompt
                   :step-id (:id step)
                   :path (step-path step)})]))))
          (:steps spec))))

(defn normalize-spec
  "Normalize and validate a declarative task spec."
  [spec]
  (when-not (map? spec)
    (throw (ex-info "Task spec must be a map"
                    {:type :task-spec/invalid
                     :spec spec})))
  (throw-validation-errors! (supported-version-errors spec))
  (when (and (some? (:steps spec))
             (not (sequential? (:steps spec))))
    (throw (ex-info "Task spec :steps must be a sequence of step maps"
                    {:type :task-spec/invalid
                     :field :steps
                     :value (:steps spec)})))
  (let [steps (mapv normalize-step (:steps spec))]
    (when-not (seq steps)
      (throw (ex-info "Task spec requires at least one step"
                      {:type :task-spec/invalid
                       :field :steps})))
    (let [ids (map :id steps)]
      (when-not (= (count ids) (count (distinct ids)))
        (throw (ex-info "Task spec step ids must be unique"
                        {:type :task-spec/invalid
                         :field :steps
                         :ids ids}))))
    (validate-step-dependencies! steps)
    (let [spec* (assoc spec
                       :kind :task
                       :version (or (:version spec) task-spec-version)
                       :steps steps)]
      (throw-validation-errors! (normalized-spec-validation-errors spec*))
      spec*)))

(defn validate-spec
  "Validate a declarative task spec.

   Returns `{:valid? boolean :errors [...] :warnings [...] :spec normalized-spec}`
   when normalization succeeds. Warnings are advisory and may depend on the
   currently registered executors/tools."
  [spec]
  (try
    (let [spec* (normalize-spec spec)]
      {:valid? true
       :errors []
       :warnings (normalized-spec-warnings spec*)
       :spec spec*})
    (catch clojure.lang.ExceptionInfo e
      {:valid? false
       :errors (validation-errors-from-exception e [])
       :warnings []})))

(defn task-contract
  [spec]
  (let [spec* (normalize-spec spec)]
    {:kind :task
     :version 1
     :goal (or (nonblank-string (:goal spec*))
               (nonblank-string (:title spec*))
               "Task spec")
     :spec spec*}))

(defn task-spec
  [task-or-contract]
  (let [contract (if (and (map? task-or-contract)
                          (contains? task-or-contract :contract))
                   (:contract task-or-contract)
                   task-or-contract)]
    (when (= :task (:kind contract))
      (:spec contract))))

(defn task-spec-task?
  [task-or-contract]
  (boolean (task-spec task-or-contract)))

(defn- initial-task-spec-state
  [spec]
  {:version task-spec-version
   :status :ready
   :steps (into {}
                (map (fn [{:keys [id kind]}]
                       [id {:id id
                            :kind kind
                            :status :pending}]))
                (:steps spec))
   :outputs {}
   :updated-at (now)})

(defn- task-spec-runtime-state
  [task]
  (get-in task [:meta runtime-key]))

(defn- task-spec-state
  [task spec]
  (merge (initial-task-spec-state spec)
         (task-spec-runtime-state task)))

(defn- merge-task-meta
  [task task-spec-state*]
  (-> (or (:meta task) {})
      (assoc runtime-key task-spec-state*)))

(defn- persist-task-spec-state!
  [task-id task-spec-state*]
  (when-let [task (db/get-task task-id)]
    (db/update-task! task-id {:meta (merge-task-meta task task-spec-state*)}))
  task-spec-state*)

(defn- sync-task-state!
  [task-id task-spec-state* attrs]
  (when-let [task (db/get-task task-id)]
    (task-runtime/sync-runtime-task!
     task-id
     (assoc attrs :meta (merge-task-meta task task-spec-state*))))
  task-spec-state*)

(defn- path-value
  [m path]
  (if (sequential? path)
    (get-in m (vec path))
    (get m path)))

(declare eval-expr
         positive-long-guardrail
         run-task!)

(defn- truthy?
  [value]
  (not (or (nil? value) (false? value))))

(defn- present?
  [value]
  (cond
    (nil? value) false
    (string? value) (boolean (nonblank-string value))
    (coll? value) (not (empty? value))
    :else true))

(defn- compare-op
  [f values]
  (boolean (apply f values)))

(defn- eval-vector-expr
  [env [op & args :as expr]]
  (let [op* (normalize-kind op)]
    (case op*
      :literal (first args)
      :input (path-value (:inputs env) (first args))
      :output (let [[step-id path] args
                    value (get (:outputs env) (normalize-id :step-id step-id))]
                (if (some? path)
                  (path-value value path)
                  value))
      :step-status (get-in env [:steps (normalize-id :step-id (first args)) :status])
      :step-ok? (contains? success-step-statuses
                           (get-in env [:steps (normalize-id :step-id (first args)) :status]))
      :step-skipped? (= :skipped
                        (get-in env [:steps (normalize-id :step-id (first args)) :status]))
      :step-failed? (= :failed
                       (get-in env [:steps (normalize-id :step-id (first args)) :status]))
      :get (let [[target key default] args
                 target* (eval-expr env target)]
             (if (and (associative? target*)
                      (contains? target* key))
               (get target* key)
               default))
      :get-in (get-in (eval-expr env (first args)) (vec (second args)))
      :count (count (eval-expr env (first args)))
      := (apply = (map #(eval-expr env %) args))
      :not= (not (apply = (map #(eval-expr env %) args)))
      :> (compare-op > (map #(eval-expr env %) args))
      :>= (compare-op >= (map #(eval-expr env %) args))
      :< (compare-op < (map #(eval-expr env %) args))
      :<= (compare-op <= (map #(eval-expr env %) args))
      :and (loop [remaining args]
             (if-let [arg (first remaining)]
               (let [value (eval-expr env arg)]
                 (if (truthy? value)
                   (recur (rest remaining))
                   value))
               true))
      :or (loop [remaining args]
            (if-let [arg (first remaining)]
              (let [value (eval-expr env arg)]
                (if (truthy? value)
                  value
                  (recur (rest remaining))))
              nil))
      :not (not (truthy? (eval-expr env (first args))))
      :empty? (empty? (eval-expr env (first args)))
      :present? (present? (eval-expr env (first args)))
      :contains? (contains? (eval-expr env (first args))
                            (eval-expr env (second args)))
      :if (if (truthy? (eval-expr env (first args)))
            (eval-expr env (second args))
            (eval-expr env (nth args 2 nil)))
      :merge (apply merge (map #(eval-expr env %) args))
      :str (apply str (map #(eval-expr env %) args))
      :keyword (some-> (eval-expr env (first args)) str keyword)
      (throw (ex-info "Unknown task spec expression operator"
                      {:type :task-spec/unknown-expression
                       :operator op
                       :expr expr})))))

(defn- eval-expr
  [env value]
  (cond
    (and (vector? value)
         (seq value)
         (or (keyword? (first value))
             (symbol? (first value))))
    (eval-vector-expr env value)

    (map? value)
    (into (empty value)
          (map (fn [[k v]]
                 [k (eval-expr env v)]))
          value)

    (vector? value)
    (mapv #(eval-expr env %) value)

    :else
    value))

(defn- eval-step-expr
  [state context expr]
  (eval-expr {:inputs (merge (get-in state [:spec :inputs])
                             (:inputs context))
              :outputs (:outputs state)
              :steps (:steps state)
              :context context}
             expr))

(defn- step-summary
  [step result]
  (or (:summary result)
      (:summary step)
      (str "Task step " (name (:id step)) " "
           (name (or (:status result) :completed)))))

(defn- result-status
  [result]
  (or (normalize-kind (:status result)) :success))

(defn- value-executor
  [{:keys [state context step]}]
  {:status :success
   :output (eval-step-expr state context (:value step))})

(defn- condition-executor
  [{:keys [state context step]}]
  (let [value (truthy? (eval-step-expr state context (:expr step)))]
    {:status (if value :success :skipped)
     :output value
     :summary (str "Condition " (name (:id step)) " was "
                   (if value "true" "false"))}))

(defn- normalize-tool-id
  [value]
  (cond
    (keyword? value) value
    (symbol? value) (keyword (name value))
    (string? value) (some-> value str/trim not-empty keyword)
    :else nil))

(defn- tool-executor
  [{:keys [state context step task-id turn-id]}]
  (let [tool-id (or (normalize-tool-id (:tool step))
                    (normalize-tool-id (:tool-id step)))
        args    (or (eval-step-expr state context (:args step)) {})]
    (when-not tool-id
      (throw (ex-info "Task spec tool step requires :tool or :tool-id"
                      {:type :task-spec/invalid
                       :step-id (:id step)})))
    (task-runtime/record-task-item!
     turn-id
     {:type :tool-call
      :status :requested
      :summary (str "Requested tool " (name tool-id))
      :tool-id (name tool-id)
      :data {:tool-name (name tool-id)
             :arguments args
             :step-id (name (:id step))}})
    (let [result (tool/execute-tool tool-id
                                    args
                                    (merge context
                                           {:task-id task-id
                                            :task-turn-id turn-id
                                            :task-step-id (:id step)}))
          status (if (:error result) :error :success)
          summary (or (:summary result)
                      (:error result)
                      (some-> (:content result) str)
                      (str "Tool " (name tool-id) " completed"))]
      (task-runtime/record-task-item!
       turn-id
       {:type :tool-result
        :status status
        :summary summary
        :tool-id (name tool-id)
        :data (cond-> {:tool-name (name tool-id)
                       :status (name status)
                       :step-id (name (:id step))}
                (contains? result :content) (assoc :content (:content result))
                (:summary result) (assoc :summary (:summary result))
                (:error result) (assoc :error (:error result))
                (contains? result :result) (assoc :result (:result result)))})
      (if (:error result)
        {:status :failed
         :error (:error result)
         :output result}
        {:status :success
         :output result
         :summary summary}))))

(defn- task-channel
  [task context]
  (or (:channel context)
      (:channel task)
      :default))

(defn- task-session-id
  [task context]
  (or (:session-id context)
      (:session-id task)))

(defn- sync-waiting-state!
  [task-id state summary]
  (task-runtime/sync-runtime-task! task-id
                                   {:state state
                                    :summary summary}))

(defn- restore-running-state!
  [task-id summary]
  (task-runtime/sync-runtime-task! task-id
                                   {:state :running
                                    :summary summary
                                    :stop-reason nil
                                    :error nil
                                    :finished-at nil}))

(defn- interaction-hooks
  [task-id turn-id]
  {:task-runtime/on-input-request
   (fn [{:keys [label mask?]}]
     (let [summary (str "Waiting for input: " label)]
       (sync-waiting-state! task-id :waiting_input summary)
       (task-runtime/record-task-item!
        turn-id
        {:type :input-request
         :status :waiting
         :summary summary
         :data {:label label
                :masked (boolean mask?)}})))

   :task-runtime/on-input-response
   (fn [{:keys [label mask? provided]}]
     (let [summary (str "Received input for " label)]
       (restore-running-state! task-id summary)
       (task-runtime/record-task-item!
        turn-id
        {:type :system-note
         :status :success
         :summary summary
         :data {:kind "input-response"
                :label label
                :masked (boolean mask?)
                :provided (boolean provided)}})))

   :task-runtime/on-approval-request
   (fn [{:keys [tool-id tool-name description arguments policy reason]}]
     (let [tool-label (or tool-name (some-> tool-id name) "approval")
           summary    (str "Waiting for approval for " tool-label)]
       (sync-waiting-state! task-id :waiting_approval summary)
       (task-runtime/record-task-item!
        turn-id
        {:type :approval-request
         :status :waiting
         :tool-id tool-label
         :summary summary
         :data (cond-> {:tool-name tool-label}
                 tool-id (assoc :tool-id (name tool-id))
                 description (assoc :description description)
                 arguments (assoc :arguments arguments)
                 policy (assoc :policy (name policy))
                 reason (assoc :reason reason))})))

   :task-runtime/on-approval-decision
   (fn [{:keys [tool-id tool-name approved? policy]}]
     (let [tool-label (or tool-name (some-> tool-id name) "approval")
           summary    (str "Approval "
                           (if approved? "granted" "denied")
                           " for "
                           tool-label)]
       (restore-running-state! task-id summary)
       (task-runtime/record-task-item!
        turn-id
        {:type :system-note
         :status (if approved? :success :error)
         :tool-id tool-label
         :summary summary
         :data (cond-> {:kind "approval-decision"
                        :tool-name tool-label
                        :approved (boolean approved?)}
                 tool-id (assoc :tool-id (name tool-id))
                 policy (assoc :policy (name policy)))})))})

(defn- interaction-context
  [task-id turn-id context]
  (let [task     (db/get-task task-id)
        context* (merge context
                        {:session-id (task-session-id task context)
                         :task-id task-id
                         :task-turn-id turn-id
                         :channel (task-channel task context)}
                        (interaction-hooks task-id turn-id))]
    context*))

(defn- with-interaction-context
  [task-id turn-id context f]
  (let [context* (interaction-context task-id turn-id context)]
    (binding [prompt/*interaction-context* context*]
      (f))))

(defn- input-label
  [step]
  (or (nonblank-string (:label step))
      (nonblank-string (:prompt step))
      (nonblank-string (:message step))
      "Input"))

(defn- input-executor
  [{:keys [context step task-id turn-id]}]
  (let [label (input-label step)
        mask? (boolean (or (:mask? step)
                           (:masked? step)))]
    (with-interaction-context
      task-id
      turn-id
      context
      (fn []
        (let [value (prompt/prompt! label :mask? mask?)]
          {:status :success
           :output value
           :summary (str "Received input for " label)})))))

(defn- approval-step-request
  [state context step]
  (let [tool-id (or (normalize-tool-id (:tool-id step))
                    (normalize-tool-id (:tool step))
                    (normalize-tool-id (:id step)))
        args    (or (eval-step-expr state context (:args step))
                    (eval-step-expr state context (:arguments step))
                    {})]
    (cond-> {:tool-id tool-id
             :tool-name (or (nonblank-string (:tool-name step))
                            (nonblank-string (:label step))
                            (some-> tool-id name))
             :description (or (nonblank-string (:description step))
                              (nonblank-string (:prompt step))
                              (nonblank-string (:message step)))
             :arguments args}
      (:reason step) (assoc :reason (:reason step))
      (:policy step) (assoc :policy (normalize-kind (:policy step))))))

(defn- approval-executor
  [{:keys [state context step task-id turn-id]}]
  (let [request   (approval-step-request state context step)
        tool-name (or (:tool-name request)
                      (some-> (:tool-id request) name)
                      "approval")]
    (with-interaction-context
      task-id
      turn-id
      context
      (fn []
        (let [approved? (prompt/approve! request)]
          (if approved?
            {:status :success
             :output {:approved true}
             :summary (str "Approval granted for " tool-name)}
            {:status :failed
             :output {:approved false}
             :error (str "approval denied for " tool-name)
             :summary (str "Approval denied for " tool-name)}))))))

(def ^:private llm-agent-modes
  #{:agent :interactive})

(defn- llm-step-mode
  [step]
  (or (normalize-kind (:mode step)) :transform))

(defn llm-agent-step?
  "Return true when an `:llm` task step should use the open-ended agent loop."
  [step]
  (contains? llm-agent-modes (llm-step-mode step)))

(defn- step-option
  [state context step key]
  (when (contains? step key)
    (eval-step-expr state context (get step key))))

(defn- map-value
  [m key]
  (when (map? m)
    (let [key-name (some-> key name)]
      (or (get m key)
          (when key-name
            (or (get m key-name)
                (get m (keyword key-name))))))))

(defn- normalize-llm-keyword
  [value]
  (cond
    (nil? value) nil
    (keyword? value) value
    (symbol? value) (keyword (name value))
    (string? value) (some-> value str/trim not-empty keyword)
    :else value))

(defn- parse-long-option
  [value]
  (cond
    (nil? value) nil
    (integer? value) (long value)
    (number? value) (long value)
    (string? value) (try
                      (Long/parseLong (str/trim value))
                      (catch Exception _
                        nil))
    :else nil))

(defn- parse-double-option
  [value]
  (cond
    (nil? value) nil
    (number? value) (double value)
    (string? value) (try
                      (Double/parseDouble (str/trim value))
                      (catch Exception _
                        nil))
    :else nil))

(defn- json-text
  [value]
  (try
    (json/write-json-str value {:indent-str "  "})
    (catch Exception _
      (pr-str value))))

(defn- llm-value-text
  [value]
  (cond
    (nil? value) nil
    (string? value) value
    :else (json-text value)))

(defn- step-text-option
  [state context step key]
  (some-> (step-option state context step key)
          llm-value-text
          nonblank-string))

(defn- llm-step-inputs
  [state context step]
  (if (contains? step :inputs)
    (let [inputs (eval-step-expr state context (:inputs step))]
      (when-not (or (nil? inputs) (map? inputs))
        (throw (ex-info "Task spec llm :inputs must evaluate to a map"
                        {:type :task-spec/invalid
                         :step-id (:id step)
                         :inputs inputs})))
      (or inputs {}))
    {}))

(defn- llm-output-schema
  [state context step]
  (when (contains? step :output-schema)
    (let [schema-form (:output-schema step)
          schema      (if (and (vector? schema-form)
                               (seq schema-form)
                               (or (keyword? (first schema-form))
                                   (symbol? (first schema-form))))
                        (eval-step-expr state context schema-form)
                        schema-form)]
      (when-not (map? schema)
        (throw (ex-info "Task spec llm :output-schema must evaluate to a map"
                        {:type :task-spec/invalid
                         :step-id (:id step)
                         :output-schema schema})))
      schema)))

(defn- llm-output-format
  [state context step]
  (or (normalize-kind (step-option state context step :output-format))
      (normalize-kind (step-option state context step :format))))

(defn- llm-json-output?
  [state context step schema]
  (boolean
   (or schema
       (= :json (llm-output-format state context step))
       (truthy? (step-option state context step :json?))
       (truthy? (step-option state context step :structured-output?)))))

(defn- schema-key
  [key]
  (cond
    (keyword? key) key
    (symbol? key) (keyword (name key))
    (string? key) (keyword key)
    :else key))

(defn- keywordize-json
  [value]
  (cond
    (map? value)
    (into {}
          (map (fn [[k v]]
                 [(schema-key k) (keywordize-json v)]))
          value)

    (vector? value)
    (mapv keywordize-json value)

    (sequential? value)
    (mapv keywordize-json value)

    :else
    value))

(defn- schema-types
  [schema]
  (let [type* (map-value schema :type)]
    (cond
      (nil? type*) []
      (sequential? type*) (into [] (keep normalize-kind) type*)
      :else (cond-> [] (normalize-kind type*) (conj (normalize-kind type*))))))

(defn- schema-path-label
  [path]
  (if (seq path)
    (str "$." (str/join "." (map name path)))
    "$"))

(defn- schema-type-match?
  [type* value]
  (case type*
    :object (map? value)
    :array (sequential? value)
    :string (string? value)
    :number (number? value)
    :integer (integer? value)
    :boolean (or (true? value) (false? value))
    :null (nil? value)
    true))

(declare validate-output-schema!)

(defn- validate-object-schema!
  [schema value path]
  (let [required   (into [] (map schema-key) (or (map-value schema :required) []))
        properties (when-let [properties* (map-value schema :properties)]
                     (into {}
                           (map (fn [[k v]]
                                  [(schema-key k) v]))
                           properties*))]
    (doseq [key required]
      (when-not (contains? value key)
        (throw (ex-info (str "LLM output missing required field "
                             (schema-path-label (conj path key)))
                        {:type :task-spec/schema-validation
                         :path (conj path key)
                         :required required
                         :output value}))))
    (doseq [[key child-schema] properties
            :when (contains? value key)]
      (validate-output-schema! child-schema (get value key) (conj path key)))))

(defn- validate-array-schema!
  [schema value path]
  (when-let [item-schema (map-value schema :items)]
    (doseq [[idx item] (map-indexed vector value)]
      (validate-output-schema! item-schema item (conj path (keyword (str idx)))))))

(defn- validate-output-schema!
  ([schema value]
   (validate-output-schema! schema value []))
  ([schema value path]
   (when (map? schema)
     (let [types       (set (schema-types schema))
           objectish?  (or (contains? types :object)
                           (map-value schema :properties)
                           (seq (map-value schema :required)))
           arrayish?   (or (contains? types :array)
                           (map-value schema :items))]
       (when (and (seq types)
                  (not-any? #(schema-type-match? % value) types))
         (throw (ex-info (str "LLM output field "
                              (schema-path-label path)
                              " does not match schema type "
                              (str/join "|" (map name types)))
                         {:type :task-spec/schema-validation
                          :path path
                          :schema schema
                          :output value})))
       (when objectish?
         (when-not (map? value)
           (throw (ex-info (str "LLM output field "
                                (schema-path-label path)
                                " must be an object")
                           {:type :task-spec/schema-validation
                            :path path
                            :schema schema
                            :output value})))
         (validate-object-schema! schema value path))
       (when arrayish?
         (when-not (sequential? value)
           (throw (ex-info (str "LLM output field "
                                (schema-path-label path)
                                " must be an array")
                           {:type :task-spec/schema-validation
                            :path path
                            :schema schema
                            :output value})))
         (validate-array-schema! schema value path))))
   value))

(defn- strip-json-fence
  [text]
  (let [text* (str/trim text)]
    (if-let [[_ body] (re-matches #"(?is)^```(?:json)?\s*(.*?)\s*```$" text*)]
      (str/trim body)
      text*)))

(defn- json-start-index
  [text]
  (let [object-start (.indexOf text "{")
        array-start  (.indexOf text "[")]
    (cond
      (and (neg? object-start) (neg? array-start)) -1
      (neg? object-start) array-start
      (neg? array-start) object-start
      :else (min object-start array-start))))

(defn- extract-json-candidate
  [text]
  (let [start (json-start-index text)]
    (when-not (neg? start)
      (let [end-char (case (.charAt text start)
                       \{ "}"
                       \[ "]"
                       nil)
            end      (when end-char
                       (.lastIndexOf text end-char))]
        (when (and end (<= start end))
          (subs text start (inc end)))))))

(defn- parse-json-output!
  [step content]
  (let [candidate (strip-json-fence content)
        parse*    (fn [text]
                    (keywordize-json (json/read-json text)))]
    (try
      (parse* candidate)
      (catch Exception e
        (if-let [extracted (extract-json-candidate candidate)]
          (try
            (parse* extracted)
            (catch Exception e2
              (throw (ex-info "LLM task step returned invalid JSON"
                              {:type :task-spec/invalid-json-output
                               :step-id (:id step)
                               :response-preview (subs candidate
                                                       0
                                                       (min 240 (count candidate)))}
                              e2))))
          (throw (ex-info "LLM task step returned invalid JSON"
                          {:type :task-spec/invalid-json-output
                           :step-id (:id step)
                           :response-preview (subs candidate
                                                   0
                                                   (min 240 (count candidate)))}
                          e)))))))

(defn- llm-step-prompt
  [state context step]
  (or (step-text-option state context step :prompt)
      (step-text-option state context step :message)
      (step-text-option state context step :goal)
      (step-text-option state context step :task)
      (nonblank-string (:message context))
      "Complete this task step."))

(defn- llm-system-instruction
  [mode]
  (str "You are executing one declarative task step. "
       "Complete only this step and do not continue the broader task. "
       (case mode
         :judge "Make a bounded judgment from the prompt and inputs. "
         :judgment "Make a bounded judgment from the prompt and inputs. "
         :transform "Transform the inputs according to the prompt. "
         "")))

(defn- llm-output-instruction
  [schema json-output?]
  (cond
    schema
    (str "Return only valid JSON matching this JSON schema. "
         "Do not include Markdown fences or commentary.\n"
         (json-text schema))

    json-output?
    "Return only valid JSON. Do not include Markdown fences or commentary."

    :else
    "Return only the result for this step."))

(defn- llm-step-messages
  [state context step inputs schema json-output?]
  (let [mode   (llm-step-mode step)
        prompt* (llm-step-prompt state context step)
        user-content (str prompt*
                          (when (seq inputs)
                            (str "\n\nInputs:\n" (json-text inputs)))
                          "\n\n"
                          (llm-output-instruction schema json-output?))]
    [{"role" "system"
      "content" (llm-system-instruction mode)}
     {"role" "user"
      "content" user-content}]))

(defn- llm-request-options
  [state context step]
  (let [budget      (when (contains? step :budget)
                      (eval-step-expr state context (:budget step)))
        provider-id (or (step-option state context step :provider-id)
                        (step-option state context step :provider)
                        (:provider-id context))
        workload    (or (step-option state context step :workload)
                        (:workload context)
                        :assistant)
        model       (or (step-option state context step :model)
                        (map-value budget :model))
        temperature (or (step-option state context step :temperature)
                        (map-value budget :temperature))
        max-tokens  (or (step-option state context step :max-tokens)
                        (step-option state context step :max-output-tokens)
                        (map-value budget :max-tokens)
                        (map-value budget :max-output-tokens))]
    (cond-> {}
      (:session-id context)
      (assoc :session-id (:session-id context))

      provider-id
      (assoc :provider-id (normalize-llm-keyword provider-id))

      workload
      (assoc :workload (normalize-llm-keyword workload))

      model
      (assoc :model (str model))

      (some? (parse-double-option temperature))
      (assoc :temperature (parse-double-option temperature))

      (some? (parse-long-option max-tokens))
      (assoc :max-tokens (parse-long-option max-tokens)))))

(defn- assistant-content
  [message]
  (or (get message "content")
      (:content message)
      ""))

(defn- short-summary
  [text max-chars]
  (when-let [text* (nonblank-string text)]
    (if (> (count text*) max-chars)
      (str (subs text* 0 max-chars) "...")
      text*)))

(defn- record-llm-request!
  [turn-id step inputs schema json-output? opts]
  (task-runtime/record-task-item!
   turn-id
   {:type :system-note
    :status :requested
    :summary (str "Requested LLM " (name (llm-step-mode step)) " step")
    :data (cond-> {:kind "llm-request"
                   :step-id (name (:id step))
                   :step-mode (name (llm-step-mode step))
                   :structured-output (boolean json-output?)
                   :inputs inputs}
            schema (assoc :output-schema schema)
            (:provider-id opts) (assoc :provider-id (name (:provider-id opts)))
            (:workload opts) (assoc :workload (name (:workload opts)))
            (:model opts) (assoc :model (:model opts))
            (:temperature opts) (assoc :temperature (:temperature opts))
            (:max-tokens opts) (assoc :max-tokens (:max-tokens opts)))}))

(defn- record-llm-result!
  [turn-id step content output metadata structured?]
  (task-runtime/record-task-item!
   turn-id
   (cond-> {:type :assistant-message
            :role :assistant
            :status :success
            :summary (or (short-summary content 240)
                         (str "LLM " (name (llm-step-mode step)) " step completed"))
            :data {:kind "llm-result"
                   :step-id (name (:id step))
                   :step-mode (name (llm-step-mode step))
                   :structured-output (boolean structured?)
                   :content content
                   :output output}}
     (:llm-call-id metadata) (assoc :llm-call-id (:llm-call-id metadata))
     (:provider-id metadata) (assoc-in [:data :provider-id] (name (:provider-id metadata)))
     (:workload metadata) (assoc-in [:data :workload] (name (:workload metadata)))
     (:model metadata) (assoc-in [:data :model] (:model metadata)))))

(defn llm-executor
  "Run a bounded `:llm` task step as first-class task-spec dataflow."
  [{:keys [state context step task-id turn-id]}]
  (if (llm-agent-step? step)
    {:status :paused
     :pause-reason :missing-executor
     :summary "Paused before agent LLM task step"
     :error "missing task step executor: llm agent"}
    (let [context* (interaction-context task-id turn-id context)]
      (binding [prompt/*interaction-context* context*]
        (let [inputs       (llm-step-inputs state context* step)
              schema       (llm-output-schema state context* step)
              json-output? (llm-json-output? state context* step schema)
              messages     (llm-step-messages state context* step inputs schema json-output?)
              opts         (llm-request-options state context* step)]
          (record-llm-request! turn-id step inputs schema json-output? opts)
          (let [message  (apply llm/chat-message messages (mapcat identity opts))
                metadata (meta message)
                content  (assistant-content message)
                output   (if json-output?
                           (let [parsed (parse-json-output! step content)]
                             (when schema
                               (validate-output-schema! schema parsed))
                             parsed)
                           content)]
            (record-llm-result! turn-id step content output metadata json-output?)
            {:status :success
             :summary (str "LLM " (name (llm-step-mode step)) " step completed")
             :output output}))))))

(defn- task-execution-mode
  [task]
  (or (get-in task [:meta :execution :mode])
      :hybrid))

(defn- subtask-title
  [step spec]
  (or (nonblank-string (:title step))
      (nonblank-string (:goal step))
      (nonblank-string (:goal spec))
      (nonblank-string (:summary step))
      (str "Subtask " (name (:id step)))))

(defn- branch-title
  [step spec]
  (or (nonblank-string (:title step))
      (nonblank-string (:task step))
      (nonblank-string (:goal step))
      (nonblank-string (:goal spec))
      (nonblank-string (:summary step))
      (str "Branch " (name (:id step)))))

(defn- subtask-raw-spec
  [step]
  (or (when-let [contract (:contract step)]
        (task-spec contract))
      (:spec step)
      (throw (ex-info "Task spec subtask step requires :spec"
                      {:type :task-spec/invalid
                       :step-id (:id step)}))))

(defn- subtask-spec
  [step]
  (let [spec  (normalize-spec (subtask-raw-spec step))
        goal* (or (nonblank-string (:goal spec))
                  (nonblank-string (:goal step))
                  (nonblank-string (:title step))
                  (nonblank-string (:summary step)))]
    (cond-> spec
      goal* (assoc :goal goal*))))

(defn- branch-raw-spec
  [step]
  (or (when-let [contract (:contract step)]
        (task-spec contract))
      (:spec step)
      (when-let [prompt* (or (nonblank-string (:prompt step))
                             (nonblank-string (:message step))
                             (nonblank-string (:goal step))
                             (nonblank-string (:task step)))]
        {:goal (or (nonblank-string (:goal step))
                   (nonblank-string (:title step))
                   (nonblank-string (:task step))
                   "Branch task")
         :steps [{:id :work-on-branch
                  :kind :llm
                  :mode :agent
                  :prompt prompt*}]})
      (throw (ex-info "Task spec branch step requires :spec or :prompt"
                      {:type :task-spec/invalid
                       :step-id (:id step)}))))

(defn- branch-spec
  [step]
  (let [spec  (normalize-spec (branch-raw-spec step))
        goal* (or (nonblank-string (:goal spec))
                  (nonblank-string (:goal step))
                  (nonblank-string (:title step))
                  (nonblank-string (:task step))
                  (nonblank-string (:summary step)))]
    (cond-> spec
      goal* (assoc :goal goal*))))

(defn- branch-mode
  [step]
  (let [mode (or (normalize-kind (:mode step)) :async)]
    (case mode
      :async :async
      :join :join
      (throw (ex-info "Task spec branch :mode must be :async or :join"
                      {:type :task-spec/invalid
                       :step-id (:id step)
                       :mode mode})))))

(defn- subtask-child-id
  [state step]
  (or (get-in state [:steps (:id step) :output :task-id])
      (get-in state [:steps (:id step) :output "task-id"])
      (get-in state [:steps (:id step) :subtask-task-id])))

(defn- subtask-task-match?
  [parent-task-id step-id task]
  (and (= parent-task-id (:parent-id task))
       (= :subtask (get-in task [:meta :trigger :kind]))
       (= step-id (get-in task [:meta :trigger :parent-step-id]))))

(defn- branch-task-match?
  [parent-task-id step-id task]
  (and (= parent-task-id (:parent-id task))
       (= :branch (get-in task [:meta :trigger :kind]))
       (= step-id (get-in task [:meta :trigger :parent-step-id]))))

(defn- latest-subtask-task
  [parent-task-id step-id]
  (->> (db/list-tasks {:limit 100000})
       (filter #(subtask-task-match? parent-task-id step-id %))
       (sort-by #(or (:updated-at %) (:created-at %)) #(compare %2 %1))
       first))

(defn- latest-branch-task
  [parent-task-id step-id]
  (->> (db/list-tasks {:limit 100000})
       (filter #(branch-task-match? parent-task-id step-id %))
       (sort-by #(or (:updated-at %) (:created-at %)) #(compare %2 %1))
       first))

(defn- create-subtask-task!
  [parent-task turn-id step spec]
  (let [contract (task-contract spec)
        spec*    (:spec contract)
        title*   (subtask-title step spec*)]
    (db/create-task!
     (cond-> {:session-id (:session-id parent-task)
              :parent-id (:id parent-task)
              :channel (or (:channel parent-task) :task-spec)
              :type :task
              :state :resumable
              :title title*
              :summary title*
              :contract contract
              :meta {:trigger {:kind :subtask
                               :parent-task-id (:id parent-task)
                               :parent-turn-id turn-id
                               :parent-step-id (:id step)}
                     :execution {:mode (task-execution-mode parent-task)}
                     runtime-key (initial-task-spec-state spec*)}}
       (:session-id parent-task) (assoc :session-role :subtask)))))

(defn- create-branch-session!
  [parent-task step]
  (when-let [parent-session-id (:session-id parent-task)]
    (db/create-session! :branch
                        {:parent-session-id parent-session-id
                         :worker? true
                         :active? false
                         :label (or (nonblank-string (:title step))
                                    (nonblank-string (:task step))
                                    (nonblank-string (:goal step))
                                    (str "Branch " (name (:id step))))})))

(defn- create-branch-task!
  [parent-task turn-id step spec]
  (let [contract         (task-contract spec)
        spec*            (:spec contract)
        title*           (branch-title step spec*)
        child-session-id (create-branch-session! parent-task step)
        parent-session-id (:session-id parent-task)
        branch-meta      (cond-> {:trigger {:kind :branch
                                            :parent-task-id (:id parent-task)
                                            :parent-turn-id turn-id
                                            :parent-step-id (:id step)}
                                  :execution {:mode :agent}
                                  :branch-worker true
                                  runtime-key (initial-task-spec-state spec*)}
                           parent-session-id
                           (assoc :parent-session-id parent-session-id
                                  :resource-session-id parent-session-id))
        task-id          (db/create-task!
                          (cond-> {:session-id child-session-id
                                   :parent-id (:id parent-task)
                                   :channel :branch
                                   :type :task
                                   :state :resumable
                                   :title title*
                                   :summary title*
                                   :contract contract
                                   :meta branch-meta}
                            child-session-id (assoc :session-role :branch)))]
    (task-runtime/attach-child-task-to-parent! parent-task task-id title*)
    task-id))

(defn- ensure-subtask-task!
  [parent-task turn-id state step spec]
  (let [child-id (subtask-child-id state step)]
    (or (when child-id
          (some-> child-id db/get-task :id))
        (some-> (latest-subtask-task (:id parent-task) (:id step)) :id)
        (create-subtask-task! parent-task turn-id step spec))))

(defn- ensure-branch-task!
  [parent-task turn-id state step spec]
  (let [child-id (subtask-child-id state step)]
    (or (when child-id
          (some-> child-id db/get-task :id))
        (some-> (latest-branch-task (:id parent-task) (:id step)) :id)
        (create-branch-task! parent-task turn-id step spec))))

(defn- subtask-step-inputs
  [state context step]
  (when (contains? step :inputs)
    (let [inputs (eval-step-expr state context (:inputs step))]
      (when-not (or (nil? inputs) (map? inputs))
        (throw (ex-info "Task spec subtask :inputs must evaluate to a map"
                        {:type :task-spec/invalid
                         :step-id (:id step)
                         :inputs inputs})))
      inputs)))

(defn- subtask-context
  [state context step parent-task-id]
  (let [inputs (subtask-step-inputs state context step)
        message (or (nonblank-string (:message step))
                    (nonblank-string (:prompt step))
                    (nonblank-string (:goal step))
                    (:message context))]
    (cond-> (assoc context :parent-task-id parent-task-id)
      inputs (update :inputs merge inputs)
      message (assoc :message message))))

(defn- branch-context
  [state context step parent-task]
  (let [context*          (subtask-context state context step (:id parent-task))
        parent-session-id (:session-id parent-task)
        resource-session-id (or (:resource-session-id context)
                                parent-session-id)]
    (cond-> (merge context*
                   {:channel :branch
                    :branch-worker? true})
      parent-session-id (assoc :parent-session-id parent-session-id)
      resource-session-id (assoc :resource-session-id resource-session-id))))

(defn- task-spec-outputs
  [task-or-state]
  (or (:outputs task-or-state)
      (get-in task-or-state [:meta runtime-key :outputs])
      {}))

(defn- subtask-output
  [child-task-id child-result]
  (let [child-task (db/get-task child-task-id)]
    (cond-> {:task-id child-task-id
             :status (:status child-result)
             :outputs (task-spec-outputs (or (:state child-result)
                                             child-task))}
      (:summary child-result) (assoc :summary (:summary child-result))
      (:turn-id child-result) (assoc :turn-id (:turn-id child-result))
      (:error child-result) (assoc :error (:error child-result)))))

(defn- completed-subtask-result
  [child-task-id child-task]
  {:status :completed
   :task-id child-task-id
   :summary (or (:summary child-task) "Subtask completed")
   :state (get-in child-task [:meta runtime-key])})

(defn- run-subtask!
  [child-task-id context executors max-steps]
  (let [child-task (db/get-task child-task-id)]
    (if (and (= :completed (:state child-task))
             (= :completed (get-in child-task [:meta runtime-key :status])))
      (completed-subtask-result child-task-id child-task)
      (run-task! child-task-id
                 :context context
                 :executors executors
                 :max-steps max-steps))))

(defn- child-result->step-result
  [kind child-task-id child-result]
  (let [output  (subtask-output child-task-id child-result)
        summary (or (:summary child-result)
                    (str (case kind
                           :branch "Branch"
                           "Subtask")
                         " "
                         (name (:status child-result))))]
    (case (:status child-result)
      :completed
      {:status :success
       :summary summary
       :output output}

      :paused
      {:status :paused
       :pause-reason (case kind
                       :branch :branch-paused
                       :subtask-paused)
       :summary summary
       :output output}

      :failed
      {:status :failed
       :summary summary
       :error (or (:error child-result)
                  (get-in output [:error])
                  (str (name kind) " failed"))
       :output output}

      {:status :paused
       :pause-reason (case kind
                       :branch :branch-pending
                       :subtask-pending)
       :summary summary
       :output output})))

(defn- subtask-executor
  [{:keys [state context step task-id turn-id executors]}]
  (let [parent-task   (or (db/get-task task-id)
                          (throw (ex-info "Task spec subtask parent task not found"
                                          {:type :task-spec/not-found
                                           :task-id task-id})))
        spec          (subtask-spec step)
        child-task-id (ensure-subtask-task! parent-task turn-id state step spec)
        child-result  (run-subtask! child-task-id
                                    (subtask-context state context step task-id)
                                    executors
                                    (or (:max-steps step) default-max-steps))]
    (child-result->step-result :subtask child-task-id child-result)))

(defn- async-branch-output
  [child-task-id future]
  (let [child-task (db/get-task child-task-id)]
    (cond-> {:task-id child-task-id
             :status :running
             :async true
             :outputs (task-spec-outputs child-task)}
      (:session-id child-task) (assoc :session-id (:session-id child-task))
      future (assoc :submitted true))))

(defn- start-branch-background!
  [child-task-id context executors max-steps]
  (async/submit-background!
   (str "task-spec-branch:" child-task-id)
   (fn []
     (try
       (run-task! child-task-id
                  :context context
                  :executors executors
                  :max-steps max-steps
                  :operation :branch-spawn)
       (finally
         (when-let [session-id (:session-id (db/get-task child-task-id))]
           (db/set-session-active! session-id false)))))))

(defn- branch-executor
  [{:keys [state context step task-id turn-id executors]}]
  (let [parent-task   (or (db/get-task task-id)
                          (throw (ex-info "Task spec branch parent task not found"
                                          {:type :task-spec/not-found
                                           :task-id task-id})))
        spec          (branch-spec step)
        mode          (branch-mode step)
        child-task-id (ensure-branch-task! parent-task turn-id state step spec)
        context*      (branch-context state context step parent-task)
        max-steps*    (or (:max-steps step) default-max-steps)]
    (case mode
      :join
      (child-result->step-result
       :branch
       child-task-id
       (run-subtask! child-task-id context* executors max-steps*))

      :async
      (if-let [future (start-branch-background! child-task-id context* executors max-steps*)]
        {:status :success
         :summary (str "Started branch " (name (:id step)))
         :output (async-branch-output child-task-id future)}
        {:status :paused
         :pause-reason :branch-unavailable
         :summary (str "Paused before starting branch " (name (:id step)))
         :output (async-branch-output child-task-id nil)}))))

(defn- control-key-string
  [value]
  (cond
    (keyword? value) (name value)
    (symbol? value) (name value)
    :else (str value)))

(defn- control-task-match?
  [kind parent-task-id step-id control-key task]
  (and (= parent-task-id (:parent-id task))
       (= kind (get-in task [:meta :trigger :kind]))
       (= step-id (get-in task [:meta :trigger :parent-step-id]))
       (= (control-key-string control-key)
          (get-in task [:meta :trigger :control-key]))))

(defn- latest-control-task
  [kind parent-task-id step-id control-key]
  (->> (db/list-tasks {:limit 100000})
       (filter #(control-task-match? kind parent-task-id step-id control-key %))
       (sort-by #(or (:updated-at %) (:created-at %)) #(compare %2 %1))
       first))

(defn- control-title
  [kind step control-key spec]
  (or (nonblank-string (:title spec))
      (nonblank-string (:goal spec))
      (nonblank-string (:title step))
      (nonblank-string (:goal step))
      (str (str/capitalize (name kind))
           " "
           (name (:id step))
           "/"
           (control-key-string control-key))))

(defn- create-control-task!
  [kind parent-task turn-id step control-key spec]
  (let [contract (task-contract spec)
        spec*    (:spec contract)
        title*   (control-title kind step control-key spec*)]
    (db/create-task!
     (cond-> {:session-id (:session-id parent-task)
              :parent-id (:id parent-task)
              :channel (or (:channel parent-task) :task-spec)
              :type :task
              :state :resumable
              :title title*
              :summary title*
              :contract contract
              :meta {:trigger {:kind kind
                               :parent-task-id (:id parent-task)
                               :parent-turn-id turn-id
                               :parent-step-id (:id step)
                               :control-key (control-key-string control-key)}
                     :execution {:mode (task-execution-mode parent-task)}
                     runtime-key (initial-task-spec-state spec*)}}
       (:session-id parent-task) (assoc :session-role kind)))))

(defn- ensure-control-task!
  [kind parent-task turn-id step control-key spec]
  (or (some-> (latest-control-task kind (:id parent-task) (:id step) control-key) :id)
      (create-control-task! kind parent-task turn-id step control-key spec)))

(defn- control-raw-spec
  [kind step entry]
  (or (when-let [contract (:contract entry)]
        (task-spec contract))
      (:spec entry)
      (when (:steps entry)
        entry)
      (throw (ex-info (str "Task spec " (name kind) " entry requires :spec")
                      {:type :task-spec/invalid
                       :step-id (:id step)
                       :entry entry}))))

(defn- control-spec
  [kind step entry]
  (let [spec  (normalize-spec (control-raw-spec kind step entry))
        goal* (or (nonblank-string (:goal spec))
                  (nonblank-string (:goal entry))
                  (nonblank-string (:title entry))
                  (nonblank-string (:goal step))
                  (nonblank-string (:title step)))]
    (cond-> spec
      goal* (assoc :goal goal*))))

(defn- evaluated-map-field
  [state context owner field label]
  (when (contains? owner field)
    (let [value (eval-step-expr state context (get owner field))]
      (when-not (or (nil? value) (map? value))
        (throw (ex-info (str "Task spec " label " must evaluate to a map")
                        {:type :task-spec/invalid
                         :field field
                         :value value})))
      value)))

(defn- control-inputs
  [state context step entry extra-inputs]
  (merge (or (evaluated-map-field state context step :inputs ":inputs") {})
         (or (evaluated-map-field state context entry :inputs ":inputs") {})
         extra-inputs))

(defn- control-context
  [context parent-task-id inputs]
  (cond-> (assoc context :parent-task-id parent-task-id)
    (seq inputs) (update :inputs merge inputs)))

(defn- control-output-step
  [step]
  (when-let [step-id (or (:output-step step)
                         (:collect-step step)
                         (:result-step step))]
    (normalize-id :output-step step-id)))

(defn- collected-child-output
  [step child-output]
  (if-let [step-id (control-output-step step)]
    (get-in child-output [:outputs step-id])
    (:outputs child-output)))

(defn- run-control-child!
  [kind parent-task turn-id state context executors step control-key entry extra-inputs]
  (let [spec          (control-spec kind step entry)
        child-task-id (ensure-control-task! kind parent-task turn-id step control-key spec)
        inputs        (control-inputs state context step entry extra-inputs)
        child-result  (run-subtask! child-task-id
                                    (control-context context (:id parent-task) inputs)
                                    executors
                                    (or (:max-steps entry)
                                        (:max-steps step)
                                        default-max-steps))
        output        (subtask-output child-task-id child-result)]
    {:key control-key
     :status (:status child-result)
     :result child-result
     :output output
     :value (collected-child-output step output)}))

(defn- control-result-status
  [children]
  (cond
    (some #(= :failed (:status %)) children) :failed
    (some #(= :paused (:status %)) children) :paused
    (every? #(= :completed (:status %)) children) :success
    :else :paused))

(defn- control-error
  [children]
  (or (some #(get-in % [:result :error]) children)
      (some #(get-in % [:output :error]) children)))

(defn- parallel-entry
  [entry]
  (when-not (map? entry)
    (throw (ex-info "Task spec parallel entries must be maps"
                    {:type :task-spec/invalid
                     :entry entry})))
  (let [id (normalize-id :parallel-entry-id (:id entry))]
    (assoc entry :id id)))

(defn- parallel-entries
  [step]
  (let [branches (or (:branches step)
                     (:tasks step)
                     (:children step))]
    (cond
      (map? branches)
      (mapv (fn [[k v]]
              (let [id (normalize-id :parallel-entry-id k)]
                (if (map? v)
                  (assoc v :id id)
                  {:id id :spec v})))
            branches)

      (sequential? branches)
      (mapv parallel-entry branches)

      :else
      (throw (ex-info "Task spec parallel step requires :branches"
                      {:type :task-spec/invalid
                       :step-id (:id step)})))))

(defn- parallel-executor
  [{:keys [state context step task-id turn-id executors]}]
  (let [parent-task (or (db/get-task task-id)
                        (throw (ex-info "Task spec parallel parent task not found"
                                        {:type :task-spec/not-found
                                         :task-id task-id})))
        entries     (parallel-entries step)
        children    (->> entries
                         (mapv (fn [entry]
                                 (future
                                   (run-control-child! :parallel
                                                       parent-task
                                                       turn-id
                                                       state
                                                       context
                                                       executors
                                                       step
                                                       (:id entry)
                                                       entry
                                                       {}))))
                         (mapv deref))
        branches    (into {}
                          (map (fn [{:keys [key output]}]
                                 [key output]))
                          children)
        values      (into {}
                          (map (fn [{:keys [key value]}]
                                 [key value]))
                          children)
        output      {:branches branches
                     :outputs values}
        status      (control-result-status children)]
    (case status
      :success
      {:status :success
       :summary (str "Parallel step " (name (:id step)) " completed")
       :output output}

      :paused
      {:status :paused
       :pause-reason :parallel-paused
       :waiting-for :children
       :summary (str "Parallel step " (name (:id step)) " paused")
       :output output}

      :failed
      {:status :failed
       :summary (str "Parallel step " (name (:id step)) " failed")
       :error (or (control-error children) "parallel child failed")
       :output output})))

(defn- map-items
  [state context step]
  (let [items-expr (or (:items step)
                       (:collection step)
                       (:coll step)
                       (:each step))
        items      (eval-step-expr state context items-expr)]
    (when-not (sequential? items)
      (throw (ex-info "Task spec map :items must evaluate to a sequential collection"
                      {:type :task-spec/invalid
                       :step-id (:id step)
                       :items items})))
    (vec items)))

(defn- map-item-key
  [step]
  (normalize-id :as (or (:as step) :item)))

(defn- map-index-key
  [step]
  (normalize-id :index-as (or (:index-as step) :index)))

(defn- map-executor
  [{:keys [state context step task-id turn-id executors]}]
  (let [parent-task (or (db/get-task task-id)
                        (throw (ex-info "Task spec map parent task not found"
                                        {:type :task-spec/not-found
                                         :task-id task-id})))
        spec-entry  {:spec (or (:spec step)
                               (throw (ex-info "Task spec map step requires :spec"
                                               {:type :task-spec/invalid
                                                :step-id (:id step)})))}
        item-key    (map-item-key step)
        index-key   (map-index-key step)
        children    (mapv (fn [idx item]
                            (let [extra-inputs {item-key item
                                                index-key idx}]
                              (assoc (run-control-child! :map
                                                         parent-task
                                                         turn-id
                                                         state
                                                         context
                                                         executors
                                                         step
                                                         idx
                                                         spec-entry
                                                         extra-inputs)
                                     :index idx
                                     :item item)))
                          (range)
                          (map-items state context step))
        results     (mapv (fn [{:keys [index item output value status]}]
                            {:index index
                             :item item
                             :status status
                             :task-id (:task-id output)
                             :outputs (:outputs output)
                             :value value})
                          children)
        output      {:results results
                     :outputs (mapv :value children)}
        status      (control-result-status children)]
    (case status
      :success
      {:status :success
       :summary (str "Map step " (name (:id step)) " completed")
       :output output}

      :paused
      {:status :paused
       :pause-reason :map-paused
       :waiting-for :children
       :summary (str "Map step " (name (:id step)) " paused")
       :output output}

      :failed
      {:status :failed
       :summary (str "Map step " (name (:id step)) " failed")
       :error (or (control-error children) "map child failed")
       :output output})))

(defn- loop-max-iterations
  [state context step]
  (or (when (contains? step :max-iterations)
        (positive-long-guardrail :max-iterations
                                 (eval-step-expr state context (:max-iterations step))))
      default-loop-max-iterations))

(defn- loop-acc-key
  [step]
  (normalize-id :acc-as (or (:acc-as step)
                            (:accumulator-as step)
                            :acc)))

(defn- loop-index-key
  [step]
  (normalize-id :index-as (or (:index-as step)
                              (:iteration-as step)
                              :iteration)))

(defn- loop-inputs
  [state context step acc iteration]
  (merge (or (evaluated-map-field state context step :inputs ":inputs") {})
         {(loop-acc-key step) acc
          (loop-index-key step) iteration}))

(defn- loop-condition-context
  [state context step acc iteration]
  (update context :inputs merge (loop-inputs state context step acc iteration)))

(defn- loop-continue?
  [state context step acc iteration]
  (let [context* (loop-condition-context state context step acc iteration)
        while?   (if (contains? step :while)
                   (truthy? (eval-step-expr state context* (:while step)))
                   true)
        until?   (when (contains? step :until)
                   (truthy? (eval-step-expr state context* (:until step))))]
    (and while?
         (not until?))))

(defn- loop-control
  [value]
  (when (map? value)
    (normalize-kind (or (:control value)
                        (:loop-control value)))))

(defn- loop-next-value
  [current-acc value]
  (if (and (map? value)
           (loop-control value))
    (if (contains? value :value)
      (:value value)
      current-acc)
    value))

(defn- loop-child-row
  [iteration child current-acc]
  (let [value   (:value child)
        control (loop-control value)
        next*   (loop-next-value current-acc value)]
    (cond-> {:index iteration
             :status (:status child)
             :task-id (get-in child [:output :task-id])
             :outputs (get-in child [:output :outputs])
             :value next*}
      control (assoc :control control
                     :raw-value value))))

(defn- loop-executor
  [{:keys [state context step task-id turn-id executors]}]
  (let [parent-task (or (db/get-task task-id)
                        (throw (ex-info "Task spec loop parent task not found"
                                        {:type :task-spec/not-found
                                         :task-id task-id})))
        spec-entry  {:spec (or (:spec step)
                               (throw (ex-info "Task spec loop step requires :spec"
                                               {:type :task-spec/invalid
                                                :step-id (:id step)})))}
        max-iterations (loop-max-iterations state context step)
        initial-acc (when (contains? step :initial)
                      (eval-step-expr state context (:initial step)))]
    (loop [iteration 0
           acc initial-acc
           children []]
      (cond
        (>= (long iteration) (long max-iterations))
        {:status :success
         :summary (str "Loop step " (name (:id step)) " reached max iterations")
         :output {:iterations children
                  :outputs (mapv :value children)
                  :value acc
                  :stopped :max-iterations
                  :max-iterations max-iterations}}

        (not (loop-continue? state context step acc iteration))
        {:status :success
         :summary (str "Loop step " (name (:id step)) " completed")
         :output {:iterations children
                  :outputs (mapv :value children)
                  :value acc
                  :stopped :condition
                  :max-iterations max-iterations}}

        :else
        (let [extra-inputs (loop-inputs state context step acc iteration)
              child       (assoc (run-control-child! :loop
                                                     parent-task
                                                     turn-id
                                                     state
                                                     context
                                                     executors
                                                     step
                                                     iteration
                                                     spec-entry
                                                     extra-inputs)
                                 :index iteration)
              child-row   (loop-child-row iteration child acc)
              children*   (conj children child-row)
              control     (:control child-row)]
          (case (:status child)
            :completed
            (case control
              :break
              {:status :success
               :summary (str "Loop step " (name (:id step)) " stopped")
               :output {:iterations children*
                        :outputs (mapv :value children*)
                        :value (:value child-row)
                        :stopped :break
                        :current-iteration iteration
                        :max-iterations max-iterations}}

              (:continue nil)
              (recur (inc iteration)
                     (:value child-row)
                     children*)

              (throw (ex-info "Task spec loop control must be :break or :continue"
                              {:type :task-spec/invalid
                               :field :control
                               :step-id (:id step)
                               :value control})))

            :paused
            {:status :paused
             :pause-reason :loop-paused
             :waiting-for :children
             :summary (str "Loop step " (name (:id step)) " paused")
             :output {:iterations children*
                      :outputs (mapv :value children*)
                      :value acc
                      :current-iteration iteration
                      :max-iterations max-iterations}}

            :failed
            {:status :failed
             :summary (str "Loop step " (name (:id step)) " failed")
             :error (or (get-in child [:result :error])
                        (get-in child [:output :error])
                        "loop child failed")
             :output {:iterations children*
                      :outputs (mapv :value children*)
                      :value acc
                      :current-iteration iteration
                      :max-iterations max-iterations}}))))))

(defn- missing-executor
  [kind]
  (fn [_]
    {:status :paused
     :pause-reason :missing-executor
     :summary (str "Paused before " (name kind) " task step")
     :error (str "missing task step executor: " (name kind))}))

(def ^:private default-executors
  {:value value-executor
   :emit value-executor
   :condition condition-executor
   :tool tool-executor
   :input input-executor
   :approval approval-executor
   :llm llm-executor
   :branch branch-executor
   :subtask subtask-executor
   :parallel parallel-executor
   :map map-executor
   :loop loop-executor})

(defn- resolve-executors
  [executors]
  (merge default-executors
         (registered-executors)
         executors))

(defn- due-step?
  [state step]
  (not (contains? terminal-step-statuses
                  (get-in state [:steps (:id step) :status]))))

(defn- dependency-status
  [state dep-id]
  (get-in state [:steps dep-id :status]))

(defn- dependency-satisfied?
  [state dep-id]
  (contains? dependency-satisfied-statuses
             (dependency-status state dep-id)))

(defn- dependencies-satisfied?
  [state step]
  (every? #(dependency-satisfied? state %) (:depends-on step)))

(defn- ready-step?
  [state step]
  (and (due-step? state step)
       (dependencies-satisfied? state step)))

(defn- blocked-dependencies
  [state step]
  (into []
        (keep (fn [dep-id]
                (when-not (dependency-satisfied? state dep-id)
                  {:step-id dep-id
                   :status (or (dependency-status state dep-id)
                               :unknown)})))
        (:depends-on step)))

(defn- dependency-blocked-result
  [state spec]
  (let [blocked (into []
                      (keep (fn [step]
                              (when (due-step? state step)
                                (when-let [deps (seq (blocked-dependencies state step))]
                                  {:step-id (:id step)
                                   :blocked-by deps}))))
                      (:steps spec))]
    (when (seq blocked)
      {:status :failed
       :summary "Task spec dependencies are blocked"
       :error "task spec has pending steps but no runnable dependency path"
       :blocked-dependencies blocked})))

(defn- next-step
  [state spec]
  (first (filter #(ready-step? state %) (:steps spec))))

(defn- update-step-state
  [state step-id f & args]
  (apply update-in state [:steps step-id] f args))

(defn- pause-reason
  [result]
  (or (:pause-reason result)
      (:reason result)
      (get-in result [:pause :reason])
      (get-in result [:pause :pause-reason])
      :paused))

(defn- normalize-pause-payload
  [result]
  (let [pause*  (when (map? (:pause result))
                  (select-keys (:pause result) pause-payload-keys))
        payload (merge pause*
                       (select-keys result pause-payload-keys))
        reason  (normalize-kind (pause-reason result))]
    (cond-> (-> payload
                (dissoc :pause-reason)
                (assoc :reason reason))
      (nil? (:waiting-for payload))
      (assoc :waiting-for reason))))

(defn- paused-result
  [result]
  (let [pause (normalize-pause-payload result)]
    (assoc result
           :pause pause
           :pause-reason (:reason pause)
           :waiting-for (:waiting-for pause))))

(defn- step-pause-state
  [state step]
  (let [step-state (get-in state [:steps (:id step)])
        pause      (:pause step-state)]
    (cond
      (map? pause)
      pause

      (:pause-reason step-state)
      (normalize-pause-payload step-state)

      :else
      nil)))

(defn- context-resume-token
  [context]
  (or (:resume-token context)
      (get-in context [:resume :token])))

(defn- context-resume-input
  [context step]
  (cond
    (contains? context :resume-input)
    {:provided? true
     :value (:resume-input context)}

    (contains? context :resume)
    {:provided? (contains? (:resume context) :input)
     :value (get-in context [:resume :input])}

    (contains? (:resume-inputs context) (:id step))
    {:provided? true
     :value (get-in context [:resume-inputs (:id step)])}

    (contains? (:resume-inputs context) (name (:id step)))
    {:provided? true
     :value (get-in context [:resume-inputs (name (:id step))])}

    :else
    {:provided? false
     :value nil}))

(defn- with-pause-context
  [context pause resume-token resume-input]
  (cond-> context
    pause
    (assoc :pause pause)

    resume-token
    (assoc :resume-token resume-token)

    (:provided? resume-input)
    (assoc :resume-input (:value resume-input)
           :resume-input-provided? true)))

(defn- mark-step-running
  [state step]
  (let [at (now)]
    (-> state
        (assoc :status :running
               :current-step-id (:id step)
               :updated-at at)
        (update-step-state (:id step)
                           merge
                           {:status :running
                            :started-at at
                            :updated-at at}))))

(defn- mark-step-result
  [state step result]
  (let [at      (now)
        status  (result-status result)
        summary (step-summary step result)]
    (cond-> (-> (cond-> state
                  (not= :paused status)
                  (as-> state*
                        (apply dissoc state* step-pause-state-keys)))
                (assoc :updated-at at)
                (update-step-state (:id step)
                                   (fn [step-state]
                                     (merge (cond-> (or step-state {})
                                              (not= :paused status)
                                              (as-> step-state*
                                                    (apply dissoc step-state*
                                                           step-pause-state-keys)))
                                            (cond-> {:status status
                                                     :summary summary
                                                     :finished-at at
                                                     :updated-at at}
                                              (contains? result :output)
                                              (assoc :output (:output result))
                                              (:error result)
                                              (assoc :error (:error result))
                                              (:attempt result)
                                              (assoc :attempt (:attempt result))
                                              (:attempts result)
                                              (assoc :attempts (:attempts result))
                                              (:max-attempts result)
                                              (assoc :max-attempts (:max-attempts result))
                                              (:timeout-ms result)
                                              (assoc :timeout-ms (:timeout-ms result))
                                              (:pause result)
                                              (assoc :pause (:pause result))
                                              (:pause-reason result)
                                              (assoc :pause-reason (:pause-reason result))
                                              (:waiting-for result)
                                              (assoc :waiting-for (:waiting-for result))
                                              (get-in result [:pause :resume-token])
                                              (assoc :resume-token (get-in result [:pause :resume-token]))
                                              (get-in result [:pause :deadline])
                                              (assoc :deadline (get-in result [:pause :deadline]))
                                              (get-in result [:pause :deadline-at])
                                              (assoc :deadline-at (get-in result [:pause :deadline-at])))))))
      (contains? result :output)
      (assoc-in [:outputs (:id step)] (:output result)))))

(defn- record-task-step-item!
  [turn-id step result]
  (task-runtime/record-task-item!
   turn-id
   (cond-> {:type :task-step
            :status (result-status result)
            :summary (step-summary step result)
            :data {:step-id (name (:id step))
                   :step-kind (name (:kind step))
                   :status (name (result-status result))}}
     (contains? result :output)
     (assoc-in [:data :output] (:output result))
     (:error result)
     (assoc-in [:data :error] (:error result))
     (:attempt result)
     (assoc-in [:data :attempt] (:attempt result))
     (:attempts result)
     (assoc-in [:data :attempts] (:attempts result))
     (:max-attempts result)
     (assoc-in [:data :max-attempts] (:max-attempts result))
     (:timeout-ms result)
     (assoc-in [:data :timeout-ms] (:timeout-ms result))
     (:pause result)
     (assoc-in [:data :pause] (:pause result))
     (:pause-reason result)
     (assoc-in [:data :pause-reason] (name (:pause-reason result)))
     (:waiting-for result)
     (assoc-in [:data :waiting-for] (name (:waiting-for result))))))

(defn- skipped-result
  [step reason]
  {:status :skipped
   :summary (or reason
                (str "Skipped task step " (name (:id step))))})

(defn- positive-long-guardrail
  [field value]
  (cond
    (or (nil? value) (false? value))
    nil

    (integer? value)
    (when (pos? (long value))
      (long value))

    (number? value)
    (let [value* (long value)]
      (when (pos? value*)
        value*))

    (string? value)
    (let [value* (try
                   (Long/parseLong (str/trim value))
                   (catch Exception _
                     nil))]
      (cond
        (and value* (pos? value*)) value*
        (and value* (zero? value*)) nil
        :else (throw (ex-info (str "Task spec " (name field) " must be a positive integer")
                              {:type :task-spec/invalid
                               :field field
                               :value value}))))

    :else
    (throw (ex-info (str "Task spec " (name field) " must be a positive integer")
                    {:type :task-spec/invalid
                     :field field
                     :value value}))))

(defn- positive-double-guardrail
  [field value]
  (cond
    (or (nil? value) (false? value))
    nil

    (number? value)
    (let [value* (double value)]
      (when (pos? value*)
        value*))

    (string? value)
    (let [value* (try
                   (Double/parseDouble (str/trim value))
                   (catch Exception _
                     nil))]
      (cond
        (and value* (pos? value*)) value*
        (and value* (zero? value*)) nil
        :else (throw (ex-info (str "Task spec " (name field) " must be a positive number")
                              {:type :task-spec/invalid
                               :field field
                               :value value}))))

    :else
    (throw (ex-info (str "Task spec " (name field) " must be a positive number")
                    {:type :task-spec/invalid
                     :field field
                     :value value}))))

(defn- step-timeout-ms
  [state context step]
  (when (contains? step :timeout-ms)
    (positive-long-guardrail :timeout-ms
                             (eval-step-expr state context (:timeout-ms step)))))

(defn- retry-form
  [state context step]
  (when (contains? step :retry)
    (eval-step-expr state context (:retry step))))

(defn- retry-max-attempts
  [retry*]
  (cond
    (or (nil? retry*) (false? retry*))
    1

    (true? retry*)
    default-retry-max-attempts

    (integer? retry*)
    (max 1 (long retry*))

    (number? retry*)
    (max 1 (long retry*))

    (map? retry*)
    (if-let [max-attempts (or (map-value retry* :max-attempts)
                              (map-value retry* :attempts))]
      (max 1 (or (positive-long-guardrail :retry.max-attempts max-attempts)
                 1))
      (if-let [max-retries (or (map-value retry* :max-retries)
                               (map-value retry* :retries))]
        (inc (or (positive-long-guardrail :retry.max-retries max-retries)
                 0))
        default-retry-max-attempts))

    :else
    (throw (ex-info "Task spec :retry must be a boolean, number, or map"
                    {:type :task-spec/invalid
                     :field :retry
                     :value retry*}))))

(defn- retry-policy
  [state context step]
  (let [retry*       (retry-form state context step)
        max-attempts (retry-max-attempts retry*)
        delay-ms     (when (map? retry*)
                       (or (map-value retry* :delay-ms)
                           (map-value retry* :initial-delay-ms)))
        max-delay-ms (when (map? retry*)
                       (map-value retry* :max-delay-ms))
        backoff      (when (map? retry*)
                       (map-value retry* :backoff-factor))]
    {:enabled? (contains? step :retry)
     :max-attempts max-attempts
     :delay-ms (or (positive-long-guardrail :retry.delay-ms delay-ms)
                   default-retry-delay-ms)
     :max-delay-ms (positive-long-guardrail :retry.max-delay-ms max-delay-ms)
     :backoff-factor (or (positive-double-guardrail :retry.backoff-factor backoff)
                         default-retry-backoff-factor)}))

(defn- retry-delay-ms
  [policy failed-attempt]
  (let [base-delay (long (:delay-ms policy 0))
        backoff    (double (:backoff-factor policy default-retry-backoff-factor))
        delay      (long (Math/round (* (double base-delay)
                                        (Math/pow backoff
                                                  (double (dec (long failed-attempt)))))))]
    (if-let [max-delay (:max-delay-ms policy)]
      (min delay (long max-delay))
      delay)))

(defn- exception-result
  [step e]
  (let [data (ex-data e)]
    (cond-> {:status :failed
             :error (or (.getMessage e) (str e))
             :summary (str "Task step " (name (:id step)) " failed")}
      (contains? data :retryable?) (assoc :retryable? (:retryable? data))
      (contains? data :type) (assoc :error-type (:type data)))))

(defn- timeout-result
  [step timeout-ms]
  {:status :failed
   :summary (str "Task step " (name (:id step)) " timed out")
   :error (str "task step " (name (:id step)) " timed out after " timeout-ms " ms")
   :timeout-ms timeout-ms
   :retryable? true})

(defn- execute-step-once
  [executors state context task-id turn-id step]
  (if (and (contains? step :when)
           (not (truthy? (eval-step-expr state context (:when step)))))
    (skipped-result step "Skipped because task step condition was false")
    (let [pause        (step-pause-state state step)
          resume-token (context-resume-token context)
          resume-input (context-resume-input context step)
          context*     (with-pause-context context pause resume-token resume-input)]
      (if-let [executor (get executors (:kind step))]
        (executor (cond-> {:task-id task-id
                           :turn-id turn-id
                           :state state
                           :context context*
                           :executors executors
                           :step step}
                    pause (assoc :pause pause)
                    resume-token (assoc :resume-token resume-token)
                    (:provided? resume-input)
                    (assoc :resume-input (:value resume-input)
                           :resume-input-provided? true)))
        {:status :paused
         :pause-reason :unsupported-step
         :waiting-for :executor
         :summary (str "Paused before unsupported task step kind "
                       (name (:kind step)))
         :error (str "unsupported task step kind: " (name (:kind step)))}))))

(defn- execute-step-attempt
  [executors state context task-id turn-id step timeout-ms]
  (if timeout-ms
    (let [attempt* (future
                     (try
                       (execute-step-once executors state context task-id turn-id step)
                       (catch Exception e
                         (exception-result step e))))
          result   (deref attempt* (long timeout-ms) ::timeout)]
      (if (= ::timeout result)
        (do
          (future-cancel attempt*)
          (timeout-result step timeout-ms))
        result))
    (try
      (execute-step-once executors state context task-id turn-id step)
      (catch Exception e
        (exception-result step e)))))

(defn- retryable-result?
  [result]
  (and (= :failed (result-status result))
       (not (false? (:retryable? result)))))

(defn- with-attempt-metadata
  [result attempt max-attempts timeout-ms]
  (cond-> (assoc result :attempt attempt)
    (or (> (long attempt) 1)
        (> (long max-attempts) 1))
    (assoc :attempts attempt
           :max-attempts max-attempts)

    timeout-ms
    (assoc :timeout-ms timeout-ms)))

(defn- record-step-retry-item!
  [turn-id step result attempt max-attempts delay-ms]
  (task-runtime/record-task-item!
   turn-id
   {:type :system-note
    :status :running
    :summary (str "Retrying task step "
                  (name (:id step))
                  " after attempt "
                  attempt
                  " failed")
    :data (cond-> {:kind "task-step-retry"
                   :step-id (name (:id step))
                   :step-kind (name (:kind step))
                   :attempt attempt
                   :max-attempts max-attempts
                   :delay-ms delay-ms
                   :status (name (result-status result))}
            (:error result) (assoc :error (:error result))
            (:timeout-ms result) (assoc :timeout-ms (:timeout-ms result)))}))

(defn- execute-step
  [executors state context task-id turn-id step]
  (let [timeout-ms* (step-timeout-ms state context step)
        policy      (retry-policy state context step)
        max-attempts (long (:max-attempts policy))]
    (loop [attempt 1]
      (let [raw-result (with-attempt-metadata
                         (execute-step-attempt executors
                                               state
                                               context
                                               task-id
                                               turn-id
                                               step
                                               timeout-ms*)
                         attempt
                         max-attempts
                         timeout-ms*)
            result     (if (= :paused (result-status raw-result))
                         (paused-result raw-result)
                         raw-result)]
        (if (and (< (long attempt) max-attempts)
                 (retryable-result? result))
          (let [delay-ms (retry-delay-ms policy attempt)]
            (record-step-retry-item! turn-id step result attempt max-attempts delay-ms)
            (when (pos? (long delay-ms))
              (Thread/sleep (long delay-ms)))
            (recur (inc attempt)))
          result)))))

(defn create-task!
  "Create a durable task from a declarative task spec without starting the LLM execution loop."
  [spec & {:keys [session-id state title summary]}]
  (let [contract (task-contract spec)
        spec*    (:spec contract)
        title*   (or (nonblank-string title)
                     (nonblank-string (:goal spec*))
                     "Task spec")
        task-id  (db/create-task! (cond-> {:session-id session-id
                                           :channel :task-spec
                                           :type :task
                                           :state (or state :resumable)
                                           :title title*
                                           :summary (or (nonblank-string summary)
                                                        title*)
                                           :contract contract
                                           :meta {:trigger {:kind :api}
                                                  :execution {:mode :hybrid}
                                                  runtime-key (initial-task-spec-state spec*)}}
                                    session-id (assoc :session-role :origin)))]
    task-id))

(defn- close-turn!
  [turn-id state summary error]
  (task-runtime/sync-runtime-task-turn!
   turn-id
   (cond-> {:state state
            :summary summary}
     error (assoc :error error))))

(defn run-task!
  "Advance a declarative task spec until it completes, fails, pauses, or
   reaches `:max-steps`.

   Step executors are resolved in this order: built-ins, globally registered
   executors, then per-run `:executors`. Executor functions receive
   `{:task-id ... :turn-id ... :state ... :context ... :step ...
     :pause ... :resume-token ... :resume-input ...}` and return a result map."
  [task-id & {:keys [context executors max-steps operation]
              :or {context {}
                   max-steps default-max-steps}}]
  (if-let [task (db/get-task task-id)]
    (let [spec (or (task-spec task)
                   (throw (ex-info "Task does not have an executable task spec"
                                   {:type :task-spec/not-task-spec
                                    :task-id task-id})))
          executors* (resolve-executors executors)
          operation* (or operation
                         (if (= :ready (:status (task-spec-state task spec)))
                           :start
                           :resume))
          turn-id    (db/start-task-turn! task-id
                                          {:operation operation*
                                           :state :running
                                           :input (or (:message context)
                                                      (:goal spec)
                                                      (:title task))
                                           :summary "Running task spec"})
          state0     (assoc (task-spec-state task spec)
                            :spec spec
                            :status :running
                            :updated-at (now))]
      (sync-task-state! task-id
                        (dissoc state0 :spec)
                        {:state :running
                         :summary "Running task spec"
                         :stop-reason nil
                         :error nil
                         :finished-at nil})
      (loop [state state0
             step-count 0]
        (cond
          (>= step-count (long max-steps))
          (let [summary "Paused after reaching task step guardrail"
                pause   {:reason :max-steps
                         :waiting-for :resume}
                state*  (assoc state
                               :status :paused
                               :updated-at (now)
                               :pause-reason :max-steps
                               :waiting-for :resume
                               :pause pause)]
            (sync-task-state! task-id
                              (dissoc state* :spec)
                              {:state :resumable
                               :stop-reason :task-spec-paused
                               :summary summary})
            (close-turn! turn-id :completed summary nil)
            {:status :paused
             :task-id task-id
             :turn-id turn-id
             :summary summary
             :pause pause
             :state (dissoc state* :spec)})

          :else
          (if-let [step (next-step state spec)]
            (let [running-state (mark-step-running state step)
                  _             (persist-task-spec-state! task-id (dissoc running-state :spec))
                  result        (try
                                  (execute-step executors*
                                                running-state
                                                context
                                                task-id
                                                turn-id
                                                step)
                                  (catch Exception e
                                    {:status :failed
                                     :error (.getMessage e)
                                     :summary (str "Task step "
                                                   (name (:id step))
                                                   " failed")}))
                  state*        (mark-step-result running-state step result)
                  status        (result-status result)
                  summary       (step-summary step result)]
              (persist-task-spec-state! task-id (dissoc state* :spec))
              (record-task-step-item! turn-id step result)
              (case status
                :success
                (recur state* (inc step-count))

                :skipped
                (recur state* (inc step-count))

                :paused
                (let [pause   (or (:pause result)
                                  (normalize-pause-payload result))
                      state** (cond-> (assoc state*
                                             :status :paused
                                             :pause pause
                                             :pause-reason (:reason pause)
                                             :waiting-for (:waiting-for pause)
                                             :updated-at (now))
                                (:resume-token pause)
                                (assoc :resume-token (:resume-token pause))
                                (:deadline pause)
                                (assoc :deadline (:deadline pause))
                                (:deadline-at pause)
                                (assoc :deadline-at (:deadline-at pause)))]
                  (sync-task-state! task-id
                                    (dissoc state** :spec)
                                    {:state :resumable
                                     :stop-reason :task-spec-paused
                                     :summary summary})
                  (close-turn! turn-id :completed summary nil)
                  {:status :paused
                   :task-id task-id
                   :turn-id turn-id
                   :summary summary
                   :pause pause
                   :state (dissoc state** :spec)})

                :failed
                (let [state** (assoc state*
                                     :status :failed
                                     :updated-at (now))]
                  (sync-task-state! task-id
                                    (dissoc state** :spec)
                                    {:state :failed
                                     :stop-reason :error
                                     :summary summary
                                     :error (:error result)
                                     :finished-at (now)})
                  (close-turn! turn-id :failed summary (:error result))
                  {:status :failed
                   :task-id task-id
                   :turn-id turn-id
                   :summary summary
                   :error (:error result)
                   :state (dissoc state** :spec)})

                (throw (ex-info "Task spec executor returned invalid status"
                                {:type :task-spec/invalid-status
                                 :step-id (:id step)
                                 :status status}))))
            (if-let [blocked (dependency-blocked-result state spec)]
              (let [summary (:summary blocked)
                    state*  (assoc state
                                   :status :failed
                                   :blocked-dependencies (:blocked-dependencies blocked)
                                   :updated-at (now))]
                (sync-task-state! task-id
                                  (dissoc state* :spec)
                                  {:state :failed
                                   :stop-reason :error
                                   :summary summary
                                   :error (:error blocked)
                                   :finished-at (now)})
                (close-turn! turn-id :failed summary (:error blocked))
                {:status :failed
                 :task-id task-id
                 :turn-id turn-id
                 :summary summary
                 :error (:error blocked)
                 :blocked-dependencies (:blocked-dependencies blocked)
                 :state (dissoc state* :spec)})
              (let [summary "Task spec completed"
                    state*  (assoc state
                                   :status :completed
                                   :current-step-id nil
                                   :updated-at (now))]
                (sync-task-state! task-id
                                  (dissoc state* :spec)
                                  {:state :completed
                                   :summary summary
                                   :stop-reason nil
                                   :error nil
                                   :finished-at (now)})
                (close-turn! turn-id :completed summary nil)
                {:status :completed
                 :task-id task-id
                 :turn-id turn-id
                 :summary summary
                 :state (dissoc state* :spec)}))))))
    {:status :not-found
     :error "task not found"}))
