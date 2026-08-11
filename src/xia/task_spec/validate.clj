(ns xia.task-spec.validate
  "Validation and normalization for declarative task specs."
  (:require [clojure.string :as str]))

(def task-spec-version 1)

(def ^:private supported-task-spec-versions #{task-spec-version})
(def ^:private builtin-step-kinds
  #{:value :emit :condition :tool :input :approval :llm :branch :subtask
    :parallel :map :loop})
(def ^:private llm-step-modes #{:transform :judge :judgment :agent :interactive})
(def ^:private branch-modes #{:async :join})
(def ^:private schema-primitive-types
  #{:object :array :string :number :integer :boolean :null})
(def ^:private step-field-aliases
  {:collect_step :collect-step
   :depends_on :depends-on
   :index_as :index-as
   :max_iterations :max-iterations
   :max_output_tokens :max-output-tokens
   :max_steps :max-steps
   :max_tokens :max-tokens
   :output_schema :output-schema
   :output_step :output-step
   :provider_id :provider-id
   :result_step :result-step
   :structured_output :structured-output?
   :timeout_ms :timeout-ms
   :tool_id :tool-id})

(defn nonblank-string
  [value]
  (some-> value str str/trim not-empty))

(defn normalize-id
  [field value]
  (cond
    (keyword? value) value
    (symbol? value) (keyword (name value))
    (string? value) (some-> value str/trim not-empty keyword)
    :else (throw (ex-info (str "Task spec " (name field) " is required")
                          {:type :task-spec/invalid
                           :field field
                           :value value}))))

(defn normalize-kind
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

(defn- normalize-step
  [step]
  (when-not (map? step)
    (throw (ex-info "Task spec step must be a map"
                    {:type :task-spec/invalid
                     :step step})))
  (let [step (reduce-kv (fn [step* alias canonical]
                          (if (contains? step* alias)
                            (let [alias-value (get step* alias)
                                  step**      (dissoc step* alias)]
                              (if (contains? step** canonical)
                                step**
                                (assoc step** canonical alias-value)))
                            step*))
                        step
                        step-field-aliases)
        id   (normalize-id :step-id (:id step))
        kind (or (normalize-kind (:kind step)) :value)
        deps (normalize-dependency-values
              id
              (or (:depends-on step)
                  (:depends_on step)
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

(declare normalize-spec)

(defn- validation-issue
  [severity message data]
  (merge {:severity severity
          :message message}
         data))

(defn validation-error
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

(def expression-arities
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

(def planner-expression-operators
  (set (map name (keys expression-arities))))

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

(defn validation-errors-from-exception
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
      (vec
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
               (seq branches)))

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
                          :value branches})])))

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
       (when (contains? step :concurrency)
         (concat
          (expression-errors step-ids step
                             (step-path step :concurrency)
                             (:concurrency step))
          (literal-positive-integer-errors step
                                           :concurrency
                                           (step-path step :concurrency)
                                           (:concurrency step))))
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

(defn- normalize-tool-id
  [value]
  (normalize-kind value))

(defn- tool-missing?
  [tool-exists? tool-id]
  (when (and tool-id tool-exists?)
    (try
      (not (boolean (tool-exists? tool-id)))
      (catch Exception _
        false))))

(defn- normalized-spec-warnings
  [spec {:keys [registered-executors tool-exists?]}]
  (let [registered (or registered-executors {})]
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
                  (when (tool-missing? tool-exists? tool-id)
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
  ([spec]
   (validate-spec spec {}))
  ([spec opts]
   (let [opts (or opts {})]
     (try
       (let [spec* (normalize-spec spec)]
         {:valid? true
          :errors []
          :warnings (normalized-spec-warnings spec* opts)
          :spec spec*})
       (catch clojure.lang.ExceptionInfo e
         {:valid? false
          :errors (validation-errors-from-exception e [])
          :warnings []})))))
