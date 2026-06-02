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
        kind (or (normalize-kind (:kind step)) :value)]
    (assoc step
           :id id
           :kind kind)))

(defn normalize-spec
  "Normalize and validate a declarative task spec."
  [spec]
  (when-not (map? spec)
    (throw (ex-info "Task spec must be a map"
                    {:type :task-spec/invalid
                     :spec spec})))
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
    (assoc spec
           :kind :task
           :version (or (:version spec) 1)
           :steps steps)))

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
   :subtask subtask-executor})

(defn- resolve-executors
  [executors]
  (merge default-executors
         (registered-executors)
         executors))

(defn- due-step?
  [state step]
  (not (contains? terminal-step-statuses
                  (get-in state [:steps (:id step) :status]))))

(defn- next-step
  [state spec]
  (first (filter #(due-step? state %) (:steps spec))))

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
               :state (dissoc state* :spec)})))))
    {:status :not-found
     :error "task not found"}))
