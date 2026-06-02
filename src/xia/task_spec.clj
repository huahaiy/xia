(ns xia.task-spec
  "Declarative task specs on top of the durable task runtime."
  (:require [clojure.string :as str]
            [xia.agent.task-runtime :as task-runtime]
            [xia.async :as async]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.tool :as tool]))

(def ^:private task-spec-version 1)
(def ^:private runtime-key :task-spec)
(def ^:private default-max-steps 100)
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
   `{:task-id ... :turn-id ... :state ... :context ... :step ...}`

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

(defn- with-interaction-context
  [task-id turn-id context f]
  (let [task     (db/get-task task-id)
        context* (merge context
                        {:session-id (task-session-id task context)
                         :task-id task-id
                         :task-turn-id turn-id
                         :channel (task-channel task context)}
                        (interaction-hooks task-id turn-id))]
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
   :llm (missing-executor :llm)
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
    (cond-> (-> state
                (assoc :updated-at at)
                (update-step-state (:id step)
                                   merge
                                   (cond-> {:status status
                                            :summary summary
                                            :finished-at at
                                            :updated-at at}
                                     (contains? result :output)
                                     (assoc :output (:output result))
                                     (:error result)
                                     (assoc :error (:error result)))))
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
     (assoc-in [:data :error] (:error result)))))

(defn- skipped-result
  [step reason]
  {:status :skipped
   :summary (or reason
                (str "Skipped task step " (name (:id step))))})

(defn- execute-step
  [executors state context task-id turn-id step]
  (if (and (contains? step :when)
           (not (truthy? (eval-step-expr state context (:when step)))))
    (skipped-result step "Skipped because task step condition was false")
    (if-let [executor (get executors (:kind step))]
      (executor {:task-id task-id
                 :turn-id turn-id
                 :state state
                 :context context
                 :executors executors
                 :step step})
      {:status :paused
       :summary (str "Paused before unsupported task step kind "
                     (name (:kind step)))
       :error (str "unsupported task step kind: " (name (:kind step)))})))

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
   `{:task-id ... :turn-id ... :state ... :context ... :step ...}` and return
   a result map."
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
                state*  (assoc state
                               :status :paused
                               :updated-at (now)
                               :pause-reason :max-steps)]
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
                (let [state** (assoc state*
                                     :status :paused
                                     :pause-reason (or (:pause-reason result)
                                                       :unsupported-step)
                                     :updated-at (now))]
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
