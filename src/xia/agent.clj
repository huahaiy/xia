(ns xia.agent
  "Agent loop — the core runtime that processes user messages.

   Loop: user message → update working memory → build context
         → LLM call (with tools) → tool calls? → response

   Skills = markdown instructions injected into the system prompt.
  Tools  = executable functions the LLM can call via function-calling."
  (:require [taoensso.timbre :as log]
            [xia.agent.branch :as agent-branch]
            [xia.agent.fact-review :as fact-review]
            [xia.agent.iteration :as iteration]
            [xia.agent.run-state :as run-state]
            [xia.agent.supervisor :as supervisor]
            [xia.agent.task-runtime :as task-runtime]
            [xia.agent.tools :as agent-tools]
            [xia.agent.turn-loop :as turn-loop]
            [xia.agent.turn-outcome :as turn-outcome]
            [xia.agent.turn-setup :as turn-setup]
            [xia.async :as async]
            [xia.autonomous :as autonomous]
            [xia.context :as context]
            [xia.db :as db]
            [xia.limits :as limits]
            [xia.llm :as llm]
            [xia.prompt :as prompt]
            [xia.runtime-state :as runtime-state]
            [xia.schedule :as schedule]
            [xia.policy :as task-policy]
            [xia.task-spec :as task-spec]
            [xia.working-memory :as wm])
  (:import [java.util.concurrent Future TimeUnit TimeoutException]))

(defonce ^:private installed-runtime-atom (atom nil))
(def ^:dynamic *turn-limit-state* nil)

(declare clear-runtime!)

(defn make-runtime
  []
  (assoc (run-state/make-runtime)
         :fact-review-runtime (fact-review/make-runtime)))

(defn- maybe-current-runtime
  []
  @installed-runtime-atom)

(defn- current-runtime
  []
  (or (maybe-current-runtime)
      (throw (ex-info "Agent runtime is not installed"
                      {:component :xia/agent-runtime}))))

(def ^:private trace-context-keys
  [:request-id
   :correlation-id
   :parent-request-id
   :session-id
   :resource-session-id
   :parent-session-id
   :schedule-id
   :channel])

(declare truncate-summary status-agenda status-stack
         sanitized-tool-result
         cancel-futures!
         request-session-cancel!
         live-run-entry-for-session
         report-autonomy-status!)

;; build-messages is now in xia.context

(defn- reserve-next-session-turn!
  [session-id metadata]
  (run-state/reserve-next-session-turn! (current-runtime) session-id metadata))

(defn- clear-session-turn-reservation!
  [session-id token]
  (run-state/clear-session-turn-reservation! (current-runtime) session-id token))

(defn- with-session-turn-lock
  ([session-id f]
   (with-session-turn-lock session-id nil f))
  ([session-id reservation-token f]
   (run-state/with-session-turn-lock (current-runtime) session-id reservation-token f)))

(defn- max-user-message-chars
  []
  (task-policy/max-user-message-chars))

(defn- max-user-message-tokens
  []
  (task-policy/max-user-message-tokens))

(defn- configured-max-tool-rounds
  []
  (task-policy/max-tool-rounds))

(defn- validate-tool-round-call-count!
  [tool-calls]
  (let [{:keys [allowed? reason tool-count max-tool-calls-per-round] :as decision}
        (task-policy/tool-call-limit-decision (count tool-calls))]
    (when-not allowed?
      (prompt/policy-decision! (assoc decision :decision-type :tool-call-policy))
      (throw (ex-info reason
                      {:type :tool-call-limit-exceeded
                       :tool-count tool-count
                       :max-tool-calls-per-round max-tool-calls-per-round})))
    tool-count))

(defn- branch-error-stack-frames
  []
  (task-policy/branch-error-stack-frames))

(defn- max-branch-tasks
  []
  (task-policy/max-branch-tasks))

(defn- max-parallel-branches
  []
  (task-policy/max-parallel-branches))

(defn- max-branch-tool-rounds
  []
  (task-policy/max-branch-tool-rounds))

(defn- task-control-wait-ms
  []
  (task-policy/task-control-wait-ms))

(defn- new-request-id
  []
  (str (random-uuid)))

(defn- trace-context
  [m]
  (select-keys m trace-context-keys))

(defn- derive-request-context
  [session-id channel tool-context]
  (let [parent-context prompt/*interaction-context*
        request-id (or (:request-id tool-context)
                       (new-request-id))
        correlation-id (or (:correlation-id tool-context)
                           (:correlation-id parent-context)
                           (:request-id parent-context)
                           request-id)
        parent-request-id (or (:parent-request-id tool-context)
                              (:request-id parent-context))]
    (cond-> (merge (trace-context parent-context)
                   (trace-context tool-context)
                   {:channel channel
                    :session-id session-id
                    :request-id request-id
                    :correlation-id correlation-id})
      parent-request-id
      (assoc :parent-request-id parent-request-id))))

(defn- summarize-error-value
  [value]
  (cond
    (nil? value) nil
    (or (string? value) (keyword? value) (number? value) (boolean? value)
        (uuid? value))
    value
    :else
    (pr-str value)))

(defn- summarize-error-data
  [data]
  (when (map? data)
    (into {}
          (map (fn [[k v]]
                 [k (summarize-error-value v)]))
          (take 8 data))))

(defn- throwable-detail
  [^Throwable t]
  (let [{:keys [cause via]} (Throwable->map t)
        causes (into []
                     (map (fn [{:keys [type message data]}]
                            (cond-> {:class (str type)
                                     :message message}
                              (seq data)
                              (assoc :data (summarize-error-data data)))))
                     via)
        root-cause (last causes)]
    {:message (or (.getMessage t) cause (str t))
     :class (.getName (class t))
     :causes causes
     :root-cause root-cause
     :stack-trace (into []
                        (map str)
                        (take (branch-error-stack-frames) (.getStackTrace t)))}))

(defn- request-cancelled-ex
  ([session-id]
   (request-cancelled-ex session-id nil nil))
  ([session-id reason]
   (request-cancelled-ex session-id reason nil))
  ([session-id reason cause]
   (ex-info "Request cancelled"
            (cond-> {:type :request-cancelled
                     :status 499
                     :error "request cancelled"
                     :session-id session-id}
              reason
              (assoc :reason reason))
            cause)))

(defn cancel-session!
  "Request cancellation of the currently running agent turn for a session.
   Returns true when an active run was found and signalled."
  ([session-id]
   (cancel-session! session-id "cancel requested"))
  ([session-id reason]
   (request-session-cancel! session-id
                            reason
                            :interrupt-supervisor? true)))

(defn cancel-all-sessions!
  "Request cancellation for every currently running agent turn."
  ([]
   (cancel-all-sessions! "runtime stopping"))
  ([reason]
   (run-state/cancel-all-sessions! (current-runtime) reason cancel-session!)))

(defn runtime-activity
  "Return coarse agent runtime activity counts for control-plane inspection."
  []
  (run-state/runtime-activity (current-runtime)))

(defn install-runtime!
  [runtime]
  (when-let [current (maybe-current-runtime)]
    (when-not (identical? current runtime)
      (clear-runtime!)))
  (fact-review/install-runtime! (:fact-review-runtime runtime))
  (reset! installed-runtime-atom runtime)
  runtime)

(defn clear-runtime!
  []
  (fact-review/clear-runtime!)
  (when-let [runtime (maybe-current-runtime)]
    (run-state/clear-runtime-state! runtime)
    (reset! installed-runtime-atom nil))
  nil)

(defn session-cancelled?
  [session-id]
  (boolean
   (or (.isInterrupted (Thread/currentThread))
       (some-> (live-run-entry-for-session session-id) :cancelled?))))

(defn- cancellation-reason
  [session-id]
  (or (some-> (live-run-entry-for-session session-id) :cancel-reason)
      (when (.isInterrupted (Thread/currentThread))
        "thread interrupted")))

(defn- throw-if-cancelled!
  [session-id]
  (when (session-cancelled? session-id)
    (throw (request-cancelled-ex session-id
                                 (cancellation-reason session-id)))))

(defn- throw-if-runtime-stopping!
  [session-id]
  (when (= :stopping (runtime-state/phase))
    (throw (request-cancelled-ex session-id "runtime is stopping"))))

(defn- with-session-run
  [session-id f]
  (run-state/with-session-run (current-runtime) session-id f))

(defn- session-run-entry
  [session-id]
  (run-state/session-run-entry (current-runtime) session-id))

(defn- task-run-entry
  [task-id]
  (run-state/task-run-entry (current-runtime) task-id))

(defn- live-run-entry-for-session
  [session-id]
  (run-state/live-run-entry-for-session (current-runtime) session-id))

(defn- wait-for-session-idle!
  [session-id timeout-ms]
  (run-state/wait-for-session-idle! (current-runtime) session-id timeout-ms))

(defn- wait-for-task-idle!
  [task-id timeout-ms]
  (run-state/wait-for-task-idle! (current-runtime) task-id timeout-ms))

(defn- register-task-run!
  [session-id task-id task-turn-id]
  (run-state/register-task-run! (current-runtime) session-id task-id task-turn-id))

(defn- clear-task-run!
  [session-id task-id task-turn-id task-run-id]
  (run-state/clear-task-run! (current-runtime) session-id task-id task-turn-id task-run-id))

(defn- register-child-session!
  [parent-session-id child-session-id]
  (run-state/register-child-session! (current-runtime) parent-session-id child-session-id))

(defn- unregister-child-session!
  [parent-session-id child-session-id]
  (run-state/unregister-child-session! (current-runtime) parent-session-id child-session-id))

(defn- begin-worker-run!
  [session-id worker-token]
  (run-state/begin-worker-run! (current-runtime) session-id worker-token))

(defn- register-worker-thread!
  [session-id worker-token]
  (run-state/register-worker-thread! (current-runtime) session-id worker-token))

(defn- register-worker-future!
  [session-id worker-token worker]
  (run-state/register-worker-future! (current-runtime) session-id worker-token worker))

(defn- clear-worker-run!
  [session-id worker-token]
  (run-state/clear-worker-run! (current-runtime) session-id worker-token))

(defn- register-parallel-tool-futures!
  [session-id worker-token futures]
  (run-state/register-parallel-tool-futures! (current-runtime) session-id worker-token futures))

(defn- clear-parallel-tool-futures!
  [session-id worker-token futures]
  (run-state/clear-parallel-tool-futures! (current-runtime) session-id worker-token futures))

(defn- interrupt-worker-thread!
  [session-id]
  (run-state/interrupt-worker-thread! (current-runtime) session-id))

(defn- request-session-cancel!
  [session-id reason & {:keys [interrupt-supervisor?]
                        :or {interrupt-supervisor? false}}]
  (run-state/request-session-cancel! (current-runtime)
                                     session-id
                                     reason
                                     :interrupt-supervisor? interrupt-supervisor?))

(defn- cancel-futures!
  [futures]
  (run-state/cancel-futures! futures))

(def ^:private future-timeout-sentinel ::future-timeout)

(defn- await-future-result
  [^Future future remaining-ms]
  (.get future (long remaining-ms) TimeUnit/MILLISECONDS))

(defn- await-futures!
  [futures timeout-ms timeout-ex-fn]
  (let [deadline-ms (+ (long (System/currentTimeMillis)) (long timeout-ms))]
    (try
      (loop [idx 0
             results []]
        (if (= idx (count futures))
          results
          (let [remaining-ms (- deadline-ms (long (System/currentTimeMillis)))
                ^Future future (nth futures idx)
                result (if (pos? remaining-ms)
                         (try
                           (await-future-result future remaining-ms)
                           (catch TimeoutException _
                             future-timeout-sentinel))
                         future-timeout-sentinel)]
            (if (= future-timeout-sentinel result)
              (do
                (cancel-futures! futures)
                (throw (timeout-ex-fn idx timeout-ms)))
              (recur (inc idx) (conj results result))))))
      (catch InterruptedException e
        (cancel-futures! futures)
        (.interrupt (Thread/currentThread))
        (throw e)))))

(defn- validate-user-message!
  [user-message]
  (let [message (or user-message "")
        char-count (long (count message))
        token-estimate (long (context/estimate-tokens message))
        {:keys [allowed? reason char-count max-chars token-estimate max-tokens] :as decision}
        (task-policy/user-message-size-decision char-count token-estimate)]
    (when-not allowed?
      (prompt/policy-decision! decision)
      (throw (ex-info reason
                      (cond-> {:type :user-message-too-large
                               :status 413
                               :error "user message too large"}
                        char-count (assoc :char-count char-count)
                        max-chars (assoc :max-chars max-chars)
                        token-estimate (assoc :token-estimate token-estimate)
                        max-tokens (assoc :max-tokens max-tokens)))))))

(defn- llm-budget-summary
  [budget-status]
  (limits/budget-summary budget-status))

(defn- handle-limit-policy-decision!
  [execution-context decision]
  (when decision
    (prompt/policy-decision! (limits/policy-decision-event decision))
    (case (:action decision)
      :warn
      nil

      :prefer-local
      nil

      :downgrade-model
      nil

      :require-approval
      (let [approved? (and (prompt/approval-available?)
                           (prompt/approve!
                            {:tool-id :xia.limits/policy
                             :tool-name "LLM usage limit"
                             :description (str "Continue after reaching the "
                                               (llm-budget-summary decision)
                                               ".")
                             :policy :limits
                             :reason (llm-budget-summary decision)
                             :arguments (select-keys decision
                                                     [:scope :state :kind :action
                                                      :used :limit])}))]
        (when-not approved?
          (throw (limits/policy-decision-ex
                  (assoc decision :approval-denied? true)))))

      :pause-schedule
      (do
        (when-let [schedule-id (:schedule-id execution-context)]
          (schedule/pause-schedule! schedule-id))
        (throw (limits/policy-decision-ex decision)))

      :deny
      (throw (limits/policy-decision-ex decision))

      nil)))

(defn- autonomy-status-fields
  [autonomy-state iteration max-iterations]
  (let [tip (autonomous/current-frame autonomy-state)
        stack (:stack autonomy-state)]
    {:iteration iteration
     :max-iterations max-iterations
     :current-focus (:title tip)
     :progress-status (some-> tip :progress-status name)
     :stack-depth (count stack)
     :agenda (status-agenda (:agenda tip))
     :stack (status-stack stack)}))

(defn- emit-status!
  [message & {:as extra}]
  (prompt/status! (merge {:state :running
                          :message message}
                         extra)))

(defn- report-status!
  [message & {:as extra}]
  (apply emit-status! message (mapcat identity extra)))

(def ^:private fact-utility-review-debounce-ms 2000)

(defn schedule-fact-utility-review!
  [session-id fact-eids user-message assistant-response & {:keys [explicit-fact-eids]}]
  (apply fact-review/schedule-fact-utility-review! session-id
         fact-eids
         user-message
         assistant-response
         (cond-> [:debounce-ms fact-utility-review-debounce-ms]
           (seq explicit-fact-eids)
           (into [:explicit-fact-eids explicit-fact-eids]))))

(defn- launch-fact-utility-review!
  [session-id fact-eids user-message assistant-response & {:keys [explicit-fact-eids]}]
  (try
    (if (seq explicit-fact-eids)
      (schedule-fact-utility-review! session-id
                                     fact-eids
                                     user-message
                                     assistant-response
                                     :explicit-fact-eids explicit-fact-eids)
      (schedule-fact-utility-review! session-id fact-eids user-message assistant-response))
    (catch Exception e
      (log/warn e "Failed to schedule fact utility review; continuing without it"
                {:fact-count (count fact-eids)})
      nil)))

(defn- launch-fact-utility-review-without-budget!
  [session-id fact-eids user-message assistant-response & {:keys [explicit-fact-eids]}]
  (binding [*turn-limit-state* nil
            llm/*request-budget-guard* nil
            llm/*request-observer* nil]
    (if (seq explicit-fact-eids)
      (launch-fact-utility-review! session-id
                                   fact-eids
                                   user-message
                                   assistant-response
                                   :explicit-fact-eids explicit-fact-eids)
      (launch-fact-utility-review! session-id
                                   fact-eids
                                   user-message
                                   assistant-response))))

(defn- tool-deps
  []
  {:await-futures! await-futures!
   :cancel-futures! cancel-futures!
   :clear-parallel-tool-futures! clear-parallel-tool-futures!
   :parallel-tool-timeout-ms task-policy/parallel-tool-timeout-ms
   :register-parallel-tool-futures! register-parallel-tool-futures!
   :throw-if-cancelled! throw-if-cancelled!
   :trace-context trace-context
   :validate-tool-round-call-count! validate-tool-round-call-count!})

(defn- truncate-summary
  [value max-len]
  (agent-tools/truncate-summary value max-len))

(defn- sanitized-tool-result
  [result]
  (agent-tools/sanitized-tool-result result))

(defn- task-runtime-deps
  []
  {:truncate-summary truncate-summary
   :sanitized-tool-result sanitized-tool-result})

(defn- save-schedule-checkpoint!
  [execution-context checkpoint]
  (let [checkpoint* (merge (trace-context execution-context)
                           checkpoint)]
    (task-runtime/record-task-item! (:task-turn-id execution-context)
                                    {:type :checkpoint
                                     :summary (or (:summary checkpoint*)
                                                  (some-> (:phase checkpoint*) name)
                                                  "Checkpoint")
                                     :data checkpoint*})
    (task-runtime/save-task-checkpoint! (:task-id execution-context)
                                        checkpoint*))
  (when-let [schedule-id (:schedule-id execution-context)]
    (try
      (schedule/save-task-checkpoint! schedule-id
                                      (merge (trace-context execution-context)
                                             checkpoint))
      (catch Exception e
        (log/warn e "Failed to persist schedule checkpoint"
                  (merge {:schedule-id schedule-id}
                         (trace-context execution-context)))))))

(defn- task-runtime-callbacks
  [runtime-task]
  (task-runtime/task-runtime-callbacks (task-runtime-deps) runtime-task))

(defn- turn-setup-deps
  []
  {:register-task-run! register-task-run!})

(defn- turn-completion-deps
  []
  {:autonomy-status-fields autonomy-status-fields
   :launch-fact-utility-review-without-budget! launch-fact-utility-review-without-budget!
   :report-autonomy-status! report-autonomy-status!
   :save-schedule-checkpoint! save-schedule-checkpoint!})

(defn- turn-observation-deps
  []
  {:report-autonomy-status! report-autonomy-status!
   :save-schedule-checkpoint! save-schedule-checkpoint!})

(defn- status-agenda
  [agenda]
  (->> agenda
       (keep (fn [{:keys [item status]}]
               (when item
                 {:item item
                  :status (some-> status name)})))
       vec
       not-empty))

(defn- status-stack
  [stack]
  (->> stack
       (keep (fn [{:keys [title progress-status next-step kind child-task-id]}]
               (when title
                 (cond-> {:title title
                          :progress_status (some-> progress-status name)
                          :next_step next-step}
                   kind (assoc :kind (name kind))
                   child-task-id (assoc :child_task_id (str child-task-id))))))
       vec
       not-empty))

(defn- report-autonomy-status!
  [phase autonomy-state iteration max-iterations & {:keys [stack-action]}]
  (apply emit-status!
         (autonomous/status-line phase
                                 autonomy-state
                                 iteration
                                 max-iterations
                                 :stack-action stack-action)
         (mapcat identity
                 (merge {:phase phase}
                        (autonomy-status-fields autonomy-state
                                                iteration
                                                max-iterations)))))

(defn- report-supervisor-status!
  [phase message autonomy-state iteration max-iterations & {:as extra}]
  (apply emit-status!
         message
         (mapcat identity
                 (merge {:phase phase}
                        (autonomy-status-fields autonomy-state
                                                iteration
                                                max-iterations)
                        extra))))

(defn- iteration-deps
  []
  {:throw-if-cancelled! throw-if-cancelled!
   :tool-deps (tool-deps)})

(defn- run-agent-iteration
  [session-id channel resource-session-id local-doc-ids artifact-ids
   execution-context assistant-provider assistant-provider-id transient-messages
   working-memory-message update-working-memory? refresh-working-memory?
   max-tool-rounds worker-state system-prompt-cache-entry turn-budget-state]
  (iteration/run-agent-iteration
   (iteration-deps)
   session-id
   channel
   resource-session-id
   local-doc-ids
   artifact-ids
   execution-context
   assistant-provider
   assistant-provider-id
   transient-messages
   working-memory-message
   update-working-memory?
   refresh-working-memory?
   max-tool-rounds
   worker-state
   system-prompt-cache-entry
   turn-budget-state))

(defn- supervisor-deps
  []
  {:begin-worker-run! begin-worker-run!
   :cancel-futures! cancel-futures!
   :cancellation-reason cancellation-reason
   :clear-worker-run! clear-worker-run!
   :interrupt-worker-thread! interrupt-worker-thread!
   :live-run-entry-for-session live-run-entry-for-session
   :register-worker-future! register-worker-future!
   :register-worker-thread! register-worker-thread!
   :report-supervisor-status! report-supervisor-status!
   :request-cancelled-ex request-cancelled-ex
   :request-session-cancel! request-session-cancel!
   :run-agent-iteration run-agent-iteration
   :save-schedule-checkpoint! save-schedule-checkpoint!
   :session-cancelled? session-cancelled?
   :truncate-summary truncate-summary})

(defn- stop-worker!
  ([session-id]
   (stop-worker! session-id nil))
  ([session-id worker]
   (supervisor/stop-worker! (supervisor-deps) session-id worker)))

(defn- turn-outcome-deps
  []
  {:cancellation-reason cancellation-reason
   :clear-task-run! clear-task-run!
   :request-cancelled-ex request-cancelled-ex
   :request-session-cancel! request-session-cancel!
   :save-schedule-checkpoint! save-schedule-checkpoint!
   :session-cancelled? session-cancelled?
   :stop-worker! stop-worker!
   :supervisor-restart-grace-ms task-policy/supervisor-restart-grace-ms})

(defn- turn-outcome-context
  [session-id channel request-context runtime-task]
  {:channel channel
   :request-context request-context
   :runtime-task @runtime-task
   :session-id session-id})

(defn- run-supervised-agent-iteration
  [session-id channel resource-session-id local-doc-ids artifact-ids
   execution-context assistant-provider assistant-provider-id transient-messages
   working-memory-message update-working-memory? refresh-working-memory?
   max-tool-rounds autonomy-state max-iterations system-prompt-cache-entry
   turn-budget-state]
  (supervisor/run-supervised-agent-iteration
   (supervisor-deps)
   session-id
   channel
   resource-session-id
   local-doc-ids
   artifact-ids
   execution-context
   assistant-provider
   assistant-provider-id
   transient-messages
   working-memory-message
   update-working-memory?
   refresh-working-memory?
   max-tool-rounds
   autonomy-state
   max-iterations
   system-prompt-cache-entry
   turn-budget-state))

(defn- turn-loop-deps
  []
  {:configured-max-tool-rounds configured-max-tool-rounds
   :handle-limit-policy-decision! handle-limit-policy-decision!
   :report-autonomy-status! report-autonomy-status!
   :run-supervised-agent-iteration run-supervised-agent-iteration
   :save-schedule-checkpoint! save-schedule-checkpoint!
   :throw-if-cancelled! throw-if-cancelled!
   :turn-completion-deps (turn-completion-deps)
   :turn-observation-deps (turn-observation-deps)})

(defn process-message
  "Process a user message in the given session. Returns the assistant's response.

   1. Updates working memory (retrieval pipeline)
   2. Builds context: system prompt (identity + WM context + skills) + history
   3. Calls the LLM with available tools (function-calling)
   4. If the LLM wants to use tools, executes them and loops
   5. Returns the final text response"
  [session-id user-message & {:keys [channel tool-context provider-id local-doc-ids artifact-ids
                                     max-tool-rounds resource-session-id
                                     persist-message? transient-messages
                                     working-memory-message task-id runtime-op
                                     interrupting-turn-id turn-reservation-token]
                              :or {channel :terminal
                                   tool-context {}
                                   persist-message? true}}]
  (runtime-state/throw-if-not-accepting-new-work!
   (or runtime-op
       (when task-id :task-turn)
       :session-message))
  (with-session-turn-lock
    session-id
    turn-reservation-token
    (fn []
      (with-session-run
        session-id
        (fn []
          (let [request-context (derive-request-context session-id channel tool-context)
                runtime-task (atom nil)]
            (binding [prompt/*interaction-context* (merge request-context
                                                          (task-runtime-callbacks runtime-task))]
              (try
                (throw-if-cancelled! session-id)
                (validate-user-message! user-message)
                (throw-if-cancelled! session-id)
                (wm/ensure-wm! session-id)
                (let [{:keys [assistant-provider assistant-provider-id
                              base-execution-context initial-autonomy-state
                              task-id task-turn-id wm-user-message]}
                      (turn-setup/prepare-turn!
                       (turn-setup-deps)
                       {:session-id session-id
                        :channel channel
                        :user-message user-message
                        :task-id task-id
                        :runtime-op runtime-op
                        :interrupting-turn-id interrupting-turn-id
                        :request-context request-context
                        :tool-context tool-context
                        :provider-id provider-id
                        :resource-session-id resource-session-id
                        :persist-message? persist-message?
                        :local-doc-ids local-doc-ids
                        :artifact-ids artifact-ids
                        :runtime-task runtime-task})
                      turn-budget-state (turn-loop/ensure-turn-budget-state
                                         *turn-limit-state*
                                         session-id
                                         channel)]
                  (binding [*turn-limit-state* turn-budget-state]
                    (turn-loop/run-turn-loop!
                     (turn-loop-deps)
                     {:session-id session-id
                      :channel channel
                      :resource-session-id resource-session-id
                      :local-doc-ids local-doc-ids
                      :artifact-ids artifact-ids
                      :user-message user-message
                      :working-memory-message working-memory-message
                      :task-id task-id
                      :task-turn-id task-turn-id
                      :assistant-provider assistant-provider
                      :assistant-provider-id assistant-provider-id
                      :base-execution-context base-execution-context
                      :initial-autonomy-state initial-autonomy-state
                      :wm-user-message wm-user-message
                      :max-tool-rounds max-tool-rounds
                      :transient-messages transient-messages
                      :turn-budget-state turn-budget-state})))
                (catch InterruptedException e
                  (turn-outcome/handle-interrupted! (turn-outcome-deps)
                                                    (turn-outcome-context session-id
                                                                          channel
                                                                          request-context
                                                                          runtime-task)
                                                    e))
                (catch clojure.lang.ExceptionInfo e
                  (turn-outcome/handle-ex-info! (turn-outcome-deps)
                                                (turn-outcome-context session-id
                                                                      channel
                                                                      request-context
                                                                      runtime-task)
                                                e))
                (catch Exception e
                  (turn-outcome/handle-exception! (turn-outcome-deps)
                                                  (turn-outcome-context session-id
                                                                        channel
                                                                        request-context
                                                                        runtime-task)
                                                  e))
                (finally
                  (turn-outcome/clear-runtime-task-run! (turn-outcome-deps)
                                                        session-id
                                                        @runtime-task))))))))))

(defn- task-spec-llm-executor
  [{:keys [session-id channel message runtime-op provider-id resource-session-id
           max-tool-rounds tool-context turn-reservation-token]}]
  (fn [{:keys [task-id step context]}]
    (let [prompt* (or (:message context)
                      message
                      (:prompt step)
                      (:goal step)
                      "Continue the task.")]
      {:status :success
       :summary "LLM task step completed"
       :output (process-message session-id
                                prompt*
                                :channel channel
                                :task-id task-id
                                :runtime-op runtime-op
                                :provider-id provider-id
                                :resource-session-id resource-session-id
                                :max-tool-rounds max-tool-rounds
                                :tool-context (or tool-context {})
                                :persist-message? false
                                :turn-reservation-token turn-reservation-token)})))

(defn- run-task-spec!
  [task-id & {:keys [message channel runtime-op provider-id resource-session-id
                     max-tool-rounds tool-context turn-reservation-token operation]
              :or {tool-context {}}}]
  (if-let [task (db/get-task task-id)]
    (let [session-id (:session-id task)
          channel*   (or channel (:channel task) :terminal)
          runtime-op* (or runtime-op
                          (if (= :ready (get-in task [:meta :task-spec :status]))
                            :start
                            :resume))]
      (task-spec/run-task!
       task-id
       :operation operation
       :context {:message message}
       :executors {:llm
                   (task-spec-llm-executor
                    {:session-id session-id
                     :channel channel*
                     :message message
                     :runtime-op runtime-op*
                     :provider-id provider-id
                     :resource-session-id resource-session-id
                     :max-tool-rounds max-tool-rounds
                     :tool-context tool-context
                     :turn-reservation-token turn-reservation-token})}))
    {:status :not-found
     :error "task not found"}))

(defn- task-control-deps
  []
  (merge (task-runtime-deps)
         {:cancel-session! cancel-session!
          :clear-session-turn-reservation! clear-session-turn-reservation!
          :process-message process-message
          :register-child-session! register-child-session!
          :reserve-next-session-turn! reserve-next-session-turn!
          :run-task-spec! run-task-spec!
          :session-run-entry session-run-entry
          :task-run-entry task-run-entry
          :task-control-wait-ms task-control-wait-ms
          :unregister-child-session! unregister-child-session!
          :wait-for-task-idle! wait-for-task-idle!
          :wait-for-session-idle! wait-for-session-idle!}))

(defn pause-task!
  [task-id]
  (task-runtime/pause-task! (task-control-deps) task-id))

(defn stop-task!
  [task-id]
  (task-runtime/stop-task! (task-control-deps) task-id))

(defn resume-task!
  [task-id & {:keys [message]}]
  (task-runtime/resume-task! (task-control-deps) task-id :message message))

(defn interrupt-task!
  [task-id]
  (task-runtime/interrupt-task! (task-control-deps) task-id))

(defn steer-task!
  [task-id message]
  (task-runtime/steer-task! (task-control-deps) task-id message))

(defn fork-task!
  [task-id message]
  (task-runtime/fork-task! (task-control-deps) task-id message))

(defn recover-runtime-tasks!
  []
  (try
    (task-runtime/recover-interrupted-tasks!)
    (catch clojure.lang.ExceptionInfo e
      (if (= "Database not connected. Call (xia.db/connect!) first."
             (.getMessage e))
        []
        (throw e)))))

(defn- branch-deps
  []
  {:await-futures! await-futures!
   :branch-task-timeout-ms task-policy/branch-task-timeout-ms
   :max-branch-tasks max-branch-tasks
   :max-branch-tool-rounds max-branch-tool-rounds
   :max-parallel-branches max-parallel-branches
   :new-request-id new-request-id
   :process-message process-message
   :register-child-session! register-child-session!
   :report-status! report-status!
   :run-task-spec! run-task-spec!
   :throw-if-cancelled! throw-if-cancelled!
   :throw-if-runtime-stopping! throw-if-runtime-stopping!
   :throwable-detail throwable-detail
   :trace-context trace-context
   :unregister-child-session! unregister-child-session!})

(defn- run-branch-task*
  [parent-session-id branch-task opts]
  (agent-branch/run-branch-task* (branch-deps) parent-session-id branch-task opts))

(defn run-branch-tasks
  "Run independent branch tasks in separate worker sessions with isolated
   working memory and shared long-term memory access. Returns structured
   branch results for the parent agent."
  [tasks & {:as opts}]
  (apply agent-branch/run-branch-tasks (branch-deps) tasks (mapcat identity opts)))
