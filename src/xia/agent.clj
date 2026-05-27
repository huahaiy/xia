(ns xia.agent
  "Agent loop — the core runtime that processes user messages.

   Loop: user message → update working memory → build context
         → LLM call (with tools) → tool calls? → response

   Skills = markdown instructions injected into the system prompt.
   Tools  = executable functions the LLM can call via function-calling."
  (:require [taoensso.timbre :as log]
            [clojure.string :as str]
            [xia.agent.branch :as agent-branch]
            [xia.agent.fact-review :as fact-review]
            [xia.agent.loop-guard :as loop-guard]
            [xia.agent.run-state :as run-state]
            [xia.agent.supervisor :as supervisor]
            [xia.agent.task-runtime :as task-runtime]
            [xia.agent.tools :as agent-tools]
            [xia.agent.turn-outcome :as turn-outcome]
            [xia.async :as async]
            [xia.autonomous :as autonomous]
            [xia.audit :as audit]
            [xia.context :as context]
            [xia.constraints :as constraints]
            [xia.db :as db]
            [xia.goal :as goal]
            [xia.limits :as limits]
            [xia.llm :as llm]
            [xia.plugin :as plugin]
            [xia.prompt :as prompt]
            [xia.retrieval-state :as retrieval-state]
            [xia.runtime-state :as runtime-state]
            [xia.schedule :as schedule]
            [xia.task-policy :as task-policy]
            [xia.tool :as tool]
            [xia.working-memory :as wm])
  (:import [java.util.concurrent Future TimeUnit TimeoutException]))

(defonce ^:private installed-runtime-atom (atom nil))
(def ^:dynamic *turn-limit-state* nil)

(declare clear-runtime!)

(defn- make-runtime
  []
  (run-state/make-runtime))

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

(declare truncate-summary status-agenda status-stack run-agent-iteration
         sanitized-tool-result
         cancel-futures!
         request-session-cancel!
         live-run-entry-for-session
         current-time-ms)

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

(defn- llm-status-preview-chars
  []
  (task-policy/llm-status-preview-chars))

(defn- llm-status-update-interval-ms
  []
  (task-policy/llm-status-update-interval-ms))

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

(defn- compose-request-limit-guard
  [outer-guard inner-guard]
  (cond
    (and outer-guard inner-guard)
    (fn [request]
      (outer-guard request)
      (inner-guard request))

    outer-guard outer-guard
    inner-guard inner-guard
    :else nil))

(defn- compose-request-limit-observer
  [outer-observer inner-observer]
  (cond
    (and outer-observer inner-observer)
    (fn [request]
      (outer-observer request)
      (inner-observer request))

    outer-observer outer-observer
    inner-observer inner-observer
    :else nil))

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
  ([] (install-runtime! (make-runtime)))
  ([runtime]
   (when-let [current (maybe-current-runtime)]
     (when-not (identical? current runtime)
       (clear-runtime!)))
   (fact-review/reset-runtime!)
   (reset! installed-runtime-atom runtime)
   runtime))

(defn clear-runtime!
  []
  (fact-review/reset-runtime!)
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

(defn- call-model
  [messages tools provider-id & {:keys [on-delta session-id]}]
  (cond
    (and provider-id (seq tools))
    (llm/chat-message messages :provider-id provider-id :tools tools :session-id session-id :on-delta on-delta)

    provider-id
    (llm/chat-message messages :provider-id provider-id :session-id session-id :on-delta on-delta)

    (seq tools)
    (llm/chat-message messages :tools tools :session-id session-id :on-delta on-delta)

    :else
    (llm/chat-message messages :session-id session-id :on-delta on-delta)))

(defn- current-time-ms
  []
  (long (System/currentTimeMillis)))

(defn- turn-budget-next-step
  [parsed autonomy-state]
  (or (some-> parsed :control :next-step str str/trim not-empty)
      (some-> parsed :intent :plan-step str str/trim not-empty)
      (some-> autonomy-state autonomous/current-frame :next-step str str/trim not-empty)))

(defn- llm-budget-summary
  [budget-status]
  (limits/budget-summary budget-status))

(defn- llm-budget-title
  [budget-status]
  (str (case (:scope budget-status)
         :task "Task"
         :turn "Turn"
         :session "Session"
         :schedule "Schedule"
         :schedule-run "Schedule run"
         :org "Organization"
         "Usage")
       " limit exhausted: "))

(defn- llm-budget-note
  [budget-status parsed autonomy-state & {:keys [before-tools?]}]
  (case (:scope budget-status)
    :task
    (str "Note: I paused this task after reaching the "
         (llm-budget-summary budget-status)
         "."
         (when before-tools?
           " I did not execute the next requested tool step.")
         (when-let [next-step (turn-budget-next-step parsed autonomy-state)]
           (str " Suggested next step: " next-step)))

    :turn
    (str "Note: I stopped this turn after reaching the "
         (llm-budget-summary budget-status)
         "."
         (when before-tools?
           " I did not execute the next requested tool step.")
         (when-let [next-step (turn-budget-next-step parsed autonomy-state)]
           (str " Suggested next step: " next-step))
         " Reply to continue from the current agenda.")

    (str "Note: I stopped this turn after reaching the "
         (llm-budget-summary budget-status)
         "."
         (when before-tools?
           " I did not execute the next requested tool step.")
         (when-let [next-step (turn-budget-next-step parsed autonomy-state)]
           (str " Suggested next step: " next-step)))))

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

(defn- intent-status-fields
  [intent]
  {:intent-focus (some-> intent :focus)
   :intent-agenda-item (some-> intent :agenda-item)
   :intent-plan-step (some-> intent :plan-step)
   :intent-why (some-> intent :why)
   :intent-tool-name (some-> intent :tool-name)
   :intent-tool-args-summary (some-> intent :tool-args-summary)})

(defn- emit-status!
  [message & {:as extra}]
  (prompt/status! (merge {:state :running
                          :message message}
                         extra)))

(defn- report-status!
  [message & {:as extra}]
  (apply emit-status! message (mapcat identity extra)))

(def ^:private fact-utility-review-debounce-ms 2000)

(defn- llm-preview-text
  [content]
  (let [text (some-> content str str/trim)]
    (when (and (seq text)
               (not (str/includes? text (autonomous/intent-marker-text)))
               (not (str/includes? text (autonomous/control-marker-text))))
      (truncate-summary text (llm-status-preview-chars)))))

(defn- make-llm-progress-reporter
  [round emit-event!]
  (let [last-report-ms (volatile! 0)]
    (fn [{:keys [content]}]
      (when-let [preview (llm-preview-text content)]
        (let [now-ms (long (System/currentTimeMillis))
              last-ms (long @last-report-ms)
              interval-ms (long (llm-status-update-interval-ms))
              should-report (or (zero? last-ms)
                                (>= (- now-ms last-ms) interval-ms))]
          (when should-report
            (vreset! last-report-ms now-ms)
            (emit-event! {:phase :llm
                          :message "Calling model"
                          :round round
                          :partial-content preview})))))))

(defn schedule-fact-utility-review!
  [session-id fact-eids user-message assistant-response & {:keys [explicit-fact-eids]}]
  (apply fact-review/schedule-fact-utility-review! session-id
         fact-eids
         user-message
         assistant-response
         (cond-> [:debounce-ms fact-utility-review-debounce-ms]
           (seq explicit-fact-eids)
           (into [:explicit-fact-eids explicit-fact-eids]))))

(defn- best-effort-update-working-memory!
  [session-id user-message channel opts]
  (when-let [message (some-> user-message str str/trim not-empty)]
    (try
      (wm/update-wm! message session-id channel opts)
      (catch Exception e
        (log/warn e "Working memory update failed; continuing without refreshed WM"
                  {:session-id session-id
                   :channel channel})
        nil))))

(defn- best-effort-refresh-working-memory!
  [session-id user-message channel opts]
  (when-let [message (some-> user-message str str/trim not-empty)]
    (try
      (wm/refresh-wm! message session-id channel opts)
      (catch Exception e
        (log/warn e "Working memory refresh failed; continuing without refreshed WM"
                  {:session-id session-id
                   :channel channel})
        nil))))

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

(defn- tool-result-audit-data
  [tool-result]
  (agent-tools/tool-result-audit-data tool-result))

(defn- truncate-summary
  [value max-len]
  (agent-tools/truncate-summary value max-len))

(defn- tool-call-names
  [tool-calls]
  (agent-tools/tool-call-names tool-calls))

(defn- response-provenance
  [response]
  (agent-tools/response-provenance response))

(defn- tool-call-summary
  [tool-calls]
  (agent-tools/tool-call-summary tool-calls))

(defn- tool-round-signature
  [tool-calls tool-results]
  (agent-tools/tool-round-signature tool-calls tool-results))

(defn- sanitized-tool-result
  [result]
  (agent-tools/sanitized-tool-result result))

(defn- task-runtime-deps
  []
  {:truncate-summary truncate-summary
   :sanitized-tool-result sanitized-tool-result})

(defn- multimodal-follow-up-messages
  [result context]
  (agent-tools/multimodal-follow-up-messages result context))

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

(defn- inject-transient-messages
  [messages transient-messages]
  (let [transient* (->> transient-messages
                        (filter map?)
                        vec)
        system-transient (->> transient*
                              (filter #(= "system" (:role %)))
                              vec)
        other-transient (->> transient*
                             (remove #(= "system" (:role %)))
                             vec)
        join-content (fn [parts]
                       (->> parts
                            (keep :content)
                            (map str)
                            (map str/trim)
                            (remove str/blank?)
                            (str/join "\n\n")))]
    (cond
      (empty? transient*)
      messages

      (and (seq system-transient)
           (seq messages)
           (= "system" (:role (first messages))))
      (let [merged-system (assoc (first messages)
                                 :content
                                 (join-content (into [(first messages)]
                                                     system-transient)))]
        (into [merged-system]
              (concat other-transient (rest messages))))

      (seq system-transient)
      (into [{:role "system"
              :content (join-content system-transient)}]
            (concat other-transient messages))

      (empty? messages)
      other-transient

      :else
      (into [(first messages)]
            (concat other-transient (rest messages))))))

(defn- execute-tool-calls
  "Execute tool calls from the LLM response, return tool result messages."
  [tool-calls context]
  (agent-tools/execute-tool-calls (tool-deps) tool-calls context))

(defn- response-content
  [response]
  (agent-tools/response-content response))

(defn- autonomous-iteration-messages
  [autonomy-state iteration max-iterations & {:keys [incoming-message]}]
  [(autonomous/controller-system-message)
   (autonomous/controller-state-message
    {:goal (autonomous/root-goal autonomy-state)
     :iteration iteration
     :max-iterations max-iterations
     :stack (:stack autonomy-state)
     :incoming-message incoming-message})])

(defn- autonomous-iteration-summary
  [{:keys [assistant-text control]}]
  (or (:summary control)
      (some-> assistant-text str not-empty)
      "Completed an autonomous iteration."))

(defn- runtime-autonomy-state
  [session-id task-id]
  (task-runtime/runtime-autonomy-state session-id task-id))

(defn- ensure-runtime-task!
  [session-id channel user-message autonomy-state task-id runtime-op interrupting-turn-id
   & {:keys [turn-input]}]
  (apply task-runtime/ensure-runtime-task!
         (task-runtime-deps)
         session-id
         channel
         user-message
         autonomy-state
         task-id
         runtime-op
         interrupting-turn-id
         (cond-> []
           turn-input (conj :turn-input turn-input))))

(defn- record-task-message-item!
  [task-turn-id item-type role text & {:keys [message-id llm-call-id data status]}]
  (task-runtime/record-task-message-item! (task-runtime-deps)
                                          task-turn-id
                                          item-type
                                          role
                                          text
                                          :message-id message-id
                                          :llm-call-id llm-call-id
                                          :data data
                                          :status status))

(defn- record-task-tool-call-items!
  [task-turn-id assistant-message-id llm-call-id tool-calls]
  (task-runtime/record-task-tool-call-items! task-turn-id
                                             assistant-message-id
                                             llm-call-id
                                             tool-calls))

(defn- sync-runtime-task!
  [task-id attrs]
  (task-runtime/sync-runtime-task! task-id attrs))

(defn- sync-runtime-task-turn!
  [task-turn-id attrs]
  (task-runtime/sync-runtime-task-turn! task-turn-id attrs))

(defn- task-runtime-callbacks
  [runtime-task]
  (task-runtime/task-runtime-callbacks (task-runtime-deps) runtime-task))

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

(defn- wm-query-signature
  [message]
  (loop-guard/wm-query-signature message))

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

(defn- emit-intent-event!
  [emit-event! execution-context parsed-response]
  (when-let [intent (:intent parsed-response)]
    (emit-event! (merge {:phase :intent
                         :message (autonomous/intent-status-line intent)
                         :iteration (:iteration execution-context)
                         :round 0
                         :checkpoint {:phase :intent
                                      :iteration (:iteration execution-context)
                                      :summary (or (:plan-step intent)
                                                   (:agenda-item intent)
                                                   (:focus intent)
                                                   "Prepared the next action.")
                                      :session-id (:session-id execution-context)
                                      :intent-focus (:focus intent)
                                      :intent-agenda-item (:agenda-item intent)
                                      :intent-plan-step (:plan-step intent)
                                      :intent-why (:why intent)
                                      :intent-tool-name (:tool-name intent)
                                      :intent-tool-args-summary (:tool-args-summary intent)}}
                        (intent-status-fields intent)))))

(defn- actionable-agenda-item
  [tip]
  (some (fn [{:keys [item status]}]
          (when (and (some-> item str str/blank? not)
                     (not (contains? #{:completed :skipped} status)))
            item))
        (:agenda tip)))

(defn- synthesize-tool-call-intent
  [session-id execution-context tool-calls]
  (let [autonomy-state (when session-id
                         (wm/autonomy-state session-id))
        tip            (some-> autonomy-state autonomous/current-frame)
        tool-names     (tool-call-names tool-calls)
        tool-name      (when (seq tool-names)
                         (str/join ", " tool-names))
        tool-count     (count tool-calls)
        plan-step      (cond
                         (= 1 tool-count)
                         (str "Call " (or (first tool-names) "the requested tool"))

                         (pos? tool-count)
                         (str "Call " tool-count " requested tools")

                         :else
                         "Call the requested tool")
        args-summary   (some-> (tool-call-summary tool-calls)
                               pr-str
                               (truncate-summary 240))]
    {:focus (or (some-> tip :title str str/trim not-empty)
                (some-> execution-context :user-message str str/trim not-empty)
                "Current task")
     :agenda-item (or (actionable-agenda-item tip)
                      (some-> tip :next-step str str/trim not-empty))
     :plan-step plan-step
     :why "The model requested tool execution for the current task."
     :tool-name tool-name
     :tool-args-summary args-summary}))

(defn- ensure-tool-call-intent
  [session-id execution-context round parsed-response tool-calls]
  (if (and (zero? round)
           (= :missing (:intent-status parsed-response))
           (seq tool-calls))
    (assoc parsed-response
           :intent-status :synthesized
           :intent (synthesize-tool-call-intent session-id
                                                execution-context
                                                tool-calls))
    parsed-response))

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

(defn- autonomous-protocol-ex
  [session-id execution-context round parsed-response message]
  (ex-info message
           {:type :autonomous-protocol-invalid
            :session-id session-id
            :channel (:channel execution-context)
            :iteration (:iteration execution-context)
            :round round
            :intent-status (:intent-status parsed-response)
            :control-status (:control-status parsed-response)}))

(defn- validate-tool-round-protocol!
  [session-id execution-context round parsed-response]
  (when (and (zero? round)
             (= :malformed (:intent-status parsed-response)))
    (throw (autonomous-protocol-ex
            session-id
            execution-context
            round
            parsed-response
            "First tool-calling response has a malformed ACTION_INTENT_JSON envelope")))
  (when (contains? #{:parsed :malformed} (:control-status parsed-response))
    (throw (autonomous-protocol-ex
            session-id
            execution-context
            round
            parsed-response
            "Tool-calling response must not include AUTONOMOUS_STATUS_JSON"))))

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

(defn- iteration-signature
  [autonomy-state control tool-activity]
  (loop-guard/iteration-signature autonomy-state control tool-activity))

(defn- update-iteration-loop-state
  [loop-state next-signature]
  (loop-guard/update-iteration-loop-state loop-state next-signature))

(defn- throw-if-identical-iteration-loop!
  [session-id channel iteration max-iterations loop-state autonomy-state control]
  (loop-guard/throw-if-identical-iteration-loop! session-id
                                                 channel
                                                 iteration
                                                 max-iterations
                                                 loop-state
                                                 autonomy-state
                                                 control))

(defn- merge-fact-eids
  [left right]
  (->> (concat (or left []) (or right []))
       distinct
       vec))

(defn- explicit-fact-ref->eid
  [used-fact-refs]
  (into {}
        (keep (fn [{:keys [eid ref]}]
                (when (and eid ref)
                  [(-> ref str str/trim str/upper-case) eid])))
        used-fact-refs))

(defn- explicit-used-fact-eids
  [used-fact-refs parsed-response]
  (let [fact-refs (explicit-fact-ref->eid used-fact-refs)]
    (->> (get-in parsed-response [:control :used-facts])
         (keep (fn [ref]
                 (get fact-refs
                      (some-> ref str str/trim str/upper-case))))
         distinct
         vec)))

(defn- final-assistant-text
  [parsed response]
  (or (some-> (:assistant-text parsed) str not-empty)
      (some-> parsed :control :summary str not-empty)
      (some-> response response-content str not-empty)
      ""))

(defn- append-assistant-note
  [text note]
  (let [text* (some-> text str str/trim)
        note* (some-> note str str/trim)]
    (cond
      (str/blank? note*) (or text* "")
      (str/blank? text*) note*
      :else (str text* "\n\n" note*))))

(defn- iteration-limit-note
  [max-iterations control]
  (str "Note: I stopped after reaching the autonomous iteration limit for this turn ("
       max-iterations
       ")."
       (when-let [next-step (some-> (:next-step control) str str/trim not-empty)]
         (str " Suggested next step: " next-step))
       " Reply to continue from the current agenda."))

(defn- clear-autonomy-state-on-terminal?
  [parsed]
  (let [control (:control parsed)]
    (and (= :parsed (:control-status parsed))
         (= :clear (:stack-action control)))))

(defn- validate-goal-complete
  [control autonomy-state]
  (let [claimed? (true? (:goal-complete? control))
        valid? (or (not claimed?)
                   (autonomous/structurally-complete? autonomy-state))]
    {:goal-complete-valid? valid?
     :control (if valid?
                control
                (assoc control :goal-complete? false))
     :autonomy-state (if valid?
                       autonomy-state
                       (autonomous/reconcile-invalid-goal-complete autonomy-state))}))

(defn- record-persistent-goal-judge!
  [session-id task-id attrs]
  (when (goal/current-goal session-id)
    (goal/judge-after-turn! session-id (assoc attrs :task-id task-id))))

(defn- persist-assistant-message!
  [session-id text execution-context response local-doc-ids artifact-ids]
  (let [{:keys [llm-call-id provider-id model workload]} (response-provenance response)
        assistant-message-id
        (db/add-message! session-id :assistant text
                         :llm-call-id llm-call-id
                         :provider-id provider-id
                         :model model
                         :workload workload
                         :local-doc-ids local-doc-ids
                         :artifact-ids artifact-ids)]
    (task-runtime/record-task-message-item! (task-runtime-deps)
                                            (:task-turn-id execution-context)
                                            :assistant-message
                                            :assistant
                                            text
                                            :message-id assistant-message-id
                                            :llm-call-id llm-call-id
                                            :data (cond-> {:provider-id provider-id
                                                           :model model
                                                           :workload workload}
                                                    (seq local-doc-ids) (assoc :local-doc-ids (vec local-doc-ids))
                                                    (seq artifact-ids) (assoc :artifact-ids (vec artifact-ids))))
    (audit/log! execution-context
                {:actor :assistant
                 :type :llm-response
                 :message-id assistant-message-id
                 :llm-call-id llm-call-id
                 :data {:provider-id (some-> provider-id name)
                        :model model
                        :workload (some-> workload name)
                        :tool-calls []}})))

(defn- persist-tool-result-message!
  [session-id execution-context llm-call-id provider-id model workload tool-result]
  (let [tool-name (:tool_name tool-result)
        tool-call-id (:tool_call_id tool-result)
        tool-message-id (db/add-message! session-id :tool
                                         nil
                                         :tool-result (:result tool-result)
                                         :tool-id tool-name
                                         :tool-call-id tool-call-id
                                         :tool-name tool-name
                                         :llm-call-id llm-call-id
                                         :provider-id provider-id
                                         :model model
                                         :workload workload)]
    (task-runtime/record-task-tool-result-item! (task-runtime-deps)
                                                (:task-turn-id execution-context)
                                                tool-message-id
                                                llm-call-id
                                                tool-result)
    (audit/log! execution-context
                {:actor :assistant
                 :type :tool-result
                 :message-id tool-message-id
                 :llm-call-id llm-call-id
                 :tool-id tool-name
                 :tool-call-id tool-call-id
                 :data (tool-result-audit-data tool-result)})
    tool-message-id))

(defn- run-agent-iteration
  [session-id channel resource-session-id local-doc-ids artifact-ids
   execution-context assistant-provider assistant-provider-id transient-messages
   working-memory-message update-working-memory? refresh-working-memory?
   max-tool-rounds worker-state system-prompt-cache-entry turn-budget-state]
  (let [emit-event! #(supervisor/emit-worker-event! worker-state %)
        retrieval-session-id (or resource-session-id session-id)]
    (when (or update-working-memory? refresh-working-memory?)
      (emit-event! {:phase :working-memory
                    :message (if update-working-memory?
                               "Updating working memory"
                               "Refreshing working memory")
                    :iteration (:iteration execution-context)})
      (if update-working-memory?
        (best-effort-update-working-memory! session-id
                                            working-memory-message
                                            channel
                                            {:resource-session-id resource-session-id})
        (best-effort-refresh-working-memory! session-id
                                             working-memory-message
                                             channel
                                             {:resource-session-id resource-session-id})))
    (throw-if-cancelled! session-id)
    (let [retrieval-version-before (retrieval-state/version retrieval-session-id)
          tools (tool/tool-definitions execution-context)
          {:keys [messages used-fact-eids used-fact-refs system-prompt-cache-entry]}
          (context/build-messages-data session-id
                                       {:provider assistant-provider
                                        :provider-id assistant-provider-id
                                        :system-prompt-cache-entry system-prompt-cache-entry
                                        :compaction-workload :history-compaction})
          messages (inject-transient-messages messages transient-messages)]
      (emit-event! {:phase :planning
                    :message "Planning next step"
                    :iteration (:iteration execution-context)
                    :round 0
                    :message-count (count messages)
                    :checkpoint {:phase :planning
                                 :iteration (:iteration execution-context)
                                 :round 0
                                 :summary "Working memory updated and context prepared."
                                 :message-count (count messages)
                                 :session-id session-id}})
      (loop [messages messages
             round 0
             tool-activity []]
        (throw-if-cancelled! session-id)
        (emit-event! {:phase :llm
                      :message (if (zero? round)
                                 "Calling model"
                                 "Calling model with tool results")
                      :iteration (:iteration execution-context)
                      :round round})
        (let [progress-reporter (make-llm-progress-reporter round emit-event!)
              response (call-model messages
                                   tools
                                   assistant-provider-id
                                   :session-id session-id
                                   :on-delta (fn [delta]
                                               (throw-if-cancelled! session-id)
                                               (progress-reporter delta)
                                               (throw-if-cancelled! session-id)))
              _ (throw-if-cancelled! session-id)
              _ (plugin/run-hooks! :post-llm
                                   (assoc execution-context
                                          :round round
                                          :response-content (response-content response)
                                          :response-provenance (response-provenance response)
                                          :tool-calls (if (map? response)
                                                        (vec (or (get response "tool_calls") []))
                                                        [])))
              tool-calls (if (map? response)
                           (vec (or (get response "tool_calls") []))
                           [])
              has-tools? (seq tool-calls)
              parsed-response (ensure-tool-call-intent
                               session-id
                               execution-context
                               round
                               (autonomous/parse-controller-response
                                (response-content response))
                               tool-calls)
              explicit-used-fact-eids (explicit-used-fact-eids used-fact-refs
                                                               parsed-response)
              assistant-content (or (:assistant-text parsed-response)
                                    (response-content response))
              budget-status (or (limits/budget-status (:task-budget-state execution-context))
                                (limits/budget-status turn-budget-state))
              _ (when (zero? round)
                  (emit-intent-event! emit-event!
                                      execution-context
                                      parsed-response))]
          (if has-tools?
            (if budget-status
              (do
                (throw-if-cancelled! session-id)
                (emit-event! {:phase :finalizing
                              :message "Stopping before the next tool step"
                              :iteration (:iteration execution-context)})
                {:response response
                 :parsed-response parsed-response
                 :used-fact-eids used-fact-eids
                 :explicit-used-fact-eids explicit-used-fact-eids
                 :tool-activity tool-activity
                 :refresh-needed? (retrieval-state/changed? retrieval-version-before
                                                            retrieval-session-id)
                 :budget-exhausted? true
                 :budget-status budget-status
                 :budget-before-tools? true
                 :system-prompt-cache-entry system-prompt-cache-entry})
              (do
              (validate-tool-round-protocol! session-id
                                             execution-context
                                             round
                                             parsed-response)
              (let [{:keys [allowed? reason rounds max-tool-rounds] :as decision}
                    (task-policy/tool-round-limit-decision round max-tool-rounds)]
                (when-not allowed?
                  (prompt/policy-decision! (assoc decision :decision-type :tool-round-policy))
                  (throw (ex-info reason
                                  {:type :tool-round-limit-exceeded
                                   :rounds rounds
                                   :max-tool-rounds max-tool-rounds}))))
              (let [{:keys [llm-call-id provider-id model workload]} (response-provenance response)
                    assistant-msg {:role "assistant"
                                   :content assistant-content
                                   :tool_calls tool-calls}
                    tool-count (count tool-calls)
                    _ (emit-event! {:phase :tool-plan
                                    :message (str "Model requested "
                                                  tool-count
                                                  " tool"
                                                  (when (not= 1 tool-count) "s"))
                                    :iteration (:iteration execution-context)
                                    :round round
                                    :tool-count tool-count})
                    tool-results (do
                                   (throw-if-cancelled! session-id)
                                   (execute-tool-calls tool-calls
                                                       (assoc execution-context
                                                              :llm-call-id llm-call-id
                                                              :provider-id provider-id
                                                              :model model
                                                              :workload workload
                                                              :round round
                                                              :tool-count tool-count
                                                              :worker-event! emit-event!)))
                    _ (throw-if-cancelled! session-id)
                    tool-history (mapv #(select-keys % [:role :tool_call_id :content])
                                       tool-results)
                    follow-up-messages (->> tool-results
                                            (mapcat :follow-up-messages)
                                            vec)
                    tool-activity* (conj tool-activity
                                         (tool-round-signature tool-calls tool-results))]
                (let [assistant-message-id
                      (db/add-message! session-id :assistant
                                       assistant-content
                                       :tool-calls tool-calls
                                       :llm-call-id llm-call-id
                                       :provider-id provider-id
                                       :model model
                                       :workload workload
                                       :local-doc-ids local-doc-ids
                                       :artifact-ids artifact-ids)]
                  (task-runtime/record-task-message-item! (task-runtime-deps)
                                                          (:task-turn-id execution-context)
                                                          :assistant-message
                                                          :assistant
                                                          assistant-content
                                                          :message-id assistant-message-id
                                                          :llm-call-id llm-call-id
                                                          :data {:provider-id provider-id
                                                                 :model model
                                                                 :workload workload
                                                                 :tool-calls (tool-call-summary tool-calls)})
                  (task-runtime/record-task-tool-call-items! (:task-turn-id execution-context)
                                                             assistant-message-id
                                                             llm-call-id
                                                             tool-calls)
                  (audit/log! execution-context
                              {:actor :assistant
                               :type :llm-response
                               :message-id assistant-message-id
                               :llm-call-id llm-call-id
                               :data {:provider-id (some-> provider-id name)
                                      :model model
                                      :workload (some-> workload name)
                                      :tool-calls (tool-call-summary tool-calls)}}))
                (doseq [tr tool-results]
                  (persist-tool-result-message! session-id
                                                execution-context
                                                llm-call-id
                                                provider-id
                                                model
                                                workload
                                                tr))
                (emit-event! {:phase :tool
                              :message (or (truncate-summary assistant-content 240)
                                           (str "Completed tool round with "
                                                tool-count
                                                " tool call"
                                                (when (not= 1 tool-count) "s")
                                                "."))
                              :iteration (:iteration execution-context)
                              :round round
                              :tool-count tool-count
                              :tool-ids (tool-call-names tool-calls)
                              :checkpoint {:phase :tool
                                           :iteration (:iteration execution-context)
                                           :round round
                                           :tool-count tool-count
                                           :tool-ids (tool-call-names tool-calls)
                                           :summary (or (truncate-summary assistant-content 240)
                                                        (str "Completed tool round with "
                                                             tool-count
                                                             " tool call"
                                                             (when (not= 1 tool-count) "s")
                                                             "."))}})
                (recur (-> messages
                           (conj assistant-msg)
                           (into tool-history)
                           (into follow-up-messages))
                       (inc round)
                       tool-activity*))))
            (do
              (throw-if-cancelled! session-id)
              (emit-event! {:phase :finalizing
                            :message "Preparing response"
                            :iteration (:iteration execution-context)})
              {:response response
               :parsed-response parsed-response
               :used-fact-eids used-fact-eids
               :explicit-used-fact-eids explicit-used-fact-eids
               :tool-activity tool-activity
               :refresh-needed? (retrieval-state/changed? retrieval-version-before
                                                          retrieval-session-id)
               :budget-exhausted? (boolean budget-status)
               :budget-status budget-status
               :system-prompt-cache-entry system-prompt-cache-entry})))))))

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
                                                          (task-runtime/task-runtime-callbacks (task-runtime-deps)
                                                                                               runtime-task))]
              (try
                (throw-if-cancelled! session-id)
                (validate-user-message! user-message)
                (throw-if-cancelled! session-id)
                (wm/ensure-wm! session-id)
                (let [active-goal* (goal/active-goal session-id)
                      task-goal-id (when task-id
                                     (some-> task-id
                                             db/get-task
                                             :meta
                                             :persistent-goal
                                             :id))
                      persistent-goal (when (and active-goal*
                                                 (or (nil? task-id)
                                                     (= (:id active-goal*) task-goal-id)))
                                        active-goal*)
                      autonomy-user-message (goal/autonomy-input persistent-goal user-message)
                      wm-user-message (goal/working-memory-input persistent-goal user-message)
                      initial-autonomy-state (autonomous/prepare-turn-state
                                              (task-runtime/runtime-autonomy-state session-id task-id)
                                              autonomy-user-message)
                      {:keys [task-id task-turn-id]} (task-runtime/ensure-runtime-task! (task-runtime-deps)
                                                                                         session-id
                                                                                         channel
                                                                                         autonomy-user-message
                                                                                         initial-autonomy-state
                                                                                         task-id
                                                                                         runtime-op
                                                                                         interrupting-turn-id
                                                                                         :turn-input user-message)
                      _ (goal/attach-task! session-id task-id)
                      task-run (register-task-run! session-id task-id task-turn-id)
                      _ (reset! runtime-task {:task-id task-id
                                              :task-turn-id task-turn-id
                                              :task-run-id (:task-run-id task-run)})
                      user-message-id (when persist-message?
                                        (db/add-message! session-id :user user-message
                                                         :local-doc-ids local-doc-ids
                                                         :artifact-ids artifact-ids))
                      _ (when user-message-id
                          (audit/log! request-context
                                      {:actor :user
                                       :type :user-message
                                       :message-id user-message-id
                                       :data {:local-doc-ids (vec (or local-doc-ids []))
                                              :artifact-ids (vec (or artifact-ids []))}}))
                      _ (task-runtime/record-task-message-item! (task-runtime-deps)
                                                                task-turn-id
                                                                :user-message
                                                                :user
                                                                user-message
                                                                :message-id user-message-id
                                                                :data (cond-> {}
                                                                        (seq local-doc-ids) (assoc :local-doc-ids (vec local-doc-ids))
                                                                        (seq artifact-ids) (assoc :artifact-ids (vec artifact-ids))))
                      operating-envelope (constraints/operating-envelope
                                           {:session-id session-id
                                            :task-id task-id})
                      pre-provider-limit-context (merge tool-context
                                                        request-context
                                                        {:session-id session-id
                                                         :task-id task-id
                                                         :task-turn-id task-turn-id
                                                         :channel channel
                                                         :persistent-goal-id (:id persistent-goal)
                                                         :resource-session-id resource-session-id
                                                         :operating-envelope operating-envelope})
                      limit-routing-decision (limits/routing-decision pre-provider-limit-context)
                      provider-selection-opts (-> (cond-> {:workload :assistant}
                                                    provider-id
                                                    (assoc :provider-id provider-id))
                                                  (limits/apply-routing-decision
                                                   limit-routing-decision))
                      {assistant-provider :provider
                       assistant-provider-id :provider-id}
                      (llm/resolve-provider-selection provider-selection-opts)
                      base-execution-context (merge tool-context
                                                    request-context
                                                    {:session-id session-id
                                                     :task-id task-id
                                                     :task-turn-id task-turn-id
                                                     :channel channel
                                                     :user-message user-message
                                                     :persistent-goal-id (:id persistent-goal)
                                                     :resource-session-id resource-session-id
                                                     :assistant-provider assistant-provider
                                                     :assistant-provider-id assistant-provider-id
                                                     :operating-envelope operating-envelope
                                                     :limit-routing-decision limit-routing-decision})
                      max-tool-rounds* (long (or max-tool-rounds
                                                 (configured-max-tool-rounds)))
                      max-iterations* (long (autonomous/max-iterations))
                      transient-messages* (vec (filter map? transient-messages))
                      initial-wm-message (or working-memory-message
                                             wm-user-message)
                      initial-wm-query-fingerprint (wm-query-signature initial-wm-message)
                      turn-budget-state (or *turn-limit-state*
                                            (atom (limits/new-turn-budget session-id
                                                                          channel)))
                      task-budget-state (task-runtime/task-limit-state task-id)
                      outer-budget-guard llm/*request-budget-guard*
                      outer-request-observer llm/*request-observer*]
                  (wm/set-autonomy-state! session-id initial-autonomy-state)
                  (binding [*turn-limit-state* turn-budget-state
                            llm/*request-budget-guard* (compose-request-limit-guard
                                                        outer-budget-guard
                                                        (fn [_request]
                                                          (handle-limit-policy-decision!
                                                           base-execution-context
                                                           (limits/policy-decision
                                                            base-execution-context))
                                                          (limits/throw-if-exhausted!
                                                           turn-budget-state)
                                                          (limits/throw-if-exhausted!
                                                           task-budget-state)))
                            llm/*request-observer* (compose-request-limit-observer
                                                    outer-request-observer
                                                    (fn [request]
                                                      (limits/record-turn-request!
                                                       turn-budget-state
                                                       request)
                                                      (task-runtime/record-task-limit-request!
                                                       task-id
                                                       task-budget-state
                                                       request)
                                                      (limits/log-usage!
                                                       base-execution-context
                                                       request)))]
                    (loop [iteration 1
                           fact-eids []
                           explicit-fact-eids []
                           loop-state nil
                           refresh-working-memory? false
                           system-prompt-cache-entry nil
                           wm-message initial-wm-message
                           wm-query-fingerprint initial-wm-query-fingerprint]
                      (throw-if-cancelled! session-id)
                      (let [autonomy-state (or (wm/autonomy-state session-id)
                                               initial-autonomy-state)
                            iteration-context (assoc base-execution-context
                                                     :task-budget-state task-budget-state
                                                     :iteration iteration
                                                     :max-iterations max-iterations*)
                            controller-messages (autonomous-iteration-messages
                                                 autonomy-state
                                                 iteration
                                                 max-iterations*
                                                 :incoming-message (when (= iteration 1)
                                                                     user-message))
                            transient-messages** (into transient-messages*
                                                       controller-messages)
                            _ (report-autonomy-status! :understanding
                                                       autonomy-state
                                                       iteration
                                                       max-iterations*)
                            update-working-memory? (= iteration 1)]
                        (save-schedule-checkpoint!
                         iteration-context
                         {:phase :understanding
                          :iteration iteration
                          :summary (if (= iteration 1)
                                     "Understanding the goal and preparing the first plan."
                                     "Resuming the autonomous loop with the updated plan.")
                          :session-id session-id})
                        (let [{:keys [response parsed-response used-fact-eids explicit-used-fact-eids tool-activity refresh-needed?
                                      system-prompt-cache-entry budget-exhausted? budget-status
                                      budget-before-tools?]}
                              (try
                                (run-supervised-agent-iteration session-id
                                                                channel
                                                                resource-session-id
                                                                local-doc-ids
                                                                artifact-ids
                                                                iteration-context
                                                                assistant-provider
                                                                assistant-provider-id
                                                                transient-messages**
                                                                wm-message
                                                                update-working-memory?
                                                                refresh-working-memory?
                                                                max-tool-rounds*
                                                                autonomy-state
                                                                max-iterations*
                                                                system-prompt-cache-entry
                                                                turn-budget-state)
                                (catch clojure.lang.ExceptionInfo e
                                  (if (limits/exhausted-exception? e)
                                    {:budget-exhausted? true
                                     :budget-status (select-keys (ex-data e)
                                                                 [:scope :kind :task-id :session-id :channel
                                                                  :llm-call-count :total-tokens
                                                                  :prompt-tokens :completion-tokens
                                                                  :elapsed-ms :llm-total-duration-ms
                                                                  :max-llm-calls :max-total-tokens
                                                                  :max-wall-clock-ms :max-llm-duration-ms])}
                                    (throw e))))
                              parsed parsed-response
                              control (:control parsed)
                              summary (autonomous-iteration-summary parsed)
                              fact-eids* (merge-fact-eids fact-eids used-fact-eids)
                              explicit-fact-eids* (merge-fact-eids explicit-fact-eids
                                                                   explicit-used-fact-eids)
                              updated-autonomy-state* (if control
                                                        (let [next-state (autonomous/apply-control autonomy-state
                                                                                                   control)]
                                                          (wm/set-autonomy-state! session-id next-state)
                                                          (or (wm/autonomy-state session-id)
                                                              next-state))
                                                        autonomy-state)
                              {:keys [goal-complete-valid? control autonomy-state]}
                              (if control
                                (validate-goal-complete control updated-autonomy-state*)
                                {:goal-complete-valid? true
                                 :control control
                                 :autonomy-state updated-autonomy-state*})
                              _ (when (and control (not goal-complete-valid?))
                                  (wm/set-autonomy-state! session-id autonomy-state))
                              updated-autonomy-state autonomy-state
                              updated-tip (autonomous/current-frame updated-autonomy-state)
                              _ (wm/snapshot! session-id)
                              text (final-assistant-text parsed response)]
                          (sync-runtime-task! task-id
                                              {:state :running
                                               :summary summary
                                               :autonomy-state updated-autonomy-state})
                          (report-autonomy-status! :observing
                                                   updated-autonomy-state
                                                   iteration
                                                   max-iterations*
                                                   :stack-action (some-> control :stack-action))
                          (save-schedule-checkpoint!
                           iteration-context
                           {:phase :observing
                            :iteration iteration
                            :summary summary
                            :session-id session-id
                            :control-status (:control-status parsed)
                            :goal-complete-valid? goal-complete-valid?
                            :status (some-> control :status)
                            :next-step (some-> control :next-step)
                            :progress-status (some-> updated-tip :progress-status)
                            :agenda (some-> updated-tip :agenda)
                            :stack (some-> updated-autonomy-state :stack)})
                          (cond
                            (and budget-exhausted?
                                 (or (nil? response)
                                     budget-before-tools?
                                     (= :continue (:status control))))
                            (let [final-text (append-assistant-note
                                              text
                                              (llm-budget-note budget-status
                                                               parsed
                                                               updated-autonomy-state
                                                               :before-tools? budget-before-tools?))]
                              (task-runtime/record-task-item! task-turn-id
                                                              {:type :system-note
                                                               :status :limit
                                                               :summary (str (llm-budget-title budget-status)
                                                                             (llm-budget-summary budget-status))
                                                               :data {:kind "budget-exhausted"
                                                                      :budget-scope (some-> (:scope budget-status) name)
                                                                      :budget-kind (some-> (:kind budget-status) name)
                                                                      :llm-call-count (:llm-call-count budget-status)
                                                                      :total-tokens (:total-tokens budget-status)
                                                                      :elapsed-ms (:elapsed-ms budget-status)
                                                                      :llm-total-duration-ms (:llm-total-duration-ms budget-status)
                                                                      :before-tools? (boolean budget-before-tools?)}})
                              (persist-assistant-message! session-id
                                                          final-text
                                                          iteration-context
                                                          response
                                                          local-doc-ids
                                                          artifact-ids)
                              (sync-runtime-task-turn! task-turn-id
                                                       {:state :completed
                                                        :summary (truncate-summary final-text 500)})
                              (sync-runtime-task! task-id
                                                  {:state :resumable
                                                   :summary (truncate-summary final-text 500)
                                                   :autonomy-state updated-autonomy-state})
                              (record-persistent-goal-judge!
                               session-id
                               task-id
                               {:task-state :resumable
                                :control control
                                :autonomy-state updated-autonomy-state
                                :guardrail :budget
                                :budget-status budget-status
                                :summary (truncate-summary final-text 500)})
                              (when-not (str/blank? text)
                                (launch-fact-utility-review-without-budget! session-id
                                                                            fact-eids*
                                                                            user-message
                                                                            text
                                                                            :explicit-fact-eids explicit-fact-eids*))
                              (save-schedule-checkpoint!
                               iteration-context
                               {:phase :complete
                                :iteration iteration
                                :summary (or (truncate-summary final-text 500)
                                             (str "Stopped after reaching the "
                                                  (llm-budget-summary budget-status)
                                                  "."))
                                :session-id session-id
                                :status :limit-exhausted
                                :budget-scope (:scope budget-status)
                                :budget-kind (:kind budget-status)
                                :llm-call-count (:llm-call-count budget-status)
                                :total-tokens (:total-tokens budget-status)
                                :elapsed-ms (:elapsed-ms budget-status)
                                :llm-total-duration-ms (:llm-total-duration-ms budget-status)
                                :next-step (turn-budget-next-step parsed updated-autonomy-state)
                                :progress-status (some-> updated-tip :progress-status)
                                :agenda (some-> updated-tip :agenda)
                                :stack (some-> updated-autonomy-state :stack)})
                              (prompt/status! (merge {:state :completed
                                                      :phase :complete
                                                      :message (str "Paused after reaching the "
                                                                    (llm-budget-summary budget-status))}
                                                     (autonomy-status-fields updated-autonomy-state
                                                                             iteration
                                                                             max-iterations*)))
                              final-text)

                            (or (nil? control)
                                (= :complete (:status control)))
                            (do
                              (persist-assistant-message! session-id
                                                          text
                                                          iteration-context
                                                          response
                                                          local-doc-ids
                                                          artifact-ids)
                              (sync-runtime-task-turn! task-turn-id
                                                       {:state :completed
                                                        :summary (truncate-summary text 500)})
                              (sync-runtime-task! task-id
                                                  {:state :completed
                                                   :summary (truncate-summary text 500)
                                                   :autonomy-state (when-not (clear-autonomy-state-on-terminal? parsed)
                                                                     updated-autonomy-state)
                                                   :finished-at (java.util.Date.)})
                              (record-persistent-goal-judge!
                               session-id
                               task-id
                               {:task-state :completed
                                :control control
                                :autonomy-state updated-autonomy-state
                                :summary (truncate-summary text 500)})
                              (when (clear-autonomy-state-on-terminal? parsed)
                                (wm/clear-autonomy-state! session-id)
                                (wm/snapshot! session-id))
                              (launch-fact-utility-review-without-budget! session-id
                                                                          fact-eids*
                                                                          user-message
                                                                          text
                                                                          :explicit-fact-eids explicit-fact-eids*)
                              (prompt/status! {:state :completed
                                               :phase :complete
                                               :message "Ready"})
                              text)

                            (>= iteration max-iterations*)
                            (let [final-text (append-assistant-note text
                                                                    (iteration-limit-note max-iterations*
                                                                                          control))]
                              (prompt/policy-decision!
                               (task-policy/autonomy-iteration-limit-policy
                                iteration
                                max-iterations*))
                              (persist-assistant-message! session-id
                                                          final-text
                                                          iteration-context
                                                          response
                                                          local-doc-ids
                                                          artifact-ids)
                              (sync-runtime-task-turn! task-turn-id
                                                       {:state :completed
                                                        :summary (truncate-summary final-text 500)})
                              (sync-runtime-task! task-id
                                                  {:state :resumable
                                                   :summary (truncate-summary final-text 500)
                                                   :autonomy-state updated-autonomy-state})
                              (record-persistent-goal-judge!
                               session-id
                               task-id
                               {:task-state :resumable
                                :control control
                                :autonomy-state updated-autonomy-state
                                :guardrail :iteration-limit
                                :summary (truncate-summary final-text 500)})
                              (launch-fact-utility-review-without-budget! session-id
                                                                          fact-eids*
                                                                          user-message
                                                                          text
                                                                          :explicit-fact-eids explicit-fact-eids*)
                              (save-schedule-checkpoint!
                               iteration-context
                               {:phase :complete
                                :iteration iteration
                                :summary (or (truncate-summary final-text 500)
                                             "Stopped after reaching the autonomous iteration limit for this turn.")
                                :session-id session-id
                                :status :iteration-limit
                                :next-step (:next-step control)
                                :progress-status (some-> updated-tip :progress-status)
                                :agenda (some-> updated-tip :agenda)
                                :stack (some-> updated-autonomy-state :stack)})
                              (prompt/status! (merge {:state :completed
                                                      :phase :complete
                                                      :message (str "Paused after reaching iteration limit ("
                                                                    max-iterations*
                                                                    ")")}
                                                     (autonomy-status-fields updated-autonomy-state
                                                                             iteration
                                                                             max-iterations*)))
                              final-text)

                            :else
                            (let [next-loop-state (update-iteration-loop-state
                                                   loop-state
                                                   (iteration-signature updated-autonomy-state
                                                                        control
                                                                        tool-activity))
                                  next-wm-message (or working-memory-message
                                                      (autonomous/retrieval-message updated-autonomy-state))
                                  next-wm-query-fingerprint (wm-query-signature next-wm-message)
                                  next-refresh-working-memory?
                                  (or refresh-needed?
                                      (and next-wm-query-fingerprint
                                           (not= wm-query-fingerprint
                                                 next-wm-query-fingerprint)))]
                              (persist-assistant-message! session-id
                                                          text
                                                          iteration-context
                                                          response
                                                          nil
                                                          nil)
                              (when-not (str/blank? text)
                                (prompt/assistant-message! {:text text
                                                            :iteration iteration
                                                            :max-iterations max-iterations*
                                                            :status :continue
                                                            :progress-status (some-> updated-tip :progress-status)
                                                            :agenda (some-> updated-tip :agenda)
                                                            :stack (some-> updated-autonomy-state :stack)}))
                              (throw-if-identical-iteration-loop! session-id
                                                                  channel
                                                                  iteration
                                                                  max-iterations*
                                                                  next-loop-state
                                                                  updated-autonomy-state
                                                                  control)
                              (report-autonomy-status! :updating
                                                       updated-autonomy-state
                                                       iteration
                                                       max-iterations*
                                                       :stack-action (:stack-action control))
                              (save-schedule-checkpoint!
                               iteration-context
                               {:phase :updating
                                :iteration iteration
                                :summary (or (:next-step control)
                                             "Updating the autonomous plan for the next iteration.")
                                :session-id session-id
                                :status :continue
                                :progress-status (some-> updated-tip :progress-status)
                                :agenda (some-> updated-tip :agenda)
                                :stack (some-> updated-autonomy-state :stack)})
                              (recur (inc iteration)
                                     fact-eids*
                                     explicit-fact-eids*
                                     next-loop-state
                                     next-refresh-working-memory?
                                     system-prompt-cache-entry
                                     next-wm-message
                                     next-wm-query-fingerprint))))))))
                (catch InterruptedException e
                  (turn-outcome/record-cancellation! session-id
                                                     @runtime-task
                                                     "request interrupted"
                                                     (some-> e .getMessage))
                  (request-session-cancel! session-id "request interrupted")
                  (if (stop-worker! session-id)
                    (let [cancel-ex (request-cancelled-ex session-id
                                                          (cancellation-reason session-id)
                                                          e)]
                      (turn-outcome/record-cancellation-status!
                       save-schedule-checkpoint!
                       request-context
                       session-id
                       (turn-outcome/cancellation-outcome
                        (:reason (ex-data cancel-ex))))
                      (throw cancel-ex))
                    (throw (ex-info "Agent supervisor could not stop the worker after request cancellation"
                                    {:type :agent-stop-timeout
                                     :session-id session-id
                                     :channel channel
                                     :grace-ms (task-policy/supervisor-restart-grace-ms)}
                                    e))))
                (catch clojure.lang.ExceptionInfo e
                  (let [data (ex-data e)]
                    (cond
                      (= :request-cancelled (:type data))
                      (do
                        (let [outcome (turn-outcome/record-cancellation!
                                       session-id
                                       @runtime-task
                                       (:reason data)
                                       (.getMessage e))]
                          (turn-outcome/record-cancellation-status!
                           save-schedule-checkpoint!
                           request-context
                           session-id
                           outcome))
                        (throw e))

                      (contains? #{:agent-stalled :autonomous-loop-stalled :agent-stop-timeout} (:type data))
                      (do
                        (turn-outcome/record-task-outcome!
                         session-id
                         @runtime-task
                         {:turn-state :failed
                          :task-state :failed
                          :stop-reason :stalled
                          :summary (.getMessage e)
                          :error (.getMessage e)
                          :guardrail :stalled})
                        (turn-outcome/record-stalled-status!
                         save-schedule-checkpoint!
                         request-context
                         session-id
                         data
                         (.getMessage e))
                        (throw e))

                      (= :task-restart-loop (:type data))
                      (do
                        (turn-outcome/record-task-outcome!
                         session-id
                         @runtime-task
                         {:turn-state :completed
                          :task-state :resumable
                          :stop-reason :restart-loop
                          :summary (.getMessage e)
                          :error (.getMessage e)
                          :guardrail :restart-loop})
                        (turn-outcome/record-restart-loop-status!
                         save-schedule-checkpoint!
                         request-context
                         session-id
                         data
                         (.getMessage e))
                        (throw e))

                      :else
                      (do
                        (turn-outcome/record-task-outcome!
                         session-id
                         @runtime-task
                         {:turn-state :failed
                          :task-state :failed
                          :stop-reason :error
                          :summary (.getMessage e)
                          :error (.getMessage e)
                          :guardrail :failed})
                        (turn-outcome/record-error-status!
                         save-schedule-checkpoint!
                         request-context
                         session-id
                         (.getMessage e))
                        (throw e)))))
                (catch Exception e
                  (if (session-cancelled? session-id)
                    (let [cancel-ex (request-cancelled-ex session-id
                                                          (cancellation-reason session-id)
                                                          e)]
                      (let [outcome (turn-outcome/record-cancellation!
                                     session-id
                                     @runtime-task
                                     (:reason (ex-data cancel-ex))
                                     (.getMessage cancel-ex))]
                        (turn-outcome/record-cancellation-status!
                         save-schedule-checkpoint!
                         request-context
                         session-id
                         outcome))
                      (throw cancel-ex))
                    (do
                      (turn-outcome/record-task-outcome!
                       session-id
                       @runtime-task
                       {:turn-state :failed
                        :task-state :failed
                        :stop-reason :error
                        :summary (.getMessage e)
                        :error (.getMessage e)
                        :guardrail :failed})
                      (turn-outcome/record-error-status!
                       save-schedule-checkpoint!
                       request-context
                       session-id
                       (.getMessage e))
                      (throw e))))
                (finally
                  (when-let [{:keys [task-id task-turn-id task-run-id]} @runtime-task]
                    (clear-task-run! session-id task-id task-turn-id task-run-id)))))))))))

(defn- task-control-deps
  []
  (merge (task-runtime-deps)
         {:cancel-session! cancel-session!
          :clear-session-turn-reservation! clear-session-turn-reservation!
          :process-message process-message
          :register-child-session! register-child-session!
          :reserve-next-session-turn! reserve-next-session-turn!
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
