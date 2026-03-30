(ns xia.agent
  "Agent loop — the core runtime that processes user messages.

   Loop: user message → update working memory → build context
         → LLM call (with tools) → tool calls? → response

   Skills = markdown instructions injected into the system prompt.
   Tools  = executable functions the LLM can call via function-calling."
  (:require [taoensso.timbre :as log]
            [charred.api :as json]
            [clojure.string :as str]
            [xia.autonomous :as autonomous]
            [xia.audit :as audit]
            [xia.config :as cfg]
            [xia.context :as context]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.prompt :as prompt]
            [xia.retrieval-state :as retrieval-state]
            [xia.runtime-state :as runtime-state]
            [xia.schedule :as schedule]
            [xia.ssrf :as ssrf]
            [xia.tool :as tool]
            [xia.working-memory :as wm])
  (:import [java.util.concurrent Future TimeUnit TimeoutException]
           [java.util.concurrent.locks ReentrantLock]))

(def ^:private default-max-tool-rounds 100)
(def ^:private default-max-tool-calls-per-round 12)
(def ^:private default-max-user-message-chars 32768)
(def ^:private default-max-user-message-tokens 8000)
(def ^:private default-max-branch-tasks 5)
(def ^:private default-max-parallel-branches 3)
(def ^:private default-max-branch-tool-rounds 5)
(def ^:private default-parallel-tool-timeout-ms 30000)
(def ^:private default-branch-task-timeout-ms 300000)
(def ^:private default-branch-error-stack-frames 12)
(def ^:private session-turn-lock-count 256)
(def ^:private default-llm-status-preview-chars 160)
(def ^:private default-llm-status-update-interval-ms 500)
(def ^:private default-supervisor-tick-ms 250)
(def ^:private default-supervisor-phase-timeout-ms 30000)
(def ^:private default-supervisor-llm-timeout-ms 120000)
(def ^:private default-supervisor-tool-timeout-ms 120000)
(def ^:private default-supervisor-max-identical-iterations 3)
(def ^:private default-supervisor-max-restarts 1)
(def ^:private default-supervisor-restart-backoff-ms 100)
(def ^:private default-supervisor-restart-grace-ms 1000)
(defonce ^:private session-turn-locks
  (vec (repeatedly session-turn-lock-count #(ReentrantLock.))))
(defonce ^:private active-session-runs (atom {}))

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
         cancel-futures!
         request-session-cancel!)

;; build-messages is now in xia.context

(defn- session-turn-lock
  [session-id]
  (when session-id
    (nth session-turn-locks
         (mod (bit-and Integer/MAX_VALUE (int (hash session-id)))
              session-turn-lock-count))))

(defn- with-session-turn-lock
  [session-id f]
  (if-let [^ReentrantLock lock (session-turn-lock session-id)]
    (if (.tryLock lock)
      (try
        (f)
        (finally
          (.unlock lock)))
      (throw (ex-info "Session is already processing another request"
                      {:type :session-busy
                       :status 409
                       :error "session is busy"
                       :session-id session-id})))
    (f)))

(defn- max-user-message-chars
  []
  (cfg/positive-long :agent/max-user-message-chars
                     default-max-user-message-chars))

(defn- max-user-message-tokens
  []
  (cfg/positive-long :agent/max-user-message-tokens
                     default-max-user-message-tokens))

(defn- configured-max-tool-rounds
  []
  (cfg/positive-long :agent/max-tool-rounds
                     default-max-tool-rounds))

(defn- configured-max-tool-calls-per-round
  []
  (cfg/positive-long :agent/max-tool-calls-per-round
                     default-max-tool-calls-per-round))

(defn- validate-tool-round-call-count!
  [tool-calls]
  (let [tool-count (count tool-calls)
        max-tool-calls-per-round (configured-max-tool-calls-per-round)]
    (when (> (long tool-count) (long max-tool-calls-per-round))
      (throw (ex-info (str "Too many tool calls in one round: "
                           tool-count
                           " (max "
                           max-tool-calls-per-round
                           ")")
                      {:type :tool-call-limit-exceeded
                       :tool-count tool-count
                       :max-tool-calls-per-round max-tool-calls-per-round})))
    tool-count))

(defn- branch-error-stack-frames
  []
  (cfg/positive-long :agent/branch-error-stack-frames
                     default-branch-error-stack-frames))

(defn- max-branch-tasks
  []
  (cfg/positive-long :agent/max-branch-tasks
                     default-max-branch-tasks))

(defn- max-parallel-branches
  []
  (cfg/positive-long :agent/max-parallel-branches
                     default-max-parallel-branches))

(defn- max-branch-tool-rounds
  []
  (cfg/positive-long :agent/max-branch-tool-rounds
                     default-max-branch-tool-rounds))

(defn- parallel-tool-timeout-ms
  []
  (cfg/positive-long :agent/parallel-tool-timeout-ms
                     default-parallel-tool-timeout-ms))

(defn- branch-task-timeout-ms
  []
  (cfg/positive-long :agent/branch-task-timeout-ms
                     default-branch-task-timeout-ms))

(defn- llm-status-preview-chars
  []
  (cfg/positive-long :agent/llm-status-preview-chars
                     default-llm-status-preview-chars))

(defn- llm-status-update-interval-ms
  []
  (cfg/positive-long :agent/llm-status-update-interval-ms
                     default-llm-status-update-interval-ms))

(defn- supervisor-tick-ms
  []
  (cfg/positive-long :agent/supervisor-tick-ms
                     default-supervisor-tick-ms))

(defn- supervisor-phase-timeout-ms
  []
  (cfg/positive-long :agent/supervisor-phase-timeout-ms
                     default-supervisor-phase-timeout-ms))

(defn- supervisor-llm-timeout-ms
  []
  (cfg/positive-long :agent/supervisor-llm-timeout-ms
                     default-supervisor-llm-timeout-ms))

(defn- supervisor-tool-timeout-ms
  []
  (cfg/positive-long :agent/supervisor-tool-timeout-ms
                     default-supervisor-tool-timeout-ms))

(defn- supervisor-max-identical-iterations
  []
  (cfg/positive-long :agent/supervisor-max-identical-iterations
                     default-supervisor-max-identical-iterations))

(defn- supervisor-max-restarts
  []
  (cfg/positive-long :agent/supervisor-max-restarts
                     default-supervisor-max-restarts))

(defn- supervisor-restart-backoff-ms
  []
  (cfg/positive-long :agent/supervisor-restart-backoff-ms
                     default-supervisor-restart-backoff-ms))

(defn- supervisor-restart-grace-ms
  []
  (cfg/positive-long :agent/supervisor-restart-grace-ms
                     default-supervisor-restart-grace-ms))

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
   (let [session-ids (keys @active-session-runs)]
     (doseq [session-id session-ids]
       (cancel-session! session-id reason))
     (count session-ids))))

(defn session-cancelled?
  [session-id]
  (boolean
   (or (.isInterrupted (Thread/currentThread))
       (some-> (get @active-session-runs session-id) :cancelled?))))

(defn- cancellation-reason
  [session-id]
  (or (some-> (get @active-session-runs session-id) :cancel-reason)
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
  (if session-id
    (let [run-id (Object.)]
      (swap! active-session-runs assoc session-id
             {:run-id run-id
              :supervisor-thread (Thread/currentThread)
              :worker-token nil
              :worker-thread nil
              :worker-future nil
              :parallel-tool-futures []
              :cancelled? false
              :cancel-reason nil})
      (try
        (f)
        (finally
          (swap! active-session-runs
                 (fn [runs]
                   (if (= run-id (get-in runs [session-id :run-id]))
                     (dissoc runs session-id)
                     runs))))))
    (f)))

(defn- session-run-entry
  [session-id]
  (when session-id
    (get @active-session-runs session-id)))

(defn- update-session-run-entry!
  [session-id f]
  (when session-id
    (swap! active-session-runs
           (fn [runs]
             (if-let [entry (get runs session-id)]
               (assoc runs session-id (f entry))
               runs)))))

(defn- begin-worker-run!
  [session-id worker-token]
  (update-session-run-entry! session-id
                             #(assoc % :worker-token worker-token
                                       :worker-thread nil
                                       :worker-future nil
                                       :parallel-tool-futures [])))

(defn- register-worker-thread!
  [session-id worker-token]
  (update-session-run-entry! session-id
                             (fn [entry]
                               (if (= worker-token (:worker-token entry))
                                 (assoc entry :worker-thread (Thread/currentThread))
                                 entry))))

(defn- clear-worker-thread!
  [session-id worker-token]
  (update-session-run-entry! session-id
                             (fn [entry]
                               (if (= worker-token (:worker-token entry))
                                 (assoc entry :worker-thread nil)
                                 entry))))

(defn- register-worker-future!
  [session-id worker-token worker]
  (update-session-run-entry! session-id
                             (fn [entry]
                               (if (= worker-token (:worker-token entry))
                                 (assoc entry :worker-future worker)
                                 entry))))

(defn- clear-worker-run!
  [session-id worker-token]
  (update-session-run-entry! session-id
                             (fn [entry]
                               (if (= worker-token (:worker-token entry))
                                 (assoc entry
                                        :worker-token nil
                                        :worker-thread nil
                                        :worker-future nil
                                        :parallel-tool-futures [])
                                 entry))))

(defn- register-parallel-tool-futures!
  [session-id worker-token futures]
  (update-session-run-entry! session-id
                             (fn [entry]
                               (if (= worker-token (:worker-token entry))
                                 (update entry
                                         :parallel-tool-futures
                                         (fn [existing]
                                           (->> (concat (or existing []) futures)
                                                distinct
                                                vec)))
                                 entry))))

(defn- clear-parallel-tool-futures!
  [session-id worker-token futures]
  (update-session-run-entry! session-id
                             (fn [entry]
                               (if (= worker-token (:worker-token entry))
                                 (update entry
                                         :parallel-tool-futures
                                         (fn [existing]
                                           (let [to-clear (set futures)]
                                             (->> (or existing [])
                                                  (remove to-clear)
                                                  vec))))
                                 entry))))

(defn- interrupt-worker-thread!
  [session-id]
  (when-let [^Thread worker-thread (:worker-thread (session-run-entry session-id))]
    (when (not= (Thread/currentThread) worker-thread)
      (.interrupt worker-thread))
    true))

(defn- request-session-cancel!
  [session-id reason & {:keys [interrupt-supervisor?]
                        :or {interrupt-supervisor? false}}]
  (let [entry* (atom nil)]
    (when session-id
      (swap! active-session-runs
             (fn [runs]
               (if-let [entry (get runs session-id)]
                 (let [updated (assoc entry
                                      :cancelled? true
                                      :cancel-reason (or (:cancel-reason entry)
                                                         reason))]
                   (reset! entry* updated)
                   (assoc runs session-id updated))
                 runs)))
      (when-let [entry @entry*]
        (when (and interrupt-supervisor?
                   (not= (Thread/currentThread) ^Thread (:supervisor-thread entry)))
          (.interrupt ^Thread (:supervisor-thread entry)))
        (when (and (:worker-thread entry)
                   (not= (Thread/currentThread) ^Thread (:worker-thread entry)))
          (.interrupt ^Thread (:worker-thread entry)))
        (when-let [parallel-tool-futures (seq (:parallel-tool-futures entry))]
          (cancel-futures! parallel-tool-futures))
        (when-let [^Future worker-future (:worker-future entry)]
          (future-cancel worker-future))
        true))))

(defn- cancel-futures!
  [futures]
  (doseq [f futures]
    (future-cancel f)))

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
        max-chars (long (max-user-message-chars))
        max-tokens (long (max-user-message-tokens))]
    (cond
      (> char-count max-chars)
      (throw (ex-info (str "User message too large: "
                           char-count
                           " chars (max "
                           max-chars
                           ")")
                      {:type :user-message-too-large
                       :status 413
                       :error "user message too large"
                       :char-count char-count
                       :max-chars max-chars}))

      (> token-estimate max-tokens)
      (throw (ex-info (str "User message too large: ~"
                           token-estimate
                           " tokens (max "
                           max-tokens
                           ")")
                      {:type :user-message-too-large
                       :status 413
                       :error "user message too large"
                       :token-estimate token-estimate
                       :max-tokens max-tokens})))))

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
(def ^:private fact-utility-review-batch-size 20)
(def ^:private fact-utility-review-max-pending 120)
(defonce ^:private fact-utility-review-state (atom {}))

(defn- fact-utility-observations
  [fact-eids user-message assistant-response]
  (into []
        (map (fn [fact-eid]
               {:fact-eid fact-eid
                :user-message user-message
                :assistant-response assistant-response}))
        (distinct fact-eids)))

(declare run-fact-utility-review-worker!)

(defn- update-fact-utility-review-state!
  ([f]
   (update-fact-utility-review-state! fact-utility-review-state compare-and-set! f))
  ([state-atom cas-fn f]
   (loop []
     (let [before @state-atom
           [after result] (f before)]
       (if (cas-fn state-atom before after)
         result
         (recur))))))

(defn- enqueue-fact-utility-review-state
  [state session-id observations]
  (let [{:keys [pending] :as session-state}
        (get state session-id)
        pending* (->> (concat pending observations)
                      (take-last fact-utility-review-max-pending)
                      vec)]
    (if-let [existing-token (:worker-token session-state)]
      [(assoc state session-id (assoc session-state :pending pending*)) nil]
      (let [token (random-uuid)]
        [(assoc state session-id {:pending pending*
                                  :worker-token token})
         token]))))

(defn- claim-fact-utility-review-batch-state
  [state session-id worker-token]
  (let [{:keys [pending] :as session-state}
        (get state session-id)]
    (if (= worker-token (:worker-token session-state))
      (let [batch*     (vec (take fact-utility-review-batch-size pending))
            remaining* (vec (drop fact-utility-review-batch-size pending))]
        [(assoc state session-id (assoc session-state :pending remaining*))
         batch*])
      [state nil])))

(defn- finish-fact-utility-review-worker-state
  [state session-id worker-token]
  (let [{:keys [pending] :as session-state} (get state session-id)]
    (cond
      (not= worker-token (:worker-token session-state))
      [state nil]

      (seq pending)
      (let [next-token (random-uuid)]
        [(assoc state session-id {:pending pending
                                  :worker-token next-token})
         next-token])

      :else
      [(dissoc state session-id) nil])))

(defn- enqueue-fact-utility-review!
  [session-id fact-eids user-message assistant-response]
  (let [observations (fact-utility-observations fact-eids user-message assistant-response)]
    (when (seq observations)
      (when-let [token (update-fact-utility-review-state!
                        #(enqueue-fact-utility-review-state % session-id observations))]
        (future (run-fact-utility-review-worker! session-id token))))
    (count observations)))

(defn- claim-fact-utility-review-batch!
  [session-id worker-token]
  (update-fact-utility-review-state!
   #(claim-fact-utility-review-batch-state % session-id worker-token)))

(defn- finish-fact-utility-review-worker!
  [session-id worker-token]
  (when-let [token
             (update-fact-utility-review-state!
              #(finish-fact-utility-review-worker-state % session-id worker-token))]
    (future (run-fact-utility-review-worker! session-id token))))

(defn- run-fact-utility-review-worker!
  [session-id worker-token]
  (try
    (Thread/sleep (long fact-utility-review-debounce-ms))
    (loop []
      (when-let [batch (seq (claim-fact-utility-review-batch! session-id worker-token))]
        (try
          (wm/review-fact-utility-observations! batch)
          (catch Exception e
            (log/warn e "Failed to review fact utility batch"
                      {:session-id session-id
                       :batch-size (count batch)})))
        (recur)))
    (catch InterruptedException _
      (.interrupt (Thread/currentThread)))
    (finally
      (finish-fact-utility-review-worker! session-id worker-token))))

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
  [session-id fact-eids user-message assistant-response]
  (when (and (seq fact-eids)
             (seq assistant-response))
    (wm/apply-fact-utility-heuristic! fact-eids assistant-response)
    (enqueue-fact-utility-review! session-id fact-eids user-message assistant-response)))

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
  [session-id fact-eids user-message assistant-response]
  (try
    (schedule-fact-utility-review! session-id fact-eids user-message assistant-response)
    (catch Exception e
      (log/warn e "Failed to schedule fact utility review; continuing without it"
                {:fact-count (count fact-eids)})
      nil)))

(defn- normalize-branch-task
  [task]
  (cond
    (string? task)
    {:task task
     :prompt task}

    (map? task)
    (let [label (or (:task task)
                    (:title task)
                    (get task "task")
                    (get task "title")
                    (get task "label"))
          prompt (or (:prompt task)
                     (:message task)
                     (get task "prompt")
                     (get task "message")
                     label)]
      {:task (str (or label prompt "branch task"))
       :prompt (str (or prompt label ""))})

    :else
    {:task (str task)
     :prompt (str task)}))

(defn- branch-task-prompt
  [{:keys [task prompt]} objective]
  (str "You are a temporary branch worker for the main Xia agent.\n"
       "You are not talking directly to the user. Work independently on the assigned subtask and report back to the parent agent.\n"
       "Rules:\n"
       "- Do not ask the user questions.\n"
       "- Use tools only when they help complete this subtask.\n"
       "- Do not create schedules, request approvals, or perform privileged actions.\n"
       "- Focus only on the assigned subtask.\n"
       "- Return concise, factual results for the parent agent.\n"
       "- End with short sections titled Findings, Evidence, and Open Questions.\n\n"
       (when (and objective (not (str/blank? objective)))
         (str "Parent objective:\n" objective "\n\n"))
       "Assigned subtask:\n"
       task
       "\n\n"
       "What to do:\n"
       prompt))

(defn- branch-result-summary
  [results]
  (let [completed (count (filter #(= "completed" (:status %)) results))
        failed (count (filter #(= "failed" (:status %)) results))]
    (str "Completed " completed " branch task"
         (when (not= 1 completed) "s")
         (when (pos? failed)
           (str "; " failed " failed")))))

(defn- parse-tool-args
  [func-name args-str]
  (cond
    (nil? args-str)
    {}

    (string? args-str)
    (try
      (json/read-json args-str)
      (catch Exception e
        (log/warn e "Failed to parse tool arguments for" func-name "- using empty args map")
        {}))

    :else
    (do
      (log/warn "Tool arguments for" func-name "were not a JSON string - using empty args map")
      {})))

(def ^:private persisted-tool-result-large-keys
  #{:image_data_url "image_data_url"})

(defn- result-value
  [result & ks]
  (some (fn [k]
          (or (get result k)
              (when (keyword? k)
                (get result (name k)))))
        ks))

(defn- public-vision-image-url?
  [url]
  (cond
    (not (string? url))
    false

    (str/starts-with? (str/lower-case url) "data:image/")
    true

    :else
    (try
      (ssrf/validate-url! url)
      true
      (catch Exception _
        false))))

(defn- tool-result-image-urls
  [result]
  (when (map? result)
    (let [image-urls-value (result-value result :image_urls)]
      (->> (concat
            [(result-value result :image_data_url :image-url)]
            [(result-value result :image_url)]
            (cond
              (nil? image-urls-value) []
              (sequential? image-urls-value) image-urls-value
              :else [image-urls-value]))
           (filter public-vision-image-url?)
           distinct
           vec))))

(defn- tool-result-summary
  [result]
  (let [summary (result-value result :summary)]
    (when (and (string? summary)
               (not (str/blank? summary)))
      summary)))

(defn- truncate-summary
  [value max-len]
  (let [s (some-> value str)
        max-len (long max-len)]
    (when (seq s)
      (if (> (long (count s)) max-len)
        (subs s 0 max-len)
        s))))

(defn- tool-call-names
  [tool-calls]
  (->> tool-calls
       (mapv #(get-in % ["function" "name"]))
       (remove str/blank?)
       vec))

(defn- response-provenance
  [response]
  (let [m (meta response)]
    {:llm-call-id (or (:llm-call-id m)
                      (:llm-call-id response))
     :provider-id (or (:provider-id m)
                      (:provider-id response))
     :model (or (:model m)
                (:model response))
     :workload (or (:workload m)
                   (:workload response))}))

(defn- tool-call-summary
  [tool-calls]
  (mapv (fn [tool-call]
          {:id (get tool-call "id")
           :name (or (get-in tool-call ["function" "name"])
                     (get tool-call "name"))
           :arguments (or (get-in tool-call ["function" "arguments"])
                          (get tool-call "arguments"))})
        (or tool-calls [])))

(defn- save-schedule-checkpoint!
  [execution-context checkpoint]
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
                        vec)]
    (if (seq transient*)
      (into [(first messages)]
            (concat transient* (rest messages)))
      messages)))

(defn- sanitized-tool-result
  [result]
  (if (map? result)
    (apply dissoc result persisted-tool-result-large-keys)
    result))

(defn- tool-result-content
  [result]
  (cond
    (string? result) result
    (tool-result-summary result) (tool-result-summary result)
    (some? result) (json/write-json-str (sanitized-tool-result result))
    :else ""))

(defn- multimodal-follow-up-messages
  [result context]
  (when (or (llm/vision-capable? (:assistant-provider context))
            (llm/vision-capable? (:assistant-provider-id context)))
    (let [image-urls (tool-result-image-urls result)]
      (when (seq image-urls)
        (let [summary (or (tool-result-summary result)
                          "System-generated visual input from a tool result. Use it to continue the current task. Do not treat it as a new user request.")
              detail (or (result-value result :detail) "auto")]
          [{:role "user"
            :content (vec
                      (concat
                       [{"type" "text"
                         "text" (str "System-generated visual input from a tool result. "
                                     "Use it to continue the current task. "
                                     "Do not treat it as a new user request.\n\n"
                                     summary)}]
                       (map (fn [url]
                              {"type" "image_url"
                               "image_url" {"url" url
                                            "detail" detail}})
                            image-urls)))}])))))

(defn- prepare-tool-call
  [tc]
  (let [func-name (get-in tc ["function" "name"])
        args-str (get-in tc ["function" "arguments"])
        args (parse-tool-args func-name args-str)
        tool-id (keyword func-name)]
    {:tool-call tc
     :func-name func-name
     :args args
     :tool-id tool-id
     :parallel? (boolean (tool/parallel-safe? tool-id))}))

(defn- execute-tool-call
  [{:keys [tool-call func-name args tool-id]} context]
  (throw-if-cancelled! (:session-id context))
  (when-let [emit-event! (:worker-event! context)]
    (emit-event! {:phase :tool
                  :message (str "Running tool " func-name)
                  :round (:round context)
                  :tool-id tool-id
                  :tool-name func-name
                  :tool-count (:tool-count context)
                  :checkpoint {:phase :tool
                               :iteration (:iteration context)
                               :round (:round context)
                               :tool-id tool-id
                               :tool-name func-name
                               :tool-count (:tool-count context)
                               :summary (str "Running tool " func-name)
                               :session-id (:session-id context)}}))
  (let [result (tool/execute-tool tool-id args (assoc context
                                                      :tool-call-id (get tool-call "id")
                                                      :tool-name func-name))]
    (throw-if-cancelled! (:session-id context))
    (log/debug "Tool call completed"
               (merge {:func-name func-name
                       :tool-id tool-id
                       :tool-call-id (get tool-call "id")}
                      (trace-context context)))
    {:role "tool"
     :tool_call_id (get tool-call "id")
     :tool_name func-name
     :result (sanitized-tool-result result)
     :content (tool-result-content result)
     :follow-up-messages (multimodal-follow-up-messages result context)}))

(defn- bind-original-tool-call-ids
  [prepared-calls tool-results]
  (let [prepared-count (count prepared-calls)
        result-count (count tool-results)]
    (when (not= prepared-count result-count)
      (throw (ex-info "Tool execution result count mismatch"
                      {:prepared-count prepared-count
                       :result-count result-count})))
    (mapv (fn [{:keys [tool-call func-name tool-id]} result]
            (let [original-id (get tool-call "id")
                  result-id (:tool_call_id result)
                  effective-id (or original-id result-id)]
              (when (and (some? original-id)
                         (some? result-id)
                         (not= original-id result-id))
                (log/warn "Tool result returned mismatched tool_call_id; using original call id"
                          (merge {:func-name func-name
                                  :tool-id tool-id
                                  :tool-call-id effective-id
                                  :result-tool-call-id result-id}
                                 (trace-context prompt/*interaction-context*))))
              (cond-> result
                effective-id (assoc :tool_call_id effective-id)
                (nil? (:role result)) (assoc :role "tool"))))
          prepared-calls
          tool-results)))

(defn- tool-call-batches
  [prepared-calls]
  ;; Only consecutive parallel-safe calls are grouped together. We preserve the
  ;; original round order across safe/unsafe boundaries, so a sequence like
  ;; [safe unsafe safe] remains three ordered batches rather than merging the
  ;; two safe calls into one parallel group.
  (->> prepared-calls
       (partition-by :parallel?)
       (mapv (fn [calls]
               {:parallel? (:parallel? (first calls))
                :calls (vec calls)}))))

(defn- execute-tool-batch
  [{:keys [parallel? calls]} context]
  (if (and parallel? (> (count calls) 1))
    (do
      (when-let [emit-event! (:worker-event! context)]
        (emit-event! {:phase :tool
                      :message (str "Running " (count calls) " independent tools in parallel")
                      :round (:round context)
                      :parallel true
                      :tool-count (count calls)}))
      (log/debug "Executing tool batch in parallel"
                 (merge {:tool-count (count calls)
                         :tool-names (mapv :func-name calls)}
                        (trace-context context)))
      (let [futures (mapv (fn [call]
                            (future
                              (try
                                {:call call
                                 :result (execute-tool-call call context)}
                                (catch Throwable t
                                  {:call call
                                   :exception t}))))
                          calls)
            _ (when-let [session-id (:session-id context)]
                (register-parallel-tool-futures! session-id
                                                (:worker-token context)
                                                futures))
            results (try
                      (await-futures! futures
                                      (parallel-tool-timeout-ms)
                                      (fn [idx timeout-ms]
                                        (let [call (nth calls idx)]
                                          (ex-info (str "Parallel tool execution timed out: " (:func-name call))
                                                   (merge (trace-context context)
                                                          {:type :parallel-tool-timeout
                                                           :timeout-ms timeout-ms
                                                           :tool-id (:tool-id call)
                                                           :func-name (:func-name call)})))))
                      (finally
                        (when-let [session-id (:session-id context)]
                          (clear-parallel-tool-futures! session-id
                                                        (:worker-token context)
                                                        futures))))
            failures (keep #(when-let [t (:exception %)]
                              (assoc % :throwable t))
                           results)]
        (when-let [{:keys [call throwable]} (first failures)]
          (doseq [{suppressed :throwable} (rest failures)]
            (.addSuppressed ^Throwable throwable ^Throwable suppressed))
          (throw (ex-info (str "Parallel tool execution failed: " (:func-name call))
                          (merge (trace-context context)
                                 {:tool-id (:tool-id call)
                                  :func-name (:func-name call)
                                  :failures (mapv (fn [{:keys [call throwable]}]
                                                    (merge (trace-context context)
                                                           {:tool-id (:tool-id call)
                                                            :func-name (:func-name call)
                                                            :message (.getMessage ^Throwable throwable)}))
                                                  failures)})
                          throwable)))
        (mapv :result results)))
    (mapv #(execute-tool-call % context) calls)))

(defn- execute-tool-calls
  "Execute tool calls from the LLM response, return tool result messages."
  [tool-calls context]
  (validate-tool-round-call-count! tool-calls)
  (let [prepared-calls (mapv prepare-tool-call tool-calls)
        tool-results (->> prepared-calls
                          tool-call-batches
                          (mapcat #(execute-tool-batch % context))
                          vec)]
    (bind-original-tool-call-ids prepared-calls tool-results)))

(defn- response-content
  [response]
  (cond
    (string? response) response
    (map? response) (or (get response "content") "")
    (nil? response) ""
    :else (str response)))

(defn- autonomous-iteration-messages
  [autonomy-state iteration max-iterations & {:keys [incoming-message]}]
  [(autonomous/controller-system-message)
   (autonomous/controller-state-message
    {:goal (:goal autonomy-state)
     :iteration iteration
     :max-iterations max-iterations
     :stack (:stack autonomy-state)
     :incoming-message incoming-message})])

(defn- autonomous-iteration-summary
  [{:keys [assistant-text control]}]
  (or (:summary control)
      (some-> assistant-text str not-empty)
      "Completed an autonomous iteration."))

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
       (keep (fn [{:keys [title progress-status next-step]}]
               (when title
                 {:title title
                  :progress_status (some-> progress-status name)
                  :next_step next-step})))
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

(defn- emit-worker-event!
  [worker-state event]
  (let [now-ms (current-time-ms)]
    (swap! worker-state
           (fn [state]
             (let [phase (:phase event)
                   previous-phase (:phase state)
                   phase-start-ms (if (= phase previous-phase)
                                    (:phase-start-ms state now-ms)
                                    now-ms)
                   seq-no (inc (long (:seq state 0)))
                   event* (merge {:message nil
                                  :partial-content nil
                                  :round nil
                                  :tool-count nil
                                  :tool-id nil
                                  :tool-name nil
                                  :parallel nil
                                  :checkpoint nil}
                                 event
                                 {:seq seq-no
                                  :phase phase
                                  :phase-start-ms phase-start-ms
                                  :last-event-ms now-ms
                                  :updated-at-ms now-ms})]
               (-> state
                   (merge (dissoc event* :events))
                   (assoc :phase phase
                          :phase-start-ms phase-start-ms
                          :last-event-ms now-ms
                          :updated-at-ms now-ms
                          :seq seq-no
                          :tool-risk? (or (:tool-risk? state)
                                          (= phase :tool)))
                   (update :events (fnil conj []) event*)))))))

(defn- worker-timeout-ms
  [phase]
  (case phase
    :llm (supervisor-llm-timeout-ms)
    :tool (supervisor-tool-timeout-ms)
    (supervisor-phase-timeout-ms)))

(defn- worker-stalled?
  [{:keys [phase last-event-ms]}]
  (when (and phase last-event-ms)
    (> (- (current-time-ms) (long last-event-ms))
       (long (worker-timeout-ms phase)))))

(defn- worker-stall-ex
  [session-id channel iteration max-iterations autonomy-state worker-state]
  (let [tip (autonomous/current-frame autonomy-state)]
    (ex-info (str "Agent supervisor stopped a stalled worker during "
                  (some-> (:phase worker-state) name)
                  " phase")
             {:type :agent-stalled
              :session-id session-id
              :channel channel
              :phase (:phase worker-state)
              :iteration iteration
              :max-iterations max-iterations
              :current-focus (:title tip)
              :progress-status (:progress-status tip)
              :timeout-ms (worker-timeout-ms (:phase worker-state))
              :last-event-ms (:last-event-ms worker-state)
              :tool-id (:tool-id worker-state)
              :tool-name (:tool-name worker-state)
              :round (:round worker-state)})))

(defn- worker-stop-timeout-ex
  [session-id channel iteration max-iterations autonomy-state worker-state]
  (let [tip (autonomous/current-frame autonomy-state)]
    (ex-info (str "Agent supervisor could not stop a stalled worker during "
                  (some-> (:phase worker-state) name)
                  " phase")
             {:type :agent-stop-timeout
              :session-id session-id
              :channel channel
              :phase (:phase worker-state)
              :iteration iteration
              :max-iterations max-iterations
              :current-focus (:title tip)
              :progress-status (:progress-status tip)
              :grace-ms (supervisor-restart-grace-ms)
              :tool-id (:tool-id worker-state)
              :tool-name (:tool-name worker-state)
              :round (:round worker-state)})))

(defn- stop-worker!
  ([session-id]
   (stop-worker! session-id nil))
  ([session-id worker]
   (let [worker* (or worker
                     (:worker-future (session-run-entry session-id)))
         parallel-tool-futures (some-> (session-run-entry session-id)
                                       :parallel-tool-futures
                                       seq)
         interrupted? (volatile! (Thread/interrupted))]
     (try
       (interrupt-worker-thread! session-id)
       (when parallel-tool-futures
         (cancel-futures! parallel-tool-futures))
       (when worker*
         (future-cancel worker*))
       (if (nil? worker*)
         true
         (let [deadline-ms (+ (current-time-ms)
                              (long (supervisor-restart-grace-ms)))]
           (loop []
             (cond
               (future-done? worker*)
               true

               (>= (current-time-ms) deadline-ms)
               false

               :else
               (do
                 (try
                   (Thread/sleep 10)
                   (catch InterruptedException _
                     (vreset! interrupted? true)))
                 (recur))))))
       (finally
         (when @interrupted?
           (.interrupt (Thread/currentThread))))))))

(defn- worker-cancel-stop-timeout-ex
  [session-id channel iteration max-iterations autonomy-state worker-state]
  (let [tip (autonomous/current-frame autonomy-state)]
    (ex-info (str "Agent supervisor could not stop the worker after request cancellation during "
                  (some-> (:phase worker-state) name)
                  " phase")
             {:type :agent-stop-timeout
              :session-id session-id
              :channel channel
              :phase (:phase worker-state)
              :iteration iteration
              :max-iterations max-iterations
              :current-focus (:title tip)
              :progress-status (:progress-status tip)
              :grace-ms (supervisor-restart-grace-ms)
              :cancel-reason (cancellation-reason session-id)
              :tool-id (:tool-id worker-state)
              :tool-name (:tool-name worker-state)
              :round (:round worker-state)})))

(def ^:private non-restartable-worker-error-types
  #{:request-cancelled
    :autonomous-loop-stalled
    :agent-stop-timeout
    :tool-round-limit-exceeded
    :tool-call-limit-exceeded
    :user-message-too-large
    :session-busy})

(defn- restartable-worker-error?
  [t worker-state]
  (let [type (some-> t ex-data :type)]
    (cond
      (:tool-risk? worker-state)
      false

      (instance? InterruptedException t)
      false

      (contains? non-restartable-worker-error-types type)
      false

      (contains? #{:agent-stalled :parallel-tool-timeout} type)
      true

      :else
      true)))

(defn- worker-failure-summary
  [t]
  (let [type (some-> t ex-data :type)
        message (or (.getMessage ^Throwable t)
                    (some-> type name)
                    "worker failure")]
    (truncate-summary message 240)))

(defn- wait-for-worker!
  [execution-context session-id channel iteration max-iterations autonomy-state worker-state worker]
  (let [handled-seq (volatile! 0)
        handle-events! (fn [snapshot]
                         (doseq [event (filter #(> (long (:seq % 0))
                                                   (long @handled-seq))
                                               (:events snapshot))]
                           (vreset! handled-seq (long (:seq event 0)))
                           (report-supervisor-status! (:phase event)
                                                      (:message event)
                                                      autonomy-state
                                                      iteration
                                                      max-iterations
                                                      :round (:round event)
                                                      :partial-content (:partial-content event)
                                                      :tool-count (:tool-count event)
                                                      :tool-id (:tool-id event)
                                                      :tool-name (:tool-name event)
                                                      :parallel (:parallel event)
                                                      :intent-focus (:intent-focus event)
                                                      :intent-agenda-item (:intent-agenda-item event)
                                                      :intent-plan-step (:intent-plan-step event)
                                                      :intent-why (:intent-why event)
                                                      :intent-tool-name (:intent-tool-name event)
                                                      :intent-tool-args-summary (:intent-tool-args-summary event))
                           (when-let [checkpoint (:checkpoint event)]
                             (save-schedule-checkpoint! execution-context checkpoint))))
        cancel-run! (fn [snapshot]
                      (report-supervisor-status! :cancelling
                                                 "Stopping current work"
                                                 autonomy-state
                                                 iteration
                                                 max-iterations
                                                 :round (:round snapshot)
                                                 :tool-count (:tool-count snapshot)
                                                 :tool-id (:tool-id snapshot)
                                                 :tool-name (:tool-name snapshot)
                                                 :parallel (:parallel snapshot))
                      (throw (if (stop-worker! session-id worker)
                               (request-cancelled-ex session-id
                                                     (cancellation-reason session-id))
                               (worker-cancel-stop-timeout-ex session-id
                                                              channel
                                                              iteration
                                                              max-iterations
                                                              autonomy-state
                                                              snapshot))))]
    (loop []
      (let [snapshot @worker-state]
        (handle-events! snapshot)
        (cond
          (session-cancelled? session-id)
          (cancel-run! snapshot)

          (future-done? worker)
          (do
            (handle-events! @worker-state)
            (try
              @worker
              (catch java.util.concurrent.ExecutionException e
                (throw (or (.getCause e) e)))))

          (worker-stalled? snapshot)
          (do
            (throw (if (stop-worker! session-id worker)
                     (worker-stall-ex session-id
                                      channel
                                      iteration
                                      max-iterations
                                      autonomy-state
                                      snapshot)
                     (worker-stop-timeout-ex session-id
                                             channel
                                             iteration
                                             max-iterations
                                             autonomy-state
                                             snapshot))))

          :else
          (do
            (try
              (Thread/sleep (long (supervisor-tick-ms)))
              (catch InterruptedException _
                (request-session-cancel! session-id
                                         (or (cancellation-reason session-id)
                                             "request interrupted"))
                (cancel-run! snapshot)))
            (recur)))))))

(defn- run-supervised-agent-iteration
  [session-id channel resource-session-id local-doc-ids artifact-ids
   execution-context assistant-provider assistant-provider-id transient-messages
   working-memory-message update-working-memory? refresh-working-memory?
   max-tool-rounds autonomy-state max-iterations system-prompt-cache-entry]
  (loop [attempt 0]
    (let [worker-token (Object.)
          worker-state (atom {:phase nil
                              :seq 0
                              :last-event-ms (current-time-ms)
                              :events []})
          _ (begin-worker-run! session-id worker-token)
          worker (future
                   (register-worker-thread! session-id worker-token)
                   (try
                     (run-agent-iteration session-id
                                          channel
                                          resource-session-id
                                          local-doc-ids
                                          artifact-ids
                                          (assoc execution-context
                                                 :worker-token worker-token)
                                          assistant-provider
                                          assistant-provider-id
                                          transient-messages
                                          working-memory-message
                                          update-working-memory?
                                          refresh-working-memory?
                                          max-tool-rounds
                                          worker-state
                                          system-prompt-cache-entry)
                     (finally
                       (clear-worker-run! session-id worker-token))))]
      (register-worker-future! session-id worker-token worker)
      (let [result (try
                     {:ok (wait-for-worker! execution-context
                                            session-id
                                            channel
                                            (:iteration execution-context)
                                            max-iterations
                                            autonomy-state
                                            worker-state
                                            worker)}
                     (catch Throwable t
                       {:error t}))]
        (if-let [t (:error result)]
          (let [worker-snapshot @worker-state
                max-restarts (long (supervisor-max-restarts))
                attempt* (inc attempt)]
            (if (and (< attempt max-restarts)
                     (restartable-worker-error? t worker-snapshot)
                     (not (session-cancelled? session-id)))
              (do
                (report-supervisor-status! :restarting
                                           (str "Restarting iteration after "
                                                (worker-failure-summary t)
                                                " (attempt "
                                                attempt*
                                                "/"
                                                max-restarts
                                                ")")
                                           autonomy-state
                                           (:iteration execution-context)
                                           max-iterations
                                           :attempt attempt*)
                (save-schedule-checkpoint! execution-context
                                           {:phase :restarting
                                            :iteration (:iteration execution-context)
                                            :summary (worker-failure-summary t)
                                            :attempt attempt*
                                            :session-id session-id
                                            :failure-phase (some-> t ex-data :phase)})
                (Thread/sleep (long (supervisor-restart-backoff-ms)))
                (recur attempt*))
              (throw t)))
          (:ok result))))))

(defn- iteration-signature
  [autonomy-state control]
  (let [tip (autonomous/current-frame autonomy-state)]
    {:title (:title tip)
     :progress-status (:progress-status tip)
     :summary (:summary control)
     :reason (:reason control)
     :next-step (:next-step control)
     :stack-action (:stack-action control)
     :agenda (:agenda tip)
     :stack (status-stack (:stack autonomy-state))}))

(defn- update-iteration-loop-state
  [{:keys [signature count]} next-signature]
  (if (= signature next-signature)
    {:signature next-signature
     :count (inc (long (or count 0)))}
    {:signature next-signature
     :count 1}))

(defn- throw-if-identical-iteration-loop!
  [session-id channel iteration max-iterations loop-state autonomy-state control]
  (let [count* (long (or (:count loop-state) 0))
        limit (long (supervisor-max-identical-iterations))]
    (when (>= count* limit)
      (let [tip (autonomous/current-frame autonomy-state)]
        (throw (ex-info (str "Autonomous loop made no progress after "
                             limit
                             " identical iterations")
                        {:type :autonomous-loop-stalled
                         :session-id session-id
                         :channel channel
                         :iteration iteration
                         :max-iterations max-iterations
                         :current-focus (:title tip)
                         :progress-status (:progress-status tip)
                         :next-step (:next-step control)
                         :agenda (:agenda tip)
                         :stack (:stack autonomy-state)}))))))

(defn- merge-fact-eids
  [left right]
  (->> (concat (or left []) (or right []))
       distinct
       vec))

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
  (boolean
   (or (not= :parsed (:control-status parsed))
       (= :clear (:stack-action control))
       (:goal-complete? control)
       (= :complete (:progress-status control))))))

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
    (audit/log! execution-context
                {:actor :assistant
                 :type :llm-response
                 :message-id assistant-message-id
                 :llm-call-id llm-call-id
                 :data {:provider-id (some-> provider-id name)
                        :model model
                        :workload (some-> workload name)
                        :tool-calls []}})))

(defn- run-agent-iteration
  [session-id channel resource-session-id local-doc-ids artifact-ids
   execution-context assistant-provider assistant-provider-id transient-messages
   working-memory-message update-working-memory? refresh-working-memory?
   max-tool-rounds worker-state system-prompt-cache-entry]
  (let [emit-event! #(emit-worker-event! worker-state %)
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
          {:keys [messages used-fact-eids system-prompt-cache-entry]}
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
             round 0]
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
              parsed-response (autonomous/parse-controller-response
                               (response-content response))
              assistant-content (or (:assistant-text parsed-response)
                                    (response-content response))
              _ (when (zero? round)
                  (emit-intent-event! emit-event!
                                      execution-context
                                      parsed-response))
              has-tools? (and (map? response) (seq (get response "tool_calls")))]
          (if has-tools?
            (do
              (when (>= (long round) (long max-tool-rounds))
                (throw (ex-info "Too many tool-calling rounds"
                                {:type :tool-round-limit-exceeded
                                 :rounds round
                                 :max-tool-rounds max-tool-rounds})))
              (let [{:keys [llm-call-id provider-id model workload]} (response-provenance response)
                    tool-calls (get response "tool_calls")
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
                                            vec)]
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
                  (db/add-message! session-id :tool
                                   nil
                                   :tool-result (:result tr)
                                   :tool-id (:tool_call_id tr)
                                   :tool-call-id (:tool_call_id tr)
                                   :tool-name (:tool_name tr)
                                   :llm-call-id llm-call-id
                                   :provider-id provider-id
                                   :model model
                                   :workload workload))
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
                       (inc round))))
            (do
              (throw-if-cancelled! session-id)
              (emit-event! {:phase :finalizing
                           :message "Preparing response"
                           :iteration (:iteration execution-context)})
              {:response response
               :parsed-response parsed-response
               :used-fact-eids used-fact-eids
               :refresh-needed? (retrieval-state/changed? retrieval-version-before
                                                         retrieval-session-id)
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
                                     working-memory-message]
                              :or {channel :terminal
                                   tool-context {}
                                   persist-message? true}}]
  (with-session-turn-lock
    session-id
    (fn []
      (with-session-run
        session-id
        (fn []
          (let [request-context (derive-request-context session-id channel tool-context)]
            (binding [prompt/*interaction-context* request-context]
              (try
                (throw-if-cancelled! session-id)
                (validate-user-message! user-message)
                (throw-if-cancelled! session-id)
                (wm/ensure-wm! session-id)
                (when persist-message?
                  (let [user-message-id (db/add-message! session-id :user user-message
                                                         :local-doc-ids local-doc-ids
                                                         :artifact-ids artifact-ids)]
                    (audit/log! request-context
                                {:actor :user
                                 :type :user-message
                                 :message-id user-message-id
                                 :data {:local-doc-ids (vec (or local-doc-ids []))
                                        :artifact-ids (vec (or artifact-ids []))}})))
                (let [{assistant-provider :provider
                       assistant-provider-id :provider-id}
                      (llm/resolve-provider-selection
                       (cond-> {:workload :assistant}
                         provider-id
                         (assoc :provider-id provider-id)))
                      base-execution-context (merge tool-context
                                                    request-context
                                                    {:session-id session-id
                                                     :channel channel
                                                     :user-message user-message
                                                     :resource-session-id resource-session-id
                                                     :assistant-provider assistant-provider
                                                     :assistant-provider-id assistant-provider-id})
                      max-tool-rounds* (long (or max-tool-rounds
                                                 (configured-max-tool-rounds)))
                      max-iterations* (long (autonomous/max-iterations))
                      transient-messages* (vec (filter map? transient-messages))
                      initial-autonomy-state (autonomous/prepare-turn-state
                                              (wm/autonomy-state session-id)
                                              user-message)]
                  (wm/set-autonomy-state! session-id initial-autonomy-state)
                  (loop [iteration 1
                         fact-eids []
                         loop-state nil
                         refresh-working-memory? false
                         system-prompt-cache-entry nil]
                    (throw-if-cancelled! session-id)
                    (let [autonomy-state (or (wm/autonomy-state session-id)
                                             initial-autonomy-state)
                          iteration-context (assoc base-execution-context
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
                          update-working-memory? (= iteration 1)
                          wm-message (or working-memory-message user-message)]
                      (save-schedule-checkpoint!
                       iteration-context
                       {:phase :understanding
                        :iteration iteration
                        :summary (if (= iteration 1)
                                   "Understanding the goal and preparing the first plan."
                                   "Resuming the autonomous loop with the updated plan.")
                        :session-id session-id})
                      (let [{:keys [response parsed-response used-fact-eids refresh-needed?
                                    system-prompt-cache-entry]}
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
                                                            system-prompt-cache-entry)
                            parsed parsed-response
                            control (:control parsed)
                            summary (autonomous-iteration-summary parsed)
                            fact-eids* (merge-fact-eids fact-eids used-fact-eids)
                            updated-autonomy-state (if control
                                                     (let [next-state (autonomous/apply-control autonomy-state
                                                                                                control)]
                                                       (wm/set-autonomy-state! session-id next-state)
                                                       (or (wm/autonomy-state session-id)
                                                           next-state))
                                                     autonomy-state)
                            updated-tip (autonomous/current-frame updated-autonomy-state)
                            _ (wm/snapshot! session-id)
                            text (final-assistant-text parsed response)]
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
                          :status (some-> control :status)
                          :next-step (some-> control :next-step)
                          :progress-status (some-> updated-tip :progress-status)
                          :agenda (some-> updated-tip :agenda)
                          :stack (some-> updated-autonomy-state :stack)})
                        (cond
                          (or (nil? control)
                              (= :complete (:status control)))
                          (do
                            (persist-assistant-message! session-id
                                                        text
                                                        iteration-context
                                                        response
                                                        local-doc-ids
                                                        artifact-ids)
                            (when (clear-autonomy-state-on-terminal? parsed)
                              (wm/clear-autonomy-state! session-id)
                              (wm/snapshot! session-id))
                            (launch-fact-utility-review! session-id fact-eids* user-message text)
                            (prompt/status! {:state :done
                                             :phase :complete
                                             :message "Ready"})
                            text)

                          (>= iteration max-iterations*)
                          (let [final-text (append-assistant-note text
                                                                  (iteration-limit-note max-iterations*
                                                                                        control))]
                            (persist-assistant-message! session-id
                                                        final-text
                                                        iteration-context
                                                        response
                                                        local-doc-ids
                                                        artifact-ids)
                            (launch-fact-utility-review! session-id fact-eids* user-message text)
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
                            (prompt/status! (merge {:state :done
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
                                                                      control))]
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
                                   next-loop-state
                                   (boolean refresh-needed?)
                                   system-prompt-cache-entry)))))))
                (catch InterruptedException e
                  (request-session-cancel! session-id "request interrupted")
                  (if (stop-worker! session-id)
                    (let [cancel-ex (request-cancelled-ex session-id
                                                          (cancellation-reason session-id)
                                                          e)]
                      (save-schedule-checkpoint! request-context
                                                 {:phase :cancelled
                                                  :summary "Request cancelled"
                                                  :session-id session-id})
                      (prompt/status! {:state :cancelled
                                       :phase :cancelled
                                       :message "Request cancelled"})
                      (throw cancel-ex))
                    (throw (ex-info "Agent supervisor could not stop the worker after request cancellation"
                                    {:type :agent-stop-timeout
                                     :session-id session-id
                                     :channel channel
                                     :grace-ms (supervisor-restart-grace-ms)}
                                    e))))
                (catch clojure.lang.ExceptionInfo e
                  (let [data (ex-data e)]
                    (cond
                      (= :request-cancelled (:type data))
                      (do
                        (save-schedule-checkpoint! request-context
                                                   {:phase :cancelled
                                                    :summary "Request cancelled"
                                                    :session-id session-id})
                        (prompt/status! {:state :cancelled
                                         :phase :cancelled
                                         :message "Request cancelled"})
                        (throw e))

                      (contains? #{:agent-stalled :autonomous-loop-stalled :agent-stop-timeout} (:type data))
                      (do
                        (save-schedule-checkpoint! request-context
                                                   {:phase :stalled
                                                    :summary (.getMessage e)
                                                    :session-id session-id
                                                    :iteration (:iteration data)
                                                    :current-focus (:current-focus data)
                                                    :progress-status (:progress-status data)})
                        (prompt/status! {:state :error
                                         :phase :stalled
                                         :message (str "Supervisor stopped the run: " (.getMessage e))})
                        (throw e))

                      :else
                      (do
                        (save-schedule-checkpoint! request-context
                                                   {:phase :error
                                                    :summary (.getMessage e)
                                                    :session-id session-id})
                        (prompt/status! {:state :error
                                         :phase :error
                                         :message (str "Request failed: " (.getMessage e))})
                        (throw e)))))
                (catch Exception e
                  (if (session-cancelled? session-id)
                    (let [cancel-ex (request-cancelled-ex session-id
                                                          (cancellation-reason session-id)
                                                          e)]
                      (save-schedule-checkpoint! request-context
                                                 {:phase :cancelled
                                                  :summary "Request cancelled"
                                                  :session-id session-id})
                      (prompt/status! {:state :cancelled
                                       :phase :cancelled
                                       :message "Request cancelled"})
                      (throw cancel-ex))
                    (do
                      (save-schedule-checkpoint! request-context
                                                 {:phase :error
                                                  :summary (.getMessage e)
                                                  :session-id session-id})
                      (prompt/status! {:state :error
                                       :phase :error
                                       :message (str "Request failed: " (.getMessage e))})
                      (throw e))))))))))))

(defn- run-branch-task*
  [parent-session-id {:keys [task prompt] :as branch-task}
   {:keys [channel provider-id resource-session-id objective
           max-tool-rounds tool-context]
    :or {channel :terminal}}]
  (throw-if-runtime-stopping! parent-session-id)
  (throw-if-cancelled! parent-session-id)
  (let [parent-trace (trace-context prompt/*interaction-context*)
        branch-request-id (new-request-id)
        branch-trace (cond-> (merge parent-trace
                                    {:channel :branch
                                     :request-id branch-request-id
                                     :correlation-id (or (:correlation-id parent-trace)
                                                         (:request-id parent-trace)
                                                         branch-request-id)})
                       (:request-id parent-trace)
                       (assoc :parent-request-id (:request-id parent-trace)))
        child-session-id (db/create-session! :branch
                                             {:parent-session-id parent-session-id
                                              :worker? true
                                              :label task})]
    (try
      (throw-if-runtime-stopping! child-session-id)
      (throw-if-cancelled! child-session-id)
      (wm/create-wm! child-session-id)
      (let [result (process-message child-session-id
                                    (branch-task-prompt branch-task objective)
                                    :channel :branch
                                    :provider-id provider-id
                                    :resource-session-id (or resource-session-id
                                                             parent-session-id)
                                    :max-tool-rounds max-tool-rounds
                                    :tool-context (merge {:branch-worker? true
                                                          :parent-session-id parent-session-id
                                                          :resource-session-id (or resource-session-id
                                                                                   parent-session-id)}
                                                         branch-trace
                                                         tool-context))
            wm-context (wm/wm->context child-session-id)]
        (merge branch-trace
               {:task task
                :status "completed"
                :session-id child-session-id
                :topics (:topics wm-context)
                :result result}))
      (catch Throwable t
        (log/error t "Branch task failed"
                   (merge {:task task
                           :session-id child-session-id
                           :parent-session-id parent-session-id}
                          branch-trace))
        (merge branch-trace
               {:task task
                :status "failed"
                :session-id child-session-id
                :error (.getMessage t)
                :error-detail (throwable-detail t)}))
      (finally
        (try
          (db/set-session-active! child-session-id false)
          (catch Throwable t
            (log/warn t "Failed to deactivate branch worker session"
                      (merge {:task task
                              :session-id child-session-id
                              :parent-session-id parent-session-id}
                             branch-trace))))
        (try
          (wm/clear-wm! child-session-id)
          (catch Throwable t
            (log/warn t "Failed to clear branch worker working memory"
                      (merge {:task task
                              :session-id child-session-id
                              :parent-session-id parent-session-id}
                             branch-trace))))))))

(defn run-branch-tasks
  "Run independent branch tasks in separate worker sessions with isolated
   working memory and shared long-term memory access. Returns structured
   branch results for the parent agent."
  [tasks & {:keys [session-id channel provider-id resource-session-id objective
                   max-parallel max-tool-rounds tool-context]
            :or {channel :terminal
                 tool-context {}}}]
  (let [parent-context prompt/*interaction-context*
        parent-session-id (or session-id (:session-id parent-context))
        channel* (or channel (:channel parent-context) :terminal)
        provider-id* (or provider-id
                         (:assistant-provider-id parent-context))
        resource-session-id* (or resource-session-id parent-session-id)
        branch-tasks (->> tasks (map normalize-branch-task) (remove #(str/blank? (:prompt %))) vec)
        task-count (count branch-tasks)
        _ (throw-if-runtime-stopping! parent-session-id)
        _ (throw-if-cancelled! parent-session-id)
        max-tasks (max-branch-tasks)
        max-parallel* (clojure.core/min (clojure.core/max 1 (long (or max-parallel (max-parallel-branches))))
                                        (clojure.core/max 1 (long max-tasks)))]
    (when (zero? task-count)
      (throw (ex-info "Branch tasks require at least one task" {})))
    (when (> (long task-count) (long max-tasks))
      (throw (ex-info (str "Too many branch tasks: " task-count " (max " max-tasks ")")
                      {:task-count task-count
                       :max-tasks max-tasks})))
    (report-status! (str "Running " task-count " branch task"
                         (when (not= 1 task-count) "s"))
                    :phase :branch
                    :branch-count task-count
                    :parallel true)
    (let [results (vec
                   (mapcat (fn [batch]
                             (let [futures (mapv (fn [branch-task]
                                                   (future
                                                     (run-branch-task* parent-session-id
                                                                       branch-task
                                                                       {:channel channel*
                                                                        :provider-id provider-id*
                                                                        :resource-session-id resource-session-id*
                                                                        :objective objective
                                                                        :max-tool-rounds (or max-tool-rounds
                                                                                             (max-branch-tool-rounds))
                                                                        :tool-context tool-context})))
                                                 batch)]
                               (await-futures! futures
                                               (branch-task-timeout-ms)
                                               (fn [idx timeout-ms]
                                                 (let [branch-task (nth batch idx)]
                                                   (ex-info (str "Branch task timed out: "
                                                                 (or (:task branch-task)
                                                                     (:prompt branch-task)
                                                                     "unnamed"))
                                                            (merge (trace-context parent-context)
                                                                   {:type :branch-task-timeout
                                                                    :timeout-ms timeout-ms
                                                                    :task (:task branch-task)
                                                                    :prompt (:prompt branch-task)})))))))
                           (partition-all max-parallel* branch-tasks)))]
      {:summary (branch-result-summary results)
       :parent_session_id parent-session-id
       :request_id (:request-id parent-context)
       :correlation_id (or (:correlation-id parent-context)
                           (:request-id parent-context))
       :branch_count task-count
       :completed_count (count (filter #(= "completed" (:status %)) results))
       :failed_count (count (filter #(= "failed" (:status %)) results))
       :results results})))
