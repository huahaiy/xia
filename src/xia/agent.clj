(ns xia.agent
  "Agent loop — the core runtime that processes user messages.

   Loop: user message → update working memory → build context
         → LLM call (with tools) → tool calls? → response

   Skills = markdown instructions injected into the system prompt.
   Tools  = executable functions the LLM can call via function-calling."
  (:require [taoensso.timbre :as log]
            [clojure.string :as str]
            [datalevin.embedding :as emb]
            [xia.agent.branch :as agent-branch]
            [xia.agent.fact-review :as fact-review]
            [xia.agent.task-runtime :as task-runtime]
            [xia.agent.tools :as agent-tools]
            [xia.async :as async]
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
            [xia.task-policy :as task-policy]
            [xia.tool :as tool]
            [xia.working-memory :as wm])
  (:import [java.util.concurrent Future TimeUnit TimeoutException]))

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
(def ^:private default-llm-status-preview-chars 160)
(def ^:private default-llm-status-update-interval-ms 500)
(def ^:private default-supervisor-tick-ms 250)
(def ^:private default-supervisor-phase-timeout-ms 30000)
(def ^:private default-supervisor-llm-timeout-ms 120000)
(def ^:private default-supervisor-tool-timeout-ms 120000)
(def ^:private default-task-control-wait-ms 10000)
(defonce ^:private active-session-turns (atom #{}))
(defonce ^:private active-session-runs (atom {}))
(defonce ^:private active-task-runs (atom {}))
(def ^:dynamic *turn-llm-budget-state* nil)

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

(defn- try-acquire-session-turn!
  [session-id]
  (loop []
    (let [active @active-session-turns]
      (cond
        (contains? active session-id)
        false

        (compare-and-set! active-session-turns active (conj active session-id))
        true

        :else
        (recur)))))

(defn- release-session-turn!
  [session-id]
  (when session-id
    (swap! active-session-turns disj session-id))
  nil)

(defn- with-session-turn-lock
  [session-id f]
  (if session-id
    (if (try-acquire-session-turn! session-id)
      (try
        (f)
        (finally
          (release-session-turn! session-id)))
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

(defn- task-control-wait-ms
  []
  (cfg/positive-long :agent/task-control-wait-ms
                     default-task-control-wait-ms))

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

(defn- task-cancellation-outcome
  [reason]
  (case reason
    "task pause requested"
    {:turn-state :paused
     :task-state :paused
     :stop-reason :paused
     :summary "Task paused by user"}

    "task interrupt requested"
    {:turn-state :cancelled
     :task-state :paused
     :stop-reason :interrupted
     :summary "Task interrupted by user"}

    "task steer requested"
    {:turn-state :cancelled
     :task-state :paused
     :stop-reason :interrupted
     :summary "Task interrupted by new instruction"}

    "task stop requested"
    {:turn-state :cancelled
     :task-state :cancelled
     :stop-reason :stopped
     :summary "Task stopped by user"}

    {:turn-state :cancelled
     :task-state :cancelled
     :stop-reason :cancelled
     :summary "Request cancelled"}))

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
  (if session-id
    (let [run-id (Object.)]
      (swap! active-session-runs assoc session-id
             {:run-id run-id
              :supervisor-thread (Thread/currentThread)
              :task-id nil
              :child-session-ids #{}
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

(defn- task-run-entry
  [task-id]
  (when task-id
    (get @active-task-runs task-id)))

(defn- session-bound-task-id
  [session-id]
  (some-> (session-run-entry session-id) :task-id))

(defn- live-run-entry-for-session
  [session-id]
  (or (some-> session-id session-bound-task-id task-run-entry)
      (session-run-entry session-id)))

(defn- wait-for-session-idle!
  [session-id timeout-ms]
  (let [deadline (+ (current-time-ms) (long timeout-ms))]
    (loop []
      (cond
        (nil? (session-run-entry session-id))
        true

        (>= (current-time-ms) deadline)
        false

        :else
        (do
          (Thread/sleep 50)
          (recur))))))

(defn- wait-for-task-idle!
  [task-id timeout-ms]
  (let [deadline (+ (current-time-ms) (long timeout-ms))]
    (loop []
      (cond
        (nil? (task-run-entry task-id))
        true

        (>= (current-time-ms) deadline)
        false

        :else
        (do
          (Thread/sleep 50)
          (recur))))))

(defn- update-session-run-entry!
  [session-id f]
  (when session-id
    (swap! active-session-runs
           (fn [runs]
             (if-let [entry (get runs session-id)]
               (assoc runs session-id (f entry))
               runs)))))

(defn- update-task-run-entry!
  [task-id f]
  (when task-id
    (swap! active-task-runs
           (fn [runs]
             (if-let [entry (get runs task-id)]
               (assoc runs task-id (f entry))
               runs)))))

(defn- register-task-run!
  [session-id task-id task-turn-id]
  (when (and session-id task-id task-turn-id)
    (when-let [entry (session-run-entry session-id)]
      (let [session-run-id (:run-id entry)
            task-run-id (Object.)
            task-entry {:task-id task-id
                        :task-turn-id task-turn-id
                        :session-id session-id
                        :task-run-id task-run-id
                        :session-run-id session-run-id
                        :supervisor-thread (:supervisor-thread entry)
                        :child-session-ids (:child-session-ids entry)
                        :cancelled? (:cancelled? entry)
                        :cancel-reason (:cancel-reason entry)}]
        (swap! active-task-runs assoc task-id task-entry)
        (update-session-run-entry! session-id
                                   (fn [run]
                                     (if (= session-run-id (:run-id run))
                                       (assoc run
                                              :task-id task-id
                                              :child-session-ids #{})
                                        run)))
        task-entry))))

(defn- clear-task-run!
  [session-id task-id task-turn-id task-run-id]
  (when task-id
    (let [expected-task-run-id (or task-run-id
                                   (some-> (task-run-entry task-id) :task-run-id))]
      (swap! active-task-runs
             (fn [runs]
               (if-let [entry (get runs task-id)]
                 (if (and (or (nil? expected-task-run-id)
                              (= expected-task-run-id (:task-run-id entry)))
                          (or (nil? session-id)
                              (= session-id (:session-id entry)))
                          (or (nil? task-turn-id)
                              (= task-turn-id (:task-turn-id entry))))
                   (dissoc runs task-id)
                   runs)
                 runs)))
      (when session-id
        (update-session-run-entry! session-id
                                   (fn [entry]
                                     (if (= task-id (:task-id entry))
                                       (assoc entry
                                              :task-id nil)
                                       entry)))))))

(defn- register-child-session!
  [parent-session-id child-session-id]
  (when (and parent-session-id
             child-session-id
             (not= parent-session-id child-session-id))
    (if-let [task-id (session-bound-task-id parent-session-id)]
      (update-task-run-entry! task-id
                              #(update % :child-session-ids (fnil conj #{}) child-session-id))
      (update-session-run-entry! parent-session-id
                                 #(update % :child-session-ids (fnil conj #{}) child-session-id)))))

(defn- unregister-child-session!
  [parent-session-id child-session-id]
  (when (and parent-session-id
             child-session-id
             (not= parent-session-id child-session-id))
    (if-let [task-id (session-bound-task-id parent-session-id)]
      (update-task-run-entry! task-id
                              #(update % :child-session-ids disj child-session-id))
      (update-session-run-entry! parent-session-id
                                 #(update % :child-session-ids disj child-session-id)))))

(defn- begin-worker-run!
  [session-id worker-token]
  (if-let [task-id (session-bound-task-id session-id)]
    (update-task-run-entry! task-id
                            #(assoc % :worker-token worker-token
                                    :worker-thread nil
                                    :worker-future nil
                                    :parallel-tool-futures []))
    (update-session-run-entry! session-id
                               #(assoc % :worker-token worker-token
                                       :worker-thread nil
                                       :worker-future nil
                                       :parallel-tool-futures []))))

(defn- register-worker-thread!
  [session-id worker-token]
  (if-let [task-id (session-bound-task-id session-id)]
    (update-task-run-entry! task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (assoc entry :worker-thread (Thread/currentThread))
                                entry)))
    (update-session-run-entry! session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (assoc entry :worker-thread (Thread/currentThread))
                                   entry)))))

(defn- clear-worker-thread!
  [session-id worker-token]
  (if-let [task-id (session-bound-task-id session-id)]
    (update-task-run-entry! task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (assoc entry :worker-thread nil)
                                entry)))
    (update-session-run-entry! session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (assoc entry :worker-thread nil)
                                   entry)))))

(defn- register-worker-future!
  [session-id worker-token worker]
  (if-let [task-id (session-bound-task-id session-id)]
    (update-task-run-entry! task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (assoc entry :worker-future worker)
                                entry)))
    (update-session-run-entry! session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (assoc entry :worker-future worker)
                                   entry)))))

(defn- clear-worker-run!
  [session-id worker-token]
  (if-let [task-id (session-bound-task-id session-id)]
    (update-task-run-entry! task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (assoc entry
                                       :worker-token nil
                                       :worker-thread nil
                                       :worker-future nil
                                       :parallel-tool-futures [])
                                entry)))
    (update-session-run-entry! session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (assoc entry
                                          :worker-token nil
                                          :worker-thread nil
                                          :worker-future nil
                                          :parallel-tool-futures [])
                                   entry)))))

(defn- register-parallel-tool-futures!
  [session-id worker-token futures]
  (if-let [task-id (session-bound-task-id session-id)]
    (update-task-run-entry! task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (update entry
                                        :parallel-tool-futures
                                        (fn [existing]
                                          (->> (concat (or existing []) futures)
                                               distinct
                                               vec)))
                                entry)))
    (update-session-run-entry! session-id
                               (fn [entry]
                                 (if (= worker-token (:worker-token entry))
                                   (update entry
                                           :parallel-tool-futures
                                           (fn [existing]
                                             (->> (concat (or existing []) futures)
                                                  distinct
                                                  vec)))
                                   entry)))))

(defn- clear-parallel-tool-futures!
  [session-id worker-token futures]
  (if-let [task-id (session-bound-task-id session-id)]
    (update-task-run-entry! task-id
                            (fn [entry]
                              (if (= worker-token (:worker-token entry))
                                (update entry
                                        :parallel-tool-futures
                                        (fn [existing]
                                          (let [to-clear (set futures)]
                                            (->> (or existing [])
                                                 (remove to-clear)
                                                 vec))))
                                entry)))
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
                                   entry)))))

(defn- interrupt-worker-thread!
  [session-id]
  (when-let [^Thread worker-thread (:worker-thread (live-run-entry-for-session session-id))]
    (when (not= (Thread/currentThread) worker-thread)
      (.interrupt worker-thread))
    true))

(defn- request-session-cancel!
  [session-id reason & {:keys [interrupt-supervisor?]
                        :or {interrupt-supervisor? false}}]
  (let [session-entry* (atom nil)
        task-entry*    (atom nil)]
    (when session-id
      (swap! active-session-runs
             (fn [runs]
               (if-let [entry (get runs session-id)]
                 (let [updated (assoc entry
                                      :cancelled? true
                                      :cancel-reason (or (:cancel-reason entry)
                                                         reason))]
                   (reset! session-entry* updated)
                   (assoc runs session-id updated))
                 runs)))
      (when-let [task-id (:task-id @session-entry*)]
        (swap! active-task-runs
               (fn [runs]
                 (if-let [entry (get runs task-id)]
                   (let [updated (assoc entry
                                        :cancelled? true
                                        :cancel-reason (or (:cancel-reason entry)
                                                           reason))]
                     (reset! task-entry* updated)
                     (assoc runs task-id updated))
                   runs))))
      (when-let [entry (or @task-entry* @session-entry*)]
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
        (doseq [child-session-id (or (:child-session-ids @task-entry*)
                                     (:child-session-ids @session-entry*))]
          (when (not= child-session-id session-id)
            (request-session-cancel! child-session-id
                                     reason
                                     :interrupt-supervisor? true)))
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

(defn- turn-budget-next-step
  [parsed autonomy-state]
  (or (some-> parsed :control :next-step str str/trim not-empty)
      (some-> parsed :intent :plan-step str str/trim not-empty)
      (some-> autonomy-state autonomous/current-frame :next-step str str/trim not-empty)))

(defn- turn-budget-note
  [budget-status parsed autonomy-state & {:keys [before-tools?]}]
  (str "Note: I stopped this turn after reaching the "
       (task-policy/turn-llm-budget-summary budget-status)
       "."
       (when before-tools?
         " I did not execute the next requested tool step.")
       (when-let [next-step (turn-budget-next-step parsed autonomy-state)]
         (str " Suggested next step: " next-step))
       " Reply to continue from the current agenda."))

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
(def ^:private fact-utility-review-state
  fact-review/fact-utility-review-state)

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
  (fact-review/schedule-fact-utility-review! session-id
                                             fact-eids
                                             user-message
                                             assistant-response
                                             :debounce-ms fact-utility-review-debounce-ms))

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

(defn- launch-fact-utility-review-without-budget!
  [session-id fact-eids user-message assistant-response]
  (binding [*turn-llm-budget-state* nil
            llm/*request-budget-guard* nil
            llm/*request-observer* nil]
    (launch-fact-utility-review! session-id
                                 fact-eids
                                 user-message
                                 assistant-response)))

(defn- tool-deps
  []
  {:await-futures! await-futures!
   :cancel-futures! cancel-futures!
   :clear-parallel-tool-futures! clear-parallel-tool-futures!
   :parallel-tool-timeout-ms parallel-tool-timeout-ms
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
  (task-runtime/record-task-item! (:task-turn-id execution-context)
                                  {:type :checkpoint
                                   :summary (or (:summary checkpoint)
                                                (some-> (:phase checkpoint) name)
                                                "Checkpoint")
                                   :data (merge (trace-context execution-context)
                                                checkpoint)})
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
  [session-id channel user-message autonomy-state task-id runtime-op interrupting-turn-id]
  (task-runtime/ensure-runtime-task! (task-runtime-deps)
                                     session-id
                                     channel
                                     user-message
                                     autonomy-state
                                     task-id
                                     runtime-op
                                     interrupting-turn-id))

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

(def ^:private loop-signature-stopwords
  #{"a" "an" "and" "are" "for" "from" "into" "its" "more" "now" "of" "on" "or"
    "remaining" "remains" "step" "steps" "still" "the" "to" "with" "work" "working"})

(def ^:private loop-signature-token-aliases
  {"again" "retry"
   "attempt" "retry"
   "attempting" "retry"
   "attempts" "retry"
   "check" "inspect"
   "checked" "inspect"
   "checking" "inspect"
   "compose" "draft"
   "composed" "draft"
   "composing" "draft"
   "drafted" "draft"
   "drafting" "draft"
   "examine" "inspect"
   "examined" "inspect"
   "examining" "inspect"
   "fetch" "search"
   "fetched" "search"
   "fetching" "search"
   "find" "search"
   "finding" "search"
   "found" "search"
   "inspect" "inspect"
   "inspected" "inspect"
   "inspecting" "inspect"
   "look" "inspect"
   "looked" "inspect"
   "looking" "inspect"
   "lookup" "search"
   "open" "inspect"
   "opened" "inspect"
   "opening" "inspect"
   "post" "send"
   "posted" "send"
   "posting" "send"
   "query" "search"
   "queried" "search"
   "querying" "search"
   "read" "inspect"
   "reading" "inspect"
   "reads" "inspect"
   "repeat" "retry"
   "repeated" "retry"
   "repeating" "retry"
   "reply" "send"
   "replied" "send"
   "replying" "send"
   "respond" "send"
   "responded" "send"
   "responding" "send"
   "review" "inspect"
   "reviewed" "inspect"
   "reviewing" "inspect"
   "scan" "inspect"
   "scanned" "inspect"
   "scanning" "inspect"
   "search" "search"
   "searched" "search"
   "searching" "search"
   "send" "send"
   "sending" "send"
   "sent" "send"
   "submit" "send"
   "submitted" "send"
   "submitting" "send"
   "view" "inspect"
   "viewed" "inspect"
   "viewing" "inspect"
   "write" "draft"
   "writing" "draft"
   "written" "draft"})

(def ^:private progress-status-scores
  {:not-started 0
   :pending 0
   :paused 0
   :resumable 0
   :diverged 0
   :blocked 0
   :in-progress 1
   :complete 5})

(defn- loop-text-signature
  [text]
  (some->> text
           str
           str/lower-case
           (re-seq #"[a-z0-9]+")
           (map #(get loop-signature-token-aliases % %))
           (remove #(or (< (count ^String %) 3)
                        (contains? loop-signature-stopwords %)))
           distinct
           sort
           vec
           not-empty))

(defn- loop-agenda-signature
  [agenda]
  (->> agenda
       (keep (fn [{:keys [item status]}]
               (when (or item status)
                 {:item-terms (loop-text-signature item)
                  :status status})))
       vec
       not-empty))

(defn- loop-stack-signature
  [stack]
  (->> stack
       (keep (fn [{:keys [title progress-status]}]
               (when (or title progress-status)
                 {:title-terms (loop-text-signature title)
                  :progress-status progress-status})))
       vec
       not-empty))

(defn- semantic-loop-fallback-signature
  [autonomy-state control]
  (let [tip (autonomous/current-frame autonomy-state)]
    {:root-goal-terms (loop-text-signature (autonomous/root-goal autonomy-state))
     :title-terms (loop-text-signature (:title tip))
     :next-step-terms (loop-text-signature (:next-step control))
     :agenda (loop-agenda-signature (:agenda tip))
     :stack (loop-stack-signature (:stack autonomy-state))}))

(defn- semantic-loop-text
  [autonomy-state control]
  (let [tip (autonomous/current-frame autonomy-state)
        goal (some-> (autonomous/root-goal autonomy-state) str not-empty)
        focus (some-> (:title tip) str not-empty)
        next-step (some-> (:next-step control) str not-empty)
        stack (->> (:stack autonomy-state)
                   (keep (fn [{:keys [title progress-status]}]
                           (when title
                             (str "- "
                                  "[" (some-> progress-status name) "] "
                                  title)))))
        agenda (->> (:agenda tip)
                    (keep (fn [{:keys [item status]}]
                            (when item
                              (str "- "
                                   "[" (some-> status name) "] "
                                   item)))))]
    (some->> [(when goal
                (str "Goal: " goal))
              (when focus
                (str "Focus: " focus))
              (when next-step
                (str "Next step: " next-step))
              (when (seq agenda)
                (str "Agenda:\n" (str/join "\n" agenda)))
              (when (seq stack)
                (str "Stack:\n" (str/join "\n" stack)))]
             (remove str/blank?)
             seq
             (str/join "\n"))))

(defn- cosine-similarity
  [left right]
  (when (and (sequential? left)
             (sequential? right)
             (= (count left) (count right))
             (pos? (count left)))
    (let [[dot norm-left norm-right]
          (reduce (fn [[dot* norm-left* norm-right*] [left-value right-value]]
                    (let [left* (double left-value)
                          right* (double right-value)]
                      [(+ dot* (* left* right*))
                       (+ norm-left* (* left* left*))
                       (+ norm-right* (* right* right*))]))
                  [0.0 0.0 0.0]
                  (map vector left right))]
      (when (and (pos? norm-left) (pos? norm-right))
        (/ dot
           (* (Math/sqrt norm-left)
              (Math/sqrt norm-right)))))))

(defn- embed-loop-text
  [embedding-cache text]
  (let [text* (some-> text str str/trim not-empty)]
    (cond
      (nil? text*)
      [embedding-cache nil]

      (contains? embedding-cache text*)
      [embedding-cache (get embedding-cache text*)]

      :else
      (let [embedding (try
                        (when-let [provider (db/current-embedding-provider)]
                          (some-> (emb/embedding provider [text*] nil)
                                  first
                                  vec))
                        (catch Throwable t
                          (log/debug t "Failed to embed autonomy loop state")
                          nil))]
        [(assoc embedding-cache text* embedding) embedding]))))

(defn- semantic-loop-equivalent?
  [embedding-cache previous-signature next-signature]
  (let [previous-fallback (:semantic-fallback previous-signature)
        next-fallback (:semantic-fallback next-signature)]
    (if (and previous-fallback
             next-fallback
             (= previous-fallback next-fallback))
      {:embedding-cache embedding-cache
       :same-semantic? true
       :semantic-similarity 1.0
       :semantic-match-source :fallback}
      (let [[embedding-cache* previous-embedding]
            (embed-loop-text embedding-cache (:semantic-text previous-signature))
            [embedding-cache** next-embedding]
            (embed-loop-text embedding-cache* (:semantic-text next-signature))
            similarity (cosine-similarity previous-embedding next-embedding)]
        {:embedding-cache embedding-cache**
         :same-semantic? (boolean (and similarity
                                       (>= similarity
                                           (task-policy/supervisor-semantic-loop-threshold))))
         :semantic-similarity similarity
         :semantic-match-source (when similarity :embedding)}))))

(defn- wm-query-signature
  [message]
  (loop-text-signature message))

(defn- iteration-progress-marker
  [autonomy-state control]
  (let [tip (autonomous/current-frame autonomy-state)
        stack (vec (:stack autonomy-state))
        agenda (vec (:agenda tip))
        stack-statuses (frequencies (keep :progress-status stack))
        agenda-statuses (frequencies (keep :status agenda))]
    {:goal-complete? (true? (:goal-complete? control))
     :stack-depth (count stack)
     :stack-complete (long (get stack-statuses :complete 0))
     :tip-status (:progress-status tip)
     :agenda-total (count agenda)
     :agenda-complete (long (+ (get agenda-statuses :completed 0)
                               (get agenda-statuses :skipped 0)))
     :agenda-active (long (get agenda-statuses :in-progress 0))
     :agenda-statuses agenda-statuses
     :stack-statuses stack-statuses}))

(defn- iteration-progress-score
  [{:keys [goal-complete? stack-complete tip-status agenda-complete agenda-active]}]
  (+ (if goal-complete? 1000 0)
     (* 100 (long (or stack-complete 0)))
     (* 10 (long (or agenda-complete 0)))
     (* 2 (long (or agenda-active 0)))
     (long (get progress-status-scores tip-status 0))))

(defn- iteration-tool-marker
  [tool-activity]
  (let [results (->> tool-activity
                     (mapcat :results)
                     vec)
        statuses (frequencies (keep :status results))
        failure-count (long (get statuses "error" 0))
        success-count (long (get statuses "success" 0))
        total-count (+ failure-count success-count)
        error-terms (->> results
                         (keep (fn [{:keys [error summary status]}]
                                 (when (= status "error")
                                   (or (loop-text-signature error)
                                       (loop-text-signature summary)))))
                         vec
                         not-empty)]
    {:round-count (count tool-activity)
     :total-count total-count
     :failure-count failure-count
     :success-count success-count
     :only-failures? (and (pos? total-count)
                          (zero? success-count))
     :error-signature error-terms}))

(defn- repeated-tool-failure-loop?
  [previous-signature next-signature]
  (let [previous-tool-marker (:tool-marker previous-signature)
        next-tool-marker (:tool-marker next-signature)]
    (and (:only-failures? previous-tool-marker)
         (:only-failures? next-tool-marker))))

(defn- iteration-stall-key
  [signature]
  (select-keys signature
               [:progress-status
                :stack-action
                :progress-marker]))

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
              :grace-ms (task-policy/supervisor-restart-grace-ms)
              :tool-id (:tool-id worker-state)
              :tool-name (:tool-name worker-state)
              :round (:round worker-state)})))

(defn- stop-worker!
  ([session-id]
   (stop-worker! session-id nil))
  ([session-id worker]
   (let [worker* (or worker
                     (:worker-future (live-run-entry-for-session session-id)))
         parallel-tool-futures (some-> (live-run-entry-for-session session-id)
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
                              (long (task-policy/supervisor-restart-grace-ms)))]
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
              :grace-ms (task-policy/supervisor-restart-grace-ms)
              :cancel-reason (cancellation-reason session-id)
              :tool-id (:tool-id worker-state)
              :tool-name (:tool-name worker-state)
              :round (:round worker-state)})))

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
             (not= :parsed (:intent-status parsed-response)))
    (throw (autonomous-protocol-ex
            session-id
            execution-context
            round
            parsed-response
            "First tool-calling response is missing a valid ACTION_INTENT_JSON envelope")))
  (when (contains? #{:parsed :malformed} (:control-status parsed-response))
    (throw (autonomous-protocol-ex
            session-id
            execution-context
            round
            parsed-response
            "Tool-calling response must not include AUTONOMOUS_STATUS_JSON"))))

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
                                                      :worker-phase (:phase event)
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
                                                 :worker-phase (:phase snapshot)
                                                 :round (:round snapshot)
                                                 :tool-count (:tool-count snapshot)
                                                 :tool-id (:tool-id snapshot)
                                                 :tool-name (:tool-name snapshot)
                                                 :parallel (:parallel snapshot)
                                                 :cancel-reason (cancellation-reason session-id))
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
   max-tool-rounds autonomy-state max-iterations system-prompt-cache-entry
   turn-budget-state]
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
                                          system-prompt-cache-entry
                                          turn-budget-state)
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
                restart-decision (task-policy/restart-policy-decision
                                  t
                                  worker-snapshot
                                  attempt
                                  :session-cancelled? (session-cancelled? session-id))
                max-restarts (:max-restarts restart-decision)
                attempt* (:attempt restart-decision)
                _ (prompt/policy-decision! (merge restart-decision
                                                  {:decision-type :restart-policy
                                                   :error (worker-failure-summary t)}))]
            (if (:allowed? restart-decision)
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
                                           :attempt attempt*
                                           :max-restarts max-restarts
                                           :failure-phase (some-> t ex-data :phase)
                                           :worker-phase (:phase worker-snapshot)
                                           :round (:round worker-snapshot)
                                           :tool-id (:tool-id worker-snapshot)
                                           :tool-name (:tool-name worker-snapshot))
                (save-schedule-checkpoint! execution-context
                                           {:phase :restarting
                                            :iteration (:iteration execution-context)
                                            :summary (worker-failure-summary t)
                                            :attempt attempt*
                                            :session-id session-id
                                            :failure-phase (some-> t ex-data :phase)})
                (Thread/sleep (long (:backoff-ms restart-decision)))
                (recur attempt*))
              (throw t)))
          (:ok result))))))

(defn- iteration-signature
  [autonomy-state control tool-activity]
  (let [tip (autonomous/current-frame autonomy-state)
        progress-marker (iteration-progress-marker autonomy-state control)]
    {:progress-status (:progress-status tip)
     :stack-action (:stack-action control)
     :tool-activity tool-activity
     :tool-marker (iteration-tool-marker tool-activity)
     :progress-marker progress-marker
     :semantic-text (semantic-loop-text autonomy-state control)
     :semantic-fallback (semantic-loop-fallback-signature autonomy-state control)}))

(defn- update-iteration-loop-state
  [{:keys [signature stall-key progress-score count embedding-cache]} next-signature]
  (let [next-stall-key (iteration-stall-key next-signature)
        next-progress-score (iteration-progress-score (:progress-marker next-signature))
        same-stall-state? (= stall-key next-stall-key)
        progressed? (> next-progress-score
                       (long (or progress-score Long/MIN_VALUE)))
        repeated-tool-failure? (and signature
                                    same-stall-state?
                                    (not progressed?)
                                    (repeated-tool-failure-loop? signature
                                                                 next-signature))
        {:keys [embedding-cache same-semantic? semantic-similarity semantic-match-source]}
        (if (and signature
                 same-stall-state?
                 (not progressed?))
          (semantic-loop-equivalent? (or embedding-cache {}) signature next-signature)
          {:embedding-cache (or embedding-cache {})
           :same-semantic? false
           :semantic-similarity nil
           :semantic-match-source nil})
        semantic-match-source (cond
                                (and repeated-tool-failure? same-semantic?)
                                :tool-failure

                                :else
                                semantic-match-source)]
    (if (and same-stall-state? same-semantic? (not progressed?))
      {:signature next-signature
       :stall-key next-stall-key
       :progress-score next-progress-score
       :embedding-cache embedding-cache
       :semantic-similarity semantic-similarity
       :semantic-match-source semantic-match-source
       :count (inc (long (or count 0)))}
      {:signature next-signature
       :stall-key next-stall-key
       :progress-score next-progress-score
       :embedding-cache embedding-cache
       :semantic-similarity semantic-similarity
       :semantic-match-source semantic-match-source
       :count 1})))

(defn- throw-if-identical-iteration-loop!
  [session-id channel iteration max-iterations loop-state autonomy-state control]
  (let [count* (long (or (:count loop-state) 0))
        limit (long (task-policy/supervisor-max-identical-iterations))]
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
                         :semantic-similarity (:semantic-similarity loop-state)
                         :semantic-match-source (:semantic-match-source loop-state)
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
              parsed-response (autonomous/parse-controller-response
                               (response-content response))
              assistant-content (or (:assistant-text parsed-response)
                                    (response-content response))
              budget-status (task-policy/turn-llm-budget-status turn-budget-state)
              _ (when (zero? round)
                  (emit-intent-event! emit-event!
                                      execution-context
                                      parsed-response))
              has-tools? (and (map? response) (seq (get response "tool_calls")))]
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
                                     interrupting-turn-id]
                              :or {channel :terminal
                                   tool-context {}
                                   persist-message? true}}]
  (with-session-turn-lock
    session-id
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
                (let [initial-autonomy-state (autonomous/prepare-turn-state
                                              (task-runtime/runtime-autonomy-state session-id task-id)
                                              user-message)
                      {:keys [task-id task-turn-id]} (task-runtime/ensure-runtime-task! (task-runtime-deps)
                                                                                         session-id
                                                                                         channel
                                                                                         user-message
                                                                                         initial-autonomy-state
                                                                                         task-id
                                                                                         runtime-op
                                                                                         interrupting-turn-id)
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
                      {assistant-provider :provider
                       assistant-provider-id :provider-id}
                      (llm/resolve-provider-selection
                       (cond-> {:workload :assistant}
                         provider-id
                         (assoc :provider-id provider-id)))
                      base-execution-context (merge tool-context
                                                    request-context
                                                    {:session-id session-id
                                                     :task-id task-id
                                                     :task-turn-id task-turn-id
                                                     :channel channel
                                                     :user-message user-message
                                                     :resource-session-id resource-session-id
                                                     :assistant-provider assistant-provider
                                                     :assistant-provider-id assistant-provider-id})
                      max-tool-rounds* (long (or max-tool-rounds
                                                 (configured-max-tool-rounds)))
                      max-iterations* (long (autonomous/max-iterations))
                      transient-messages* (vec (filter map? transient-messages))
                      initial-wm-message (or working-memory-message
                                             user-message)
                      initial-wm-query-fingerprint (wm-query-signature initial-wm-message)
                      turn-budget-state (or *turn-llm-budget-state*
                                            (atom (task-policy/new-turn-llm-budget session-id
                                                                                   channel)))]
                  (wm/set-autonomy-state! session-id initial-autonomy-state)
                  (binding [*turn-llm-budget-state* turn-budget-state
                            llm/*request-budget-guard* (fn [_request]
                                                         (task-policy/throw-if-turn-llm-budget-exhausted!
                                                          turn-budget-state))
                            llm/*request-observer* (fn [request]
                                                     (task-policy/record-turn-llm-request!
                                                      turn-budget-state
                                                      request))]
                    (loop [iteration 1
                           fact-eids []
                           loop-state nil
                           refresh-working-memory? false
                           system-prompt-cache-entry nil
                           wm-message initial-wm-message
                           wm-query-fingerprint initial-wm-query-fingerprint]
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
                            update-working-memory? (= iteration 1)]
                        (save-schedule-checkpoint!
                         iteration-context
                         {:phase :understanding
                          :iteration iteration
                          :summary (if (= iteration 1)
                                     "Understanding the goal and preparing the first plan."
                                     "Resuming the autonomous loop with the updated plan.")
                          :session-id session-id})
                        (let [{:keys [response parsed-response used-fact-eids tool-activity refresh-needed?
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
                                  (if (= :turn-budget-exhausted (:type (ex-data e)))
                                    {:budget-exhausted? true
                                     :budget-status (select-keys (ex-data e)
                                                                 [:kind :session-id :channel
                                                                  :llm-call-count :total-tokens
                                                                  :prompt-tokens :completion-tokens
                                                                  :elapsed-ms :max-llm-calls
                                                                  :max-total-tokens :max-wall-clock-ms])}
                                    (throw e))))
                              parsed parsed-response
                              control (:control parsed)
                              summary (autonomous-iteration-summary parsed)
                              fact-eids* (merge-fact-eids fact-eids used-fact-eids)
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
                                              (turn-budget-note budget-status
                                                                parsed
                                                                updated-autonomy-state
                                                                :before-tools? budget-before-tools?))]
                              (task-runtime/record-task-item! task-turn-id
                                                              {:type :system-note
                                                               :status :limit
                                                               :summary (str "Turn budget exhausted: "
                                                                             (task-policy/turn-llm-budget-summary budget-status))
                                                               :data {:kind "budget-exhausted"
                                                                      :budget-kind (some-> (:kind budget-status) name)
                                                                      :llm-call-count (:llm-call-count budget-status)
                                                                      :total-tokens (:total-tokens budget-status)
                                                                      :elapsed-ms (:elapsed-ms budget-status)
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
                              (when-not (str/blank? text)
                                (launch-fact-utility-review-without-budget! session-id
                                                                            fact-eids*
                                                                            user-message
                                                                            text))
                              (save-schedule-checkpoint!
                               iteration-context
                               {:phase :complete
                                :iteration iteration
                                :summary (or (truncate-summary final-text 500)
                                             "Stopped after reaching the cumulative turn budget.")
                                :session-id session-id
                                :status :turn-budget
                                :budget-kind (:kind budget-status)
                                :llm-call-count (:llm-call-count budget-status)
                                :total-tokens (:total-tokens budget-status)
                                :elapsed-ms (:elapsed-ms budget-status)
                                :next-step (turn-budget-next-step parsed updated-autonomy-state)
                                :progress-status (some-> updated-tip :progress-status)
                                :agenda (some-> updated-tip :agenda)
                                :stack (some-> updated-autonomy-state :stack)})
                              (prompt/status! (merge {:state :done
                                                      :phase :complete
                                                      :message (str "Paused after reaching the "
                                                                    (task-policy/turn-llm-budget-summary budget-status))}
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
                              (when (clear-autonomy-state-on-terminal? parsed)
                                (wm/clear-autonomy-state! session-id)
                                (wm/snapshot! session-id))
                              (launch-fact-utility-review-without-budget! session-id
                                                                          fact-eids*
                                                                          user-message
                                                                          text)
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
                              (sync-runtime-task-turn! task-turn-id
                                                       {:state :completed
                                                        :summary (truncate-summary final-text 500)})
                              (sync-runtime-task! task-id
                                                  {:state :resumable
                                                   :summary (truncate-summary final-text 500)
                                                   :autonomy-state updated-autonomy-state})
                              (launch-fact-utility-review-without-budget! session-id
                                                                          fact-eids*
                                                                          user-message
                                                                          text)
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
                                     next-loop-state
                                     next-refresh-working-memory?
                                     system-prompt-cache-entry
                                     next-wm-message
                                     next-wm-query-fingerprint))))))))
                (catch InterruptedException e
                  (let [{:keys [task-id task-turn-id]} @runtime-task
                        outcome (task-cancellation-outcome "request interrupted")]
                    (sync-runtime-task-turn! task-turn-id
                                             {:state (:turn-state outcome)
                                              :summary (:summary outcome)
                                              :error (some-> e .getMessage)})
                    (sync-runtime-task! task-id
                                        {:state (:task-state outcome)
                                         :stop-reason (:stop-reason outcome)
                                         :summary (:summary outcome)
                                         :autonomy-state (wm/autonomy-state session-id)
                                         :finished-at (java.util.Date.)}))
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
                                     :grace-ms (task-policy/supervisor-restart-grace-ms)}
                                    e))))
                (catch clojure.lang.ExceptionInfo e
                  (let [data (ex-data e)]
                    (cond
                      (= :request-cancelled (:type data))
                      (do
                        (let [reason (:reason data)
                              outcome (task-cancellation-outcome reason)
                              {:keys [task-id task-turn-id]} @runtime-task]
                          (sync-runtime-task-turn! task-turn-id
                                                   {:state (:turn-state outcome)
                                                    :summary (:summary outcome)
                                                    :error (.getMessage e)})
                          (sync-runtime-task! task-id
                                              {:state (:task-state outcome)
                                               :stop-reason (:stop-reason outcome)
                                               :summary (:summary outcome)
                                               :autonomy-state (wm/autonomy-state session-id)
                                               :finished-at (java.util.Date.)}))
                        (save-schedule-checkpoint! request-context
                                                   {:phase :cancelled
                                                    :summary (or (:summary (task-cancellation-outcome (:reason data)))
                                                                 "Request cancelled")
                                                    :session-id session-id})
                        (prompt/status! {:state (if (= :paused (:task-state (task-cancellation-outcome (:reason data))))
                                                  :paused
                                                  :cancelled)
                                         :phase (if (= :paused (:task-state (task-cancellation-outcome (:reason data))))
                                                  :paused
                                                  :cancelled)
                                         :message (:summary (task-cancellation-outcome (:reason data)))})
                        (throw e))

                      (contains? #{:agent-stalled :autonomous-loop-stalled :agent-stop-timeout} (:type data))
                      (do
                        (let [{:keys [task-id task-turn-id]} @runtime-task]
                          (sync-runtime-task-turn! task-turn-id
                                                   {:state :failed
                                                    :summary (.getMessage e)
                                                    :error (.getMessage e)})
                          (sync-runtime-task! task-id
                                              {:state :failed
                                               :stop-reason :stalled
                                               :summary (.getMessage e)
                                               :error (.getMessage e)
                                               :autonomy-state (wm/autonomy-state session-id)
                                               :finished-at (java.util.Date.)}))
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
                        (let [{:keys [task-id task-turn-id]} @runtime-task]
                          (sync-runtime-task-turn! task-turn-id
                                                   {:state :failed
                                                    :summary (.getMessage e)
                                                    :error (.getMessage e)})
                          (sync-runtime-task! task-id
                                              {:state :failed
                                               :stop-reason :error
                                               :summary (.getMessage e)
                                               :error (.getMessage e)
                                               :autonomy-state (wm/autonomy-state session-id)
                                               :finished-at (java.util.Date.)}))
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
                      (let [reason (:reason (ex-data cancel-ex))
                            outcome (task-cancellation-outcome reason)
                            {:keys [task-id task-turn-id]} @runtime-task]
                        (sync-runtime-task-turn! task-turn-id
                                                 {:state (:turn-state outcome)
                                                  :summary (:summary outcome)
                                                  :error (.getMessage cancel-ex)})
                        (sync-runtime-task! task-id
                                            {:state (:task-state outcome)
                                             :stop-reason (:stop-reason outcome)
                                             :summary (:summary outcome)
                                             :autonomy-state (wm/autonomy-state session-id)
                                             :finished-at (java.util.Date.)}))
                      (save-schedule-checkpoint! request-context
                                                 {:phase :cancelled
                                                  :summary (:summary (task-cancellation-outcome (:reason (ex-data cancel-ex))))
                                                  :session-id session-id})
                      (prompt/status! {:state (if (= :paused (:task-state (task-cancellation-outcome (:reason (ex-data cancel-ex)))))
                                                :paused
                                                :cancelled)
                                       :phase (if (= :paused (:task-state (task-cancellation-outcome (:reason (ex-data cancel-ex)))))
                                                :paused
                                                :cancelled)
                                       :message (:summary (task-cancellation-outcome (:reason (ex-data cancel-ex))))})
                      (throw cancel-ex))
                    (do
                      (let [{:keys [task-id task-turn-id]} @runtime-task]
                        (sync-runtime-task-turn! task-turn-id
                                                 {:state :failed
                                                  :summary (.getMessage e)
                                                  :error (.getMessage e)})
                        (sync-runtime-task! task-id
                                            {:state :failed
                                             :stop-reason :error
                                             :summary (.getMessage e)
                                             :error (.getMessage e)
                                             :autonomy-state (wm/autonomy-state session-id)
                                             :finished-at (java.util.Date.)}))
                      (save-schedule-checkpoint! request-context
                                                 {:phase :error
                                                  :summary (.getMessage e)
                                                  :session-id session-id})
                      (prompt/status! {:state :error
                                       :phase :error
                                       :message (str "Request failed: " (.getMessage e))})
                      (throw e))))
                (finally
                  (when-let [{:keys [task-id task-turn-id task-run-id]} @runtime-task]
                    (clear-task-run! session-id task-id task-turn-id task-run-id)))))))))))

(defn- task-control-deps
  []
  (merge (task-runtime-deps)
         {:cancel-session! cancel-session!
          :process-message process-message
          :register-child-session! register-child-session!
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

(defn- branch-deps
  []
  {:await-futures! await-futures!
   :branch-task-timeout-ms branch-task-timeout-ms
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
