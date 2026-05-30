(ns xia.policy.agent
  "Agent, supervisor, branch, and turn-loop policy."
  (:require [xia.config :as cfg]))

(def ^:private default-supervisor-max-identical-iterations 3)
(def ^:private default-supervisor-semantic-loop-threshold 0.88)
(def ^:private default-supervisor-max-restarts 1)
(def ^:private default-supervisor-restart-backoff-ms 100)
(def ^:private default-supervisor-restart-grace-ms 1000)
(def ^:private default-task-restart-loop-limit 3)
(def ^:private default-task-restart-loop-window-ms (* 30 60 1000))
(def ^:private default-autonomous-max-iterations 6)
(def ^:private default-autonomous-max-stack-depth 32)
(def ^:private default-max-tool-rounds 100)
(def ^:private default-max-tool-calls-per-round 12)
(def ^:private default-parallel-tool-timeout-ms 30000)
(def ^:private default-branch-task-timeout-ms 300000)
(def ^:private default-supervisor-phase-timeout-ms 30000)
(def ^:private default-supervisor-llm-timeout-ms 120000)
(def ^:private default-supervisor-tool-timeout-ms 120000)
(def ^:private default-max-user-message-chars 32768)
(def ^:private default-max-user-message-tokens 8000)
(def ^:private default-max-branch-tasks 5)
(def ^:private default-max-parallel-branches 3)
(def ^:private default-max-branch-tool-rounds 5)
(def ^:private default-branch-error-stack-frames 12)
(def ^:private default-llm-status-preview-chars 160)
(def ^:private default-llm-status-update-interval-ms 500)
(def ^:private default-supervisor-tick-ms 250)
(def ^:private default-task-control-wait-ms 10000)

(defn supervisor-max-identical-iterations
  []
  (cfg/positive-long :agent/supervisor-max-identical-iterations
                     default-supervisor-max-identical-iterations))

(defn supervisor-semantic-loop-threshold
  []
  (cfg/positive-double :agent/supervisor-semantic-loop-threshold
                       default-supervisor-semantic-loop-threshold))

(defn supervisor-max-restarts
  []
  (cfg/positive-long :agent/supervisor-max-restarts
                     default-supervisor-max-restarts))

(defn supervisor-restart-backoff-ms
  []
  (cfg/positive-long :agent/supervisor-restart-backoff-ms
                     default-supervisor-restart-backoff-ms))

(defn supervisor-restart-grace-ms
  []
  (cfg/positive-long :agent/supervisor-restart-grace-ms
                     default-supervisor-restart-grace-ms))

(defn task-restart-loop-limit
  []
  (cfg/positive-long :agent/task-restart-loop-limit
                     default-task-restart-loop-limit))

(defn task-restart-loop-window-ms
  []
  (cfg/positive-long :agent/task-restart-loop-window-ms
                     default-task-restart-loop-window-ms))

(defn autonomous-max-iterations
  []
  (cfg/positive-long :autonomous/max-iterations
                     default-autonomous-max-iterations))

(defn autonomous-max-stack-depth
  []
  (cfg/positive-long :autonomous/max-stack-depth
                     default-autonomous-max-stack-depth))

(defn max-tool-rounds
  []
  (cfg/positive-long :agent/max-tool-rounds
                     default-max-tool-rounds))

(defn max-tool-calls-per-round
  []
  (cfg/positive-long :agent/max-tool-calls-per-round
                     default-max-tool-calls-per-round))

(defn parallel-tool-timeout-ms
  []
  (cfg/positive-long :agent/parallel-tool-timeout-ms
                     default-parallel-tool-timeout-ms))

(defn branch-task-timeout-ms
  []
  (cfg/positive-long :agent/branch-task-timeout-ms
                     default-branch-task-timeout-ms))

(defn supervisor-phase-timeout-ms
  []
  (cfg/positive-long :agent/supervisor-phase-timeout-ms
                     default-supervisor-phase-timeout-ms))

(defn supervisor-llm-timeout-ms
  []
  (cfg/positive-long :agent/supervisor-llm-timeout-ms
                     default-supervisor-llm-timeout-ms))

(defn supervisor-tool-timeout-ms
  []
  (cfg/positive-long :agent/supervisor-tool-timeout-ms
                     default-supervisor-tool-timeout-ms))

(defn supervisor-worker-timeout-ms
  [phase]
  (case phase
    :llm (supervisor-llm-timeout-ms)
    :tool (supervisor-tool-timeout-ms)
    (supervisor-phase-timeout-ms)))

(defn max-user-message-chars
  []
  (cfg/positive-long :agent/max-user-message-chars
                     default-max-user-message-chars))

(defn max-user-message-tokens
  []
  (cfg/positive-long :agent/max-user-message-tokens
                     default-max-user-message-tokens))

(defn max-branch-tasks
  []
  (cfg/positive-long :agent/max-branch-tasks
                     default-max-branch-tasks))

(defn max-parallel-branches
  []
  (cfg/positive-long :agent/max-parallel-branches
                     default-max-parallel-branches))

(defn max-branch-tool-rounds
  []
  (cfg/positive-long :agent/max-branch-tool-rounds
                     default-max-branch-tool-rounds))

(defn branch-error-stack-frames
  []
  (cfg/positive-long :agent/branch-error-stack-frames
                     default-branch-error-stack-frames))

(defn llm-status-preview-chars
  []
  (cfg/positive-long :agent/llm-status-preview-chars
                     default-llm-status-preview-chars))

(defn llm-status-update-interval-ms
  []
  (cfg/positive-long :agent/llm-status-update-interval-ms
                     default-llm-status-update-interval-ms))

(defn supervisor-tick-ms
  []
  (cfg/positive-long :agent/supervisor-tick-ms
                     default-supervisor-tick-ms))

(defn task-control-wait-ms
  []
  (cfg/positive-long :agent/task-control-wait-ms
                     default-task-control-wait-ms))

(defn tool-call-limit-decision
  [tool-count]
  (let [tool-count (long tool-count)
        max-tool-calls-per-round (long (max-tool-calls-per-round))
        allowed? (<= tool-count max-tool-calls-per-round)]
    {:allowed? allowed?
     :mode (if allowed? :within-limit :round-call-limit)
     :tool-count tool-count
     :max-tool-calls-per-round max-tool-calls-per-round
     :reason (when-not allowed?
               (str "Too many tool calls in one round: "
                    tool-count
                    " (max "
                    max-tool-calls-per-round
                    ")"))}))

(defn autonomy-iteration-limit-policy
  [iteration max-iterations]
  {:decision-type :autonomy-iteration-policy
   :allowed? false
   :mode :iteration-limit
   :iteration (long iteration)
   :max-iterations (long max-iterations)
   :reason (str "Reached autonomous iteration limit for this turn ("
                (long iteration)
                "/"
                (long max-iterations)
                ")")})

(defn tool-round-limit-decision
  [round max-tool-rounds]
  (let [rounds (long round)
        max-tool-rounds (long max-tool-rounds)
        allowed? (< rounds max-tool-rounds)]
    {:allowed? allowed?
     :mode (if allowed? :within-limit :round-limit)
     :rounds rounds
     :max-tool-rounds max-tool-rounds
     :reason (when-not allowed?
               "Too many tool-calling rounds")}))

(defn user-message-size-decision
  [char-count token-estimate]
  (let [char-count (long char-count)
        token-estimate (long token-estimate)
        max-chars (long (max-user-message-chars))
        max-tokens (long (max-user-message-tokens))]
    (cond
      (> char-count max-chars)
      {:decision-type :user-message-size-policy
       :allowed? false
       :mode :char-limit
       :char-count char-count
       :max-chars max-chars
       :reason (str "User message too large: "
                    char-count
                    " chars (max "
                    max-chars
                    ")")}

      (> token-estimate max-tokens)
      {:decision-type :user-message-size-policy
       :allowed? false
       :mode :token-limit
       :token-estimate token-estimate
       :max-tokens max-tokens
       :reason (str "User message too large: ~"
                    token-estimate
                    " tokens (max "
                    max-tokens
                    ")")}

      :else
      {:decision-type :user-message-size-policy
       :allowed? true
       :mode :within-limit
       :char-count char-count
       :token-estimate token-estimate
       :max-chars max-chars
       :max-tokens max-tokens})))

(defn branch-task-count-policy
  [task-count max-tasks]
  (let [task-count (long task-count)
        max-tasks (long max-tasks)
        allowed? (<= task-count max-tasks)]
    {:decision-type :branch-task-count-policy
     :allowed? allowed?
     :mode (if allowed? :within-limit :task-limit)
     :task-count task-count
     :max-tasks max-tasks
     :reason (when-not allowed?
               (str "Too many branch tasks: "
                    task-count
                    " (max "
                    max-tasks
                    ")"))}))

(defn parallel-tool-timeout-policy
  [tool-id tool-name timeout-ms]
  {:decision-type :parallel-tool-timeout-policy
   :allowed? false
   :mode :timeout
   :tool-id tool-id
   :tool-name tool-name
   :timeout-ms (long timeout-ms)
   :reason (str "Parallel tool execution timed out: " tool-name)})

(defn branch-task-timeout-policy
  [task prompt timeout-ms]
  (let [task-label (or task prompt "unnamed")]
    {:decision-type :branch-task-timeout-policy
     :allowed? false
     :mode :timeout
     :task task
     :prompt prompt
     :timeout-ms (long timeout-ms)
     :reason (str "Branch task timed out: " task-label)}))

(def ^:private non-restartable-worker-error-types
  #{:request-cancelled
    :autonomous-loop-stalled
    :autonomous-protocol-invalid
    :limit-exhausted
    :agent-stop-timeout
    :tool-round-limit-exceeded
    :tool-call-limit-exceeded
    :user-message-too-large
    :session-busy})

(defn restart-policy-decision
  [t worker-state attempt & {:keys [session-cancelled?
                                    recent-restart-count
                                    recent-restart-limit
                                    restart-window-ms]}]
  (let [failure-type (some-> t ex-data :type)
        next-attempt (inc (long (or attempt 0)))
        max-restarts (long (supervisor-max-restarts))
        recent-restart-count* (long (or recent-restart-count 0))
        recent-restart-limit* (long (or recent-restart-limit
                                        (task-restart-loop-limit)))
        restart-window-ms* (long (or restart-window-ms
                                     (task-restart-loop-window-ms)))
        restart-loop? (>= recent-restart-count* recent-restart-limit*)
        allowed? (cond
                   session-cancelled? false
                   restart-loop? false
                   (:tool-risk? worker-state) false
                   (instance? InterruptedException t) false
                   (contains? non-restartable-worker-error-types failure-type) false
                   (> next-attempt max-restarts) false
                   :else true)
        mode (cond
               session-cancelled? :cancelled
               restart-loop? :restart-loop
               (:tool-risk? worker-state) :tool-risk
               (instance? InterruptedException t) :interrupted
               (contains? non-restartable-worker-error-types failure-type) :non-restartable
               (> next-attempt max-restarts) :restart-limit
               :else :restarting)]
    {:allowed? allowed?
     :mode mode
     :attempt next-attempt
     :max-restarts max-restarts
     :backoff-ms (when allowed?
                   (long (supervisor-restart-backoff-ms)))
     :grace-ms (long (supervisor-restart-grace-ms))
     :recent-restart-count recent-restart-count*
     :recent-restart-limit recent-restart-limit*
     :restart-window-ms restart-window-ms*
     :failure-type failure-type
     :failure-phase (some-> t ex-data :phase)
     :worker-phase (:phase worker-state)
     :tool-risk? (boolean (:tool-risk? worker-state))
     :tool-risk-mode (:tool-risk-mode worker-state)
     :tool-risk-reason (:tool-risk-reason worker-state)
     :tool-id (:tool-id worker-state)
     :tool-name (:tool-name worker-state)
     :round (:round worker-state)}))
