(ns xia.task-policy
  "Task policy helpers shared across task runtimes."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xia.config :as cfg]))

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
(def ^:private default-http-max-attempts 3)
(def ^:private default-http-initial-backoff-ms 1000)
(def ^:private default-http-max-backoff-ms 8000)
(def ^:private default-http-retry-statuses #{408 409 425 429 500 502 503 504})
(def ^:private default-http-retry-methods #{:delete :get :head :options :put :trace})
(def ^:private default-llm-retry-statuses #{408 409 425 429 500 502 503 504})
(def ^:private default-max-provider-retry-rounds 4)
(def ^:private default-max-provider-retry-wait-ms 300000)
(def ^:private default-schedule-failure-backoff-minutes 15)
(def ^:private default-schedule-max-failure-backoff-minutes (* 12 60))
(def ^:private default-schedule-pause-after-repeated-failures 3)
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
(def ^:private default-max-schedules 50)
(def ^:private default-min-schedule-interval-minutes 5)
(def ^:private default-scheduler-max-concurrent-runs 4)
(def ^:private default-async-background-max-threads 4)
(def ^:private default-async-background-queue-capacity 256)
(def ^:private default-async-parallel-max-threads
  (max 4 (.availableProcessors (Runtime/getRuntime))))
(def ^:private default-async-parallel-queue-capacity 256)
(def ^:private default-tool-sci-eval-timeout-ms 10000)
(def ^:private default-tool-sci-handler-timeout-ms 120000)
(def ^:private default-tool-max-active-sci-workers 32)
(def ^:private default-tool-pipeline-timeout-ms 120000)
(def ^:private default-tool-pipeline-max-calls 8)
(def ^:private default-tool-pipeline-max-code-chars 12000)
(def ^:private default-plugin-hook-timeout-ms 5000)
(def ^:private default-plugin-hook-max-code-chars 12000)
(def ^:private default-plugin-max-hooks 32)
(def ^:private default-plugin-max-active-workers 16)
(def ^:private default-local-doc-ocr-timeout-ms 120000)
(def ^:private default-local-doc-ocr-max-tokens 2048)
(def ^:private default-browser-playwright-timeout-ms 15000)
 
(def ^:private restart-risk-tool-tags
  #{:branch :cleanup :delete :import :output :publish :write})

(def ^:private restart-risk-handler-rules
  [{:match "xia.agent/run-branch-tasks"
    :mode :branch
    :reason "spawns branch workers that should not be replayed automatically"}
   {:match "xia.artifact/create-artifact!"
    :mode :artifact-create
    :reason "creates a new artifact that could be duplicated on replay"}
   {:match "xia.artifact/delete-artifact!"
    :mode :artifact-delete
    :reason "deletes an artifact and should not be replayed automatically"}])

(def ^:private branch-worker-blocked-tool-ids
  #{:branch-tasks
    :browser-bootstrap-runtime
    :browser-install-deps
    :peer-instance-list
    :peer-instance-start
    :peer-instance-status
    :peer-instance-stop
    :schedule-list
    :schedule-create
    :schedule-manage})

(def ^:private privileged-handler-rules
  [{:match "xia.service/request"
    :policy :session
    :autonomous-scope :service
    :reason "uses stored service credentials"}
   {:match "xia.peer/chat"
    :policy :session
    :autonomous-scope :service
    :reason "communicates with a configured Xia peer through stored service credentials"}
   {:match "xia.instance-supervisor/"
    :policy :session
    :autonomous-scope nil
    :reason "starts or stops managed local Xia instances on the host"}
   {:match "xia.email/"
    :policy :session
    :autonomous-scope :service
    :reason "uses stored email service credentials"}
   {:match "xia.browser/open-session"
    :policy :session
    :session-scope :browser
    :autonomous-scope nil
    :reason "uses live browser automation"}
   {:match "xia.browser/navigate"
    :policy :session
    :session-scope :browser
    :autonomous-scope nil
    :reason "uses live browser automation"}
   {:match "xia.browser/read-page"
    :policy :session
    :session-scope :browser
    :autonomous-scope nil
    :reason "uses live browser automation"}
   {:match "xia.browser/query-elements"
    :policy :session
    :session-scope :browser
    :autonomous-scope nil
    :reason "uses live browser automation"}
   {:match "xia.browser/wait-for-page"
    :policy :session
    :session-scope :browser
    :autonomous-scope nil
    :reason "uses live browser automation"}
   {:match "xia.browser/screenshot"
    :policy :session
    :session-scope :browser
    :autonomous-scope nil
    :reason "uses live browser automation"}
   {:match "xia.browser/login-interactive"
    :policy :session
    :session-scope :browser
    :autonomous-scope nil
    :reason "prompts for interactive credentials"}
   {:match "xia.browser/login"
    :policy :session
    :session-scope :browser
    :autonomous-scope :site
    :reason "uses stored site credentials"}
   {:match "xia.browser/fill-form"
    :policy :session
    :session-scope :browser
    :autonomous-scope nil
    :reason "submits data into live browser sessions"}
   {:match "xia.browser/click"
    :policy :session
    :session-scope :browser
    :autonomous-scope nil
    :reason "can trigger live browser actions"}
   {:match "xia.schedule/create-schedule!"
    :policy :session
    :autonomous-scope nil
    :reason "creates autonomous background tasks"}
   {:match "xia.schedule/update-schedule!"
    :policy :session
    :autonomous-scope nil
    :reason "changes autonomous background tasks"}
   {:match "xia.schedule/remove-schedule!"
    :policy :session
    :autonomous-scope nil
    :reason "changes autonomous background tasks"}
   {:match "xia.schedule/pause-schedule!"
    :policy :session
    :autonomous-scope nil
    :reason "changes autonomous background tasks"}
   {:match "xia.schedule/resume-schedule!"
    :policy :session
    :autonomous-scope nil
    :reason "changes autonomous background tasks"}])

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

(defn schedule-failure-backoff-minutes
  []
  (cfg/positive-long :schedule/failure-backoff-minutes
                     default-schedule-failure-backoff-minutes))

(defn schedule-max-failure-backoff-minutes
  []
  (cfg/positive-long :schedule/max-failure-backoff-minutes
                     default-schedule-max-failure-backoff-minutes))

(defn schedule-pause-after-repeated-failures
  []
  (cfg/positive-long :schedule/pause-after-repeated-failures
                     default-schedule-pause-after-repeated-failures))

(defn llm-max-provider-retry-rounds
  []
  (cfg/positive-long :llm/max-provider-retry-rounds
                     default-max-provider-retry-rounds))

(defn llm-max-provider-retry-wait-ms
  []
  (cfg/positive-long :llm/max-provider-retry-wait-ms
                     default-max-provider-retry-wait-ms))

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

(defn max-schedules
  []
  (cfg/positive-long :schedule/max-schedules
                     default-max-schedules))

(defn min-schedule-interval-minutes
  []
  (cfg/positive-long :schedule/min-interval-minutes
                     default-min-schedule-interval-minutes))

(defn scheduler-max-concurrent-runs
  []
  (cfg/positive-long :scheduler/max-concurrent-runs
                     default-scheduler-max-concurrent-runs))

(defn async-background-max-threads
  []
  (cfg/positive-long :async/background-max-threads
                     default-async-background-max-threads))

(defn async-background-queue-capacity
  []
  (cfg/positive-long :async/background-queue-capacity
                     default-async-background-queue-capacity))

(defn async-parallel-max-threads
  []
  (cfg/positive-long :async/parallel-max-threads
                     default-async-parallel-max-threads))

(defn async-parallel-queue-capacity
  []
  (cfg/positive-long :async/parallel-queue-capacity
                     default-async-parallel-queue-capacity))

(defn tool-sci-eval-timeout-ms
  []
  (cfg/positive-long :tool/sci-eval-timeout-ms
                     default-tool-sci-eval-timeout-ms))

(defn tool-sci-handler-timeout-ms
  []
  (cfg/positive-long :tool/sci-handler-timeout-ms
                     default-tool-sci-handler-timeout-ms))

(defn tool-max-active-sci-workers
  []
  (cfg/positive-long :tool/max-active-sci-workers
                     default-tool-max-active-sci-workers))

(defn tool-pipeline-timeout-ms
  []
  (cfg/positive-long :tool/pipeline-timeout-ms
                     default-tool-pipeline-timeout-ms))

(defn tool-pipeline-max-calls
  []
  (cfg/positive-long :tool/pipeline-max-calls
                     default-tool-pipeline-max-calls))

(defn tool-pipeline-max-code-chars
  []
  (cfg/positive-long :tool/pipeline-max-code-chars
                     default-tool-pipeline-max-code-chars))

(defn plugin-hook-timeout-ms
  []
  (cfg/positive-long :plugin/hook-timeout-ms
                     default-plugin-hook-timeout-ms))

(defn plugin-hook-max-code-chars
  []
  (cfg/positive-long :plugin/hook-max-code-chars
                     default-plugin-hook-max-code-chars))

(defn plugin-max-hooks
  []
  (cfg/positive-long :plugin/max-hooks
                     default-plugin-max-hooks))

(defn plugin-max-active-workers
  []
  (cfg/positive-long :plugin/max-active-workers
                     default-plugin-max-active-workers))

(defn local-doc-ocr-timeout-ms
  []
  (cfg/positive-long :local-doc/ocr-timeout-ms
                     default-local-doc-ocr-timeout-ms))

(defn local-doc-ocr-max-tokens
  []
  (cfg/positive-long :local-doc/ocr-max-tokens
                     default-local-doc-ocr-max-tokens))

(defn browser-playwright-timeout-ms
  []
  (cfg/positive-long :browser/playwright-timeout-ms
                     default-browser-playwright-timeout-ms))

(defn http-request-retry-config
  [req]
  {:max-attempts (long (or (:max-attempts req)
                           default-http-max-attempts))
   :initial-backoff-ms (long (or (:initial-backoff-ms req)
                                 default-http-initial-backoff-ms))
   :max-backoff-ms (long (or (:max-backoff-ms req)
                             default-http-max-backoff-ms))
   :retry-statuses (or (:retry-statuses req)
                       default-http-retry-statuses)
   :retry-methods (or (:retry-methods req)
                      default-http-retry-methods)})

(defn normalize-approval-policy
  [approval]
  (case (cond
          (keyword? approval) approval
          (string? approval) (keyword approval)
          :else :auto)
    :session :session
    :always :always
    :auto :auto
    :auto))

(defn matching-privileged-rules
  [tool]
  (let [handler (or (:tool/handler tool) (:handler tool) "")]
    (filterv (fn [{:keys [match]}]
               (str/includes? handler match))
             privileged-handler-rules)))

(defn inferred-tool-approval-policy
  [tool]
  (or (first (matching-privileged-rules tool))
      {:policy :auto}))

(defn tool-approval-policy
  ([tool]
   (tool-approval-policy tool (inferred-tool-approval-policy tool)))
  ([tool inferred-decision]
   (let [approval (or (:tool/approval tool) (:approval tool))
         explicit-decision (when approval
                             {:policy (normalize-approval-policy approval)})]
     (assoc (merge inferred-decision explicit-decision)
            :policy (normalize-approval-policy
                     (or (:policy explicit-decision)
                         (:policy inferred-decision)))))))

(defn tool-autonomous-scopes
  [tool]
  (->> (matching-privileged-rules tool)
       (map :autonomous-scope)
       set))

(defn- autonomous-supported-scope?
  [scope]
  (contains? #{:service :site} scope))

(defn autonomous-tool-allowed?
  [tool trusted? scope-available?]
  (let [scopes (tool-autonomous-scopes tool)]
    (and trusted?
         (seq scopes)
         (every? autonomous-supported-scope? scopes)
         (every? scope-available? scopes))))

(defn autonomous-tool-block-message
  [tool trusted? scope-available?]
  (let [scopes (tool-autonomous-scopes tool)
        unavailable (->> scopes
                         (filter autonomous-supported-scope?)
                         (remove scope-available?)
                         vec)]
    (cond
      (not trusted?)
      "tool requires live approval and is unavailable during autonomous execution"

      (empty? scopes)
      (str "trusted autonomous execution is not allowed for tool "
           (name (:tool/id tool)))

      (some (complement autonomous-supported-scope?) scopes)
      (str "trusted autonomous execution is not allowed for tool "
           (name (:tool/id tool)))

      (= unavailable [:service])
      "no approved services are available for autonomous execution"

      (= unavailable [:site])
      "no approved site accounts are available for autonomous execution"

      (seq unavailable)
      "required services or site accounts are not approved for autonomous execution"

      :else
      (str "trusted autonomous execution is not allowed for tool "
           (name (:tool/id tool))))))

(defn branch-worker-tool-allowed?
  [tool approval-decision]
  (and (= :auto (:policy approval-decision))
       (not (contains? branch-worker-blocked-tool-ids
                       (:tool/id tool)))))

(defn tool-restart-risk-policy
  [tool approval-decision]
  (let [tool-id (:tool/id tool)
        tool-name (or (:tool/name tool)
                      (some-> tool-id name)
                      "unknown-tool")
        tool-tags (set (:tool/tags tool))
        approval-policy (:policy approval-decision)
        handler (or (:tool/handler tool) (:handler tool) "")
        handler-rule (some (fn [{:keys [match] :as rule}]
                             (when (str/includes? handler match)
                               rule))
                           restart-risk-handler-rules)
        risky-tags (seq (sort (set/intersection restart-risk-tool-tags tool-tags)))
        tool-risk? (or (not= :auto approval-policy)
                       (some? handler-rule)
                       (seq risky-tags))
        mode (cond
               (not= :auto approval-policy) :approval-gated
               handler-rule (:mode handler-rule)
               (seq risky-tags) :stateful-tag
               :else :read-only)
        reason (cond
                 (not= :auto approval-policy)
                 "uses approval-gated or privileged effects that should not be replayed automatically"

                 handler-rule
                 (:reason handler-rule)

                 (seq risky-tags)
                 (str "tool carries stateful tags: "
                      (str/join ", " (map name risky-tags)))

                 :else
                 "tool is treated as restart-safe")]
    {:decision-type :tool-restart-risk-policy
     :tool-id tool-id
     :tool-name tool-name
     :tool-risk? (boolean tool-risk?)
     :mode mode
     :policy approval-policy
     :tags (vec risky-tags)
     :reason reason}))

(defn tool-execution-decision
  [{:keys [tool-id tool-name
           channel-compatible? channel-error
           vision-compatible? vision-error
           branch-worker? branch-allowed? branch-error
           approval-decision]}]
  (cond
    (false? channel-compatible?)
    {:decision-type :execution-policy
     :tool-id tool-id
     :tool-name tool-name
     :allowed? false
     :policy :channel
     :mode :channel-blocked
     :error channel-error}

    (false? vision-compatible?)
    {:decision-type :execution-policy
     :tool-id tool-id
     :tool-name tool-name
     :allowed? false
     :policy :vision
     :mode :vision-blocked
     :error vision-error}

    (and branch-worker? (false? branch-allowed?))
    {:decision-type :execution-policy
     :tool-id tool-id
     :tool-name tool-name
     :allowed? false
     :policy :branch
     :mode :branch-blocked
     :error branch-error}

    approval-decision
    (assoc approval-decision
           :decision-type :execution-policy
           :tool-id tool-id
           :tool-name tool-name)

    :else
    {:decision-type :execution-policy
     :tool-id tool-id
     :tool-name tool-name
     :allowed? true
     :policy :auto
     :mode :not-required}))

(defn http-request-retry-enabled?
  [{:keys [method retry-enabled? retry-methods]}]
  (if (some? retry-enabled?)
    retry-enabled?
    (contains? (or retry-methods default-http-retry-methods)
               (or method :get))))

(defn http-request-backoff-ms
  [attempt initial-backoff-ms max-backoff-ms]
  (min (long max-backoff-ms)
       (* (long initial-backoff-ms)
          (bit-shift-left 1 (dec (long attempt))))))

(defn http-request-retry-decision
  [req attempt {:keys [status transient-exception? reason]}]
  (let [{:keys [max-attempts initial-backoff-ms max-backoff-ms retry-statuses retry-methods]}
        (http-request-retry-config req)
        retry-enabled? (http-request-retry-enabled? {:method (:method req)
                                                     :retry-enabled? (:retry-enabled? req)
                                                     :retry-methods retry-methods})
        allowed? (and retry-enabled?
                      (< (long attempt) max-attempts)
                      (or transient-exception?
                          (contains? retry-statuses status)))
        mode (cond
               allowed? (if transient-exception?
                          :transient-exception
                          :transient-status)
               (not retry-enabled?) :retry-disabled
               (>= (long attempt) max-attempts) :attempt-limit
               status :permanent-status
               :else :not-retryable)]
    {:allowed? allowed?
     :mode mode
     :attempt (long attempt)
     :max-attempts max-attempts
     :status status
     :reason reason
     :delay-ms (when allowed?
                 (long (http-request-backoff-ms attempt
                                                initial-backoff-ms
                                                max-backoff-ms)))}))

(defn llm-retry-after-ms
  [headers now-ms-fn]
  (when-let [raw (some-> (or (get headers "retry-after")
                             (get headers "Retry-After"))
                         str)]
    (let [value (str/trim raw)]
      (or (try
            (* 1000 (max 0 (Long/parseLong value)))
            (catch Exception _
              nil))
          (try
            (max 0
                 (- (.toEpochMilli (.toInstant (java.time.ZonedDateTime/parse value
                                                                                java.time.format.DateTimeFormatter/RFC_1123_DATE_TIME)))
                    (long (now-ms-fn))))
            (catch Exception _
              nil))))))

(defn llm-retryable-error?
  [^Throwable e]
  (let [status (some-> e ex-data :status)]
    (or (contains? default-llm-retry-statuses status)
        (boolean
         (some #(instance? Throwable %)
               (filter (fn [cause]
                         (or (instance? java.util.concurrent.TimeoutException cause)
                             (instance? java.net.http.HttpTimeoutException cause)
                             (instance? java.net.http.HttpConnectTimeoutException cause)
                             (instance? java.io.IOException cause)))
                       (take-while some? (iterate ex-cause e))))))))

(defn llm-retry-sleep-ms
  [started-at round max-retry-rounds max-retry-wait-ms requested-delay-ms now-ms-fn]
  (let [remaining-ms (- (long max-retry-wait-ms)
                        (- (long (now-ms-fn)) (long started-at)))]
    (when (and (< (long round) (long max-retry-rounds))
               (pos? remaining-ms)
               (pos? (long (or requested-delay-ms 0))))
      (min remaining-ms (long requested-delay-ms)))))

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

(defn schedule-frequency-policy
  [{:keys [interval-minutes spec]}]
  (let [minimum (long (min-schedule-interval-minutes))]
    (cond
      (some? interval-minutes)
      {:decision-type :schedule-frequency-policy
       :allowed? false
       :mode :interval-limit
       :interval-minutes (long interval-minutes)
       :min-interval-minutes minimum
       :reason (str "Interval too frequent (minimum " minimum " minutes)")}

      :else
      {:decision-type :schedule-frequency-policy
       :allowed? false
       :mode :calendar-frequency
       :spec spec
       :min-interval-minutes minimum
       :reason (str "Schedule too frequent (minimum " minimum " minutes)")})))

(defn schedule-count-policy
  [current-count]
  (let [current-count (long current-count)
        max-schedules (long (max-schedules))
        allowed? (< current-count max-schedules)]
    {:decision-type :schedule-count-policy
     :allowed? allowed?
     :mode (if allowed? :within-limit :schedule-limit)
     :current-count current-count
     :max-schedules max-schedules
     :reason (when-not allowed?
               (str "Too many schedules (max " max-schedules ")"))}))

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

(defn schedule-failure-backoff-ms
  ^long
  [consecutive-failures]
  (* 60 1000
     (min (long (schedule-max-failure-backoff-minutes))
          (* (long (schedule-failure-backoff-minutes))
             (long (Math/pow 2.0 (double (max 0 (dec (long consecutive-failures))))))))))

(defn schedule-failure-policy
  [{:keys [same-failure? previous-failures now]}]
  (let [previous-failures (long (or previous-failures 0))
        consecutive-failures (if same-failure?
                               (inc previous-failures)
                               1)
        pause-threshold (long (schedule-pause-after-repeated-failures))
        paused? (and same-failure?
                     (>= consecutive-failures pause-threshold))
        backoff-ms (when-not paused?
                     (long (schedule-failure-backoff-ms consecutive-failures)))
        backoff-until (when backoff-ms
                        (java.util.Date.
                         (long (+ (.getTime ^java.util.Date now) backoff-ms))))]
    {:decision-type :schedule-failure-policy
     :mode (if paused? :pause :backoff)
     :same-failure? (boolean same-failure?)
     :consecutive-failures consecutive-failures
     :pause-threshold pause-threshold
     :backoff-ms backoff-ms
     :backoff-minutes (when backoff-ms
                        (long (/ backoff-ms 60000)))
     :max-backoff-minutes (long (schedule-max-failure-backoff-minutes))
     :backoff-until backoff-until
     :reason (if paused?
               "Paused after repeated identical schedule failures"
               "Applied schedule failure backoff")})) 

(defn provider-rate-limit-policy
  [provider-id workload limit]
  {:decision-type :provider-rate-limit-policy
   :allowed? false
   :mode :rate-limit
   :provider-id provider-id
   :workload workload
   :limit (long limit)
   :reason (str "Rate limit exceeded for provider " (name provider-id)
                " (max " (long limit) " requests/minute)")})

(defn service-rate-limit-policy
  [service-id limit]
  {:decision-type :service-rate-limit-policy
   :allowed? false
   :mode :rate-limit
   :service-id service-id
   :limit (long limit)
   :reason (str "Rate limit exceeded for service " (name service-id)
                " (max " (long limit) " requests/minute)")})

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
