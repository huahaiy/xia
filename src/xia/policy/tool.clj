(ns xia.policy.tool
  "Tool, permission, and sandbox policy."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xia.config :as cfg]))

(def ^:private default-tool-sci-eval-timeout-ms 10000)
(def ^:private default-tool-sci-handler-timeout-ms 120000)
(def ^:private default-tool-max-active-sci-workers 32)
(def ^:private default-tool-pipeline-timeout-ms 120000)
(def ^:private default-tool-pipeline-max-calls 8)
(def ^:private default-tool-pipeline-max-code-chars 12000)

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

(def ^:private first-party-handler-policy-targets
  {"xia.tool.builtin/artifact-create" ["xia.artifact/create-artifact!"]
   "xia.tool.builtin/artifact-delete" ["xia.artifact/delete-artifact!"]
   "xia.tool.builtin/branch-tasks" ["xia.agent/run-branch-tasks"]
   "xia.tool.builtin/browser-click" ["xia.browser/click"]
   "xia.tool.builtin/browser-fill-form" ["xia.browser/fill-form"]
   "xia.tool.builtin/browser-login" ["xia.browser/login"]
   "xia.tool.builtin/browser-login-interactive" ["xia.browser/login-interactive"]
   "xia.tool.builtin/browser-navigate" ["xia.browser/navigate"]
   "xia.tool.builtin/browser-open" ["xia.browser/open-session"]
   "xia.tool.builtin/browser-query-elements" ["xia.browser/query-elements"]
   "xia.tool.builtin/browser-read-page" ["xia.browser/read-page"]
   "xia.tool.builtin/browser-screenshot" ["xia.browser/screenshot"]
   "xia.tool.builtin/browser-wait" ["xia.browser/wait-for-page"]
   "xia.tool.builtin/email-delete" ["xia.email/"]
   "xia.tool.builtin/email-draft-delete" ["xia.email/"]
   "xia.tool.builtin/email-draft-list" ["xia.email/"]
   "xia.tool.builtin/email-draft-read" ["xia.email/"]
   "xia.tool.builtin/email-draft-save" ["xia.email/"]
   "xia.tool.builtin/email-draft-send" ["xia.email/"]
   "xia.tool.builtin/email-label-list" ["xia.email/"]
   "xia.tool.builtin/email-list" ["xia.email/"]
   "xia.tool.builtin/email-read" ["xia.email/"]
   "xia.tool.builtin/email-send" ["xia.email/"]
   "xia.tool.builtin/email-update" ["xia.email/"]
   "xia.tool.builtin/peer-chat" ["xia.peer/chat"]
   "xia.tool.builtin/peer-instance-list" ["xia.instance-supervisor/"]
   "xia.tool.builtin/peer-instance-start" ["xia.instance-supervisor/"]
   "xia.tool.builtin/peer-instance-status" ["xia.instance-supervisor/"]
   "xia.tool.builtin/peer-instance-stop" ["xia.instance-supervisor/"]
   "xia.tool.builtin/schedule-create" ["xia.schedule/create-schedule!"]
   "xia.tool.builtin/schedule-manage" ["xia.schedule/pause-schedule!"
                                       "xia.schedule/resume-schedule!"
                                       "xia.schedule/remove-schedule!"]})

(defn tool-id
  [tool]
  (or (:tool/id tool) (:id tool)))

(defn tool-name
  [tool]
  (or (:tool/name tool)
      (:name tool)
      (some-> (tool-id tool) name)
      "unknown-tool"))

(defn tool-description
  [tool]
  (or (:tool/description tool)
      (:description tool)))

(defn tool-channel-compatible?
  [tool context]
  (case (tool-id tool)
    :browser-login-interactive
    (= :terminal (or (:channel context) :terminal))
    true))

(defn tool-channel-block-message
  [tool]
  (case (tool-id tool)
    :browser-login-interactive "interactive login is only available in terminal sessions"
    (str "tool " (name (tool-id tool)) " is not available on this channel")))

(defn- tool-requires-vision?
  [tool]
  (contains? (set (:tool/tags tool)) :vision))

(defn- vision-capable-provider?
  [policy-env provider]
  (when-let [vision-capable? (:vision-capable? policy-env)]
    (boolean (vision-capable? provider))))

(defn tool-vision-compatible?
  ([tool context]
   (tool-vision-compatible? tool context {}))
  ([tool context policy-env]
   (boolean
    (or (not (tool-requires-vision? tool))
        (and (nil? (:assistant-provider context))
             (nil? (:assistant-provider-id context)))
        (vision-capable-provider? policy-env (:assistant-provider context))
        (vision-capable-provider? policy-env (:assistant-provider-id context))))))

(defn tool-vision-block-message
  [_tool]
  "requires a vision-capable model")

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

(defn- tool-handler-match-text
  [tool]
  (let [handler (or (:tool/handler tool) (:handler tool))
        handler-var (or (:tool/handler-var tool) (:handler-var tool))
        handler-var-text (when (some? handler-var) (str handler-var))]
    (str/join "\n"
              (remove str/blank?
                      (concat [(str handler)
                               handler-var-text]
                              (get first-party-handler-policy-targets
                                   handler-var-text))))))

(defn matching-privileged-rules
  [tool]
  (let [handler (tool-handler-match-text tool)]
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
           (name (tool-id tool)))

      (some (complement autonomous-supported-scope?) scopes)
      (str "trusted autonomous execution is not allowed for tool "
           (name (tool-id tool)))

      (= unavailable [:service])
      "no approved services are available for autonomous execution"

      (= unavailable [:site])
      "no approved site accounts are available for autonomous execution"

      (seq unavailable)
      "required services or site accounts are not approved for autonomous execution"

      :else
      (str "trusted autonomous execution is not allowed for tool "
           (name (tool-id tool))))))

(defn branch-worker-tool-allowed?
  [tool approval-decision]
  (and (= :auto (:policy approval-decision))
       (not (contains? branch-worker-blocked-tool-ids
                       (tool-id tool)))))

(defn- context-autonomous-run?
  [context policy-env]
  (if-let [autonomous-run? (:autonomous-run? policy-env)]
    (boolean (autonomous-run? context))
    (true? (:autonomous-run? context))))

(defn- context-trusted?
  [context policy-env]
  (if-let [trusted? (:trusted? policy-env)]
    (boolean (trusted? context))
    (and (context-autonomous-run? context policy-env)
         (true? (:approval-bypass? context)))))

(defn- context-scope-available?
  [policy-env]
  (or (:scope-available? policy-env)
      (constantly false)))

(defn tool-autonomous-allowed?
  ([tool context]
   (tool-autonomous-allowed? tool context {}))
  ([tool context policy-env]
   (autonomous-tool-allowed? tool
                             (context-trusted? context policy-env)
                             (context-scope-available? policy-env))))

(defn tool-autonomous-block-message
  ([tool context]
   (tool-autonomous-block-message tool context {}))
  ([tool context policy-env]
   (autonomous-tool-block-message tool
                                  (context-trusted? context policy-env)
                                  (context-scope-available? policy-env))))

(defn tool-execution-policy-context
  ([tool context]
   (tool-execution-policy-context tool context {}))
  ([tool context policy-env]
   (let [id                (tool-id tool)
         approval-decision (tool-approval-policy tool)
         branch-worker?    (:branch-worker? context)]
     {:tool-id id
      :tool-name (tool-name tool)
      :channel-compatible? (tool-channel-compatible? tool context)
      :channel-error (tool-channel-block-message tool)
      :vision-compatible? (tool-vision-compatible? tool context policy-env)
      :vision-error (tool-vision-block-message tool)
      :branch-worker? branch-worker?
      :branch-allowed? (branch-worker-tool-allowed? tool approval-decision)
      :branch-error (str "tool " (name id)
                         " is not available to branch workers")
      :approval-policy-decision approval-decision})))

(defn tool-visible?
  ([tool context]
   (tool-visible? tool context {}))
  ([tool context policy-env]
   (let [{:keys [channel-compatible?
                 vision-compatible?
                 branch-worker?
                 branch-allowed?
                 approval-policy-decision]}
         (tool-execution-policy-context tool context policy-env)
         {:keys [policy]} approval-policy-decision]
     (and channel-compatible?
          vision-compatible?
          (cond
            branch-worker? branch-allowed?
            (context-autonomous-run? context policy-env)
            (or (= :auto policy)
                (tool-autonomous-allowed? tool context policy-env))
            :else true)))))

(defn tool-description-for-llm
  ([tool context approval-note]
   (tool-description-for-llm tool context approval-note {}))
  ([tool context approval-note policy-env]
   (let [{:keys [policy]} (tool-approval-policy tool)
         desc             (tool-description tool)]
     (if (or (= :auto policy)
             (tool-autonomous-allowed? tool context policy-env))
       desc
       (str desc approval-note)))))

(defn tool-autonomous-approval-decision
  ([tool approval-decision context]
   (tool-autonomous-approval-decision tool approval-decision context {}))
  ([tool approval-decision context policy-env]
   (let [{:keys [policy reason]} approval-decision
         autonomous?       (context-autonomous-run? context policy-env)
         autonomous-allowed? (tool-autonomous-allowed? tool context policy-env)]
     (cond
       autonomous-allowed?
       {:allowed? true
        :policy   policy
        :mode     :autonomous-bypass
        :reason   reason}

       (and autonomous? (not= :auto policy))
       {:allowed? false
        :policy   policy
        :mode     :autonomous-blocked
        :reason   reason
        :error    (tool-autonomous-block-message tool context policy-env)}))))

(defn tool-restart-risk-policy
  [tool approval-decision]
  (let [tool-id (tool-id tool)
        tool-name (tool-name tool)
        tool-tags (set (:tool/tags tool))
        approval-policy (:policy approval-decision)
        handler (tool-handler-match-text tool)
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

(defn tool-preflight-decision
  ([tool context]
   (tool-preflight-decision tool context {}))
  ([tool context policy-env]
   (tool-execution-decision
    (tool-execution-policy-context tool context policy-env))))

(defn tool-execution-decision-for-approval
  ([tool context approval-decision]
   (tool-execution-decision-for-approval tool context approval-decision {}))
  ([tool context approval-decision policy-env]
   (tool-execution-decision
    (assoc (tool-execution-policy-context tool context policy-env)
           :approval-decision approval-decision))))
