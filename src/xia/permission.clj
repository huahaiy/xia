(ns xia.permission
  "Permission decisions for tool execution.

   The permission layer decides whether a tool invocation may proceed. UI
   channels are still handled by xia.prompt, but tool code no longer owns the
   approval/session-grant mechanics."
  (:require [xia.audit :as audit]
            [xia.autonomous.access :as autonomous-access]
            [xia.interaction-context :as interaction-context]
            [xia.llm :as llm]
            [xia.prompt :as prompt]
            [xia.runtime-context :as runtime-context]
            [xia.policy :as task-policy]))

(def ^:private runtime-context-key :xia/permission-runtime)

(defn make-runtime
  []
  {:session-grants (atom {})})

(defn- maybe-current-runtime
  []
  (runtime-context/runtime runtime-context-key))

(defn- current-runtime
  []
  (or (maybe-current-runtime)
      (throw (ex-info "Permission runtime is not installed"
                      {:component :xia/permission-runtime}))))

(defn- session-grants
  []
  (:session-grants (current-runtime)))

(def ^:dynamic *approval-callback*
  "Optional bridge callback for permission prompts.

   The callback receives the approval request map and returns truthy to allow.
   Context may also provide :permission/request-approval! or
   :permission/approval-callback for per-invocation bridging."
  nil)

(defn reset-runtime!
  []
  (reset! (session-grants) {}))

(defn clear-runtime!
  []
  (when-let [runtime (maybe-current-runtime)]
    (reset-runtime!))
  nil)

(defn clear-session-grants!
  [session-id]
  (swap! (session-grants) dissoc session-id))

(defn tool-approval-policy
  [tool]
  (task-policy/tool-approval-policy tool))

(defn- tool-id
  [tool]
  (task-policy/tool-id tool))

(defn- tool-name
  [tool]
  (task-policy/tool-name tool))

(defn- tool-description
  [tool]
  (task-policy/tool-description tool))

(defn- tool-policy-env
  []
  {:vision-capable? llm/vision-capable?
   :autonomous-run? interaction-context/autonomous-run?
   :trusted? interaction-context/trusted?
   :scope-available? autonomous-access/scope-available?})

(defn- approved-for-session?
  [session-id grant-key]
  (contains? (get @(session-grants) session-id #{}) grant-key))

(defn- remember-session-grant!
  [session-id grant-key]
  (when session-id
    (swap! (session-grants) update session-id (fnil conj #{}) grant-key)))

(defn- session-grant-key
  [tool]
  (or (:session-scope (tool-approval-policy tool))
      (tool-id tool)))

(defn- invoke-runtime-hook!
  [context hook-key payload]
  (when-let [f (get context hook-key)]
    (try
      (f payload)
      (catch Throwable _
        nil))))

(defn- approval-request-audit-data
  [req]
  {:tool-name   (:tool-name req)
   :description (:description req)
   :arguments   (:arguments req)
   :policy      (some-> (:policy req) name)
   :reason      (:reason req)})

(defn- approval-decision-audit-data
  [req approved?]
  {:tool-name (:tool-name req)
   :approved  approved?
   :policy    (some-> (:policy req) name)})

(defn- callback-approval!
  [context callback request]
  (let [req (merge {:channel (or (:channel context) :default)}
                   context
                   request)]
    (invoke-runtime-hook! context :task-runtime/on-approval-request req)
    (audit/log! context
                {:actor  :user
                 :type   :approval-request
                 :tool-id (some-> (:tool-id req) name)
                 :data   (approval-request-audit-data req)})
    (let [result    (callback req)
          approved? (boolean (if (map? result)
                               (:approved? result)
                               result))]
      (invoke-runtime-hook! context
                            :task-runtime/on-approval-decision
                            (assoc req :approved? approved?))
      (audit/log! context
                  {:actor  :user
                   :type   :approval-decision
                   :tool-id (some-> (:tool-id req) name)
                   :data   (approval-decision-audit-data req approved?)})
      approved?)))

(defn- approval-callback
  [context]
  (or (:permission/request-approval! context)
      (:permission/approval-callback context)
      *approval-callback*))

(defn request-approval!
  [request context]
  (if-let [callback (approval-callback context)]
    (callback-approval! context callback request)
    (prompt/approve! request)))

(defn- permission-request
  [tool arguments approval-decision]
  (let [{:keys [policy reason]} approval-decision]
    {:tool-id     (tool-id tool)
     :tool-name   (tool-name tool)
     :description (tool-description tool)
     :arguments   arguments
     :policy      policy
     :reason      reason}))

(defn- approval-decision!
  [tool arguments context]
  (let [approval-decision (tool-approval-policy tool)
        {:keys [policy reason]} approval-decision
        id                (tool-id tool)
        name*             (tool-name tool)
        session-id        (:session-id context)
        grant-key         (session-grant-key tool)
        policy-env        (tool-policy-env)
        request           (permission-request tool arguments approval-decision)]
    (letfn [(record-decision [decision]
              (prompt/policy-decision! (assoc decision
                                              :decision-type :approval-policy
                                              :tool-id id
                                              :tool-name name*))
              decision)
            (interactive-decision []
              (try
                (prompt/status! {:state    :waiting
                                 :phase    :approval
                                 :message  (str "Waiting for approval for " name*)
                                 :tool-id  id
                                 :tool-name name*})
                (if (request-approval! request context)
                  (do
                    (remember-session-grant! session-id grant-key)
                    {:allowed? true
                     :policy   policy
                     :mode     :interactive
                     :reason   reason})
                  {:allowed? false
                   :policy   policy
                   :mode     :denied
                   :reason   reason
                   :error    (str "user denied approval for privileged tool "
                                  (name id))})
                (catch Exception e
                  {:allowed? false
                   :policy   policy
                   :mode     :approval-error
                   :reason   reason
                   :error    (.getMessage e)})))]
      (record-decision
       (or (task-policy/tool-autonomous-approval-decision
            tool
            approval-decision
            context
            policy-env)
           (case policy
             :auto
             {:allowed? true
              :policy   policy
              :mode     :not-required
              :reason   reason}

             :session
             (if (and session-id (approved-for-session? session-id grant-key))
               {:allowed? true
                :policy   policy
                :mode     :session-cached
                :reason   reason}
               (interactive-decision))

             :always
             (interactive-decision)

             {:allowed? true
              :policy   policy
              :mode     :not-required
              :reason   reason}))))))

(defn authorize-tool!
  "Return the execution decision for a tool invocation and emit policy events.

   This function is the single permission gate for normal tool execution. It
   checks static execution constraints, approval policy, session grants, and
   autonomous bypass rules before a tool handler can run."
  [tool arguments context]
  (let [policy-env        (tool-policy-env)
        preflight        (task-policy/tool-preflight-decision
                          tool
                          context
                          policy-env)
        approval-decision (when (:allowed? preflight)
                            (approval-decision! tool arguments context))
        execution-decision (if approval-decision
                             (task-policy/tool-execution-decision-for-approval
                              tool
                              context
                              approval-decision
                              policy-env)
                             preflight)]
    (prompt/policy-decision! execution-decision)
    execution-decision))
