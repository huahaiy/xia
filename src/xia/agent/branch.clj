(ns xia.agent.branch
  "Branch-task orchestration for parallel worker sessions."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.agent.child-task-orchestration :as child-orch]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.policy :as task-policy]
            [xia.task-spec :as task-spec]
            [xia.working-memory :as wm]))

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

(defn- trace-context
  [deps context]
  ((:trace-context deps) context))

(defn run-branch-task*
  [deps parent-session-id {:keys [task prompt] :as branch-task}
   {:keys [channel provider-id resource-session-id objective parent-task-id
           max-tool-rounds tool-context]
    :or {channel :terminal}}]
  ((:throw-if-runtime-stopping! deps) parent-session-id)
  ((:throw-if-cancelled! deps) parent-session-id)
  (let [parent-trace (trace-context deps prompt/*interaction-context*)
        branch-request-id ((:new-request-id deps))
        branch-trace (cond-> (merge parent-trace
                                    {:channel :branch
                                     :request-id branch-request-id
                                     :correlation-id (or (:correlation-id parent-trace)
                                                         (:request-id parent-trace)
                                                         branch-request-id)})
                       (:request-id parent-trace)
                       (assoc :parent-request-id (:request-id parent-trace)))
        prompt* (branch-task-prompt branch-task objective)
        resource-session-id* (or resource-session-id parent-session-id)
        worker (child-orch/create-branch-worker!
                {:parent-session-id parent-session-id
                 :parent-task-id parent-task-id
                 :parent-task (some-> parent-task-id db/get-task)
                 :title task
                 :summary task
                 :prompt prompt
                 :work-prompt prompt*
                 :objective objective
                 :resource-session-id resource-session-id*
                 :session-active? false
                 :state :running
                 :meta {:branch-worker true
                        :parent-session-id parent-session-id
                        :resource-session-id resource-session-id*
                        :objective objective}})
        child-session-id (:session-id worker)
        child-task-id (:task-id worker)
        log-context (merge {:task task
                            :session-id child-session-id
                            :parent-session-id parent-session-id}
                           branch-trace)]
    (child-orch/with-worker-session!
     {:deps deps
      :parent-session-id parent-session-id
      :child-session-id child-session-id
      :log-context log-context
      :deactivate-message "Failed to deactivate branch worker session"
      :clear-autonomy-message "Failed to clear branch worker autonomy state"
      :clear-memory-message "Failed to clear branch worker working memory"
      :clear-autonomy? true}
     (fn []
       (try
         ((:throw-if-runtime-stopping! deps) child-session-id)
         ((:throw-if-cancelled! deps) child-session-id)
         (wm/create-wm! child-session-id)
         (let [tool-context* (child-orch/branch-worker-tool-context
                              parent-session-id
                              resource-session-id*
                              branch-trace
                              tool-context)
               run-result (if-let [run-task-spec! (:run-task-spec! deps)]
                            (run-task-spec! child-task-id
                                            :message prompt*
                                            :channel :branch
                                            :runtime-op :start
                                            :operation :branch-spawn
                                            :provider-id provider-id
                                            :resource-session-id resource-session-id*
                                            :max-tool-rounds max-tool-rounds
                                            :tool-context tool-context*)
                            ((:process-message deps) child-session-id
                             prompt*
                             :channel :branch
                             :task-id child-task-id
                             :runtime-op :start
                             :provider-id provider-id
                             :resource-session-id resource-session-id*
                             :max-tool-rounds max-tool-rounds
                             :tool-context tool-context*))
               wm-context (wm/wm->context child-session-id)]
           (child-orch/normalize-branch-worker-result
            {:trace branch-trace
             :task task
             :child-task-id child-task-id
             :child-session-id child-session-id
             :run-result run-result
             :topics (:topics wm-context)}))
         (catch Throwable t
           (log/error t "Branch task failed" log-context)
           (child-orch/normalize-branch-worker-result
            {:trace branch-trace
             :task task
             :child-task-id child-task-id
             :child-session-id child-session-id
             :run-result {:status :failed}
             :error (.getMessage t)
             :error-detail ((:throwable-detail deps) t)})))))))

(defn- branch-entry-id
  [idx]
  (keyword (str "branch-" idx)))

(defn- branch-worker-step
  [branch-task timeout-ms]
  (cond-> {:id :run-branch
           :kind :llm
           :mode :agent
           :prompt (:prompt branch-task)
           :branch-task branch-task}
    timeout-ms (assoc :timeout-ms timeout-ms)))

(defn- branch-parallel-entry
  [idx branch-task timeout-ms]
  {:id (branch-entry-id idx)
   :spec {:goal (:task branch-task)
          :steps [(branch-worker-step branch-task timeout-ms)]}})

(defn- branch-parallel-spec
  [objective branch-tasks max-parallel timeout-ms]
  {:goal (or (some-> objective str str/trim not-empty)
             "Run branch tasks")
   :steps [{:id :run-branches
            :kind :parallel
            :concurrency max-parallel
            :output-step :run-branch
            :branches (mapv (fn [idx branch-task]
                               (branch-parallel-entry idx branch-task timeout-ms))
                             (range)
                             branch-tasks)}]})

(defn- branch-worker-executor
  [deps parent-session-id
   {:keys [channel provider-id resource-session-id objective parent-task-id
           max-tool-rounds tool-context]}]
  (fn [{:keys [step]}]
    (let [branch-task (:branch-task step)
          result      (run-branch-task* deps
                                        parent-session-id
                                        branch-task
                                        {:channel channel
                                         :provider-id provider-id
                                         :resource-session-id resource-session-id
                                         :parent-task-id parent-task-id
                                         :objective objective
                                         :max-tool-rounds max-tool-rounds
                                         :tool-context tool-context})
          success?    (= "completed" (:status result))]
      (cond-> {:status (if success? :success :failed)
               :summary (or (:summary result)
                            (str "Branch task " (:status result)))
               :output result}
        (not success?)
        (assoc :error (or (:error result)
                          (:summary result)
                          "branch task failed"))))))

(defn- fallback-branch-result
  [branch-task branch-output]
  {:task (:task branch-task)
   :status "failed"
   :task-id (:task-id branch-output)
   :error (or (:error branch-output)
              (:summary branch-output)
              "branch task failed")})

(defn- branch-results-from-parallel-output
  [branch-tasks parallel-output]
  (let [values   (:outputs parallel-output)
        branches (:branches parallel-output)]
    (mapv (fn [idx branch-task]
            (let [entry-id (branch-entry-id idx)
                  value    (get values entry-id)]
              (if (map? value)
                value
                (fallback-branch-result branch-task
                                        (get branches entry-id)))))
          (range)
          branch-tasks)))

(defn run-branch-tasks
  [deps tasks & {:keys [session-id channel provider-id resource-session-id objective
                        max-parallel max-tool-rounds tool-context]
                 :or {channel :terminal
                      tool-context {}}}]
  (let [parent-context prompt/*interaction-context*
        parent-session-id (or session-id (:session-id parent-context))
        channel* (or channel (:channel parent-context) :terminal)
        provider-id* (or provider-id
                         (:assistant-provider-id parent-context))
        parent-task-id (:task-id parent-context)
        resource-session-id* (or resource-session-id parent-session-id)
        branch-tasks (->> tasks (map normalize-branch-task) (remove #(str/blank? (:prompt %))) vec)
        task-count (count branch-tasks)
        _ ((:throw-if-runtime-stopping! deps) parent-session-id)
        _ ((:throw-if-cancelled! deps) parent-session-id)
        max-tasks ((:max-branch-tasks deps))
        max-parallel* (clojure.core/min (clojure.core/max 1 (long (or max-parallel ((:max-parallel-branches deps)))))
                                        (clojure.core/max 1 (long max-tasks)))
        timeout-ms ((:branch-task-timeout-ms deps))]
    (when (zero? task-count)
      (throw (ex-info "Branch tasks require at least one task" {})))
    (let [{:keys [allowed? reason] :as decision}
          (task-policy/branch-task-count-policy task-count max-tasks)]
      (when-not allowed?
        (prompt/policy-decision! decision)
        (throw (ex-info reason
                        {:task-count task-count
                         :max-tasks max-tasks}))))
    ((:report-status! deps) (str "Running " task-count " branch task"
                                 (when (not= 1 task-count) "s"))
                            :phase :branch
                            :branch-count task-count
                            :parallel true)
    (let [task-id (task-spec/create-task!
                   (branch-parallel-spec objective
                                         branch-tasks
                                         max-parallel*
                                         timeout-ms)
                   :session-id parent-session-id
                   :title (or (some-> objective str str/trim not-empty)
                              "Branch tasks")
                   :summary (str "Running " task-count " branch task"
                                 (when (not= 1 task-count) "s")))
          run-result (task-spec/run-task!
                      task-id
                      :operation :branch-spawn
                      :context {:message objective
                                :parent-task-id parent-task-id
                                :resource-session-id resource-session-id*}
                      :executors {:llm
                                  (branch-worker-executor
                                   deps
                                   parent-session-id
                                   {:channel channel*
                                    :provider-id provider-id*
                                    :resource-session-id resource-session-id*
                                    :parent-task-id parent-task-id
                                    :objective objective
                                    :max-tool-rounds (or max-tool-rounds
                                                         ((:max-branch-tool-rounds deps)))
                                    :tool-context tool-context})})
          parallel-output (or (get-in run-result [:state :outputs :run-branches])
                              (get-in (db/get-task task-id)
                                      [:meta :task-spec :outputs :run-branches])
                              {})
          results (branch-results-from-parallel-output branch-tasks
                                                       parallel-output)]
      {:summary (branch-result-summary results)
       :parent_session_id parent-session-id
       :request_id (:request-id parent-context)
       :correlation_id (or (:correlation-id parent-context)
                           (:request-id parent-context))
       :branch_count task-count
       :completed_count (count (filter #(= "completed" (:status %)) results))
       :failed_count (count (filter #(= "failed" (:status %)) results))
       :results results})))
