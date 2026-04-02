(ns xia.agent.branch
  "Branch-task orchestration for parallel worker sessions."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.async :as async]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.task-policy :as task-policy]
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
   {:keys [channel provider-id resource-session-id objective
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
        child-session-id (db/create-session! :branch
                                             {:parent-session-id parent-session-id
                                              :worker? true
                                              :active? false
                                              :label task})]
    ((:register-child-session! deps) parent-session-id child-session-id)
    (try
      ((:throw-if-runtime-stopping! deps) child-session-id)
      ((:throw-if-cancelled! deps) child-session-id)
      (wm/create-wm! child-session-id)
      (let [result ((:process-message deps) child-session-id
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
                :error-detail ((:throwable-detail deps) t)}))
      (finally
        ((:unregister-child-session! deps) parent-session-id child-session-id)
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
  [deps tasks & {:keys [session-id channel provider-id resource-session-id objective
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
        _ ((:throw-if-runtime-stopping! deps) parent-session-id)
        _ ((:throw-if-cancelled! deps) parent-session-id)
        max-tasks ((:max-branch-tasks deps))
        max-parallel* (clojure.core/min (clojure.core/max 1 (long (or max-parallel ((:max-parallel-branches deps)))))
                                        (clojure.core/max 1 (long max-tasks)))]
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
    (let [batches (partition-all max-parallel* branch-tasks)
          results (loop [remaining batches
                         acc []]
                    (if-let [batch (first remaining)]
                      (let [futures (mapv (fn [branch-task]
                                            (async/submit-parallel!
                                             (str "branch-task:" (or (:task branch-task)
                                                                     (:prompt branch-task)
                                                                     "unnamed"))
                                             #(run-branch-task* deps
                                                                parent-session-id
                                                                branch-task
                                                                {:channel channel*
                                                                 :provider-id provider-id*
                                                                 :resource-session-id resource-session-id*
                                                                 :objective objective
                                                                 :max-tool-rounds (or max-tool-rounds
                                                                                      ((:max-branch-tool-rounds deps)))
                                                                 :tool-context tool-context})))
                                          batch)
                            batch-results ((:await-futures! deps)
                                           futures
                                           ((:branch-task-timeout-ms deps))
                                           (fn [idx timeout-ms]
                                             (let [branch-task (nth batch idx)
                                                   decision (task-policy/branch-task-timeout-policy
                                                             (:task branch-task)
                                                             (:prompt branch-task)
                                                             timeout-ms)]
                                               (prompt/policy-decision! decision)
                                               (ex-info (:reason decision)
                                                        (merge (trace-context deps parent-context)
                                                               {:type :branch-task-timeout
                                                                :timeout-ms timeout-ms
                                                                :task (:task branch-task)
                                                                :prompt (:prompt branch-task)})))))]
                        (recur (next remaining)
                               (into acc batch-results)))
                      (vec acc)))]
      {:summary (branch-result-summary results)
       :parent_session_id parent-session-id
       :request_id (:request-id parent-context)
       :correlation_id (or (:correlation-id parent-context)
                           (:request-id parent-context))
       :branch_count task-count
       :completed_count (count (filter #(= "completed" (:status %)) results))
       :failed_count (count (filter #(= "failed" (:status %)) results))
       :results results})))
