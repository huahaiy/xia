(ns xia.agent.child-task
  "Shared helpers for branch-style child task workers."
  (:require [taoensso.timbre :as log]
            [xia.db :as db]
            [xia.working-memory :as wm]))

(defn branch-worker-contract
  [{:keys [title prompt work-prompt objective parent-task-id]}]
  (cond-> {:kind :task
           :version 1
           :goal title
           :spec {:kind :task
                  :version 1
                  :goal title
                  :steps [{:id :work-on-branch
                           :kind :llm
                           :mode :agent
                           :prompt work-prompt}]}}
    (some? prompt) (assoc :prompt prompt)
    (some? objective) (assoc :objective objective)
    parent-task-id (assoc :parent-task-id parent-task-id)))

(defn branch-worker-tool-context
  [parent-session-id resource-session-id & contexts]
  (apply merge
         {:branch-worker? true
          :parent-session-id parent-session-id
          :resource-session-id (or resource-session-id parent-session-id)}
         contexts))

(defn create-branch-worker!
  [{:keys [parent-session-id parent-task-id title summary prompt work-prompt
           objective resource-session-id session-active? session-role state
           contract meta started-at]}]
  (let [session-opts    (cond-> {:parent-session-id parent-session-id
                                 :worker? true
                                 :label title}
                          (some? session-active?)
                          (assoc :active? session-active?))
        child-session-id (db/create-session! :branch session-opts)
        contract*        (or contract
                             (branch-worker-contract
                              {:title title
                               :prompt prompt
                               :work-prompt work-prompt
                               :objective objective
                               :parent-task-id parent-task-id}))
        meta*            (merge (cond-> {:trigger {:kind :branch}
                                         :execution {:mode :agent}}
                           parent-task-id
                           (assoc-in [:trigger :parent-task-id] parent-task-id))
                                meta)
        task-id          (db/create-task!
                          (cond-> {:session-id child-session-id
                                   :channel :branch
                                   :type :task
                                   :state (or state :running)
                                   :title title
                                   :summary (or summary title)
                                   :contract contract*
                                   :meta (cond-> meta*
                                           (and resource-session-id
                                                (not (contains? meta*
                                                                :resource-session-id)))
                                           (assoc :resource-session-id
                                                  resource-session-id))
                                   :started-at (or started-at
                                                   (java.util.Date.))}
                            parent-task-id
                            (assoc :parent-id parent-task-id)
                            session-role
                            (assoc :session-role session-role)))]
    {:session-id child-session-id
     :task-id task-id
     :task (db/get-task task-id)}))

(defn deactivate-worker-session!
  [session-id log-message log-context]
  (try
    (db/set-session-active! session-id false)
    (catch Throwable t
      (log/warn t log-message log-context))))

(defn clear-worker-autonomy-state!
  [session-id log-message log-context]
  (try
    (wm/clear-autonomy-state! session-id)
    (catch Throwable t
      (log/warn t log-message log-context))))

(defn clear-worker-memory!
  [session-id log-message log-context]
  (try
    (wm/clear-wm! session-id)
    (catch Throwable t
      (log/warn t log-message log-context))))

(defn with-registered-worker-session!
  [{:keys [deps parent-session-id child-session-id log-context
           deactivate-message clear-autonomy-message clear-memory-message
           clear-autonomy? clear-memory?]
    :or {clear-memory? true}}
   f]
  (when-let [register! (:register-child-session! deps)]
    (register! parent-session-id child-session-id))
  (try
    (f)
    (finally
      (when-let [unregister! (:unregister-child-session! deps)]
        (unregister! parent-session-id child-session-id))
      (deactivate-worker-session!
       child-session-id
       (or deactivate-message "Failed to deactivate child task session")
       log-context)
      (when clear-autonomy?
        (clear-worker-autonomy-state!
         child-session-id
         (or clear-autonomy-message
             "Failed to clear child task autonomy state")
         log-context))
      (when clear-memory?
        (clear-worker-memory!
         child-session-id
         (or clear-memory-message
             "Failed to clear child task working memory")
         log-context)))))
