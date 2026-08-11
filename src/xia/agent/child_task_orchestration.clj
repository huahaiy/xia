(ns xia.agent.child-task-orchestration
  "Shared child task orchestration across task runtime, branch tools, and task specs."
  (:require [xia.agent.child-task :as child-task]
            [xia.async :as async]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.working-memory :as wm]))

(defn branch-worker-tool-context
  [parent-session-id resource-session-id & contexts]
  (apply child-task/branch-worker-tool-context
         parent-session-id
         resource-session-id
         contexts))

(defn branch-worker-contract
  [opts]
  (child-task/branch-worker-contract opts))

(defn attach-child-task-to-parent!
  [task child-task-id title]
  (when-let [task-id (:id task)]
    (let [session-id   (:session-id task)
          base-state   (or (when session-id
                             (wm/autonomy-state session-id))
                           (:autonomy-state task)
                           (autonomous/initial-state (:title task)))
          summary      (str "Delegated child task: " title)
          next-state   (autonomous/attach-child-task
                        base-state
                        child-task-id
                        :title title
                        :summary summary
                        :reason "Delegated work to a child task."
                        :progress-status :in-progress)]
      (db/update-task! task-id
                       {:autonomy-state next-state
                        :summary summary})
      (when session-id
        (wm/set-autonomy-state! session-id next-state))
      next-state)))

(defn create-branch-worker!
  [{:keys [parent-task parent-task-id title attach?]
    :or {attach? true}
    :as opts}]
  (let [worker       (child-task/create-branch-worker! (dissoc opts :parent-task :attach?))
        parent-task* (or parent-task
                         (some-> parent-task-id db/get-task))]
    (when (and attach? parent-task*)
      (attach-child-task-to-parent! parent-task* (:task-id worker) title))
    worker))

(defn with-worker-session!
  [opts f]
  (child-task/with-registered-worker-session! opts f))

(defn submit-worker!
  [{:keys [label deps parent-session-id child-session-id log-context
           deactivate-message clear-autonomy-message clear-memory-message
           clear-autonomy? clear-memory? run]}]
  (async/submit-background!
   label
   #(with-worker-session!
      (cond-> {:deps deps
               :parent-session-id parent-session-id
               :child-session-id child-session-id
               :log-context log-context
               :deactivate-message deactivate-message
               :clear-autonomy-message clear-autonomy-message
               :clear-memory-message clear-memory-message}
        (some? clear-autonomy?) (assoc :clear-autonomy? clear-autonomy?)
        (some? clear-memory?) (assoc :clear-memory? clear-memory?))
      run)))

(defn mark-worker-unavailable!
  [{:keys [child-task-id child-session-id summary error log-context deactivate-message]}]
  (db/update-task! child-task-id
                   {:state :failed
                    :summary summary
                    :error error
                    :finished-at (java.util.Date.)})
  (child-task/deactivate-worker-session!
   child-session-id
   deactivate-message
   log-context)
  (db/get-task child-task-id))

(defn normalize-branch-worker-result
  [{:keys [trace task child-task-id child-session-id run-result topics error error-detail]}]
  (merge trace
         {:task task
          :status (if (or (not (map? run-result))
                          (= :completed (:status run-result)))
                    "completed"
                    "failed")
          :task-id child-task-id
          :session-id child-session-id
          :topics topics
          :result (or (get-in run-result [:state :outputs :work-on-branch])
                      run-result)}
         (when error
           {:error error})
         (when error-detail
           {:error-detail error-detail})
         (when (and (map? run-result)
                    (not= :completed (:status run-result)))
           {:error (or error
                       (:error run-result)
                       (:summary run-result))})))

(defn state-child-task-id
  [state step-id]
  (or (get-in state [:steps step-id :output :task-id])
      (get-in state [:steps step-id :output "task-id"])
      (get-in state [:steps step-id :subtask-task-id])))

(defn control-key-string
  [value]
  (cond
    (keyword? value) (name value)
    (symbol? value) (name value)
    :else (str value)))

(defn triggered-task-match?
  [{:keys [kind parent-task-id step-id control-key]} task]
  (and (= parent-task-id (:parent-id task))
       (= kind (get-in task [:meta :trigger :kind]))
       (= step-id (get-in task [:meta :trigger :parent-step-id]))
       (or (nil? control-key)
           (= (control-key-string control-key)
              (get-in task [:meta :trigger :control-key])))))

(defn latest-triggered-task
  [{:keys [parent-task-id] :as criteria}]
  (->> (db/list-tasks {:limit 100000})
       (filter #(triggered-task-match? criteria %))
       (sort-by #(or (:updated-at %) (:created-at %)) #(compare %2 %1))
       first))

(defn create-task-spec-child!
  [{:keys [kind parent-task turn-id step-id control-key session-id channel
           session-role title contract runtime-key runtime-state execution-mode meta]}]
  (db/create-task!
   (cond-> {:session-id session-id
            :parent-id (:id parent-task)
            :channel (or channel (:channel parent-task) :task-spec)
            :type :task
            :state :resumable
            :title title
            :summary title
            :contract contract
            :meta (merge {:trigger (cond-> {:kind kind
                                            :parent-task-id (:id parent-task)
                                            :parent-turn-id turn-id
                                            :parent-step-id step-id}
                                     (some? control-key)
                                     (assoc :control-key (control-key-string control-key)))
                          :execution {:mode execution-mode}
                          runtime-key runtime-state}
                         meta)}
     session-role (assoc :session-role session-role))))

(defn create-branch-session!
  [parent-task label]
  (when-let [parent-session-id (:session-id parent-task)]
    (db/create-session! :branch
                        {:parent-session-id parent-session-id
                         :worker? true
                         :active? false
                         :label label})))

(defn create-task-spec-branch!
  [{:keys [parent-task turn-id step-id title contract runtime-key runtime-state]}]
  (let [parent-session-id (:session-id parent-task)
        child-session-id  (create-branch-session! parent-task title)
        branch-meta       (cond-> {:branch-worker true}
                            parent-session-id
                            (assoc :parent-session-id parent-session-id
                                   :resource-session-id parent-session-id))
        task-id           (create-task-spec-child!
                           {:kind :branch
                            :parent-task parent-task
                            :turn-id turn-id
                            :step-id step-id
                            :session-id child-session-id
                            :channel :branch
                            :session-role (when child-session-id :branch)
                            :title title
                            :contract contract
                            :runtime-key runtime-key
                            :runtime-state runtime-state
                            :execution-mode :agent
                            :meta branch-meta})]
    (attach-child-task-to-parent! parent-task task-id title)
    task-id))

(defn ensure-triggered-child!
  [{:keys [parent-task state step-id kind control-key create!]}]
  (let [child-id (when-not control-key
                   (state-child-task-id state step-id))]
    (or (when child-id
          (some-> child-id db/get-task :id))
        (some-> (latest-triggered-task {:kind kind
                                        :parent-task-id (:id parent-task)
                                        :step-id step-id
                                        :control-key control-key})
                :id)
        (create!))))

(defn task-spec-outputs
  [runtime-key task-or-state]
  (or (:outputs task-or-state)
      (get-in task-or-state [:meta runtime-key :outputs])
      {}))

(defn child-output
  [runtime-key child-task-id child-result]
  (let [child-task (db/get-task child-task-id)]
    (cond-> {:task-id child-task-id
             :status (:status child-result)
             :outputs (task-spec-outputs runtime-key
                                         (or (:state child-result)
                                             child-task))}
      (:summary child-result) (assoc :summary (:summary child-result))
      (:turn-id child-result) (assoc :turn-id (:turn-id child-result))
      (:error child-result) (assoc :error (:error child-result)))))

(defn completed-child-result
  [runtime-key child-task-id child-task default-summary]
  {:status :completed
   :task-id child-task-id
   :summary (or (:summary child-task) default-summary)
   :state (get-in child-task [:meta runtime-key])})

(defn child-result->step-result
  [runtime-key kind child-task-id child-result]
  (let [output  (child-output runtime-key child-task-id child-result)
        summary (or (:summary child-result)
                    (str (case kind
                           :branch "Branch"
                           "Subtask")
                         " "
                         (name (:status child-result))))]
    (case (:status child-result)
      :completed
      {:status :success
       :summary summary
       :output output}

      :paused
      {:status :paused
       :pause-reason (case kind
                       :branch :branch-paused
                       :subtask-paused)
       :summary summary
       :output output}

      :failed
      {:status :failed
       :summary summary
       :error (or (:error child-result)
                  (get-in output [:error])
                  (str (name kind) " failed"))
       :output output}

      {:status :paused
       :pause-reason (case kind
                       :branch :branch-pending
                       :subtask-pending)
       :summary summary
       :output output})))

(defn async-child-output
  [runtime-key child-task-id future]
  (let [child-task (db/get-task child-task-id)]
    (cond-> {:task-id child-task-id
             :status :running
             :async true
             :outputs (task-spec-outputs runtime-key child-task)}
      (:session-id child-task) (assoc :session-id (:session-id child-task))
      future (assoc :submitted true))))

(defn start-async-child!
  [{:keys [label child-task-id context executors max-steps run-task! operation]}]
  (async/submit-background!
   label
   (fn []
     (try
       (run-task! child-task-id
                  :context context
                  :executors executors
                  :max-steps max-steps
                  :operation operation)
       (finally
         (when-let [session-id (:session-id (db/get-task child-task-id))]
           (db/set-session-active! session-id false)))))))
