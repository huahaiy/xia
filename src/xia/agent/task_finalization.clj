(ns xia.agent.task-finalization
  "Post-task finalization hooks that must not affect task completion."
  (:require [taoensso.timbre :as log]
            [xia.agent.run-state :as run-state]
            [xia.async :as async]
            [xia.db :as db]
            [xia.runtime-context :as runtime-context]
            [xia.skill.proposal :as skill-proposal]))

(def ^:private runtime-context-key :xia/agent-runtime)

(defn- now [] (java.util.Date.))

(defn- current-runtime
  []
  (or (runtime-context/runtime runtime-context-key)
      (throw (ex-info "Agent runtime is not installed"
                      {:component runtime-context-key}))))

(defn- claim-learning!
  [task-id]
  (run-state/claim-skill-learning! (current-runtime) task-id))

(defn- release-learning!
  [task-id]
  (run-state/release-skill-learning! (current-runtime) task-id))

(defn- task-learning-status
  [task]
  (get-in task [:meta :skill-learning :status]))

(defn- nullish-value?
  [value]
  (or (nil? value)
      (= :json/null value)
      (= "json/null" value)
      (= ":json/null" value)))

(defn- sanitize-doc-value
  [value]
  (cond
    (nullish-value? value)
    nil

    (map? value)
    (let [entries (keep (fn [[k v]]
                          (when-let [v* (sanitize-doc-value v)]
                            [k v*]))
                        value)]
      (when (seq entries)
        (into (empty value) entries)))

    (vector? value)
    (let [items (into [] (keep sanitize-doc-value) value)]
      (when (seq items)
        items))

    (sequential? value)
    (let [items (keep sanitize-doc-value value)]
      (when (seq items)
        (into (empty value) items)))

    :else
    value))

(defn- learnable-task?
  [task]
  (and task
       (= :completed (:state task))
       (nil? (:parent-id task))
       (not (true? (get-in task [:meta :branch-worker])))
       (not= :completed (task-learning-status task))))

(defn- update-learning-meta!
  [task-id attrs]
  (when-let [task (db/get-task task-id)]
    (db/update-task! task-id
                     {:meta (sanitize-doc-value
                             (assoc (or (:meta task) {})
                                    :skill-learning
                                    (merge (get-in task [:meta :skill-learning])
                                           attrs)))})))

(defn- review-summary
  [review]
  (cond-> {:decision (some-> (:decision review) name)
           :reason (some-> (:reason review) name)
           :message (:message review)
           :proposal-id (some-> (get-in review [:proposal :skill.proposal/id]) str)}
    (:skill review)
    (assoc :skill-id (some-> (get-in review [:skill :skill/id]) name))))

(defn- learning-summary
  [{:keys [proposals reviews]}]
  {:proposal-count (count proposals)
   :review-count (count reviews)
   :applied-count (count (filter #(= :approved (:decision %)) reviews))
   :rejected-count (count (filter #(= :rejected (:decision %)) reviews))
   :human-review-count (count (filter #(= :needs-human-review (:decision %)) reviews))
   :proposal-ids (mapv #(str (:skill.proposal/id %)) proposals)
   :reviews (mapv review-summary reviews)})

(defn- run-skill-learning!
  [task-id opts]
  (try
    (if-let [task (db/get-task task-id)]
      (if (learnable-task? task)
        (do
          (update-learning-meta! task-id {:status :running
                                          :phase :execution
                                          :started-at (now)
                                          :finished-at nil
                                          :error nil})
          (try
            (let [result (apply skill-proposal/generate-and-review-proposals-for-task!
                                task-id
                                (mapcat identity opts))]
              (update-learning-meta! task-id
                                     (merge {:status :completed
                                             :phase :completed
                                             :finished-at (now)}
                                            (learning-summary result))))
            (catch Throwable t
              (log/warn t "Post-task skill learning failed"
                        {:task-id task-id})
              (update-learning-meta! task-id {:status :failed
                                              :phase :execution
                                              :finished-at (now)
                                              :error (.getMessage t)}))))
        (log/debug "Skipping post-task skill learning"
                   {:task-id task-id
                    :state (:state task)
                    :parent-id (:parent-id task)
                    :learning-status (task-learning-status task)}))
      (log/debug "Skipping post-task skill learning for missing task"
                 {:task-id task-id}))
    (finally
      (release-learning! task-id))))

(defn launch-skill-learning!
  "Launch post-task skill proposal generation/review in the background.

   Completion has already been persisted before this is called; failures here
   are recorded in task metadata and never rethrow into task finalization."
  [task-id & {:as opts}]
  (when (and task-id
             (learnable-task? (db/get-task task-id))
             (claim-learning! task-id))
    (try
      (if (async/submit-background!
           (str "task-skill-learning:" task-id)
           #(run-skill-learning! task-id opts))
        true
        (do
          (release-learning! task-id)
          (update-learning-meta! task-id {:status :failed
                                          :phase :submission
                                          :finished-at (now)
                                          :error "Post-task skill learning worker was not accepted"})
          false))
      (catch Throwable t
        (release-learning! task-id)
        (update-learning-meta! task-id {:status :failed
                                        :phase :submission
                                        :finished-at (now)
                                        :error (or (.getMessage t)
                                                   "Post-task skill learning launch failed")})
        (log/warn t "Unable to launch post-task skill learning"
                  {:task-id task-id})
        false))))
