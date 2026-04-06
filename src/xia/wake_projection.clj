(ns xia.wake-projection
  "Canonical control-plane wake projection derived from persisted schedule state.

   This snapshot is intended to be the shared payload shape for the control
   plane pull path: GET /command/managed/wake-projection."
  (:require [datalevin.core :as d]
            [xia.db :as db]
            [xia.paths :as paths])
  (:import [java.time ZoneId]
           [java.util Date]))

(def projection-schema-version 1)

(defn- keyword->str
  [value]
  (cond
    (keyword? value) (if-let [ns-part (namespace value)]
                       (str ns-part "/" (name value))
                       (name value))
    (symbol? value)  (if-let [ns-part (namespace value)]
                       (str ns-part "/" (name value))
                       (name value))
    (some? value)    (str value)
    :else            nil))

(defn- instant->str
  [value]
  (when (instance? Date value)
    (str (.toInstant ^Date value))))

(defn- projection-status
  [sched task-state]
  (cond
    (not (:enabled? sched)) :disabled
    (= :backoff (:status task-state)) :backoff
    (= :running (:status task-state)) :running
    :else :scheduled))

(defn- wake-reason
  [status next-wake-at]
  (when next-wake-at
    (case status
      :backoff :retry_backoff
      :running :user_schedule
      :scheduled :user_schedule
      nil)))

(defn- schedule-row
  [sched task-state]
  (let [status        (projection-status sched task-state)
        next-wake-at  (when (not= :disabled status)
                        (:next-run sched))]
    {:schedule-id  (keyword->str (:id sched))
     :enabled      (boolean (:enabled? sched))
     :status       status
     :next-wake-at next-wake-at
     :wake-reason  (wake-reason status next-wake-at)}))

(defn- snapshot-db
  []
  (d/db (db/conn)))

(defn- schedule-records
  [db-value]
  (->> (d/q '[:find ?e :where [?e :schedule/id _]] db-value)
       (map (fn [[eid]]
              (let [entity (into {} (d/entity db-value eid))]
                {:id        (:schedule/id entity)
                 :enabled?  (:schedule/enabled? entity)
                 :next-run  (:schedule/next-run entity)})))
       vec))

(defn- task-state-index
  [db-value]
  (->> (d/q '[:find ?e :where [?e :schedule.state/schedule-id _]] db-value)
       (map (fn [[eid]]
              (let [entity (into {} (d/entity db-value eid))]
                [(:schedule.state/schedule-id entity)
                 {:status (:schedule.state/status entity)}])))
       (into {})))

(defn current-snapshot
  "Return the latest fully committed wake projection snapshot derived from the DB.

   The returned shape is the canonical payload for the control-plane pull path."
  ([] (current-snapshot {}))
  ([{:keys [generated-at]
     :or {generated-at (Date.)}}]
   (let [db-value              (snapshot-db)
         workspace-tx          (some-> (:max-tx db-value) long)
         task-states           (task-state-index db-value)
         tenant-id             (or (some-> (db/current-instance-id) str)
                                  paths/default-instance-id)
         timezone-id          (str (ZoneId/systemDefault))
         rows                 (->> (schedule-records db-value)
                                   (map #(schedule-row % (get task-states (:id %))))
                                   (sort-by :schedule-id)
                                   vec)
         next-global-wake-at  (->> rows
                                   (keep :next-wake-at)
                                   (sort)
                                   first)]
     {:tenant_id                  tenant-id
      :projection_schema_version  projection-schema-version
      :projection_seq             workspace-tx
      :workspace_tx               workspace-tx
      :generated_at               (instant->str generated-at)
      :effective_timezone         timezone-id
      :next_global_wake_at        (instant->str next-global-wake-at)
      :schedules                  (mapv (fn [{:keys [schedule-id enabled status next-wake-at wake-reason]}]
                                          {:schedule_id schedule-id
                                           :enabled enabled
                                           :status (keyword->str status)
                                           :next_wake_at (instant->str next-wake-at)
                                           :wake_reason (keyword->str wake-reason)})
                                        rows)})))
