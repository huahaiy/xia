(ns xia.wake-projection
  "Canonical control-plane wake projection derived from persisted schedule state.

   This snapshot is intended to be the shared payload shape for both:
   - pull: GET /command/managed/wake-projection
   - push: wake_projection.updated"
  (:require [datalevin.core :as d]
            [xia.db :as db]
            [xia.paths :as paths])
  (:import [java.math BigInteger]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.time ZoneId]
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

(defn- sha256-hex
  [text]
  (let [digest (doto (MessageDigest/getInstance "SHA-256")
                 (.update (.getBytes ^String text StandardCharsets/UTF_8)))]
    (format "%064x" (BigInteger. 1 (.digest digest)))))

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

(defn- projection-seq
  [tenant-id timezone-id next-global-wake-at rows]
  (str "wp_"
       (sha256-hex
         (pr-str [projection-schema-version
                  tenant-id
                  timezone-id
                  (instant->str next-global-wake-at)
                  (mapv (fn [{:keys [schedule-id enabled status next-wake-at wake-reason]}]
                          [schedule-id
                           enabled
                           (keyword->str status)
                           (instant->str next-wake-at)
                           (keyword->str wake-reason)])
                        rows)]))))

(defn current-snapshot
  "Return the latest fully committed wake projection snapshot derived from the DB.

   The returned shape is the canonical payload to reuse for both pull and push
   control-plane paths."
  ([] (current-snapshot {}))
  ([{:keys [generated-at]
     :or {generated-at (Date.)}}]
   (let [db-value              (snapshot-db)
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
                                   first)
         seq-token            (projection-seq tenant-id timezone-id next-global-wake-at rows)]
     {:tenant_id                  tenant-id
      :projection_schema_version  projection-schema-version
      :projection_seq             seq-token
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
