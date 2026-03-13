(ns xia.schedule
  "Schedule management — CRUD operations for scheduled tasks.

   Schedules persist in Datalevin. Two execution types:
   - :tool  — directly call a registered tool with fixed arguments
   - :prompt — send a message through the agent loop (LLM decides what to do)

   Schedule specs are data maps (not cron strings):
   - Calendar: {:minute #{0} :hour #{9} :dow #{1 3 5}}
   - Interval: {:interval-minutes 30}

   This namespace handles data operations only. The background executor
  lives in xia.scheduler (separate to avoid circular deps with agent)."
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [xia.cron :as cron]
            [xia.db :as db]))

;; ---------------------------------------------------------------------------
;; Limits
;; ---------------------------------------------------------------------------

(def ^:private max-schedules 50)
(def ^:private min-interval-minutes 5)

(defn- actions-doc
  [actions]
  {:events (vec actions)})

(defn- read-actions-doc
  [value]
  (cond
    (map? value)    (vec (or (:events value) []))
    (sequential? value) (vec value)
    :else           value))

;; ---------------------------------------------------------------------------
;; Spec coercion (from tool JSON params to internal data)
;; ---------------------------------------------------------------------------

(defn- coerce-spec
  "Coerce a spec from tool params (JSON arrays) to internal format (sets).
   Accepts {:minute [0] :hour [9]} or {:interval-minutes 30}."
  [spec]
  (if (:interval-minutes spec)
    (select-keys spec [:interval-minutes])
    (reduce (fn [m k]
              (if-let [v (get m k)]
                (assoc m k (if (sequential? v) (set v) v))
                m))
            spec
            [:minute :hour :dom :month :dow])))

;; ---------------------------------------------------------------------------
;; Validation
;; ---------------------------------------------------------------------------

(defn- validate-spec! [spec]
  (cron/validate! spec)
  ;; Reject intervals < 5 minutes
  (when-let [m (:interval-minutes spec)]
    (when (< m min-interval-minutes)
      (throw (ex-info (str "Interval too frequent (minimum " min-interval-minutes " minutes)")
                      {:interval-minutes m}))))
  ;; For calendar specs, reject if it would fire more than 12 times per hour
  (when-not (:interval-minutes spec)
    (let [norm (cron/normalize spec)]
      (when (and (= 24 (count (:hour norm)))
                 (= 12 (count (:month norm)))
                 (> (count (:minute norm)) (/ 60 min-interval-minutes)))
        (throw (ex-info (str "Schedule too frequent (minimum " min-interval-minutes " minutes)")
                        {:spec spec}))))))

;; ---------------------------------------------------------------------------
;; CRUD
;; ---------------------------------------------------------------------------

(defn create-schedule!
  "Create a new schedule. Returns the schedule id.
   Options:
     :id          — keyword (required)
     :name        — display name
     :description — what this schedule does
     :spec        — schedule spec map (required), e.g. {:minute #{0} :hour #{9}}
                    or {:interval-minutes 30}
     :type        — :tool or :prompt (required)
     :tool-id     — keyword, required when type = :tool
     :tool-args   — map, optional args for the tool
     :prompt      — string, required when type = :prompt
     :trusted?    — allow autonomous-approved privileged tools to run without live approval (default true)"
  [{:keys [id name description spec type tool-id tool-args prompt trusted?]}]
  (when-not id
    (throw (ex-info "Schedule must have an :id" {})))
  (when-not spec
    (throw (ex-info "Schedule must have a :spec" {:id id})))
  (when-not (#{:tool :prompt} type)
    (throw (ex-info "Schedule type must be :tool or :prompt" {:id id :type type})))
  (when (and (= type :tool) (not tool-id))
    (throw (ex-info "Tool schedule must specify :tool-id" {:id id})))
  (when (and (= type :prompt) (not prompt))
    (throw (ex-info "Prompt schedule must specify :prompt" {:id id})))

  (let [spec (coerce-spec spec)]
    ;; Validate spec + check frequency
    (validate-spec! spec)

    ;; Check schedule limit
    (let [current-count (count (db/q '[:find ?e :where [?e :schedule/id _]]))]
      (when (>= current-count max-schedules)
        (throw (ex-info (str "Too many schedules (max " max-schedules ")")
                        {:current current-count}))))

    ;; Calculate first run time
    (let [now      (java.util.Date.)
          next-run (cron/next-run spec now)]
      (db/transact!
        [(cond-> {:schedule/id          id
                  :schedule/name        (or name (clojure.core/name id))
                  :schedule/spec        (pr-str spec)
                  :schedule/type        type
                  :schedule/trusted?    (if (nil? trusted?) true (boolean trusted?))
                  :schedule/enabled?    true
                  :schedule/created-at  now}
           description (assoc :schedule/description description)
           next-run    (assoc :schedule/next-run next-run)
           tool-id     (assoc :schedule/tool-id tool-id)
           tool-args   (assoc :schedule/tool-args tool-args)
           prompt      (assoc :schedule/prompt prompt))])
      (log/info "Created schedule:" (clojure.core/name id)
                "spec:" (cron/describe spec))
      {:id id :spec spec :next-run next-run})))

(defn- read-spec [s]
  (when s (edn/read-string s)))

(defn get-schedule
  "Get a schedule by id. Returns nil if not found."
  [schedule-id]
  (let [eid (ffirst (db/q '[:find ?e :in $ ?id :where [?e :schedule/id ?id]]
                          schedule-id))]
    (when eid
      (let [e (into {} (db/entity eid))]
        {:id          (:schedule/id e)
         :name        (:schedule/name e)
         :description (:schedule/description e)
         :spec        (read-spec (:schedule/spec e))
         :type        (:schedule/type e)
         :trusted?    (:schedule/trusted? e)
         :tool-id     (:schedule/tool-id e)
         :tool-args   (:schedule/tool-args e)
         :prompt      (:schedule/prompt e)
         :enabled?    (:schedule/enabled? e)
         :last-run    (:schedule/last-run e)
         :next-run    (:schedule/next-run e)
         :created-at  (:schedule/created-at e)}))))

(defn list-schedules
  "List all schedules."
  []
  (let [eids (db/q '[:find ?e :where [?e :schedule/id _]])]
    (->> eids
         (map (fn [[eid]]
                (let [e (into {} (db/entity eid))]
                  {:id        (:schedule/id e)
                   :name      (:schedule/name e)
                   :spec      (read-spec (:schedule/spec e))
                   :type      (:schedule/type e)
                   :trusted?  (:schedule/trusted? e)
                   :enabled?  (:schedule/enabled? e)
                   :last-run  (:schedule/last-run e)
                   :next-run  (:schedule/next-run e)})))
         (sort-by :name)
         vec)))

(defn update-schedule!
  "Update a schedule. Supported keys: :name :description :spec :enabled? :trusted? :tool-args :prompt"
  [schedule-id updates]
  (when-not (get-schedule schedule-id)
    (throw (ex-info "Schedule not found" {:id schedule-id})))
  (let [tx (cond-> {:schedule/id schedule-id}
             (:name updates)        (assoc :schedule/name (:name updates))
             (:description updates) (assoc :schedule/description (:description updates))
             (contains? updates :enabled?) (assoc :schedule/enabled? (:enabled? updates))
             (contains? updates :trusted?) (assoc :schedule/trusted? (boolean (:trusted? updates)))
             (:tool-args updates)   (assoc :schedule/tool-args (:tool-args updates))
             (:prompt updates)      (assoc :schedule/prompt (:prompt updates)))]
    ;; If spec changed, re-validate and update next-run
    (if-let [new-spec (:spec updates)]
      (let [spec     (coerce-spec new-spec)
            _        (validate-spec! spec)
            next-run (cron/next-run spec (java.util.Date.))]
        (db/transact! [(assoc tx
                              :schedule/spec (pr-str spec)
                              :schedule/next-run next-run)]))
      (db/transact! [tx]))
    (log/info "Updated schedule:" (clojure.core/name schedule-id))
    (get-schedule schedule-id)))

(defn remove-schedule!
  "Delete a schedule and its run history."
  [schedule-id]
  ;; Remove run history
  (let [run-eids (db/q '[:find ?e :in $ ?sid
                          :where [?e :schedule-run/schedule-id ?sid]]
                        schedule-id)]
    (when (seq run-eids)
      (db/transact! (mapv (fn [[eid]] [:db/retractEntity eid]) run-eids))))
  ;; Remove schedule
  (when-let [eid (ffirst (db/q '[:find ?e :in $ ?id
                                  :where [?e :schedule/id ?id]]
                                schedule-id))]
    (db/transact! [[:db/retractEntity eid]])
    (log/info "Removed schedule:" (clojure.core/name schedule-id)))
  {:status "removed" :id schedule-id})

(defn pause-schedule!
  "Disable a schedule (stop it from running)."
  [schedule-id]
  (update-schedule! schedule-id {:enabled? false}))

(defn resume-schedule!
  "Re-enable a paused schedule and recalculate next run."
  [schedule-id]
  (let [sched (get-schedule schedule-id)]
    (when-not sched
      (throw (ex-info "Schedule not found" {:id schedule-id})))
    (let [next-run (cron/next-run (:spec sched) (java.util.Date.)
                                  :last-run (:last-run sched))]
      (db/transact! [{:schedule/id       schedule-id
                      :schedule/enabled? true
                      :schedule/next-run next-run}])
      (get-schedule schedule-id))))

;; ---------------------------------------------------------------------------
;; Run history
;; ---------------------------------------------------------------------------

(defn record-run!
  "Record a schedule execution result."
  [schedule-id {:keys [started-at finished-at status result error actions]}]
  (db/transact!
    [(cond-> {:schedule-run/id          (random-uuid)
              :schedule-run/schedule-id schedule-id
              :schedule-run/started-at  started-at
              :schedule-run/status      status}
       finished-at (assoc :schedule-run/finished-at finished-at)
       result      (assoc :schedule-run/result
                          (if (> (count (str result)) 4000)
                            (subs (str result) 0 4000)
                            (str result)))
       error       (assoc :schedule-run/error
                          (if (> (count (str error)) 2000)
                            (subs (str error) 0 2000)
                            (str error)))
       (some? actions) (assoc :schedule-run/actions (actions-doc actions)))])
  ;; Update schedule's last-run and next-run
  (let [now      (java.util.Date.)
        sched    (get-schedule schedule-id)
        next-run (cron/next-run (:spec sched) now :last-run started-at)]
    (db/transact! [(cond-> {:schedule/id       schedule-id
                            :schedule/last-run started-at}
                     next-run (assoc :schedule/next-run next-run))])))

(defn schedule-history
  "Get recent run history for a schedule. Returns up to `limit` most recent runs."
  ([schedule-id] (schedule-history schedule-id 10))
  ([schedule-id limit]
   (let [runs (db/q '[:find ?e ?started
                       :in $ ?sid
                       :where
                       [?e :schedule-run/schedule-id ?sid]
                       [?e :schedule-run/started-at ?started]]
                     schedule-id)]
     (->> runs
          (sort-by second #(compare %2 %1))
          (take limit)
          (mapv (fn [[eid _]]
                  (let [e (into {} (db/entity eid))]
                    {:id          (:schedule-run/id e)
                     :schedule-id (:schedule-run/schedule-id e)
                     :started-at  (:schedule-run/started-at e)
                     :finished-at (:schedule-run/finished-at e)
                     :status      (:schedule-run/status e)
                     :actions     (some-> (:schedule-run/actions e)
                                          read-actions-doc)
                     :result      (:schedule-run/result e)
                     :error       (:schedule-run/error e)})))))))

(defn safe-schedule-history
  "Schedule history view for sandboxed tools.
   Omits result bodies and error text, which may contain sensitive data."
  ([schedule-id] (safe-schedule-history schedule-id 10))
  ([schedule-id limit]
   (mapv #(dissoc % :result :error :actions)
         (schedule-history schedule-id limit))))

(defn trim-history!
  "Keep only the most recent `keep-count` runs per schedule."
  [schedule-id keep-count]
  (let [runs (db/q '[:find ?e ?started
                      :in $ ?sid
                      :where
                      [?e :schedule-run/schedule-id ?sid]
                      [?e :schedule-run/started-at ?started]]
                    schedule-id)
        to-remove (->> runs
                       (sort-by second #(compare %2 %1))
                       (drop keep-count))]
    (when (seq to-remove)
      (db/transact! (mapv (fn [[eid _]] [:db/retractEntity eid]) to-remove)))))

;; ---------------------------------------------------------------------------
;; Query helpers for the scheduler
;; ---------------------------------------------------------------------------

(defn due-schedules
  "Find all enabled schedules whose next-run is at or before `now`."
  [^java.util.Date now]
  (let [eids (db/q '[:find ?e
                      :in $ ?now
                      :where
                      [?e :schedule/enabled? true]
                      [?e :schedule/next-run ?next]
                      [(<= ?next ?now)]]
                    now)]
    (mapv (fn [[eid]]
            (let [e (into {} (db/entity eid))]
              {:id        (:schedule/id e)
               :type      (:schedule/type e)
               :trusted?  (:schedule/trusted? e)
               :tool-id   (:schedule/tool-id e)
               :tool-args (:schedule/tool-args e)
               :prompt    (:schedule/prompt e)
               :spec      (read-spec (:schedule/spec e))}))
          eids)))
