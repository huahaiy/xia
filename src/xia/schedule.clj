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
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.config :as cfg]
            [xia.cron :as cron]
            [xia.db :as db]
            [xia.remote-bridge :as remote-bridge]))

;; ---------------------------------------------------------------------------
;; Limits
;; ---------------------------------------------------------------------------

(def ^:private max-schedules 50)
(def ^:private min-interval-minutes 5)
(def ^:private default-failure-backoff-minutes 15)
(def ^:private default-max-failure-backoff-minutes (* 12 60))
(def ^:private default-pause-after-repeated-failures 3)

(defn- failure-backoff-minutes
  []
  (cfg/positive-long :schedule/failure-backoff-minutes
                     default-failure-backoff-minutes))

(defn- max-failure-backoff-minutes
  []
  (cfg/positive-long :schedule/max-failure-backoff-minutes
                     default-max-failure-backoff-minutes))

(defn- pause-after-repeated-failures
  []
  (cfg/positive-long :schedule/pause-after-repeated-failures
                     default-pause-after-repeated-failures))

(defn- actions-doc
  [actions]
  {:events (vec actions)})

(defn- read-actions-doc
  [value]
  (cond
    (map? value)    (vec (or (:events value) []))
    (sequential? value) (vec value)
    :else           value))

(defn- truncate-string
  [value max-len]
  (let [s (some-> value str)]
    (when (seq s)
      (if (> (count s) max-len)
        (subs s 0 max-len)
        s))))

(defn- checkpoint-doc
  [checkpoint]
  (when (map? checkpoint)
    (cond-> (reduce-kv (fn [acc k v]
                         (assoc acc k
                                (cond
                                  (string? v) (truncate-string v 500)
                                  (and (sequential? v) (not (map? v)))
                                  (vec (map #(if (string? %) (truncate-string % 120) %) (take 10 v)))
                                  :else v)))
                       {}
                       checkpoint)
      (:summary checkpoint)
      (update :summary truncate-string 500))))

(defn- read-checkpoint-doc
  [value]
  (when (map? value)
    value))

(defn- normalize-failure-signature
  [error]
  (some-> error
          str
          str/lower-case
          str/trim
          (str/replace #"\d+" "#")
          (str/replace #"[0-9a-f]{8}-[0-9a-f-]{27}" "<uuid>")
          (str/replace #"\s+" " ")
          (truncate-string 240)))

(defn- recovery-hint
  [error]
  (let [message (some-> error str str/lower-case)]
    (cond
      (or (str/includes? message "no element matches selector")
          (str/includes? message "selector")
          (str/includes? message "no form found")
          (str/includes? message "element not found")
          (str/includes? message "click failed"))
      "The previous attempt likely relied on a stale DOM path. Re-inspect the page with browser-query-elements before retrying selectors or clicks."

      (or (str/includes? message "timed out")
          (str/includes? message "timeout"))
      "Treat the previous failure as a timing issue first. Re-open or re-read the page, wait explicitly, then verify the target elements before continuing."

      (or (str/includes? message "403")
          (str/includes? message "401")
          (str/includes? message "unauthorized")
          (str/includes? message "forbidden")
          (str/includes? message "login"))
      "The previous attempt may have lost authentication. Check login/session state before repeating the same path."

      :else
      "Do not repeat the previous failing path blindly. Inspect the current page state and choose the next step from live evidence.")))

(defn- schedule-state-eid
  [schedule-id]
  (ffirst (db/q '[:find ?e :in $ ?sid
                  :where [?e :schedule.state/schedule-id ?sid]]
                schedule-id)))

(defn- normalize-session-id
  [value]
  (cond
    (instance? java.util.UUID value) value
    (string? value)                  (try
                                       (java.util.UUID/fromString (str/trim value))
                                       (catch Exception _
                                         nil))
    :else                            nil))

(defn task-state
  "Return durable execution state for a schedule, if any."
  [schedule-id]
  (when-let [eid (schedule-state-eid schedule-id)]
    (let [e (db/entity eid)]
      {:schedule-id           (:schedule.state/schedule-id e)
       :status                (:schedule.state/status e)
       :phase                 (:schedule.state/phase e)
       :checkpoint            (read-checkpoint-doc (:schedule.state/checkpoint e))
       :checkpoint-at         (:schedule.state/checkpoint-at e)
       :last-success-at       (:schedule.state/last-success-at e)
       :last-success-summary  (:schedule.state/last-success-summary e)
       :last-failure-at       (:schedule.state/last-failure-at e)
       :last-error            (:schedule.state/last-error e)
       :last-failure-signature (:schedule.state/last-failure-signature e)
       :last-recovery-hint    (:schedule.state/last-recovery-hint e)
       :consecutive-failures  (or (:schedule.state/consecutive-failures e) 0)
       :backoff-until         (:schedule.state/backoff-until e)})))

(defn resumable-session-id
  "Return the session id that should be reused for a recovering scheduled prompt,
   or nil if the task should start fresh."
  [schedule-id]
  (let [{:keys [status checkpoint]} (task-state schedule-id)
        session-id (normalize-session-id (:session-id checkpoint))]
    (when (and (#{:running :backoff :paused} status)
               session-id
               (ffirst (db/q '[:find ?e :in $ ?sid
                               :where [?e :session/id ?sid]]
                             session-id)))
      session-id)))

(defn save-task-checkpoint!
  "Persist a lightweight checkpoint for a scheduled task run."
  [schedule-id checkpoint]
  (let [now            (java.util.Date.)
        state-eid      (schedule-state-eid schedule-id)
        checkpoint-doc (checkpoint-doc checkpoint)]
    (db/transact!
      (cond-> [(cond-> {:schedule.state/schedule-id schedule-id
                        :schedule.state/status      :running
                        :schedule.state/checkpoint-at now}
                 (:phase checkpoint) (assoc :schedule.state/phase (:phase checkpoint))
                 checkpoint-doc (assoc :schedule.state/checkpoint checkpoint-doc))]
        state-eid
        (conj [:db/retract state-eid :schedule.state/backoff-until])))
    (task-state schedule-id)))

(defn- failure-backoff-ms
  [consecutive-failures]
  (* 60 1000
     (min (max-failure-backoff-minutes)
          (* (failure-backoff-minutes)
             (long (Math/pow 2.0 (double (max 0 (dec consecutive-failures)))))))))

(defn record-task-success!
  "Mark a schedule task run as successful and clear failure state."
  [schedule-id result-summary]
  (let [now       (java.util.Date.)
        state     (task-state schedule-id)
        state-eid (schedule-state-eid schedule-id)]
    (db/transact!
      (cond-> [{:schedule.state/schedule-id schedule-id
                :schedule.state/status :success
                :schedule.state/phase :complete
                :schedule.state/last-success-at now
                :schedule.state/last-success-summary (truncate-string result-summary 1000)
                :schedule.state/consecutive-failures 0
                :schedule.state/last-error ""
                :schedule.state/last-failure-signature ""
                :schedule.state/last-recovery-hint ""}]
        state-eid
        (conj [:db/retract state-eid :schedule.state/backoff-until])))
    (when (pos? (long (or (:consecutive-failures state) 0)))
      (remote-bridge/notify-schedule-recovered!
        schedule-id
        {:previous-failures (:consecutive-failures state)
         :result-summary result-summary}))
    (task-state schedule-id)))

(defn record-task-failure!
  "Persist failure state for a schedule task and apply backoff or pause policy."
  [schedule-id error-message]
  (let [now                 (java.util.Date.)
        state-eid           (schedule-state-eid schedule-id)
        state               (task-state schedule-id)
        error-message       (or (truncate-string error-message 2000)
                                "Unknown scheduled task failure")
        signature           (normalize-failure-signature error-message)
        same-failure?       (= signature (:last-failure-signature state))
        consecutive-failures (if same-failure?
                               (inc (or (:consecutive-failures state) 0))
                               1)
        hint                (recovery-hint error-message)
        pause-threshold     (pause-after-repeated-failures)
        paused?             (and same-failure?
                                 (>= consecutive-failures pause-threshold))
        backoff-until       (when-not paused?
                              (java.util.Date.
                                (long (+ (.getTime ^java.util.Date now)
                                         (failure-backoff-ms consecutive-failures)))))]
    (db/transact!
      (cond-> [(cond-> {:schedule.state/schedule-id schedule-id
                        :schedule.state/status (if paused? :paused :backoff)
                        :schedule.state/phase :error
                        :schedule.state/last-failure-at now
                        :schedule.state/last-error error-message
                        :schedule.state/last-failure-signature signature
                        :schedule.state/last-recovery-hint hint
                        :schedule.state/consecutive-failures consecutive-failures}
                 backoff-until (assoc :schedule.state/backoff-until backoff-until))
               (cond-> {:schedule/id schedule-id}
                 paused? (assoc :schedule/enabled? false)
                 backoff-until (assoc :schedule/next-run backoff-until))]
        (and paused? state-eid)
        (conj [:db/retract state-eid :schedule.state/backoff-until])))
    (remote-bridge/notify-schedule-failure!
      schedule-id
      {:paused? paused?
       :consecutive-failures consecutive-failures
       :backoff-until backoff-until
       :error-message error-message})
    (task-state schedule-id)))

(defn recovery-brief
  "Return a compact recovery summary for the next scheduled prompt attempt."
  [schedule-id]
  (when-let [{:keys [consecutive-failures last-failure-at last-error
                     last-recovery-hint checkpoint]} (task-state schedule-id)]
    (when (pos? (or consecutive-failures 0))
      (str/join
        "\n"
        (cond-> ["Recovery context from previous scheduled attempts:"
                 (str "- Consecutive failures: " consecutive-failures)]
          last-failure-at
          (conj (str "- Last failure at: " last-failure-at))

          (seq last-error)
          (conj (str "- Last error: " last-error))

          (seq last-recovery-hint)
          (conj (str "- Recovery hint: " last-recovery-hint))

          (seq (:summary checkpoint))
          (conj (str "- Last checkpoint"
                     (when-let [phase (:phase checkpoint)]
                       (str " (" (name phase)
                            (when-let [round (:round checkpoint)]
                              (str ", round " round))
                            ")"))
                     ": "
                     (:summary checkpoint)))

          (normalize-session-id (:session-id checkpoint))
          (conj "- Resume the existing scheduled session instead of starting over from the beginning."))))))

(defn augment-prompt-with-recovery-context
  "Inject prior failure/checkpoint context into a scheduled prompt."
  [schedule-id prompt]
  (if-let [brief (recovery-brief schedule-id)]
    (str prompt "\n\n" brief)
    prompt))

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
  ;; Remove task state
  (when-let [state-eid (schedule-state-eid schedule-id)]
    (db/transact! [[:db/retractEntity state-eid]]))
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
