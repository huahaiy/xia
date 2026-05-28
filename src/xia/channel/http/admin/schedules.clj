(ns xia.channel.http.admin.schedules
  "Schedule admin HTTP handlers."
  (:require [xia.channel.http.admin.common :as common]
            [xia.schedule :as schedule]))

(defn- parse-integer-list
  [value field-name]
  (cond
    (nil? value)
    nil

    (not (sequential? value))
    (throw (ex-info (str field-name " must be a list of integers")
                    {:field field-name}))

    :else
    (mapv (fn [entry]
            (try
              (Integer/parseInt (str (or entry "")))
              (catch Exception _
                (throw (ex-info (str field-name " must contain only integers")
                                {:field field-name
                                 :value entry})))))
          value)))

(defn- parse-schedule-type
  [value]
  (let [schedule-type (some-> value common/nonblank-str keyword)]
    (when-not (#{:tool :prompt} schedule-type)
      (throw (ex-info "invalid schedule type"
                      {:field "type"
                       :value value})))
    schedule-type))

(defn- parse-schedule-spec
  [data]
  (let [interval-minutes (common/parse-optional-positive-long (get data "interval_minutes")
                                                              "interval_minutes")
        minute           (parse-integer-list (get data "minute") "minute")
        hour             (parse-integer-list (get data "hour") "hour")
        dom              (parse-integer-list (get data "dom") "dom")
        month            (parse-integer-list (get data "month") "month")
        dow              (parse-integer-list (get data "dow") "dow")]
    (cond
      interval-minutes
      {:interval-minutes interval-minutes}

      (some identity [minute hour dom month dow])
      (cond-> {}
        minute (assoc :minute minute)
        hour   (assoc :hour hour)
        dom    (assoc :dom dom)
        month  (assoc :month month)
        dow    (assoc :dow dow))

      :else
      (throw (ex-info "missing schedule timing fields"
                      {:field "interval_minutes"})))))

(defn- infer-schedule-id
  [data]
  (let [name-text (common/nonblank-str (get data "name"))
        tool-id   (some-> (get data "tool_id") common/nonblank-str common/normalize-id-segment)
        prompt    (some-> (get data "prompt")
                          common/nonblank-str
                          (subs 0 (min 48 (count (common/nonblank-str (get data "prompt")))))
                          common/normalize-id-segment)
        base      (or (common/normalize-id-segment name-text)
                      tool-id
                      prompt
                      "schedule")
        used-ids  (map :id (schedule/list-schedules))]
    (common/next-available-id base used-ids)))

(defn schedule->admin-body
  [deps sched]
  (let [task-state (schedule/task-state (:id sched))
        latest-run (first (schedule/schedule-history (:id sched) 1))]
    {:id                        (some-> (:id sched) name)
     :name                      (:name sched)
     :description               (:description sched)
     :spec                      (:spec sched)
     :type                      (some-> (:type sched) name)
     :tool_id                   (some-> (:tool-id sched) name)
     :tool_args                 (:tool-args sched)
     :prompt                    (:prompt sched)
     :trusted                   (boolean (:trusted? sched))
     :enabled                   (boolean (:enabled? sched))
     :created_at                (common/instant->str deps (:created-at sched))
     :last_run                  (common/instant->str deps (:last-run sched))
     :next_run                  (common/instant->str deps (:next-run sched))
     :latest_status             (some-> (:status latest-run) name)
     :latest_error              (common/truncate-text deps (:error latest-run) 160)
     :task_status               (some-> (:status task-state) name)
     :task_phase                (some-> (:phase task-state) name)
     :task_last_error           (:last-error task-state)
     :task_backoff_until        (common/instant->str deps (:backoff-until task-state))
     :task_checkpoint_at        (common/instant->str deps (:checkpoint-at task-state))
     :task_last_success_at      (common/instant->str deps (:last-success-at task-state))
     :task_last_failure_at      (common/instant->str deps (:last-failure-at task-state))
     :task_consecutive_failures (or (:consecutive-failures task-state) 0)}))

(defn handle-save-schedule
  [deps req]
  (try
    (let [data          (or (common/read-body deps req) {})
          schedule-id   (if-let [id-text (common/nonblank-str (get data "id"))]
                          (common/parse-keyword-id id-text "id")
                          (infer-schedule-id data))
          existing      (schedule/get-schedule schedule-id)
          schedule-type (parse-schedule-type (get data "type"))
          name          (or (common/nonblank-str (get data "name"))
                            (some-> existing :name)
                            (name schedule-id))
          description   (if (contains? data "description")
                          (or (common/nonblank-str (get data "description")) "")
                          (:description existing))
          spec          (parse-schedule-spec data)
          tool-id       (when (= schedule-type :tool)
                          (common/parse-keyword-id (get data "tool_id") "tool_id"))
          tool-args     (when (= schedule-type :tool)
                          (common/parse-json-object-value (get data "tool_args") "tool_args"))
          prompt        (when (= schedule-type :prompt)
                          (common/nonblank-str (get data "prompt")))
          trusted?      (if (contains? data "trusted")
                          (true? (get data "trusted"))
                          (if existing
                            (boolean (:trusted? existing))
                            true))
          enabled?      (if (contains? data "enabled")
                          (true? (get data "enabled"))
                          (if existing
                            (boolean (:enabled? existing))
                            true))
          saved         (if existing
                          (schedule/update-schedule!
                            schedule-id
                            (cond-> {:name        name
                                     :description description
                                     :spec        spec
                                     :type        schedule-type
                                     :trusted?    trusted?
                                     :enabled?    enabled?}
                              (= schedule-type :tool)
                              (assoc :tool-id tool-id
                                     :tool-args tool-args)
                              (= schedule-type :prompt)
                              (assoc :prompt prompt)))
                          (do
                            (schedule/create-schedule!
                              (cond-> {:id          schedule-id
                                       :name        name
                                       :description description
                                       :spec        spec
                                       :type        schedule-type
                                       :trusted?    trusted?}
                                (= schedule-type :tool)
                                (assoc :tool-id tool-id
                                       :tool-args tool-args)
                                (= schedule-type :prompt)
                                (assoc :prompt prompt)))
                            (if enabled?
                              (schedule/get-schedule schedule-id)
                              (schedule/pause-schedule! schedule-id))))]
      (common/json-response deps 200 {:schedule (schedule->admin-body deps saved)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-delete-schedule
  [deps schedule-id]
  (try
    (let [schedule-key (common/parse-keyword-id schedule-id "schedule_id")]
      (if (schedule/get-schedule schedule-key)
        (do
          (schedule/remove-schedule! schedule-key)
          (common/json-response deps 200 {:status "deleted"
                                          :schedule_id (name schedule-key)}))
        (common/json-response deps 404 {:error "schedule not found"})))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-pause-schedule
  [deps schedule-id]
  (try
    (let [schedule-key (common/parse-keyword-id schedule-id "schedule_id")
          saved        (schedule/pause-schedule! schedule-key)]
      (common/json-response deps 200 {:schedule (schedule->admin-body deps saved)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-resume-schedule
  [deps schedule-id]
  (try
    (let [schedule-key (common/parse-keyword-id schedule-id "schedule_id")
          saved        (schedule/resume-schedule! schedule-key)]
      (common/json-response deps 200 {:schedule (schedule->admin-body deps saved)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))
