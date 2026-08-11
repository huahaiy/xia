(ns xia.channel.http.command
  "Machine command-channel HTTP handlers."
  (:require [taoensso.timbre :as log]
            [xia.bridge :as bridge]
            [xia.channel.http.common :as http-common]
            [xia.checkpoint :as checkpoint]
            [xia.mcp :as mcp]
            [xia.runtime-health :as runtime-health]
            [xia.runtime-state :as runtime-state]
            [xia.snapshot :as snapshot]
            [xia.wake-projection :as wake-projection]))

(defn idle-body
  [deps]
  (let [{:keys [phase draining? drain-requested-at accepting-new-work? idle? shutdown-allowed? blockers activity]}
        (runtime-health/idle-status)
        memory-consolidation (bridge/memory-consolidation-summary)]
    {:phase (some-> phase name)
     :draining draining?
     :drain_requested_at (http-common/instant->str deps drain-requested-at)
     :accepting_new_work accepting-new-work?
     :idle idle?
     :shutdown_allowed shutdown-allowed?
     :blockers (mapv (fn [{:keys [component kind count reason]}]
                       {:component (some-> component name)
                        :kind (some-> kind name)
                        :count count
                        :reason reason})
                     blockers)
     :activity {"agent" {"active_session_turn_count" (get-in activity [:agent :active-session-turn-count] 0)
                         "active_session_run_count" (get-in activity [:agent :active-session-run-count] 0)
                         "active_task_run_count" (get-in activity [:agent :active-task-run-count] 0)}
                "scheduler" {"running" (boolean (get-in activity [:scheduler :running?]))
                             "running_schedule_count" (get-in activity [:scheduler :running-schedule-count] 0)
                             "maintenance_running" (boolean (get-in activity [:scheduler :maintenance-running?]))}
                "hippocampus" {"accepting" (boolean (get-in activity [:hippocampus :accepting?]))
                               "pending_background_task_count" (get-in activity [:hippocampus :pending-background-task-count] 0)}
                "llm" {"accepting" (boolean (get-in activity [:llm :accepting?]))
                       "pending_log_write_count" (get-in activity [:llm :pending-log-write-count] 0)}}
     :memory_consolidation memory-consolidation}))

(defn handle-shutdown
  [deps _req]
  (if-let [handler ((:command-shutdown-handler deps))]
    (let [{:keys [shutdown-allowed?]} (runtime-health/idle-status)]
      (if shutdown-allowed?
        (do
          (future
            (try
              (handler)
              (catch Throwable e
                (log/error e "Command shutdown handler failed"))))
          (http-common/json-response deps 202 {:status "stopping"}))
        (http-common/json-response deps 409
                                   (assoc (idle-body deps)
                                          :error "runtime must be draining and idle before shutdown"))))
    (http-common/json-response deps 503 {:error "shutdown control unavailable"})))

(defn handle-runtime-status
  [deps _req]
  (http-common/json-response deps 200 (idle-body deps)))

(defn handle-runtime-drain
  [deps _req]
  (runtime-state/request-drain!)
  (http-common/json-response deps 200 (idle-body deps)))

(defn handle-runtime-undrain
  [deps _req]
  (runtime-state/clear-drain!)
  (http-common/json-response deps 200 (idle-body deps)))

(defn handle-wake-projection
  [deps _req]
  (let [projection (wake-projection/current-snapshot)]
    (cond-> (http-common/json-response deps 200 projection)
      (:projection_seq projection)
      (assoc-in [:headers "ETag"] (str "\"" (:projection_seq projection) "\"")))))

(defn handle-mcp
  [deps req]
  (let [request  (or (http-common/read-body deps req) {})
        response (mcp/handle-json-rpc
                  request
                  {:request-id  (str (random-uuid))
                   :remote-addr (:remote-addr req)})]
    (if response
      (http-common/json-response deps 200 response)
      (http-common/json-response deps 202 {:ok true}))))

(defn handle-create-checkpoint
  [deps req]
  (let [body         (or (http-common/read-body deps req) {})
        staging-root (http-common/nonblank-str deps (get body "staging_root"))
        checkpoint*  (checkpoint/submit-online-checkpoint!
                      (cond-> {}
                        staging-root
                        (assoc :staging-root staging-root)))]
    (http-common/json-response deps 202 checkpoint*)))

(defn handle-get-checkpoint
  [deps checkpoint-id]
  (if-let [status (checkpoint/checkpoint-status checkpoint-id)]
    (http-common/json-response deps 200 status)
    (http-common/json-response deps 404 {:error "checkpoint not found"})))

(defn- snapshot-body
  [snapshot*]
  {:snapshot_id (:snapshot/id snapshot*)
   :label (:snapshot/label snapshot*)
   :created_at (:snapshot/created-at snapshot*)
   :path (:snapshot/path snapshot*)
   :db {:source_path (get-in snapshot* [:db :source-path])
        :archive (get-in snapshot* [:db :archive])}
   :workspace {:included (boolean (get-in snapshot* [:workspace :included?]))
               :source_root (get-in snapshot* [:workspace :source-root])
               :entry (get-in snapshot* [:workspace :entry])}})

(defn handle-list-snapshots
  [deps _req]
  (http-common/json-response deps 200
                             {:snapshots (mapv snapshot-body
                                               (snapshot/list-snapshots))}))

(defn handle-create-snapshot
  [deps req]
  (let [body               (or (http-common/read-body deps req) {})
        label              (http-common/nonblank-str deps (get body "label"))
        snapshot-root      (http-common/nonblank-str deps (get body "snapshot_root"))
        include-workspace? (not= false (get body "include_workspace"))
        snapshot*          (snapshot/create-snapshot!
                            :label label
                            :snapshot-root snapshot-root
                            :include-workspace? include-workspace?)]
    (http-common/json-response deps 201 (snapshot-body snapshot*))))
