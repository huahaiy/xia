(ns xia.tool.builtin
  "First-party tool handlers.

   Bundled tool EDN points at these vars with :handler-var. User/plugin tools
   can still use SCI handler strings."
  (:require [xia.agent :as agent]
            [xia.artifact :as artifact]
            [xia.board :as board]
            [xia.browser :as browser]
            [xia.calendar :as calendar]
            [xia.email :as email]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.local-doc :as local-doc]
            [xia.memory :as memory]
            [xia.memory-edit :as memory-edit]
            [xia.peer :as peer]
            [xia.pipeline :as pipeline]
            [xia.schedule :as schedule]
            [xia.web :as web]
            [xia.workspace :as workspace]))

(defn- redact-artifact
  [artifact]
  (dissoc artifact :text :sha256 :meta))

(defn- redact-local-doc
  [doc]
  (dissoc doc :text :sha256))

(defn- redact-imported-local-doc
  [doc]
  (dissoc doc :sha256))

(defn- redact-workspace-item
  [item]
  (dissoc item :payload-path :sha256 :meta))

(defn- redact-workspace-imported-artifact
  [artifact]
  (dissoc artifact :sha256 :meta :blob-id :blob-codec :compressed-size-bytes))

(defn artifact-create
  [args]
  (let [spec (cond-> {:name       (get args "name")
                      :title      (get args "title")
                      :kind       (get args "kind")
                      :media-type (get args "media_type")
                      :meta       (get args "meta")
                      :source     :agent}
               (contains? args "content") (assoc :content (get args "content"))
               (contains? args "rows") (assoc :rows (get args "rows"))
               (contains? args "data") (assoc :data (get args "data"))
               (contains? args "bytes_base64") (assoc :bytes-base64 (get args "bytes_base64")))
        artifact (artifact/create-artifact! spec)]
    (redact-artifact artifact)))

(defn artifact-delete
  [args]
  (artifact/delete-artifact! (get args "artifact_id")))

(defn artifact-list
  [args]
  (mapv redact-artifact
        (artifact/list-visible-artifacts :top (or (get args "top") 20))))

(defn artifact-read
  [args]
  (artifact/read-visible-artifact (get args "artifact_id")
                                  :offset (or (get args "offset") 0)
                                  :max-chars (or (get args "max_chars") 4000)))

(defn artifact-search
  [args]
  (artifact/search-visible-artifacts (get args "query")
                                     :top (or (get args "top") 5)))

(defn board-claim
  [args]
  (board/claim-card!
    (get args "card_id")
    {:assignee (get args "assignee")
     :claim-token (get args "claim_token")}))

(defn board-comment
  [args]
  (board/comment-card!
    (get args "card_id")
    {:author (get args "author")
     :text (get args "text")}))

(defn board-create
  [args]
  (board/create-card!
    {:title (get args "title")
     :description (get args "description")
     :priority (get args "priority")
     :assignee (get args "assignee")
     :parent-id (get args "parent_id")}))

(defn board-heartbeat
  [args]
  (board/heartbeat-card!
    (get args "card_id")
    {:claim-token (get args "claim_token")}))

(defn board-list
  [args]
  (board/list-cards
    {:status (get args "status")
     :assignee (get args "assignee")
     :include-terminal? (get args "include_terminal")
     :limit (get args "limit")}))

(defn board-update
  [args]
  (board/update-card!
    (get args "card_id")
    {:claim-token (get args "claim_token")
     :title (get args "title")
     :description (get args "description")
     :status (get args "status")
     :priority (get args "priority")
     :assignee (get args "assignee")}))

(defn branch-tasks
  [args]
  (agent/run-branch-tasks
    (or (get args "tasks") [])
    :objective (get args "objective")
    :max-parallel (get args "max_parallel")
    :max-tool-rounds (get args "max_rounds")))

(defn browser-bootstrap-runtime
  [args]
  (browser/bootstrap-browser-runtime! :backend (or (get args "backend") "auto")))

(defn browser-click
  [args]
  (browser/click (get args "session_id")
                 (get args "selector")))

(defn browser-close
  [args]
  (browser/close-session (get args "session_id")))

(defn browser-fill-form
  [args]
  (browser/fill-form (get args "session_id")
                     (get args "fields")
                     :form-selector (get args "form_selector")
                     :submit (boolean (get args "submit"))))

(defn browser-install-deps
  [args]
  (browser/install-browser-deps!
    :dry-run (if (contains? args "dry_run")
               (boolean (get args "dry_run"))
               true)))

(defn browser-list-sessions
  [_args]
  (browser/list-sessions))

(defn browser-list-sites
  [_args]
  (browser/list-sites))

(defn browser-login-interactive
  [args]
  (browser/login-interactive (get args "url")
                             (get args "fields")
                             :backend (get args "backend")))

(defn browser-login
  [args]
  (browser/login (get args "site")
                 :backend (get args "backend")))

(defn browser-navigate
  [args]
  (browser/navigate (get args "session_id")
                    (get args "url")))

(defn browser-open
  [args]
  (let [js      (get args "js")
        backend (get args "backend")]
    (browser/open-session (get args "url")
                          :js (if (nil? js) true js)
                          :backend backend)))

(defn browser-query-elements
  [args]
  (browser/query-elements (get args "session_id")
                          :kind (get args "kind")
                          :selector (get args "selector")
                          :text-contains (get args "text_contains")
                          :visible-only (boolean (get args "visible_only"))
                          :offset (or (get args "offset") 0)
                          :limit (or (get args "limit") 25)))

(defn browser-read-page
  [args]
  (browser/read-page (get args "session_id")))

(defn browser-runtime-status
  [_args]
  (browser/browser-runtime-status))

(defn browser-screenshot
  [args]
  (browser/screenshot (get args "session_id")
                      :full-page (boolean (get args "full_page"))
                      :detail (or (get args "detail") "auto")))

(defn browser-wait
  [args]
  (browser/wait-for-page (get args "session_id")
                         :timeout-ms (or (get args "timeout_ms") 10000)
                         :interval-ms (or (get args "interval_ms") 500)
                         :selector (get args "selector")
                         :text (get args "text")
                         :url-contains (get args "url_contains")))

(defn calendar-availability
  [args]
  (calendar/find-availability
    :service-id (get args "service_id")
    :calendars (or (get args "calendars")
                   (get args "calendar_ids"))
    :time-min (get args "time_min")
    :time-max (get args "time_max")
    :time-zone (get args "time_zone")
    :interval-minutes (get args "interval_minutes")))

(defn calendar-event-create
  [args]
  (calendar/create-event
    (get args "summary")
    (get args "start")
    (get args "end")
    :description (get args "description")
    :location (get args "location")
    :attendees (get args "attendees")
    :calendar-id (get args "calendar_id")
    :service-id (get args "service_id")
    :time-zone (get args "time_zone")
    :all-day? (boolean (get args "all_day"))
    :recurrence (get args "recurrence")
    :transparency (get args "transparency")
    :visibility (get args "visibility")
    :show-as (get args "show_as")
    :sensitivity (get args "sensitivity")
    :send-updates (get args "send_updates")
    :html? (boolean (get args "html"))))

(defn calendar-event-delete
  [args]
  (calendar/delete-event
    (get args "event_id")
    :calendar-id (get args "calendar_id")
    :service-id (get args "service_id")
    :send-updates (get args "send_updates")))

(defn calendar-event-list
  [args]
  (calendar/list-events
    :service-id (get args "service_id")
    :calendar-id (get args "calendar_id")
    :time-min (get args "time_min")
    :time-max (get args "time_max")
    :query (get args "query")
    :max-results (or (get args "max_results") 10)
    :page-token (get args "page_token")
    :include-cancelled? (boolean (get args "include_cancelled"))
    :time-zone (get args "time_zone")))

(defn calendar-event-read
  [args]
  (calendar/read-event
    (get args "event_id")
    :calendar-id (get args "calendar_id")
    :service-id (get args "service_id")
    :time-zone (get args "time_zone")))

(defn calendar-event-update
  [args]
  (calendar/update-event
    (get args "event_id")
    :summary (get args "summary")
    :start (get args "start")
    :end (get args "end")
    :description (get args "description")
    :location (get args "location")
    :attendees (get args "attendees")
    :calendar-id (get args "calendar_id")
    :service-id (get args "service_id")
    :time-zone (get args "time_zone")
    :all-day? (get args "all_day")
    :recurrence (get args "recurrence")
    :transparency (get args "transparency")
    :visibility (get args "visibility")
    :show-as (get args "show_as")
    :sensitivity (get args "sensitivity")
    :send-updates (get args "send_updates")
    :html? (boolean (get args "html"))))

(defn calendar-list
  [args]
  (calendar/list-calendars
    :service-id (get args "service_id")
    :max-results (or (get args "max_results") 10)
    :page-token (get args "page_token")
    :include-hidden? (boolean (get args "include_hidden"))
    :time-zone (get args "time_zone")))

(defn email-delete
  [args]
  (email/delete-message
    (get args "message_id")
    :permanent? (boolean (get args "permanent"))
    :service-id (get args "service_id")))

(defn email-draft-delete
  [args]
  (email/delete-draft
    (get args "draft_id")
    :service-id (get args "service_id")))

(defn email-draft-list
  [args]
  (email/list-drafts
    :service-id (get args "service_id")
    :query (get args "query")
    :max-results (or (get args "max_results") 10)
    :page-token (get args "page_token")
    :include-spam-trash? (boolean (get args "include_spam_trash"))))

(defn email-draft-read
  [args]
  (email/read-draft
    (get args "draft_id")
    :include-attachment-data? (boolean (get args "include_attachment_data"))
    :max-attachment-bytes (or (get args "max_attachment_bytes") 262144)
    :save-attachments? (boolean (get args "save_attachments"))
    :max-saved-attachment-bytes (or (get args "max_saved_attachment_bytes") 26214400)
    :service-id (get args "service_id")))

(defn email-draft-save
  [args]
  (email/save-draft
    (get args "to")
    (get args "subject")
    (get args "body")
    :draft-id (get args "draft_id")
    :cc (get args "cc")
    :bcc (get args "bcc")
    :reply-to (get args "reply_to")
    :html-body (get args "html_body")
    :in-reply-to (get args "in_reply_to")
    :references (get args "references")
    :thread-id (get args "thread_id")
    :attachments (get args "attachments")
    :service-id (get args "service_id")))

(defn email-draft-send
  [args]
  (email/send-draft
    (get args "draft_id")
    :service-id (get args "service_id")))

(defn email-label-list
  [args]
  (email/list-labels
    :service-id (get args "service_id")))

(defn email-list
  [args]
  (email/list-messages
    :service-id (get args "service_id")
    :query (get args "query")
    :max-results (or (get args "max_results") 10)
    :page-token (get args "page_token")
    :unread-only? (boolean (get args "unread_only"))
    :inbox-only? (if (contains? args "inbox_only")
                   (boolean (get args "inbox_only"))
                   true)
    :include-spam-trash? (boolean (get args "include_spam_trash"))))

(defn email-read
  [args]
  (email/read-message
    (get args "message_id")
    :include-attachment-data? (boolean (get args "include_attachment_data"))
    :max-attachment-bytes (or (get args "max_attachment_bytes") 262144)
    :save-attachments? (boolean (get args "save_attachments"))
    :max-saved-attachment-bytes (or (get args "max_saved_attachment_bytes") 26214400)
    :service-id (get args "service_id")))

(defn email-send
  [args]
  (email/send-message
    (get args "to")
    (get args "subject")
    (get args "body")
    :cc (get args "cc")
    :bcc (get args "bcc")
    :reply-to (get args "reply_to")
    :html-body (get args "html_body")
    :in-reply-to (get args "in_reply_to")
    :references (get args "references")
    :thread-id (get args "thread_id")
    :attachments (get args "attachments")
    :service-id (get args "service_id")))

(defn email-update
  [args]
  (email/update-message
    (get args "message_id")
    :archive? (when (contains? args "archive")
                (boolean (get args "archive")))
    :read? (when (contains? args "read")
             (boolean (get args "read")))
    :add-labels (get args "add_labels")
    :remove-labels (get args "remove_labels")
    :service-id (get args "service_id")))

(defn local-doc-read
  [args]
  (local-doc/read-visible-doc (get args "doc_id")
                              :offset (or (get args "offset") 0)
                              :max-chars (or (get args "max_chars") 4000)))

(defn local-doc-search
  [args]
  (local-doc/search-visible-docs (get args "query")
                                 :top (or (get args "top") 5)))

(defn memory-correct-fact
  [args]
  (memory-edit/correct-fact!
    {:fact-id (get args "fact_id")
     :old-fact (get args "old_fact")
     :corrected-fact (get args "corrected_fact")
     :entity-name (get args "entity_name")}))

(defn peer-chat
  [args]
  (peer/chat
    (get args "service_id")
    (get args "message")
    :session-id (get args "session_id")
    :timeout-ms (get args "timeout_ms")))

(defn peer-instance-list
  [_args]
  {:instances (instance-supervisor/list-managed-instances)})

(defn peer-instance-start
  [args]
  (instance-supervisor/start-instance!
    (get args "instance_id")
    :template-instance (get args "template_instance")
    :port (get args "port")
    :service-id (get args "service_id")
    :service-name (get args "service_name")
    :wait-for-ready-ms (get args "wait_for_ready_ms")))

(defn peer-instance-status
  [args]
  (if-let [status (instance-supervisor/instance-status
                    (get args "instance_id"))]
    status
    (throw (ex-info "Managed Xia instance not found"
                    {:type :instance-supervisor/not-found
                     :instance-id (get args "instance_id")}))))

(defn peer-instance-stop
  [args]
  (instance-supervisor/stop-instance!
    (get args "instance_id")
    :timeout-ms (get args "timeout_ms")))

(defn peer-list
  [_args]
  {:peers (peer/list-peers)})

(defn pipeline-run
  [args]
  (pipeline/run! args))

(defn recent-work
  [args]
  (let [episode-limit  (or (get args "episode_limit") 5)
        artifact-limit (or (get args "artifact_limit") 5)
        document-limit (or (get args "document_limit") 5)
        browser-limit  (or (get args "browser_limit") 5)]
    {:recent_episodes (memory/recent-episodes episode-limit)
     :browser_sessions (->> (browser/list-sessions)
                            (sort-by (fn [session]
                                       [(long (or (:age-seconds session) 0))
                                        (str (:session-id session))]))
                            (take browser-limit)
                            vec)
     :artifacts (mapv redact-artifact
                      (artifact/list-visible-artifacts :top artifact-limit))
     :local_documents (mapv redact-local-doc
                            (local-doc/list-visible-docs :top document-limit))}))

(defn schedule-create
  [args]
  (let [id     (get args "id")
        nm     (get args "name")
        desc   (get args "description")
        typ    (get args "type")
        ival   (get args "interval_minutes")
        mins   (get args "minute")
        hrs    (get args "hour")
        doms   (get args "dom")
        mos    (get args "month")
        dows   (get args "dow")
        tid    (get args "tool_id")
        targs  (get args "tool_args")
        prompt (get args "prompt")
        spec   (if ival
                 {:interval-minutes ival}
                 (cond-> {}
                   mins (assoc :minute (set mins))
                   hrs (assoc :hour (set hrs))
                   doms (assoc :dom (set doms))
                   mos (assoc :month (set mos))
                   dows (assoc :dow (set dows))))]
    (schedule/create-schedule!
      (cond-> {:id (keyword id) :spec spec :type (keyword typ)}
        nm (assoc :name nm)
        desc (assoc :description desc)
        tid (assoc :tool-id (keyword tid))
        targs (assoc :tool-args targs)
        prompt (assoc :prompt prompt)))))

(defn schedule-list
  [_args]
  (schedule/list-schedules))

(defn schedule-manage
  [args]
  (let [sid    (keyword (get args "id"))
        action (get args "action")]
    (case action
      "pause" (schedule/pause-schedule! sid)
      "resume" (schedule/resume-schedule! sid)
      "remove" (schedule/remove-schedule! sid)
      "history" (schedule/safe-schedule-history sid)
      "details" (schedule/get-schedule sid))))

(defn web-extract
  [args]
  (web/extract-data (get args "url")
                    (get args "selectors")))

(defn web-fetch
  [args]
  (web/fetch-page (get args "url")
                  :max-tokens (or (get args "max_tokens") 2000)))

(defn web-search
  [args]
  (web/search-web (get args "query")
                  :max-results (or (get args "max_results") 5)))

(defn workspace-import-artifact
  [args]
  (redact-workspace-imported-artifact
    (workspace/import-item-as-artifact!
      (get args "item_id")
      :workspace-id (get args "workspace_id")
      :name (get args "name")
      :title (get args "title"))))

(defn workspace-import-doc
  [args]
  (redact-imported-local-doc
    (workspace/import-item-as-local-doc!
      (get args "item_id")
      :workspace-id (get args "workspace_id")
      :name (get args "name")
      :ocr-mode (get args "ocr_mode"))))

(defn workspace-list
  [args]
  {:workspace_id (or (get args "workspace_id") "default")
   :items (mapv redact-workspace-item
                (workspace/list-items
                  :workspace-id (get args "workspace_id")
                  :top (or (get args "top") 20)
                  :source-type (get args "source_type")))})

(defn workspace-publish-artifact
  [args]
  (redact-workspace-item
    (workspace/publish-artifact!
      (get args "artifact_id")
      :workspace-id (get args "workspace_id")
      :name (get args "name"))))

(defn workspace-publish-doc
  [args]
  (redact-workspace-item
    (workspace/publish-local-doc!
      (get args "doc_id")
      :workspace-id (get args "workspace_id")
      :name (get args "name"))))

(defn workspace-read
  [args]
  (redact-workspace-item
    (workspace/read-item
      (get args "item_id")
      :workspace-id (get args "workspace_id")
      :offset (or (get args "offset") 0)
      :max-chars (or (get args "max_chars") 4000))))

(defn workspace-write-note
  [args]
  (redact-workspace-item
    (workspace/write-note!
      (get args "content")
      :workspace-id (get args "workspace_id")
      :title (get args "title")
      :name (get args "name")
      :media-type (get args "media_type"))))
