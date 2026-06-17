(ns xia.channel.http.routes
  "HTTP route matching and dispatch."
  (:require [xia.channel.http.admin :as http-admin]
            [xia.channel.http.command :as http-command]
            [xia.channel.http.messaging :as http-messaging]))

(defn route
  [deps req]
  (let [{:keys [admin-handler-deps
                command-handler-deps
                command-route-response
                exception-response
                handle-chat
                handle-clear-goal
                handle-close-session
                handle-create-artifact
                handle-create-artifact-scratch-pad
                handle-create-local-doc-scratch-pad
                handle-create-local-docs
                handle-create-scratch-pad
                handle-create-session
                handle-delete-artifact
                handle-delete-knowledge-fact
                handle-delete-local-doc
                handle-delete-scratch-pad
                handle-download-artifact
                handle-download-workspace-item
                handle-edit-scratch-pad
                handle-fork-task
                handle-get-approval
                handle-get-artifact
                handle-get-current-task
                handle-get-goal
                handle-get-live-task-events
                handle-get-llm-call
                handle-get-local-doc
                handle-get-prompt
                handle-get-scratch-pad
                handle-get-status
                handle-get-task
                handle-get-task-approval
                handle-get-task-event-stream
                handle-get-task-events
                handle-get-task-prompt
                handle-health
                handle-history-schedule-runs
                handle-history-schedules
                handle-history-sessions
                handle-history-tasks
                handle-home
                handle-interrupt-task
                handle-list-artifacts
                handle-list-knowledge-node-facts
                handle-list-llm-calls
                handle-list-local-docs
                handle-list-scratch-pads
                handle-list-workspace-items
                handle-local-close-session
                handle-local-get-status
                handle-local-goal
                handle-local-session-bootstrap
                handle-pause-goal
                handle-pause-task
                handle-resume-goal
                handle-resume-task
                handle-save-scratch-pad
                handle-search-knowledge-nodes
                handle-session-audit
                handle-session-messages
                handle-set-goal
                handle-steer-task
                handle-stop-task
                handle-submit-approval
                handle-submit-prompt
                handle-submit-task-approval
                handle-submit-task-prompt
                handle-task-board
                handle-web-dev-reload
                json-response
                protected-route-response
                static-asset-response
                static-asset-uri?
                websocket-handshake?
                workspace-handler-deps
                ws-handler]} deps]
    (try
      (let [uri    (:uri req)
            method (:request-method req)
            session-close-match (re-matches #"/sessions/([0-9a-fA-F-]+)" uri)
            session-match      (re-matches #"/sessions/([0-9a-fA-F-]+)/messages" uri)
            session-audit-match (re-matches #"/sessions/([0-9a-fA-F-]+)/audit" uri)
            session-task-match (re-matches #"/sessions/([0-9a-fA-F-]+)/task" uri)
            session-goal-match (re-matches #"/sessions/([0-9a-fA-F-]+)/goal" uri)
            session-goal-status-match (re-matches #"/sessions/([0-9a-fA-F-]+)/goal/status" uri)
            session-goal-pause-match (re-matches #"/sessions/([0-9a-fA-F-]+)/goal/pause" uri)
            session-goal-resume-match (re-matches #"/sessions/([0-9a-fA-F-]+)/goal/resume" uri)
            session-goal-clear-match (re-matches #"/sessions/([0-9a-fA-F-]+)/goal/clear" uri)
            status-match       (re-matches #"/sessions/([0-9a-fA-F-]+)/status" uri)
            prompt-match       (re-matches #"/sessions/([0-9a-fA-F-]+)/prompt" uri)
            approval-match     (re-matches #"/sessions/([0-9a-fA-F-]+)/approval" uri)
            command-session-close-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)" uri)
            command-session-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/messages" uri)
            command-session-audit-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/audit" uri)
            command-session-task-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/task" uri)
            command-session-goal-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/goal" uri)
            command-session-goal-status-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/goal/status" uri)
            command-session-goal-pause-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/goal/pause" uri)
            command-session-goal-resume-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/goal/resume" uri)
            command-session-goal-clear-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/goal/clear" uri)
            command-status-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/status" uri)
            command-runtime-status-match (= uri "/command/runtime/status")
            command-runtime-drain-match (= uri "/command/runtime/drain")
            command-runtime-undrain-match (= uri "/command/runtime/undrain")
            command-mcp-match (= uri "/command/mcp")
            command-managed-checkpoints-match (= uri "/command/managed/checkpoints")
            command-managed-checkpoint-match (re-matches #"/command/managed/checkpoints/([^/]+)" uri)
            command-managed-snapshots-match (= uri "/command/managed/snapshots")
            command-wake-projection-match (= uri "/command/managed/wake-projection")
            command-prompt-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/prompt" uri)
            command-approval-match (re-matches #"/command/sessions/([0-9a-fA-F-]+)/approval" uri)
            task-board-match    (= uri "/tasks/board")
            task-match         (re-matches #"/tasks/([0-9a-fA-F-]+)" uri)
            task-events-match  (re-matches #"/tasks/([0-9a-fA-F-]+)/events" uri)
            task-live-events-match (re-matches #"/tasks/([0-9a-fA-F-]+)/live-events" uri)
            task-stream-match  (re-matches #"/tasks/([0-9a-fA-F-]+)/stream" uri)
            task-prompt-match  (re-matches #"/tasks/([0-9a-fA-F-]+)/prompt" uri)
            task-approval-match (re-matches #"/tasks/([0-9a-fA-F-]+)/approval" uri)
            task-pause-match   (re-matches #"/tasks/([0-9a-fA-F-]+)/pause" uri)
            task-stop-match    (re-matches #"/tasks/([0-9a-fA-F-]+)/stop" uri)
            task-interrupt-match (re-matches #"/tasks/([0-9a-fA-F-]+)/interrupt" uri)
            task-steer-match   (re-matches #"/tasks/([0-9a-fA-F-]+)/steer" uri)
            task-fork-match    (re-matches #"/tasks/([0-9a-fA-F-]+)/fork" uri)
            task-resume-match  (re-matches #"/tasks/([0-9a-fA-F-]+)/resume" uri)
            history-schedule-match (re-matches #"/history/schedules/([^/]+)/runs" uri)
            scratch-list-match (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads" uri)
            scratch-pad-match  (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads/([^/]+)" uri)
            scratch-edit-match (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads/([^/]+)/edit" uri)
            local-doc-list-match (re-matches #"/sessions/([0-9a-fA-F-]+)/local-documents" uri)
            local-doc-match      (re-matches #"/sessions/([0-9a-fA-F-]+)/local-documents/([^/]+)" uri)
            local-doc-scratch-match (re-matches #"/sessions/([0-9a-fA-F-]+)/local-documents/([^/]+)/scratch-pads" uri)
            artifact-list-match (re-matches #"/sessions/([0-9a-fA-F-]+)/artifacts" uri)
            artifact-match      (re-matches #"/sessions/([0-9a-fA-F-]+)/artifacts/([^/]+)" uri)
            artifact-scratch-match (re-matches #"/sessions/([0-9a-fA-F-]+)/artifacts/([^/]+)/scratch-pads" uri)
            artifact-download-match (re-matches #"/sessions/([0-9a-fA-F-]+)/artifacts/([^/]+)/download" uri)
            workspace-list-match (= uri "/workspace/items")
            workspace-download-match (re-matches #"/workspace/items/([^/]+)/download" uri)
            knowledge-node-facts-match (re-matches #"/knowledge/nodes/([^/]+)/facts" uri)
            knowledge-fact-match (re-matches #"/knowledge/facts/([^/]+)" uri)
            admin-managed-instance-stop-match (re-matches #"/admin/managed-instances/([^/]+)/stop" uri)
            admin-site-match   (re-matches #"/admin/sites/([^/]+)" uri)
            admin-schedule-match (re-matches #"/admin/schedules/([^/]+)" uri)
            admin-schedule-pause-match (re-matches #"/admin/schedules/([^/]+)/pause" uri)
            admin-schedule-resume-match (re-matches #"/admin/schedules/([^/]+)/resume" uri)
            admin-plugin-enable-match (re-matches #"/admin/plugins/([^/]+)/enable" uri)
            admin-plugin-disable-match (re-matches #"/admin/plugins/([^/]+)/disable" uri)
            admin-skill-proposal-llm-review-match (re-matches #"/admin/skill-proposals/([0-9a-fA-F-]+)/llm-review" uri)
            admin-skill-proposal-approve-match (re-matches #"/admin/skill-proposals/([0-9a-fA-F-]+)/approve" uri)
            admin-skill-proposal-reject-match (re-matches #"/admin/skill-proposals/([0-9a-fA-F-]+)/reject" uri)
            admin-skill-update-check-match (re-matches #"/admin/skills/([^/]+)/update-check" uri)
            admin-skill-match  (re-matches #"/admin/skills/([^/]+)" uri)
            llm-call-match (re-matches #"/llm-calls/([0-9a-fA-F-]+)" uri)
            admin-oauth-match  (re-matches #"/admin/oauth-accounts/([^/]+)" uri)
            admin-oauth-connect-match (re-matches #"/admin/oauth-accounts/([^/]+)/connect" uri)
            admin-oauth-refresh-match (re-matches #"/admin/oauth-accounts/([^/]+)/refresh" uri)]
        (cond
          (and (= method :get) (= uri "/"))
          (handle-home req)

          (and (= method :get) (= uri "/local-session"))
          (handle-local-session-bootstrap req)

          (and (= method :get) (static-asset-uri? uri))
          (static-asset-response uri)

          (and (= method :get) (= uri "/__dev/web-reload"))
          (handle-web-dev-reload req)

          (and (= method :get) (= uri "/oauth/callback"))
          (http-admin/handle-oauth-callback (admin-handler-deps) req)

          (and (= uri "/ws") (websocket-handshake? req))
          (ws-handler req)

          (and (= method :post) (= uri "/sessions"))
          (protected-route-response req handle-create-session)

          (and (= method :post) (= uri "/chat"))
          (protected-route-response req #(handle-chat req))

          (and (= method :post) (= uri "/command/sessions"))
          (command-route-response req (fn [_req] (handle-create-session :command)))

          (and (= method :post) (= uri "/command/chat"))
          (command-route-response req #(handle-chat % :command))

          (and (= method :post) (= uri "/hooks/slack/events"))
          (http-messaging/handle-slack-events (workspace-handler-deps) req)

          (and (= method :post) (= uri "/hooks/telegram"))
          (http-messaging/handle-telegram-webhook (workspace-handler-deps) req)

          (and (= method :post) (= uri "/command/shutdown"))
          (command-route-response req #(http-command/handle-shutdown (command-handler-deps) %))

          (and (= method :get) command-runtime-status-match)
          (command-route-response req #(http-command/handle-runtime-status (command-handler-deps) %))

          (and (= method :post) command-runtime-drain-match)
          (command-route-response req #(http-command/handle-runtime-drain (command-handler-deps) %))

          (and (= method :post) command-runtime-undrain-match)
          (command-route-response req #(http-command/handle-runtime-undrain (command-handler-deps) %))

          (and (= method :post) command-mcp-match)
          (command-route-response req #(http-command/handle-mcp (command-handler-deps) %))

          (and (= method :post) command-managed-checkpoints-match)
          (command-route-response req #(http-command/handle-create-checkpoint (command-handler-deps) %))

          (and (= method :get) command-managed-checkpoint-match)
          (command-route-response req
                                  (fn [_req]
                                    (http-command/handle-get-checkpoint
                                     (command-handler-deps)
                                     (second command-managed-checkpoint-match))))

          (and (= method :get) command-managed-snapshots-match)
          (command-route-response req #(http-command/handle-list-snapshots (command-handler-deps) %))

          (and (= method :post) command-managed-snapshots-match)
          (command-route-response req #(http-command/handle-create-snapshot (command-handler-deps) %))

          (and (= method :get) command-wake-projection-match)
          (command-route-response req #(http-command/handle-wake-projection (command-handler-deps) %))

          (and (= method :delete) command-session-close-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-close-session (second command-session-close-match) :command)))

          (and (= method :get) command-status-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-get-status (second command-status-match) :command)))

          (and (= method :get) command-session-task-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-get-current-task (second command-session-task-match) :command)))

          (and (= method :get) command-session-goal-status-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-get-goal (second command-session-goal-status-match) :command)))

          (and (= method :get) command-session-goal-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-get-goal (second command-session-goal-match) :command)))

          (and (= method :post) command-session-goal-match)
          (command-route-response req
                                  #(handle-set-goal (second command-session-goal-match) % :command))

          (and (= method :post) command-session-goal-pause-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-pause-goal (second command-session-goal-pause-match) :command)))

          (and (= method :post) command-session-goal-resume-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-resume-goal (second command-session-goal-resume-match) :command)))

          (or (and (= method :post) command-session-goal-clear-match)
              (and (= method :delete) command-session-goal-match))
          (command-route-response req
                                  (fn [_req]
                                    (handle-clear-goal (second (or command-session-goal-clear-match
                                                                    command-session-goal-match))
                                                       :command)))

          (and (= method :get) command-prompt-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-get-prompt (second command-prompt-match) :command)))

          (and (= method :post) command-prompt-match)
          (command-route-response req #(handle-submit-prompt (second command-prompt-match) % :command))

          (and (= method :get) command-approval-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-get-approval (second command-approval-match) :command)))

          (and (= method :post) command-approval-match)
          (command-route-response req #(handle-submit-approval (second command-approval-match) % :command))

          (and (= method :get) command-session-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-session-messages (second command-session-match) :command)))

          (and (= method :get) command-session-audit-match)
          (command-route-response req
                                  (fn [_req]
                                    (handle-session-audit (second command-session-audit-match) :command)))

          (and (= method :delete) session-close-match)
          (protected-route-response req #(handle-local-close-session (second session-close-match)))

          (and (= method :get) status-match)
          (protected-route-response req #(handle-local-get-status (second status-match)))

          (and (= method :get) session-task-match)
          (protected-route-response req #(handle-get-current-task (second session-task-match) :http))

          (and (= method :get) session-goal-status-match)
          (protected-route-response req
                                    #(handle-local-goal (second session-goal-status-match)
                                                        handle-get-goal))

          (and (= method :get) session-goal-match)
          (protected-route-response req
                                    #(handle-local-goal (second session-goal-match)
                                                        handle-get-goal))

          (and (= method :post) session-goal-match)
          (protected-route-response req
                                    #(handle-local-goal (second session-goal-match)
                                                        (fn [sid expected-channel]
                                                          (handle-set-goal sid req expected-channel))))

          (and (= method :post) session-goal-pause-match)
          (protected-route-response req
                                    #(handle-local-goal (second session-goal-pause-match)
                                                        handle-pause-goal))

          (and (= method :post) session-goal-resume-match)
          (protected-route-response req
                                    #(handle-local-goal (second session-goal-resume-match)
                                                        handle-resume-goal))

          (or (and (= method :post) session-goal-clear-match)
              (and (= method :delete) session-goal-match))
          (protected-route-response req
                                    #(handle-local-goal (second (or session-goal-clear-match
                                                                    session-goal-match))
                                                        handle-clear-goal))

          (and (= method :get) prompt-match)
          (protected-route-response req #(handle-get-prompt (second prompt-match) :http))

          (and (= method :post) prompt-match)
          (protected-route-response req #(handle-submit-prompt (second prompt-match) req :http))

          (and (= method :get) approval-match)
          (protected-route-response req #(handle-get-approval (second approval-match) :http))

          (and (= method :post) approval-match)
          (protected-route-response req #(handle-submit-approval (second approval-match) req :http))

          (and (= method :get) session-match)
          (protected-route-response req #(handle-session-messages (second session-match) :http))

          (and (= method :get) session-audit-match)
          (protected-route-response req #(handle-session-audit (second session-audit-match) :http))

          (and (= method :get) (= uri "/history/sessions"))
          (protected-route-response req handle-history-sessions)

          (and (= method :get) (= uri "/history/tasks"))
          (protected-route-response req handle-history-tasks)

          (and (= method :get) (= uri "/history/schedules"))
          (protected-route-response req handle-history-schedules)

          (and (= method :get) history-schedule-match)
          (protected-route-response req #(handle-history-schedule-runs (second history-schedule-match)))

          (and (= method :get) task-board-match)
          (protected-route-response req handle-task-board)

          (and (= method :get) task-events-match)
          (protected-route-response req #(handle-get-task-events (second task-events-match)))

          (and (= method :get) task-live-events-match)
          (protected-route-response req #(handle-get-live-task-events (second task-live-events-match) req))

          (and (= method :get) task-stream-match)
          (protected-route-response req #(handle-get-task-event-stream (second task-stream-match) req))

          (and (= method :get) task-prompt-match)
          (protected-route-response req #(handle-get-task-prompt (second task-prompt-match)))

          (and (= method :post) task-prompt-match)
          (protected-route-response req #(handle-submit-task-prompt (second task-prompt-match) req))

          (and (= method :get) task-approval-match)
          (protected-route-response req #(handle-get-task-approval (second task-approval-match)))

          (and (= method :post) task-approval-match)
          (protected-route-response req #(handle-submit-task-approval (second task-approval-match) req))

          (and (= method :get) task-match)
          (protected-route-response req #(handle-get-task (second task-match)))

          (and (= method :post) task-pause-match)
          (protected-route-response req #(handle-pause-task (second task-pause-match)))

          (and (= method :post) task-stop-match)
          (protected-route-response req #(handle-stop-task (second task-stop-match)))

          (and (= method :post) task-interrupt-match)
          (protected-route-response req #(handle-interrupt-task (second task-interrupt-match)))

          (and (= method :post) task-steer-match)
          (protected-route-response req #(handle-steer-task (second task-steer-match) req))

          (and (= method :post) task-fork-match)
          (protected-route-response req #(handle-fork-task (second task-fork-match) req))

          (and (= method :post) task-resume-match)
          (protected-route-response req #(handle-resume-task (second task-resume-match) req))

          (and (= method :get) (= uri "/llm-calls"))
          (protected-route-response req #(handle-list-llm-calls req))

          (and (= method :get) llm-call-match)
          (protected-route-response req #(handle-get-llm-call (second llm-call-match)))

          (and (= method :get) scratch-list-match)
          (protected-route-response req #(handle-list-scratch-pads (second scratch-list-match)))

          (and (= method :post) scratch-list-match)
          (protected-route-response req #(handle-create-scratch-pad (second scratch-list-match) req))

          (and (= method :get) scratch-pad-match)
          (protected-route-response req #(handle-get-scratch-pad (second scratch-pad-match)
                                                                 (nth scratch-pad-match 2)))

          (and (= method :put) scratch-pad-match)
          (protected-route-response req #(handle-save-scratch-pad (second scratch-pad-match)
                                                                  (nth scratch-pad-match 2)
                                                                  req))

          (and (= method :delete) scratch-pad-match)
          (protected-route-response req #(handle-delete-scratch-pad (second scratch-pad-match)
                                                                    (nth scratch-pad-match 2)))

          (and (= method :post) scratch-edit-match)
          (protected-route-response req #(handle-edit-scratch-pad (second scratch-edit-match)
                                                                  (nth scratch-edit-match 2)
                                                                  req))

          (and (= method :get) local-doc-list-match)
          (protected-route-response req #(handle-list-local-docs (second local-doc-list-match)))

          (and (= method :post) local-doc-list-match)
          (protected-route-response req #(handle-create-local-docs (second local-doc-list-match) req))

          (and (= method :get) local-doc-match)
          (protected-route-response req #(handle-get-local-doc (second local-doc-match)
                                                               (nth local-doc-match 2)))

          (and (= method :delete) local-doc-match)
          (protected-route-response req #(handle-delete-local-doc (second local-doc-match)
                                                                  (nth local-doc-match 2)))

          (and (= method :post) local-doc-scratch-match)
          (protected-route-response req #(handle-create-local-doc-scratch-pad (second local-doc-scratch-match)
                                                                              (nth local-doc-scratch-match 2)))

          (and (= method :get) artifact-list-match)
          (protected-route-response req #(handle-list-artifacts (second artifact-list-match)))

          (and (= method :post) artifact-list-match)
          (protected-route-response req #(handle-create-artifact (second artifact-list-match) req))

          (and (= method :get) artifact-match)
          (protected-route-response req #(handle-get-artifact (second artifact-match)
                                                              (nth artifact-match 2)))

          (and (= method :get) artifact-download-match)
          (protected-route-response req #(handle-download-artifact (second artifact-download-match)
                                                                   (nth artifact-download-match 2)))

          (and (= method :post) artifact-scratch-match)
          (protected-route-response req #(handle-create-artifact-scratch-pad (second artifact-scratch-match)
                                                                             (nth artifact-scratch-match 2)))

          (and (= method :delete) artifact-match)
          (protected-route-response req #(handle-delete-artifact (second artifact-match)
                                                                 (nth artifact-match 2)))

          (and (= method :get) workspace-list-match)
          (protected-route-response req #(handle-list-workspace-items req))

          (and (= method :get) workspace-download-match)
          (protected-route-response req #(handle-download-workspace-item
                                           (second workspace-download-match)
                                           req))

          (and (= method :get) (= uri "/knowledge/nodes"))
          (protected-route-response req #(handle-search-knowledge-nodes req))

          (and (= method :get) knowledge-node-facts-match)
          (protected-route-response req #(handle-list-knowledge-node-facts
                                           (second knowledge-node-facts-match)))

          (and (= method :delete) knowledge-fact-match)
          (protected-route-response req #(handle-delete-knowledge-fact
                                           (second knowledge-fact-match)))

          (and (= method :get) (= uri "/admin/config"))
          (protected-route-response req #(http-admin/handle-admin-config (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/runtime-overlay/reload"))
          (protected-route-response req #(http-admin/handle-reload-runtime-overlay (admin-handler-deps) req))

          (and (= method :get) (= uri "/admin/managed-instances"))
          (protected-route-response req #(http-admin/handle-list-managed-instances (admin-handler-deps) req))

          (and (= method :post) admin-managed-instance-stop-match)
          (protected-route-response req #(http-admin/handle-stop-managed-instance (admin-handler-deps)
                                                                                  (second admin-managed-instance-stop-match)))

          (and (= method :post) (= uri "/admin/providers"))
          (protected-route-response req #(http-admin/handle-save-provider (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/provider-models"))
          (protected-route-response req #(http-admin/handle-fetch-provider-models (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/provider-model-metadata"))
          (protected-route-response req #(http-admin/handle-fetch-provider-model-metadata (admin-handler-deps) req))

          (and (= method :delete) (= uri "/admin/providers"))
          (protected-route-response req #(http-admin/handle-delete-provider (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/memory-retention"))
          (protected-route-response req #(http-admin/handle-save-memory-retention (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/identity"))
          (protected-route-response req #(http-admin/handle-save-identity (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/web-search"))
          (protected-route-response req #(http-admin/handle-save-web-search (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/context"))
          (protected-route-response req #(http-admin/handle-save-conversation-context (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/knowledge-decay"))
          (protected-route-response req #(http-admin/handle-save-knowledge-decay (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/local-doc-summarization"))
          (protected-route-response req #(http-admin/handle-save-local-doc-summarization (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/local-doc-ocr"))
          (protected-route-response req #(http-admin/handle-save-local-doc-ocr (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/database-backup"))
          (protected-route-response req #(http-admin/handle-save-database-backup (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/messaging"))
          (protected-route-response req #(http-admin/handle-save-messaging (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/oauth-accounts"))
          (protected-route-response req #(http-admin/handle-save-oauth-account (admin-handler-deps) req))

          (and (= method :post) admin-oauth-connect-match)
          (protected-route-response req #(http-admin/handle-start-oauth-connect (admin-handler-deps)
                                                                                (second admin-oauth-connect-match)
                                                                                req))

          (and (= method :post) admin-oauth-refresh-match)
          (protected-route-response req #(http-admin/handle-refresh-oauth-account (admin-handler-deps)
                                                                                  (second admin-oauth-refresh-match)))

          (and (= method :delete) admin-oauth-match)
          (protected-route-response req #(http-admin/handle-delete-oauth-account (admin-handler-deps)
                                                                                 (second admin-oauth-match)))

          (and (= method :post) (= uri "/admin/services"))
          (protected-route-response req #(http-admin/handle-save-service (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/sites"))
          (protected-route-response req #(http-admin/handle-save-site (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/schedules"))
          (protected-route-response req #(http-admin/handle-save-schedule (admin-handler-deps) req))

          (and (= method :post) admin-schedule-pause-match)
          (protected-route-response req #(http-admin/handle-pause-schedule (admin-handler-deps)
                                                                           (second admin-schedule-pause-match)))

          (and (= method :post) admin-schedule-resume-match)
          (protected-route-response req #(http-admin/handle-resume-schedule (admin-handler-deps)
                                                                            (second admin-schedule-resume-match)))

          (and (= method :post) (= uri "/admin/skills"))
          (protected-route-response req #(http-admin/handle-save-skill (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/skills/import-openclaw"))
          (protected-route-response req #(http-admin/handle-import-openclaw-skill (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/skills/curate"))
          (protected-route-response req #(http-admin/handle-curate-skills (admin-handler-deps) req))

          (and (= method :get) (= uri "/admin/skill-proposals"))
          (protected-route-response req #(http-admin/handle-skill-proposals (admin-handler-deps) req))

          (and (= method :post) (= uri "/admin/skill-proposals"))
          (protected-route-response req #(http-admin/handle-create-skill-proposal (admin-handler-deps) req))

          (and (= method :post) admin-skill-proposal-llm-review-match)
          (protected-route-response req #(http-admin/handle-llm-review-skill-proposal
                                           (admin-handler-deps)
                                           (second admin-skill-proposal-llm-review-match)
                                           req))

          (and (= method :post) admin-skill-proposal-approve-match)
          (protected-route-response req #(http-admin/handle-approve-skill-proposal
                                           (admin-handler-deps)
                                           (second admin-skill-proposal-approve-match)
                                           req))

          (and (= method :post) admin-skill-proposal-reject-match)
          (protected-route-response req #(http-admin/handle-reject-skill-proposal
                                           (admin-handler-deps)
                                           (second admin-skill-proposal-reject-match)
                                           req))

          (and (= method :post) (= uri "/admin/plugins"))
          (protected-route-response req #(http-admin/handle-save-plugin (admin-handler-deps) req))

          (and (= method :post) admin-plugin-enable-match)
          (protected-route-response req #(http-admin/handle-enable-plugin
                                           (admin-handler-deps)
                                           (second admin-plugin-enable-match)
                                           true))

          (and (= method :post) admin-plugin-disable-match)
          (protected-route-response req #(http-admin/handle-enable-plugin
                                           (admin-handler-deps)
                                           (second admin-plugin-disable-match)
                                           false))

          (and (= method :post) admin-skill-update-check-match)
          (protected-route-response req #(http-admin/handle-check-skill-update
                                           (admin-handler-deps)
                                           (second admin-skill-update-check-match)))

          (and (= method :delete) admin-site-match)
          (protected-route-response req #(http-admin/handle-delete-site (admin-handler-deps)
                                                                        (second admin-site-match)))

          (and (= method :delete) admin-schedule-match)
          (protected-route-response req #(http-admin/handle-delete-schedule (admin-handler-deps)
                                                                            (second admin-schedule-match)))

          (and (= method :get) admin-skill-match)
          (protected-route-response req #(http-admin/handle-get-skill (admin-handler-deps)
                                                                      (second admin-skill-match)))

          (and (= method :delete) admin-skill-match)
          (protected-route-response req #(http-admin/handle-delete-skill (admin-handler-deps)
                                                                         (second admin-skill-match)))

          (and (= method :get) (= uri "/skills"))
          (protected-route-response req #(http-admin/handle-skills (admin-handler-deps) req))

          (and (= method :get) (= uri "/health"))
          (handle-health req)

          :else
          (json-response 404 {:error "not found"})))
      (catch clojure.lang.ExceptionInfo e
        (exception-response e)))))
