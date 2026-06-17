(ns xia.channel.http.admin
  "Stable facade for admin HTTP handlers.

   Handler implementations live in xia.channel.http.admin.* namespaces by
   vertical area. This namespace keeps existing route call sites stable."
  (:require [xia.channel.http.admin.config :as admin-config]
            [xia.channel.http.admin.instances :as admin-instances]
            [xia.channel.http.admin.oauth :as admin-oauth]
            [xia.channel.http.admin.plugins :as admin-plugins]
            [xia.channel.http.admin.providers :as admin-providers]
            [xia.channel.http.admin.schedules :as admin-schedules]
            [xia.channel.http.admin.services :as admin-services]
            [xia.channel.http.admin.sites :as admin-sites]
            [xia.channel.http.admin.skills :as admin-skills]))

(def handle-list-managed-instances admin-instances/handle-list-managed-instances)
(def handle-stop-managed-instance admin-instances/handle-stop-managed-instance)

(def handle-reload-runtime-overlay admin-config/handle-reload-runtime-overlay)
(def handle-admin-config admin-config/handle-admin-config)
(def handle-save-memory-retention admin-config/handle-save-memory-retention)
(def handle-save-web-search admin-config/handle-save-web-search)
(def handle-save-identity admin-config/handle-save-identity)
(def handle-save-conversation-context admin-config/handle-save-conversation-context)
(def handle-save-knowledge-decay admin-config/handle-save-knowledge-decay)
(def handle-save-local-doc-summarization admin-config/handle-save-local-doc-summarization)
(def handle-save-local-doc-ocr admin-config/handle-save-local-doc-ocr)
(def handle-save-database-backup admin-config/handle-save-database-backup)
(def handle-save-messaging admin-config/handle-save-messaging)

(def handle-fetch-provider-models admin-providers/handle-fetch-provider-models)
(def handle-fetch-provider-model-metadata admin-providers/handle-fetch-provider-model-metadata)
(def handle-save-provider admin-providers/handle-save-provider)
(def handle-delete-provider admin-providers/handle-delete-provider)

(def handle-save-service admin-services/handle-save-service)

(def handle-save-oauth-account admin-oauth/handle-save-oauth-account)
(def handle-delete-oauth-account admin-oauth/handle-delete-oauth-account)
(def handle-start-oauth-connect admin-oauth/handle-start-oauth-connect)
(def handle-refresh-oauth-account admin-oauth/handle-refresh-oauth-account)
(def handle-oauth-callback admin-oauth/handle-oauth-callback)

(def handle-save-site admin-sites/handle-save-site)
(def handle-delete-site admin-sites/handle-delete-site)

(def handle-save-schedule admin-schedules/handle-save-schedule)
(def handle-delete-schedule admin-schedules/handle-delete-schedule)
(def handle-pause-schedule admin-schedules/handle-pause-schedule)
(def handle-resume-schedule admin-schedules/handle-resume-schedule)

(def handle-save-skill admin-skills/handle-save-skill)
(def handle-get-skill admin-skills/handle-get-skill)
(def handle-delete-skill admin-skills/handle-delete-skill)
(def handle-check-skill-update admin-skills/handle-check-skill-update)
(def handle-curate-skills admin-skills/handle-curate-skills)
(def handle-import-openclaw-skill admin-skills/handle-import-openclaw-skill)
(def handle-skills admin-skills/handle-skills)
(def handle-skill-proposals admin-skills/handle-skill-proposals)
(def handle-create-skill-proposal admin-skills/handle-create-skill-proposal)
(def handle-approve-skill-proposal admin-skills/handle-approve-skill-proposal)
(def handle-llm-review-skill-proposal admin-skills/handle-llm-review-skill-proposal)
(def handle-reject-skill-proposal admin-skills/handle-reject-skill-proposal)

(def handle-save-plugin admin-plugins/handle-save-plugin)
(def handle-enable-plugin admin-plugins/handle-enable-plugin)
