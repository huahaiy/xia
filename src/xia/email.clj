(ns xia.email
  "Tool-facing email helpers.

   The provider implementations live under `xia.email.*`; this namespace keeps
   the stable public call surface used by bundled tools and SCI."
  (:require [xia.email.backend :as backend]
            [xia.email.impl :as impl]))

(def default-service-id impl/default-service-id)

(def EmailBackend backend/EmailBackend)
(def backend-key backend/backend-key)
(def backend-label backend/backend-label)
(def backend-default-service-id backend/backend-default-service-id)
(def supports-service? backend/supports-service?)
(def auto-detect-service-id backend/auto-detect-service-id)
(def backend-list-labels backend/backend-list-labels)
(def backend-list-messages backend/backend-list-messages)
(def backend-read-message backend/backend-read-message)
(def backend-send-message backend/backend-send-message)
(def backend-delete-message backend/backend-delete-message)
(def backend-update-message backend/backend-update-message)
(def backend-list-drafts backend/backend-list-drafts)
(def backend-read-draft backend/backend-read-draft)
(def backend-save-draft backend/backend-save-draft)
(def backend-send-draft backend/backend-send-draft)
(def backend-delete-draft backend/backend-delete-draft)

(def ^:private default-max-results @#'impl/default-max-results)
(def ^:private default-max-attachment-bytes @#'impl/default-max-attachment-bytes)
(def ^:private default-max-saved-attachment-bytes @#'impl/default-max-saved-attachment-bytes)

(defn- resolve-email-target
  [service-id]
  (#'impl/resolve-email-target service-id))

(defn list-labels
  "List labels using the detected email backend."
  [& {:keys [service-id]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend/backend-list-labels backend service-id {})))

(defn list-messages
  "List recent messages using the detected email backend."
  [& {:keys [service-id query max-results unread-only? inbox-only? page-token include-spam-trash?]
      :or   {max-results  default-max-results
             unread-only? false
             inbox-only?  true}}]
  (let [{service-id* :service-id
         query*      :query} (#'impl/list-request {:service-id service-id
                                                   :query query})
        {:keys [backend service-id]} (resolve-email-target service-id*)]
    (backend/backend-list-messages backend
                                   service-id
                                   {:query               query*
                                    :max-results         max-results
                                    :unread-only?        unread-only?
                                    :inbox-only?         inbox-only?
                                    :page-token          page-token
                                    :include-spam-trash? include-spam-trash?})))

(defn read-message
  "Read a message by id using the detected email backend."
  [message-id & {:keys [service-id include-attachment-data? max-attachment-bytes
                        save-attachments? max-saved-attachment-bytes]
                 :or   {include-attachment-data? false
                        max-attachment-bytes default-max-attachment-bytes
                        save-attachments? false
                        max-saved-attachment-bytes default-max-saved-attachment-bytes}}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)
        opts {:include-attachment-data? include-attachment-data?
              :max-attachment-bytes max-attachment-bytes
              :save-attachments? save-attachments?
              :max-saved-attachment-bytes max-saved-attachment-bytes}]
    (#'impl/finalize-attachment-artifacts
     (backend/backend-read-message backend
                                   service-id
                                   message-id
                                   (#'impl/attachment-fetch-opts opts))
     service-id
     message-id
     :message
     opts)))

(defn send-message
  "Send an email through the detected email backend."
  [to subject body & {:keys [cc bcc reply-to in-reply-to references thread-id service-id html-body attachments]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend/backend-send-message backend
                                  service-id
                                  to
                                  subject
                                  body
                                  {:cc          cc
                                   :bcc         bcc
                                   :reply-to    reply-to
                                   :in-reply-to in-reply-to
                                   :references  references
                                   :thread-id   thread-id
                                   :html-body   html-body
                                   :attachments attachments})))

(defn delete-message
  "Delete a message using the detected email backend."
  [message-id & {:keys [service-id permanent?]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend/backend-delete-message backend
                                    service-id
                                    message-id
                                    {:permanent? permanent?})))

(defn update-message
  "Update message or thread labels/state using the detected email backend."
  [message-id & {:keys [service-id archive? read? add-labels remove-labels]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend/backend-update-message backend
                                    service-id
                                    message-id
                                    {:archive?      archive?
                                     :read?         read?
                                     :add-labels    add-labels
                                     :remove-labels remove-labels})))

(defn list-drafts
  "List drafts using the detected email backend."
  [& {:keys [service-id query max-results page-token include-spam-trash?]
      :or   {max-results default-max-results}}]
  (let [{service-id* :service-id
         query*      :query} (#'impl/list-request {:service-id service-id
                                                   :query query})
        {:keys [backend service-id]} (resolve-email-target service-id*)]
    (backend/backend-list-drafts backend
                                 service-id
                                 {:query               query*
                                  :max-results         max-results
                                  :page-token          page-token
                                  :include-spam-trash? include-spam-trash?})))

(defn read-draft
  "Read a draft by id using the detected email backend."
  [draft-id & {:keys [service-id include-attachment-data? max-attachment-bytes
                      save-attachments? max-saved-attachment-bytes]
               :or   {include-attachment-data? false
                      max-attachment-bytes default-max-attachment-bytes
                      save-attachments? false
                      max-saved-attachment-bytes default-max-saved-attachment-bytes}}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)
        opts {:include-attachment-data? include-attachment-data?
              :max-attachment-bytes max-attachment-bytes
              :save-attachments? save-attachments?
              :max-saved-attachment-bytes max-saved-attachment-bytes}]
    (#'impl/finalize-attachment-artifacts
     (backend/backend-read-draft backend
                                 service-id
                                 draft-id
                                 (#'impl/attachment-fetch-opts opts))
     service-id
     draft-id
     :draft
     opts)))

(defn save-draft
  "Create or update a draft using the detected email backend."
  [to subject body & {:keys [draft-id cc bcc reply-to in-reply-to references thread-id service-id html-body attachments]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend/backend-save-draft backend
                                service-id
                                to
                                subject
                                body
                                {:draft-id    draft-id
                                 :cc          cc
                                 :bcc         bcc
                                 :reply-to    reply-to
                                 :in-reply-to in-reply-to
                                 :references  references
                                 :thread-id   thread-id
                                 :html-body   html-body
                                 :attachments attachments})))

(defn send-draft
  "Send an existing draft using the detected email backend."
  [draft-id & {:keys [service-id]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend/backend-send-draft backend service-id draft-id {})))

(defn delete-draft
  "Delete a draft using the detected email backend."
  [draft-id & {:keys [service-id]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend/backend-delete-draft backend service-id draft-id {})))
