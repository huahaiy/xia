(ns xia.email.backend
  "Email backend protocol used by xia.email to dispatch provider operations.")

(defprotocol EmailBackend
  (backend-key [backend]
    "Stable backend identifier keyword.")
  (backend-label [backend]
    "Human-readable backend label for errors and diagnostics.")
  (backend-default-service-id [backend]
    "Default service id for the backend when no configured service is detected.")
  (supports-service? [backend service]
    "Whether a saved service belongs to this backend.")
  (auto-detect-service-id [backend]
    "Return a configured service id for this backend, creating one if the backend
     supports an auto-managed service and the current account state allows it.")
  (backend-list-labels [backend service-id opts]
    "List labels for the backend.")
  (backend-list-messages [backend service-id opts]
    "List recent messages for the backend.")
  (backend-read-message [backend service-id message-id opts]
    "Read one message from the backend.")
  (backend-send-message [backend service-id to subject body opts]
    "Send one message through the backend.")
  (backend-delete-message [backend service-id message-id opts]
    "Delete or trash one message through the backend.")
  (backend-update-message [backend service-id message-id opts]
    "Update message or thread labels/state through the backend.")
  (backend-list-drafts [backend service-id opts]
    "List drafts for the backend.")
  (backend-read-draft [backend service-id draft-id opts]
    "Read one draft from the backend.")
  (backend-save-draft [backend service-id to subject body opts]
    "Create or update one draft through the backend.")
  (backend-send-draft [backend service-id draft-id opts]
    "Send one existing draft through the backend.")
  (backend-delete-draft [backend service-id draft-id opts]
    "Delete one draft through the backend."))
