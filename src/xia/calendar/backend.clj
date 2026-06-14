(ns xia.calendar.backend
  "Calendar backend protocol used by xia.calendar to dispatch provider operations.")

(defprotocol CalendarBackend
  (backend-key [backend]
    "Stable backend identifier keyword.")
  (backend-label [backend]
    "Human-readable backend label for errors and diagnostics.")
  (backend-default-service-id [backend]
    "Default service id for the backend when no configured service is detected.")
  (supports-service? [backend service]
    "Whether a saved service belongs to this backend.")
  (auto-detect-service-id [backend]
    "Return a configured service id for this backend, creating one when safe.")
  (backend-list-calendars [backend service-id opts]
    "List calendars for the backend.")
  (backend-list-events [backend service-id opts]
    "List events for the backend.")
  (backend-read-event [backend service-id calendar-id event-id opts]
    "Read one event for the backend.")
  (backend-create-event [backend service-id opts]
    "Create one event for the backend.")
  (backend-update-event [backend service-id calendar-id event-id opts]
    "Patch one event for the backend.")
  (backend-delete-event [backend service-id calendar-id event-id opts]
    "Delete one event for the backend.")
  (backend-find-availability [backend service-id opts]
    "Return free/busy information for the backend."))
