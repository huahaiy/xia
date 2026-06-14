(ns xia.calendar
  "Tool-facing calendar helpers.

   The provider implementations live under `xia.calendar.*`; this namespace
   keeps the stable public call surface used by bundled tools and SCI."
  (:require [xia.calendar.backend :as backend]
            [xia.calendar.impl :as impl]))

(def default-service-id impl/default-service-id)

(def CalendarBackend backend/CalendarBackend)
(def backend-key backend/backend-key)
(def backend-label backend/backend-label)
(def backend-default-service-id backend/backend-default-service-id)
(def supports-service? backend/supports-service?)
(def auto-detect-service-id backend/auto-detect-service-id)
(def backend-list-calendars backend/backend-list-calendars)
(def backend-list-events backend/backend-list-events)
(def backend-read-event backend/backend-read-event)
(def backend-create-event backend/backend-create-event)
(def backend-update-event backend/backend-update-event)
(def backend-delete-event backend/backend-delete-event)
(def backend-find-availability backend/backend-find-availability)

(defn list-calendars
  "List calendars using the detected calendar backend."
  [& opts]
  (apply impl/list-calendars opts))

(defn list-events
  "List upcoming or bounded events using the detected calendar backend."
  [& opts]
  (apply impl/list-events opts))

(defn read-event
  "Read a calendar event by id."
  [event-id & opts]
  (apply impl/read-event event-id opts))

(defn create-event
  "Create a calendar event."
  [summary start end & opts]
  (apply impl/create-event summary start end opts))

(defn update-event
  "Patch a calendar event by id."
  [event-id & opts]
  (apply impl/update-event event-id opts))

(defn delete-event
  "Delete a calendar event by id."
  [event-id & opts]
  (apply impl/delete-event event-id opts))

(defn find-availability
  "Find busy blocks for one or more calendars over a required time range."
  [& opts]
  (apply impl/find-availability opts))
