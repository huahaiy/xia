(ns xia.channel.http.status
  "HTTP session status and live task-runtime event state."
  (:require [xia.bridge :as bridge])
  (:import [java.util Date]))

(defn- session-statuses-atom
  [deps]
  (:session-statuses-atom deps))

(defn- runtime-event-store
  [deps]
  (:runtime-event-store deps))

(defn clear-session-status!
  [deps session-id]
  (when session-id
    (swap! (session-statuses-atom deps) dissoc (str session-id))))

(defn register-runtime-stream-subscriber!
  [deps task-id subscriber-id callback]
  (bridge/register-task-runtime-event-subscriber!
   (runtime-event-store deps)
   task-id
   subscriber-id
   callback))

(defn unregister-runtime-stream-subscriber!
  [deps task-id subscriber-id]
  (bridge/unregister-task-runtime-event-subscriber!
   (runtime-event-store deps)
   task-id
   subscriber-id))

(defn task-runtime-events-after
  [deps task-id stream-index]
  (bridge/task-runtime-events-after (runtime-event-store deps) task-id stream-index))

(defn latest-task-status-event
  [deps task-id]
  (bridge/latest-task-runtime-status-event (runtime-event-store deps) task-id))

(defn- terminal-status-state?
  [state]
  (contains? #{:completed :done :error :cancelled} state))

(defn status-handler
  [deps {:keys [session-id state] :as status}]
  (when-let [sid (some-> session-id str)]
    (if (terminal-status-state? state)
      (clear-session-status! deps sid)
      (swap! (session-statuses-atom deps) assoc sid (assoc status :updated-at (Date.))))))

(defn runtime-event-handler
  [deps event]
  (bridge/handle-task-runtime-event! (runtime-event-store deps) event))
