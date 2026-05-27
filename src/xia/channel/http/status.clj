(ns xia.channel.http.status
  "HTTP session status and live task-runtime event state."
  (:require [taoensso.timbre :as log])
  (:import [java.util Date]))

(def ^:private max-live-task-runtime-events 200)

(defn- session-statuses-atom
  [deps]
  (:session-statuses-atom deps))

(defn- task-runtime-events-atom
  [deps]
  (:task-runtime-events-atom deps))

(defn- task-runtime-stream-subscribers-atom
  [deps]
  (:task-runtime-stream-subscribers-atom deps))

(defn clear-session-status!
  [deps session-id]
  (when session-id
    (swap! (session-statuses-atom deps) dissoc (str session-id))))

(defn- append-runtime-event!
  [deps event]
  (when-let [task-id (some-> (:task-id event) str)]
    (let [received-at (Date.)]
      (-> (swap! (task-runtime-events-atom deps)
                 (fn [state]
                   (let [{:keys [next-index events]} (get state task-id)
                         next-index* (inc (long (or next-index 0)))
                         event* (assoc event
                                       :stream-index next-index*
                                       :received-at received-at)
                         events* (conj (vec (or events [])) event*)
                         trimmed (if (> (count events*) max-live-task-runtime-events)
                                   (subvec events* (- (count events*) max-live-task-runtime-events))
                                   events*)]
                     (assoc state task-id {:next-index next-index*
                                           :events trimmed}))))
          (get task-id)
          :events
          last))))

(defn register-runtime-stream-subscriber!
  [deps task-id subscriber-id callback]
  (when (and task-id subscriber-id callback)
    (swap! (task-runtime-stream-subscribers-atom deps)
           update
           (str task-id)
           (fnil assoc {})
           subscriber-id
           callback)))

(defn unregister-runtime-stream-subscriber!
  [deps task-id subscriber-id]
  (when (and task-id subscriber-id)
    (swap! (task-runtime-stream-subscribers-atom deps)
           (fn [state]
             (let [task-key (str task-id)
                   subscribers (dissoc (get state task-key {}) subscriber-id)]
               (if (seq subscribers)
                 (assoc state task-key subscribers)
                 (dissoc state task-key)))))))

(defn- notify-runtime-stream-subscribers!
  [deps event]
  (when-let [task-id (some-> (:task-id event) str)]
    (doseq [[subscriber-id callback] (get @(task-runtime-stream-subscribers-atom deps) task-id)]
      (try
        (callback event)
        (catch Exception e
          (log/warn e "Failed to deliver runtime event to task stream subscriber"
                    "task" task-id
                    "subscriber" subscriber-id)
          (unregister-runtime-stream-subscriber! deps task-id subscriber-id))))))

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
  (when-let [event* (append-runtime-event! deps event)]
    (notify-runtime-stream-subscribers! deps event*)))
