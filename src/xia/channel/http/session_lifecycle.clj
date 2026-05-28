(ns xia.channel.http.session-lifecycle
  "REST session lifecycle and idle-finalizer wiring for HTTP channels."
  (:require [xia.bridge :as bridge]
            [xia.session-lifecycle :as session-life])
  (:import [java.util.concurrent ScheduledExecutorService ScheduledFuture TimeUnit]))

(def ^:private default-idle-timeout-ms (* 30 60 1000))
(def ^:private rest-session-channels #{:http :command})
(def ^:private local-ui-session-channels #{:http :websocket})
(def ^:private busy-session-states #{:running :waiting_input :waiting_approval})

(defn parse-session-id
  [session-id]
  (session-life/parse-session-id session-id))

(defn session-exists?
  [session-id]
  (session-life/session-exists? session-id))

(defn session-id-str
  [session-id]
  (session-life/session-id-str session-id))

(defn session-uuid
  [session-id]
  (session-life/session-uuid session-id))

(defn session-channel
  [session-id]
  (session-life/session-channel session-id))

(defn session-accessible?
  [session-id expected-channel]
  (session-life/session-accessible? session-id expected-channel))

(defn active?
  [session-id]
  (session-life/active? session-id))

(defn rest-session-channel?
  [channel]
  (contains? rest-session-channels channel))

(defn local-ui-session-allowed?
  [session-id]
  (contains? local-ui-session-channels
             (session-channel session-id)))

(defn session-busy?
  [deps session-id]
  (let [state (:state (get @(:session-statuses-atom deps) (str session-id)))]
    (contains? busy-session-states
               (cond
                 (keyword? state) state
                 (string? state) (keyword state)
                 :else state))))

(declare finalize!)

(defn cancel-finalizer!
  [deps session-id]
  (when-let [sid (session-id-str session-id)]
    (when-let [^ScheduledFuture future (get @(:rest-session-finalizers-atom deps) sid)]
      (.cancel future false))
    (swap! (:rest-session-finalizers-atom deps) dissoc sid)))

(defn clear-finalizers!
  [deps]
  (doseq [[_ ^ScheduledFuture future] @(:rest-session-finalizers-atom deps)]
    (.cancel future false))
  (reset! (:rest-session-finalizers-atom deps) {}))

(defn- schedule-finalizer!
  [deps session-id]
  (when-let [sid (session-id-str session-id)]
    (cancel-finalizer! deps sid)
    (when-let [^ScheduledExecutorService exec @(:rest-session-finalizer-executor-atom deps)]
      (let [task ^Runnable
            (fn []
              (swap! (:rest-session-finalizers-atom deps) dissoc sid)
              (finalize! deps sid :idle-timeout))]
        (swap! (:rest-session-finalizers-atom deps) assoc
               sid
               (.schedule exec task (long default-idle-timeout-ms) TimeUnit/MILLISECONDS))))))

(defn touch!
  [deps session-id]
  (when (active? session-id)
    (schedule-finalizer! deps session-id)))

(defn maybe-resume!
  [deps session-id expected-channel]
  (when (and (= expected-channel :http)
             (session-accessible? session-id expected-channel)
             (not (active? session-id)))
    (bridge/resume-session! session-id
                            :expected-channel expected-channel
                            :locks (:session-finalize-locks deps)
                            :touch! #(touch! deps %))))

(defn clear-state!
  [deps session-id]
  (session-life/clear-session-state! session-id
                                     :clear-status! (:clear-session-status! deps)
                                     :cancel-finalizer! #(cancel-finalizer! deps %)))

(defn finalize!
  ([deps session-id]
   (finalize! deps session-id :explicit))
  ([deps session-id reason]
   (bridge/finalize-session! session-id
                             :locks (:session-finalize-locks deps)
                             :reason reason
                             :default-channel :http
                             :clear-state! #(clear-state! deps %)
                             :mark-inactive? true
                             :consolidation-mode :sync)))
