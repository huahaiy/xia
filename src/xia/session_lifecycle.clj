(ns xia.session-lifecycle
  "Shared session lifecycle helpers for channel adapters and schedulers."
  (:require [taoensso.timbre :as log]
            [xia.db :as db]
            [xia.hippocampus :as hippo]
            [xia.prompt :as prompt]
            [xia.working-memory :as wm])
  (:import [java.util UUID]))

(def default-finalize-lock-count 256)

(defn make-finalize-locks
  ([] (make-finalize-locks default-finalize-lock-count))
  ([lock-count]
   (vec (repeatedly lock-count #(Object.)))))

(defn parse-session-id
  "Return the canonical session UUID string, or nil for invalid input."
  [session-id]
  (cond
    (instance? UUID session-id)
    (str session-id)

    :else
    (try
      (some-> session-id UUID/fromString str)
      (catch IllegalArgumentException _
        nil)
      (catch NullPointerException _
        nil))))

(defn session-id-str
  [session-id]
  (cond
    (instance? UUID session-id) (str session-id)
    :else                      (parse-session-id session-id)))

(defn session-uuid
  [session-id]
  (some-> (session-id-str session-id) UUID/fromString))

(defn session-eid
  [session-id]
  (when-let [sid (session-uuid session-id)]
    (ffirst (db/q '[:find ?e :in $ ?sid
                    :where
                    [?e :session/id ?sid]]
                  sid))))

(defn session-exists?
  [session-id]
  (boolean (session-eid session-id)))

(defn session-channel
  [session-id]
  (when-let [eid (session-eid session-id)]
    (ffirst (db/q '[:find ?channel :in $ ?e
                    :where
                    [?e :session/channel ?channel]]
                  eid))))

(defn session-accessible?
  [session-id expected-channel]
  (when-let [sid (session-id-str session-id)]
    (and (session-exists? sid)
         (or (nil? expected-channel)
             (= expected-channel (session-channel sid))))))

(defn active?
  [session-id]
  (when-let [eid (session-eid session-id)]
    (boolean (:session/active? (db/entity eid)))))

(defn set-active!
  [session-id active?]
  (when-let [sid (session-uuid session-id)]
    (db/set-session-active! sid active?)))

(defn create!
  "Create a session and ensure working memory is ready."
  ([channel]
   (create! channel nil))
  ([channel opts]
   (let [session-id (if (some? opts)
                      (db/create-session! channel opts)
                      (db/create-session! channel))]
     (wm/ensure-wm! session-id)
     {:session-id session-id
      :channel channel})))

(defn- finalize-lock
  [locks session-id]
  (when-let [sid (session-id-str session-id)]
    (let [lock-count (count locks)]
      (when (pos? lock-count)
        (nth locks
             (mod (bit-and Integer/MAX_VALUE (int (hash sid)))
                  lock-count))))))

(defn with-finalize-lock
  [locks session-id f]
  (if-let [lock (finalize-lock locks session-id)]
    (locking lock
      (f))
    (f)))

(defn clear-session-state!
  "Clear lifecycle side state owned outside the DB session entity."
  [session-id & {:keys [clear-status! cancel-finalizer!]}]
  (when session-id
    (let [sid (str session-id)]
      (when clear-status!
        (clear-status! sid))
      (prompt/clear-pending-interaction! {:session-id sid})
      (when cancel-finalizer!
        (cancel-finalizer! sid)))))

(defn resume!
  "Mark an inactive session active and ensure working memory is installed.

  Returns true when the session was actually resumed."
  [session-id & {:keys [expected-channel locks touch!]}]
  (when (and (session-accessible? session-id expected-channel)
             (not (active? session-id)))
    (when-let [sid (session-uuid session-id)]
      (with-finalize-lock
        locks
        sid
        (fn []
          (when (and (session-accessible? sid expected-channel)
                     (not (active? sid)))
            (set-active! sid true)
            (wm/ensure-wm! sid)
            (when touch!
              (touch! sid))
            (log/info "Resumed session" (str sid)
                      "channel" (name (or expected-channel
                                          (session-channel sid)
                                          :unknown)))
            true))))))

(defn- record-conversation!
  [session-id channel topics consolidation-mode]
  (apply hippo/record-conversation!
         session-id
         channel
         (cond-> [:topics topics]
           (some? consolidation-mode)
           (conj :consolidation-mode consolidation-mode))))

(defn finalize!
  "Persist session working memory, clear runtime state, and optionally close it.

  Returns true when an active session was finalized, false when the session was
  already inactive, and nil when `session-id` is invalid."
  [session-id & {:keys [locks reason default-channel clear-state!
                        mark-inactive? consolidation-mode]
                 :or   {reason :explicit
                        default-channel :http
                        mark-inactive? true}}]
  (when-let [sid (session-uuid session-id)]
    (with-finalize-lock
      locks
      sid
      (fn []
        (let [sid-str     (str sid)
              channel     (or (session-channel sid) default-channel)
              was-active? (active? sid)
              reason*     (or reason :explicit)]
          (try
            (when was-active?
              (let [topics (:topics (wm/get-wm sid))]
                (try
                  (wm/clear-autonomy-state! sid)
                  (catch Exception e
                    (log/error e "Failed to clear session autonomy state during finalization"
                               sid-str
                               "channel" (name channel)
                               "reason" (name reason*))))
                (try
                  (wm/snapshot! sid)
                  (catch Exception e
                    (log/error e "Failed to snapshot session working memory during finalization"
                               sid-str
                               "channel" (name channel)
                               "reason" (name reason*))))
                (try
                  (record-conversation! sid channel topics consolidation-mode)
                  (catch Exception e
                    (log/warn e "Failed to record session conversation during finalization"
                              sid-str
                              "channel" (name channel)
                              "reason" (name reason*))))))
            (catch Exception e
              (log/error e "Failed to finalize session"
                         sid-str
                         "channel" (name channel)
                         "reason" (name reason*)))
            (finally
              (try
                (wm/clear-wm! sid)
                (catch Exception e
                  (log/error e "Failed to clear session working memory"
                             sid-str
                             "channel" (name channel))))
              (when clear-state!
                (clear-state! sid))
              (when (and mark-inactive? was-active?)
                (try
                  (set-active! sid false)
                  (catch Exception e
                    (log/error e "Failed to mark session inactive"
                               sid-str
                               "channel" (name channel)))))))
          (when was-active?
            (log/info "Finalized session"
                      sid-str
                      "channel" (name channel)
                      "reason" (name reason*)))
          was-active?)))))
