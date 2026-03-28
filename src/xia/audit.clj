(ns xia.audit
  "Shared helpers for persisting interactive audit events."
  (:require [taoensso.timbre :as log]
            [xia.db :as db]))

(defn log!
  ([event]
   (log! nil event))
  ([ctx event]
   (when-let [session-id (:session-id ctx)]
     (try
       (db/log-audit-event!
         (merge {:session-id session-id
                 :channel    (:channel ctx)}
                event))
       (catch Exception e
         (log/debug e "Failed to persist audit event"))))))
