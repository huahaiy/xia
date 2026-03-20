(ns xia.rate-limit
  (:import [java.util Map$Entry]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicLong]))

(defn- recent-timestamps
  [timestamps cutoff]
  (let [cutoff* (long cutoff)]
    (reduce (fn [acc timestamp]
              (let [timestamp* (long timestamp)]
                (if (> timestamp* cutoff*)
                  (conj acc timestamp*)
                  acc)))
            []
            timestamps)))

(defn consume-slot!
  "Atomically record a request timestamp within a sliding window. Throws when
   the caller has already consumed the allowed number of slots."
  [state now window-ms limit error-fn]
  (let [now* (long now)
        window-ms* (long window-ms)]
    (swap! state
           (fn [{:keys [timestamps]}]
             (let [cutoff (- now* window-ms*)
                   recent (recent-timestamps timestamps cutoff)]
               (when (>= (long (count recent)) (long limit))
                 (throw (error-fn)))
               {:timestamps (conj recent now*)
                :cleaned    now*})))))

(defn maybe-prune-states!
  "Occasionally remove inactive rate-limit buckets from a concurrent map.
   `cleanup-state` should be an AtomicLong holding the last sweep timestamp."
  [^ConcurrentHashMap states ^AtomicLong cleanup-state now window-ms]
  (let [now* (long now)
        window-ms* (long window-ms)
        sweep-interval-ms window-ms*]
    (loop []
      (let [last-sweep (.get cleanup-state)]
        (cond
          (< (- now* last-sweep) sweep-interval-ms)
          nil

          (not (.compareAndSet cleanup-state last-sweep now*))
          (recur)

          :else
          (let [cutoff (- now* window-ms*)]
            (doseq [^Map$Entry entry (iterator-seq (.iterator (.entrySet states)))]
              (let [key (.getKey entry)
                    state (.getValue entry)
                    {:keys [timestamps cleaned]} @state
                    recent (recent-timestamps timestamps cutoff)]
                (cond
                  (seq recent)
                  (when (not= recent timestamps)
                    (swap! state assoc :timestamps recent))

                  (<= (long (or cleaned 0)) cutoff)
                  (.remove states key state)

                  :else
                  (when (seq timestamps)
                    (swap! state assoc :timestamps [])))))))))))
