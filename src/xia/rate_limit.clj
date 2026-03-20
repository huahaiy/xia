(ns xia.rate-limit)

(defn consume-slot!
  "Atomically record a request timestamp within a sliding window. Throws when
   the caller has already consumed the allowed number of slots."
  [state now window-ms limit error-fn]
  (swap! state
         (fn [{:keys [timestamps]}]
           (let [cutoff (- (long now) (long window-ms))
                 recent (filterv #(> (long %) cutoff) timestamps)]
             (when (>= (long (count recent)) (long limit))
               (throw (error-fn)))
             {:timestamps (conj recent now)
              :cleaned    now}))))
