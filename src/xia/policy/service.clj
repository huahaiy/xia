(ns xia.policy.service
  "Service policy.")

(defn service-rate-limit-policy
  [service-id limit]
  {:decision-type :service-rate-limit-policy
   :allowed? false
   :mode :rate-limit
   :service-id service-id
   :limit (long limit)
   :reason (str "Rate limit exceeded for service " (name service-id)
                " (max " (long limit) " requests/minute)")})
