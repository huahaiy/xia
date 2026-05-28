(ns xia.channel.http.admin.instances
  "Managed-instance admin HTTP handlers."
  (:require [xia.channel.http.admin.common :as common]
            [xia.instance-supervisor :as instance-supervisor]))

(defn instance->admin-body
  [deps instance]
  {:instance_id       (:instance_id instance)
   :service_id        (:service_id instance)
   :service_name      (:service_name instance)
   :base_url          (:base_url instance)
   :port              (:port instance)
   :pid               (:pid instance)
   :state             (:state instance)
   :alive             (boolean (:alive instance))
   :attached          (boolean (:attached instance))
   :template_instance (:template_instance instance)
   :log_path          (:log_path instance)
   :started_at        (common/instant->str deps (:started_at instance))
   :exited_at         (common/instant->str deps (:exited_at instance))
   :exit_code         (:exit_code instance)})

(defn handle-list-managed-instances
  [deps _req]
  (common/json-response deps 200
                        {:instances (mapv #(instance->admin-body deps %)
                                          (instance-supervisor/list-managed-instances))}))

(defn handle-stop-managed-instance
  [deps instance-id]
  (let [stopped (instance-supervisor/stop-instance! instance-id)]
    (common/json-response deps 200
                          {:status "stopped"
                           :instance (instance->admin-body deps stopped)})))
