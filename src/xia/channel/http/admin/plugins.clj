(ns xia.channel.http.admin.plugins
  "Plugin admin HTTP handlers."
  (:require [clojure.string :as str]
            [xia.channel.http.admin.common :as common]
            [xia.db :as db]
            [xia.plugin :as plugin]))

(defn- sort-by-name
  [entries]
  (->> entries
       (sort-by (fn [entry]
                  (str/lower-case (or (:name entry) (:id entry) ""))))
       vec))

(defn plugin->admin-body
  [deps plugin]
  (let [manifest (:plugin/manifest plugin)]
    {:id           (some-> (:plugin/id plugin) name)
     :name         (:plugin/name plugin)
     :description  (:plugin/description plugin)
     :version      (:plugin/version plugin)
     :enabled      (boolean (:plugin/enabled? plugin))
     :capabilities (mapv str (sort-by str (:plugin/capabilities plugin)))
     :hooks        (mapv (fn [{:keys [id event]}]
                           {:id (some-> id name)
                            :event (some-> event name)})
                         (:hooks manifest))
     :installed_at (common/instant->str deps (:plugin/installed-at plugin))
     :updated_at   (common/instant->str deps (:plugin/updated-at plugin))}))

(defn handle-save-plugin
  [deps req]
  (try
    (let [data     (or (common/read-body deps req) {})
          manifest (or (get data "manifest")
                       (get data :manifest)
                       data)
          saved    (plugin/install-plugin! manifest)]
      (common/json-response deps 200
                            {:status "saved"
                             :plugin (plugin->admin-body deps saved)
                             :plugins (->> (db/list-plugins)
                                           (into [] (map #(plugin->admin-body deps %)))
                                           sort-by-name)}))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))

(defn handle-enable-plugin
  [deps plugin-id enabled?]
  (try
    (let [plugin-id* (common/parse-keyword-id plugin-id "plugin_id")
          existing   (db/get-plugin plugin-id*)]
      (if existing
        (let [saved (plugin/enable-plugin! plugin-id* enabled?)]
          (common/json-response deps 200
                                {:status (if enabled? "enabled" "disabled")
                                 :plugin (plugin->admin-body deps saved)
                                 :plugins (->> (db/list-plugins)
                                               (into [] (map #(plugin->admin-body deps %)))
                                               sort-by-name)}))
        (common/json-response deps 404 {:error "plugin not found"})))
    (catch clojure.lang.ExceptionInfo e
      (common/exception-response deps e))))
