(ns xia.instance-supervisor
  "Host-side supervisor for starting and stopping local Xia child instances.

   This is intentionally outside SCI's general-purpose sandbox escape hatch:
   it only launches Xia itself, on loopback, behind a host capability that
   child Xia instances automatically disable."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.http-client :as http-client]
            [xia.paths :as paths]
            [xia.runtime :as runtime])
  (:import [java.io File IOException]
           [java.lang ProcessBuilder$Redirect ProcessHandle]
           [java.net ServerSocket]
           [java.security SecureRandom]
           [java.util Base64 Date]
           [java.util.concurrent TimeUnit]))

(def ^:private default-command "xia")
(def ^:private default-bind "127.0.0.1")
(def ^:private default-ready-timeout-ms 20000)
(def ^:private default-stop-timeout-ms 10000)
(def ^:private poll-interval-ms 200)
(def ^:private managed-service-prefix "xia-managed-instance-")
(def ^:private allowed-bind-hosts #{"127.0.0.1" "localhost" "::1" "[::1]"})
(def ^:private instance-management-config-key :instance/management-enabled?)
(def ^:private parent-instance-config-key :instance/parent-instance-id)
(def ^:private parent-instance-env "XIA_PARENT_INSTANCE_ID")

(defonce ^:private capability-state (atom {:enabled? false
                                           :command nil}))
(defonce ^:private managed-instances (atom {}))

(defn- nonblank-string
  [value]
  (let [text (some-> value str str/trim)]
    (when (seq text)
      text)))

(defn- env-value
  [k]
  (System/getenv k))

(defn- current-process-command
  []
  (try
    (some-> (ProcessHandle/current)
            .info
            .command
            .orElse
            nonblank-string)
    (catch Throwable _
      nil)))

(defn- resolve-instance-command
  [command]
  (or (nonblank-string command)
      (some-> (env-value "XIA_INSTANCE_COMMAND") nonblank-string)
      (when (runtime/native-image?)
        (current-process-command))
      default-command))

(defn configure!
  [{:keys [enabled? command]}]
  (reset! capability-state {:enabled? (boolean enabled?)
                            :command  (when enabled?
                                        (resolve-instance-command command))})
  nil)

(defn- host-capability-enabled?
  []
  (:enabled? @capability-state))

(defn- default-controller-instance?
  []
  (= paths/default-instance-id
     (some-> (db/current-instance-id) str paths/normalize-instance-id)))

(defn instance-management-configured?
  []
  (cfg/boolean-option instance-management-config-key
                      (default-controller-instance?)))

(defn instance-management-enabled?
  []
  (and (host-capability-enabled?)
       (instance-management-configured?)))

(defn instance-command
  []
  (:command @capability-state))

(defn capabilities
  []
  {:instance_management_configured (instance-management-configured?)
   :instance_management_enabled    (instance-management-enabled?)})

(declare shutdown!)

(defn set-instance-management-enabled!
  [enabled?]
  (db/set-config! instance-management-config-key
                  (if enabled? "true" "false"))
  (when-not enabled?
    (shutdown!))
  (capabilities))

(defn- require-capability!
  []
  (when-not (instance-management-configured?)
    (throw (ex-info "Controller mode is disabled for this Xia instance. Enable it in Settings before starting or stopping child Xia instances."
                    {:type :instance-supervisor/capability-disabled
                     :capability :instance-management
                     :scope :instance})))
  (when-not (host-capability-enabled?)
    (throw (ex-info "Host instance management is disabled for this Xia process."
                    {:type :instance-supervisor/capability-disabled
                     :capability :instance-management
                     :scope :host})))
  (when-not (nonblank-string (instance-command))
    (throw (ex-info "No Xia command is configured for starting child instances"
                    {:type :instance-supervisor/command-unavailable}))))

(defn- normalize-instance-id
  [value field type-key]
  (or (some-> value paths/normalize-instance-id)
      (throw (ex-info (str field " must be a non-empty string")
                      {:type type-key
                       :field field
                       :value value}))))

(defn- normalize-bind
  [bind]
  (let [bind* (or (nonblank-string bind) default-bind)]
    (when-not (contains? allowed-bind-hosts bind*)
      (throw (ex-info "Child Xia instances may only bind to loopback addresses"
                      {:type :instance-supervisor/invalid-bind
                       :bind bind*})))
    bind*))

(defn- normalize-port
  [port]
  (when (some? port)
    (let [port* (long port)]
      (when-not (<= 1 port* 65535)
        (throw (ex-info "port must be between 1 and 65535"
                        {:type :instance-supervisor/invalid-port
                         :port port})))
      (int port*))))

(defn- normalize-service-id
  [instance-id service-id]
  (or (some-> service-id paths/normalize-instance-id)
      (str managed-service-prefix instance-id)))

(defn- normalize-template-instance
  [value]
  (when (some? value)
    (normalize-instance-id value "template_instance" :instance-supervisor/invalid-template-instance)))

(defn- reserve-port
  []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

(defn- service-base-url
  [bind port]
  (let [host (case bind
               "::1" "[::1]"
               "[::1]" "[::1]"
               "localhost" "localhost"
               "127.0.0.1")]
    (str "http://" host ":" (long port))))

(defn- managed-service-name
  [instance-id service-name]
  (or (nonblank-string service-name)
      (str "Managed Xia " instance-id)))

(defn- random-token
  []
  (let [bytes (byte-array 24)
        _     (.nextBytes (SecureRandom.) bytes)]
    (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bytes)))

(defn- process-alive?
  [process]
  (.isAlive ^Process process))

(defn- process-pid
  [process]
  (try
    (.pid ^Process process)
    (catch Throwable _
      nil)))

(defn- process-exit-value
  [process]
  (try
    (.exitValue ^Process process)
    (catch IllegalThreadStateException _
      nil)))

(defn- wait-for-exit
  [process timeout-ms]
  (.waitFor ^Process process (long timeout-ms) TimeUnit/MILLISECONDS))

(defn- destroy-process!
  [process force?]
  (if force?
    (.destroyForcibly ^Process process)
    (.destroy ^Process process)))

(defn- instance-log-path
  [instance-id]
  (let [root (paths/default-instance-root instance-id)
        _    (.mkdirs (io/file root))]
    (paths/path-str root "xia.log")))

(defn- spawn-process!
  [command args env log-path]
  (let [builder (ProcessBuilder. (into-array String (cons command args)))
        log-file (io/file log-path)]
    (.mkdirs (.getParentFile log-file))
    (.redirectErrorStream builder true)
    (.redirectOutput builder (ProcessBuilder$Redirect/appendTo log-file))
    (let [env-map (.environment builder)]
      (doseq [[k v] env]
        (if (some? v)
          (.put env-map ^String k ^String (str v))
          (.remove env-map ^String k))))
    (try
      (.start builder)
      (catch IOException e
        (throw (ex-info "Failed to start child Xia process"
                        {:type :instance-supervisor/start-failed
                         :command command
                         :args args
                         :log-path log-path}
                        e))))))

(defn- child-ready?
  [base-url]
  (try
    (= 200 (:status (http-client/request {:method :get
                                          :url (str base-url "/health")
                                          :timeout 1000
                                          :as :text
                                          :allow-private-network? true})))
    (catch Exception _
      false)))

(defn- wait-until-ready!
  [base-url process wait-for-ready-ms log-path]
  (let [deadline (+ (System/currentTimeMillis)
                    (long (or wait-for-ready-ms default-ready-timeout-ms)))]
    (loop []
      (cond
        (child-ready? base-url)
        true

        (not (process-alive? process))
        (throw (ex-info "Child Xia instance exited before becoming ready"
                        {:type :instance-supervisor/child-exited
                         :base-url base-url
                         :exit-code (process-exit-value process)
                         :log-path log-path}))

        (>= (System/currentTimeMillis) deadline)
        (throw (ex-info "Timed out waiting for child Xia instance to become ready"
                        {:type :instance-supervisor/start-timeout
                         :base-url base-url
                         :timeout-ms wait-for-ready-ms
                         :log-path log-path}))

        :else
        (do
          (Thread/sleep poll-interval-ms)
          (recur))))))

(defn- disable-managed-service!
  [service-id]
  (when service-id
    (try
      (db/enable-service! (keyword service-id) false)
      (catch Exception e
        (log/warn e "Failed to disable managed Xia service" service-id)))))

(defn parent-instance-id
  []
  (some-> (db/get-config parent-instance-config-key)
          paths/normalize-instance-id))

(defn record-parent-link-from-env!
  []
  (when-let [parent-instance-id* (some-> (env-value parent-instance-env)
                                         nonblank-string
                                         paths/normalize-instance-id)]
    (db/set-config! parent-instance-config-key parent-instance-id*)
    {:parent_instance_id parent-instance-id*}))

(defn- persist-managed-child!
  [{:keys [instance-id service-id service-name base-url template-instance state pid
           log-path started-at exited-at exit-code]}]
  (db/save-managed-child! {:id                (keyword instance-id)
                           :name              (or service-name instance-id)
                           :service-id        (some-> service-id keyword)
                           :service-name      service-name
                           :base-url          base-url
                           :template-instance template-instance
                           :state             state
                           :pid               pid
                           :log-path          log-path
                           :started-at        started-at
                           :exited-at         exited-at
                           :exit-code         exit-code}))

(defn- public-status
  [{:keys [instance-id service-id service-name base-url port process started-at
           exited-at exit-code log-path state template-instance]}]
  {:instance_id       instance-id
   :service_id        service-id
   :service_name      service-name
   :base_url          base-url
   :port              port
   :pid               (some-> process process-pid)
   :state             (name state)
   :alive             (boolean (and process (process-alive? process)))
   :attached          true
   :template_instance template-instance
   :log_path          log-path
   :started_at        started-at
   :exited_at         exited-at
   :exit_code         exit-code})

(defn- persisted-status
  [record]
  {:instance_id       (some-> (:managed.child/id record) name)
   :service_id        (some-> (:managed.child/service-id record) name)
   :service_name      (:managed.child/service-name record)
   :base_url          (:managed.child/base-url record)
   :port              nil
   :pid               (:managed.child/pid record)
   :state             (some-> (:managed.child/state record) name)
   :alive             false
   :attached          false
   :template_instance (:managed.child/template-instance record)
   :log_path          (:managed.child/log-path record)
   :started_at        (:managed.child/started-at record)
   :exited_at         (:managed.child/exited-at record)
   :exit_code         (:managed.child/exit-code record)})

(defn- mark-exited!
  [instance-id process]
  (when-let [entry (get @managed-instances instance-id)]
    (when (= process (:process entry))
      (let [exit-code (process-exit-value process)
            exited-at (Date.)
            service-id (:service-id entry)]
        (swap! managed-instances assoc instance-id
               (-> entry
                   (assoc :state :exited
                          :exited-at exited-at
                          :exit-code exit-code)))
        (persist-managed-child! (-> entry
                                    (assoc :state :exited
                                           :pid nil
                                           :exited-at exited-at
                                           :exit-code exit-code)))
        (disable-managed-service! service-id)))))

(defn- start-exit-watcher!
  [instance-id process]
  (future
    (try
      (.waitFor ^Process process)
      (mark-exited! instance-id process)
      (catch Exception e
        (log/debug e "Managed Xia child watcher stopped unexpectedly" instance-id)))))

(defn list-managed-instances
  []
  (let [persisted (->> (db/list-managed-children)
                       (map persisted-status)
                       (map (fn [entry] [(:instance_id entry) entry]))
                       (into {}))
        runtime   (->> @managed-instances
                       vals
                       (map public-status)
                       (map (fn [entry] [(:instance_id entry) entry]))
                       (into {}))]
    (->> (merge-with merge persisted runtime)
         vals
         (sort-by (fn [entry]
                    [(str (:instance_id entry))]))
         vec)))

(defn instance-status
  [instance-id]
  (let [instance-id* (normalize-instance-id instance-id
                                            "instance_id"
                                            :instance-supervisor/invalid-instance-id)]
    (some (fn [entry]
            (when (= instance-id* (:instance_id entry))
              entry))
          (list-managed-instances))))

(defn- wait-until-stopped!
  [base-url wait-ms]
  (let [deadline (+ (System/currentTimeMillis)
                    (long (or wait-ms default-stop-timeout-ms)))]
    (loop []
      (cond
        (not (child-ready? base-url))
        true

        (>= (System/currentTimeMillis) deadline)
        false

        :else
        (do
          (Thread/sleep poll-interval-ms)
          (recur))))))

(defn- shutdown-detached-instance!
  [instance-id timeout-ms]
  (let [instance-id* (normalize-instance-id instance-id
                                            "instance_id"
                                            :instance-supervisor/invalid-instance-id)
        record       (or (db/get-managed-child (keyword instance-id*))
                         (throw (ex-info "Managed Xia instance not found"
                                         {:type :instance-supervisor/not-found
                                          :instance-id instance-id*})))
        status       (persisted-status record)
        service-id   (or (:service_id status)
                         (throw (ex-info "Managed Xia instance cannot be stopped because its controller link is incomplete"
                                         {:type :instance-supervisor/unreachable
                                          :instance-id instance-id*})))
        service      (db/get-service (keyword service-id))
        token        (some-> service :service/auth-key nonblank-string)
        base-url     (or (:base_url status)
                         (some-> service :service/base-url nonblank-string))]
    (when-not (and service token base-url)
      (throw (ex-info "Managed Xia instance cannot be stopped because its controller link is incomplete"
                      {:type :instance-supervisor/unreachable
                       :instance-id instance-id*
                       :service-id service-id})))
    (persist-managed-child! {:instance-id       instance-id*
                             :service-id        service-id
                             :service-name      (:service_name status)
                             :base-url          base-url
                             :template-instance (:template_instance status)
                             :state             :stopping
                             :pid               nil
                             :log-path          (:log_path status)
                             :started-at        (:started_at status)
                             :exited-at         nil
                             :exit-code         nil})
    (try
      (let [response (http-client/request {:method :post
                                           :url (str base-url "/command/shutdown")
                                           :headers {"Authorization" (str "Bearer " token)}
                                           :timeout (long timeout-ms)
                                           :as :text
                                           :allow-private-network? true})
            status-code (:status response)]
        (when-not (<= 200 (long status-code) 299)
          (throw (ex-info "Child Xia shutdown request failed"
                          {:type :instance-supervisor/shutdown-failed
                           :instance-id instance-id*
                           :service-id service-id
                           :status status-code
                           :body (:body response)}))))
      (catch Throwable t
        (when (child-ready? base-url)
          (throw t))))
    (let [stopped? (wait-until-stopped! base-url timeout-ms)
          exited-at (Date.)]
      (when stopped?
        (disable-managed-service! service-id))
      (persist-managed-child! {:instance-id       instance-id*
                               :service-id        service-id
                               :service-name      (:service_name status)
                               :base-url          base-url
                               :template-instance (:template_instance status)
                               :state             (if stopped? :exited :stopping)
                               :pid               nil
                               :log-path          (:log_path status)
                               :started-at        (:started_at status)
                               :exited-at         (when stopped? exited-at)
                               :exit-code         nil})
      (instance-status instance-id*))))

(defn- stop-managed-instance!
  [instance-id timeout-ms]
  (let [instance-id* (normalize-instance-id instance-id
                                            "instance_id"
                                            :instance-supervisor/invalid-instance-id)
        entry        (get @managed-instances instance-id*)
        process      (:process entry)]
    (if-not entry
      (shutdown-detached-instance! instance-id* timeout-ms)
      (do
        (swap! managed-instances assoc instance-id*
               (assoc entry :state :stopping))
        (persist-managed-child! (-> entry
                                    (assoc :state :stopping)))
        (when (and process (process-alive? process))
          (destroy-process! process false)
          (when-not (wait-for-exit process timeout-ms)
            (destroy-process! process true)
            (wait-for-exit process timeout-ms)))
        (mark-exited! instance-id* process)
        (public-status (get @managed-instances instance-id*))))))

(defn start-instance!
  [instance-id & {:keys [template-instance port bind service-id service-name
                         wait-for-ready-ms autonomous-approved?]
                  :or {wait-for-ready-ms default-ready-timeout-ms}}]
  (require-capability!)
  (let [instance-id*        (normalize-instance-id instance-id
                                                   "instance_id"
                                                   :instance-supervisor/invalid-instance-id)
        template-instance*  (normalize-template-instance template-instance)
        parent-instance-id  (some-> (db/current-instance-id) str)
        _                   (when (= instance-id* parent-instance-id)
                              (throw (ex-info "Child instance id must differ from the current Xia instance"
                                              {:type :instance-supervisor/instance-conflict
                                               :instance-id instance-id*
                                               :current-instance-id parent-instance-id})))
        bind*               (normalize-bind bind)
        port*               (or (normalize-port port) (reserve-port))
        service-id*         (normalize-service-id instance-id* service-id)
        service-name*       (managed-service-name instance-id* service-name)
        command             (instance-command)
        log-path            (instance-log-path instance-id*)
        base-url            (service-base-url bind* port*)
        token               (random-token)
        started-at          (Date.)]
    (when-let [existing (get @managed-instances instance-id*)]
      (when (contains? #{"starting" "running" "stopping"} (:state (public-status existing)))
        (throw (ex-info "Managed Xia instance is already running"
                        {:type :instance-supervisor/already-running
                         :instance-id instance-id*
                         :service-id (:service-id existing)
                         :base-url (:base-url existing)}))))
    (let [args (cond-> ["--instance" instance-id*
                        "--mode" "server"
                        "--bind" bind*
                        "--port" (str port*)
                        "--log-file" log-path]
                 template-instance*
                 (conj "--template-instance" template-instance*))
          process (spawn-process! command
                                  args
                                  {"XIA_ALLOW_INSTANCE_MANAGEMENT" "false"
                                   "XIA_COMMAND_TOKEN" token
                                   parent-instance-env parent-instance-id
                                   "XIA_INSTANCE_COMMAND" nil}
                                  log-path)
          entry   {:instance-id instance-id*
                   :template-instance template-instance*
                   :service-id service-id*
                   :service-name service-name*
                   :base-url base-url
                   :port port*
                   :log-path log-path
                   :started-at started-at
                   :state :starting
                   :command command
                   :args args
                   :process process}]
      (swap! managed-instances assoc instance-id* entry)
      (try
        (wait-until-ready! base-url process wait-for-ready-ms log-path)
        (let [existing-service (db/get-service (keyword service-id*))
              autonomous-approved* (if (some? autonomous-approved?)
                                     (boolean autonomous-approved?)
                                     (if (some? existing-service)
                                       (boolean (:service/autonomous-approved? existing-service))
                                       false))
              running-entry (assoc entry :state :running)]
          (db/save-service! {:id                     (keyword service-id*)
                             :name                   service-name*
                             :base-url               base-url
                             :auth-type              :bearer
                             :auth-key               token
                             :allow-private-network? true
                             :autonomous-approved?   autonomous-approved*
                             :enabled?               true})
          (persist-managed-child! (-> running-entry
                                      (assoc :pid (process-pid process))))
          (swap! managed-instances assoc instance-id* running-entry)
          (start-exit-watcher! instance-id* process)
          (public-status running-entry))
        (catch Throwable t
          (swap! managed-instances dissoc instance-id*)
          (try
            (destroy-process! process true)
            (catch Exception _))
          (throw t))))))

(defn stop-instance!
  [instance-id & {:keys [timeout-ms]
                  :or {timeout-ms default-stop-timeout-ms}}]
  (require-capability!)
  (stop-managed-instance! instance-id timeout-ms))

(defn shutdown!
  []
  (doseq [instance-id (keys @managed-instances)]
    (try
      (stop-managed-instance! instance-id default-stop-timeout-ms)
      (catch Exception e
        (log/warn e "Failed to stop managed Xia instance during shutdown" instance-id))))
  (reset! managed-instances {})
  nil)
