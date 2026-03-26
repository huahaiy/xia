(ns xia.core
  "Entry point for Xia — a portable personal AI assistant.

   Xia = single binary + Datalevin DB.
   Download, run, answer a few questions, done."
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.paths :as paths]
            [xia.logging :as logging]
            [xia.pack :as pack]
            [xia.setup :as setup]
            [xia.identity :as identity]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.scheduler :as scheduler]
            [xia.skill :as skill]
            [xia.tool :as tool]
            [xia.channel.terminal :as terminal]
            [xia.channel.http :as http])
  (:import [java.nio.file Files Paths])
  (:gen-class))

;; ---------------------------------------------------------------------------
;; CLI options
;; ---------------------------------------------------------------------------

(def default-run-options
  {:instance paths/default-instance-id
   :template-instance nil
   :instance-command nil
   :db (paths/default-db-path)
   :bind "127.0.0.1"
   :port 3008
   :mode "terminal"
   :web-dev false})

(def cli-options
  [["-i" "--instance ID" "Instance id for instance-scoped default storage (or set XIA_INSTANCE)"]
   ["-t" "--template-instance ID" "Seed a new instance from another instance's initial settings (or set XIA_TEMPLATE_INSTANCE)"]
   [nil "--instance-command PATH" "Executable path to use when starting child Xia instances (or set XIA_INSTANCE_COMMAND)"]
   ["-d" "--db PATH" "Database path"]
   ["-b" "--bind HOST" "HTTP/WebSocket bind address (default: 127.0.0.1)"
    :default (:bind default-run-options)]
   ["-p" "--port PORT" "HTTP/WebSocket port"
    :default (:port default-run-options)
    :parse-fn #(Integer/parseInt %)]
   ["-m" "--mode MODE" "Run mode: terminal, server, both"
    :default (:mode default-run-options)]
   [nil "--web-dev" "Enable live-reloading local web assets from resources/web"]
   ["-l" "--log-file PATH" "Write INFO+ logs to this file (or set XIA_LOG_FILE)"]
   ["-h" "--help" "Show help"]])

(def pack-cli-options
  [["-i" "--instance ID" "Instance id for instance-scoped default storage (or set XIA_INSTANCE)"]
   ["-d" "--db PATH" "Database path"]
   ["-f" "--force" "Overwrite existing archive"]
   ["-h" "--help" "Show help"]])

(defn- print-help [summary]
  (println)
  (println "Xia — your portable personal assistant")
  (println)
  (println "Usage: xia [archive.xia] [options]")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  xia                     Start in terminal mode")
  (println "  xia backup.xia          Open a packed Xia archive directly")
  (println "  xia pack                Create a portable archive at <db>.xia")
  (println "  xia pack backup.xia     Create a portable archive at a specific path")
  (println "  xia --mode server       Start HTTP/WebSocket server only")
  (println "  xia --mode both         Start both terminal and server")
  (println "  xia --mode both --web-dev  Live-reload resources/web during UI work")
  (println "  xia --instance ops      Use ~/.xia/instances/ops/db by default")
  (println "  xia --instance qa --template-instance base  Seed qa from base")
  (println "  xia --log-file xia.log  Write logs to a file")
  (println "  xia --bind 0.0.0.0      Expose server beyond localhost")
  (println "  xia --db /path/to/db    Use a specific database"))

(defn- print-pack-help [summary]
  (println)
  (println "`xia pack` — package a portable Xia archive")
  (println)
  (println "Usage: xia pack [archive-path] [options]")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  xia pack")
  (println "  xia pack backup.xia")
  (println "  xia pack --instance ops")
  (println "  xia pack backup.xia --db /path/to/db")
  (println "  xia pack backup.xia --force"))

;; ---------------------------------------------------------------------------
;; Startup
;; ---------------------------------------------------------------------------

(defn- ensure-db-dir! [db-path]
  (when-let [parent (.getParent (Paths/get db-path (make-array String 0)))]
    (Files/createDirectories parent (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- local-ui-url [bind port]
  (str "http://"
       (if (#{"0.0.0.0" "::" "[::]"} bind) "localhost" bind)
       ":"
       port
       "/"))

(defn- env-value
  [k]
  (System/getenv k))

(defn- truthy-env-value?
  [k]
  (contains? #{"true" "1" "yes" "on"}
             (some-> (env-value k) str str/trim str/lower-case)))

(defn- falsy-env-value?
  [k]
  (contains? #{"false" "0" "no" "off"}
             (some-> (env-value k) str str/trim str/lower-case)))

(defn- read-startup-secret
  [label mode]
  (if-let [console (System/console)]
    (String. (.readPassword console "%s: " (into-array Object [label])))
    (when (not= mode "server")
      (print (str label " (input visible): "))
      (flush)
      (or (read-line) ""))))

(defn- startup-passphrase-provider
  [mode]
  (let [cached-passphrase (atom ::unset)]
    (fn [{:keys [new?]}]
      (if-let [cached (when (not= ::unset @cached-passphrase)
                        @cached-passphrase)]
        cached
        (do
          (println)
          (println (if new?
                     "Create a master passphrase to protect Xia secrets."
                     "Enter the master passphrase to unlock Xia secrets."))
          (loop []
            (let [passphrase (read-startup-secret "Master passphrase" mode)]
              (if (and new? (some? passphrase))
                (let [confirm (read-startup-secret "Confirm master passphrase" mode)]
                  (if (= passphrase confirm)
                    (do
                      (reset! cached-passphrase passphrase)
                      passphrase)
                    (do
                      (println "Master passphrases did not match. Please try again.")
                      (recur))))
                (do
                  (reset! cached-passphrase passphrase)
                  passphrase)))))))))

(declare make-cleanup)

(defn- apply-run-defaults
  [options]
  (let [instance-id (paths/resolve-instance-id (:instance options))
        raw-template-instance (or (:template-instance options)
                                  (env-value "XIA_TEMPLATE_INSTANCE"))
        template-instance     (some-> raw-template-instance paths/normalize-instance-id)
        instance-command
        (or (:instance-command options)
            (some-> (env-value "XIA_INSTANCE_COMMAND") str/trim not-empty))
        _                     (paths/warn-if-instance-id-normalized! (:instance options) instance-id)
        _                     (when template-instance
                                (paths/warn-if-instance-id-normalized! raw-template-instance
                                                                       template-instance))
        db-path     (or (:db options)
                        (paths/default-db-path instance-id))]
    (merge default-run-options
           options
           {:instance instance-id
            :template-instance template-instance
            :instance-command instance-command
            :db       db-path})))

(defn- maybe-seed-instance-template!
  [{:keys [db instance template-instance crypto-opts]}]
  (when template-instance
    (if (db/initial-settings-empty?)
      (let [source-db-path (paths/default-db-path template-instance)
            target-db-path (.getCanonicalPath (io/file db))
            source-db-path* (.getCanonicalPath (io/file source-db-path))]
        (when (= source-db-path* target-db-path)
          (throw (ex-info "Template instance must be different from the target instance"
                          {:instance instance
                           :template-instance template-instance
                           :db db
                           :template-db source-db-path})))
        (when-not (.exists (io/file source-db-path))
          (throw (ex-info "Template instance database does not exist"
                          {:instance instance
                           :template-instance template-instance
                           :template-db source-db-path})))
        (let [result (db/seed-initial-settings-from-db! {:source-db-path source-db-path
                                                         :crypto-opts crypto-opts})]
          (when (:seeded? result)
            (log/info "Seeded Xia instance" instance
                      "from template instance" template-instance
                      "providers" (:provider-count result)
                      "oauth-accounts" (:oauth-account-count result)
                      "services" (:service-count result)
                      "sites" (:site-count result)
                      "skipped-secrets" (:skipped-secret-count result)))))
      (log/info "Skipping template seed for Xia instance" instance
                "because initial settings already exist"))))

(defn- initialize-runtime!
  [{:keys [db mode crypto-opts instance template-instance
           instance-command] :as options}]
  (ensure-db-dir! db)
  (db/connect! db (merge {:passphrase-provider (startup-passphrase-provider mode)
                          :instance-id instance}
                         crypto-opts))
  (maybe-seed-instance-template! (assoc options :template-instance template-instance))
  (instance-supervisor/configure! {:enabled? (not (falsy-env-value? "XIA_ALLOW_INSTANCE_MANAGEMENT"))
                                   :command instance-command})
  (log/info "Xia instance" instance)
  (log/info "Database opened at" db)
  (log/info "Support directory" (paths/support-dir-path db))
  (log/info "Master key source" (pr-str (crypto/current-key-source)))

  ;; First-run setup if needed
  (when (setup/needs-setup?)
    (if (= "terminal" mode)
      (setup/run-setup!)
      (log/info "Skipping interactive first-run setup in"
                mode
                "mode; complete provider onboarding in the local web UI.")))

  ;; Initialize identity and load skills + tools
  (identity/init-identity!)
  (let [bundled-count (tool/ensure-bundled-tools!)]
    (when (pos? (long bundled-count))
      (log/info "Installed" bundled-count "bundled tools")))
  (tool/reset-runtime!)
  (tool/load-all-tools!)
  (log/info "Loaded" (count (tool/registered-tools)) "tools,"
            (count (skill/all-enabled-skills)) "skills")

  ;; Start background scheduler
  (scheduler/start!))

(defn start-server-runtime!
  "Start Xia in non-blocking server mode for REPL-driven development."
  [{:keys [bind port web-dev] :as options}]
  (let [options* (apply-run-defaults
                   (merge {:mode "server"}
                          options))
        bind*    (or bind (:bind options*))
        port*    (or port (:port options*))]
    (initialize-runtime! options*)
    (http/start! bind*
                 port*
                 {:web-dev? (true? web-dev)})
    (let [options** (assoc options* :port (or (http/current-port) port*))]
      (println (str "Xia server running on " (:bind options**) ":" (:port options**)))
      (println (str "open " (local-ui-url (:bind options**) (:port options**))))
      options**)))

(defn stop-runtime!
  "Stop Xia runtime components that were started in the current process."
  [options]
  ((make-cleanup (apply-run-defaults options))))

(defn- start!
  [options]
  (let [options* (apply-run-defaults options)]
    (initialize-runtime! options*)

    ;; Start channels based on mode
    (case (:mode options*)
      "server"   (do (http/start! (:bind options*)
                                  (:port options*)
                                  {:web-dev? (:web-dev options*)})
                     (let [port* (or (http/current-port) (:port options*))]
                       (println (str "Xia server running on " (:bind options*) ":" port*))
                       (println (str "open " (local-ui-url (:bind options*) port*))))
                     @(promise))
      "both"     (do (http/start! (:bind options*)
                                  (:port options*)
                                  {:web-dev? (:web-dev options*)})
                     (let [port* (or (http/current-port) (:port options*))]
                       (println (str "Xia server running on " (:bind options*) ":" port*))
                       (println (str "open " (local-ui-url (:bind options*) port*))))
                     (terminal/start!))
      ;; default: terminal
      (terminal/start!))))

(defn- resolve-run-options
  [options arguments]
  (let [options* (apply-run-defaults options)]
    (cond
      (empty? arguments)
      options*

      (> (count arguments) 1)
      (throw (ex-info "Xia accepts at most one positional archive path"
                      {:arguments arguments}))

      :else
      (let [argument (first arguments)]
        (if (pack/archive-path? argument)
          (let [archive (pack/open-archive! argument)]
            (apply-run-defaults
              (assoc options*
                     :db (:db-path archive)
                     :archive-context archive
                     :crypto-opts (:crypto-opts archive))))
          (throw (ex-info "Unrecognized positional argument"
                          {:argument argument})))))))

(defn- save-archive!
  [{:keys [db archive-context]}]
  (when-let [{:keys [archive-path]} archive-context]
    (when (.exists (io/file db))
      (let [result (pack/pack! db archive-path :force? true)]
        (log/info "Saved archive" (:archive result))))))

(defn- make-cleanup
  [options]
  (let [ran? (atom false)]
    (fn []
      (when (compare-and-set! ran? false true)
        (try
          (http/stop!)
          (catch Exception e
            (log/error e "Failed to stop HTTP server during shutdown")))
        (try
          (scheduler/stop!)
          (catch Exception e
            (log/error e "Failed to stop scheduler during shutdown")))
        (try
          (instance-supervisor/shutdown!)
          (catch Exception e
            (log/error e "Failed to stop managed Xia instances during shutdown")))
        (try
          (db/close!)
          (catch Exception e
            (log/error e "Failed to close database during shutdown")))
        (try
          (save-archive! options)
          (catch Exception e
            (log/error e "Failed to save archive during shutdown")
            (println (str "Archive save failed: " (.getMessage e)))))))))

(defn- register-shutdown-hook!
  [cleanup]
  (let [hook (Thread.
               (reify Runnable
                 (run [_]
                   (cleanup)))
               "xia-shutdown")]
    (.addShutdownHook (Runtime/getRuntime) hook)
    hook))

(defn- remove-shutdown-hook!
  [hook]
  (try
    (.removeShutdownHook (Runtime/getRuntime) hook)
    (catch IllegalStateException _)
    (catch IllegalArgumentException _)))

(defn- run-pack!
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args pack-cli-options)]
    (cond
      (:help options)
      (print-pack-help summary)

      errors
      (do (doseq [e errors] (println "Error:" e))
          (print-pack-help summary)
          (System/exit 1))

      (> (count arguments) 1)
      (do (println "Error: pack accepts at most one archive path")
          (print-pack-help summary)
          (System/exit 1))

      :else
      (let [options* (apply-run-defaults options)
            archive  (or (first arguments)
                         (pack/default-archive-path (:db options*)))
            result   (pack/pack! (:db options*) archive :force? (:force options*))]
        (println (str "packed " (:archive result)))
        (println (str "key source: " (name (:key-source result))))
        (doseq [req (:restore-requires result)]
          (println (str "restore: " req)))))))

;; ---------------------------------------------------------------------------
;; Main
;; ---------------------------------------------------------------------------

(defn -main [& args]
  (let [[command & rest-args] args]
    (if (= "pack" command)
      (try
        (run-pack! rest-args)
        (catch Exception e
          (log/error e "Pack failed")
          (println (str "Pack failed: " (.getMessage e)))
          (System/exit 1)))
      (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
        (cond
          (:help options)
          (print-help summary)

          errors
          (do (doseq [e errors] (println "Error:" e))
              (print-help summary)
              (System/exit 1))

          :else
          (try
            (let [_           (logging/configure! options)
                  run-options (resolve-run-options options arguments)
                  cleanup     (make-cleanup run-options)
                  hook        (register-shutdown-hook! cleanup)]
              (try
                (start! run-options)
                (catch Exception e
                  (log/error e "Fatal error")
                  (println (str "Fatal error: " (.getMessage e)))
                  (System/exit 1))
                (finally
                  (remove-shutdown-hook! hook)
                  (cleanup))))
            (catch Exception e
              (log/error e "Fatal error")
              (println (str "Fatal error: " (.getMessage e)))
              (System/exit 1))))))))
