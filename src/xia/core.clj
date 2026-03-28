(ns xia.core
  "Entry point for Xia — a portable personal AI assistant.

   Xia = single binary + Datalevin DB.
   Download, run, answer a few questions, done."
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [integrant.core :as ig]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.paths :as paths]
            [xia.logging :as logging]
            [xia.pack :as pack]
            [xia.runtime-state :as runtime-state]
            [xia.system]
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

(defn- local-ui-url [bind port]
  (str "http://"
       (if (#{"0.0.0.0" "::" "[::]"} bind) "localhost" bind)
       ":"
       port
       "/"))

(defn- env-value
  [k]
  (System/getenv k))

(defonce ^:private last-stop-event-atom (atom nil))

(defn- stack-frame->summary
  [^StackTraceElement frame]
  (str (.getClassName frame) "/" (.getMethodName frame) ":" (.getLineNumber frame)))

(defn- capture-callsite-summary
  [label]
  (->> (.getStackTrace (Throwable. label))
       (drop 1)
       (map stack-frame->summary)
       (take 12)
       vec))

(defn last-stop-event
  "Return the most recent stop-runtime! event for debugging."
  []
  @last-stop-event-atom)

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

(defonce ^:private runtime-system-atom (atom nil))

(declare make-cleanup)
(declare stop-runtime!)

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

(def ^:private base-runtime-root-keys
  [:xia/scheduler])

(def ^:private server-runtime-root-keys
  [:xia/http])

(defn- system-config
  [{:keys [db mode crypto-opts instance template-instance
           instance-command bind port web-dev] :as options}]
  (let [connect-options (merge {:passphrase-provider (startup-passphrase-provider mode)
                                :instance-id instance}
                               crypto-opts)]
    {:xia/db
     {:db-path db
      :connect-options connect-options}

     :xia/runtime-support
     {:db (ig/ref :xia/db)}

     :xia/instance-supervisor
     {:db (ig/ref :xia/db)
      :enabled? (not (falsy-env-value? "XIA_ALLOW_INSTANCE_MANAGEMENT"))
      :command instance-command}

     :xia/bootstrap
     {:db (ig/ref :xia/db)
      :runtime-support (ig/ref :xia/runtime-support)
      :instance-supervisor (ig/ref :xia/instance-supervisor)
      :db-path db
      :instance instance
      :template-instance template-instance
      :mode mode
      :crypto-opts crypto-opts}

     :xia/identity
     {:bootstrap (ig/ref :xia/bootstrap)}

     :xia/tool-runtime
     {:identity (ig/ref :xia/identity)}

     :xia/scheduler
     {:tool-runtime (ig/ref :xia/tool-runtime)}

     :xia/http
     {:scheduler (ig/ref :xia/scheduler)
      :bind-host bind
      :port port
      :web-dev? web-dev}

     :xia/options
     options}))

(defn- initialize-runtime!
  ([options]
   (initialize-runtime! options base-runtime-root-keys))
  ([options root-keys]
   (when @runtime-system-atom
     (throw (ex-info "Xia runtime is already running"
                     {:root-keys (:root-keys @runtime-system-atom)
                      :db (get-in @runtime-system-atom [:options :db])})))
   (let [config  (system-config options)
         system  (ig/init config root-keys)
         state   {:config config
                  :system system
                  :root-keys (vec root-keys)
                  :options options}]
     (reset! runtime-system-atom state)
     system)))

(defn- halt-runtime!
  []
  (when-let [{:keys [system]} @runtime-system-atom]
    (try
      (ig/halt! system)
      (finally
        (reset! runtime-system-atom nil)))))

(defn- register-http-runtime-controls!
  [options root-keys]
  (if (some #{:xia/http} root-keys)
    (http/register-command-shutdown-handler!
      #(stop-runtime! options))
    (http/clear-command-shutdown-handler!)))

(defn start-server-runtime!
  "Start Xia in non-blocking server mode for REPL-driven development."
  [{:keys [bind port web-dev] :as options}]
  (let [options* (apply-run-defaults
                   (merge {:mode "server"}
                          options))
        bind*    (or bind (:bind options*))
        port*    (or port (:port options*))]
    (runtime-state/mark-starting!)
    (try
      (initialize-runtime! options* server-runtime-root-keys)
      (register-http-runtime-controls! options* server-runtime-root-keys)
      (runtime-state/mark-running!)
      (let [options** (assoc options* :port (or (http/current-port) port*))]
        (println (str "Xia server running on " (:bind options**) ":" (:port options**)))
        (println (str "open " (local-ui-url (:bind options**) (:port options**))))
        options**)
      (catch Throwable t
        (try
          (halt-runtime!)
          (catch Exception halt-error
            (log/error halt-error "Failed to halt partially initialized runtime")))
        (runtime-state/mark-stopped!)
        (throw t)))))

(defn stop-runtime!
  "Stop Xia runtime components that were started in the current process."
  [options]
  (let [options* (apply-run-defaults options)
        phase    (runtime-state/phase)
        event    {:at       (java.time.Instant/now)
                  :phase    phase
                  :db-path  (:db options*)
                  :callsite (capture-callsite-summary "stop-runtime! callsite")}]
    (reset! last-stop-event-atom event)
    (log/info "stop-runtime! invoked"
              "phase" (name phase)
              "db" (:db options*))
    ((make-cleanup options*))))

(defn- start!
  [options]
  (let [options* (apply-run-defaults options)]
    (runtime-state/mark-starting!)
    (try
      (let [root-keys (case (:mode options*)
                        "server" server-runtime-root-keys
                        "both" server-runtime-root-keys
                        base-runtime-root-keys)]
        (initialize-runtime! options* root-keys)
        (register-http-runtime-controls! options* root-keys))

      ;; Start channels based on mode
      (case (:mode options*)
        "server"   (do (runtime-state/mark-running!)
                       (let [port* (or (http/current-port) (:port options*))]
                         (println (str "Xia server running on " (:bind options*) ":" port*))
                         (println (str "open " (local-ui-url (:bind options*) port*))))
                       @(promise))
        "both"     (do (runtime-state/mark-running!)
                       (let [port* (or (http/current-port) (:port options*))]
                         (println (str "Xia server running on " (:bind options*) ":" port*))
                         (println (str "open " (local-ui-url (:bind options*) port*))))
                       (terminal/start!))
        ;; default: terminal
        (do
          (runtime-state/mark-running!)
          (terminal/start!)))
      (catch Throwable t
        (try
          (halt-runtime!)
          (catch Exception halt-error
            (log/error halt-error "Failed to halt partially initialized runtime")))
        (runtime-state/mark-stopped!)
        (throw t)))))

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
        (runtime-state/mark-stopping!)
        (try
        (try
          (http/clear-command-shutdown-handler!)
          (halt-runtime!)
          (catch Exception e
            (log/error e "Failed to halt Xia runtime during shutdown")))
          (try
            (save-archive! options)
            (catch Exception e
              (log/error e "Failed to save archive during shutdown")
              (println (str "Archive save failed: " (.getMessage e)))))
          (finally
            (runtime-state/mark-stopped!)))))))

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
