(ns xia.core
  "Entry point for xia — a portable personal AI assistant.

   A xia = single binary + Datalevin DB.
   Download, run, answer a few questions, done."
  (:require [clojure.tools.cli :refer [parse-opts]]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.logging :as logging]
            [xia.pack :as pack]
            [xia.setup :as setup]
            [xia.identity :as identity]
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

(def cli-options
  [["-d" "--db PATH" "Database path"
    :default (str (System/getProperty "user.home") "/.xia/db")]
   ["-b" "--bind HOST" "HTTP/WebSocket bind address (default: 127.0.0.1)"
    :default "127.0.0.1"]
   ["-p" "--port PORT" "HTTP/WebSocket port"
    :default 18790
    :parse-fn #(Integer/parseInt %)]
   ["-m" "--mode MODE" "Run mode: terminal, server, both"
    :default "terminal"]
   ["-l" "--log-file PATH" "Write INFO+ logs to this file (or set XIA_LOG_FILE)"]
   ["-h" "--help" "Show help"]])

(def pack-cli-options
  [["-d" "--db PATH" "Database path"
    :default (str (System/getProperty "user.home") "/.xia/db")]
   ["-f" "--force" "Overwrite existing archive"]
   ["-h" "--help" "Show help"]])

(defn- print-help [summary]
  (println)
  (println "xia — your portable personal assistant")
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
  (println "  xia --log-file xia.log  Write logs to a file")
  (println "  xia --bind 0.0.0.0      Expose server beyond localhost")
  (println "  xia --db /path/to/db    Use a specific database"))

(defn- print-pack-help [summary]
  (println)
  (println "xia pack — package a portable Xia archive")
  (println)
  (println "Usage: xia pack [archive-path] [options]")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  xia pack")
  (println "  xia pack backup.xia")
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
  (fn [{:keys [new?]}]
    (println)
    (println (if new?
               "Create a master passphrase to protect Xia secrets."
               "Enter the master passphrase to unlock Xia secrets."))
    (let [passphrase (read-startup-secret "Master passphrase" mode)]
      (when (and new? (some? passphrase))
        (let [confirm (read-startup-secret "Confirm master passphrase" mode)]
          (when-not (= passphrase confirm)
            (throw (ex-info "Master passphrases did not match" {})))))
      passphrase)))

(defn- start! [{:keys [db bind port mode crypto-opts]}]
  (ensure-db-dir! db)
  (db/connect! db (merge {:passphrase-provider (startup-passphrase-provider mode)}
                         crypto-opts))
  (log/info "Database opened at" db)
  (log/info "Master key source" (pr-str (crypto/current-key-source)))

  ;; First-run setup if needed
  (when (setup/needs-setup?)
    (setup/run-setup!))

  ;; Initialize identity and load skills + tools
  (identity/init-identity!)
  (let [bundled-count (tool/ensure-bundled-tools!)]
    (when (pos? bundled-count)
      (log/info "Installed" bundled-count "bundled tools")))
  (tool/load-all-tools!)
  (log/info "Loaded" (count (tool/registered-tools)) "tools,"
            (count (skill/all-enabled-skills)) "skills")

  ;; Start background scheduler
  (scheduler/start!)

  ;; Start channels based on mode
  (case mode
    "server"   (do (http/start! bind port)
                   (println (str "xia server running on " bind ":" port))
                   (println (str "open " (local-ui-url bind port)))
                   @(promise))
    "both"     (do (http/start! bind port)
                   (println (str "xia server running on " bind ":" port))
                   (println (str "open " (local-ui-url bind port)))
                   (terminal/start!))
    ;; default: terminal
    (terminal/start!)))

(defn- resolve-run-options
  [options arguments]
  (cond
    (empty? arguments)
    options

    (> (count arguments) 1)
    (throw (ex-info "xia accepts at most one positional archive path"
                    {:arguments arguments}))

    :else
    (let [argument (first arguments)]
      (if (pack/archive-path? argument)
        (let [archive (pack/open-archive! argument)]
          (assoc options
                 :db (:db-path archive)
                 :archive-context archive
                 :crypto-opts (:crypto-opts archive)))
        (throw (ex-info "Unrecognized positional argument"
                        {:argument argument}))))))

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
      (let [archive (or (first arguments)
                        (pack/default-archive-path (:db options)))
            result  (pack/pack! (:db options) archive :force? (:force options))]
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
