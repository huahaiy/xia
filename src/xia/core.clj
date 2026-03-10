(ns xia.core
  "Entry point for xia — a portable personal AI assistant.

   A xia = single binary + Datalevin DB.
   Download, run, answer a few questions, done."
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log]
            [xia.db :as db]
            [xia.setup :as setup]
            [xia.identity :as identity]
            [xia.scheduler :as scheduler]
            [xia.skill :as skill]
            [xia.tool :as tool]
            [xia.channel.terminal :as terminal]
            [xia.channel.http :as http])
  (:gen-class))

;; ---------------------------------------------------------------------------
;; CLI options
;; ---------------------------------------------------------------------------

(def cli-options
  [["-d" "--db PATH" "Database path"
    :default (str (System/getProperty "user.home") "/.xia/db")]
   ["-p" "--port PORT" "HTTP/WebSocket port"
    :default 18790
    :parse-fn #(Integer/parseInt %)]
   ["-m" "--mode MODE" "Run mode: terminal, server, both"
    :default "terminal"]
   ["-h" "--help" "Show help"]])

(defn- print-help [summary]
  (println)
  (println "xia — your portable personal assistant")
  (println)
  (println "Usage: xia [options]")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  xia                     Start in terminal mode")
  (println "  xia --mode server       Start HTTP/WebSocket server only")
  (println "  xia --mode both         Start both terminal and server")
  (println "  xia --db /path/to/db    Use a specific database"))

;; ---------------------------------------------------------------------------
;; Startup
;; ---------------------------------------------------------------------------

(defn- ensure-db-dir! [db-path]
  (let [parent (.getParentFile (java.io.File. db-path))]
    (when-not (.exists parent)
      (.mkdirs parent))))

(defn- start! [{:keys [db port mode]}]
  (ensure-db-dir! db)
  (db/connect! db)
  (log/info "Database opened at" db)

  ;; First-run setup if needed
  (when (setup/needs-setup?)
    (setup/run-setup!))

  ;; Initialize identity and load skills + tools
  (identity/init-identity!)
  (tool/load-all-tools!)
  (log/info "Loaded" (count (tool/registered-tools)) "tools,"
            (count (skill/all-enabled-skills)) "skills")

  ;; Start background scheduler
  (scheduler/start!)

  ;; Start channels based on mode
  (case mode
    "server"   (do (http/start! port)
                   (println (str "xia server running on port " port))
                   @(promise))
    "both"     (do (http/start! port)
                   (println (str "xia server running on port " port))
                   (terminal/start!))
    ;; default: terminal
    (terminal/start!)))

;; ---------------------------------------------------------------------------
;; Main
;; ---------------------------------------------------------------------------

(defn -main [& args]
  (let [{:keys [options errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options)
      (print-help summary)

      errors
      (do (doseq [e errors] (println "Error:" e))
          (print-help summary)
          (System/exit 1))

      :else
      (try
        (start! options)
        (catch Exception e
          (log/error e "Fatal error")
          (println (str "Fatal error: " (.getMessage e)))
          (System/exit 1))
        (finally
          (scheduler/stop!)
          (db/close!))))))
