(ns xia.sci-env
  "SCI execution environment for tool handlers.
   GraalVM native-image cannot eval Clojure code, so all user-provided
   tool handlers run inside SCI (Small Clojure Interpreter).

   This module configures the SCI context with the namespaces and vars
   that tool code is allowed to access.

   SECURITY: Tool handlers run untrusted code. All DB access goes through
   xia.secret safe wrappers that block access to credentials and secrets.
   The sandbox explicitly denies file I/O, shell execution, source
   introspection vars, and direct long-term memory mutation so SCI default
   namespace changes do not widen access."
  (:require [sci.core :as sci]
            [xia.browser :as browser]
            [xia.config :as cfg]
            [xia.cron :as cron]
            [xia.memory :as memory]
            [xia.scratch :as scratch]
            [xia.schedule :as schedule]
            [xia.secret :as secret]
            [xia.service :as service]
            [xia.skill :as skill]
            [xia.web :as web]
            [xia.working-memory :as wm]))

(def ^:private default-sci-eval-timeout-ms 10000)
(def ^:private default-sci-handler-timeout-ms 120000)

(defn- blocked-sci-fn
  [sym]
  (fn [& _]
    (throw (ex-info (str sym " is not available in Xia's SCI sandbox")
                    {:symbol sym}))))

(def ^:private xia-memory-read-ns
  {'find-node                memory/find-node
   'node-facts               memory/node-facts
   'node-edges               memory/node-edges
   'recent-episodes          memory/recent-episodes
   'search-nodes             memory/search-nodes
   'search-facts             memory/search-facts
   'search-episodes          memory/search-episodes
   'recall-knowledge         memory/recall-knowledge
   'node-properties          memory/node-properties
   'query-nodes-by-property  memory/query-nodes-by-property})

(def ^:private xia-memory-write-blocked-ns
  {'record-episode!          (blocked-sci-fn 'xia.memory/record-episode!)
   'add-node!                (blocked-sci-fn 'xia.memory/add-node!)
   'add-edge!                (blocked-sci-fn 'xia.memory/add-edge!)
   'add-fact!                (blocked-sci-fn 'xia.memory/add-fact!)
   'set-node-property!       (blocked-sci-fn 'xia.memory/set-node-property!)
   'remove-node-property!    (blocked-sci-fn 'xia.memory/remove-node-property!)})

(def ^:private xia-memory-ns
  (merge xia-memory-read-ns
         xia-memory-write-blocked-ns))

(def ^:private xia-wm-ns
  {'get-wm      wm/get-wm
   'wm->context wm/wm->context
   'pin!        wm/pin!
   'unpin!      wm/unpin!})

(def ^:private xia-db-ns
  {'get-config  secret/safe-get-config
   'set-config! secret/safe-set-config!
   'q           secret/safe-q})

(def ^:private xia-scratch-ns
  {'list-pads    scratch/list-pads
   'get-pad      scratch/get-pad
   'create-pad!  scratch/create-pad!
   'save-pad!    scratch/save-pad!
   'edit-pad!    scratch/edit-pad!
   'delete-pad!  scratch/delete-pad!})

(def ^:private xia-service-ns
  {'request       service/request
   'list-services service/list-services})

(def ^:private xia-web-ns
  {'fetch-page   web/fetch-page
   'search-web   web/search-web
   'extract-data web/extract-data})

(def ^:private xia-browser-ns
  {'open-session      browser/open-session
   'navigate          browser/navigate
   'click             browser/click
   'fill-form         browser/fill-form
   'read-page         browser/read-page
   'wait-for-page     browser/wait-for-page
   'close-session     browser/close-session
   'list-sessions     browser/list-sessions
   'runtime-status    browser/browser-runtime-status
   'bootstrap-runtime! browser/bootstrap-browser-runtime!
   'login             browser/login
   'login-interactive browser/login-interactive
   'list-sites        browser/list-sites})

(def ^:private xia-schedule-ns
  {'create-schedule!  schedule/create-schedule!
   'get-schedule      schedule/get-schedule
   'list-schedules    schedule/list-schedules
   'update-schedule!  schedule/update-schedule!
   'remove-schedule!  schedule/remove-schedule!
   'pause-schedule!   schedule/pause-schedule!
   'resume-schedule!  schedule/resume-schedule!
   'schedule-history  schedule/safe-schedule-history})

(def ^:private xia-cron-ns
  {'describe cron/describe})

(def ^:private xia-skill-ns
  {'search-skills       skill/search-skills
   'match-skills        skill/match-skills
   'skill-section       skill/skill-section
   'skill-headings      skill/skill-headings
   'patch-skill-section! skill/patch-skill-section!})

(def ^:private sci-core-overrides
  {'slurp       (blocked-sci-fn 'clojure.core/slurp)
   'spit        (blocked-sci-fn 'clojure.core/spit)
   'load-file   (blocked-sci-fn 'clojure.core/load-file)
   'load-reader (blocked-sci-fn 'clojure.core/load-reader)})

(def ^:private sci-io-overrides
  {'delete-file   (blocked-sci-fn 'clojure.java.io/delete-file)
   'file          (blocked-sci-fn 'clojure.java.io/file)
   'input-stream  (blocked-sci-fn 'clojure.java.io/input-stream)
   'output-stream (blocked-sci-fn 'clojure.java.io/output-stream)
   'reader        (blocked-sci-fn 'clojure.java.io/reader)
   'resource      (blocked-sci-fn 'clojure.java.io/resource)
   'writer        (blocked-sci-fn 'clojure.java.io/writer)})

(def ^:private sci-shell-overrides
  {'sh          (blocked-sci-fn 'clojure.java.shell/sh)
   'with-sh-dir (blocked-sci-fn 'clojure.java.shell/with-sh-dir)
   'with-sh-env (blocked-sci-fn 'clojure.java.shell/with-sh-env)})

(def ^:private sci-repl-overrides
  {'source    (blocked-sci-fn 'clojure.repl/source)
   'source-fn (blocked-sci-fn 'clojure.repl/source-fn)})

(def ^:private denied-sci-symbols
  '[slurp
    spit
    load-file
    load-reader
    clojure.java.io/delete-file
    clojure.java.io/file
    clojure.java.io/input-stream
    clojure.java.io/output-stream
    clojure.java.io/reader
    clojure.java.io/resource
    clojure.java.io/writer
    clojure.java.shell/sh
    clojure.java.shell/with-sh-dir
    clojure.java.shell/with-sh-env
    clojure.repl/source
    clojure.repl/source-fn])

(defn make-ctx
  "Create a SCI evaluation context with xia APIs available."
  []
  (sci/init
    {:namespaces {'clojure.core       sci-core-overrides
                  'clojure.java.io    sci-io-overrides
                  'clojure.java.shell sci-shell-overrides
                  'clojure.repl       sci-repl-overrides
                  'xia.memory         xia-memory-ns
                  'xia.working-memory xia-wm-ns
                  'xia.skill          xia-skill-ns
                  'xia.schedule       xia-schedule-ns
                  'xia.scratch        xia-scratch-ns
                  'xia.cron           xia-cron-ns
                  'xia.service        xia-service-ns
                  'xia.web            xia-web-ns
                  'xia.browser        xia-browser-ns
                  'xia.db             xia-db-ns}
     :deny       denied-sci-symbols
     :classes    {'java.util.Date java.util.Date
                  'java.util.UUID java.util.UUID}}))

(defonce ^:private default-ctx (delay (make-ctx)))

(defn- sci-eval-timeout-ms
  []
  (cfg/positive-long :tool/sci-eval-timeout-ms
                     default-sci-eval-timeout-ms))

(defn- sci-handler-timeout-ms
  []
  (cfg/positive-long :tool/sci-handler-timeout-ms
                     default-sci-handler-timeout-ms))

(defn- call-with-timeout
  [timeout-ms stage f]
  (let [worker   (future (f))
        timeout  (Object.)
        result   (deref worker timeout-ms timeout)]
    (if (identical? timeout result)
      (do
        (future-cancel worker)
        (throw (ex-info (str "SCI " (name stage) " timed out after " timeout-ms " ms")
                        {:stage stage
                         :timeout-ms timeout-ms})))
      result)))

(defn eval-string
  "Evaluate a string of Clojure code in the SCI sandbox."
  [code-str]
  (call-with-timeout (sci-eval-timeout-ms)
                     :eval
                     #(sci/eval-string* @default-ctx code-str)))

(defn call-fn
  "Invoke a compiled SCI function with a bounded execution time."
  [f & args]
  (call-with-timeout (sci-handler-timeout-ms)
                     :handler
                     #(apply f args)))
