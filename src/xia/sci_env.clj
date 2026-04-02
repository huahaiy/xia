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
  (:require [clojure.string :as str]
            [clojure.tools.reader :as tr]
            [clojure.tools.reader.reader-types :as rt]
            [sci.core :as sci]
            [taoensso.timbre :as log]
            [xia.artifact :as artifact]
            [xia.browser :as browser]
            [xia.cron :as cron]
            [xia.email :as email]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.local-doc :as local-doc]
            [xia.memory :as memory]
            [xia.memory-edit :as memory-edit]
            [xia.peer :as peer]
            [xia.scratch :as scratch]
            [xia.schedule :as schedule]
            [xia.secret :as secret]
            [xia.service :as service]
            [xia.skill :as skill]
            [xia.task-policy :as task-policy]
            [xia.web :as web]
            [xia.workspace :as workspace]
            [xia.working-memory :as wm])
  (:import [java.util Date]
           [java.util.concurrent.atomic AtomicLong]))

(def ^:private sci-timeout-stop-grace-ms 100)
(def ^:private sci-timeout-check-interval-mask 127)
(def ^:private reader-eof (Object.))

(def ^:dynamic *sci-timeout-state* nil)
(defonce ^:private sci-worker-seq (AtomicLong. 0))
(defonce ^:private active-sci-workers (atom {}))
(defonce ^:private shutdown? (atom false))
(defonce ^:private current-ctx* (atom nil))

(defn- sci-timeout-ex
  [stage timeout-ms]
  (ex-info (str "SCI " (name stage) " timed out after " timeout-ms " ms")
           {:stage stage
            :timeout-ms timeout-ms}))

(defn check-timeout!
  []
  (when-let [{:keys [^long deadline-nanos ^long timeout-ms stage ^longs counter]}
             *sci-timeout-state*]
    (let [n (aget counter 0)]
      (aset ^longs counter 0 (unchecked-inc n))
      (when (and (zero? (bit-and n (long sci-timeout-check-interval-mask)))
                 (>= (System/nanoTime) deadline-nanos))
        (throw (sci-timeout-ex stage timeout-ms)))))
  nil)

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

(def ^:private xia-memory-edit-ns
  {'correct-fact! memory-edit/correct-fact!})

(def ^:private xia-wm-ns
  {'get-wm      wm/get-wm
   'wm->context wm/wm->context
   'pin!        wm/pin!
   'unpin!      wm/unpin!})

(def ^:private xia-db-ns
  {'get-config  secret/safe-get-config
   'set-config! secret/safe-set-config!
   'q           secret/safe-q})

(def ^:private xia-sci-env-ns
  {'check-timeout! check-timeout!})

(def ^:private xia-scratch-ns
  {'list-pads    scratch/list-pads
   'get-pad      scratch/get-pad
   'create-pad!  scratch/create-pad!
   'save-pad!    scratch/save-pad!
   'edit-pad!    scratch/edit-pad!
   'delete-pad!  scratch/delete-pad!})

(def ^:private xia-local-doc-ns
  {'list-docs   local-doc/list-docs
   'list-visible-docs local-doc/list-visible-docs
   'search-docs local-doc/search-docs
   'search-visible-docs local-doc/search-visible-docs
   'read-doc    local-doc/read-doc
   'read-visible-doc local-doc/read-visible-doc})

(def ^:private xia-artifact-ns
  {'list-artifacts                 artifact/list-artifacts
   'list-visible-artifacts         artifact/list-visible-artifacts
   'search-artifacts               artifact/search-artifacts
   'search-visible-artifacts       artifact/search-visible-artifacts
   'get-artifact                   artifact/get-artifact
   'read-artifact                  artifact/read-artifact
   'read-visible-artifact          artifact/read-visible-artifact
   'create-artifact!               artifact/create-artifact!
   'create-scratch-pad-from-artifact! artifact/create-scratch-pad-from-artifact!
   'delete-artifact!               artifact/delete-artifact!})

(def ^:private xia-service-ns
  {'request       service/request
   'list-services service/list-services})

(def ^:private xia-peer-ns
  {'list-peers peer/list-peers
   'chat       peer/chat})

(def ^:private xia-instance-supervisor-ns
  {'instance-management-enabled? instance-supervisor/instance-management-enabled?
   'list-managed-instances       instance-supervisor/list-managed-instances
   'instance-status              instance-supervisor/instance-status
   'start-instance!              instance-supervisor/start-instance!
   'stop-instance!               instance-supervisor/stop-instance!})

(def ^:private xia-workspace-ns
  {'list-items               workspace/list-items
   'get-item                 workspace/get-item
   'read-item                workspace/read-item
   'publish-artifact!        workspace/publish-artifact!
   'publish-local-doc!       workspace/publish-local-doc!
   'write-note!              workspace/write-note!
   'import-item-as-artifact! workspace/import-item-as-artifact!
   'import-item-as-local-doc! workspace/import-item-as-local-doc!})

(def ^:private xia-email-ns
  {'list-messages email/list-messages
   'read-message  email/read-message
   'send-message  email/send-message
   'delete-message email/delete-message})

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
   'query-elements    browser/query-elements
   'screenshot        browser/screenshot
   'wait-for-page     browser/wait-for-page
   'close-session     browser/close-session
   'list-sessions     browser/list-sessions
   'runtime-status    browser/browser-runtime-status
   'bootstrap-runtime! browser/bootstrap-browser-runtime!
   'install-browser-deps! browser/install-browser-deps!
   'login             browser/login
   'login-interactive browser/login-interactive
   'list-sites        browser/list-sites})

(defn- run-branch-tasks
  [& args]
  (let [f (requiring-resolve 'xia.agent/run-branch-tasks)]
    (apply f args)))

(def ^:private xia-agent-ns
  {'run-branch-tasks run-branch-tasks})

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
   'load-reader (blocked-sci-fn 'clojure.core/load-reader)
   ;; Dynamic resolution is unnecessary for tool handlers and can hand back
   ;; vars/classes we intentionally do not expose in the sandbox.
   'resolve     (blocked-sci-fn 'clojure.core/resolve)
   'ns-resolve  (blocked-sci-fn 'clojure.core/ns-resolve)
   'find-var    (blocked-sci-fn 'clojure.core/find-var)
   'requiring-resolve (blocked-sci-fn 'clojure.core/requiring-resolve)
   'find-ns     (blocked-sci-fn 'clojure.core/find-ns)
   'the-ns      (blocked-sci-fn 'clojure.core/the-ns)
   'all-ns      (blocked-sci-fn 'clojure.core/all-ns)
   'ns-publics  (blocked-sci-fn 'clojure.core/ns-publics)
   'ns-interns  (blocked-sci-fn 'clojure.core/ns-interns)
   'ns-map      (blocked-sci-fn 'clojure.core/ns-map)
   'ns-refers   (blocked-sci-fn 'clojure.core/ns-refers)
   'ns-aliases  (blocked-sci-fn 'clojure.core/ns-aliases)})

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
    resolve
    ns-resolve
    find-var
    requiring-resolve
    find-ns
    the-ns
    all-ns
    ns-publics
    ns-interns
    ns-map
    ns-refers
    ns-aliases
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
                  'xia.memory-edit    xia-memory-edit-ns
                  'xia.working-memory xia-wm-ns
                  'xia.skill          xia-skill-ns
                  'xia.schedule       xia-schedule-ns
                  'xia.scratch        xia-scratch-ns
                  'xia.local-doc      xia-local-doc-ns
                  'xia.artifact       xia-artifact-ns
                  'xia.cron           xia-cron-ns
                  'xia.service        xia-service-ns
                  'xia.peer           xia-peer-ns
                  'xia.instance-supervisor xia-instance-supervisor-ns
                  'xia.workspace      xia-workspace-ns
                  'xia.email          xia-email-ns
                  'xia.web            xia-web-ns
                  'xia.browser        xia-browser-ns
                  'xia.agent          xia-agent-ns
                  'xia.db             xia-db-ns
                  'xia.sci-env        xia-sci-env-ns}
     :deny       denied-sci-symbols
     :classes    {'java.util.Date java.util.Date
                  'java.util.UUID java.util.UUID}}))

(defn- current-ctx
  []
  (or @current-ctx*
      (let [ctx (make-ctx)]
        (if (compare-and-set! current-ctx* nil ctx)
          ctx
          @current-ctx*))))

(defn- sci-eval-timeout-ms
  []
  (task-policy/tool-sci-eval-timeout-ms))

(defn- sci-handler-timeout-ms
  []
  (task-policy/tool-sci-handler-timeout-ms))

(defn- max-active-sci-workers
  []
  (task-policy/tool-max-active-sci-workers))

(def ^:private timeout-check-form
  '(xia.sci-env/check-timeout!))

(declare instrument-timeouts)
(declare unregister-sci-worker!)

(defn- instrument-body
  [body]
  (let [body* (map instrument-timeouts body)]
    (cons timeout-check-form body*)))

(defn- instrument-binding-vector
  [bindings]
  (into []
        (map-indexed (fn [idx form]
                       (if (even? idx)
                         form
                         (instrument-timeouts form))))
        bindings))

(defn- instrument-fn-clause
  [[params & body]]
  (list* params (instrument-body body)))

(defn- instrument-fn-form
  [[op & more]]
  (let [[name more] (if (symbol? (first more))
                      [(first more) (rest more)]
                      [nil more])]
    (if (vector? (first more))
      (let [[params & body] more]
        (list* op
               (concat (when name [name])
                       [params]
                       (instrument-body body))))
      (list* op
             (concat (when name [name])
                     (map instrument-fn-clause more))))))

(defn- instrument-loop-form
  [[op bindings & body]]
  (list* op
         (instrument-binding-vector bindings)
         (instrument-body body)))

(defn- instrument-letfn-form
  [[op bindings & body]]
  (list* op
         (into []
               (map (fn [[fname params & fbody]]
                      (list* fname params (instrument-body fbody))))
               bindings)
         (map instrument-timeouts body)))

(defn- instrument-while-form
  [[op test & body]]
  (list* op
         (instrument-timeouts test)
         (instrument-body body)))

(defn- instrument-timeouts
  [form]
  (cond
    (list? form)
    (let [op (first form)]
      (case op
        fn     (instrument-fn-form form)
        fn*    (instrument-fn-form form)
        loop   (instrument-loop-form form)
        loop*  (instrument-loop-form form)
        let    (list* op
                      (instrument-binding-vector (second form))
                      (map instrument-timeouts (nnext form)))
        let*   (list* op
                      (instrument-binding-vector (second form))
                      (map instrument-timeouts (nnext form)))
        letfn  (instrument-letfn-form form)
        while  (instrument-while-form form)
        doseq  (instrument-loop-form form)
        dotimes (instrument-loop-form form)
        for    (instrument-loop-form form)
        (apply list (map instrument-timeouts form))))

    (vector? form)
    (mapv instrument-timeouts form)

    (map? form)
    (reduce-kv (fn [m k v]
                 (assoc m
                        (instrument-timeouts k)
                        (instrument-timeouts v)))
               (empty form)
               form)

    (set? form)
    (into (empty form) (map instrument-timeouts) form)

    :else
    form))

(defn- instrument-code-string
  [code-str]
  (let [reader (rt/indexing-push-back-reader
                 (rt/string-push-back-reader code-str))]
    (binding [*print-meta* true]
      (loop [forms []]
        (let [form (tr/read {:eof reader-eof
                             :read-cond :allow
                             :features #{:clj}}
                            reader)]
          (if (identical? reader-eof form)
            (str/join "\n" (map (comp pr-str instrument-timeouts) forms))
            (recur (conj forms form))))))))

(defn- reap-finished-sci-workers!
  []
  (swap! active-sci-workers
         (fn [workers]
           (reduce-kv (fn [live worker-id {:keys [^Thread thread] :as worker-state}]
                        (if (and thread (.isAlive thread))
                          (assoc live worker-id worker-state)
                          live))
                      {}
                      workers))))

(defn- active-worker-summary
  [workers]
  {:active-workers (count workers)
   :timed-out-workers (count (filter :timed-out? (vals workers)))})

(defn- ensure-sci-worker-capacity!
  []
  (let [workers (reap-finished-sci-workers!)
        active-count (long (count workers))
        max-workers (long (max-active-sci-workers))]
    (when (>= active-count max-workers)
      (throw (ex-info (str "SCI worker capacity exceeded; "
                           active-count
                           " worker thread(s) still active")
                      (merge {:type :sci/worker-cap-exceeded
                              :status 503
                              :max-active-workers max-workers}
                             (active-worker-summary workers)))))))

(defn- register-sci-worker!
  [worker-id stage timeout-ms ^Thread worker]
  (swap! active-sci-workers
         assoc
         worker-id
         {:worker-id worker-id
          :thread worker
          :thread-name (.getName worker)
          :stage stage
          :timeout-ms timeout-ms
          :started-at (Date.)
          :timed-out? false}))

(defn- unregister-sci-worker!
  [worker-id]
  (swap! active-sci-workers dissoc worker-id))

(defn- mark-sci-worker-timed-out!
  [worker-id]
  (swap! active-sci-workers
         (fn [workers]
           (if-let [worker-state (get workers worker-id)]
             (assoc workers
                    worker-id
                    (assoc worker-state
                           :timed-out? true
                           :timed-out-at (Date.)))
             workers))))

(defn reset-runtime!
  []
  (reset! shutdown? false)
  (reap-finished-sci-workers!)
  (reset! current-ctx* (make-ctx))
  nil)

(defn- sci-worker-thread
  [worker-id stage timeout-ms f result*]
  (let [runner (bound-fn*
                 (fn []
                   (binding [*sci-timeout-state* {:stage stage
                                                  :timeout-ms timeout-ms
                                                  :deadline-nanos (+ (System/nanoTime)
                                                                     (* 1000000
                                                                        (long timeout-ms)))
                                                  :counter (long-array 1)}]
                     (try
                       (deliver result* {:status :ok
                                         :value  (f)})
                       (catch Throwable t
                         (deliver result* {:status :error
                                           :throwable t}))
                       (finally
                         (unregister-sci-worker! worker-id))))))]
    (doto (Thread.
            ^Runnable
            (reify Runnable
              (run [_]
                (runner)))
            (str "xia-sci-" (name stage) "-" (System/nanoTime)))
      (.setDaemon true))))

(defn- interrupt-sci-worker!
  [worker-id stage timeout-ms ^Thread worker]
  (.interrupt worker)
  (.join worker (long sci-timeout-stop-grace-ms))
  (when (.isAlive worker)
    (mark-sci-worker-timed-out! worker-id)
    (log/warn "Timed out SCI worker thread ignored interrupt and is still running"
              (merge {:thread (.getName worker)
                      :worker-id worker-id
                      :stage stage
                      :timeout-ms timeout-ms
                      :max-active-workers (max-active-sci-workers)}
                     (active-worker-summary (reap-finished-sci-workers!))))
    false)
  (not (.isAlive worker)))

(defn prepare-shutdown!
  []
  (reset! shutdown? true)
  (let [workers (vals (reap-finished-sci-workers!))]
    (doseq [{:keys [worker-id stage timeout-ms ^Thread thread]} workers]
      (when (and thread (.isAlive thread))
        (interrupt-sci-worker! worker-id stage timeout-ms thread)))
    (reset! current-ctx* nil)
    (count workers)))

(defn- call-with-timeout
  [timeout-ms stage f]
  (when @shutdown?
    (throw (ex-info "SCI runtime is shutting down"
                    {:type :sci/shutdown
                     :status 503
                     :stage stage})))
  (ensure-sci-worker-capacity!)
  (let [worker-id (.incrementAndGet ^AtomicLong sci-worker-seq)
        result*  (promise)
        ^Thread worker (sci-worker-thread worker-id stage timeout-ms f result*)
        timeout  (Object.)
        _        (register-sci-worker! worker-id stage timeout-ms worker)
        _        (try
                   (.start worker)
                   (catch Throwable t
                     (unregister-sci-worker! worker-id)
                     (throw t)))
        result   (deref result* timeout-ms timeout)]
    (if (identical? timeout result)
      (do
        (interrupt-sci-worker! worker-id stage timeout-ms worker)
        (throw (sci-timeout-ex stage timeout-ms)))
      (case (:status result)
        :ok    (:value result)
        :error (throw (:throwable result))))))

(defn eval-string
  "Evaluate a string of Clojure code in the SCI sandbox."
  [code-str]
  (call-with-timeout (sci-eval-timeout-ms)
                     :eval
                     #(sci/eval-string* (current-ctx)
                                        (instrument-code-string code-str))))

(defn call-fn
  "Invoke a compiled SCI function with a bounded execution time."
  [f & args]
  (call-with-timeout (sci-handler-timeout-ms)
                     :handler
                     #(apply f args)))
