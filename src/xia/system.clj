(ns xia.system
  "Integrant system definitions for Xia runtime services."
  (:require [clojure.java.io :as io]
            [integrant.core :as ig]
            [taoensso.timbre :as log]
            [xia.agent :as agent]
            [xia.async :as async]
            [xia.browser :as browser]
            [xia.channel.messaging :as messaging]
            [xia.checkpoint :as checkpoint]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.hippocampus :as hippo]
            [xia.identity :as identity]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.llm :as llm]
            [xia.local-ocr :as local-ocr]
            [xia.oauth :as oauth]
            [xia.paths :as paths]
            [xia.prompt :as prompt]
            [xia.retrieval-state :as retrieval-state]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.runtime-state :as runtime-state]
            [xia.sci-env :as sci-env]
            [xia.scheduler :as scheduler]
            [xia.setup :as setup]
            [xia.skill :as skill]
            [xia.tool :as tool]
            [xia.working-memory :as wm]
            [xia.browser.playwright :as playwright]
            [xia.channel.http :as http])
  (:import [java.nio.file Files Paths]))

(defn- ensure-db-dir!
  [db-path]
  (when-let [parent (.getParent (Paths/get db-path (make-array String 0)))]
    (Files/createDirectories parent (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- maybe-seed-instance-template!
  [{:keys [db-path instance template-instance crypto-opts]}]
  (when template-instance
    (if (db/initial-settings-empty?)
      (let [source-db-path (paths/default-db-path template-instance)
            target-db-path (.getCanonicalPath (io/file db-path))
            source-db-path* (.getCanonicalPath (io/file source-db-path))]
        (when (= source-db-path* target-db-path)
          (throw (ex-info "Template instance must be different from the target instance"
                          {:instance instance
                           :template-instance template-instance
                           :db db-path
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

(defmethod ig/init-key :xia/db
  [_ {:keys [db-path connect-options]}]
  (ensure-db-dir! db-path)
  (db/install-runtime!)
  (db/connect! db-path connect-options)
  {:db-path db-path})

(defmethod ig/halt-key! :xia/db
  [_ _]
  (db/close!)
  (db/clear-runtime!))

(defmethod ig/init-key :xia/runtime-overlay
  [_ {:keys [overlay-path]}]
  (runtime-overlay/load-file! overlay-path)
  {:overlay-path overlay-path
   :snapshot-id (runtime-overlay/snapshot-id)})

(defmethod ig/halt-key! :xia/runtime-overlay
  [_ _]
  (runtime-overlay/clear!))

(defmethod ig/init-key :xia/runtime-state-runtime
  [_ _]
  {:runtime (runtime-state/install-runtime!)})

(defmethod ig/halt-key! :xia/runtime-state-runtime
  [_ _]
  (runtime-state/clear-runtime!))

(defmethod ig/init-key :xia/retrieval-runtime
  [_ _]
  {:runtime (retrieval-state/install-runtime!)})

(defmethod ig/halt-key! :xia/retrieval-runtime
  [_ _]
  (retrieval-state/clear-runtime!))

(defmethod ig/init-key :xia/oauth-runtime
  [_ _]
  {:runtime (oauth/install-runtime!)})

(defmethod ig/halt-key! :xia/oauth-runtime
  [_ _]
  (oauth/clear-runtime!))

(defmethod ig/init-key :xia/browser-runtime
  [_ _]
  {:runtime (playwright/install-runtime!)})

(defmethod ig/halt-key! :xia/browser-runtime
  [_ _]
  (playwright/clear-runtime!))

(defmethod ig/init-key :xia/working-memory-runtime
  [_ {:keys [async-runtime]}]
  {:runtime (wm/install-runtime!)})

(defmethod ig/halt-key! :xia/working-memory-runtime
  [_ _]
  (wm/prepare-shutdown!)
  (wm/snapshot-all!)
  (wm/clear-runtime!))

(defmethod ig/init-key :xia/async-runtime
  [_ _]
  {:runtime (async/install-runtime!)})

(defmethod ig/halt-key! :xia/async-runtime
  [_ _]
  (async/clear-runtime!))

(defmethod ig/init-key :xia/prompt-runtime
  [_ {:keys [async-runtime]}]
  {:runtime (prompt/install-runtime!)
   :async-runtime async-runtime})

(defmethod ig/halt-key! :xia/prompt-runtime
  [_ _]
  (prompt/clear-runtime!))

(defmethod ig/init-key :xia/agent-runtime
  [_ {:keys [async-runtime]}]
  {:runtime (agent/install-runtime!)
   :async-runtime async-runtime})

(defmethod ig/halt-key! :xia/agent-runtime
  [_ _]
  (agent/clear-runtime!))

(defmethod ig/init-key :xia/runtime-support
  [_ {:keys [db overlay runtime-state-runtime retrieval-runtime oauth-runtime
             browser-runtime async-runtime prompt-runtime agent-runtime
             working-memory-runtime]}]
  (hippo/reset-runtime!)
  (checkpoint/reset-runtime!)
  (llm/reset-runtime!)
  (local-ocr/reset-runtime!)
  (let [recovered (agent/recover-runtime-tasks!)]
    (when (seq recovered)
      (log/info "Recovered" (count recovered) "interrupted tasks after runtime restart")))
  {:db db
   :overlay overlay
   :runtime-state-runtime runtime-state-runtime
   :retrieval-runtime retrieval-runtime
   :oauth-runtime oauth-runtime
   :browser-runtime browser-runtime
   :async-runtime async-runtime
   :prompt-runtime prompt-runtime
   :agent-runtime agent-runtime
   :working-memory-runtime working-memory-runtime})

(defmethod ig/halt-key! :xia/runtime-support
  [_ _]
  (agent/cancel-all-sessions! "runtime stopping")
  (browser/release-all-sessions!)
  (hippo/prepare-shutdown!)
  (checkpoint/prepare-shutdown!)
  (llm/prepare-shutdown!)
  (hippo/await-background-tasks!)
  (checkpoint/await-background-tasks!)
  (llm/await-background-tasks!)
  (local-ocr/reset-runtime!))

(defmethod ig/init-key :xia/http-runtime
  [_ {:keys [runtime-support]}]
  {:runtime (http/install-runtime!)
   :runtime-support runtime-support})

(defmethod ig/halt-key! :xia/http-runtime
  [_ _]
  (http/clear-runtime!))

(defmethod ig/init-key :xia/sci-runtime
  [_ {:keys [db]}]
  (sci-env/reset-runtime!)
  {:db db})

(defmethod ig/halt-key! :xia/sci-runtime
  [_ _]
  (sci-env/prepare-shutdown!))

(defmethod ig/init-key :xia/instance-supervisor
  [_ {:keys [db enabled? command]}]
  (instance-supervisor/configure! {:enabled? enabled?
                                   :command command})
  {:db db
   :enabled? enabled?
   :command command})

(defmethod ig/halt-key! :xia/instance-supervisor
  [_ _]
  (instance-supervisor/shutdown!))

(defmethod ig/init-key :xia/bootstrap
  [_ {:keys [db overlay runtime-support instance-supervisor db-path instance template-instance
             mode crypto-opts]}]
  (maybe-seed-instance-template! {:db-path db-path
                                  :instance instance
                                  :template-instance template-instance
                                  :crypto-opts crypto-opts})
  (instance-supervisor/record-parent-link-from-env!)
  (log/info "Xia instance" instance)
  (log/info "Database opened at" db-path)
  (log/info "Support directory" (paths/support-dir-path db-path))
  (log/info "Master key source" (pr-str (crypto/current-key-source)))
  (when (setup/needs-setup?)
    (if (= "terminal" mode)
      (setup/run-setup!)
      (log/info "Skipping interactive first-run setup in"
                mode
                "mode; complete provider onboarding in the local web UI.")))
  {:db db
   :overlay overlay
   :runtime-support runtime-support
   :instance-supervisor instance-supervisor
   :instance instance})

(defmethod ig/halt-key! :xia/bootstrap
  [_ _]
  nil)

(defmethod ig/init-key :xia/identity
  [_ {:keys [bootstrap]}]
  (identity/init-identity!)
  {:bootstrap bootstrap})

(defmethod ig/halt-key! :xia/identity
  [_ _]
  nil)

(defmethod ig/init-key :xia/tool-runtime
  [_ {:keys [identity sci-runtime]}]
  (let [bundled-count (tool/ensure-bundled-tools!)]
    (when (pos? (long bundled-count))
      (log/info "Installed" bundled-count "bundled tools")))
  (tool/reset-runtime!)
  (tool/load-all-tools!)
  (log/info "Loaded" (count (tool/registered-tools)) "tools,"
            (count (skill/all-enabled-skills)) "skills")
  {:identity identity
   :sci-runtime sci-runtime})

(defmethod ig/halt-key! :xia/tool-runtime
  [_ _]
  (tool/reset-runtime!))

(defmethod ig/init-key :xia/scheduler
  [_ {:keys [tool-runtime]}]
  (scheduler/install-runtime!)
  (scheduler/start!)
  {:tool-runtime tool-runtime})

(defmethod ig/halt-key! :xia/scheduler
  [_ _]
  (scheduler/stop!)
  (scheduler/clear-runtime!))

(defmethod ig/init-key :xia/messaging
  [_ {:keys [runtime-support]}]
  (messaging/start!)
  {:runtime-support runtime-support})

(defmethod ig/halt-key! :xia/messaging
  [_ _]
  (messaging/stop!))

(defmethod ig/init-key :xia/http
  [_ {:keys [http-runtime scheduler messaging bind-host port web-dev?]}]
  (http/start! bind-host port {:web-dev? (true? web-dev?)})
  {:http-runtime http-runtime
   :scheduler scheduler
   :messaging messaging
   :bind-host bind-host
   :port (or (http/current-port) port)})

(defmethod ig/halt-key! :xia/http
  [_ _]
  (http/stop!))
