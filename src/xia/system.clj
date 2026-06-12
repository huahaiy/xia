(ns xia.system
  "Integrant system definitions for Xia runtime services."
  (:require [clojure.java.io :as io]
            [integrant.core :as ig]
            [taoensso.timbre :as log]
            [xia.agent :as agent]
            [xia.async :as async]
            [xia.browser :as browser]
            [xia.bridge :as bridge]
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
            [xia.runtime-context :as runtime-context]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.runtime-state :as runtime-state]
            [xia.sci-env :as sci-env]
            [xia.scheduler :as scheduler]
            [xia.service :as service]
            [xia.setup :as setup]
            [xia.skill :as skill]
            [xia.tool :as tool]
            [xia.web :as web]
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

(defn- with-component-runtime-context
  [component f]
  (runtime-context/with-runtime-context (:runtime-context component) f))

(defmethod ig/init-key :xia/db
  [_ {:keys [db-path connect-options]}]
  (ensure-db-dir! db-path)
  (let [runtime (db/install-runtime! (db/make-runtime))
        runtime-context (runtime-context/make {:xia/db {:runtime runtime}})]
    (runtime-context/with-runtime-context
      runtime-context
      #(db/connect! db-path connect-options))
    {:runtime runtime
     :runtime-context runtime-context
     :db-path db-path}))

(defmethod ig/halt-key! :xia/db
  [_ component]
  (with-component-runtime-context
    component
    #(do
       (db/close!)
       (db/clear-runtime!))))

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
  (let [runtime (runtime-state/install-runtime! (runtime-state/make-runtime))
        runtime-context (runtime-context/make {:xia/runtime-state-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context}))

(defmethod ig/halt-key! :xia/runtime-state-runtime
  [_ component]
  (with-component-runtime-context component runtime-state/clear-runtime!))

(defmethod ig/init-key :xia/retrieval-runtime
  [_ _]
  (let [runtime (retrieval-state/install-runtime! (retrieval-state/make-runtime))
        runtime-context (runtime-context/make {:xia/retrieval-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context}))

(defmethod ig/halt-key! :xia/retrieval-runtime
  [_ component]
  (with-component-runtime-context component retrieval-state/clear-runtime!))

(defmethod ig/init-key :xia/oauth-runtime
  [_ _]
  (let [runtime (oauth/install-runtime! (oauth/make-runtime))
        runtime-context (runtime-context/make {:xia/oauth-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context}))

(defmethod ig/halt-key! :xia/oauth-runtime
  [_ component]
  (with-component-runtime-context component oauth/clear-runtime!))

(defmethod ig/init-key :xia/browser-runtime
  [_ {:keys [db]}]
  (let [runtime (playwright/install-runtime! (playwright/make-runtime))
        runtime-context (runtime-context/make {:xia/db db
                                               :xia/browser-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context
     :db db}))

(defmethod ig/halt-key! :xia/browser-runtime
  [_ component]
  (with-component-runtime-context
    component
    #(do
       (browser/release-all-sessions!)
       (playwright/clear-runtime!))))

(defmethod ig/init-key :xia/working-memory-runtime
  [_ {:keys [async-runtime]}]
  (let [runtime (wm/install-runtime! (wm/make-runtime))
        runtime-context (runtime-context/make {:xia/async-runtime async-runtime
                                               :xia/working-memory-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context}))

(defmethod ig/halt-key! :xia/working-memory-runtime
  [_ component]
  (with-component-runtime-context
    component
    #(do
       (wm/prepare-shutdown!)
       (wm/snapshot-all!)
       (wm/clear-runtime!))))

(defmethod ig/init-key :xia/async-runtime
  [_ {:keys [db]}]
  (let [runtime (async/install-runtime! (async/make-runtime))
        runtime-context (runtime-context/make {:xia/db db
                                               :xia/async-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context
     :db db}))

(defmethod ig/halt-key! :xia/async-runtime
  [_ component]
  (with-component-runtime-context
    component
    #(do
       (async/prepare-shutdown!)
       (async/await-background-tasks!)
       (async/clear-runtime!))))

(defmethod ig/init-key :xia/prompt-runtime
  [_ {:keys [async-runtime]}]
  (let [runtime (prompt/install-runtime! (prompt/make-runtime))
        runtime-context (runtime-context/make {:xia/async-runtime async-runtime
                                               :xia/prompt-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context
     :async-runtime async-runtime}))

(defmethod ig/halt-key! :xia/prompt-runtime
  [_ component]
  (with-component-runtime-context component prompt/clear-runtime!))

(defmethod ig/init-key :xia/agent-runtime
  [_ {:keys [db async-runtime]}]
  (let [runtime         (agent/install-runtime! (agent/make-runtime))
        runtime-context (runtime-context/make {:xia/db db
                                               :xia/async-runtime async-runtime
                                               :xia/agent-runtime {:runtime runtime}})
        recovered       (runtime-context/with-runtime-context
                          runtime-context
                          agent/recover-runtime-tasks!)]
    (when (seq recovered)
      (log/info "Recovered" (count recovered) "interrupted tasks after runtime restart"))
    {:runtime runtime
     :runtime-context runtime-context
     :db db
     :async-runtime async-runtime
     :recovered recovered}))

(defmethod ig/halt-key! :xia/agent-runtime
  [_ component]
  (with-component-runtime-context
    component
    #(do
       (agent/cancel-all-sessions! "runtime stopping")
       (agent/clear-runtime!))))

(defmethod ig/init-key :xia/bridge-runtime
  [_ _]
  (let [runtime (bridge/install-runtime! (bridge/make-runtime))
        runtime-context (runtime-context/make {:xia/bridge-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context}))

(defmethod ig/halt-key! :xia/bridge-runtime
  [_ {:keys [runtime] :as component}]
  (with-component-runtime-context
    component
    #(if runtime
       (bridge/clear-runtime! runtime)
       (bridge/clear-runtime!))))

(defmethod ig/init-key :xia/hippocampus-runtime
  [_ {:keys [db llm-runtime]}]
  (let [runtime (hippo/install-runtime! (hippo/make-runtime))
        runtime-context (runtime-context/make {:xia/db db
                                               :xia/llm-runtime llm-runtime
                                               :xia/hippocampus-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context
     :db db
     :llm-runtime llm-runtime}))

(defmethod ig/halt-key! :xia/hippocampus-runtime
  [_ component]
  (with-component-runtime-context
    component
    #(do
       (hippo/prepare-shutdown!)
       (hippo/await-background-tasks!)
       (hippo/clear-runtime!))))

(defmethod ig/init-key :xia/checkpoint-runtime
  [_ {:keys [db]}]
  (let [runtime (checkpoint/install-runtime! (checkpoint/make-runtime))
        runtime-context (runtime-context/make {:xia/db db
                                               :xia/checkpoint-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context
     :db db}))

(defmethod ig/halt-key! :xia/checkpoint-runtime
  [_ component]
  (with-component-runtime-context
    component
    #(do
       (checkpoint/prepare-shutdown!)
       (checkpoint/await-background-tasks!)
       (checkpoint/clear-runtime!))))

(defmethod ig/init-key :xia/llm-runtime
  [_ {:keys [db]}]
  (let [runtime (llm/install-runtime! (llm/make-runtime))
        runtime-context (runtime-context/make {:xia/db db
                                               :xia/llm-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context
     :db db}))

(defmethod ig/halt-key! :xia/llm-runtime
  [_ component]
  (with-component-runtime-context component llm/clear-runtime!))

(defmethod ig/init-key :xia/local-ocr-runtime
  [_ _]
  (let [runtime (local-ocr/install-runtime! (local-ocr/make-runtime))
        runtime-context (runtime-context/make {:xia/local-ocr-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context}))

(defmethod ig/halt-key! :xia/local-ocr-runtime
  [_ component]
  (with-component-runtime-context component local-ocr/clear-runtime!))

(defmethod ig/init-key :xia/service-runtime
  [_ _]
  (let [runtime (service/install-runtime! (service/make-runtime))
        runtime-context (runtime-context/make {:xia/service-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context}))

(defmethod ig/halt-key! :xia/service-runtime
  [_ component]
  (with-component-runtime-context component service/clear-runtime!))

(defmethod ig/init-key :xia/web-runtime
  [_ _]
  (let [runtime (web/install-runtime! (web/make-runtime))
        runtime-context (runtime-context/make {:xia/web-runtime {:runtime runtime}})]
    {:runtime runtime
     :runtime-context runtime-context}))

(defmethod ig/halt-key! :xia/web-runtime
  [_ component]
  (with-component-runtime-context component web/clear-runtime!))

(defmethod ig/init-key :xia/runtime-support
  [_ {:keys [db overlay runtime-state-runtime retrieval-runtime oauth-runtime
             browser-runtime async-runtime prompt-runtime agent-runtime
             working-memory-runtime bridge-runtime hippocampus-runtime
             checkpoint-runtime llm-runtime local-ocr-runtime
             service-runtime web-runtime]}]
  (runtime-context/make
    {:xia/db db
     :xia/runtime-overlay overlay
     :xia/runtime-state-runtime runtime-state-runtime
     :xia/retrieval-runtime retrieval-runtime
     :xia/oauth-runtime oauth-runtime
     :xia/browser-runtime browser-runtime
     :xia/async-runtime async-runtime
     :xia/prompt-runtime prompt-runtime
     :xia/agent-runtime agent-runtime
     :xia/working-memory-runtime working-memory-runtime
     :xia/bridge-runtime bridge-runtime
     :xia/hippocampus-runtime hippocampus-runtime
     :xia/checkpoint-runtime checkpoint-runtime
     :xia/llm-runtime llm-runtime
     :xia/local-ocr-runtime local-ocr-runtime
     :xia/service-runtime service-runtime
     :xia/web-runtime web-runtime}))

(defmethod ig/halt-key! :xia/runtime-support
  [_ _]
  nil)

(defmethod ig/init-key :xia/http-runtime
  [_ {:keys [runtime-support]}]
  (let [runtime (http/make-runtime)
        runtime-context (runtime-context/assoc-component runtime-support
                                                         :xia/http-runtime
                                                         {:runtime runtime})
        runtime (assoc runtime :runtime-context runtime-context)]
    (http/install-runtime! runtime)
    {:runtime runtime
     :runtime-context runtime-context
     :runtime-support runtime-support}))

(defmethod ig/halt-key! :xia/http-runtime
  [_ {:keys [runtime] :as component}]
  (with-component-runtime-context
    component
    #(if runtime
       (http/clear-runtime! runtime)
       (http/clear-runtime!))))

(defmethod ig/init-key :xia/sci-runtime
  [_ {:keys [db runtime-support]}]
  (let [runtime (sci-env/make-runtime)
        runtime-context (runtime-context/assoc-component runtime-support
                                                         :xia/sci-runtime
                                                         {:runtime runtime})
        runtime (assoc runtime :runtime-context runtime-context)]
    {:runtime (sci-env/install-runtime! runtime)
     :runtime-context runtime-context
     :db db}))

(defmethod ig/halt-key! :xia/sci-runtime
  [_ component]
  (with-component-runtime-context component sci-env/clear-runtime!))

(defmethod ig/init-key :xia/instance-supervisor
  [_ {:keys [db runtime-support enabled? command]}]
  (let [runtime (instance-supervisor/make-runtime)
        runtime-context (runtime-context/assoc-component runtime-support
                                                         :xia/instance-supervisor
                                                         {:runtime runtime})
        runtime (assoc runtime :runtime-context runtime-context)]
    (instance-supervisor/install-runtime! runtime)
    (runtime-context/with-runtime-context
      runtime-context
      #(instance-supervisor/configure! {:enabled? enabled?
                                        :command command}))
    {:runtime runtime
     :runtime-context runtime-context
     :db db
     :enabled? enabled?
     :command command}))

(defmethod ig/halt-key! :xia/instance-supervisor
  [_ component]
  (with-component-runtime-context component instance-supervisor/clear-runtime!))

(defmethod ig/init-key :xia/bootstrap
  [_ {:keys [db overlay runtime-support instance-supervisor db-path instance template-instance
             mode crypto-opts]}]
  (runtime-context/with-runtime-context
    runtime-support
    #(do
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
        :instance instance})))

(defmethod ig/halt-key! :xia/bootstrap
  [_ _]
  nil)

(defmethod ig/init-key :xia/identity
  [_ {:keys [bootstrap]}]
  (runtime-context/with-runtime-context (:runtime-support bootstrap)
                                       identity/init-identity!)
  {:bootstrap bootstrap})

(defmethod ig/halt-key! :xia/identity
  [_ _]
  nil)

(defmethod ig/init-key :xia/tool-runtime
  [_ {:keys [identity sci-runtime runtime-support]}]
  (let [runtime       (tool/make-runtime)
        runtime-context (-> runtime-support
                            (runtime-context/assoc-component :xia/sci-runtime sci-runtime)
                            (runtime-context/assoc-component :xia/tool-runtime {:runtime runtime}))
        runtime       (tool/install-runtime! (assoc runtime :runtime-context runtime-context))
        bundled-count (runtime-context/with-runtime-context
                        runtime-context
                        tool/ensure-bundled-tools!)]
    (when (pos? (long bundled-count))
      (log/info "Installed" bundled-count "bundled tools"))
    (runtime-context/with-runtime-context runtime-context tool/load-all-tools!)
    (runtime-context/with-runtime-context
      runtime-context
      #(log/info "Loaded" (count (tool/registered-tools)) "tools,"
                 (count (skill/all-enabled-skills)) "skills"))
    {:runtime runtime
     :runtime-context runtime-context
     :identity identity
     :sci-runtime sci-runtime}))

(defmethod ig/halt-key! :xia/tool-runtime
  [_ component]
  (with-component-runtime-context component tool/clear-runtime!))

(defmethod ig/init-key :xia/scheduler
  [_ {:keys [tool-runtime runtime-support]}]
  (let [runtime (scheduler/make-runtime)
        runtime-context (-> runtime-support
                            (runtime-context/assoc-component :xia/tool-runtime tool-runtime)
                            (runtime-context/assoc-component :xia/scheduler {:runtime runtime}))
        runtime (assoc runtime :runtime-context runtime-context)]
    (scheduler/install-runtime! runtime)
    (runtime-context/with-runtime-context runtime-context scheduler/start!)
    {:runtime runtime
     :runtime-context runtime-context
     :runtime-support runtime-support
     :tool-runtime tool-runtime}))

(defmethod ig/halt-key! :xia/scheduler
  [_ component]
  (with-component-runtime-context
    component
    #(do
       (scheduler/stop!)
       (scheduler/clear-runtime!))))

(defmethod ig/init-key :xia/messaging
  [_ {:keys [runtime-support]}]
  (let [runtime (messaging/make-runtime)
        runtime-context (runtime-context/assoc-component runtime-support
                                                         :xia/messaging
                                                         {:runtime runtime})
        runtime (assoc runtime :runtime-context runtime-context)]
    (messaging/install-runtime! runtime)
    (runtime-context/with-runtime-context runtime-context messaging/start!)
    {:runtime runtime
     :runtime-context runtime-context
     :runtime-support runtime-support}))

(defmethod ig/halt-key! :xia/messaging
  [_ component]
  (with-component-runtime-context component messaging/clear-runtime!))

(defmethod ig/init-key :xia/http
  [_ {:keys [http-runtime scheduler messaging bind-host port web-dev?]}]
  (let [runtime (:runtime http-runtime)]
    (http/with-runtime runtime
      #(do
         (http/start! bind-host port {:web-dev? (true? web-dev?)})
         {:http-runtime http-runtime
          :runtime runtime
          :runtime-context (:runtime-context http-runtime)
          :scheduler scheduler
          :messaging messaging
          :bind-host bind-host
          :port (or (http/current-port) port)}))))

(defmethod ig/halt-key! :xia/http
  [_ {:keys [runtime] :as component}]
  (with-component-runtime-context
    component
    #(if runtime
       (http/stop! runtime)
       (http/stop!))))
