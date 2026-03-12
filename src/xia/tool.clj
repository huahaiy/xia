(ns xia.tool
  "Tool system — executable functions the LLM can call via function-calling.

   Tools are code (interpreted via SCI in native-image) that the LLM
   invokes by name with structured arguments. This is the 'tool_calls'
   mechanism in the OpenAI API.

   Contrast with skills: a skill is text the LLM reads and follows;
   a tool is code the LLM triggers and gets results from."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [charred.api :as json]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.sci-env :as sci-env]
            [xia.working-memory :as wm]))

;; ---------------------------------------------------------------------------
;; Tool registry (runtime — compiled handlers)
;; ---------------------------------------------------------------------------

(defonce ^:private registry (atom {}))
(defonce ^:private session-approvals (atom {}))

(def ^:private bundled-tool-resources
  ["tools/web-search.edn"
   "tools/web-fetch.edn"
   "tools/web-extract.edn"
   "tools/browser-open.edn"
   "tools/browser-navigate.edn"
   "tools/browser-read-page.edn"
   "tools/browser-wait.edn"
   "tools/browser-click.edn"
   "tools/browser-fill-form.edn"
   "tools/browser-list-sessions.edn"
   "tools/browser-list-sites.edn"
   "tools/browser-close.edn"
   "tools/browser-login.edn"
   "tools/browser-login-interactive.edn"
   "tools/schedule-list.edn"
   "tools/schedule-create.edn"
   "tools/schedule-manage.edn"])

(def ^:private approval-note
  " Requires user approval before execution.")

(def ^:private privileged-handler-rules
  [{:match "xia.service/request"
    :policy :session
    :reason "uses stored service credentials"}
   {:match "xia.browser/login-interactive"
    :policy :session
    :reason "prompts for interactive credentials"}
   {:match "xia.browser/login"
    :policy :session
    :reason "uses stored site credentials"}
   {:match "xia.browser/fill-form"
    :policy :session
    :reason "submits data into live browser sessions"}
   {:match "xia.browser/click"
    :policy :session
    :reason "can trigger live browser actions"}
   {:match "xia.schedule/create-schedule!"
    :policy :session
    :reason "creates autonomous background tasks"}
   {:match "xia.schedule/update-schedule!"
    :policy :session
    :reason "changes autonomous background tasks"}
   {:match "xia.schedule/remove-schedule!"
    :policy :session
    :reason "changes autonomous background tasks"}
   {:match "xia.schedule/pause-schedule!"
    :policy :session
    :reason "changes autonomous background tasks"}
   {:match "xia.schedule/resume-schedule!"
    :policy :session
    :reason "changes autonomous background tasks"}])

(defn registered-tools
  "Return all currently loaded tool handlers."
  []
  @registry)

(defn clear-session-approvals!
  "Clear cached approval decisions for a session."
  [session-id]
  (swap! session-approvals dissoc session-id))

;; ---------------------------------------------------------------------------
;; Tool loading
;; ---------------------------------------------------------------------------

(defn- compile-handler
  "Interpret a tool handler string via SCI. Returns a callable fn."
  [handler-str]
  (when (and handler-str (seq handler-str))
    (try
      (sci-env/eval-string handler-str)
      (catch Exception e
        (log/error e "Failed to compile tool handler")
        nil))))

(defn load-tool!
  "Load a tool from the DB into the runtime registry."
  [tool-id]
  (if-let [tool (db/get-tool tool-id)]
    (let [handler (compile-handler (:tool/handler tool))]
      (swap! registry assoc tool-id
             {:tool    tool
              :handler handler})
      (log/info "Loaded tool:" (name tool-id))
      tool)
    (log/warn "Tool not found:" tool-id)))

(defn load-all-tools!
  "Load all enabled tools from DB into the runtime registry."
  []
  (doseq [tool (db/list-tools)]
    (when (:tool/enabled? tool)
      (load-tool! (:tool/id tool)))))

;; ---------------------------------------------------------------------------
;; Tool import
;; ---------------------------------------------------------------------------

(defn import-tool!
  "Import a tool from an EDN definition map. Installs in DB and loads it."
  [tool-def]
  (let [{:keys [id name description parameters handler approval]} tool-def]
    (when-not id
      (throw (ex-info "Tool definition must have an :id" {:def tool-def})))
    (db/install-tool! {:id          id
                       :name        (or name (clojure.core/name id))
                       :description (or description "")
                       :parameters  (if (map? parameters)
                                      (json/write-json-str parameters)
                                      (or parameters "{}"))
                       :handler     (if (string? handler) handler (pr-str handler))
                       :approval    (cond
                                      (keyword? approval) approval
                                      (string? approval) (keyword approval)
                                      :else :auto)})
    (load-tool! id)
    (log/info "Imported tool:" (or name (clojure.core/name id)))
    tool-def))

(defn- explicit-approval-policy
  [tool]
  (when-let [approval (or (:tool/approval tool) (:approval tool))]
    {:policy (cond
               (keyword? approval) approval
               (string? approval) (keyword approval)
               :else :auto)}))

(defn- inferred-approval-policy
  [tool]
  (let [handler (or (:tool/handler tool) "")]
    (or (some (fn [{:keys [match] :as rule}]
                (when (str/includes? handler match)
                  rule))
              privileged-handler-rules)
        {:policy :auto})))

(defn- tool-approval-policy
  [tool]
  (let [{:keys [policy] :as decision} (or (explicit-approval-policy tool)
                                          (inferred-approval-policy tool))]
    (assoc decision :policy (case policy
                              :session :session
                              :always  :always
                              :auto    :auto
                              :auto))))

(defn- tool-description-for-llm
  [tool]
  (let [{:keys [policy]} (tool-approval-policy tool)
        desc             (:tool/description tool)]
    (if (= :auto policy)
      desc
      (str desc approval-note))))

(defn import-tool-file!
  "Import tools from an EDN file."
  [path]
  (let [data (edn/read-string (slurp path))]
    (if (vector? data)
      (mapv import-tool! data)
      (import-tool! data))))

(defn ensure-bundled-tools!
  "Install bundled tool definitions that are not already present in the DB."
  []
  (reduce
    (fn [installed-count resource-path]
      (let [data (some-> resource-path io/resource slurp edn/read-string)
            defs (if (vector? data) data [data])
            missing (filterv #(nil? (db/get-tool (:id %))) defs)]
        (doseq [tool-def missing]
          (import-tool! tool-def))
        (+ installed-count (clojure.core/count missing))))
    0
    bundled-tool-resources))

;; ---------------------------------------------------------------------------
;; OpenAI function-calling format
;; ---------------------------------------------------------------------------

(defn tool-definitions
  "Return all loaded tools formatted as OpenAI tool definitions."
  []
  (->> @registry
       vals
       (filter :handler)
       (mapv (fn [{:keys [tool]}]
               {:type     "function"
                :function {:name        (name (:tool/id tool))
                           :description (tool-description-for-llm tool)
                           :parameters  (json/read-json
                                          (or (:tool/parameters tool) "{}"))}}))))

(defn- approval-error
  [tool-id message]
  {:error (str "Tool " (name tool-id) " blocked: " message)})

(defn- approved-for-session?
  [session-id tool-id]
  (contains? (get @session-approvals session-id #{}) tool-id))

(defn- remember-session-approval!
  [session-id tool-id]
  (when session-id
    (swap! session-approvals update session-id (fnil conj #{}) tool-id)))

(defn- ensure-approved
  [tool-id tool arguments context]
  (let [{:keys [policy reason]} (tool-approval-policy tool)
        session-id              (:session-id context)
        bypass?                 (:approval-bypass? context)
        tool-name               (or (:tool/name tool) (name tool-id))
        request                 {:tool-id     tool-id
                                 :tool-name   tool-name
                                 :description (:tool/description tool)
                                 :arguments   arguments
                                 :policy      policy
                                 :reason      reason}]
    (if bypass?
      {:allowed? true}
      (case policy
        :auto
        {:allowed? true}

        :session
        (if (and session-id (approved-for-session? session-id tool-id))
          {:allowed? true}
          (try
            (prompt/status! {:state    :waiting
                             :phase    :approval
                             :message  (str "Waiting for approval for " tool-name)
                             :tool-id  tool-id
                             :tool-name tool-name})
            (if (prompt/approve! request)
              (do
                (remember-session-approval! session-id tool-id)
                {:allowed? true})
              {:allowed? false
               :error    (str "user denied approval for privileged tool "
                              (name tool-id))})
            (catch Exception e
              {:allowed? false :error (.getMessage e)})))

        :always
        (try
          (prompt/status! {:state    :waiting
                           :phase    :approval
                           :message  (str "Waiting for approval for " tool-name)
                           :tool-id  tool-id
                           :tool-name tool-name})
          (if (prompt/approve! request)
            {:allowed? true}
            {:allowed? false
             :error    (str "user denied approval for privileged tool "
                            (name tool-id))})
          (catch Exception e
            {:allowed? false :error (.getMessage e)}))

        {:allowed? true}))))

(defn execute-tool
  "Execute a tool by id with the given arguments map."
  ([tool-id arguments]
   (execute-tool tool-id arguments {}))
  ([tool-id arguments context]
   (if-let [{:keys [tool handler]} (get @registry tool-id)]
    (if (fn? handler)
      (try
        (binding [prompt/*interaction-context* context
                  wm/*session-id*            (:session-id context)]
          (let [{:keys [allowed? error]} (ensure-approved tool-id tool arguments context)]
            (if allowed?
              (do
                (prompt/status! {:state    :running
                                 :phase    :tool
                                 :message  (str "Running tool " (or (:tool/name tool)
                                                                    (name tool-id)))
                                 :tool-id  tool-id
                                 :tool-name (or (:tool/name tool) (name tool-id))})
                (let [result (handler arguments)]
                  (prompt/status! {:state    :running
                                   :phase    :tool
                                   :message  (str "Finished tool " (or (:tool/name tool)
                                                                       (name tool-id)))
                                   :tool-id  tool-id
                                   :tool-name (or (:tool/name tool) (name tool-id))})
                  result))
              (do
                (prompt/status! {:state    :running
                                 :phase    :approval
                                 :message  (str "Skipped tool " (or (:tool/name tool)
                                                                    (name tool-id))
                                                ": " error)
                                 :tool-id  tool-id
                                 :tool-name (or (:tool/name tool) (name tool-id))})
                (approval-error tool-id error)))))
        (catch Exception e
          (prompt/status! {:state   :error
                           :phase   :tool
                           :message (str "Tool " (name tool-id) " failed: " (.getMessage e))
                           :tool-id tool-id
                           :tool-name (or (:tool/name tool) (name tool-id))})
          (log/error e "Tool execution failed:" tool-id)
          {:error (str "Tool execution failed: " (.getMessage e))}))
      {:error (str "Tool " tool-id " has no callable handler")})
    {:error (str "Unknown tool: " tool-id)})))
