(ns xia.tool
  "Tool system — executable functions the LLM can call via function-calling.

   Tools are code (interpreted via SCI in native-image) that the LLM
   invokes by name with structured arguments. This is the 'tool_calls'
   mechanism in the OpenAI API.

   Contrast with skills: a skill is text the LLM reads and follows;
   a tool is code the LLM triggers and gets results from."
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.llm :as llm]
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
   "tools/browser-runtime-status.edn"
   "tools/browser-bootstrap-runtime.edn"
   "tools/browser-install-deps.edn"
   "tools/browser-open.edn"
   "tools/browser-navigate.edn"
   "tools/browser-read-page.edn"
   "tools/browser-query-elements.edn"
   "tools/browser-screenshot.edn"
   "tools/browser-wait.edn"
   "tools/browser-click.edn"
   "tools/browser-fill-form.edn"
   "tools/browser-list-sessions.edn"
   "tools/browser-list-sites.edn"
   "tools/browser-close.edn"
   "tools/browser-login.edn"
   "tools/browser-login-interactive.edn"
   "tools/branch-tasks.edn"
   "tools/artifact-create.edn"
   "tools/artifact-list.edn"
   "tools/artifact-search.edn"
   "tools/artifact-read.edn"
   "tools/artifact-delete.edn"
   "tools/local-doc-search.edn"
   "tools/local-doc-read.edn"
   "tools/schedule-list.edn"
   "tools/schedule-create.edn"
   "tools/schedule-manage.edn"])

(def ^:private approval-note
  " Requires user approval before execution.")

(def ^:private ignored-selection-terms
  #{"a" "an" "and" "any" "for" "from" "get" "how" "i" "in" "into" "is"
    "it" "me" "my" "of" "on" "or" "the" "to" "up" "use" "using" "with"})

(def ^:private branch-worker-blocked-tool-ids
  #{:branch-tasks
    :browser-bootstrap-runtime
    :browser-install-deps
    :schedule-list
    :schedule-create
    :schedule-manage})

(def ^:private privileged-handler-rules
  [{:match "xia.service/request"
    :policy :session
    :autonomous-scope :service
    :reason "uses stored service credentials"}
   {:match "xia.browser/login-interactive"
    :policy :session
    :autonomous-scope nil
    :reason "prompts for interactive credentials"}
   {:match "xia.browser/login"
    :policy :session
    :autonomous-scope :site
    :reason "uses stored site credentials"}
   {:match "xia.browser/fill-form"
    :policy :session
    :autonomous-scope nil
    :reason "submits data into live browser sessions"}
   {:match "xia.browser/click"
    :policy :session
    :autonomous-scope nil
    :reason "can trigger live browser actions"}
   {:match "xia.schedule/create-schedule!"
    :policy :session
    :autonomous-scope nil
    :reason "creates autonomous background tasks"}
   {:match "xia.schedule/update-schedule!"
    :policy :session
    :autonomous-scope nil
    :reason "changes autonomous background tasks"}
   {:match "xia.schedule/remove-schedule!"
    :policy :session
    :autonomous-scope nil
    :reason "changes autonomous background tasks"}
   {:match "xia.schedule/pause-schedule!"
    :policy :session
    :autonomous-scope nil
    :reason "changes autonomous background tasks"}
   {:match "xia.schedule/resume-schedule!"
    :policy :session
    :autonomous-scope nil
    :reason "changes autonomous background tasks"}])

(defn registered-tools
  "Return all currently loaded tool handlers."
  []
  @registry)

(defn clear-session-approvals!
  "Clear cached approval decisions for a session."
  [session-id]
  (swap! session-approvals dissoc session-id))

(declare matching-privileged-rules autonomous-tool-allowed?)

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
  (let [{:keys [id name description tags parameters handler approval execution-mode]} tool-def]
    (when-not id
      (throw (ex-info "Tool definition must have an :id" {:def tool-def})))
    (db/install-tool! {:id          id
                       :name        (or name (clojure.core/name id))
                       :description (or description "")
                       :tags        (or tags #{})
                       :parameters  (or parameters {})
                       :handler     (if (string? handler) handler (pr-str handler))
                       :approval    (cond
                                      (keyword? approval) approval
                                      (string? approval) (keyword approval)
                                      :else :auto)
                       :execution-mode (cond
                                         (keyword? execution-mode) execution-mode
                                         (string? execution-mode) (keyword execution-mode)
                                         :else nil)})
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
  (or (first (matching-privileged-rules tool))
      {:policy :auto}))

(defn- matching-privileged-rules
  [tool]
  (let [handler (or (:tool/handler tool) "")]
    (filterv (fn [{:keys [match]}]
               (str/includes? handler match))
             privileged-handler-rules)))

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
  [tool context]
  (let [{:keys [policy]} (tool-approval-policy tool)
        desc             (:tool/description tool)]
    (if (or (= :auto policy)
            (autonomous-tool-allowed? tool context))
      desc
      (str desc approval-note))))

(defn- autonomous-run?
  [context]
  (autonomous/autonomous-run? context))

(defn- autonomous-supported-scope?
  [scope]
  (contains? #{:service :site} scope))

(defn- autonomous-tool-scopes
  [tool]
  (->> (matching-privileged-rules tool)
       (map :autonomous-scope)
       set))

(defn- autonomous-tool-allowed?
  [tool context]
  (let [scopes (autonomous-tool-scopes tool)]
    (and (autonomous/trusted? context)
         (seq scopes)
         (every? autonomous-supported-scope? scopes)
         (every? autonomous/scope-available? scopes))))

(defn- autonomous-block-message
  [tool context]
  (let [scopes (autonomous-tool-scopes tool)
        unavailable (remove autonomous/scope-available? (filter autonomous-supported-scope? scopes))]
    (cond
      (not (autonomous/trusted? context))
      "tool requires live approval and is unavailable during autonomous execution"

      (empty? scopes)
      (str "trusted autonomous execution is not allowed for tool "
           (name (:tool/id tool)))

      (some (complement autonomous-supported-scope?) scopes)
      (str "trusted autonomous execution is not allowed for tool "
           (name (:tool/id tool)))

      (= unavailable '(:service))
      "no approved services are available for autonomous execution"

      (= unavailable '(:site))
      "no approved site accounts are available for autonomous execution"

      (seq unavailable)
      "required services or site accounts are not approved for autonomous execution"

      :else
      (str "trusted autonomous execution is not allowed for tool "
           (name (:tool/id tool))))))

(defn- tool-visible?
  [tool context]
  (let [requires-vision? (contains? (set (:tool/tags tool)) :vision)
        vision-allowed?  (or (not requires-vision?)
                             (llm/vision-capable? (:assistant-provider context))
                             (llm/vision-capable? (:assistant-provider-id context)))
        {:keys [policy]} (tool-approval-policy tool)
        branch-worker?   (:branch-worker? context)
        branch-allowed?  (and branch-worker?
                              (= :auto policy)
                              (not (contains? branch-worker-blocked-tool-ids
                                              (:tool/id tool))))]
    (and vision-allowed?
         (cond
           branch-worker? branch-allowed?
           (autonomous-run? context)
           (or (= :auto policy)
               (autonomous-tool-allowed? tool context))
           :else true))))

(defn- execution-mode
  [tool]
  (let [mode (or (:tool/execution-mode tool)
                 (:execution-mode tool))]
    (cond
      (keyword? mode) mode
      (string? mode)  (keyword mode)
      :else           :sequential)))

(defn parallel-safe?
  "True when a tool is safe to execute in parallel with other independent
   tool calls in the same model round."
  [tool-id]
  (when-let [{:keys [tool]} (get @registry tool-id)]
    (let [{:keys [policy]} (tool-approval-policy tool)]
      (and (= :auto policy)
           (= :parallel-safe (execution-mode tool))))))

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
      (let [resource (io/resource resource-path)
            _        (when-not resource
                       (throw (ex-info "Bundled tool resource missing from classpath"
                                       {:resource-path resource-path})))
            data     (some-> resource slurp edn/read-string)
            _        (when (nil? data)
                       (throw (ex-info "Bundled tool resource was empty"
                                       {:resource-path resource-path})))
            defs (if (vector? data) data [data])
            missing (filterv #(nil? (db/get-tool (:id %))) defs)]
        (doseq [tool-def missing]
          (import-tool! tool-def))
        (doseq [{:keys [id execution-mode tags]} defs]
          (let [tool (db/get-tool id)]
            (when (and tool
                       (or (and execution-mode
                                (nil? (:tool/execution-mode tool)))
                           (and (seq tags)
                                (empty? (:tool/tags tool)))))
              (db/install-tool! (cond-> {:id id}
                                  execution-mode
                                  (assoc :execution-mode (if (keyword? execution-mode)
                                                           execution-mode
                                                           (keyword execution-mode)))
                                  (seq tags)
                                  (assoc :tags tags)))
              (when (contains? @registry id)
                (load-tool! id)))))
        (+ installed-count (clojure.core/count missing))))
    0
    bundled-tool-resources))

;; ---------------------------------------------------------------------------
;; OpenAI function-calling format
;; ---------------------------------------------------------------------------

(defn tool-definitions
  "Return all loaded tools formatted as OpenAI tool definitions."
  ([] (tool-definitions nil))
  ([context]
   (letfn [(tokenize [value]
             (->> (str/split (str/lower-case (str value)) #"[^\p{Alnum}]+")
                  (remove str/blank?)
                  (remove ignored-selection-terms)
                  (filter #(> (count %) 1))
                  set))
           (tool-tags [tool]
             (->> (:tool/tags tool)
                  (map name)
                  (map str/lower-case)
                  set))
           (tool-name-terms [tool]
             (tokenize (str (name (:tool/id tool)) " " (:tool/name tool))))
           (tool-description-terms [tool]
             (tokenize (:tool/description tool)))
           (context-terms []
             (let [wm-context (when-let [session-id (:session-id context)]
                                (wm/wm->context session-id))]
               (->> (concat
                      (tokenize (:user-message context))
                      (tokenize (:topics wm-context))
                      (mapcat (comp tokenize :name) (:entities wm-context)))
                    set)))
           (tool-match-score [tool terms]
             (let [tag-matches  (count (set/intersection (tool-tags tool) terms))
                   name-matches (count (set/intersection (tool-name-terms tool) terms))
                   desc-matches (count (set/intersection (tool-description-terms tool) terms))]
               (+ (* 8 tag-matches)
                  (* 4 name-matches)
                  desc-matches)))
           (tool-sort-name [tool]
             (str/lower-case (or (:tool/name tool)
                                 (some-> (:tool/id tool) name)
                                 "")))]
     (let [visible-tools (->> @registry
                              vals
                              (filter :handler)
                              (map :tool)
                              (filter #(tool-visible? % context))
                              vec)
           terms         (context-terms)
           selected      (if (seq terms)
                           (let [scored (->> visible-tools
                                             (map (fn [tool]
                                                    {:tool  tool
                                                     :score (tool-match-score tool terms)}))
                                             (filter #(pos? (:score %)))
                                             (sort-by (fn [{:keys [tool score]}]
                                                        [(- score) (tool-sort-name tool)]))
                                             (mapv :tool))]
                             (if (seq scored)
                               scored
                               (sort-by tool-sort-name visible-tools)))
                           (sort-by tool-sort-name visible-tools))]
       (mapv (fn [tool]
               {:type     "function"
                :function {:name        (name (:tool/id tool))
                           :description (tool-description-for-llm tool context)
                           :parameters  (or (:tool/parameters tool) {})}})
             selected)))))

(defn- approval-error
  [tool-id message]
  {:error (str "Tool " (name tool-id) " blocked: " message)})

(defn- audit-entry!
  [context tool-id tool arguments details]
  (autonomous/audit! context
                     (merge {:type      "tool"
                             :tool-id   (name tool-id)
                             :tool-name (or (:tool/name tool) (name tool-id))
                             :arguments arguments}
                            details)))

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
        autonomous?             (autonomous-run? context)
        bypass?                 (autonomous-tool-allowed? tool context)
        tool-name               (or (:tool/name tool) (name tool-id))
        request                 {:tool-id     tool-id
                                 :tool-name   tool-name
                                 :description (:tool/description tool)
                                 :arguments   arguments
                                 :policy      policy
                                 :reason      reason}]
    (cond
      bypass?
      {:allowed? true
       :policy   policy
       :mode     :autonomous-bypass}

      (and autonomous? (not= :auto policy))
      {:allowed? false
       :policy   policy
       :mode     :autonomous-blocked
       :error    (autonomous-block-message tool context)}

      :else
      (case policy
        :auto
        {:allowed? true
         :policy   policy
         :mode     :not-required}

        :session
        (if (and session-id (approved-for-session? session-id tool-id))
          {:allowed? true
           :policy   policy
           :mode     :session-cached}
          (try
            (prompt/status! {:state    :waiting
                             :phase    :approval
                             :message  (str "Waiting for approval for " tool-name)
                             :tool-id  tool-id
                             :tool-name tool-name})
            (if (prompt/approve! request)
              (do
                (remember-session-approval! session-id tool-id)
                {:allowed? true
                 :policy   policy
                 :mode     :interactive})
              {:allowed? false
               :policy   policy
               :mode     :denied
               :error    (str "user denied approval for privileged tool "
                              (name tool-id))})
            (catch Exception e
              {:allowed? false
               :policy   policy
               :mode     :approval-error
               :error    (.getMessage e)})))

        :always
        (try
          (prompt/status! {:state    :waiting
                           :phase    :approval
                           :message  (str "Waiting for approval for " tool-name)
                           :tool-id  tool-id
                           :tool-name tool-name})
          (if (prompt/approve! request)
            {:allowed? true
             :policy   policy
             :mode     :interactive}
            {:allowed? false
             :policy   policy
             :mode     :denied
             :error    (str "user denied approval for privileged tool "
                            (name tool-id))})
          (catch Exception e
            {:allowed? false
             :policy   policy
             :mode     :approval-error
             :error    (.getMessage e)}))

        {:allowed? true
         :policy   policy
         :mode     :not-required}))))

(defn execute-tool
  "Execute a tool by id with the given arguments map."
  ([tool-id arguments]
   (execute-tool tool-id arguments {}))
  ([tool-id arguments context]
   (if-let [{:keys [tool handler]} (get @registry tool-id)]
     (if (fn? handler)
       (try
         (binding [prompt/*interaction-context* context
                   wm/*session-id*            (or (:resource-session-id context)
                                                  (:session-id context))]
           (let [branch-blocked? (and (:branch-worker? context)
                                      (not (tool-visible? tool context)))
                 {:keys [allowed? error policy mode]}
                 (if branch-blocked?
                   {:allowed? false
                    :error    (str "tool " (name tool-id)
                                   " is not available to branch workers")
                    :policy   :branch
                    :mode     :branch-blocked}
                   (ensure-approved tool-id
                                    tool
                                    arguments
                                    context))]
             (if allowed?
               (do
                 (prompt/status! {:state    :running
                                  :phase    :tool
                                  :message  (str "Running tool " (or (:tool/name tool)
                                                                     (name tool-id)))
                                  :tool-id  tool-id
                                  :tool-name (or (:tool/name tool) (name tool-id))})
                 (try
                   (let [result (sci-env/call-fn handler arguments)]
                     (audit-entry! context tool-id tool arguments
                                   {:status          "success"
                                    :approval-policy (name policy)
                                    :approval-mode   (name mode)})
                     (prompt/status! {:state    :running
                                      :phase    :tool
                                      :message  (str "Finished tool " (or (:tool/name tool)
                                                                          (name tool-id)))
                                      :tool-id  tool-id
                                      :tool-name (or (:tool/name tool) (name tool-id))})
                     result)
                   (catch Exception e
                     (audit-entry! context tool-id tool arguments
                                   {:status          "error"
                                    :approval-policy (name policy)
                                    :approval-mode   (name mode)
                                    :error           (.getMessage e)})
                     (throw e))))
               (do
                 (audit-entry! context tool-id tool arguments
                               {:status          "blocked"
                                :approval-policy (name policy)
                                :approval-mode   (name mode)
                                :error           error})
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
       (do
         (audit-entry! context tool-id tool arguments
                       {:status "error"
                        :error  (str "Tool " tool-id " has no callable handler")})
         {:error (str "Tool " tool-id " has no callable handler")}))
     (do
       (when-let [audit-log (:audit-log context)]
         (swap! audit-log conj {:at      (str (java.time.Instant/now))
                                :tool-id (name tool-id)
                                :status  "error"
                                :error   (str "Unknown tool: " tool-id)
                                :arguments arguments}))
       {:error (str "Unknown tool: " tool-id)}))))
