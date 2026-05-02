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
            [xia.audit :as audit]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.prompt :as prompt]
            [xia.sci-env :as sci-env]
            [xia.task-policy :as task-policy]
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
   "tools/recent-work.edn"
   "tools/peer-list.edn"
   "tools/peer-chat.edn"
   "tools/peer-instance-list.edn"
   "tools/peer-instance-start.edn"
   "tools/peer-instance-status.edn"
   "tools/peer-instance-stop.edn"
   "tools/workspace-list.edn"
   "tools/workspace-read.edn"
   "tools/workspace-publish-artifact.edn"
   "tools/workspace-publish-doc.edn"
   "tools/workspace-write-note.edn"
   "tools/workspace-import-artifact.edn"
   "tools/workspace-import-doc.edn"
   "tools/artifact-create.edn"
   "tools/artifact-list.edn"
   "tools/artifact-search.edn"
   "tools/artifact-read.edn"
   "tools/artifact-delete.edn"
   "tools/email-label-list.edn"
   "tools/email-list.edn"
   "tools/email-read.edn"
   "tools/email-send.edn"
   "tools/email-delete.edn"
   "tools/email-update.edn"
   "tools/email-draft-list.edn"
   "tools/email-draft-read.edn"
   "tools/email-draft-save.edn"
   "tools/email-draft-send.edn"
   "tools/email-draft-delete.edn"
   "tools/calendar-list.edn"
   "tools/calendar-event-list.edn"
   "tools/calendar-event-read.edn"
   "tools/calendar-event-create.edn"
   "tools/calendar-event-update.edn"
   "tools/calendar-event-delete.edn"
   "tools/calendar-availability.edn"
   "tools/memory-correct-fact.edn"
   "tools/local-doc-search.edn"
   "tools/local-doc-read.edn"
   "tools/schedule-list.edn"
   "tools/schedule-create.edn"
   "tools/schedule-manage.edn"])

(def ^:private approval-note
  " Requires user approval before execution.")

(def ^:private max-tool-result-depth 12)
(def ^:private max-tool-result-items 1000)

(def ^:private ignored-selection-terms
  #{"a" "an" "and" "any" "for" "from" "get" "how" "i" "in" "into" "is"
    "it" "me" "my" "of" "on" "or" "the" "to" "up" "use" "using" "with"})

(def ^:private expected-tool-input-error-types
  #{:artifact/missing-content
    :artifact/unsupported-kind
    :artifact/invalid-bytes
    :artifact/invalid-bytes-base64
   :artifact/invalid-csv-rows
   :artifact/session-required
   :artifact/session-not-found
   :artifact/not-found
   :local-doc/invalid-session-id
   :local-doc/session-required
   :local-doc/session-not-found
   :local-doc/unsupported-format
   :workspace/invalid-workspace-id
   :workspace/invalid-item-id
   :workspace/invalid-source-type
   :workspace/session-required
   :workspace/not-found
   :workspace/source-not-found
   :workspace/payload-missing
   :workspace/missing-content
   :email/invalid-body
   :email/invalid-attachment-bytes
   :email/invalid-attachment-bytes-base64
   :email/missing-attachment-content
   :email/attachment-artifact-not-found
   :email/missing-message-id
   :email/missing-draft-id
   :calendar/missing-event-id
   :calendar/missing-time-range
   :calendar/missing-summary
   :calendar/missing-event-time
   :calendar/unsupported-recurrence
   :calendar/read-only-backend
   :calendar/event-not-found
   :instance-supervisor/capability-disabled
   :instance-supervisor/command-unavailable
   :instance-supervisor/invalid-instance-id
   :instance-supervisor/invalid-template-instance
   :instance-supervisor/invalid-bind
   :instance-supervisor/invalid-port
   :instance-supervisor/instance-conflict
   :instance-supervisor/already-running
   :instance-supervisor/not-found})

(defn- expected-tool-input-error?
  [^Throwable e]
  (contains? expected-tool-input-error-types
             (some-> e ex-data :type)))

(defn- cancelled-tool-error?
  [^Throwable e]
  (contains? #{:request-cancelled :sci/shutdown}
             (some-> e ex-data :type)))

(defn- safe-result-preview
  [value]
  (let [text (try
               (pr-str value)
               (catch Throwable _
                 (str "<" (.getName (class value)) ">")))]
    (if (> (count text) 240)
      (str (subs text 0 239) "…")
      text)))

(declare normalize-tool-result-value)

(defn- unsupported-tool-result-ex
  [tool-id value reason]
  (ex-info (str "Tool handler returned an unsupported result value: " reason)
           {:type :tool/invalid-result
            :tool-id tool-id
            :reason reason
            :result-class (some-> value class .getName)
            :preview (safe-result-preview value)}))

(defn- normalize-tool-result-key
  [tool-id key]
  (cond
    (string? key) key
    (keyword? key) key
    (symbol? key) (str key)
    (number? key) (str key)
    (boolean? key) (str key)
    (nil? key) "null"
    (instance? java.util.UUID key) (str key)
    (instance? java.time.Instant key) (str key)
    (instance? java.util.Date key) (str (.toInstant ^java.util.Date key))
    :else (throw (unsupported-tool-result-ex tool-id key "unsupported map key type"))))

(defn- normalize-tool-result-coll
  [tool-id coll depth]
  (let [items (doall (take (inc max-tool-result-items) coll))]
    (when (> (count items) max-tool-result-items)
      (throw (unsupported-tool-result-ex tool-id coll "collection exceeds size limit")))
    (mapv #(normalize-tool-result-value tool-id % (inc depth)) items)))

(defn- normalize-tool-result-value
  [tool-id value depth]
  (when (> depth max-tool-result-depth)
    (throw (unsupported-tool-result-ex tool-id value "result nesting is too deep")))
  (cond
    (or (nil? value)
        (string? value)
        (boolean? value)
        (number? value)
        (keyword? value))
    value

    (symbol? value)
    (str value)

    (instance? java.util.UUID value)
    value

    (instance? java.time.Instant value)
    value

    (instance? java.util.Date value)
    value

    (map? value)
    (do
      (when (> (count value) max-tool-result-items)
        (throw (unsupported-tool-result-ex tool-id value "map exceeds size limit")))
      (reduce-kv (fn [m k v]
                   (assoc m
                          (normalize-tool-result-key tool-id k)
                          (normalize-tool-result-value tool-id v (inc depth))))
                 {}
                 value))

    (vector? value)
    (normalize-tool-result-coll tool-id value depth)

    (set? value)
    (normalize-tool-result-coll tool-id (seq value) depth)

    (sequential? value)
    (normalize-tool-result-coll tool-id value depth)

    :else
    (throw (unsupported-tool-result-ex tool-id value "unsupported value type"))))

(defn- normalize-tool-result
  [tool-id value]
  (normalize-tool-result-value tool-id value 0))

(defn registered-tools
  "Return all currently loaded tool handlers."
  []
  @registry)

(defn reset-runtime!
  "Clear runtime-only tool state so a fresh load can rebuild handlers."
  []
  (reset! registry {})
  (reset! session-approvals {}))

(defn clear-session-approvals!
  "Clear cached approval decisions for a session."
  [session-id]
  (swap! session-approvals dissoc session-id))

(declare autonomous-tool-allowed?)

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

(defn- bundled-tool-install-map
  [{:keys [id name description tags parameters handler approval execution-mode]}]
  {:id          id
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

(defn- installed-tool-install-map
  [tool]
  {:id             (:tool/id tool)
   :name           (:tool/name tool)
   :description    (:tool/description tool)
   :tags           (:tool/tags tool)
   :parameters     (:tool/parameters tool)
   :handler        (:tool/handler tool)
   :approval       (:tool/approval tool)
   :execution-mode (:tool/execution-mode tool)})

(defn- bundled-tool-refresh-needed?
  [installed desired]
  (let [keys* (cond-> [:name :description :tags :parameters :handler :approval]
                (some? (:execution-mode desired)) (conj :execution-mode))]
    (some (fn [k]
            (not= (get installed k) (get desired k)))
          keys*)))

(defn- tool-approval-policy
  [tool]
  (task-policy/tool-approval-policy tool))

(defn- tool-channel-compatible?
  [tool context]
  (case (:tool/id tool)
    :browser-login-interactive
    (= :terminal (or (:channel context) :terminal))
    true))

(defn- tool-channel-block-message
  [tool]
  (case (:tool/id tool)
    :browser-login-interactive "interactive login is only available in terminal sessions"
    (str "tool " (name (:tool/id tool)) " is not available on this channel")))

(defn- tool-requires-vision?
  [tool]
  (contains? (set (:tool/tags tool)) :vision))

(defn- tool-vision-compatible?
  [tool context]
  (or (not (tool-requires-vision? tool))
      (and (nil? (:assistant-provider context))
           (nil? (:assistant-provider-id context)))
      (llm/vision-capable? (:assistant-provider context))
      (llm/vision-capable? (:assistant-provider-id context))))

(defn- tool-vision-block-message
  [_tool]
  "requires a vision-capable model")

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

(defn- autonomous-tool-allowed?
  [tool context]
  (task-policy/autonomous-tool-allowed? tool
                                        (autonomous/trusted? context)
                                        autonomous/scope-available?))

(defn- autonomous-block-message
  [tool context]
  (task-policy/autonomous-tool-block-message tool
                                             (autonomous/trusted? context)
                                             autonomous/scope-available?))

(defn- tool-visible?
  [tool context]
  (let [channel-compatible? (tool-channel-compatible? tool context)
        vision-compatible?  (tool-vision-compatible? tool context)
        approval-decision  (tool-approval-policy tool)
        {:keys [policy]} approval-decision
        branch-worker?   (:branch-worker? context)
        branch-allowed?  (task-policy/branch-worker-tool-allowed? tool approval-decision)]
    (and channel-compatible?
         vision-compatible?
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

(defn restart-risk-policy
  "Return the restart-risk policy decision for a loaded tool."
  [tool-id]
  (if-let [{:keys [tool]} (get @registry tool-id)]
    (task-policy/tool-restart-risk-policy tool (tool-approval-policy tool))
    {:decision-type :tool-restart-risk-policy
     :tool-id tool-id
     :tool-name (name tool-id)
     :tool-risk? true
     :mode :unknown-tool
     :reason "tool is not loaded, so automatic replay is not considered restart-safe"}))

(defn import-tool-file!
  "Import tools from an EDN file."
  [path]
  (let [data (edn/read-string (slurp path))]
    (if (vector? data)
      (mapv import-tool! data)
      (import-tool! data))))

(defn ensure-bundled-tools!
  "Install bundled tool definitions and refresh existing bundled definitions."
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
            defs (if (vector? data) data [data])]
        (+ (long installed-count)
           (reduce
             (fn [count* tool-def]
               (let [desired (bundled-tool-install-map tool-def)
                     id      (:id desired)]
                 (if-let [tool (db/get-tool id)]
                   (do
                     (when (bundled-tool-refresh-needed?
                             (installed-tool-install-map tool)
                             desired)
                       (db/install-tool! desired)
                       (when (contains? @registry id)
                         (load-tool! id)))
                     count*)
                   (do
                     (import-tool! tool-def)
                     (inc count*)))))
             0
             defs))))
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
                                             (filter #(pos? (long (:score %))))
                                             (sort-by (fn [{:keys [tool score]}]
                                                        [(- (long score)) (tool-sort-name tool)]))
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
  (let [entry (merge {:type      "tool"
                      :tool-id   (name tool-id)
                      :tool-name (or (:tool/name tool) (name tool-id))
                      :arguments arguments}
                     details)]
    (autonomous/audit! context entry)
    (audit/log! context
                {:actor        :assistant
                 :type         :tool-execution
                 :llm-call-id  (:llm-call-id context)
                 :tool-id      (name tool-id)
                 :tool-call-id (:tool-call-id context)
                 :data         entry})))

(defn- approved-for-session?
  [session-id approval-key]
  (contains? (get @session-approvals session-id #{}) approval-key))

(defn- remember-session-approval!
  [session-id approval-key]
  (when session-id
    (swap! session-approvals update session-id (fnil conj #{}) approval-key)))

(defn- session-approval-key
  [tool-id tool]
  (or (:session-scope (tool-approval-policy tool))
      tool-id))

(defn- ensure-approved
  [tool-id tool arguments context]
  (let [{:keys [policy reason]} (tool-approval-policy tool)
        session-id              (:session-id context)
        approval-key            (session-approval-key tool-id tool)
        autonomous?             (autonomous-run? context)
        bypass?                 (autonomous-tool-allowed? tool context)
        tool-name               (or (:tool/name tool) (name tool-id))
        request                 {:tool-id     tool-id
                                 :tool-name   tool-name
                                 :description (:tool/description tool)
                                 :arguments   arguments
                                 :policy      policy
                                 :reason      reason}]
    (letfn [(record-decision [decision]
              (prompt/policy-decision! (assoc decision
                                              :decision-type :approval-policy
                                              :tool-id tool-id
                                              :tool-name tool-name))
              decision)
            (interactive-decision []
              (try
                (prompt/status! {:state    :waiting
                                 :phase    :approval
                                 :message  (str "Waiting for approval for " tool-name)
                                 :tool-id  tool-id
                                 :tool-name tool-name})
                (if (prompt/approve! request)
                  (do
                    (remember-session-approval! session-id approval-key)
                    {:allowed? true
                     :policy   policy
                     :mode     :interactive
                     :reason   reason})
                  {:allowed? false
                   :policy   policy
                   :mode     :denied
                   :reason   reason
                   :error    (str "user denied approval for privileged tool "
                                  (name tool-id))})
                (catch Exception e
                  {:allowed? false
                   :policy   policy
                   :mode     :approval-error
                   :reason   reason
                   :error    (.getMessage e)})))]
      (record-decision
       (cond
         bypass?
         {:allowed? true
          :policy   policy
          :mode     :autonomous-bypass
          :reason   reason}

         (and autonomous? (not= :auto policy))
         {:allowed? false
          :policy   policy
          :mode     :autonomous-blocked
          :reason   reason
          :error    (autonomous-block-message tool context)}

         :else
         (case policy
           :auto
           {:allowed? true
            :policy   policy
            :mode     :not-required
            :reason   reason}

           :session
           (if (and session-id (approved-for-session? session-id approval-key))
             {:allowed? true
              :policy   policy
              :mode     :session-cached
              :reason   reason}
             (interactive-decision))

           :always
           (interactive-decision)

           {:allowed? true
            :policy   policy
            :mode     :not-required
            :reason   reason}))))))

(defn execute-tool
  "Execute a tool by id with the given arguments map."
  ([tool-id arguments]
   (execute-tool tool-id arguments {}))
  ([tool-id arguments context]
   (if-let [{:keys [tool handler]} (get @registry tool-id)]
     (if (fn? handler)
        (try
          (binding [prompt/*interaction-context* (assoc context
                                                       :tool-id tool-id
                                                       :tool-name (or (:tool/name tool)
                                                                      (name tool-id)))
                   wm/*session-id*            (or (:resource-session-id context)
                                                  (:session-id context))]
           (let [channel-blocked? (not (tool-channel-compatible? tool context))
                 vision-compatible? (tool-vision-compatible? tool context)
                 branch-worker? (:branch-worker? context)
                 branch-allowed? (task-policy/branch-worker-tool-allowed?
                                  tool
                                  (tool-approval-policy tool))
                 tool-name (or (:tool/name tool) (name tool-id))
                 decision-context {:tool-id tool-id
                                   :tool-name tool-name
                                   :channel-compatible? (not channel-blocked?)
                                   :channel-error (tool-channel-block-message tool)
                                   :vision-compatible? vision-compatible?
                                   :vision-error (tool-vision-block-message tool)
                                   :branch-worker? branch-worker?
                                   :branch-allowed? branch-allowed?
                                   :branch-error (str "tool " (name tool-id)
                                                      " is not available to branch workers")}
                 preflight-decision (task-policy/tool-execution-decision decision-context)
                 approval-decision (when (:allowed? preflight-decision)
                                     (ensure-approved tool-id
                                                      tool
                                                      arguments
                                                      context))
                 {:keys [allowed? error policy mode] :as execution-decision}
                 (if approval-decision
                   (task-policy/tool-execution-decision
                    (assoc decision-context :approval-decision approval-decision))
                   preflight-decision)
                 _ (prompt/policy-decision! execution-decision)]
             (if allowed?
               (do
                 (prompt/status! {:state    :running
                                  :phase    :tool
                                  :message  (str "Running tool " tool-name)
                                  :tool-id  tool-id
                                  :tool-name tool-name})
                 (try
                   (let [result (normalize-tool-result tool-id
                                                      (sci-env/call-fn handler arguments))]
                     (audit-entry! context tool-id tool arguments
                                   {:status          "success"
                                    :approval-policy (name policy)
                                    :approval-mode   (name mode)})
                     (prompt/status! {:state    :running
                                      :phase    :tool
                                      :message  (str "Finished tool " tool-name)
                                      :tool-id  tool-id
                                      :tool-name tool-name})
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
                                  :message  (str "Skipped tool " tool-name
                                                 ": " error)
                                  :tool-id  tool-id
                                  :tool-name tool-name})
                 (approval-error tool-id error)))))
         (catch Exception e
           (let [cancelled? (cancelled-tool-error? e)
                 message    (if cancelled?
                              (str "Tool " (name tool-id) " cancelled: " (.getMessage e))
                              (str "Tool " (name tool-id) " failed: " (.getMessage e)))]
             (prompt/status! {:state   (if cancelled? :cancelled :error)
                              :phase   :tool
                              :message message
                              :tool-id tool-id
                              :tool-name (or (:tool/name tool) (name tool-id))})
             (cond
               cancelled?
               (log/info "Tool execution cancelled:" tool-id
                         "type" (some-> e ex-data :type)
                         "message" (.getMessage e))

               (expected-tool-input-error? e)
             (log/warn "Tool execution rejected invalid input:" tool-id
                       "type" (some-> e ex-data :type)
                       "message" (.getMessage e))
               :else
               (log/error e "Tool execution failed:" tool-id))
             {:error message})))
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
