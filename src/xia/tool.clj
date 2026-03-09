(ns xia.tool
  "Tool system — executable functions the LLM can call via function-calling.

   Tools are code (interpreted via SCI in native-image) that the LLM
   invokes by name with structured arguments. This is the 'tool_calls'
   mechanism in the OpenAI API.

   Contrast with skills: a skill is text the LLM reads and follows;
   a tool is code the LLM triggers and gets results from."
  (:require [clojure.tools.logging :as log]
            [charred.api :as json]
            [xia.db :as db]
            [xia.sci-env :as sci-env]))

;; ---------------------------------------------------------------------------
;; Tool registry (runtime — compiled handlers)
;; ---------------------------------------------------------------------------

(defonce ^:private registry (atom {}))

(defn registered-tools
  "Return all currently loaded tool handlers."
  []
  @registry)

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
  (let [{:keys [id name description parameters handler]} tool-def]
    (when-not id
      (throw (ex-info "Tool definition must have an :id" {:def tool-def})))
    (db/install-tool! {:id          id
                       :name        (or name (clojure.core/name id))
                       :description (or description "")
                       :parameters  (if (map? parameters)
                                      (json/write-json-str parameters)
                                      (or parameters "{}"))
                       :handler     (if (string? handler) handler (pr-str handler))})
    (load-tool! id)
    (log/info "Imported tool:" (or name (clojure.core/name id)))
    tool-def))

(defn import-tool-file!
  "Import tools from an EDN file."
  [path]
  (let [data (read-string (slurp path))]
    (if (vector? data)
      (mapv import-tool! data)
      (import-tool! data))))

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
                           :description (:tool/description tool)
                           :parameters  (json/read-json
                                          (or (:tool/parameters tool) "{}"))}}))))

(defn execute-tool
  "Execute a tool by id with the given arguments map."
  [tool-id arguments]
  (if-let [{:keys [handler]} (get @registry tool-id)]
    (if (fn? handler)
      (try
        (handler arguments)
        (catch Exception e
          (log/error e "Tool execution failed:" tool-id)
          {:error (str "Tool execution failed: " (.getMessage e))}))
      {:error (str "Tool " tool-id " has no callable handler")})
    {:error (str "Unknown tool: " tool-id)}))
