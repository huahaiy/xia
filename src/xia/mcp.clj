(ns xia.mcp
  "Minimal MCP-compatible JSON-RPC facade over Xia's tool registry.

   This is intentionally exposed through the command channel rather than the
   browser session surface. It only advertises tools on an explicit allowlist,
   and tool execution still passes through Xia's normal tool approval and audit
   path."
  (:require [charred.api :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.tool :as tool]))

(def protocol-version "2025-11-25")

(def ^:private server-name "xia")

(def ^:private default-allowed-tool-ids
  #{:artifact-list
    :artifact-read
    :artifact-search
    :board-comment
    :board-create
    :board-heartbeat
    :board-claim
    :board-list
    :board-update
    :local-doc-read
    :local-doc-search
    :peer-list
    :recent-work
    :schedule-list
    :web-extract
    :web-fetch
    :web-search
    :workspace-list
    :workspace-read})

(defn- nonblank-str
  [value]
  (some-> value str str/trim not-empty))

(defn- jsonrpc-id
  [request]
  (or (get request "id")
      (:id request)))

(defn- request-method
  [request]
  (or (get request "method")
      (:method request)))

(defn- request-params
  [request]
  (or (get request "params")
      (:params request)
      {}))

(defn- param
  [params k]
  (or (get params k)
      (get params (name k))))

(defn- normalize-tool-id
  [value]
  (cond
    (keyword? value) value
    (string? value)  (keyword (str/trim value))
    :else nil))

(defn- parse-allowlist
  [raw]
  (let [value (cond
                (nil? raw) nil
                (coll? raw) raw
                (string? raw)
                (try
                  (edn/read-string raw)
                  (catch Exception _
                    (str/split raw #",")))
                :else nil)]
    (when (coll? value)
      (->> value
           (keep normalize-tool-id)
           (remove #(= :* %))
           set))))

(defn allowed-tool-ids
  []
  (cfg/custom-option :mcp/tool-allowlist
                     default-allowed-tool-ids
                     parse-allowlist))

(defn tool-allowed?
  [tool-id]
  (contains? (allowed-tool-ids) tool-id))

(defn- tool-enabled?
  [tool]
  (not (false? (:tool/enabled? tool))))

(defn- tool->mcp
  [tool]
  {"name" (name (:tool/id tool))
   "description" (or (:tool/description tool) "")
   "inputSchema" (or (:tool/parameters tool)
                     {"type" "object"
                      "properties" {}})})

(defn list-tools
  []
  (->> (db/list-tools)
       (filter tool-enabled?)
       (filter #(tool-allowed? (:tool/id %)))
       (sort-by (comp name :tool/id))
       (mapv tool->mcp)))

(defn- jsonrpc-response
  [id result]
  {"jsonrpc" "2.0"
   "id" id
   "result" result})

(defn- jsonrpc-error
  ([id code message]
   (jsonrpc-error id code message nil))
  ([id code message data]
   (cond-> {"jsonrpc" "2.0"
            "id" id
            "error" {"code" code
                     "message" message}}
     (some? data) (assoc-in ["error" "data"] data))))

(defn- initialize-result
  [params]
  (let [client-version (nonblank-str (param params :protocolVersion))]
    {"protocolVersion" (or client-version protocol-version)
     "capabilities" {"tools" {"listChanged" false}}
     "serverInfo" {"name" server-name
                   "version" "0.1.0-SNAPSHOT"}
     "instructions" "Xia exposes an allowlisted subset of its existing tools through MCP. Secret-backed work still goes through Xia's normal capability and approval checks."}))

(defn- result-error?
  [result]
  (and (map? result)
       (some? (or (:error result)
                  (get result "error")))))

(defn- result-text
  [result]
  (cond
    (nil? result) ""
    (string? result) result
    :else (json/write-json-str result)))

(defn- call-tool-result
  [result]
  (cond-> {"content" [{"type" "text"
                       "text" (result-text result)}]
           "isError" (boolean (result-error? result))}
    (map? result) (assoc "structuredContent" result)))

(defn- ensure-loaded-tool!
  [tool-id]
  (when-let [tool (db/get-tool tool-id)]
    (when (tool-enabled? tool)
      (tool/load-tool! tool-id)))
  nil)

(defn call-tool
  [name arguments context]
  (let [tool-id (normalize-tool-id name)]
    (cond
      (nil? tool-id)
      (throw (ex-info "Tool name is required"
                      {:type :mcp/invalid-tool-name}))

      (not (tool-allowed? tool-id))
      (throw (ex-info "Tool is not exposed through Xia MCP"
                      {:type :mcp/tool-not-allowed
                       :tool-id tool-id}))

      :else
      (do
        (ensure-loaded-tool! tool-id)
        (tool/execute-tool tool-id
                           (or arguments {})
                           (merge {:channel :mcp}
                                  context))))))

(defn- handle-tools-call
  [params context]
  (let [name      (param params :name)
        arguments (or (param params :arguments) {})]
    (call-tool-result (call-tool name arguments context))))

(defn handle-json-rpc
  "Handle a single MCP JSON-RPC request map.

   Returns nil for notifications, which lets the HTTP adapter answer with 202."
  ([request]
   (handle-json-rpc request nil))
  ([request context]
   (let [id     (jsonrpc-id request)
         method (request-method request)
         params (request-params request)]
     (try
       (case method
         nil
         (jsonrpc-error id -32600 "Invalid Request")

         "initialize"
         (jsonrpc-response id (initialize-result params))

         "ping"
         (jsonrpc-response id {})

         "notifications/initialized"
         nil

         "tools/list"
         (jsonrpc-response id {"tools" (list-tools)})

         "tools/call"
         (jsonrpc-response id (handle-tools-call params context))

         (jsonrpc-error id -32601 "Method not found" {"method" method}))
       (catch clojure.lang.ExceptionInfo ex
         (jsonrpc-error id
                        -32602
                        (.getMessage ex)
                        (update (ex-data ex) :tool-id #(some-> % name))))
       (catch Throwable t
         (jsonrpc-error id -32603 (.getMessage t)))))))
