(ns xia.agent.recorder
  "Persistence and audit helpers for agent messages and tool rounds."
  (:require [xia.agent.task-runtime :as task-runtime]
            [xia.agent.tools :as agent-tools]
            [xia.audit :as audit]
            [xia.db :as db]))

(defn- task-runtime-deps
  []
  {:truncate-summary agent-tools/truncate-summary
   :sanitized-tool-result agent-tools/sanitized-tool-result})

(defn persist-assistant-message!
  [session-id text execution-context response local-doc-ids artifact-ids]
  (let [{:keys [llm-call-id provider-id model workload]} (agent-tools/response-provenance response)
        assistant-message-id
        (db/add-message! session-id :assistant text
                         :llm-call-id llm-call-id
                         :provider-id provider-id
                         :model model
                         :workload workload
                         :local-doc-ids local-doc-ids
                         :artifact-ids artifact-ids)]
    (task-runtime/record-task-message-item! (task-runtime-deps)
                                            (:task-turn-id execution-context)
                                            :assistant-message
                                            :assistant
                                            text
                                            :message-id assistant-message-id
                                            :llm-call-id llm-call-id
                                            :data (cond-> {:provider-id provider-id
                                                           :model model
                                                           :workload workload}
                                                    (seq local-doc-ids) (assoc :local-doc-ids (vec local-doc-ids))
                                                    (seq artifact-ids) (assoc :artifact-ids (vec artifact-ids))))
    (audit/log! execution-context
                {:actor :assistant
                 :type :llm-response
                 :message-id assistant-message-id
                 :llm-call-id llm-call-id
                 :data {:provider-id (some-> provider-id name)
                        :model model
                        :workload (some-> workload name)
                        :tool-calls []}})))

(defn persist-tool-result-message!
  [session-id execution-context llm-call-id provider-id model workload tool-result]
  (let [tool-name (:tool_name tool-result)
        tool-call-id (:tool_call_id tool-result)
        tool-message-id (db/add-message! session-id :tool
                                         nil
                                         :tool-result (:result tool-result)
                                         :tool-id tool-name
                                         :tool-call-id tool-call-id
                                         :tool-name tool-name
                                         :llm-call-id llm-call-id
                                         :provider-id provider-id
                                         :model model
                                         :workload workload)]
    (task-runtime/record-task-tool-result-item! (task-runtime-deps)
                                                (:task-turn-id execution-context)
                                                tool-message-id
                                                llm-call-id
                                                tool-result)
    (audit/log! execution-context
                {:actor :assistant
                 :type :tool-result
                 :message-id tool-message-id
                 :llm-call-id llm-call-id
                 :tool-id tool-name
                 :tool-call-id tool-call-id
                 :data (agent-tools/tool-result-audit-data tool-result)})
    tool-message-id))

(defn record-tool-round!
  [session-id execution-context provenance assistant-content tool-calls tool-results
   local-doc-ids artifact-ids]
  (let [{:keys [llm-call-id provider-id model workload]} provenance
        assistant-message-id
        (db/add-message! session-id :assistant
                         assistant-content
                         :tool-calls tool-calls
                         :llm-call-id llm-call-id
                         :provider-id provider-id
                         :model model
                         :workload workload
                         :local-doc-ids local-doc-ids
                         :artifact-ids artifact-ids)
        tool-summary (agent-tools/tool-call-summary tool-calls)]
    (task-runtime/record-task-message-item! (task-runtime-deps)
                                            (:task-turn-id execution-context)
                                            :assistant-message
                                            :assistant
                                            assistant-content
                                            :message-id assistant-message-id
                                            :llm-call-id llm-call-id
                                            :data {:provider-id provider-id
                                                   :model model
                                                   :workload workload
                                                   :tool-calls tool-summary})
    (task-runtime/record-task-tool-call-items! (:task-turn-id execution-context)
                                               assistant-message-id
                                               llm-call-id
                                               tool-calls)
    (audit/log! execution-context
                {:actor :assistant
                 :type :llm-response
                 :message-id assistant-message-id
                 :llm-call-id llm-call-id
                 :data {:provider-id (some-> provider-id name)
                        :model model
                        :workload (some-> workload name)
                        :tool-calls tool-summary}})
    (doseq [tr tool-results]
      (persist-tool-result-message! session-id
                                    execution-context
                                    llm-call-id
                                    provider-id
                                    model
                                    workload
                                    tr))
    assistant-message-id))
