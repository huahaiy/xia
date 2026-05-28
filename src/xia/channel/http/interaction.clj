(ns xia.channel.http.interaction
  "HTTP prompt and approval interaction handlers."
  (:require [xia.bridge :as bridge])
  (:import [java.util Date]))

(def ^:private interaction-timeout-ms (* 5 60 1000))

(defn- instant->str
  [value]
  (cond
    (instance? Date value) (str (.toInstant ^Date value))
    (instance? java.time.Instant value) (str value)
    :else nil))

(defn approval->body
  [{:keys [approval-id tool-id tool-name description arguments reason policy created-at]}]
  {:approval_id approval-id
   :tool_id     (name tool-id)
   :tool_name   tool-name
   :description description
   :arguments   arguments
   :reason      reason
   :policy      (name policy)
   :created_at  (instant->str created-at)})

(defn prompt->body
  [{:keys [prompt-id label mask? created-at]}]
  {:prompt_id  prompt-id
   :label      label
   :masked     (boolean mask?)
   :created_at (instant->str created-at)})

(defn prompt-handler
  [label & {:keys [mask?] :or {mask? false}}]
  (let [interaction-context (bridge/interaction-context)
        sid (some-> (:session-id interaction-context) str)]
    (when-not sid
      (throw (ex-info "HTTP prompt requires a session id"
                      {:label label})))
    (let [task-id   (or (:task-id interaction-context)
                        (bridge/current-session-task-id sid))
          prompt-id (str (random-uuid))
          response  (promise)
          prompt*   (bridge/register-interaction!
                     {:interaction-id prompt-id
                      :kind :prompt
                      :channel (or (:channel interaction-context) :http)
                      :session-id sid
                      :task-id task-id
                      :prompt-id  prompt-id
                      :label      label
                      :mask?      (boolean mask?)
                      :created-at (Date.)
                      :response   response})]
      (try
        (let [result (deref response interaction-timeout-ms ::timeout)]
          (if (= result ::timeout)
            (throw (ex-info "Timed out waiting for interactive input"
                            {:label label
                             :session-id sid}))
            (str (or result ""))))
        (finally
          (bridge/clear-pending-interaction! {:interaction-id (:interaction-id prompt*)}))))))

(defn approval-handler
  [{:keys [session-id tool-id tool-name description arguments reason policy]}]
  (let [interaction-context (bridge/interaction-context)
        sid (some-> session-id str)]
    (when-not sid
      (throw (ex-info "HTTP approval requires a session id"
                      {:tool-id tool-id})))
    (let [task-id     (or (:task-id interaction-context)
                          (bridge/current-session-task-id sid))
          approval-id (str (random-uuid))
          response    (promise)
          approval*   (bridge/register-interaction!
                       {:interaction-id approval-id
                        :kind :approval
                        :channel (or (:channel interaction-context) :http)
                        :session-id sid
                        :task-id task-id
                        :approval-id approval-id
                        :tool-id     tool-id
                        :tool-name   (or tool-name (name tool-id))
                        :description description
                        :arguments   arguments
                        :reason      reason
                        :policy      policy
                        :created-at  (Date.)
                        :response    response})]
      (try
        (let [result (deref response interaction-timeout-ms ::timeout)]
          (case result
            :allow true
            :deny  false
            (throw (ex-info "Timed out waiting for tool approval"
                            {:tool-id tool-id
                             :session-id sid}))))
        (finally
          (bridge/clear-pending-interaction! {:interaction-id (:interaction-id approval*)}))))))
