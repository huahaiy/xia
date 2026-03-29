(ns xia.autonomous
  "Helpers for autonomous scheduled execution."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.prompt :as prompt]))

(def ^:private default-max-iterations 6)
(def ^:private default-control-field-chars 280)
(def ^:private control-marker "AUTONOMOUS_STATUS_JSON:")

(defn context
  []
  prompt/*interaction-context*)

(defn autonomous-run?
  ([] (autonomous-run? (context)))
  ([ctx]
   (true? (:autonomous-run? ctx))))

(defn trusted?
  ([] (trusted? (context)))
  ([ctx]
   (and (autonomous-run? ctx)
        (true? (:approval-bypass? ctx)))))

(defn audit!
  ([event]
   (audit! (context) event))
  ([ctx event]
   (when-let [audit-log (:audit-log ctx)]
     (swap! audit-log conj
            (merge {:at (str (java.time.Instant/now))}
                   event)))))

(defn- enabled-by-default?
  [value]
  (not (false? value)))

(defn oauth-account-autonomous-approved?
  [account]
  (when account
    (enabled-by-default? (:oauth.account/autonomous-approved? account))))

(defn site-autonomous-approved?
  [site]
  (when site
    (enabled-by-default? (:site-cred/autonomous-approved? site))))

(defn service-autonomous-approved?
  [service]
  (when service
    (enabled-by-default? (:service/autonomous-approved? service))))

(defn oauth-account-approved?
  [account-id]
  (when account-id
    (oauth-account-autonomous-approved? (db/get-oauth-account account-id))))

(defn site-approved?
  [site-id]
  (when site-id
    (site-autonomous-approved? (db/get-site-cred site-id))))

(defn service-approved?
  [service-id]
  (when-let [service (db/get-service service-id)]
    (and (service-autonomous-approved? service)
         (if (= :oauth-account (:service/auth-type service))
           (oauth-account-approved? (:service/oauth-account service))
           true))))

(defn scope-available?
  [scope]
  (case scope
    :service (boolean (some #(service-approved? (:service/id %))
                            (db/list-services)))
    :site    (boolean (some #(site-approved? (:site-cred/id %))
                            (db/list-site-creds)))
    false))

(defn max-iterations
  []
  (cfg/positive-long :autonomous/max-iterations
                     default-max-iterations))

(defn control-marker-text
  []
  control-marker)

(defn- truncate-field
  [value]
  (let [text (some-> value str str/trim)
        limit (long default-control-field-chars)]
    (when (seq text)
      (if (> (long (count text)) limit)
        (str (subs text 0 (max 1 (dec limit))) "…")
        text))))

(defn controller-system-message
  []
  {:role "system"
   :content
   (str "You are running inside Xia's autonomous schedule controller.\n"
        "For each iteration, follow this loop:\n"
        "1. Understand the current goal and state.\n"
        "2. Update the plan.\n"
        "3. Act using tools when useful.\n"
        "4. Observe the results already present in tool outputs and conversation history.\n"
        "5. Update the plan again.\n"
        "6. End the iteration by deciding whether to continue or complete.\n\n"
        "Rules:\n"
        "- Do not ask the user questions during a scheduled autonomous run.\n"
        "- If progress now depends on missing approval, missing credentials, or waiting for later external change, describe the blocker and mark the run complete.\n"
        "- Prefer bounded progress in each iteration over aimless repetition.\n"
        "- Reuse observations already present in the session instead of repeating the same tool calls.\n\n"
        "At the very end of every response, append the literal marker "
        control-marker
        " followed by one valid JSON object with this exact shape:\n"
        "{\"status\":\"continue|complete\",\"summary\":\"...\",\"next_step\":\"...\",\"reason\":\"...\",\"goal_complete\":true|false}\n"
        "- summary: short factual summary of what changed this iteration.\n"
        "- next_step: short concrete next action when status=continue, otherwise empty.\n"
        "- reason: why you are continuing or completing.\n"
        "- goal_complete: true only when the scheduled goal is fully satisfied.\n"
        "- Return JSON only after the marker, with no markdown fencing.")})

(defn controller-state-message
  [{:keys [goal iteration max-iterations previous-summary previous-next-step previous-reason]}]
  {:role "system"
   :content
   (str "Autonomous schedule run.\n\n"
        "Goal:\n"
        (or (some-> goal str str/trim) "")
        "\n\n"
        "Current iteration: " iteration " of " max-iterations ".\n"
        "Observe the current session history and tool results before deciding the next step.\n"
        (when (or (seq (some-> previous-summary str str/trim))
                  (seq (some-> previous-next-step str str/trim))
                  (seq (some-> previous-reason str str/trim)))
          (str "\nState from the previous iteration:\n"
               (when-let [summary (truncate-field previous-summary)]
                 (str "- Summary: " summary "\n"))
               (when-let [next-step (truncate-field previous-next-step)]
                 (str "- Next step: " next-step "\n"))
               (when-let [reason (truncate-field previous-reason)]
                 (str "- Reason: " reason "\n"))))
        "\nMake real progress now. If another iteration is needed, stop after updating the state for the next iteration.")})

(defn- parse-control-json
  [tail]
  (let [trimmed (some-> tail str str/trim)]
    (when (seq trimmed)
      (let [json-text (if-let [[_ body] (re-matches #"(?s)```(?:json)?\s*(\{.*\})\s*```" trimmed)]
                        body
                        trimmed)]
        (json/read-json json-text)))))

(defn parse-controller-response
  [response]
  (let [text  (or response "")
        idx   (str/index-of text control-marker)
        body  (when idx (subs text (+ idx (count control-marker))))
        head  (some-> (if idx (subs text 0 idx) text) str/trim)
        parsed (when body
                 (try
                   (parse-control-json body)
                   (catch Exception _
                     nil)))
        status-raw (some-> (or (get parsed "status")
                               (:status parsed))
                           str
                           str/trim
                           str/lower-case)
        status     (if (= "continue" status-raw) :continue :complete)
        summary    (or (truncate-field (or (get parsed "summary")
                                           (:summary parsed)))
                       (truncate-field head))
        next-step  (truncate-field (or (get parsed "next_step")
                                       (:next_step parsed)
                                       (get parsed "next-step")
                                       (:next-step parsed)))
        reason     (truncate-field (or (get parsed "reason")
                                       (:reason parsed)))
        goal-complete? (true? (or (get parsed "goal_complete")
                                  (:goal_complete parsed)
                                  (get parsed "goal-complete")
                                  (:goal-complete parsed)))]
    {:assistant-text head
     :control       (when parsed
                      {:status         status
                       :summary        summary
                       :next-step      next-step
                       :reason         reason
                       :goal-complete? goal-complete?})}))
