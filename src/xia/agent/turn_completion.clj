(ns xia.agent.turn-completion
  "Completion handlers for autonomous agent turn outcomes."
  (:require [clojure.string :as str]
            [xia.agent.loop-guard :as loop-guard]
            [xia.agent.recorder :as recorder]
            [xia.agent.task-finalization :as task-finalization]
            [xia.agent.task-runtime :as task-runtime]
            [xia.agent.tools :as agent-tools]
            [xia.autonomous :as autonomous]
            [xia.goal :as goal]
            [xia.limits :as limits]
            [xia.prompt :as prompt]
            [xia.policy :as task-policy]
            [xia.working-memory :as wm])
  (:import [java.util Date]))

(defn- truncate-summary
  [value max-len]
  (agent-tools/truncate-summary value max-len))

(defn- turn-budget-next-step
  [parsed autonomy-state]
  (or (some-> parsed :control :next-step str str/trim not-empty)
      (some-> parsed :intent :plan-step str str/trim not-empty)
      (some-> autonomy-state autonomous/current-frame :next-step str str/trim not-empty)))

(defn- llm-budget-title
  [budget-status]
  (str (case (:scope budget-status)
         :task "Task"
         :turn "Turn"
         :session "Session"
         :schedule "Schedule"
         :schedule-run "Schedule run"
         :org "Organization"
         "Usage")
       " limit exhausted: "))

(defn- llm-budget-note
  [budget-status parsed autonomy-state & {:keys [before-tools?]}]
  (case (:scope budget-status)
    :task
    (str "Note: I paused this task after reaching the "
         (limits/budget-summary budget-status)
         "."
         (when before-tools?
           " I did not execute the next requested tool step.")
         (when-let [next-step (turn-budget-next-step parsed autonomy-state)]
           (str " Suggested next step: " next-step)))

    :turn
    (str "Note: I stopped this turn after reaching the "
         (limits/budget-summary budget-status)
         "."
         (when before-tools?
           " I did not execute the next requested tool step.")
         (when-let [next-step (turn-budget-next-step parsed autonomy-state)]
           (str " Suggested next step: " next-step))
         " Reply to continue from the current agenda.")

    (str "Note: I stopped this turn after reaching the "
         (limits/budget-summary budget-status)
         "."
         (when before-tools?
           " I did not execute the next requested tool step.")
         (when-let [next-step (turn-budget-next-step parsed autonomy-state)]
           (str " Suggested next step: " next-step)))))

(defn- append-assistant-note
  [text note]
  (let [text* (some-> text str str/trim)
        note* (some-> note str str/trim)]
    (cond
      (str/blank? note*) (or text* "")
      (str/blank? text*) note*
      :else (str text* "\n\n" note*))))

(defn- iteration-limit-note
  [max-iterations control]
  (str "Note: I stopped after reaching the autonomous iteration limit for this turn ("
       max-iterations
       ")."
       (when-let [next-step (some-> (:next-step control) str str/trim not-empty)]
         (str " Suggested next step: " next-step))
       " Reply to continue from the current agenda."))

(defn- clear-autonomy-state-on-terminal?
  [parsed]
  (let [control (:control parsed)]
    (and (= :parsed (:control-status parsed))
         (= :clear (:stack-action control)))))

(defn- record-persistent-goal-judge!
  [session-id task-id attrs]
  (when (goal/current-goal session-id)
    (goal/judge-after-turn! session-id (assoc attrs :task-id task-id))))

(defn budget-pause!
  [deps {:keys [session-id task-id task-turn-id user-message local-doc-ids artifact-ids
                iteration max-iterations iteration-context response parsed control text
                fact-eids explicit-fact-eids updated-autonomy-state updated-tip
                budget-status budget-before-tools?]}]
  (let [final-text (append-assistant-note
                    text
                    (llm-budget-note budget-status
                                     parsed
                                     updated-autonomy-state
                                     :before-tools? budget-before-tools?))]
    (task-runtime/record-task-item! task-turn-id
                                    {:type :system-note
                                     :status :limit
                                     :summary (str (llm-budget-title budget-status)
                                                   (limits/budget-summary budget-status))
                                     :data {:kind "budget-exhausted"
                                            :budget-scope (some-> (:scope budget-status) name)
                                            :budget-kind (some-> (:kind budget-status) name)
                                            :llm-call-count (:llm-call-count budget-status)
                                            :total-tokens (:total-tokens budget-status)
                                            :elapsed-ms (:elapsed-ms budget-status)
                                            :llm-total-duration-ms (:llm-total-duration-ms budget-status)
                                            :before-tools? (boolean budget-before-tools?)}})
    (recorder/persist-assistant-message! session-id
                                         final-text
                                         iteration-context
                                         response
                                         local-doc-ids
                                         artifact-ids)
    (task-runtime/sync-runtime-task-turn! task-turn-id
                                          {:state :completed
                                           :summary (truncate-summary final-text 500)})
    (task-runtime/sync-runtime-task! task-id
                                     {:state :resumable
                                      :summary (truncate-summary final-text 500)
                                      :autonomy-state updated-autonomy-state})
    (record-persistent-goal-judge!
     session-id
     task-id
     {:task-state :resumable
      :control control
      :autonomy-state updated-autonomy-state
      :guardrail :budget
      :budget-status budget-status
      :summary (truncate-summary final-text 500)})
    (when-not (str/blank? text)
      ((:launch-fact-utility-review-without-budget! deps)
       session-id
       fact-eids
       user-message
       text
       :explicit-fact-eids explicit-fact-eids))
    ((:save-schedule-checkpoint! deps)
     iteration-context
     {:phase :complete
      :iteration iteration
      :summary (or (truncate-summary final-text 500)
                   (str "Stopped after reaching the "
                        (limits/budget-summary budget-status)
                        "."))
      :session-id session-id
      :status :limit-exhausted
      :budget-scope (:scope budget-status)
      :budget-kind (:kind budget-status)
      :llm-call-count (:llm-call-count budget-status)
      :total-tokens (:total-tokens budget-status)
      :elapsed-ms (:elapsed-ms budget-status)
      :llm-total-duration-ms (:llm-total-duration-ms budget-status)
      :next-step (turn-budget-next-step parsed updated-autonomy-state)
      :progress-status (some-> updated-tip :progress-status)
      :agenda (some-> updated-tip :agenda)
      :stack (some-> updated-autonomy-state :stack)})
    (prompt/status! (merge {:state :completed
                            :phase :complete
                            :message (str "Paused after reaching the "
                                          (limits/budget-summary budget-status))}
                           ((:autonomy-status-fields deps) updated-autonomy-state
                                                           iteration
                                                           max-iterations)))
    final-text))

(defn complete!
  [deps {:keys [session-id task-id task-turn-id user-message local-doc-ids artifact-ids
                iteration-context response parsed control text fact-eids explicit-fact-eids
                updated-autonomy-state]}]
  (recorder/persist-assistant-message! session-id
                                       text
                                       iteration-context
                                       response
                                       local-doc-ids
                                       artifact-ids)
  (task-runtime/sync-runtime-task-turn! task-turn-id
                                        {:state :completed
                                         :summary (truncate-summary text 500)})
  (task-runtime/sync-runtime-task! task-id
                                   {:state :completed
                                    :summary (truncate-summary text 500)
                                    :autonomy-state (when-not (clear-autonomy-state-on-terminal? parsed)
                                                      updated-autonomy-state)
                                    :finished-at (Date.)})
  (record-persistent-goal-judge!
   session-id
   task-id
   {:task-state :completed
    :control control
    :autonomy-state updated-autonomy-state
    :summary (truncate-summary text 500)})
  (when (clear-autonomy-state-on-terminal? parsed)
    (wm/clear-autonomy-state! session-id)
    (wm/snapshot! session-id))
  ((:launch-fact-utility-review-without-budget! deps)
   session-id
   fact-eids
   user-message
   text
   :explicit-fact-eids explicit-fact-eids)
  (prompt/status! {:state :completed
                   :phase :complete
                   :message "Ready"})
  (task-finalization/launch-skill-learning! task-id)
  text)

(defn iteration-limit!
  [deps {:keys [session-id task-id task-turn-id user-message local-doc-ids artifact-ids
                iteration max-iterations iteration-context response control text fact-eids
                explicit-fact-eids updated-autonomy-state updated-tip]}]
  (let [final-text (append-assistant-note text
                                          (iteration-limit-note max-iterations
                                                                control))]
    (prompt/policy-decision!
     (task-policy/autonomy-iteration-limit-policy
      iteration
      max-iterations))
    (recorder/persist-assistant-message! session-id
                                         final-text
                                         iteration-context
                                         response
                                         local-doc-ids
                                         artifact-ids)
    (task-runtime/sync-runtime-task-turn! task-turn-id
                                          {:state :completed
                                           :summary (truncate-summary final-text 500)})
    (task-runtime/sync-runtime-task! task-id
                                     {:state :resumable
                                      :summary (truncate-summary final-text 500)
                                      :autonomy-state updated-autonomy-state})
    (record-persistent-goal-judge!
     session-id
     task-id
     {:task-state :resumable
      :control control
      :autonomy-state updated-autonomy-state
      :guardrail :iteration-limit
      :summary (truncate-summary final-text 500)})
    ((:launch-fact-utility-review-without-budget! deps)
     session-id
     fact-eids
     user-message
     text
     :explicit-fact-eids explicit-fact-eids)
    ((:save-schedule-checkpoint! deps)
     iteration-context
     {:phase :complete
      :iteration iteration
      :summary (or (truncate-summary final-text 500)
                   "Stopped after reaching the autonomous iteration limit for this turn.")
      :session-id session-id
      :status :iteration-limit
      :next-step (:next-step control)
      :progress-status (some-> updated-tip :progress-status)
      :agenda (some-> updated-tip :agenda)
      :stack (some-> updated-autonomy-state :stack)})
    (prompt/status! (merge {:state :completed
                            :phase :complete
                            :message (str "Paused after reaching iteration limit ("
                                          max-iterations
                                          ")")}
                           ((:autonomy-status-fields deps) updated-autonomy-state
                                                           iteration
                                                           max-iterations)))
    final-text))

(defn continue!
  [deps {:keys [session-id channel iteration max-iterations iteration-context response control
                text tool-activity loop-state refresh-needed? working-memory-message
                wm-query-fingerprint system-prompt-cache-entry updated-autonomy-state
                updated-tip]}]
  (let [next-loop-state (loop-guard/update-iteration-loop-state
                         loop-state
                         (loop-guard/iteration-signature updated-autonomy-state
                                                         control
                                                         tool-activity))
        next-wm-message (or working-memory-message
                            (autonomous/retrieval-message updated-autonomy-state))
        next-wm-query-fingerprint (loop-guard/wm-query-signature next-wm-message)
        next-refresh-working-memory? (or refresh-needed?
                                         (and next-wm-query-fingerprint
                                              (not= wm-query-fingerprint
                                                    next-wm-query-fingerprint)))]
    (recorder/persist-assistant-message! session-id
                                         text
                                         iteration-context
                                         response
                                         nil
                                         nil)
    (when-not (str/blank? text)
      (prompt/assistant-message! {:text text
                                  :iteration iteration
                                  :max-iterations max-iterations
                                  :status :continue
                                  :progress-status (some-> updated-tip :progress-status)
                                  :agenda (some-> updated-tip :agenda)
                                  :stack (some-> updated-autonomy-state :stack)}))
    (loop-guard/throw-if-identical-iteration-loop! session-id
                                                   channel
                                                   iteration
                                                   max-iterations
                                                   next-loop-state
                                                   updated-autonomy-state
                                                   control)
    ((:report-autonomy-status! deps)
     :updating
     updated-autonomy-state
     iteration
     max-iterations
     :stack-action (:stack-action control))
    ((:save-schedule-checkpoint! deps)
     iteration-context
     {:phase :updating
      :iteration iteration
      :summary (or (:next-step control)
                   "Updating the autonomous plan for the next iteration.")
      :session-id session-id
      :status :continue
      :progress-status (some-> updated-tip :progress-status)
      :agenda (some-> updated-tip :agenda)
      :stack (some-> updated-autonomy-state :stack)})
    {:iteration (inc iteration)
     :loop-state next-loop-state
     :refresh-working-memory? next-refresh-working-memory?
     :system-prompt-cache-entry system-prompt-cache-entry
     :wm-message next-wm-message
     :wm-query-fingerprint next-wm-query-fingerprint}))
