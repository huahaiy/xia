(ns xia.autonomous
  "Helpers for autonomous scheduled execution."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.prompt :as prompt]))

(def ^:private default-max-iterations 6)
(def ^:private default-control-field-chars 280)
(def ^:private max-agenda-items 8)
(def ^:private max-agenda-item-chars 160)
(def ^:private max-stack-depth 8)
(def ^:private intent-marker "ACTION_INTENT_JSON:")
(def ^:private control-marker "AUTONOMOUS_STATUS_JSON:")

(defn context
  []
  prompt/*interaction-context*)

(declare controller-state-message current-frame)

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

(defn intent-marker-text
  []
  intent-marker)

(defn- truncate-field
  [value]
  (let [text (some-> value str str/trim)
        limit (long default-control-field-chars)]
    (when (seq text)
      (if (> (long (count text)) limit)
        (str (subs text 0 (max 1 (dec limit))) "…")
        text))))

(defn- truncate-agenda-item
  [value]
  (let [text (some-> value str str/trim)
        limit (long max-agenda-item-chars)]
    (when (seq text)
      (if (> (long (count text)) limit)
        (str (subs text 0 (max 1 (dec limit))) "…")
        text))))

(defn- truncate-tool-name
  [value]
  (some-> value truncate-agenda-item))

(def ^:private progress-status-aliases
  {"not_started" :not-started
   "not-started" :not-started
   "pending"     :pending
   "in_progress" :in-progress
   "in-progress" :in-progress
   "active"      :in-progress
   "paused"      :paused
   "resumable"   :resumable
   "resumeable"  :resumable
   "diverged"    :diverged
   "blocked"     :blocked
   "complete"    :complete
   "completed"   :complete
   "done"        :complete})

(def ^:private agenda-status-aliases
  {"pending"     :pending
   "todo"        :pending
   "not_started" :pending
   "not-started" :pending
   "in_progress" :in-progress
   "in-progress" :in-progress
   "active"      :in-progress
   "paused"      :paused
   "resumable"   :resumable
   "resumeable"  :resumable
   "diverged"    :diverged
   "blocked"     :blocked
   "complete"    :completed
   "completed"   :completed
   "done"        :completed
   "skipped"     :skipped})

(def ^:private stack-action-aliases
  {"stay"    :stay
   "update"  :stay
   "noop"    :stay
   "none"    :stay
   "push"    :push
   "enter"   :push
   "pop"     :pop
   "return"  :pop
   "replace" :replace
   "switch"  :replace
   "clear"   :clear
   "reset"   :clear})

(defn- normalize-progress-status
  [value]
  (some-> (cond
            (keyword? value) (name value)
            :else value)
          str
          str/trim
          str/lower-case
          progress-status-aliases))

(defn- normalize-agenda-status
  [value]
  (or (some-> (cond
                (keyword? value) (name value)
                :else value)
              str
              str/trim
              str/lower-case
              agenda-status-aliases)
      :pending))

(defn- normalize-agenda-item
  [entry]
  (cond
    (map? entry)
    (let [item (truncate-agenda-item (or (get entry "item")
                                         (:item entry)
                                         (get entry "step")
                                         (:step entry)
                                         (get entry "title")
                                         (:title entry)))]
      (when item
        {:item item
         :status (normalize-agenda-status (or (get entry "status")
                                              (:status entry)))}))

    (string? entry)
    (when-let [item (truncate-agenda-item entry)]
      {:item item
       :status :pending})

    :else
    nil))

(defn- normalize-agenda
  [value]
  (when (sequential? value)
    (->> value
         (keep normalize-agenda-item)
         (take max-agenda-items)
         vec
         not-empty)))

(defn- normalize-stack-action
  [value]
  (or (some-> (cond
                (keyword? value) (name value)
                :else value)
              str
              str/trim
              str/lower-case
              stack-action-aliases)
      :stay))

(defn- agenda-status-label
  [status]
  (some-> status name (str/replace "-" "_")))

(defn- progress-status-label
  [status]
  (some-> status name (str/replace "-" "_")))

(defn- normalize-intent
  [intent]
  (when (map? intent)
    (let [focus            (truncate-agenda-item (or (get intent "focus")
                                                     (:focus intent)
                                                     (get intent "current_focus")
                                                     (:current_focus intent)
                                                     (get intent "current-focus")
                                                     (:current-focus intent)))
          agenda-item      (truncate-agenda-item (or (get intent "agenda_item")
                                                     (:agenda_item intent)
                                                     (get intent "agenda-item")
                                                     (:agenda-item intent)
                                                     (get intent "item")
                                                     (:item intent)))
          plan-step        (truncate-field (or (get intent "plan_step")
                                               (:plan_step intent)
                                               (get intent "plan-step")
                                               (:plan-step intent)
                                               (get intent "step")
                                               (:step intent)))
          why              (truncate-field (or (get intent "why")
                                               (:why intent)
                                               (get intent "reason")
                                               (:reason intent)))
          tool-name        (truncate-tool-name (or (get intent "tool")
                                                   (:tool intent)
                                                   (get intent "tool_name")
                                                   (:tool_name intent)
                                                   (get intent "tool-name")
                                                   (:tool-name intent)))
          tool-args-summary (truncate-field (or (get intent "tool_args_summary")
                                                (:tool_args_summary intent)
                                                (get intent "tool-args-summary")
                                                (:tool-args-summary intent)
                                                (get intent "arguments")
                                                (:arguments intent)))]
      (when (or focus agenda-item plan-step why tool-name tool-args-summary)
        {:focus focus
         :agenda-item agenda-item
         :plan-step plan-step
         :why why
         :tool-name tool-name
         :tool-args-summary tool-args-summary}))))

(defn- agenda-lines
  [agenda]
  (->> agenda
       (keep (fn [{:keys [item status]}]
               (when item
                 (str "  - [" (or (agenda-status-label status) "pending") "] " item))))
       vec
       not-empty))

(defn- stack-line
  [{:keys [title progress-status next-step]}]
  (let [title*      (truncate-agenda-item title)
        status*     (progress-status-label progress-status)
        next-step*  (truncate-field next-step)]
    (when title*
      (str "  - "
           "[" (or status* "pending") "] "
           title*
           (when next-step*
             (str " -> " next-step*))))))

(defn- stack-lines
  [stack]
  (->> stack
       (keep stack-line)
       vec
       not-empty))

(defn- derive-progress-status
  [status goal-complete? agenda]
  (cond
    goal-complete? :complete
    (some #(= :diverged (:status %)) agenda) :diverged
    (some #(= :resumable (:status %)) agenda) :resumable
    (some #(= :paused (:status %)) agenda) :paused
    (some #(= :blocked (:status %)) agenda) :blocked
    (seq agenda)
    (if (some #(contains? #{:in-progress :completed :skipped} (:status %)) agenda)
      :in-progress
      :pending)
    (= status :continue) :in-progress
    :else :complete))

(defn- default-frame-title
  [goal]
  (or (truncate-agenda-item goal)
      "Current task"))

(defn- normalize-frame
  [frame default-title]
  (let [agenda           (normalize-agenda (or (:agenda frame)
                                               (get frame "agenda")))
        status           (or (normalize-progress-status (or (:progress-status frame)
                                                            (get frame "progress_status")
                                                            (get frame "progress-status")))
                             (normalize-progress-status (or (:status frame)
                                                            (get frame "status")))
                             (derive-progress-status :continue false agenda)
                             :pending)
        title            (or (truncate-agenda-item (or (:title frame)
                                                       (get frame "title")))
                             (truncate-agenda-item (or (:current-focus frame)
                                                       (:current_focus frame)
                                                       (get frame "current_focus")
                                                       (get frame "current-focus")))
                             default-title)]
    {:title           title
     :summary         (truncate-field (or (:summary frame)
                                          (get frame "summary")))
     :next-step       (truncate-field (or (:next-step frame)
                                          (:next_step frame)
                                          (get frame "next_step")
                                          (get frame "next-step")))
     :reason          (truncate-field (or (:reason frame)
                                          (get frame "reason")))
     :progress-status status
     :agenda          agenda}))

(defn- normalize-stack
  [goal stack]
  (let [default-title (default-frame-title goal)
        stack*        (->> (or stack [])
                           (keep #(when (map? %)
                                    (normalize-frame % default-title)))
                           (take-last max-stack-depth)
                           vec)]
    (if (seq stack*)
      stack*
      [(normalize-frame {:title default-title
                         :progress-status :pending}
                        default-title)])))

(defn initial-state
  [goal]
  (let [goal* (truncate-field goal)]
    {:goal  goal*
     :stack (normalize-stack goal* nil)}))

(defn normalize-state
  [state]
  (let [raw-stack (or (:stack state)
                      (get state "stack"))
        goal*     (truncate-field (or (:goal state)
                                      (get state "goal")
                                      (some-> raw-stack peek :title)
                                      (some-> raw-stack peek (get "title"))))
        stack*    (normalize-stack goal* raw-stack)]
    {:goal  goal*
     :stack stack*}))

(defn prepare-turn-state
  [state user-message]
  (let [state*           (when (map? state)
                           (normalize-state state))
        current-status   (some-> state* current-frame :progress-status)
        resumable-state? (contains? #{:pending
                                      :in-progress
                                      :paused
                                      :resumable
                                      :diverged
                                      :blocked}
                                    current-status)]
    (if resumable-state?
      state*
      (initial-state user-message))))

(defn current-frame
  [state]
  (peek (:stack (if (map? state)
                  (normalize-state state)
                  (initial-state nil)))))

(defn apply-control
  [state control]
  (let [goal          (truncate-field (or (:goal state)
                                          (:current-focus control)
                                          (:summary control)))
        stack         (normalize-stack goal (:stack state))
        current-tip   (peek stack)
        action        (normalize-stack-action (:stack-action control))
        tip-title     (or (truncate-agenda-item (:current-focus control))
                          (:title current-tip)
                          (default-frame-title goal))
        next-tip      (normalize-frame (assoc control :title tip-title)
                                       tip-title)
        replace-top   (fn [frames frame]
                        (conj (vec (butlast frames)) frame))
        next-stack    (case action
                        :push
                        (->> (conj stack next-tip)
                             (take-last max-stack-depth)
                             vec)

                        :pop
                        (if (> (count stack) 1)
                          (let [parent-stack (vec (butlast stack))
                                parent-tip   (peek parent-stack)
                                parent-title (or (truncate-agenda-item (:current-focus control))
                                                 (:title parent-tip)
                                                 (default-frame-title goal))
                                parent-frame (normalize-frame (assoc control :title parent-title)
                                                              parent-title)]
                            (replace-top parent-stack parent-frame))
                          [(normalize-frame (assoc control :title tip-title)
                                            tip-title)])

                        :replace
                        (replace-top stack next-tip)

                        :clear
                        []

                        :stay
                        (replace-top stack next-tip))]
    {:goal  goal
     :stack next-stack}))

(defn working-memory-message
  [{:keys [goal stack] :as state} iteration max-iterations]
  (:content (controller-state-message
             {:goal goal
              :iteration iteration
              :max-iterations max-iterations
              :stack stack
              :previous-summary (some-> (peek stack) :summary)
              :previous-next-step (some-> (peek stack) :next-step)
              :previous-reason (some-> (peek stack) :reason)
              :previous-progress-status (some-> (peek stack) :progress-status)
              :previous-agenda (some-> (peek stack) :agenda)})))

(defn status-line
  [phase {:keys [stack] :as state} iteration max-iterations & {:keys [stack-action]}]
  (let [stack*        (normalize-stack (:goal state) stack)
        tip           (peek stack*)
        title         (or (:title tip) (default-frame-title (:goal state)))
        progress      (progress-status-label (:progress-status tip))
        next-step     (truncate-field (:next-step tip))
        depth         (count stack*)
        prefix        (case phase
                        :understanding "Understanding"
                        :observing     "Observed"
                        :updating      (case (normalize-stack-action stack-action)
                                         :push "Entering"
                                         :pop "Returning to"
                                         :replace "Switching to"
                                         "Updating")
                        "Working")]
    (str "Iteration "
         iteration
         "/"
         max-iterations
         ": "
         prefix
         " "
         title
         (when progress
           (str " [" progress "]"))
         (when (> depth 1)
           (str " (stack " depth ")"))
         (when (and (= phase :updating) next-step)
           (str " -> " next-step)))))

(defn intent-status-line
  [{:keys [focus agenda-item plan-step tool-name tool-args-summary]}]
  (str "Intent: "
       (or plan-step
           agenda-item
           focus
           "Proceed with the current tip")
       (when tool-name
         (str " via " tool-name))
       (when tool-args-summary
         (str " (" tool-args-summary ")"))))

(defn controller-system-message
  []
  (let [ctx            (context)
        autonomous?    (autonomous-run? ctx)
        branch-worker? (true? (:branch-worker? ctx))
        direct-user?   (not (or autonomous? branch-worker? (= :branch (:channel ctx))))]
    {:role "system"
     :content
     (str "You are running inside Xia's iterative control loop.\n"
        "For each iteration, follow this loop:\n"
        "1. Understand the current goal and state.\n"
        "2. Update the plan.\n"
        "3. Act using tools when useful.\n"
        "4. Observe the results already present in tool outputs and conversation history.\n"
        "5. Update the plan again.\n"
        "6. End the iteration by deciding whether to continue or complete.\n\n"
        "Rules:\n"
        (if direct-user?
          "- You are interacting with the user directly. If you need input from them, ask one focused question and mark the iteration complete.\n"
          "- Do not ask the user questions in this execution context.\n")
        "- If progress now depends on missing approval, missing credentials, or waiting for later external change, describe the blocker and mark the run complete.\n"
        "- Prefer bounded progress in each iteration over aimless repetition.\n"
        "- Always work on the current stack tip. Do not choose among stack frames.\n"
        "- Treat new user input, tool results, and observations as inputs about the current tip. If they imply subordinate work, interruption, return-to-parent, replacement, or discard, report that with stack_action at the end of the iteration.\n"
        "- Use stay when continuing the current tip. Use push to suspend the current tip and enter a child task. Use pop when the current child is done and control should return to its parent. Use replace when the current tip should be superseded. Use clear only when prior stack state should be discarded.\n"
        "- Reuse observations already present in the session instead of repeating the same tool calls.\n\n"
        "At the start of the first assistant response in every iteration, before any explanation or tool call, prepend the literal marker "
        intent-marker
        " followed by one valid JSON object with this exact shape:\n"
        "{\"focus\":\"...\",\"agenda_item\":\"...\",\"plan_step\":\"...\",\"why\":\"...\",\"tool\":\"optional-tool-name\",\"tool_args_summary\":\"optional short arguments summary\"}\n"
        "- focus: the current stack tip you are acting on right now.\n"
        "- agenda_item: the agenda item for this step when there is one.\n"
        "- plan_step: the concrete next action you are about to take in this iteration.\n"
        "- why: why this action helps the current tip.\n"
        "- tool: the tool you expect to call next, or empty when none.\n"
        "- tool_args_summary: short human-readable summary of the expected tool arguments.\n"
        "- Emit this intent object even when you will immediately call tools.\n\n"
        "At the very end of every response, append the literal marker "
        control-marker
        " followed by one valid JSON object with this exact shape:\n"
        "{\"status\":\"continue|complete\",\"summary\":\"...\",\"next_step\":\"...\",\"reason\":\"...\",\"goal_complete\":true|false,\"current_focus\":\"...\",\"stack_action\":\"stay|push|pop|replace|clear\",\"progress_status\":\"not_started|pending|in_progress|paused|resumable|diverged|blocked|complete\",\"agenda\":[{\"item\":\"...\",\"status\":\"pending|in_progress|paused|resumable|diverged|completed|blocked|skipped\"}]}\n"
        "- summary: short factual summary of what changed this iteration.\n"
        "- next_step: short concrete next action when status=continue, otherwise empty.\n"
        "- reason: why you are continuing or completing.\n"
        "- goal_complete: true only when the current goal is fully satisfied.\n"
        "- current_focus: title of the current stack tip after this iteration.\n"
        "- stack_action: stay to keep working the current frame, push to enter a subroutine, pop to return to the parent frame, replace to switch the current frame, clear to empty the stack.\n"
        "- progress_status: overall status of the current stack tip.\n"
        "- agenda: ordered short checklist for the current stack tip only, not the full stack.\n"
        "- Use paused when work should stop for now. Use resumable when it is paused but has a clear restart path. Use diverged when the work has meaningfully branched away from the original plan.\n"
        (when direct-user?
          "- For direct user-facing turns, use continue only when another internal iteration can make more progress without waiting on the user.\n")
        "- Return JSON only after the marker, with no markdown fencing.")}))

(defn controller-state-message
  [{:keys [goal iteration max-iterations stack previous-summary previous-next-step
           previous-reason previous-progress-status previous-agenda incoming-message]}]
  (let [stack* (when (seq stack)
                 (normalize-stack goal stack))
        tip    (peek stack*)]
    {:role "system"
     :content
     (str "Autonomous agent control state.\n\n"
          "Goal:\n"
          (or (some-> goal str str/trim) "")
          "\n\n"
          "Current iteration: " iteration " of " max-iterations ".\n"
          "Observe the current session history and tool results before deciding the next step.\n"
          "Start the first assistant response in this iteration with "
          intent-marker
          "{...} before any explanation or tool call.\n"
          (when-let [input (truncate-field incoming-message)]
            (str "\nNew input for this turn:\n"
                 input
                 "\n"
                 "Interpret this input relative to the current stack tip. Work on that tip in this iteration. If the input means the tip should be suspended, returned from, replaced, or discarded, report that with stack_action at the end of the iteration.\n"))
          (if (seq stack*)
            (str "\nCurrent execution stack (bottom -> top):\n"
                 (str/join "\n" (stack-lines stack*))
                 "\n"
                 (when-let [summary (some-> tip :summary truncate-field)]
                   (str "- Tip summary: " summary "\n"))
                 (when-let [reason (some-> tip :reason truncate-field)]
                   (str "- Tip reason: " reason "\n"))
                 (when-let [lines (some-> tip :agenda agenda-lines)]
                   (str "- Tip agenda:\n"
                        (str/join "\n" lines)
                        "\n")))
            (when (or (seq (some-> previous-summary str str/trim))
                      (seq (some-> previous-next-step str str/trim))
                      (seq (some-> previous-reason str str/trim))
                      previous-progress-status
                      (seq previous-agenda))
              (str "\nState from the previous iteration:\n"
                   (when-let [summary (truncate-field previous-summary)]
                     (str "- Summary: " summary "\n"))
                   (when-let [next-step (truncate-field previous-next-step)]
                     (str "- Next step: " next-step "\n"))
                   (when-let [reason (truncate-field previous-reason)]
                     (str "- Reason: " reason "\n"))
                   (when-let [progress-status (progress-status-label previous-progress-status)]
                     (str "- Progress status: " progress-status "\n"))
                   (when-let [lines (agenda-lines previous-agenda)]
                     (str "- Agenda:\n"
                          (str/join "\n" lines)
                          "\n")))))
          "\nMake real progress now. Use stack_action=push when entering a subroutine and stack_action=pop when returning to a parent frame.")}))

(defn- extract-json-object
  [text]
  (let [trimmed (some-> text str str/triml)]
    (when (seq trimmed)
      (let [[body fenced?]
            (if-let [[_ inner] (re-matches #"(?s)\A```(?:json)?\s*(.*)\z" trimmed)]
              [inner true]
              [trimmed false])]
        (when (and (seq body) (= \{ (.charAt ^String body 0)))
          (loop [idx 0
                 depth 0
                 in-string? false
                 escape? false]
            (when (< idx (count body))
              (let [ch (.charAt ^String body idx)]
                (cond
                  in-string?
                  (cond
                    escape?
                    (recur (inc idx) depth true false)

                    (= ch \\)
                    (recur (inc idx) depth true true)

                    (= ch \")
                    (recur (inc idx) depth false false)

                    :else
                    (recur (inc idx) depth true false))

                  (= ch \")
                  (recur (inc idx) depth true false)

                  (= ch \{)
                  (recur (inc idx) (inc depth) false false)

                  (= ch \})
                  (let [depth* (dec depth)]
                    (if (zero? depth*)
                      (let [json-text (subs body 0 (inc idx))
                            rest      (subs body (inc idx))
                            rest*     (if fenced?
                                        (str/replace-first rest #"^\s*```" "")
                                        rest)]
                        {:json-text json-text
                         :rest rest*})
                      (recur (inc idx) depth* false false)))

                  :else
                  (recur (inc idx) depth false false))))))))))

(defn- parse-json-after-marker
  [text marker]
  (when-let [idx (str/index-of (or text "") marker)]
    (let [before (subs text 0 idx)
          after  (subs text (+ idx (count marker)))]
      (when-let [{:keys [json-text rest]} (extract-json-object after)]
        (try
          {:before before
           :after rest
           :parsed (json/read-json json-text)}
          (catch Exception _
            nil))))))

(defn parse-controller-response
  [response]
  (let [text            (or response "")
        parsed-intent   (parse-json-after-marker text intent-marker)
        text-without-intent (str (or (:before parsed-intent) text)
                                 (or (:after parsed-intent) ""))
        parsed-control  (parse-json-after-marker text-without-intent control-marker)
        head            (some-> (if parsed-control
                                  (str (:before parsed-control)
                                       (:after parsed-control))
                                  text-without-intent)
                                str/trim)
        parsed          (:parsed parsed-control)
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
                                  (:goal-complete parsed)))
        current-focus (truncate-agenda-item
                        (or (get parsed "current_focus")
                            (:current_focus parsed)
                            (get parsed "current-focus")
                            (:current-focus parsed)))
        stack-action   (normalize-stack-action
                         (or (get parsed "stack_action")
                             (:stack_action parsed)
                             (get parsed "stack-action")
                             (:stack-action parsed)))
        agenda     (normalize-agenda (or (get parsed "agenda")
                                         (:agenda parsed)))
        progress-status (or (normalize-progress-status
                              (or (get parsed "progress_status")
                                  (:progress_status parsed)
                                  (get parsed "progress-status")
                                  (:progress-status parsed)))
                            (derive-progress-status status
                                                    goal-complete?
                                                    agenda))]
    {:assistant-text head
     :intent        (some-> parsed-intent :parsed normalize-intent)
     :control       (when parsed
                      {:status         status
                       :summary        summary
                       :next-step      next-step
                       :reason         reason
                       :goal-complete? goal-complete?
                       :current-focus  current-focus
                       :stack-action   stack-action
                       :progress-status progress-status
                       :agenda         agenda})}))
