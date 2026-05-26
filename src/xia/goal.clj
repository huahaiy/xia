(ns xia.goal
  "Persistent user-facing goals for a session."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.working-memory :as wm]))

(def ^:private meta-key :persistent-goal)
(def ^:private default-max-turns 20)

(defn- now []
  (java.util.Date.))

(defn- compact
  [value]
  (cond
    (map? value)
    (let [entries (keep (fn [[k v]]
                          (let [v* (compact v)]
                            (when (some? v*)
                              [k v*])))
                        value)]
      (when (seq entries)
        (into (empty value) entries)))

    (vector? value)
    (let [items (into [] (comp (map compact) (filter some?)) value)]
      (when (seq items)
        items))

    (sequential? value)
    (let [items (filter some? (map compact value))]
      (when (seq items)
        (into (empty value) items)))

    :else
    value))

(defn- deep-merge
  [& maps]
  (letfn [(merge-entry [left right]
            (if (and (map? left) (map? right))
              (merge-with merge-entry left right)
              right))]
    (reduce (fn [acc value]
              (if (map? value)
                (merge-entry acc value)
                acc))
            {}
            maps)))

(defn- session-meta
  [session-id]
  (or (db/session-external-meta session-id) {}))

(defn- goal-task-id
  [goal]
  (let [value (:last-task-id goal)]
    (cond
      (uuid? value) value
      (string? value) (try
                        (java.util.UUID/fromString value)
                        (catch IllegalArgumentException _ nil))
      :else nil)))

(defn task-snapshot
  [goal]
  (when goal
    (select-keys goal
                 [:id :text :status :source :turn-count :max-turns
                  :contract
                  :last-judge-status :last-judge-reason :last-used-at
                  :last-task-id :last-task-state :last-summary :next-step
                  :last-guardrail :last-budget-status :created-at :updated-at
                  :last-judged-at :paused-at :resumed-at :completed-at :cleared-at])))

(defn- sync-task-snapshot!
  [goal]
  (when-let [task-id (goal-task-id goal)]
    (when-let [task (db/get-task task-id)]
      (db/update-task! task-id
                       {:meta (compact (assoc (or (:meta task) {})
                                              meta-key
                                              (task-snapshot goal)))}))))

(defn- clear-task-snapshot!
  [goal]
  (when-let [task-id (goal-task-id goal)]
    (when-let [task (db/get-task task-id)]
      (let [meta* (dissoc (or (:meta task) {}) meta-key)]
        (db/update-task! task-id {:meta (compact meta*)})))))

(defn- reset-execution-state!
  [session-id]
  (wm/clear-autonomy-state! session-id)
  (wm/snapshot! session-id))

(defn current-goal
  [session-id]
  (get (session-meta session-id) meta-key))

(defn active-goal
  [session-id]
  (let [goal (current-goal session-id)]
    (when (= :active (:status goal))
      goal)))

(defn active?
  [goal]
  (= :active (:status goal)))

(defn goal-contract
  "Return the explicit user-authored goal contract, with legacy goals normalized."
  [goal]
  (when goal
    (compact
     (assoc (or (:contract goal) {})
            :goal/intent (or (get-in goal [:contract :goal/intent])
                             (:text goal))))))

(defn operating-envelope-source
  "Return the goal-level source used by xia.constraints.

   Goal preferences and constraints are merged into the envelope root so they
   can participate in the same precedence rules as task constraints and user
   preferences. Goal metadata stays under :goal for inspection."
  [goal]
  (when goal
    (let [contract (goal-contract goal)]
      (compact
       (deep-merge
        (:goal/preferences contract)
        (:goal/constraints contract)
        {:goal (cond-> {:id (:id goal)
                        :intent (:goal/intent contract)
                        :status (:status goal)
                        :turn-count (long (or (:turn-count goal) 0))
                        :max-turns (:max-turns goal)}
                 (:goal/success-criteria contract)
                 (assoc :success-criteria (:goal/success-criteria contract))

                 (:goal/budget contract)
                 (assoc :budget (:goal/budget contract))

                 (:goal/resume-policy contract)
                 (assoc :resume-policy (:goal/resume-policy contract)))}
        (when-let [budget (:goal/budget contract)]
          {:limits {:goal budget}}))))))

(defn- save-goal!
  [session-id goal]
  (let [meta* (assoc (session-meta session-id) meta-key (compact goal))]
    (db/save-session-external-meta! session-id meta*)
    (sync-task-snapshot! goal)
    (current-goal session-id)))

(defn- remove-goal!
  [session-id]
  (db/save-session-external-meta! session-id (dissoc (session-meta session-id) meta-key))
  nil)

(defn- positive-long
  [value]
  (cond
    (and (integer? value) (pos? value)) (long value)
    (and (number? value) (pos? value)) (long value)
    (string? value) (try
                      (let [n (Long/parseLong (str/trim value))]
                        (when (pos? n) n))
                      (catch Exception _ nil))
    :else nil))

(defn- nonblank-string
  [value]
  (some-> value str str/trim not-empty))

(defn- invalid-goal!
  [message data]
  (throw (ex-info message
                  (merge {:type :goal/invalid
                          :status 400
                          :error message}
                         data))))

(defn- normalized-map-field
  [field value]
  (cond
    (nil? value)
    nil

    (map? value)
    (compact value)

    :else
    (invalid-goal! (str "Persistent goal " (name field) " must be a map")
                   {:field field})))

(defn- normalized-success-criteria
  [value]
  (cond
    (nil? value)
    nil

    (string? value)
    (some-> value nonblank-string vector)

    (sequential? value)
    (let [items (into [] (keep nonblank-string) value)]
      (when (seq items)
        items))

    :else
    (invalid-goal! "Persistent goal success criteria must be a string or list"
                   {:field :success-criteria})))

(defn- build-contract
  [intent {:keys [success-criteria constraints preferences budget resume-policy]}]
  (compact
   #:goal{:intent intent
          :success-criteria (normalized-success-criteria success-criteria)
          :constraints (normalized-map-field :constraints constraints)
          :preferences (normalized-map-field :preferences preferences)
          :budget (normalized-map-field :budget budget)
          :resume-policy (normalized-map-field :resume-policy resume-policy)}))

(defn set-goal!
  [session-id text & {:keys [max-turns source success-criteria constraints preferences
                             budget resume-policy]}]
  (let [text* (some-> text str str/trim not-empty)]
    (when-not text*
      (throw (ex-info "Persistent goal text is required"
                      {:type :goal/invalid
                       :status 400
                       :error "goal text is required"})))
    (let [at (now)
          contract (build-contract text*
                                   {:success-criteria success-criteria
                                    :constraints constraints
                                    :preferences preferences
                                    :budget budget
                                    :resume-policy resume-policy})
          goal {:id (str (random-uuid))
                :text text*
                :contract contract
                :status :active
                :source (or source :user)
                :turn-count 0
                :max-turns (or (positive-long max-turns) default-max-turns)
                :created-at at
                :updated-at at}]
      (when-let [previous (current-goal session-id)]
        (clear-task-snapshot! previous))
      (reset-execution-state! session-id)
      (save-goal! session-id goal))))

(defn pause-goal!
  [session-id]
  (when-let [goal (current-goal session-id)]
    (save-goal! session-id
                (assoc goal
                       :status :paused
                       :paused-at (now)
                       :updated-at (now)
                       :last-judge-status :paused
                       :last-judge-reason "Paused by user"))))

(defn resume-goal!
  [session-id]
  (when-let [goal (current-goal session-id)]
    (let [turn-count (long (or (:turn-count goal) 0))
          max-turns  (long (or (:max-turns goal) default-max-turns))]
      (cond
        (= :completed (:status goal))
        (throw (ex-info "Persistent goal is complete; set a new goal to continue"
                        {:type :goal/not-resumable
                         :status 409
                         :error "persistent goal is complete; set a new goal to continue"}))

        (>= turn-count max-turns)
        (throw (ex-info "Persistent goal turn guardrail reached"
                        {:type :goal/max-turns
                         :status 409
                         :error "persistent goal turn guardrail reached; set a new goal to continue"}))

        :else
        (save-goal! session-id
                    (-> goal
                        (assoc :status :active
                               :resumed-at (now)
                               :updated-at (now)
                               :last-judge-status :continue
                               :last-judge-reason "Resumed by user")
                        (dissoc :paused-at)))))))

(defn clear-goal!
  [session-id]
  (when-let [goal (current-goal session-id)]
    (clear-task-snapshot! (assoc goal
                                 :status :cleared
                                 :cleared-at (now)
                                 :updated-at (now))))
  (reset-execution-state! session-id)
  (remove-goal! session-id))

(defn autonomy-input
  [goal user-message]
  (if (active? goal)
    (:text goal)
    user-message))

(defn working-memory-input
  [goal user-message]
  (let [message* (some-> user-message str str/trim not-empty)]
    (if (active? goal)
      (let [contract (goal-contract goal)]
        (str "Persistent goal: " (:text goal)
             (when-let [criteria (seq (:goal/success-criteria contract))]
               (str "\nSuccess criteria:\n"
                    (str/join "\n" (map #(str "- " %) criteria))))
           (when message*
             (str "\n\nCurrent user turn: " message*))))
      user-message)))

(defn attach-task!
  [session-id task-id]
  (when-let [goal (active-goal session-id)]
    (let [at    (now)
          goal* (assoc goal
                       :last-task-id task-id
                       :updated-at at
                       :last-used-at at)]
      (save-goal! session-id goal*)
      goal*)))

(defn- judge-complete?
  [control autonomy-state]
  (or (and (true? (:goal-complete? control))
           (autonomous/structurally-complete? autonomy-state))
      (and (= :complete (:status control))
           (autonomous/structurally-complete? autonomy-state))))

(defn- current-next-step
  [control autonomy-state]
  (or (some-> (:next-step control) str str/trim not-empty)
      (some-> autonomy-state autonomous/current-frame :next-step str str/trim not-empty)))

(defn judge-after-turn!
  "Update persistent goal state after an agent turn.

  The judgment deliberately reuses the autonomous controller result and
  structural goal-complete check instead of introducing a second protocol."
  [session-id {:keys [task-id task-state control autonomy-state guardrail budget-status summary]}]
  (when-let [goal (active-goal session-id)]
    (try
      (let [at           (now)
            turn-count   (inc (long (or (:turn-count goal) 0)))
            max-turns    (long (or (:max-turns goal) default-max-turns))
            complete?    (judge-complete? control autonomy-state)
            guardrail*   (or guardrail
                              (when (= :resumable task-state) :resumable))
            paused?      (or (= :paused task-state)
                             (contains? #{:paused :cancelled :stopped :interrupted
                                          :failed :stalled :restart-loop}
                                        guardrail))
            maxed?       (and (not complete?) (>= turn-count max-turns))
            next-step    (current-next-step control autonomy-state)
            status       (cond
                           complete? :completed
                           paused? :paused
                           maxed? :paused
                           :else :active)
            judge-status (cond
                           complete? :complete
                           paused? (or guardrail :paused)
                           maxed? :max-turns
                           guardrail* guardrail*
                           :else :continue)
            reason       (cond
                           complete? "The autonomous controller marked the root goal complete."
                           maxed? (str "Paused after reaching the persistent goal turn guardrail ("
                                       max-turns
                                       ").")
                           (= :budget guardrail*) "Paused after an LLM budget guardrail."
                           (= :iteration-limit guardrail*) "Paused after the autonomous iteration guardrail."
                           (= :resumable guardrail*) "The task is resumable."
                           paused? (or summary "The backing task stopped before the goal completed.")
                           :else (or (:reason control) "Goal remains active."))]
        (save-goal! session-id
                    (cond-> (assoc goal
                                   :status status
                                   :turn-count turn-count
                                   :updated-at at
                                   :last-judged-at at
                                   :last-used-at at
                                   :last-task-id task-id
                                   :last-task-state task-state
                                   :last-judge-status judge-status
                                   :last-judge-reason reason
                                   :last-summary (or summary (:summary control))
                                   :next-step next-step)
                      complete? (assoc :completed-at at)
                      maxed? (assoc :paused-at at)
                      guardrail* (assoc :last-guardrail guardrail*)
                      budget-status (assoc :last-budget-status
                                           (select-keys budget-status
                                                        [:scope :kind :llm-call-count :total-tokens
                                                         :elapsed-ms :llm-total-duration-ms])))))
      (catch Exception e
        (log/warn e "Failed to update persistent goal judgment"
                  {:session-id session-id
                   :task-id task-id})
        nil))))
