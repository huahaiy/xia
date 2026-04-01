(ns xia.autonomous
  "Helpers for autonomous scheduled execution."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.prompt :as prompt])
  (:import (com.fasterxml.jackson.core JsonFactory JsonToken)))

(def ^:private default-max-iterations 6)
(def ^:private default-control-field-chars 280)
(def ^:private default-goal-chars 1200)
(def ^:private default-summary-chars 1200)
(def ^:private default-next-step-chars 800)
(def ^:private default-reason-chars 600)
(def ^:private max-agenda-items 8)
(def ^:private max-agenda-item-chars 160)
(def ^:private default-max-stack-depth 32)
(def ^:private compressed-frame-preview-limit 6)
(def ^:private compressed-frame-reason
  "Older suspended stack frames were compressed to stay within the depth limit.")
(def ^:private normalized-state-meta-key ::normalized-state)
(def ^:private intent-marker "ACTION_INTENT_JSON:")
(def ^:private control-marker "AUTONOMOUS_STATUS_JSON:")
(defonce ^:private controller-system-message-cache (atom {}))
(defonce ^:private json-extract-factory (JsonFactory.))
(def ^:private fenced-json-opening-pattern
  #"(?s)\A```(?:[A-Za-z0-9_-]+)?[ \t]*\r?\n?")
(def ^:private fenced-json-closing-pattern #"^\s*```")

(defn context
  []
  prompt/*interaction-context*)

(declare controller-state-message
         current-frame
         apply-control
         suspend-progress-status
         normalized-state?
         normalize-state
         initial-state
         mark-normalized-state)

(defn autonomous-run?
  ([] (autonomous-run? (context)))
  ([ctx]
   (true? (:autonomous-run? ctx))))

(defn trusted?
  ([] (trusted? (context)))
  ([ctx]
   (and (autonomous-run? ctx)
        (true? (:approval-bypass? ctx)))))

(defn- controller-system-message-mode
  [ctx]
  (let [autonomous?    (autonomous-run? ctx)
        branch-worker? (true? (:branch-worker? ctx))
        direct-user?   (not (or autonomous? branch-worker? (= :branch (:channel ctx))))]
    (cond
      direct-user? :direct-user
      branch-worker? :branch-worker
      :else :autonomous)))

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

(defn- max-stack-depth
  []
  (cfg/positive-long :autonomous/max-stack-depth
                     default-max-stack-depth))

(defn control-marker-text
  []
  control-marker)

(defn intent-marker-text
  []
  intent-marker)

(defn- truncate-field
  ([value]
   (truncate-field value default-control-field-chars))
  ([value limit]
   (let [text (some-> value str str/trim)
         limit (long limit)]
     (when (seq text)
       (if (> (long (count text)) limit)
         (str (subs text 0 (max 1 (dec limit))) "…")
         text)))))

(defn- truncate-goal
  [value]
  (truncate-field value default-goal-chars))

(defn- truncate-summary
  [value]
  (truncate-field value default-summary-chars))

(defn- truncate-next-step
  [value]
  (truncate-field value default-next-step-chars))

(defn- truncate-reason
  [value]
  (truncate-field value default-reason-chars))

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
  [{:keys [title progress-status next-step compressed? compressed-count kind]}]
  (let [title*      (truncate-agenda-item title)
        status*     (progress-status-label progress-status)
        next-step*  (truncate-field next-step)]
    (when title*
      (str "  - "
           "[" (or status* "pending") "] "
           title*
           (when (= kind :child-task)
             " (child task)")
           (when compressed?
             (str " (compressed " (or compressed-count 1) ")"))
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
  (or (truncate-goal goal)
      "Current task"))

(def ^:private missing-field ::missing-field)

(defn- first-present
  [m ks]
  (reduce (fn [acc k]
            (if (and (map? m) (contains? m k))
              (reduced (get m k))
              acc))
          missing-field
          ks))

(defn- nullish-value?
  [value]
  (or (nil? value)
      (= :json/null value)
      (= "json/null" value)
      (= ":json/null" value)))

(defn- first-meaningful
  [& values]
  (first (remove nullish-value? values)))

(defn- normalize-frame
  [frame default-title]
  (let [agenda           (normalize-agenda (first-meaningful (:agenda frame)
                                                             (get frame "agenda")))
        child-task-id    (let [value (first-meaningful (:child-task-id frame)
                                                       (:child_task_id frame)
                                                       (get frame "child_task_id")
                                                       (get frame "child-task-id"))]
                           (cond
                             (uuid? value) value
                             (string? value) (try
                                               (java.util.UUID/fromString value)
                                               (catch Exception _
                                                 nil))
                             :else nil))
        kind             (let [raw-kind (or (:kind frame)
                                            (get frame "kind"))]
                           (cond
                             child-task-id :child-task
                             (contains? #{:child-task "child-task" "child_task"} raw-kind) :child-task
                             :else nil))
        child-task       (when child-task-id
                           (db/get-task child-task-id))
        child-progress   (case (:state child-task)
                           :running :in-progress
                           :waiting_input :blocked
                           :waiting_approval :blocked
                           :paused :paused
                           :resumable :resumable
                           :completed :complete
                           :failed :blocked
                           :cancelled :blocked
                           nil)
        compressed?      (true? (or (:compressed? frame)
                                    (get frame "compressed?")))
        compressed-count (when compressed?
                           (some-> (or (:compressed-count frame)
                                       (get frame "compressed_count")
                                       (get frame "compressed-count"))
                                   long
                                   (max 1)))
        compressed-preview (when compressed?
                             (->> (first-meaningful (:compressed-preview frame)
                                                    (get frame "compressed_preview")
                                                    (get frame "compressed-preview"))
                                  (keep truncate-agenda-item)
                                  (take-last compressed-frame-preview-limit)
                                  vec
                                  not-empty))
        status           (or child-progress
                             (normalize-progress-status (first-meaningful (:progress-status frame)
                                                                          (get frame "progress_status")
                                                                          (get frame "progress-status")))
                             (normalize-progress-status (first-meaningful (:status frame)
                                                                          (get frame "status")))
                             (derive-progress-status :continue false agenda)
                             :pending)
        title            (or (some-> child-task :title truncate-goal)
                             (truncate-goal (first-meaningful (:title frame)
                                                              (get frame "title")))
                             (truncate-goal (first-meaningful (:current-focus frame)
                                                              (:current_focus frame)
                                                              (get frame "current_focus")
                                                              (get frame "current-focus")))
                             default-title)]
    (let [summary   (truncate-summary (first-meaningful (some-> child-task :summary)
                                                        (:summary frame)
                                                        (get frame "summary")))
          next-step (truncate-next-step (first-meaningful (:next-step frame)
                                                          (:next_step frame)
                                                          (get frame "next_step")
                                                          (get frame "next-step")))
          reason    (truncate-reason (first-meaningful (:reason frame)
                                                       (get frame "reason")))]
      (cond-> {:title title
               :progress-status status}
        summary (assoc :summary summary)
        next-step (assoc :next-step next-step)
        reason (assoc :reason reason)
        agenda (assoc :agenda agenda)
      kind (assoc :kind kind)
      child-task-id (assoc :child-task-id child-task-id)
      compressed? (assoc :compressed? true)
      compressed-count (assoc :compressed-count compressed-count)
      compressed-preview (assoc :compressed-preview compressed-preview)))))

(defn- compressed-frame?
  [frame]
  (true? (:compressed? frame)))

(defn- summarize-compressed-preview
  [total-count preview]
  (let [preview* (->> preview
                      (keep truncate-agenda-item)
                      (remove str/blank?)
                      vec
                      not-empty)
        hidden   (max 0 (- total-count (count (or preview* []))))]
    (truncate-summary
     (str "Compressed "
          total-count
          " suspended stack frames to stay within the depth limit."
          (when (seq preview*)
            (str " Recent archived lineage: "
                 (str/join " -> " preview*)
                 "."))
          (when (pos? hidden)
            (str " "
                 hidden
                 " older frame"
                 (when (not= hidden 1) "s")
                 " omitted from the preview."))))))

(defn- progress-status-present?
  [frame]
  (if (and (map? frame) (contains? frame :progress-status-explicit?))
    (true? (:progress-status-explicit? frame))
    (not= missing-field
          (first-present frame
                         [:progress-status
                          :progress_status
                          :status
                          "progress_status"
                          "progress-status"
                          "status"]))))

(defn- agenda-present?
  [frame]
  (not= missing-field
        (first-present frame
                       [:agenda
                        "agenda"])))

(defn- next-step-present?
  [frame]
  (not= missing-field
        (first-present frame
                       [:next-step
                        :next_step
                        "next_step"
                        "next-step"])))

(defn- normalize-match-tokens
  [value]
  (some->> value
           str
           str/lower-case
           (re-seq #"[a-z0-9]+")
           vec
           not-empty))

(defn- matching-task?
  [left right]
  (let [left*  (normalize-match-tokens left)
        right* (normalize-match-tokens right)]
    (and left*
         right*
         (or (= left* right*)
             (= (set left*) (set right*))))))

(def ^:private actionable-agenda-statuses
  #{:pending :in-progress :paused :resumable :diverged :blocked})

(def ^:private terminal-agenda-statuses
  #{:completed :skipped})

(defn- derive-parent-agenda-on-pop
  [parent-tip child-tip pop-completion-status]
  (let [agenda      (:agenda parent-tip)
        child-title (:title child-tip)
        parent-next (:next-step parent-tip)]
    (when (and (= :completed pop-completion-status)
               (seq agenda))
      (let [match? (fn [{:keys [item status]}]
                     (and (contains? actionable-agenda-statuses status)
                          (or (matching-task? item child-title)
                              (matching-task? item parent-next))))
            idx    (first (keep-indexed (fn [i entry]
                                          (when (match? entry)
                                            i))
                                        agenda))]
        (when (some? idx)
          (assoc-in (vec agenda) [idx :status] :completed))))))

(defn- first-actionable-agenda-item
  [agenda]
  (some (fn [{:keys [item status]}]
          (when (and item
                     (contains? actionable-agenda-statuses status))
            item))
        agenda))

(defn structurally-complete?
  [state]
  (let [stack* (:stack (cond
                         (normalized-state? state) state
                         (map? state)             (normalize-state state)
                         :else                    (initial-state nil)))]
    (and (seq stack*)
         (every? #(= :complete (:progress-status %)) stack*)
         (every? (fn [{:keys [agenda]}]
                   (every? (fn [{:keys [status]}]
                             (contains? terminal-agenda-statuses status))
                           agenda))
                 stack*))))

(defn- derive-incomplete-progress-status
  [stack tip]
  (let [agenda (:agenda tip)]
    (cond
      (some #(= :diverged (:status %)) agenda) :diverged
      (some #(= :resumable (:status %)) agenda) :resumable
      (some #(= :paused (:status %)) agenda) :paused
      (some #(= :blocked (:status %)) agenda) :blocked
      (seq agenda)
      (if (some #(contains? #{:in-progress :completed :skipped} (:status %)) agenda)
        :in-progress
        :pending)
      (> (count stack) 1) :resumable
      :else :in-progress)))

(defn reconcile-invalid-goal-complete
  [state]
  (let [state* (cond
                 (normalized-state? state) state
                 (map? state)             (normalize-state state)
                 :else                    (initial-state nil))
        stack* (:stack state*)
        tip    (peek stack*)]
    (if (or (empty? stack*)
            (structurally-complete? state*))
      state*
      (let [updated-tip (assoc tip :progress-status (derive-incomplete-progress-status stack* tip))]
        (mark-normalized-state
         {:stack (conj (vec (butlast stack*)) updated-tip)})))))

(defn- derive-parent-control-on-pop
  [parent-tip child-tip control parent-title]
  (let [pop-completion-status (or (:pop-completion-status control) :completed)
        derived-agenda     (when-not (agenda-present? control)
                             (derive-parent-agenda-on-pop parent-tip
                                                          child-tip
                                                          pop-completion-status))
        effective-agenda   (or derived-agenda
                               (:agenda control)
                               (:agenda parent-tip))
        derived-next-step  (when-not (next-step-present? control)
                             (let [parent-next (:next-step parent-tip)]
                               (when (or (str/blank? parent-next)
                                 (matching-task? parent-next (:title child-tip)))
                                 (first-actionable-agenda-item effective-agenda))))
        derived-progress   (when-not (progress-status-present? control)
                             (case pop-completion-status
                               :failed :blocked
                               :cancelled :resumable
                               (derive-progress-status (:status control)
                                                       (:goal-complete? control)
                                                       effective-agenda)))]
    (cond-> (assoc control :title parent-title)
      derived-agenda (assoc :agenda derived-agenda)
      derived-next-step (assoc :next-step derived-next-step)
      derived-progress (assoc :progress-status derived-progress))))

(defn- child-task-terminal-state
  [child-task-id]
  (some-> child-task-id
          db/get-task
          :state
          ({:completed :completed
            :failed :failed
            :cancelled :cancelled})))

(defn- child-task-pop-control
  [parent-tip child-tip]
  (let [child-task   (some-> child-tip :child-task-id db/get-task)
        outcome      (or ({:completed :completed
                           :failed :failed
                           :cancelled :cancelled}
                          (:state child-task))
                         :completed)
        child-title  (:title child-tip)
        child-summary (or (:summary child-task)
                          (:summary child-tip))
        base-summary (case outcome
                       :completed (str "Child task completed: " child-title)
                       :failed (str "Child task failed: " child-title)
                       :cancelled (str "Child task cancelled: " child-title)
                       (str "Child task finished: " child-title))
        summary      (truncate-summary
                      (if (and child-summary
                               (not= child-summary child-title))
                        (str base-summary ". " child-summary)
                        base-summary))
        next-step    (case outcome
                       :failed (truncate-next-step (str "Handle the failed child task: " child-title))
                       :cancelled (truncate-next-step (str "Decide whether to resume child task: " child-title))
                       nil)
        reason       (truncate-reason
                      (case outcome
                        :completed "The child task reached completion and control returns to the parent."
                        :failed "The child task failed and the parent now needs to handle that failure."
                        :cancelled "The child task was cancelled and the parent may need to decide how to continue."
                        "The child task finished and control returns to the parent."))]
    (cond-> {:status :continue
             :summary summary
             :reason reason
             :current-focus (:title parent-tip)
             :stack-action :pop
             :pop-completion-status outcome}
      next-step (assoc :next-step next-step)
      (= outcome :completed) (assoc :goal-complete? false)
      (= outcome :failed) (assoc :progress-status :blocked)
      (= outcome :cancelled) (assoc :progress-status :resumable))))

(defn reconcile-child-task-state
  [state]
  (let [state* (cond
                 (normalized-state? state) state
                 (map? state)             (normalize-state state)
                 :else                    (initial-state nil))]
    (loop [current state*]
      (let [stack* (:stack current)
            tip    (peek stack*)
            child-outcome (and (> (count stack*) 1)
                               (= :child-task (:kind tip))
                               (child-task-terminal-state (:child-task-id tip)))]
        (if child-outcome
          (let [parent-tip (peek (vec (butlast stack*)))]
            (recur (apply-control current
                                  (child-task-pop-control parent-tip tip))))
          current)))))

(defn- compress-stack
  [stack]
  (let [stack* (vec stack)
        limit  (long (max-stack-depth))]
    (if (<= (count stack*) limit)
      stack*
      (let [root          (first stack*)
            tail-count    (max 0 (- limit 2))
            descendants   (subvec stack* 1)
            kept-tail     (vec (take-last tail-count descendants))
            compressed    (vec (drop-last tail-count descendants))
            aggregate     (reduce (fn [{:keys [count preview representative]} frame]
                                    (if (compressed-frame? frame)
                                      {:count (+ count (or (:compressed-count frame) 1))
                                       :preview (into (vec preview)
                                                      (or (:compressed-preview frame)
                                                          (when-let [title (:title frame)]
                                                            [title])))
                                       :representative (or representative frame)}
                                      {:count (inc count)
                                       :preview (conj (vec preview) (:title frame))
                                       :representative frame}))
                                  {:count 0
                                   :preview []
                                   :representative nil}
                                  compressed)
            representative (or (:representative aggregate)
                               (last compressed)
                               root)
            preview        (->> (:preview aggregate)
                                (keep truncate-agenda-item)
                                (remove str/blank?)
                                (take-last compressed-frame-preview-limit)
                                vec
                                not-empty)
            archive        {:title (:title representative)
                            :summary (summarize-compressed-preview (:count aggregate) preview)
                            :next-step (or (:next-step representative)
                                           (first-actionable-agenda-item (:agenda representative)))
                            :reason (truncate-reason compressed-frame-reason)
                            :progress-status (suspend-progress-status (:progress-status representative))
                            :agenda (:agenda representative)
                            :compressed? true
                            :compressed-count (:count aggregate)
                            :compressed-preview preview}]
        (vec (concat [root archive] kept-tail))))))

(defn- normalize-stack
  [goal stack]
  (cond
    (and (some? stack)
         (sequential? stack)
         (empty? stack))
    []

    :else
    (let [limit         (long (max-stack-depth))
          default-title (default-frame-title goal)
          stack*        (->> (or stack [])
                             (keep #(when (map? %)
                                      (normalize-frame % default-title)))
                             vec)]
      (if (seq stack*)
        (if (<= (count stack*) limit)
          stack*
          (compress-stack stack*))
        [(normalize-frame {:title default-title
                           :progress-status :pending}
                          default-title)]))))

(defn- normalized-state?
  [state]
  (true? (some-> state meta normalized-state-meta-key)))

(defn- mark-normalized-state
  [state]
  (if (instance? clojure.lang.IObj state)
    (vary-meta state assoc normalized-state-meta-key true)
    state))

(defn initial-state
  [goal]
  (let [goal* (truncate-goal goal)]
    (mark-normalized-state
     {:stack (normalize-stack goal* nil)})))

(defn normalize-state
  [state]
  (if (normalized-state? state)
    state
    (let [raw-stack (or (:stack state)
                        (get state "stack"))
          seed-title (truncate-goal (or (some-> raw-stack first :title)
                                        (some-> raw-stack first (get "title"))
                                        (some-> raw-stack peek :title)
                                        (some-> raw-stack peek (get "title"))))
          stack*     (normalize-stack seed-title raw-stack)]
      (mark-normalized-state
       {:stack stack*}))))

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
                                      :blocked
                                      :complete}
                                    current-status)]
    (if resumable-state?
      state*
      (initial-state user-message))))

(defn- effective-stack
  [state]
  (cond
    (normalized-state? state)
    (normalize-stack (some-> state :stack first :title)
                     (:stack state))

    (map? state)
    (:stack (normalize-state state))

    :else
    (:stack (initial-state nil))))

(defn root-frame
  [state]
  (first (effective-stack state)))

(defn root-goal
  [state]
  (some-> (root-frame state) :title))

(defn- goal-section-text
  [goal incoming-message]
  (let [goal*  (truncate-goal goal)
        input* (truncate-goal incoming-message)
        same?  (and goal* input* (= goal* input*))]
    (cond
      (and goal* input* (not same?))
      (str "Current root goal:\n"
           goal*
           "\n\n"
           "Latest turn input:\n"
           input*
           "\n\n"
           "The stack is the source of truth for execution. Treat the latest turn input as the freshest signal for this turn. "
           "If it changes the task, reflect that with current_focus and stack_action at the end of the iteration.")

      goal* goal*
      input* input*
      :else "")))

(defn current-frame
  [state]
  (peek (effective-stack state)))

(defn- suspend-progress-status
  [status]
  (case status
    :paused :paused
    :resumable :resumable
    :diverged :diverged
    :blocked :blocked
    :complete :complete
    :resumable))

(defn- suspend-frame
  [frame]
  (when (map? frame)
    (assoc frame :progress-status (suspend-progress-status (:progress-status frame)))))

(defn- merge-frame
  [existing control default-title]
  (let [existing*    (when (map? existing)
                       (normalize-frame existing default-title))
        control*     (normalize-frame control default-title)
        progress?    (progress-status-present? control)
        merged-title (or (:title control*)
                         (:title existing*)
                         default-title)]
    (let [summary   (or (:summary control*)
                        (:summary existing*))
          next-step (or (:next-step control*)
                        (:next-step existing*))
          reason    (or (:reason control*)
                        (:reason existing*))
          agenda    (or (:agenda control*)
                        (:agenda existing*))]
      (cond-> {:title           merged-title
               :progress-status (if progress?
                                  (or (:progress-status control*)
                                      (:progress-status existing*)
                                      :pending)
                                  (or (:progress-status existing*)
                                      (:progress-status control*)
                                      :pending))}
        summary (assoc :summary summary)
        next-step (assoc :next-step next-step)
        reason (assoc :reason reason)
        agenda (assoc :agenda agenda)
      (or (:kind control*)
          (:kind existing*))
      (assoc :kind (or (:kind control*)
                       (:kind existing*)))

      (or (:child-task-id control*)
          (:child-task-id existing*))
      (assoc :child-task-id (or (:child-task-id control*)
                                (:child-task-id existing*)))))))

(defn apply-control
  [state control]
  (let [seed-title    (or (root-goal state)
                          (truncate-goal (:current-focus control))
                          (truncate-goal (:summary control)))
        stack         (normalize-stack seed-title (:stack state))
        current-tip   (peek stack)
        action        (normalize-stack-action (:stack-action control))
        tip-title     (or (truncate-agenda-item (:current-focus control))
                          (:title current-tip)
                          (default-frame-title seed-title))
        next-tip      (normalize-frame (assoc control :title tip-title)
                                       tip-title)
        merged-tip    (merge-frame current-tip
                                   (assoc control :title tip-title)
                                   tip-title)
        replace-top   (fn [frames frame]
                        (if (seq frames)
                          (conj (vec (butlast frames)) frame)
                          [frame]))
        next-stack    (case action
                        :push
                        (->> (conj (replace-top stack (suspend-frame current-tip))
                                   next-tip)
                             compress-stack
                             vec)

                        :pop
                        (if (> (count stack) 1)
                          (let [parent-stack (vec (butlast stack))
                                parent-tip   (peek parent-stack)
                                parent-title (or (truncate-agenda-item (:current-focus control))
                                                 (:title parent-tip)
                                                 (default-frame-title seed-title))
                                parent-control (derive-parent-control-on-pop parent-tip
                                                                             current-tip
                                                                             control
                                                                             parent-title)
                                parent-frame (merge-frame parent-tip
                                                          parent-control
                                                          parent-title)]
                            (replace-top parent-stack parent-frame))
                          [merged-tip])

                        :replace
                        (replace-top stack next-tip)

                        :clear
                        (if (= :continue (:status control))
                          [next-tip]
                          [])

                        :stay
                        (replace-top stack merged-tip))]
    (mark-normalized-state
     {:stack next-stack})))

(defn working-memory-message
  [{:keys [stack] :as state} iteration max-iterations]
  (:content (controller-state-message
             {:goal (root-goal state)
              :iteration iteration
              :max-iterations max-iterations
              :stack stack
              :previous-summary (some-> (peek stack) :summary)
              :previous-next-step (some-> (peek stack) :next-step)
              :previous-reason (some-> (peek stack) :reason)
              :previous-progress-status (some-> (peek stack) :progress-status)
              :previous-agenda (some-> (peek stack) :agenda)})))

(defn retrieval-message
  [{:keys [stack] :as state} & {:keys [incoming-message]}]
  (let [root-title    (some-> (root-frame state) :title)
        tip           (current-frame state)
        tip-title     (:title tip)
        summary       (:summary tip)
        next-step     (:next-step tip)
        agenda-items  (->> (:agenda tip)
                           (keep (fn [{:keys [item status]}]
                                   (when (and item
                                              (contains? actionable-agenda-statuses status))
                                     item)))
                           (take 4)
                           vec
                           not-empty)
        input*        (truncate-field incoming-message)]
    (->> [(when root-title
            (str "Goal: " root-title))
          (when (and tip-title
                     (not= tip-title root-title))
            (str "Current focus: " tip-title))
          (when summary
            (str "Current state: " summary))
          (when next-step
            (str "Next step: " next-step))
          (when (seq agenda-items)
            (str "Active agenda: " (str/join "; " agenda-items)))
          (when input*
            (str "New input: " input*))]
         (remove str/blank?)
         (str/join "\n"))))

(defn child-task-frame
  [child-task-id & {:keys [title summary next-step reason progress-status]}]
  (normalize-frame
   (cond-> {:kind :child-task
            :child-task-id child-task-id}
     title (assoc :title title)
     summary (assoc :summary summary)
     next-step (assoc :next-step next-step)
     reason (assoc :reason reason)
     progress-status (assoc :progress-status progress-status))
   (or (truncate-goal title)
       "Child task")))

(defn attach-child-task
  [state child-task-id & {:keys [title summary next-step reason progress-status]}]
  (let [state*      (cond
                      (normalized-state? state) state
                      (map? state)             (normalize-state state)
                      :else                    (initial-state title))
        control     (cond-> {:status :continue
                             :stack-action :push
                             :kind :child-task
                             :child-task-id child-task-id
                             :current-focus (or title
                                                (some-> (db/get-task child-task-id) :title)
                                                "Child task")
                             :agenda []}
                      summary (assoc :summary summary)
                      next-step (assoc :next-step next-step)
                      reason (assoc :reason reason)
                      progress-status (assoc :progress-status progress-status))]
    (apply-control state* control)))

(defn status-line
  [phase {:keys [stack] :as state} iteration max-iterations & {:keys [stack-action]}]
  (let [stack*        (normalize-stack (root-goal state) stack)
        tip           (peek stack*)
        title         (or (some-> tip :title truncate-agenda-item)
                          (some-> stack* first :title truncate-agenda-item)
                          (default-frame-title nil))
        progress      (progress-status-label (:progress-status tip))
        next-step     (truncate-field (:next-step tip))
        child-task?   (= :child-task (:kind tip))
        compressed?   (true? (:compressed? tip))
        compressed-count (:compressed-count tip)
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
         (when child-task?
           " (child task)")
         (when compressed?
           (str " (compressed " (or compressed-count 1) ")"))
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
        mode           (controller-system-message-mode ctx)]
    (or (get @controller-system-message-cache mode)
        (let [direct-user? (= mode :direct-user)
              message {:role "system"
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
                            "- Use stay when continuing the current tip. Use push to suspend the current tip and enter a child task. Use pop when the current child is done and control should return to its parent. Use replace when the current tip should be superseded. Use clear only when prior stack state should be discarded and the stack should be reset.\n"
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
                            "If you request any tool calls in a response, do not append "
                            control-marker
                            " yet. In tool-requesting responses, emit the intent object but leave the control envelope out.\n"
                            "Append "
                            control-marker
                            " only on the final assistant response of the iteration, after all needed tool rounds are complete and you are not requesting more tools.\n"
                            "At the very end of that final response, append the literal marker "
                            control-marker
                            " followed by one valid JSON object with this exact shape:\n"
                            "{\"status\":\"continue|complete\",\"summary\":\"...\",\"next_step\":\"...\",\"reason\":\"...\",\"goal_complete\":true|false,\"current_focus\":\"...\",\"stack_action\":\"stay|push|pop|replace|clear\",\"progress_status\":\"not_started|pending|in_progress|paused|resumable|diverged|blocked|complete\",\"agenda\":[{\"item\":\"...\",\"status\":\"pending|in_progress|paused|resumable|diverged|completed|blocked|skipped\"}]}\n"
                            "- summary: short factual summary of what changed this iteration.\n"
                            "- next_step: short concrete next action when status=continue, otherwise empty.\n"
                            "- reason: why you are continuing or completing.\n"
                            "- goal_complete: true only when the current goal is fully satisfied.\n"
                            "- current_focus: title of the current stack tip after this iteration.\n"
                            "- stack_action: stay to keep working the current frame, push to enter a subroutine, pop to return to the parent frame, replace to switch the current frame, clear to discard prior stack state and reset from the current focus.\n"
                            "- progress_status: overall status of the current stack tip.\n"
                            "- agenda: ordered short checklist for the current stack tip only, not the full stack.\n"
                            "- Use paused when work should stop for now. Use resumable when it is paused but has a clear restart path. Use diverged when the work has meaningfully branched away from the original plan.\n"
                            (when direct-user?
                              "- For direct user-facing turns, use continue only when another internal iteration can make more progress without waiting on the user.\n")
                            "- Return JSON only after the marker. Raw JSON is preferred; fenced ```json blocks are also accepted.")}]
          (or (get (swap! controller-system-message-cache assoc mode message) mode)
              message)))))

(defn controller-state-message
  [{:keys [goal iteration max-iterations stack previous-summary previous-next-step
           previous-reason previous-progress-status previous-agenda incoming-message]}]
  (let [stack* (when (seq stack)
                 (normalize-stack goal stack))
        tip    (peek stack*)
        goal*  (goal-section-text goal incoming-message)]
    {:role "system"
     :content
     (str "Autonomous agent control state.\n\n"
          "Goal:\n"
          goal*
          "\n\n"
          "Current iteration: " iteration " of " max-iterations ".\n"
          "Observe the current session history and tool results before deciding the next step.\n"
          "Start the first assistant response in this iteration with "
          intent-marker
          "{...} before any explanation or tool call.\n"
          "If this response requests tools, do not append "
          control-marker
          " yet. Append it only on the final response of the iteration when you are not requesting more tools.\n"
          (when-let [input (truncate-field incoming-message)]
            (str "\nNew input for this turn:\n"
                 input
                 "\n"
                 "Interpret this input relative to the current stack tip. Work on that tip in this iteration. If the input means the tip should be suspended, returned from, replaced, or discarded, report that with stack_action at the end of the iteration.\n"))
          (if (seq stack*)
            (str "\nCurrent execution stack (bottom -> top):\n"
                 (str/join "\n" (stack-lines stack*))
                 "\n"
                 (when-let [summary (some-> tip :summary truncate-summary)]
                   (str "- Tip summary: " summary "\n"))
                 (when-let [reason (some-> tip :reason truncate-reason)]
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
                   (when-let [summary (truncate-summary previous-summary)]
                     (str "- Summary: " summary "\n"))
                   (when-let [next-step (truncate-next-step previous-next-step)]
                     (str "- Next step: " next-step "\n"))
                   (when-let [reason (truncate-reason previous-reason)]
                     (str "- Reason: " reason "\n"))
                   (when-let [progress-status (progress-status-label previous-progress-status)]
                     (str "- Progress status: " progress-status "\n"))
                   (when-let [lines (agenda-lines previous-agenda)]
                     (str "- Agenda:\n"
                          (str/join "\n" lines)
                          "\n")))))
          "\nMake real progress now. Use stack_action=push when entering a subroutine and stack_action=pop when returning to a parent frame.")}))

(defn- extract-leading-json-object
  [text]
  (let [body (some-> text str str/triml)]
    (when (and (seq body) (= \{ (.charAt ^String body 0)))
      (try
        (with-open [parser (.createParser ^JsonFactory json-extract-factory ^String body)]
          (when (= JsonToken/START_OBJECT (.nextToken parser))
            (loop [depth 1]
              (when-let [token (.nextToken parser)]
                (let [depth* (cond
                               (= token JsonToken/START_OBJECT) (inc depth)
                               (= token JsonToken/END_OBJECT)   (dec depth)
                               :else                            depth)]
                  (if (zero? depth*)
                    (let [offset (long (.getCharOffset (.getCurrentLocation parser)))]
                      {:json-text (subs body 0 offset)
                       :consumed  offset
                       :rest      (subs body offset)})
                    (recur depth*)))))))
        (catch Exception _
          nil)))))

(defn- brace-start-candidate-indices
  [text]
  (let [text* (or text "")]
    (loop [idx 0
           indices []]
      (if (>= idx (count text*))
        indices
        (let [ch (.charAt ^String text* idx)]
          (if (= ch \{)
            (recur (inc idx) (conj indices idx))
            (recur (inc idx) indices)))))))

(defn- extract-fenced-json-object
  [text]
  (let [trimmed (some-> text str str/triml)]
    (when-let [opening (when trimmed
                         (re-find fenced-json-opening-pattern trimmed))]
      (let [body (subs trimmed (count opening))
            search-window (if-let [closing-idx (str/index-of body "```")]
                            (subs body 0 closing-idx)
                            body)]
        (some (fn [candidate-idx]
                (when-let [{:keys [json-text consumed]}
                           (extract-leading-json-object (subs body candidate-idx))]
                  {:json-text json-text
                   :rest      (-> (subs body (+ candidate-idx consumed))
                                  (str/replace-first fenced-json-closing-pattern ""))}))
              (brace-start-candidate-indices search-window))))))

(defn- extract-json-object
  [text]
  (let [trimmed (some-> text str str/triml)]
    (when (seq trimmed)
      (or (extract-fenced-json-object trimmed)
          (extract-leading-json-object trimmed)))))

(defn- next-protocol-marker-index
  [text]
  (->> [(str/index-of text intent-marker)
        (str/index-of text control-marker)]
       (remove nil?)
       sort
       first))

(defn- json-start-candidate-indices
  [text]
  (let [text* (or text "")]
    (loop [idx 0
           indices []]
      (if (>= idx (count text*))
        indices
        (let [ch (.charAt ^String text* idx)]
          (cond
            (= ch \{)
            (recur (inc idx) (conj indices idx))

            (and (= ch \`)
                 (.startsWith ^String text* "```" idx))
            (recur (+ idx 3) (conj indices idx))

            :else
            (recur (inc idx) indices)))))))

(defn- parse-json-after-marker
  [text marker]
  (let [text* (or text "")]
    (if-let [idx (str/index-of text* marker)]
      (let [before (subs text* 0 idx)
            after  (subs text* (+ idx (count marker)))
            search-window (if-let [next-marker-idx (next-protocol-marker-index after)]
                            (subs after 0 next-marker-idx)
                            after)]
        (if-let [{:keys [parsed after]}
                 (some (fn [candidate-idx]
                         (when-let [{:keys [json-text rest]}
                                    (extract-json-object (subs after candidate-idx))]
                           (try
                             {:parsed (json/read-json json-text)
                              :after rest}
                             (catch Exception _
                               nil))))
                       (json-start-candidate-indices search-window))]
          {:status :parsed
           :before before
           :after after
           :parsed parsed}
          {:status :malformed
           :before before
           :after after}))
      {:status :missing
       :before text*
       :after ""})))

(defn parse-controller-response
  [response]
  (let [text            (or response "")
        parsed-intent   (parse-json-after-marker text intent-marker)
        text-without-intent (if (contains? #{:parsed :malformed} (:status parsed-intent))
                              (str (:before parsed-intent)
                                   (:after parsed-intent))
                              text)
        parsed-control  (parse-json-after-marker text-without-intent control-marker)
        head            (some-> (case (:status parsed-control)
                                  :parsed
                                  (str (:before parsed-control)
                                       (:after parsed-control))

                                  :malformed
                                  (:before parsed-control)

                                  text-without-intent)
                                str/trim)
        parsed          (:parsed parsed-control)
        status-raw (some-> (or (get parsed "status")
                               (:status parsed))
                           str
                           str/trim
                           str/lower-case)
        status     (if (= "continue" status-raw) :continue :complete)
        summary    (or (truncate-summary (or (get parsed "summary")
                                             (:summary parsed)))
                       (truncate-summary head))
        next-step  (truncate-next-step (or (get parsed "next_step")
                                           (:next_step parsed)
                                           (get parsed "next-step")
                                           (:next-step parsed)))
        reason     (truncate-reason (or (get parsed "reason")
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
        raw-progress-status (first-present parsed
                                          ["progress_status"
                                           :progress_status
                                           "progress-status"
                                           :progress-status])
        stack-action   (normalize-stack-action
                         (or (get parsed "stack_action")
                             (:stack_action parsed)
                             (get parsed "stack-action")
                             (:stack-action parsed)))
        agenda     (normalize-agenda (or (get parsed "agenda")
                                         (:agenda parsed)))
        progress-status (or (normalize-progress-status
                              raw-progress-status)
                            (derive-progress-status status
                                                    goal-complete?
                                                    agenda))]
    {:assistant-text head
     :intent-status (:status parsed-intent)
     :intent        (some-> parsed-intent :parsed normalize-intent)
     :control-status (:status parsed-control)
     :control       (when parsed
                      {:status         status
                       :summary        summary
                       :next-step      next-step
                       :reason         reason
                       :goal-complete? goal-complete?
                       :current-focus  current-focus
                       :stack-action   stack-action
                       :progress-status-explicit? (not= missing-field raw-progress-status)
                       :progress-status progress-status
                       :agenda         agenda})}))
