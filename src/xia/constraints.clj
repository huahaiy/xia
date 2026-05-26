(ns xia.constraints
  "Resolve the effective operating envelope for a turn."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.goal :as goal]
            [xia.scratch :as scratch]))

(def precedence
  "Lowest to highest precedence. Later sources override earlier sources."
  [:session-context
   :user-preferences
   :task-constraints
   :goal-contract
   :org-policy])

(defn- parse-edn-map
  [value]
  (cond
    (nil? value)
    nil

    (map? value)
    value

    (string? value)
    (try
      (let [parsed (edn/read-string value)]
        (when (map? parsed)
          parsed))
      (catch Exception _
        nil))

    :else
    nil))

(defn org-policy
  []
  (or (cfg/custom-option :constraints/org-policy {} parse-edn-map)
      {}))

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

(defn- task*
  [task-or-id]
  (cond
    (map? task-or-id) task-or-id
    task-or-id       (db/get-task task-or-id)
    :else            nil))

(defn- scratch-context
  [session-id]
  (when session-id
    (try
      (let [pads (scratch/list-pads {:scope :session
                                     :session-id session-id})]
        (when (seq pads)
          {:scratch-pads
           (mapv #(select-keys % [:id :title :mime :version :updated-at])
                 pads)}))
      (catch Exception _
        nil))))

(defn- session-context
  [session-id]
  (when session-id
    (let [history-recap (db/session-history-recap session-id)
          tool-recap    (db/session-tool-recap session-id)
          scratch*      (scratch-context session-id)]
      (cond-> {:session {:id session-id}}
        history-recap (assoc-in [:session :history-recap] history-recap)
        tool-recap (assoc-in [:session :tool-recap] tool-recap)
        scratch* (update :session merge scratch*)))))

(defn sources
  "Return each policy/context source used to resolve a turn envelope."
  [{:keys [session-id task-id task goal]}]
  (let [task*       (task* (or task task-id))
        session-id* (or session-id (:session-id task*))
        profile     (when session-id*
                      (db/session-user-profile session-id*))
        goal*       (or goal
                        (when session-id*
                          (goal/current-goal session-id*)))]
    {:session-context   (or (session-context session-id*) {})
     :user-preferences  (or (:preferences profile) {})
     :task-constraints  (or (:constraints task*) {})
     :goal-contract     (or (goal/operating-envelope-source goal*) {})
     :org-policy        (org-policy)
     :resolved          (cond-> {:session-id session-id*}
                          (:id task*) (assoc :task-id (:id task*))
                          (:id goal*) (assoc :goal-id (:id goal*))
                          (:id profile) (assoc :user-profile-id (:id profile)))}))

(defn operating-envelope
  "Resolve the effective operating envelope for a session/task turn.

   Precedence is org policy > goal contract > task constraints >
   user preferences > session scratch/context. The `:precedence` vector is
   listed lowest-to-highest to match the merge implementation."
  [opts]
  (let [sources* (sources opts)
        effective (apply deep-merge
                         (map sources*
                              [:session-context
                               :user-preferences
                               :task-constraints
                               :goal-contract
                               :org-policy]))]
    {:precedence precedence
     :sources    sources*
     :effective  effective}))

(def turn-envelope operating-envelope)

(defn summarize-envelope
  [envelope]
  (let [resolved (:resolved (:sources envelope))
        effective (:effective envelope)]
    (str/trim
     (str "Operating envelope"
          (when (:task-id resolved)
            (str " for task " (:task-id resolved)))
          (when (:goal-id resolved)
            (str ", goal " (:goal-id resolved)))
          ": "
          (pr-str effective)))))
