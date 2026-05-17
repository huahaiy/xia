(ns xia.constraints
  "Resolve the effective operating envelope for a turn."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.paths :as paths]
            [xia.scratch :as scratch]))

(def precedence
  "Lowest to highest precedence. Later sources override earlier sources."
  [:session-context
   :user-preferences
   :task-constraints
   :project-constraints
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

(defn- workspace*
  [session-id workspace-id]
  (or (when workspace-id
        (db/get-workspace workspace-id))
      (when session-id
        (db/session-workspace session-id))
      (db/get-workspace paths/default-workspace-id)))

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
  [{:keys [session-id task-id task workspace-id]}]
  (let [task*       (task* (or task task-id))
        session-id* (or session-id (:session-id task*))
        workspace   (workspace* session-id* workspace-id)
        profile     (when session-id*
                      (db/session-user-profile session-id*))
        project*    (deep-merge (:preferences workspace)
                                (:constraints workspace))]
    {:session-context   (or (session-context session-id*) {})
     :user-preferences  (or (:preferences profile) {})
     :task-constraints  (or (:constraints task*) {})
     :project-constraints project*
     :org-policy        (org-policy)
     :resolved          (cond-> {:session-id session-id*
                                 :workspace-id (:id workspace)}
                          (:id task*) (assoc :task-id (:id task*))
                          (:id profile) (assoc :user-profile-id (:id profile)))}))

(defn operating-envelope
  "Resolve the effective operating envelope for a session/task turn.

   Precedence is org policy > project constraints > task constraints >
   user preferences > session scratch/context. The `:precedence` vector is
   listed lowest-to-highest to match the merge implementation."
  [opts]
  (let [sources* (sources opts)
        effective (apply deep-merge
                         (map sources*
                              [:session-context
                               :user-preferences
                               :task-constraints
                               :project-constraints
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
          (when (:workspace-id resolved)
            (str " for workspace " (:workspace-id resolved)))
          (when (:task-id resolved)
            (str ", task " (:task-id resolved)))
          ": "
          (pr-str effective)))))
