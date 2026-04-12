(ns xia.retrieval-state
  "Tracks retrieval-relevant datastore mutations so the agent can refresh
   working memory only when search-visible state actually changed.")

(defonce ^:private installed-runtime-atom (atom nil))
(declare clear-runtime!)

(defn- make-runtime
  []
  {:retrieval-state-atom
   (atom {:knowledge-epoch 0
          :local-doc-epochs {}})})

(defn- maybe-current-runtime
  []
  @installed-runtime-atom)

(defn- ensure-runtime-installed!
  []
  (or (maybe-current-runtime)
      (let [runtime (make-runtime)]
        (reset! installed-runtime-atom runtime)
        runtime)))

(defn- retrieval-state-atom
  []
  (:retrieval-state-atom (ensure-runtime-installed!)))

(defn install-runtime!
  ([] (or (maybe-current-runtime)
          (install-runtime! (make-runtime))))
  ([runtime]
   (when-let [current (maybe-current-runtime)]
     (when-not (identical? current runtime)
       (clear-runtime!)))
   (reset! installed-runtime-atom runtime)
   runtime))

(defn clear-runtime!
  []
  (when-let [runtime (maybe-current-runtime)]
    (reset! (:retrieval-state-atom runtime)
            {:knowledge-epoch 0
             :local-doc-epochs {}})
    (reset! installed-runtime-atom nil))
  nil)

(defn reset-runtime!
  []
  (reset! (retrieval-state-atom)
          {:knowledge-epoch 0
           :local-doc-epochs {}})
  nil)

(defn- bump-counter
  ^long [value]
  (inc (long (or value 0))))

(defn bump-knowledge!
  []
  (swap! (retrieval-state-atom) update :knowledge-epoch bump-counter)
  nil)

(defn bump-local-docs!
  [session-id]
  (when session-id
    (swap! (retrieval-state-atom) update-in [:local-doc-epochs (str session-id)] bump-counter))
  nil)

(defn version
  ([]
   (version nil))
  ([local-doc-session-id]
   (let [session-id* (some-> local-doc-session-id str)
         {:keys [knowledge-epoch local-doc-epochs]} @(retrieval-state-atom)]
     {:knowledge-epoch   (long (or knowledge-epoch 0))
      :local-doc-session (when session-id* session-id*)
      :local-doc-epoch   (long (get local-doc-epochs session-id* 0))})))

(defn changed?
  [baseline local-doc-session-id]
  (not=
   (select-keys (or baseline {}) [:knowledge-epoch :local-doc-session :local-doc-epoch])
   (select-keys (version local-doc-session-id) [:knowledge-epoch :local-doc-session :local-doc-epoch])))
