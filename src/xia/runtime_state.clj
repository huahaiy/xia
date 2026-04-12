(ns xia.runtime-state
  "Tracks coarse runtime lifecycle state for user-facing diagnostics.")

(defonce ^:private installed-runtime-atom (atom nil))
(declare clear-runtime!)

(defn- make-runtime
  []
  {:phase-atom (atom :stopped)
   :drain-state-atom (atom nil)})

(defn- maybe-current-runtime
  []
  @installed-runtime-atom)

(defn- ensure-runtime-installed!
  []
  (or (maybe-current-runtime)
      (let [runtime (make-runtime)]
        (reset! installed-runtime-atom runtime)
        runtime)))

(defn- maybe-phase-atom
  []
  (some-> (maybe-current-runtime) :phase-atom))

(defn- maybe-drain-state-atom
  []
  (some-> (maybe-current-runtime) :drain-state-atom))

(defn- phase-atom
  []
  (:phase-atom (ensure-runtime-installed!)))

(defn- drain-state-atom
  []
  (:drain-state-atom (ensure-runtime-installed!)))

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
    (reset! (:phase-atom runtime) :stopped)
    (reset! (:drain-state-atom runtime) nil)
    (reset! installed-runtime-atom nil))
  nil)

(defn phase
  []
  (if-let [phase-atom* (maybe-phase-atom)]
    @phase-atom*
    :stopped))

(defn drain-state
  []
  (some-> (maybe-drain-state-atom) deref))

(defn draining?
  []
  (boolean (:draining? (drain-state))))

(defn drain-requested-at
  []
  (:requested-at (drain-state)))

(defn accepting-new-work?
  []
  (not (draining?)))

(defn request-drain!
  ([] (request-drain! nil))
  ([reason]
   (let [drain-state-atom* (drain-state-atom)]
     (or @drain-state-atom*
         (reset! drain-state-atom*
                 {:draining? true
                  :reason reason
                  :requested-at (java.time.Instant/now)})))))

(defn clear-drain!
  []
  (reset! (drain-state-atom) nil)
  nil)

(defn reject-new-work-data
  [work-kind]
  (let [phase* (phase)
        drain  (drain-state)
        draining?* (boolean (:draining? drain))]
    {:status 409
     :reason :runtime/draining
     :phase phase*
     :draining? draining?*
     :drain-requested-at (:requested-at drain)
     :work-kind work-kind
     :error "runtime is draining; new work is temporarily disabled"}))

(defn throw-if-not-accepting-new-work!
  [work-kind]
  (when (draining?)
    (throw (ex-info "Runtime is not accepting new work."
                    (reject-new-work-data work-kind)))))

(defn mark-starting!
  []
  (clear-drain!)
  (reset! (phase-atom) :starting))

(defn mark-running!
  []
  (reset! (phase-atom) :running))

(defn mark-stopping!
  []
  (reset! (phase-atom) :stopping))

(defn mark-stopped!
  []
  (clear-drain!)
  (reset! (phase-atom) :stopped))

(defn restarting?
  []
  (contains? #{:starting :stopping} (phase)))

(defn reset-runtime!
  []
  (clear-drain!)
  (reset! (phase-atom) :stopped))
