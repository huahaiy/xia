(ns xia.runtime-state
  "Tracks coarse runtime lifecycle state for user-facing diagnostics.")

(defonce ^:private phase-atom
  (atom :stopped))

(defonce ^:private drain-state-atom
  (atom nil))

(defn phase
  []
  @phase-atom)

(defn drain-state
  []
  @drain-state-atom)

(defn draining?
  []
  (boolean (:draining? @drain-state-atom)))

(defn drain-requested-at
  []
  (:requested-at @drain-state-atom))

(defn accepting-new-work?
  []
  (not (draining?)))

(defn request-drain!
  ([] (request-drain! nil))
  ([reason]
   (or @drain-state-atom
       (reset! drain-state-atom
               {:draining? true
                :reason reason
                :requested-at (java.time.Instant/now)}))))

(defn clear-drain!
  []
  (reset! drain-state-atom nil)
  nil)

(defn reject-new-work-data
  [work-kind]
  (let [phase* (phase)
        drain  @drain-state-atom
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
  (reset! phase-atom :starting))

(defn mark-running!
  []
  (reset! phase-atom :running))

(defn mark-stopping!
  []
  (reset! phase-atom :stopping))

(defn mark-stopped!
  []
  (clear-drain!)
  (reset! phase-atom :stopped))

(defn restarting?
  []
  (contains? #{:starting :stopping} @phase-atom))

(defn reset-runtime!
  []
  (clear-drain!)
  (reset! phase-atom :stopped))
