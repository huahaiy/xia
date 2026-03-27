(ns xia.runtime-state
  "Tracks coarse runtime lifecycle state for user-facing diagnostics.")

(defonce ^:private phase-atom
  (atom :stopped))

(defn phase
  []
  @phase-atom)

(defn mark-starting!
  []
  (reset! phase-atom :starting))

(defn mark-running!
  []
  (reset! phase-atom :running))

(defn mark-stopping!
  []
  (reset! phase-atom :stopping))

(defn mark-stopped!
  []
  (reset! phase-atom :stopped))

(defn restarting?
  []
  (contains? #{:starting :stopping} @phase-atom))
