(ns xia.tool.callbacks
  "Narrow callbacks registered into the tool runtime by higher layers."
  (:require [xia.runtime-context :as runtime-context]))

(def ^:private runtime-context-key :xia/tool-runtime)

(defn- current-runtime
  []
  (or (runtime-context/runtime runtime-context-key)
      (throw (ex-info "Tool runtime is not installed"
                      {:component :xia/tool-runtime}))))

(defn- callbacks-atom
  []
  (:callbacks-atom (current-runtime)))

(defn register-branch-task-launcher!
  [f]
  (swap! (callbacks-atom) assoc :branch-task-launcher f)
  nil)

(defn clear-callbacks!
  []
  (reset! (callbacks-atom) {})
  nil)

(defn branch-task-launcher
  []
  (or (:branch-task-launcher @(callbacks-atom))
      (throw (ex-info "Branch task launcher is not registered"
                      {:component :xia/tool-runtime
                       :callback :branch-task-launcher}))))
