(ns xia.test-runner
  "Project test runner that enables compiler warnings before loading test namespaces."
  (:require [cognitect.test-runner :as test-runner]))

(defn -main
  [& args]
  (binding [*warn-on-reflection* true
            *unchecked-math* :warn-on-boxed]
    (apply test-runner/-main args)))
