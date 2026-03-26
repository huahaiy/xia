(ns xia.test-runner
  "Project test runner that enables compiler warnings before loading test namespaces."
  (:require [cognitect.test-runner :as test-runner]
            [taoensso.timbre :as timbre]))

(defn -main
  [& args]
  (binding [*warn-on-reflection* true
            *unchecked-math* :warn-on-boxed]
    ;; Most test exception paths are asserted behavior. Keep Timbre quiet so
    ;; real test failures and clojure.test errors are easier to spot.
    (timbre/set-min-level! :fatal)
    (apply test-runner/-main args)))
