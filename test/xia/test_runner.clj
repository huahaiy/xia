(ns xia.test-runner
  "Project test runner that enables compiler warnings before loading test namespaces."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [cognitect.test-runner :as test-runner]
            [taoensso.timbre :as timbre]))

(defn- load-test-suites
  []
  (or (some-> "xia/test_suites.edn" io/resource slurp edn/read-string)
      (throw (ex-info "Missing test suite configuration"
                      {:resource "xia/test_suites.edn"}))))

(defn- suite-regex
  [suite]
  (let [namespaces (get (load-test-suites) suite)]
    (when-not (seq namespaces)
      (throw (ex-info "Unknown or empty test suite"
                      {:suite suite})))
    (str "^(?:"
         (str/join "|"
                   (map #(java.util.regex.Pattern/quote (str %)) namespaces))
         ")$")))

(defn- parse-suite-args
  [args]
  (loop [remaining args
         suite nil
         passthrough []]
    (if-let [arg (first remaining)]
      (if (= "--suite" arg)
        (recur (nnext remaining) (keyword (second remaining)) passthrough)
        (recur (next remaining) suite (conj passthrough arg)))
      {:suite suite
       :args passthrough})))

(defn- has-explicit-test-selection?
  [args]
  (boolean (some #{"-n" "--namespace" "-r" "--regex"} args)))

(defn- expand-suite-args
  [args]
  (let [{:keys [suite args]} (parse-suite-args args)]
    (if (and suite (not (has-explicit-test-selection? args)))
      (into ["-r" (suite-regex suite)] args)
      args)))

(defn -main
  [& args]
  (binding [*warn-on-reflection* true
            *unchecked-math* :warn-on-boxed]
    ;; Most test exception paths are asserted behavior. Keep Timbre quiet so
    ;; real test failures and clojure.test errors are easier to spot.
    (timbre/set-min-level! :fatal)
    (apply test-runner/-main (expand-suite-args args))))
