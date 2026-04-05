(ns xia.test-runner
  "Command-line test runner for the native-test uberjar."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class))

(def ^:private cli-options
  [[nil "--suite NAME" "Run a named suite from xia/test_suites.edn"]
   [nil "--list-suites" "Print available suites and exit"]
   ["-h" "--help" "Show help"]])

(defn- load-suites
  []
  (if-let [resource (io/resource "xia/test_suites.edn")]
    (edn/read-string (slurp resource))
    {}))

(defn- all-suite-targets
  [suites]
  (->> suites
       vals
       (apply concat)
       distinct
       vec))

(defn- print-help
  [summary]
  (println)
  (println "Usage: xia-tests [options] [namespace ...]")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  xia-tests")
  (println "  xia-tests --suite test0")
  (println "  xia-tests xia.core-test xia.paths-test"))

(defn- print-suites
  [suites]
  (doseq [[suite targets] suites]
    (println (str (name suite) ": "
                  (str/join ", " (map name targets))))))

(defn- parse-namespace-arg
  [value]
  (cond
    (symbol? value) value
    (keyword? value) (symbol (name value))
    (string? value) (symbol value)
    :else (throw (ex-info "Unsupported test namespace selector."
                          {:value value}))))

(defn- selected-targets
  [{:keys [suite]} args suites]
  (cond
    (seq args)
    (mapv parse-namespace-arg args)

    suite
    (or (get suites (keyword suite))
        (throw (ex-info "Unknown test suite."
                        {:suite suite
                         :available (sort (map name (keys suites)))})))

    :else
    (let [targets (all-suite-targets suites)]
      (when (empty? targets)
        (throw (ex-info "No default tests are configured."
                        {:reason :no-test-suites})))
      targets)))

(defn- run-targets!
  [targets]
  (doseq [target targets]
    (require target))
  (let [summary (apply t/run-tests targets)
        failures (+ (long (or (:fail summary) 0))
                    (long (or (:error summary) 0)))]
    (shutdown-agents)
    (if (zero? failures) 0 1)))

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        suites (load-suites)]
    (cond
      (:help options)
      (do
        (print-help summary)
        (System/exit 0))

      (seq errors)
      (do
        (doseq [error errors]
          (println "Error:" error))
        (print-help summary)
        (System/exit 1))

      (:list-suites options)
      (do
        (print-suites suites)
        (System/exit 0))

      :else
      (try
        (System/exit (run-targets! (selected-targets options arguments suites)))
        (catch Exception e
          (println "Test runner failed:" (.getMessage e))
          (System/exit 1))))))
