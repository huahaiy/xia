(ns xia.native-test-main
  (:require [clojure.string :as str]
            [clojure.test :as t]
            [taoensso.timbre :as timbre]
            [xia.agent-test]
            [xia.artifact-test]
            [xia.backup-test]
            [xia.browser-login-test]
            [xia.browser-session-options-test]
            [xia.browser-test]
            [xia.channel.http-test]
            [xia.context-test]
            [xia.core-test]
            [xia.cron-test]
            [xia.crypto-test]
            [xia.db-security-test]
            [xia.db-test]
            [xia.email-test]
            [xia.extractive-summary-test]
            [xia.hippocampus-test]
            [xia.http-client-test]
            [xia.identity-test]
            [xia.instance-supervisor-test]
            [xia.llm-test]
            [xia.local-doc-test]
            [xia.local-ocr-test]
            [xia.logging-test]
            [xia.memory-edit-test]
            [xia.memory-test]
            [xia.oauth-template-test]
            [xia.oauth-test]
            [xia.pack-test]
            [xia.paths-test]
            [xia.peer-test]
            [xia.prompt-test]
            [xia.remote-bridge-test]
            [xia.schedule-test]
            [xia.scheduler-test]
            [xia.sci-env-test]
            [xia.scratch-test]
            [xia.secret-test]
            [xia.service-test]
            [xia.skill-openclaw-test]
            [xia.skill-test]
            [xia.summarizer-test]
            [xia.tool-test]
            [xia.web-test]
            [xia.working-memory-test]
            [xia.workspace-test])
  (:gen-class))

(def ^:private supported-test-namespaces
  '[xia.agent-test
    xia.artifact-test
    xia.backup-test
    xia.browser-login-test
    xia.browser-session-options-test
    xia.browser-test
    xia.channel.http-test
    xia.context-test
    xia.core-test
    xia.cron-test
    xia.crypto-test
    xia.db-security-test
    xia.db-test
    xia.email-test
    xia.extractive-summary-test
    xia.hippocampus-test
    xia.http-client-test
    xia.identity-test
    xia.instance-supervisor-test
    xia.llm-test
    xia.local-doc-test
    xia.local-ocr-test
    xia.logging-test
    xia.memory-edit-test
    xia.memory-test
    xia.oauth-template-test
    xia.oauth-test
    xia.pack-test
    xia.paths-test
    xia.peer-test
    xia.prompt-test
    xia.remote-bridge-test
    xia.schedule-test
    xia.scheduler-test
    xia.sci-env-test
    xia.scratch-test
    xia.secret-test
    xia.service-test
    xia.skill-openclaw-test
    xia.skill-test
    xia.summarizer-test
    xia.tool-test
    xia.web-test
    xia.working-memory-test
    xia.workspace-test])

(def ^:private supported-test-set
  (set supported-test-namespaces))

(defn- parse-selected-tests
  [args]
  (if (seq args)
    (let [requested (mapv symbol args)
          unknown   (remove supported-test-set requested)]
      (when (seq unknown)
        (binding [*out* *err*]
          (println "Unknown or JVM-only native test namespaces:"
                   (str/join ", " unknown))
          (println "Available namespaces:"
                   (str/join ", " supported-test-namespaces)))
        (System/exit 2))
      requested)
    supported-test-namespaces))

(defn -main
  [& args]
  (timbre/set-min-level! :fatal)
  (let [selected (parse-selected-tests args)
        summary  (apply t/run-tests selected)
        failures (+ (long (or (:fail summary) 0))
                    (long (or (:error summary) 0)))]
    (shutdown-agents)
    (System/exit (if (zero? failures) 0 1))))
