(ns xia.native
  "Native-image helpers."
  (:require [clojure.tools.logging.impl :as impl]))

(defn logging-factory
  "Return a logger factory that resolves the concrete backend lazily at runtime.

   This keeps SLF4J/Logback objects out of the image heap during GraalVM analysis
   while preserving the normal backend choice when the native binary actually runs."
  []
  (let [factory* (delay (impl/find-factory))]
    (reify impl/LoggerFactory
      (name [_]
        "xia-deferred")
      (get-logger [_ logger-ns]
        (impl/get-logger @factory* logger-ns)))))
