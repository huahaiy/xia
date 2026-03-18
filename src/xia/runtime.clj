(ns xia.runtime
  "Runtime feature detection helpers."
  (:require [clojure.string :as str]))

(defn native-image?
  "True when running inside a GraalVM native-image runtime."
  []
  (= "runtime"
     (some-> (System/getProperty "org.graalvm.nativeimage.imagecode")
             str
             str/lower-case)))
