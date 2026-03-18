(ns build
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.build.api :as b]))

(def lib 'com.xia/xia)
(def version "0.1.0")
(def class-dir "target/classes")
(def uber-file "target/xia.jar")
(def basis (delay (b/create-basis {:project "deps.edn"})))

(def ^:private native-resource-config
  "META-INF/native-image/xia/xia/resource-config.json")

(def ^:private common-resource-patterns
  ["oauth_providers.edn"
   "example-tool.edn"
   "example-skill.md"
   "log4j2.component.properties"
   "log4j2.simplelog.properties"
   "tools/"
   "web/"])

(defn- resource-pattern
  [path]
  (str "\\\\Q" path "\\\\E.*"))

(defn- native-driver-resource-dir
  []
  (let [os-name (some-> (System/getProperty "os.name") str/lower-case)
        arch    (some-> (System/getProperty "os.arch") str/lower-case)]
    (cond
      (and (str/includes? os-name "mac")
           (#{"aarch64" "arm64"} arch))
      "driver/mac-arm64/"

      (and (str/includes? os-name "linux")
           (#{"aarch64" "arm64"} arch))
      "driver/linux-arm64/"

      (and (str/includes? os-name "linux")
           (#{"x86_64" "amd64"} arch))
      "driver/linux/"

      (and (str/includes? os-name "windows")
           (#{"x86_64" "amd64"} arch))
      "driver/win32_x64/"

      :else
      (throw (ex-info "Unsupported platform for Playwright native driver packaging."
                      {:os-name os-name
                       :arch arch})))))

(defn- write-native-resource-config!
  []
  (let [path     (str class-dir "/" native-resource-config)
        patterns (map resource-pattern
                      (conj (vec common-resource-patterns)
                            (native-driver-resource-dir)))
        body     (str "{\n"
                      "  \"resources\": {\n"
                      "    \"includes\": [\n"
                      (str/join ",\n"
                                (map (fn [pattern]
                                       (str "      {\n"
                                            "        \"pattern\": \"" pattern "\"\n"
                                            "      }"))
                                     patterns))
                      "\n"
                      "    ]\n"
                      "  }\n"
                      "}\n")]
    (io/make-parents path)
    (spit path body)))

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"] :target-dir class-dir})
  (b/compile-clj {:basis @basis :ns-compile '[xia.core] :class-dir class-dir})
  (b/uber {:class-dir class-dir :uber-file uber-file :basis @basis
           :main 'xia.core}))

(defn native-uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"] :target-dir class-dir})
  (write-native-resource-config!)
  (b/compile-clj {:basis @basis :ns-compile '[xia.core] :class-dir class-dir})
  (b/uber {:class-dir class-dir :uber-file uber-file :basis @basis
           :main 'xia.core}))
