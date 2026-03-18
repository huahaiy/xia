(ns build
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.build.api :as b]))

(def lib 'com.xia/xia)
(def version "0.1.0")
(def class-dir "target/classes")
(def uber-file "target/xia.jar")
(def test-class-dir "target/test-classes")
(def test-uber-file "target/xia-tests.jar")
(def generated-test-src-dir "target/generated-test-src")
(def basis (delay (b/create-basis {:project "deps.edn"})))
(def compiler-bindings
  {#'*warn-on-reflection* true
   #'*unchecked-math* :warn-on-boxed})

(def ^:private native-resource-config
  "META-INF/native-image/xia/xia/resource-config.json")

(def ^:private common-resource-patterns
 ["oauth_providers.edn"
   "example-tool.edn"
   "example-skill.md"
   "eval/"
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
  ([target-class-dir]
   (write-native-resource-config! target-class-dir []))
  ([target-class-dir extra-patterns]
  (let [path     (str target-class-dir "/" native-resource-config)
        patterns (map resource-pattern
                      (concat common-resource-patterns
                              extra-patterns
                              [(native-driver-resource-dir)]))
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
    (spit path body))))

(defn- test-namespace-symbols
  []
  (->> (file-seq (io/file "test"))
       (filter #(.isFile ^java.io.File %))
       (map #(.getPath ^java.io.File %))
       (filter #(str/ends-with? % "_test.clj"))
       (map #(-> %
                 (str/replace #"^test[\\/]" "")
                 (str/replace #"\.clj$" "")
                 (str/replace #"_" "-")
                 (str/replace #"[\\/]" ".")))
       sort
       (mapv symbol)))

(defn- write-native-test-runner!
  []
  (let [path       (str generated-test-src-dir "/xia/native_test_runner.clj")
        test-nss   (test-namespace-symbols)
        require-ss (->> test-nss
                        (map (fn [ns-sym]
                               (str "[" ns-sym "]")))
                        (str/join "\n            "))]
    (io/make-parents path)
    (spit path
          (str "(ns xia.native-test-runner\n"
               "  (:gen-class)\n"
               "  (:require [clojure.string :as str]\n"
               "            [clojure.test :as t]\n"
               "            " require-ss "))\n\n"
               "(def ^:private all-test-namespaces\n"
               "  '" (pr-str test-nss) ")\n\n"
               "(def ^:private all-test-namespace-set\n"
               "  (set all-test-namespaces))\n\n"
               "(defn- select-test-namespaces\n"
               "  [args]\n"
               "  (if (seq args)\n"
               "    (let [selected (mapv symbol args)\n"
               "          unknown  (remove all-test-namespace-set selected)]\n"
               "      (when (seq unknown)\n"
               "        (binding [*out* *err*]\n"
               "          (println \"Unknown test namespaces:\" (str/join \", \" unknown))\n"
               "          (println \"Available namespaces:\" (str/join \", \" all-test-namespaces)))\n"
               "        (System/exit 2))\n"
               "      selected)\n"
               "    all-test-namespaces))\n\n"
               "(defn -main\n"
               "  [& args]\n"
               "  (let [selected (select-test-namespaces args)\n"
               "        summary  (apply t/run-tests selected)\n"
               "        failures (+ (:fail summary 0) (:error summary 0))]\n"
               "    (shutdown-agents)\n"
               "    (System/exit (if (zero? failures) 0 1))))\n"))))

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"] :target-dir class-dir})
  (b/compile-clj {:basis @basis
                  :ns-compile '[xia.core]
                  :class-dir class-dir
                  :bindings compiler-bindings})
  (b/uber {:class-dir class-dir :uber-file uber-file :basis @basis
           :main 'xia.core}))

(defn native-uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"] :target-dir class-dir})
  (write-native-resource-config! class-dir)
  (b/compile-clj {:basis @basis
                  :ns-compile '[xia.core]
                  :class-dir class-dir
                  :bindings compiler-bindings})
  (b/uber {:class-dir class-dir :uber-file uber-file :basis @basis
           :main 'xia.core}))

(defn native-test-uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources" "test" "dev" "dev-resources"]
               :target-dir test-class-dir})
  (write-native-resource-config! test-class-dir ["fixtures/"])
  (write-native-test-runner!)
  (b/copy-dir {:src-dirs [generated-test-src-dir] :target-dir test-class-dir})
  (b/compile-clj {:basis @basis
                  :ns-compile '[xia.native-test-runner]
                  :class-dir test-class-dir
                  :bindings compiler-bindings})
  (b/uber {:class-dir test-class-dir
           :uber-file test-uber-file
           :basis @basis
           :main 'xia.native-test-runner}))
