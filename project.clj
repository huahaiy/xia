(require '[clojure.edn :as edn]
         '[clojure.java.io :as io])

(def xia-dependency-catalog
  (-> (io/file (.getParentFile (io/file *file*)) "build/dependencies.edn")
      slurp
      edn/read-string))

(defproject xia "0.1.0-SNAPSHOT"
  :description "Secure and portable personal AI assistant for online work."
  :url "https://github.com/huahaiy/xia"
  :license {:name "Apache-2.0"
            :url  "https://www.apache.org/licenses/LICENSE-2.0"}
  :min-lein-version "2.11.2"
  :dependencies ~(:dependencies xia-dependency-catalog)
  :source-paths ["src"]
  :resource-paths ["resources"]
  :test-paths ["test"]
  :main xia.core
  :cljfmt {:paths ["src" "test" "project.clj"]
           :parallel? true}
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies ~(:dev-dependencies xia-dependency-catalog)
                   :repl-options {:init-ns user}}
             :quality {:plugins [[dev.weavejester/lein-cljfmt "0.13.1"]
                                 [jonase/eastwood "1.4.3"]]}
             :coverage {:plugins [[lein-cloverage "1.2.4"]]}
             :uberjar {:aot :all}
             :release {:source-paths ^:replace ["src"]
                       :resource-paths ^:replace ["resources"]
                       :main xia.core
                       :omit-source true
                       :uberjar-name "xia.jar"}
             :native-test {:source-paths ^:replace ["src" "test"]
                           :resource-paths ^:replace ["resources"]
                           :main xia.test-runner
                           :uberjar-name "xia-tests.jar"}})
