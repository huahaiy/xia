(defproject xia "0.1.0-SNAPSHOT"
  :description "Secure and portable personal AI assistant for online work."
  :url "https://github.com/huahaiy/xia"
  :license {:name "Apache-2.0"
            :url  "https://www.apache.org/licenses/LICENSE-2.0"}
  :min-lein-version "2.11.2"
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [integrant/integrant "1.0.1"]
                 [org.datalevin/datalevin-embedded "1.0.0"]
                 [org.clojure/tools.cli "1.4.256"]
                 [com.cnuernber/charred "1.041"]
                 [org.babashka/sci "0.15.56"]
                 [org.jsoup/jsoup "1.23.1"]
                 [org.eclipse.angus/jakarta.mail "2.0.5"]
                 [com.github.librepdf/openpdf "3.0.5"
                  :exclusions [com.ibm.icu/icu4j
                               org.bouncycastle/bcprov-jdk18on
                               org.bouncycastle/bcpkix-jdk18on
                               org.apache.xmlgraphics/fop]]
                 [com.microsoft.playwright/playwright "1.62.0"]
                 [http-kit/http-kit "2.8.1"]
                 [ring/ring-core "1.15.5"]
                 [com.taoensso/timbre "6.8.0"]
                 [com.taoensso/encore "3.171.1"]
                 [com.taoensso/truss "2.5.1"]]
  :source-paths ["src"]
  :resource-paths ["resources"]
  :test-paths ["test"]
  :main xia.core
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[nrepl/nrepl "1.7.0"]
                                  [org.clojure/tools.namespace "1.5.1"]]
                   :repl-options {:init-ns user}}
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
