(defproject xia "0.1.0-SNAPSHOT"
  :description "Secure and portable personal AI assistant for online work."
  :url "https://github.com/huahaiy/xia"
  :license {:name "Apache-2.0"
            :url  "https://www.apache.org/licenses/LICENSE-2.0"}
  :min-lein-version "2.11.2"
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [integrant/integrant "0.13.1"]
                 [org.datalevin/datalevin-embedded "0.10.15"]
                 [org.clojure/tools.cli "1.4.256"]
                 [com.cnuernber/charred "1.038"]
                 [org.babashka/sci "0.12.51"]
                 [org.jsoup/jsoup "1.22.1"]
                 [org.eclipse.angus/jakarta.mail "2.0.5"]
                 [com.github.librepdf/openpdf "3.0.0"
                  :exclusions [com.ibm.icu/icu4j
                               org.bouncycastle/bcprov-jdk18on
                               org.bouncycastle/bcpkix-jdk18on
                               org.apache.xmlgraphics/fop]]
                 [com.microsoft.playwright/playwright "1.52.0"]
                 [http-kit/http-kit "2.8.1"]
                 [ring/ring-core "1.12.2"]
                 [com.taoensso/timbre "6.5.0"]
                 [com.taoensso/encore "3.158.0"]
                 [com.taoensso/truss "2.2.0"]]
  :source-paths ["src"]
  :resource-paths ["resources"]
  :test-paths ["test"]
  :main xia.core
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[nrepl/nrepl "1.3.1"]
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
