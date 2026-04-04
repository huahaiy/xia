(ns xia.runtime-overlay-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.runtime-overlay :as runtime-overlay]))

(use-fixtures :each
  (fn [f]
    (runtime-overlay/clear!)
    (try
      (f)
      (finally
        (runtime-overlay/clear!)))))

(defn- temp-overlay-file
  [payload]
  (let [path (str (java.nio.file.Files/createTempFile
                    "xia-overlay"
                    ".edn"
                    (make-array java.nio.file.attribute.FileAttribute 0)))]
    (spit (io/file path) (pr-str payload))
    path))

(deftest load-file-activates-runtime-overlay
  (let [overlay-file (temp-overlay-file
                       {:overlay/version 1
                        :snapshot/id "snapshot-42"
                        :config-overrides {:browser/backend-default :remote}
                        :forced-keys #{:browser/backend-default}
                        :tx-data [{:llm.provider/id :platform-openai
                                   :llm.provider/name "OpenAI (Platform)"
                                   :llm.provider/default? true}]})]
    (runtime-overlay/load-file! overlay-file)
    (is (= "snapshot-42" (runtime-overlay/snapshot-id)))
    (is (= "remote" (runtime-overlay/config-db-value :browser/backend-default)))
    (is (= :platform-openai (runtime-overlay/provider-default-id)))
    (is (= "OpenAI (Platform)"
           (get-in (runtime-overlay/entity :provider :platform-openai)
                   [:llm.provider/name])))
    (is (= 1 (runtime-overlay/overlay-version)))
    (is (= overlay-file (runtime-overlay/source-path)))
    (is (number? (runtime-overlay/loaded-at-ms)))
    (is (= 1 (get (runtime-overlay/admin-summary) :source_overlay_version)))
    (is (= true (get (runtime-overlay/admin-summary) :reloadable)))))

(deftest activate-migrates-versionless-legacy-overlay
  (runtime-overlay/activate!
    {:snapshot/id "snapshot-legacy"
     :config-overrides {:browser/backend-default :remote}})
  (is (= "snapshot-legacy" (runtime-overlay/snapshot-id)))
  (is (= 1 (runtime-overlay/overlay-version)))
  (is (= "remote" (runtime-overlay/config-db-value :browser/backend-default)))
  (is (= 0 (get (runtime-overlay/admin-summary) :source_overlay_version))))

(deftest load-file-rejects-unsupported-overlay-version
  (let [overlay-file (temp-overlay-file
                       {:overlay/version 99
                        :snapshot/id "snapshot-invalid"})]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Unsupported runtime overlay version"
          (runtime-overlay/load-file! overlay-file)))))

(deftest reload-reuses-current-source-path-and-updates-overlay
  (let [overlay-file (temp-overlay-file
                       {:overlay/version 1
                        :snapshot/id "snapshot-1"
                        :config-overrides {:browser/backend-default :playwright}})]
    (runtime-overlay/load-file! overlay-file)
    (let [loaded-at-ms (runtime-overlay/loaded-at-ms)]
      (spit (io/file overlay-file)
            (pr-str {:overlay/version 1
                     :snapshot/id "snapshot-2"
                     :config-overrides {:browser/backend-default :remote}}))
      (runtime-overlay/reload!)
      (is (= "snapshot-2" (runtime-overlay/snapshot-id)))
      (is (= "remote" (runtime-overlay/config-db-value :browser/backend-default)))
      (is (= overlay-file (runtime-overlay/source-path)))
      (is (<= loaded-at-ms (runtime-overlay/loaded-at-ms)))
      (is (= 2 (get (runtime-overlay/admin-summary) :reload_count))))))

(deftest reload-preserves-previous-overlay-when-updated-file-is-invalid
  (let [overlay-file (temp-overlay-file
                       {:overlay/version 1
                        :snapshot/id "snapshot-good"
                        :config-overrides {:browser/backend-default :remote}})]
    (runtime-overlay/load-file! overlay-file)
    (let [loaded-at-ms (runtime-overlay/loaded-at-ms)]
      (spit (io/file overlay-file)
            (pr-str {:overlay/version 99
                     :snapshot/id "snapshot-bad"}))
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"Unsupported runtime overlay version"
            (runtime-overlay/reload!)))
      (is (= "snapshot-good" (runtime-overlay/snapshot-id)))
      (is (= "remote" (runtime-overlay/config-db-value :browser/backend-default)))
      (is (= overlay-file (runtime-overlay/source-path)))
      (is (= loaded-at-ms (runtime-overlay/loaded-at-ms)))
      (is (= 1 (get (runtime-overlay/admin-summary) :reload_count))))))

(deftest activate-rejects-secret-refs-on-nonsecret-fields
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"only allowed for secret config keys"
        (runtime-overlay/activate!
          {:overlay/version 1
           :snapshot/id "snapshot-secret-invalid-config"
           :config-overrides {:browser/backend-default {:secret-env "XIA_BAD_SECRET"}}})))
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"only allowed for encrypted attrs"
        (runtime-overlay/activate!
          {:overlay/version 1
           :snapshot/id "snapshot-secret-invalid-entity"
           :tx-data [{:llm.provider/id :platform-openai
                      :llm.provider/name {:secret-env "XIA_BAD_SECRET"}}]}))))

(deftest activate-rejects-invalid-config-rule-shapes
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"unsupported merge mode"
        (runtime-overlay/activate!
          {:overlay/version 1
           :snapshot/id "snapshot-bad-merge"
           :config-overrides {:agent/max-turn-llm-calls {:merge :bogus
                                                         :value 10}}})))
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"require a :value"
        (runtime-overlay/activate!
          {:overlay/version 1
           :snapshot/id "snapshot-missing-rule-value"
           :config-overrides {:agent/max-turn-llm-calls {:merge :cap}}}))))
