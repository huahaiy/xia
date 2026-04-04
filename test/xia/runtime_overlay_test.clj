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

(deftest load-file-activates-runtime-overlay
  (let [overlay-file (doto (io/file (str (java.nio.file.Files/createTempFile
                                           "xia-overlay"
                                           ".edn"
                                           (make-array java.nio.file.attribute.FileAttribute 0))))
                       (spit (pr-str
                               {:overlay/version 1
                                :snapshot/id "snapshot-42"
                                :config-overrides {:browser/backend-default :remote}
                                :forced-keys #{:browser/backend-default}
                                :tx-data [{:llm.provider/id :platform-openai
                                           :llm.provider/name "OpenAI (Platform)"
                                           :llm.provider/default? true}]})))]
    (runtime-overlay/load-file! (.getAbsolutePath overlay-file))
    (is (= "snapshot-42" (runtime-overlay/snapshot-id)))
    (is (= "remote" (runtime-overlay/config-db-value :browser/backend-default)))
    (is (= :platform-openai (runtime-overlay/provider-default-id)))
    (is (= "OpenAI (Platform)"
           (get-in (runtime-overlay/entity :provider :platform-openai)
                   [:llm.provider/name])))
    (is (= 1 (runtime-overlay/overlay-version)))
    (is (= 1 (get (runtime-overlay/admin-summary) :source_overlay_version)))))

(deftest activate-migrates-versionless-legacy-overlay
  (runtime-overlay/activate!
    {:snapshot/id "snapshot-legacy"
     :config-overrides {:browser/backend-default :remote}})
  (is (= "snapshot-legacy" (runtime-overlay/snapshot-id)))
  (is (= 1 (runtime-overlay/overlay-version)))
  (is (= "remote" (runtime-overlay/config-db-value :browser/backend-default)))
  (is (= 0 (get (runtime-overlay/admin-summary) :source_overlay_version))))

(deftest load-file-rejects-unsupported-overlay-version
  (let [overlay-file (doto (io/file (str (java.nio.file.Files/createTempFile
                                           "xia-overlay-invalid"
                                           ".edn"
                                           (make-array java.nio.file.attribute.FileAttribute 0))))
                       (spit (pr-str
                               {:overlay/version 99
                                :snapshot/id "snapshot-invalid"})))]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Unsupported runtime overlay version"
          (runtime-overlay/load-file! (.getAbsolutePath overlay-file))))))

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
