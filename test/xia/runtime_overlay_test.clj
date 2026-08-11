(ns xia.runtime-overlay-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.db-schema :as db-schema]
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

(defn- temp-secret-file
  [payload]
  (let [path (str (java.nio.file.Files/createTempFile
                   "xia-overlay-secret"
                   ".txt"
                   (make-array java.nio.file.attribute.FileAttribute 0)))]
    (spit (io/file path) payload)
    path))

(defn- overlay
  [m]
  (merge {:overlay/schema-version 1
          :snapshot/id "snapshot-test"
          :tenant/id "tenant-test"
          :runtime/id "runtime-test"
          :generated-at "2026-04-04T10:15:00Z"
          :config-overrides {}
          :bounded-config {}
          :tx-data []
          :forced-keys #{}}
         m))

(deftest load-file-activates-runtime-overlay
  (let [overlay-file (temp-overlay-file
                      (overlay
                       {:snapshot/id "snapshot-42"
                        :config-overrides {:browser/backend-default :remote}
                        :forced-keys #{:browser/backend-default}
                        :tx-data [{:llm.provider/id :platform-openai
                                   :llm.provider/name "OpenAI (Platform)"
                                   :llm.provider/default? true}]}))]
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
    (is (= 1 (get (runtime-overlay/admin-summary) :source_overlay_schema_version)))
    (is (= db-schema/current-version
           (get (runtime-overlay/admin-summary) :required_db_schema_version)))
    (is (= db-schema/current-version
           (get (runtime-overlay/admin-summary) :current_db_schema_version)))
    (is (= true (get (runtime-overlay/admin-summary) :reloadable)))))

(deftest activate-rejects-legacy-overlay-version-key
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"unknown top-level keys"
       (runtime-overlay/activate!
        {:overlay/version 1
         :snapshot/id "snapshot-legacy"
         :config-overrides {:browser/backend-default :remote}}))))

(deftest load-file-rejects-unsupported-overlay-version
  (let [overlay-file (temp-overlay-file
                      (overlay
                       {:overlay/schema-version 99
                        :snapshot/id "snapshot-invalid"}))]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Unsupported runtime overlay schema version"
         (runtime-overlay/load-file! overlay-file)))))

(deftest reload-reuses-current-source-path-and-updates-overlay
  (let [overlay-file (temp-overlay-file
                      (overlay
                       {:snapshot/id "snapshot-1"
                        :config-overrides {:browser/backend-default :playwright}}))]
    (runtime-overlay/load-file! overlay-file)
    (let [loaded-at-ms (runtime-overlay/loaded-at-ms)]
      (spit (io/file overlay-file)
            (pr-str (overlay
                     {:snapshot/id "snapshot-2"
                      :config-overrides {:browser/backend-default :remote}})))
      (runtime-overlay/reload!)
      (is (= "snapshot-2" (runtime-overlay/snapshot-id)))
      (is (= "remote" (runtime-overlay/config-db-value :browser/backend-default)))
      (is (= overlay-file (runtime-overlay/source-path)))
      (is (<= loaded-at-ms (runtime-overlay/loaded-at-ms)))
      (is (= 2 (get (runtime-overlay/admin-summary) :reload_count))))))

(deftest reload-preserves-previous-overlay-when-updated-file-is-invalid
  (let [overlay-file (temp-overlay-file
                      (overlay
                       {:snapshot/id "snapshot-good"
                        :config-overrides {:browser/backend-default :remote}}))]
    (runtime-overlay/load-file! overlay-file)
    (let [loaded-at-ms (runtime-overlay/loaded-at-ms)]
      (spit (io/file overlay-file)
            (pr-str (overlay
                     {:overlay/schema-version 99
                      :snapshot/id "snapshot-bad"})))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Unsupported runtime overlay schema version"
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
        (overlay
         {:snapshot/id "snapshot-secret-invalid-config"
          :config-overrides {:browser/backend-default {:secret/file "/tmp/nope"}}}))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"only allowed for encrypted attrs"
       (runtime-overlay/activate!
        (overlay
         {:snapshot/id "snapshot-secret-invalid-entity"
          :tx-data [{:llm.provider/id :platform-openai
                     :llm.provider/name {:secret/file "/tmp/nope"}}]})))))

(deftest activate-resolves-secret-file-refs
  (let [secret-file (temp-secret-file "platform-secret\n")]
    (runtime-overlay/activate!
     (overlay
      {:snapshot/id "snapshot-secret-file"
       :config-overrides {:secret/command-channel-token {:secret/file secret-file}}
       :tx-data [{:llm.provider/id :platform-openai
                  :llm.provider/name "OpenAI (Platform)"
                  :llm.provider/api-key {:secret/file secret-file}}]}))
    (is (= "platform-secret"
           (runtime-overlay/config-value :secret/command-channel-token)))
    (is (= "platform-secret"
           (:llm.provider/api-key
            (first (runtime-overlay/entities :provider)))))))

(deftest activate-rejects-legacy-secret-ref-shapes
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"support only :secret/file"
       (runtime-overlay/activate!
        (overlay
         {:snapshot/id "snapshot-legacy-secret-ref"
          :tx-data [{:llm.provider/id :platform-openai
                     :llm.provider/api-key {:secret-file "/tmp/nope"}}]})))))

(deftest activate-rejects-invalid-config-rule-shapes
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"literal override values"
       (runtime-overlay/activate!
        (overlay
         {:snapshot/id "snapshot-bad-merge"
          :config-overrides {:agent/max-turn-llm-calls {:merge :bogus
                                                        :value 10}}}))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"cannot appear in both"
       (runtime-overlay/activate!
        (overlay
         {:snapshot/id "snapshot-duplicate-config"
          :config-overrides {:agent/max-turn-llm-calls 10}
          :bounded-config {:agent/max-turn-llm-calls 8}})))))

(deftest activate-rejects-overlay-attrs-outside-current-db-schema
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"not in the current DB schema"
       (runtime-overlay/activate!
        (overlay
         {:snapshot/id "snapshot-unknown-attr"
          :tx-data [{:llm.provider/id :platform-openai
                     :llm.provider/imaginary-flag true}]})))))

(deftest activate-rejects-overlay-attrs-for-the-wrong-entity-kind
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"not valid for this entity kind"
       (runtime-overlay/activate!
        (overlay
         {:snapshot/id "snapshot-wrong-kind"
          :tx-data [{:service/id :platform-search
                     :llm.provider/name "wrong"}]})))))

(deftest activate-rejects-unknown-top-level-fields
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"unknown top-level keys"
       (runtime-overlay/activate!
        (assoc (overlay {:snapshot/id "snapshot-unknown-top"})
               :overlay/requires-db-schema-version 99)))))
