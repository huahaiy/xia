(ns xia.config-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

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

(deftest numeric-config-readers-apply-bounded-config-cap
  (db/set-config! :agent/max-turn-llm-calls 25)
  (runtime-overlay/activate!
   (overlay
    {:snapshot/id "snapshot-config-rules"
     :bounded-config {:agent/max-turn-llm-calls 10}}))
  (is (= 10 (cfg/positive-long :agent/max-turn-llm-calls 99)))
  (is (= 10 (get-in (cfg/positive-long-resolution :agent/max-turn-llm-calls 99)
                    [:raw :overlay]))))

(deftest numeric-config-rules-apply-against-defaults-when-tenant-value-is-absent
  (runtime-overlay/activate!
   (overlay
    {:snapshot/id "snapshot-config-default-rules"
     :bounded-config {:agent/max-task-llm-calls 8}}))
  (is (= 8 (cfg/positive-long :agent/max-task-llm-calls 20)))
  (is (= :runtime-overlay
         (:source (cfg/positive-long-resolution :agent/max-task-llm-calls 20)))))

(deftest nonreplace-rules-are-rejected-by-string-readers
  (runtime-overlay/activate!
   (overlay
    {:snapshot/id "snapshot-config-string-rule"
     :bounded-config {:browser/remote-base-url "https://browser.example"}}))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"not supported"
       (cfg/string-option :browser/remote-base-url nil))))

(deftest keyword-readers-accept-literal-keyword-overlay-values
  (runtime-overlay/activate!
   (overlay
    {:snapshot/id "snapshot-config-keyword-rule"
     :config-overrides {:browser/backend-default :remote}}))
  (is (= :remote
         (cfg/keyword-option :browser/backend-default
                             :auto
                             #{:auto :remote :playwright}))))

(deftest config-readers-fall-back-to-explicit-get-config-stubs-when-db-is-not-connected
  (with-redefs [db/tenant-config-value (fn [_]
                                         (throw (ex-info "Database not connected. Call (xia.db/connect!) first." {})))
                db/get-config (constantly "true")]
    (is (true? (cfg/boolean-option :messaging/slack-enabled? false)))))
