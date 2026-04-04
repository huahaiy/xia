(ns xia.config-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest numeric-config-readers-apply-overlay-cap-and-floor
  (db/set-config! :agent/max-turn-llm-calls 25)
  (db/set-config! :memory/knowledge-decay-min-confidence 0.2)
  (runtime-overlay/activate!
    {:overlay/version 1
     :snapshot/id "snapshot-config-rules"
     :config-overrides {:agent/max-turn-llm-calls {:merge :cap :value 10}
                        :memory/knowledge-decay-min-confidence {:merge :floor :value 0.4}}})
  (is (= 10 (cfg/positive-long :agent/max-turn-llm-calls 99)))
  (is (= 0.4 (cfg/bounded-double :memory/knowledge-decay-min-confidence 0.1))))

(deftest numeric-config-rules-apply-against-defaults-when-tenant-value-is-absent
  (runtime-overlay/activate!
    {:overlay/version 1
     :snapshot/id "snapshot-config-default-rules"
     :config-overrides {:agent/max-task-llm-calls {:merge :cap :value 8}
                        :agent/supervisor-semantic-loop-threshold {:merge :floor :value 0.85}}})
  (is (= 8 (cfg/positive-long :agent/max-task-llm-calls 20)))
  (is (= 0.85 (cfg/positive-double :agent/supervisor-semantic-loop-threshold 0.5))))

(deftest nonreplace-rules-are-rejected-by-string-readers
  (runtime-overlay/activate!
    {:overlay/version 1
     :snapshot/id "snapshot-config-string-rule"
     :config-overrides {:browser/remote-base-url {:merge :cap
                                                  :value "https://browser.example"}}})
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"not supported"
        (cfg/string-option :browser/remote-base-url nil))))
