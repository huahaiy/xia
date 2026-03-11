(ns xia.sci-env-test
  (:require [clojure.test :refer :all]
            [xia.schedule :as schedule]
            [xia.sci-env :as sci-env]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(deftest system-class-is-not-exposed-to-sci
  (is (thrown? Exception
               (sci-env/eval-string "(System/getenv \"PATH\")"))))

(deftest schedule-history-is-redacted-in-sci
  (schedule/create-schedule!
    {:id :sandbox-history
     :spec {:minute #{0} :hour #{9}}
     :type :tool
     :tool-id :x})
  (schedule/record-run! :sandbox-history
    {:started-at  (java.util.Date.)
     :finished-at (java.util.Date.)
     :status      :error
     :result      "{\"secret\":true}"
     :error       "sensitive failure"})
  (let [history (sci-env/eval-string "(xia.schedule/schedule-history :sandbox-history 1)")
        run     (first history)]
    (is (= 1 (count history)))
    (is (= :error (:status run)))
    (is (not (contains? run :result)))
    (is (not (contains? run :error)))))
