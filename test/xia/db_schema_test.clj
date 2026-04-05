(ns xia.db-schema-test
  (:require [clojure.test :refer [deftest is]]
            [xia.db-schema :as db-schema]))

(deftest frozen-schema-discipline-freezes-v1
  (is (= [1] (db-schema/frozen-schema-versions)))
  (is (= 1 (db-schema/released-schema-version)))
  (is (true? (db-schema/schema-frozen? 1)))
  (is (true? (db-schema/ensure-frozen-schema-integrity!))))

(deftest current-schema-checks-frozen-integrity
  (let [called? (atom false)]
    (with-redefs [xia.db-schema/ensure-frozen-schema-integrity!
                  (fn []
                    (reset! called? true)
                    true)]
      (is (map? (db-schema/current-schema)))
      (is (true? @called?)))))

(deftest ensure-frozen-schema-integrity-rejects-modified-frozen-schema
  (let [ex (try
             (with-redefs [xia.db-schema/schema-digest
                           (fn [version]
                             (if (= version 1)
                               "0000000000000000000000000000000000000000000000000000000000000000"
                               (throw (ex-info "unexpected version" {:version version}))))]
               (db-schema/ensure-frozen-schema-integrity!)
               nil)
             (catch clojure.lang.ExceptionInfo ex
               ex))]
    (is (instance? clojure.lang.ExceptionInfo ex))
    (is (= :db-schema/frozen-schema-modified
           (:reason (ex-data ex))))
    (is (= 1 (:version (ex-data ex))))
    (is (= 1 (:released-schema-version (ex-data ex))))
    (is (= db-schema/current-version
           (:current-schema-version (ex-data ex))))
    (is (re-find #"Bump the schema version instead of editing a frozen schema"
                 (.getMessage ex)))))
