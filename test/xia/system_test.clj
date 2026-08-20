(ns xia.system-test
  (:require [clojure.test :refer [deftest is]]
            [integrant.core :as ig]
            [xia.async]
            [xia.db]
            [xia.runtime-context :as runtime-context]
            [xia.system]))

(deftest database-halt-closes-and-clears-its-runtime-once
  (let [calls     (atom 0)
        runtime   (xia.db/make-runtime)
        component {:runtime runtime
                   :runtime-context
                   (runtime-context/make {:xia/db {:runtime runtime}})}]
    (with-redefs [xia.db/clear-runtime! #(swap! calls inc)]
      (ig/halt-key! :xia/db component))
    (is (= 1 @calls))))

(deftest runtime-support-exposes-explicit-runtime-context
  (let [db-runtime    {:runtime-name :db}
        async-runtime {:runtime-name :async}
        support       (ig/init-key
                       :xia/runtime-support
                       {:db {:runtime db-runtime
                             :db-path "test.db"}
                        :overlay {:snapshot-id 1}
                        :async-runtime {:runtime async-runtime}})]
    (is (= db-runtime
           (runtime-context/runtime support :xia/db)))
    (is (= async-runtime
           (runtime-context/runtime support :xia/async-runtime)))
    (is (= {:runtime async-runtime}
           (runtime-context/component support :xia/async-runtime)))))

(deftest runtime-context-merge-preserves-outer-keys-and-prefers-later-contexts
  (let [outer-db      {:runtime-name :outer-db}
        inner-db      {:runtime-name :inner-db}
        inner-http    {:runtime-name :inner-http}
        outer-context (runtime-context/make
                       {:xia/db {:runtime outer-db}})
        inner-context (runtime-context/make
                       {:xia/db {:runtime inner-db}
                        :xia/http-runtime {:runtime inner-http}})
        merged        (runtime-context/merge-contexts outer-context inner-context)]
    (is (= inner-db
           (runtime-context/runtime merged :xia/db)))
    (is (= inner-http
           (runtime-context/runtime merged :xia/http-runtime)))
    (is (nil? (runtime-context/merge-contexts nil nil)))))

(deftest async-runtime-uses-bound-runtime-context
  (xia.async/clear-runtime!)
  (let [scoped-runtime (xia.async/make-runtime)
        context        (runtime-context/make
                        {:xia/async-runtime {:runtime scoped-runtime}})]
    (try
      (runtime-context/with-runtime-context
        context
        #(let [future (xia.async/submit-background! "scoped-runtime-test" (fn [] :scoped))]
           (is (= :scoped (deref future 1000 ::timeout)))
           (is (contains? @(:executors-atom scoped-runtime) :background))))
      (finally
        (runtime-context/with-runtime-context context #(xia.async/clear-runtime!))))))
