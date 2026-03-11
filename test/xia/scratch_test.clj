(ns xia.scratch-test
  (:require [clojure.test :refer :all]
            [datalevin.core :as d]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.scratch :as scratch]
            [xia.test-helpers :refer [with-test-db]])
  (:import [datalevin.db DB]
           [datalevin.storage Store]))

(use-fixtures :each with-test-db)

(defn- scratch-lmdb []
  (let [db-value (d/db (db/conn))]
    (.-lmdb ^Store (.-store ^DB db-value))))

(defn- raw-pad [pad-id]
  (d/open-dbi (scratch-lmdb) "xia/scratch" {:key-size 128})
  (d/get-value (scratch-lmdb) "xia/scratch" pad-id :string :data))

(deftest scratch-pads-are-encrypted-at-rest
  (let [sid (str (db/create-session! :http))
        pad (scratch/create-pad! {:scope :session
                                  :session-id sid
                                  :title "Draft"
                                  :content "top secret"})]
    (let [raw (raw-pad (:id pad))]
      (is (crypto/encrypted? (:title raw)))
      (is (crypto/encrypted? (:content raw))))
    (is (= "Draft" (:title (scratch/get-pad (:id pad)))))
    (is (= "top secret" (:content (scratch/get-pad (:id pad)))))))

(deftest scratch-pad-edit-operations-work
  (let [sid     (str (db/create-session! :http))
        created (scratch/create-pad! {:scope :session
                                      :session-id sid
                                      :title "Draft"
                                      :content "alpha\nbeta\ngamma"})
        pad-id  (:id created)
        step-1  (scratch/edit-pad! pad-id {:op :append :separator "\n" :text "delta"})
        step-2  (scratch/edit-pad! pad-id {:op :replace-string
                                           :match "beta"
                                           :replacement "BETA"
                                           :expected-version (:version step-1)})
        step-3  (scratch/edit-pad! pad-id {:op :replace-lines
                                           :start-line 3
                                           :end-line 4
                                           :text "third\nfourth"
                                           :expected-version (:version step-2)})
        step-4  (scratch/edit-pad! pad-id {:op :insert-at
                                           :offset 0
                                           :text "> "
                                           :expected-version (:version step-3)})]
    (is (= "> alpha\nBETA\nthird\nfourth" (:content step-4)))
    (is (= 5 (:version step-4)))))

(deftest scratch-pad-save-detects-version-conflicts
  (let [sid (str (db/create-session! :http))
        pad (scratch/create-pad! {:scope :session
                                  :session-id sid
                                  :content "first"})]
    (scratch/save-pad! (:id pad) {:content "second" :expected-version 1})
    (try
      (scratch/save-pad! (:id pad) {:content "third" :expected-version 1})
      (is false "Expected version conflict")
      (catch clojure.lang.ExceptionInfo e
        (is (= :scratch/version-conflict (:type (ex-data e))))
        (is (= 1 (:expected-version (ex-data e))))
        (is (= 2 (:actual-version (ex-data e))))))))

(deftest scratch-pad-listing-is-session-scoped
  (let [sid-a (str (db/create-session! :http))
        sid-b (str (db/create-session! :http))]
    (scratch/create-pad! {:scope :session :session-id sid-a :title "A" :content "one"})
    (scratch/create-pad! {:scope :session :session-id sid-b :title "B" :content "two"})
    (is (= ["A"] (mapv :title (scratch/list-pads {:scope :session :session-id sid-a}))))
    (is (= ["B"] (mapv :title (scratch/list-pads {:scope :session :session-id sid-b}))))))
