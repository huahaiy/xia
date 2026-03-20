(ns xia.oauth-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.oauth :as oauth]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.util Date]))

(use-fixtures :each with-test-db)

(defn- oauth-account
  [id & {:keys [autonomous-approved? access-token refresh-token expires-at]}]
  {:id                     id
   :name                   (name id)
   :authorize-url          "https://example.com/oauth/authorize"
   :token-url              "https://example.com/oauth/token"
   :client-id              (str (name id) "-client")
   :client-secret          "secret"
   :autonomous-approved?   autonomous-approved?
   :access-token           access-token
   :refresh-token          refresh-token
   :token-type             "Bearer"
   :expires-at             expires-at
   :connected-at           (Date.)})

(deftest refresh-autonomous-accounts-refreshes-only-expiring-approved-accounts
  (let [now       (System/currentTimeMillis)
        refreshed (atom [])]
    (db/register-oauth-account!
      (oauth-account :approved-expiring
                     :autonomous-approved? true
                     :access-token "old-access"
                     :refresh-token "refresh-a"
                     :expires-at (Date. (- now 1000))))
    (db/register-oauth-account!
      (oauth-account :approved-fresh
                     :autonomous-approved? true
                     :access-token "fresh-access"
                     :refresh-token "refresh-b"
                     :expires-at (Date. (+ now 3600000))))
    (db/register-oauth-account!
      (oauth-account :unapproved-expiring
                     :autonomous-approved? false
                     :access-token "old-access"
                     :refresh-token "refresh-c"
                     :expires-at (Date. (- now 1000))))
    (db/register-oauth-account!
      (oauth-account :approved-without-refresh
                     :autonomous-approved? true
                     :access-token "old-access"
                     :refresh-token nil
                     :expires-at (Date. (- now 1000))))
    (with-redefs [xia.oauth/refresh-account! (fn [account-id]
                                               (swap! refreshed conj account-id)
                                               (db/get-oauth-account account-id))]
      (let [result (oauth/refresh-autonomous-accounts! {:force? true})]
        (is (= :ok (:status result)))
        (is (= 1 (:checked result)))
        (is (= [:approved-expiring] (:refreshed result)))
        (is (empty? (:errors result)))
        (is (= [:approved-expiring] @refreshed))))))

(deftest refresh-autonomous-accounts-records-per-account-failures
  (let [now (System/currentTimeMillis)]
    (db/register-oauth-account!
      (oauth-account :approved-expiring
                     :autonomous-approved? true
                     :access-token "old-access"
                     :refresh-token "refresh-a"
                     :expires-at (Date. (- now 1000))))
    (with-redefs [xia.oauth/refresh-account! (fn [_account-id]
                                               (throw (ex-info "refresh failed" {:type :test})))]
      (let [result (oauth/refresh-autonomous-accounts! {:force? true})]
        (is (= :error (:status result)))
        (is (= 1 (:checked result)))
        (is (empty? (:refreshed result)))
        (is (= [{:account-id :approved-expiring
                 :error "refresh failed"}]
               (:errors result)))))))
