(ns xia.remote-bridge-test
  (:require [charred.api :as json]
            [clojure.test :refer :all]
            [xia.db :as db]
            [xia.remote-bridge :as remote-bridge]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.security KeyPairGenerator]
           [java.security.spec NamedParameterSpec]
           [java.util Base64 UUID]))

(use-fixtures :each with-test-db)

(defn- x25519-public-key []
  (let [generator (KeyPairGenerator/getInstance "X25519")]
    (.initialize generator (NamedParameterSpec. "X25519"))
    (-> (Base64/getUrlEncoder)
        (.withoutPadding)
        (.encodeToString (.getEncoded (.getPublic (.generateKeyPair generator)))))))

(defn- pairing-token
  [{:keys [device-id device-name platform topics push-token]
    :or {device-name "Phone"
         platform "ios"}}]
  (json/write-json-str
    {"device_id"   (str (or device-id (random-uuid)))
     "device_name" device-name
     "public_key"  (x25519-public-key)
     "platform"    platform
     "topics"      (or topics ["schedule.failed" "schedule.recovered"])
     "push_token"  push-token}))

(deftest bridge-config-initializes-identity
  (let [bridge (remote-bridge/bridge-config)]
    (is (= :disabled (:connection-state bridge)))
    (is (= false (:enabled? bridge)))
    (is (string? (:instance-id bridge)))
    (is (string? (:public-key bridge)))
    (is (= true (:keypair-ready? bridge)))
    (is (string? (db/get-config :secret/remote-bridge-private-key)))))

(deftest pair-device-stores-paired-device
  (let [device-id (random-uuid)
        device    (remote-bridge/pair-device!
                    (pairing-token {:device-id device-id
                                    :device-name "Hyang iPhone"
                                    :platform "ios"
                                    :topics ["schedule.failed" "service.attention"]
                                    :push-token "push-token-1"}))
        devices   (remote-bridge/list-devices)]
    (is (= device-id (:id device)))
    (is (= "Hyang iPhone" (:name device)))
    (is (= :ios (:platform device)))
    (is (= :paired (:status device)))
    (is (= [:schedule.failed :service.attention] (:topics device)))
    (is (= 1 (count devices)))
    (is (= "Hyang iPhone" (:name (first devices))))))

(deftest record-event-only-when-bridge-enabled
  (remote-bridge/notify-schedule-failure! :daily-brief
                                          {:consecutive-failures 1
                                           :error-message "boom"})
  (is (= [] (remote-bridge/list-events)))
  (remote-bridge/save-bridge-config! {:enabled? true
                                      :instance-label "Desk Xia"
                                      :relay-url "https://relay.example.test"})
  (remote-bridge/notify-schedule-failure! :daily-brief
                                          {:consecutive-failures 1
                                           :error-message "boom"})
  (remote-bridge/notify-schedule-recovered! :daily-brief
                                            {:previous-failures 1})
  (let [events (remote-bridge/list-events 10)
        event-types (set (map :type events))]
    (is (= 2 (count events)))
    (is (contains? event-types :schedule.failed))
    (is (contains? event-types :schedule.recovered))))

(deftest recent-runs-by-status-uses-ordered-limited-query
  (let [query-seen (atom nil)
        now        (java.util.Date.)]
    (with-redefs [db/q (fn [query & _]
                         (reset! query-seen query)
                         [[1 now] [2 now]])
                  db/entity (fn [eid]
                              {:schedule-run/id eid
                               :schedule-run/schedule-id :daily-brief
                               :schedule-run/status :error
                               :schedule-run/started-at now
                               :schedule-run/finished-at now
                               :schedule-run/error "boom"})
                  remote-bridge/schedule-name (constantly "Daily Brief")]
      (let [runs (#'xia.remote-bridge/recent-runs-by-status :error 2)]
        (is (= 2 (count runs)))
        (is (= (#'xia.remote-bridge/recent-runs-query 2)
               @query-seen))))))

(deftest status-snapshot-includes-attention-items
  (remote-bridge/save-bridge-config! {:enabled? true
                                      :instance-label "Desk Xia"})
  (db/register-oauth-account! {:id            :github
                               :name          "GitHub"
                               :authorize-url "https://github.com/login/oauth/authorize"
                               :token-url     "https://github.com/login/oauth/access_token"
                               :client-id     "client-id"
                               :client-secret "client-secret"
                               :autonomous-approved? true})
  (db/register-service! {:id          :github
                         :name        "GitHub API"
                         :base-url    "https://api.github.com"
                         :auth-type   :oauth-account
                         :oauth-account :github
                         :enabled?    true})
  (db/register-service! {:id          :slack
                         :name        "Slack API"
                         :base-url    "https://slack.com/api"
                         :auth-type   :api-key-header
                         :enabled?    true})
  (let [snapshot (remote-bridge/status-snapshot)
        titles   (set (map :title (:attention snapshot)))]
    (is (= "Desk Xia" (get-in snapshot [:instance :label])))
    (is (= true (get-in snapshot [:connectivity :enabled?])))
    (is (contains? titles "OAuth account needs attention: GitHub"))
    (is (contains? titles "Service needs attention: GitHub API"))
    (is (contains? titles "Service needs attention: Slack API"))))
