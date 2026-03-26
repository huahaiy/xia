(ns xia.peer-test
  (:require [clojure.test :refer :all]
            [xia.peer :as peer]
            [xia.prompt :as prompt]
            [xia.db :as db]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(deftest list-peers-returns-enabled-bearer-services
  (db/register-service! {:id          :ops-peer
                         :name        "Ops Xia"
                         :base-url    "http://127.0.0.1:4011"
                         :auth-type   :bearer
                         :auth-key    "tok"
                         :allow-private-network? true})
  (db/register-service! {:id          :public-api
                         :name        "Public API"
                         :base-url    "https://api.example.com"
                         :auth-type   :bearer
                         :auth-key    "tok"})
  (db/register-service! {:id            :oauth-only
                         :name          "OAuth Only"
                         :base-url      "https://example.com"
                         :auth-type     :oauth-account
                         :oauth-account :missing})
  (db/enable-service! :public-api false)
  (is (= [{:service_id "ops-peer"
           :name "Ops Xia"
           :base_url "http://127.0.0.1:4011"
           :allow_private_network true
           :local true
           :autonomous_approved true}]
         (peer/list-peers))))

(deftest list-peers-filters-unapproved-services-during-autonomous-runs
  (db/register-service! {:id                   :ops-peer
                         :name                 "Ops Xia"
                         :base-url             "https://ops.example"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :autonomous-approved? false})
  (db/register-service! {:id                   :research-peer
                         :name                 "Research Xia"
                         :base-url             "https://research.example"
                         :auth-type            :bearer
                         :auth-key             "tok"
                         :autonomous-approved? true})
  (binding [prompt/*interaction-context* {:channel          :scheduler
                                          :autonomous-run?  true
                                          :approval-bypass? true}]
    (is (= ["research-peer"]
           (mapv :service_id (peer/list-peers))))))

(deftest chat-sends-command-chat-request-through-service-proxy
  (let [calls (atom [])]
    (with-redefs [xia.service/request
                  (fn [service-id method path & opts]
                    (swap! calls conj {:service-id service-id
                                       :method method
                                       :path path
                                       :opts (apply hash-map opts)})
                    {:status 200
                     :body {"session_id" "peer-session"
                            "role" "assistant"
                            "content" "Peer reply"}})]
      (is (= {:service_id "ops-peer"
              :session_id "peer-session"
              :role "assistant"
              :content "Peer reply"}
             (peer/chat "ops-peer"
                        "Summarize the deploy"
                        :session-id "existing-peer-session"
                        :timeout-ms 45000)))
      (is (= [{:service-id :ops-peer
               :method :post
               :path "/command/chat"
               :opts {:body {"message" "Summarize the deploy"
                             "session_id" "existing-peer-session"}
                      :as :json
                      :timeout 45000}}]
             @calls)))))

(deftest chat-raises-structured-error-on-peer-failure
  (with-redefs [xia.service/request (fn [_service-id _method _path & _]
                                      {:status 409
                                       :body {"error" "session closed"}})]
    (let [ex (is (thrown? clojure.lang.ExceptionInfo
                          (peer/chat :ops-peer "Continue work")))]
      (is (= 409 (:status (ex-data ex))))
      (is (= :ops-peer (:service_id (ex-data ex))))
      (is (re-find #"session closed" (.getMessage ^Throwable ex))))))
