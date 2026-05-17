(ns xia.permission-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.permission :as permission]
            [xia.prompt :as prompt]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(defn- test-tool
  [approval]
  {:tool/id :guarded-tool
   :tool/name "Guarded tool"
   :tool/description "Needs a permission decision"
   :tool/approval approval
   :tool/handler "(fn [_] {\"status\" \"ok\"})"})

(defn- events-of-type
  [events type]
  (filterv #(= type (:type %)) events))

(deftest bridge-approval-callback-is-audited-and-cached-for-session
  (let [session-id (db/create-session! :terminal)
        calls      (atom [])
        hooks      (atom [])
        context    {:channel :ide
                    :session-id session-id
                    :task-runtime/on-approval-request
                    (fn [req]
                      (swap! hooks conj [:request (:tool-id req)]))
                    :task-runtime/on-approval-decision
                    (fn [req]
                      (swap! hooks conj [:decision
                                         (:tool-id req)
                                         (:approved? req)]))}
        context*   (assoc context
                          :permission/approval-callback
                          (fn [req]
                            (swap! calls conj
                                   (select-keys req
                                                [:channel
                                                 :session-id
                                                 :tool-id
                                                 :arguments]))
                            true))
        tool       (test-tool :session)]
    (binding [prompt/*interaction-context* context*]
      (is (= {:allowed? true
              :policy :session
              :mode :interactive}
             (select-keys (permission/authorize-tool! tool {"x" 1} context*)
                          [:allowed? :policy :mode])))
      (is (= {:allowed? true
              :policy :session
              :mode :session-cached}
             (select-keys (permission/authorize-tool! tool {"x" 2} context*)
                          [:allowed? :policy :mode]))))
    (is (= [{:channel :ide
             :session-id session-id
             :tool-id :guarded-tool
             :arguments {"x" 1}}]
           @calls))
    (is (= [[:request :guarded-tool]
            [:decision :guarded-tool true]]
           @hooks))
    (let [events             (db/session-audit-events session-id)
          approval-requests  (events-of-type events :approval-request)
          approval-decisions (events-of-type events :approval-decision)
          policy-decisions   (events-of-type events :policy-decision)]
      (is (= 1 (count approval-requests)))
      (is (= 1 (count approval-decisions)))
      (is (= {"tool-name" "Guarded tool"
              "approved" true
              "policy" "session"}
             (:data (first approval-decisions))))
      (is (= ["approval-policy" "execution-policy"
              "approval-policy" "execution-policy"]
             (mapv #(get-in % [:data "decision-type"]) policy-decisions))))))

(deftest always-policy-approval-is-not-session-cached
  (let [session-id (db/create-session! :terminal)
        calls      (atom 0)
        context    {:channel :ide
                    :session-id session-id
                    :permission/approval-callback
                    (fn [_]
                      (swap! calls inc)
                      true)}
        tool       (test-tool :always)]
    (binding [prompt/*interaction-context* context]
      (is (= :interactive
             (:mode (permission/authorize-tool! tool {} context))))
      (is (= :interactive
             (:mode (permission/authorize-tool! tool {} context)))))
    (is (= 2 @calls))))

(deftest channel-blocking-happens-before-approval-callback
  (let [session-id (db/create-session! :http)
        calls      (atom 0)
        context    {:channel :http
                    :session-id session-id
                    :permission/approval-callback
                    (fn [_]
                      (swap! calls inc)
                      true)}
        tool       {:tool/id :browser-login-interactive
                    :tool/name "Browser login"
                    :tool/description "Interactive login"
                    :tool/approval :session
                    :tool/handler "(fn [_] {})"}
        decision   (binding [prompt/*interaction-context* context]
                     (permission/authorize-tool! tool {} context))]
    (is (= {:allowed? false
            :policy :channel
            :mode :channel-blocked}
           (select-keys decision [:allowed? :policy :mode])))
    (is (= "interactive login is only available in terminal sessions"
           (:error decision)))
    (is (zero? @calls))))
