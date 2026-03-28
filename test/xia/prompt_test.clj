(ns xia.prompt-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(deftest prompt-and-approval-decisions-are-audited
  (let [session-id (db/create-session! :terminal)]
    (prompt/register-prompt! :terminal (fn [_ & _] "hunter2"))
    (prompt/register-approval! :terminal (fn [_] true))
    (try
      (binding [prompt/*interaction-context* {:channel :terminal
                                              :session-id session-id}]
        (is (= "hunter2" (prompt/prompt! "Password" :mask? true)))
        (is (true? (prompt/approve! {:tool-id :browser-open
                                     :tool-name "browser-open"
                                     :arguments {:url "https://example.com"}
                                     :policy :session}))))
      (let [events (db/session-audit-events session-id)]
        (is (= [:input-request :input-response :approval-request :approval-decision]
               (mapv :type events)))
        (is (= {"label" "Password"
                "masked" true}
               (:data (first events))))
        (is (= {"label" "Password"
                "masked" true
                "provided" true}
               (:data (second events))))
        (is (= {"tool-name" "browser-open"
                "approved" true
                "policy" "session"}
               (:data (last events)))))
      (finally
        (prompt/register-prompt! :terminal nil)
        (prompt/register-approval! :terminal nil)))))
