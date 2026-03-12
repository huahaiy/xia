(ns xia.agent-test
  (:require [clojure.test :refer :all]
            [xia.agent :as agent]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.test-helpers :refer [with-test-db]]
            [xia.working-memory :as wm]))

(use-fixtures :each with-test-db)

(deftest process-message-reports-progress
  (let [session-id (db/create-session! :terminal)
        statuses   (atom [])]
    (wm/ensure-wm! session-id)
    (prompt/register-status! :terminal
                             (fn [status]
                               (swap! statuses conj (select-keys status
                                                                 [:state :phase :message]))))
    (try
      (with-redefs [xia.tool/tool-definitions (constantly [])
                    xia.llm/chat-simple       (fn [_messages] "All set.")]
        (is (= "All set."
               (agent/process-message session-id "hello" :channel :terminal))))
      (is (= [{:state :running
               :phase :working-memory
               :message "Updating working memory"}
              {:state :running
               :phase :llm
               :message "Calling model"}
              {:state :running
               :phase :finalizing
               :message "Preparing response"}
              {:state :done
               :phase :complete
               :message "Ready"}]
             @statuses))
      (finally
        (prompt/register-status! :terminal nil)))))
