(ns xia.board-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [xia.board :as board]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest board-cards-are-durable-tasks
  (let [card (board/create-card! {:title "Investigate MCP"
                                  :description "Wire first integration slice"
                                  :priority "high"
                                  :assignee "research"})]
    (is (uuid? (:id card)))
    (is (= :open (:status card)))
    (is (= :high (:priority card)))
    (is (= "research" (:assignee card)))
    (is (= "Investigate MCP" (:title (board/get-card (:id card)))))
    (is (= [(:id card)]
           (mapv :id (board/list-cards {:assignee "research"}))))))

(deftest board-claim-token-guards-claimed-updates
  (let [card    (board/create-card! {:title "Write board test"})
        claimed (board/claim-card! (:id card) {:assignee "worker-a"})
        token   (:claim-token claimed)]
    (is (= :claimed (:status claimed)))
    (is (string? token))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"claim token"
         (board/update-card! (:id card) {:status :completed
                                         :claim-token "wrong"})))
    (let [updated (board/update-card! (:id card) {:status "completed"
                                                  :claim-token token})]
      (is (= :completed (:status updated)))
      (is (some? (:finished-at updated))))
    (testing "terminal cards are hidden unless requested"
      (is (empty? (board/list-cards)))
      (is (= [(:id card)]
             (mapv :id (board/list-cards {:include-terminal? true})))))))

(deftest board-comments-and-heartbeats-update-card
  (let [card    (board/create-card! {:title "Coordinate worker"})
        claimed (board/claim-card! (:id card) {:assignee "worker-b"})
        token   (:claim-token claimed)
        beat    (board/heartbeat-card! (:id card) {:claim-token token})
        commented (board/comment-card! (:id card)
                                       {:author "worker-b"
                                        :text "Started inspection."})]
    (is (some? (:heartbeat-at beat)))
    (is (= 1 (count (:comments commented))))
    (is (= "Started inspection." (-> commented :comments first :text)))))
