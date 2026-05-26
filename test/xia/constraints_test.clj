(ns xia.constraints-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.constraints :as constraints]
            [xia.db :as db]
            [xia.goal :as goal]
            [xia.scratch :as scratch]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest operating-envelope-respects-declared-precedence
  (let [profile-id (db/ensure-user-profile!
                    {:key "primary"
                     :preferences {:model {:tier :user
                                            :style :concise}
                                   :user-only true}})
        session-id (db/create-session! :terminal
                                       {:user-profile-id profile-id})
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :interactive
                                     :state :running
                                     :title "Investigate"
                                     :constraints {:model {:tier :task}
                                                   :limits {:llm-calls 10}
                                                   :task-only true}})
        goal*      (goal/set-goal! session-id
                                   "Finish investigation"
                                   :success-criteria ["Root cause identified"]
                                   :constraints {:model {:tier :goal}
                                                 :limits {:llm-calls 20}
                                                 :goal-only true}
                                   :budget {:max-llm-calls 3})
        _          (db/save-session-history-recap! session-id "Earlier turn context" 3)
        _          (scratch/create-pad! {:session-id session-id
                                         :title "Scratch note"
                                         :content "Lowest-precedence working note"})
        _          (db/set-config! :constraints/org-policy
                                   (pr-str {:model {:tier :org}
                                            :org-only true}))
        envelope   (constraints/operating-envelope {:session-id session-id
                                                    :task-id task-id})]
    (is (= [:session-context
            :user-preferences
            :task-constraints
            :goal-contract
            :org-policy]
           (:precedence envelope)))
    (is (= (:id goal*) (get-in envelope [:sources :resolved :goal-id])))
    (is (= :org (get-in envelope [:effective :model :tier])))
    (is (= :concise (get-in envelope [:effective :model :style])))
    (is (= 20 (get-in envelope [:effective :limits :llm-calls])))
    (is (true? (get-in envelope [:effective :user-only])))
    (is (true? (get-in envelope [:effective :task-only])))
    (is (true? (get-in envelope [:effective :goal-only])))
    (is (true? (get-in envelope [:effective :org-only])))
    (is (= "Finish investigation"
           (get-in envelope [:effective :goal :intent])))
    (is (= ["Root cause identified"]
           (get-in envelope [:effective :goal :success-criteria])))
    (is (= 3 (get-in envelope [:effective :goal :budget :max-llm-calls])))
    (is (= "Earlier turn context"
           (get-in envelope [:sources :session-context :session :history-recap :content])))
    (is (= ["Scratch note"]
           (mapv :title (get-in envelope [:sources :session-context :session :scratch-pads]))))))
