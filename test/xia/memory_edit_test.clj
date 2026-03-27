(ns xia.memory-edit-test
  (:require [clojure.test :refer :all]
            [xia.memory :as memory]
            [xia.memory-edit :as memory-edit]
            [xia.prompt :as prompt]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest correct-fact-replaces-exact-match-after-approval
  (let [node-eid   (th/seed-node! "Gmail" "service")
        _          (th/seed-fact! node-eid
                                  "service_id: hyang@juji-inc.com"
                                  :confidence 0.7
                                  :utility 0.9)
        approvals* (atom 0)]
    (prompt/register-approval! :terminal
                               (fn [_]
                                 (swap! approvals* inc)
                                 true))
    (try
      (let [result (binding [prompt/*interaction-context* {:channel :terminal}]
                     (memory-edit/correct-fact!
                       {:old-fact "service_id: hyang@juji-inc.com"
                        :corrected-fact "Gmail service id is gmail"
                        :entity-name "Gmail"}))
            facts  (memory/node-facts-with-eids node-eid)]
        (is (= "corrected" (:status result)))
        (is (= "Gmail" (:entity_name result)))
        (is (= 1 @approvals*))
        (is (= 1 (count facts)))
        (is (= "Gmail service id is gmail" (:content (first facts))))
        (is (< (Math/abs (- 0.7 (double (:confidence (first facts))))) 1.0e-6))
        (is (< (Math/abs (- 0.9 (double (:utility (first facts))))) 1.0e-6)))
      (finally
        (prompt/register-approval! :terminal nil)))))

(deftest correct-fact-reports-ambiguous-match-with-candidates
  (let [alice (th/seed-node! "Alice" "person")
        bob   (th/seed-node! "Bob" "person")]
    (th/seed-fact! alice "prefers tea")
    (th/seed-fact! bob "prefers tea")
    (let [result (memory-edit/correct-fact!
                   {:old-fact "prefers tea"
                    :corrected-fact "prefers coffee"})]
      (is (= "ambiguous" (:status result)))
      (is (= 2 (count (:candidate_facts result))))
      (is (= #{"Alice" "Bob"}
             (set (map :entity_name (:candidate_facts result))))))))

(deftest correct-fact-suggests-nearby-facts-when-exact-match-missing
  (let [node-eid (th/seed-node! "Email" "service")]
    (th/seed-fact! node-eid "Gmail service id is gmail")
    (let [result (memory-edit/correct-fact!
                   {:old-fact "service_id: hyang@juji-inc.com"
                    :corrected-fact "Gmail service id is gmail"
                    :entity-name "Email"})
          facts  (memory/node-facts-with-eids node-eid)]
      (is (= "not_found" (:status result)))
      (is (= [{:entity_name "Email"
               :fact "Gmail service id is gmail"}]
             (mapv #(select-keys % [:entity_name :fact])
                   (:candidate_facts result))))
      (is (= 1 (count facts)))
      (is (= "Gmail service id is gmail" (:content (first facts)))))))
