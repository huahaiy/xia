(ns xia.agent-test
  (:require [clojure.test :refer :all]
            [xia.agent :as agent]))

(deftest first-tool-call-response-without-intent-gets-synthesized-intent
  (let [tool-calls [{"id" "call-1"
                     "function" {"name" "email-list"
                                 "arguments" "{\"query\":\"from:michelle\"}"}}]
        parsed (#'agent/ensure-tool-call-intent
                nil
                {:user-message "Find the Michelle Zhou email"}
                0
                {:assistant-text ""
                 :intent-status :missing
                 :control-status :missing}
                tool-calls)]
    (is (= :synthesized (:intent-status parsed)))
    (is (= "Find the Michelle Zhou email" (get-in parsed [:intent :focus])))
    (is (= "Call email-list" (get-in parsed [:intent :plan-step])))
    (is (= "email-list" (get-in parsed [:intent :tool-name])))
    (is (re-find #"from:michelle"
                 (get-in parsed [:intent :tool-args-summary])))))

(deftest malformed-first-tool-call-intent-still-fails-protocol-validation
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"malformed ACTION_INTENT_JSON"
       (#'agent/validate-tool-round-protocol!
        nil
        {:channel :http
         :iteration 1}
        0
        {:intent-status :malformed
         :control-status :missing}))))
