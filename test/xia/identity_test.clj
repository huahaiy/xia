(ns xia.identity-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.identity :as identity]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest init-identity-adds-default-role
  (identity/init-identity!)
  (is (= "Personal Assistant"
         (:role (identity/get-soul)))))

(deftest system-prompt-guides-resume-reply-style
  (let [prompt (identity/system-prompt)]
    (is (str/includes? prompt "## Role\nPersonal Assistant"))
    (is (str/includes? prompt
                       "quietly help manage the user’s digital life"))
    (is (str/includes? prompt
                       "Be Proactive, Not Passive"))
    (is (str/includes? prompt
                       "Avoid unnecessary filler"))
    (is (str/includes? prompt
                       "If resumable state is found, briefly summarize what you recovered and propose the next step."))
    (is (str/includes? prompt
                       "If resumable state is not found, say that you could not recover prior work from stored history, mention what you checked, and ask one focused follow-up question that would let you continue."))
    (is (str/includes? prompt
                       "Do not speculate with boilerplate like 'the session expired', 'you're in a fresh session', or 'it was saved elsewhere' unless you have evidence."))
    (is (str/includes? prompt
                       "Do not ask a broad questionnaire when one targeted question will move the task forward."))))
