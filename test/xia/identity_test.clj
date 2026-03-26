(ns xia.identity-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.identity :as identity]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest init-identity-adds-default-role
  (identity/init-identity!)
  (is (= "General personal assistant for everyday digital work."
         (:role (identity/get-soul)))))

(deftest system-prompt-guides-resume-reply-style
  (let [prompt (identity/system-prompt)]
    (is (str/includes? prompt "## Role\nGeneral personal assistant for everyday digital work."))
    (is (str/includes? prompt
                       "If resumable state is found, briefly summarize what you recovered and propose the next step."))
    (is (str/includes? prompt
                       "If resumable state is not found, say that you could not recover prior work from stored history, mention what you checked, and ask one focused follow-up question that would let you continue."))
    (is (str/includes? prompt
                       "Do not speculate with boilerplate like 'the session expired', 'you're in a fresh session', or 'it was saved elsewhere' unless you have evidence."))
    (is (str/includes? prompt
                       "Do not ask a broad questionnaire when one targeted question will move the task forward."))))
