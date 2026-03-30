(ns xia.channel.terminal-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.channel.terminal]))

(deftest terminal-assistant-message-prints-a-visible-assistant-line
  (let [output (with-out-str
                 (#'xia.channel.terminal/terminal-assistant-message
                  {:text "Checked inbox."}))]
    (is (str/includes? output "Xia> Checked inbox."))))

(deftest terminal-assistant-message-ignores-blank-text
  (is (= ""
         (with-out-str
           (#'xia.channel.terminal/terminal-assistant-message
            {:text "   "})))))
