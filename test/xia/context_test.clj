(ns xia.context-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.context :as context]
            [xia.db :as db]))

(defn- fake-history-message
  [eid]
  {:role :user
   :content (str "m" eid)})

(deftest compact-history-falls-back-to-bounded-tail-when-summary-throws
  (let [messages (vec (map fake-history-message (range 10)))]
    (with-redefs-fn {#'xia.context/estimate-tokens
                     (fn [_] 10)

                     #'xia.context/summarize-history-text
                     (fn [& _]
                       (throw (ex-info "boom" {})))}
      (fn []
        (let [compacted (#'xia.context/compact-history messages 50 {:allow-summary? true})]
          (is (= 5 (count compacted)))
          (is (= ["m5" "m6" "m7" "m8" "m9"]
                 (mapv :content compacted))))))))

(deftest build-history-with-session-recap-keeps-bounded-recent-history-when-recap-update-throws
  (with-redefs-fn {#'xia.context/estimate-tokens
                   (fn [_] 10)

                   #'xia.context/summarize-history-text
                   (fn [& _]
                     (throw (ex-info "boom" {})))

                   #'xia.db/session-message-count
                   (fn [_] 10)

                   #'xia.db/session-history-recap
                   (fn [_] nil)

                   #'xia.db/session-tool-recap
                   (fn [_] nil)

                   #'xia.db/session-message-metadata-range
                   (fn [_ start end _]
                     (mapv (fn [eid] {:eid eid}) (range start end)))

                   #'xia.db/session-messages-by-eids
                   (fn [eids]
                     (mapv fake-history-message eids))

                   #'xia.db/save-session-tool-recap!
                   (fn [& _] nil)}
    (fn []
      (let [{:keys [messages history-recap-updated?]}
            (#'xia.context/build-history-with-session-recap
             :session
             {:recent-message-limit 4
              :history-budget 30})]
        (is (false? history-recap-updated?))
        (is (= ["m6" "m7" "m8" "m9"]
               (mapv :content messages)))))))

(deftest build-history-with-session-recap-does-not-fall-back-to-full-history-when-recap-is-blank
  (with-redefs-fn {#'xia.context/estimate-tokens
                   (fn [_] 10)

                   #'xia.context/summarize-history-text
                   (fn [& _] "")

                   #'xia.db/session-message-count
                   (fn [_] 10)

                   #'xia.db/session-history-recap
                   (fn [_] nil)

                   #'xia.db/session-tool-recap
                   (fn [_] nil)

                   #'xia.db/session-message-metadata-range
                   (fn [_ start end _]
                     (mapv (fn [eid] {:eid eid}) (range start end)))

                   #'xia.db/session-messages-by-eids
                   (fn [eids]
                     (mapv fake-history-message eids))

                   #'xia.db/save-session-tool-recap!
                   (fn [& _] nil)}
    (fn []
      (let [{:keys [messages history-recap-updated?]}
            (#'xia.context/build-history-with-session-recap
             :session
             {:recent-message-limit 4
              :history-budget 30})]
        (is (false? history-recap-updated?))
        (is (= ["m6" "m7" "m8" "m9"]
               (mapv :content messages)))))))

(deftest resolve-history-budget-clamps-to-provider-recommendation
  (is (= 4000
         (#'xia.context/resolve-history-budget
          {:llm.provider/history-budget 9000
           :llm.provider/recommended-history-budget 4000}))))

(deftest compact-history-truncates-giant-recent-messages-even-when-few-are-kept
  (let [oversized (apply str (repeat 2000 "x"))
        messages  [{:role "assistant"
                    :content "planning"
                    :tool_calls [{:id "call_1"
                                  :type "function"
                                  :function {:name "search"
                                             :arguments oversized}}]}
                   {:role "tool"
                    :tool_call_id "call_1"
                    :content oversized}
                   {:role "user"
                    :content oversized}]]
    (let [compacted (#'xia.context/compact-history messages 60 {:allow-summary? false})]
      (is (= 3 (count compacted)))
      (is (every? #(<= (#'xia.context/estimate-history-message-tokens %) 60)
                  compacted))
      (is (str/includes? (get-in compacted [0 :tool_calls 0 :function :arguments])
                         "truncated to fit history budget"))
      (is (str/includes? (:content (second compacted))
                         "truncated to fit history budget"))
      (is (str/includes? (:content (nth compacted 2))
                         "truncated to fit history budget")))))
