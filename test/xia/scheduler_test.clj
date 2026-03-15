(ns xia.scheduler-test
  (:require [clojure.test :refer :all]
            [xia.agent]
            [xia.db :as db]
            [xia.hippocampus]
            [xia.schedule]
            [xia.scheduler :as scheduler]
            [xia.test-helpers :refer [with-test-db]]
            [xia.working-memory]))

(use-fixtures :each with-test-db)

(deftest execute-prompt-schedule-records-conversation-on-success
  (let [sid       (random-uuid)
        lifecycle (atom [])
        run*      (atom nil)]
    (with-redefs [xia.db/create-session! (fn [channel]
                                           (is (= :scheduler channel))
                                           sid)
                  xia.working-memory/ensure-wm! (fn [session-id]
                                                  (swap! lifecycle conj [:ensure session-id]))
                  xia.agent/process-message (fn [session-id prompt & {:keys [channel tool-context]}]
                                              (swap! lifecycle conj [:process session-id channel prompt
                                                                     (:schedule-id tool-context)
                                                                     (:approval-bypass? tool-context)])
                                              "done")
                  xia.schedule/record-run! (fn [schedule-id run]
                                             (reset! run* {:schedule-id schedule-id
                                                           :run run}))
                  xia.working-memory/get-wm (fn [session-id]
                                              (swap! lifecycle conj [:get-wm session-id])
                                              {:topics "nightly summary"})
                  xia.working-memory/snapshot! (fn [session-id]
                                                (swap! lifecycle conj [:snapshot session-id]))
                  xia.hippocampus/record-conversation! (fn [session-id channel & {:keys [topics]}]
                                                         (swap! lifecycle conj [:record session-id channel topics]))
                  xia.working-memory/clear-wm! (fn [session-id]
                                                 (swap! lifecycle conj [:clear session-id]))]
      (#'scheduler/execute-prompt-schedule {:id :nightly-review
                                            :prompt "summarize the week"
                                            :trusted? true}))
    (is (= {:schedule-id :nightly-review
            :run {:status :success
                  :actions []
                  :result "done"}}
           (update @run* :run select-keys [:status :actions :result])))
    (is (= [[:ensure sid]
            [:process sid :scheduler "summarize the week" :nightly-review true]
            [:get-wm sid]
            [:snapshot sid]
            [:record sid :scheduler "nightly summary"]
            [:clear sid]]
           @lifecycle))))

(deftest execute-prompt-schedule-records-conversation-on-error
  (let [sid       (random-uuid)
        lifecycle (atom [])
        run*      (atom nil)]
    (with-redefs [xia.db/create-session! (fn [_channel] sid)
                  xia.working-memory/ensure-wm! (fn [session-id]
                                                  (swap! lifecycle conj [:ensure session-id]))
                  xia.agent/process-message (fn [session-id _prompt & _]
                                              (swap! lifecycle conj [:process session-id])
                                              (throw (ex-info "boom" {:type :test})))
                  xia.schedule/record-run! (fn [schedule-id run]
                                             (reset! run* {:schedule-id schedule-id
                                                           :run run}))
                  xia.working-memory/get-wm (fn [session-id]
                                              (swap! lifecycle conj [:get-wm session-id])
                                              {:topics "failed run"})
                  xia.working-memory/snapshot! (fn [session-id]
                                                (swap! lifecycle conj [:snapshot session-id]))
                  xia.hippocampus/record-conversation! (fn [session-id channel & {:keys [topics]}]
                                                         (swap! lifecycle conj [:record session-id channel topics]))
                  xia.working-memory/clear-wm! (fn [session-id]
                                                 (swap! lifecycle conj [:clear session-id]))]
      (#'scheduler/execute-prompt-schedule {:id :nightly-review
                                            :prompt "summarize the week"
                                            :trusted? false}))
    (is (= {:schedule-id :nightly-review
            :run {:status :error
                  :actions []
                  :error "boom"}}
           (update @run* :run select-keys [:status :actions :error])))
    (is (= [[:ensure sid]
            [:process sid]
            [:get-wm sid]
            [:snapshot sid]
            [:record sid :scheduler "failed run"]
            [:clear sid]]
           @lifecycle))))
