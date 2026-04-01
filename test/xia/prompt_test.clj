(ns xia.prompt-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(deftest prompt-and-approval-decisions-are-audited
  (let [session-id (db/create-session! :terminal)]
    (prompt/register-prompt! :terminal (fn [_ & _] "hunter2"))
    (prompt/register-approval! :terminal (fn [_] true))
    (try
      (binding [prompt/*interaction-context* {:channel :terminal
                                              :session-id session-id}]
        (is (= "hunter2" (prompt/prompt! "Password" :mask? true)))
        (is (true? (prompt/approve! {:tool-id :browser-open
                                     :tool-name "browser-open"
                                     :arguments {:url "https://example.com"}
                                     :policy :session}))))
      (let [events (db/session-audit-events session-id)]
        (is (= [:input-request :input-response :approval-request :approval-decision]
               (mapv :type events)))
        (is (= {"label" "Password"
                "masked" true}
               (:data (first events))))
        (is (= {"label" "Password"
                "masked" true
                "provided" true}
               (:data (second events))))
        (is (= {"tool-name" "browser-open"
                "approved" true
                "policy" "session"}
               (:data (last events)))))
      (finally
        (prompt/register-prompt! :terminal nil)
        (prompt/register-approval! :terminal nil)))))

(deftest assistant-message-dispatches-to-the-current-channel-handler
  (let [session-id (db/create-session! :terminal)
        messages   (atom [])]
    (prompt/register-assistant-message! :terminal
                                        (fn [message]
                                          (swap! messages conj message)))
    (try
      (binding [prompt/*interaction-context* {:channel :terminal
                                              :session-id session-id}]
        (prompt/assistant-message! {:text "Checked inbox."
                                    :iteration 1}))
      (is (= [{:channel :terminal
               :session-id session-id
               :text "Checked inbox."
               :iteration 1}]
             @messages))
      (finally
        (prompt/register-assistant-message! :terminal nil)))))

(deftest channel-adapter-registers-and-clears-the-shared-interaction-surface
  (let [session-id (db/create-session! :terminal)
        events     (atom [])]
    (prompt/register-channel-adapter!
     :terminal
     {:prompt (fn [label & {:keys [mask?]}]
                (swap! events conj [:prompt label mask?])
                "123456")
      :approval (fn [request]
                  (swap! events conj [:approval (:tool-id request)])
                  true)
      :status (fn [status]
                (swap! events conj [:status (:phase status)])
                nil)
      :assistant-message (fn [message]
                           (swap! events conj [:assistant (:text message)])
                           nil)
      :runtime-event (fn [event]
                       (swap! events conj [:runtime-event (:type event)])
                       nil)})
    (try
      (binding [prompt/*interaction-context* {:channel :terminal
                                              :session-id session-id}]
        (is (= "123456" (prompt/prompt! "OTP Code" :mask? true)))
        (is (true? (prompt/approve! {:tool-id :browser-open
                                     :tool-name "browser-open"
                                     :arguments {:url "https://example.com"}
                                     :policy :session})))
        (is (nil? (prompt/status! {:phase :tool})))
        (is (nil? (prompt/assistant-message! {:text "Checked inbox."})))
        (is (nil? (prompt/runtime-event! {:type :task.status}))))
      (is (= [[:prompt "OTP Code" true]
              [:approval :browser-open]
              [:status :tool]
              [:assistant "Checked inbox."]
              [:runtime-event :task.status]]
             @events))
      (prompt/clear-channel-adapter! :terminal)
      (binding [prompt/*interaction-context* {:channel :terminal
                                              :session-id session-id}]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"No interactive prompt available"
                              (prompt/prompt! "OTP Code")))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"No approval handler available"
                              (prompt/approve! {:tool-id :browser-open})))
        (is (nil? (prompt/status! {:phase :tool})))
        (is (nil? (prompt/assistant-message! {:text "Checked inbox."})))
        (is (nil? (prompt/runtime-event! {:type :task.status}))))
      (finally
        (prompt/clear-channel-adapter! :terminal)))))

(deftest pending-interactions-prefer-task-correlation-over-session-correlation
  (let [session-id (db/create-session! :terminal)
        prompt-a   (prompt/register-interaction! {:kind :prompt
                                                  :session-id session-id
                                                  :task-id (random-uuid)
                                                  :prompt-id "prompt-a"
                                                  :label "OTP Code"
                                                  :response (promise)})
        prompt-b   (prompt/register-interaction! {:kind :approval
                                                  :session-id session-id
                                                  :task-id (random-uuid)
                                                  :approval-id "approval-b"
                                                  :tool-id :browser-open
                                                  :response (promise)})]
    (try
      (is (= "prompt-a"
             (:prompt-id (prompt/pending-interaction {:task-id (:task-id prompt-a)
                                                      :kind :prompt}))))
      (is (= "approval-b"
             (:approval-id (prompt/pending-interaction {:task-id (:task-id prompt-b)
                                                        :kind :approval}))))
      (is (= "approval-b"
             (:approval-id (prompt/pending-interaction {:session-id session-id}))))
      (is (true? (prompt/deliver-pending-interaction! {:task-id (:task-id prompt-a)}
                                                      "123456")))
      (is (= "123456" (deref (:response prompt-a) 0 nil)))
      (is (nil? (prompt/pending-interaction {:task-id (:task-id prompt-a)})))
      (is (true? (prompt/cancel-pending-interaction! {:task-id (:task-id prompt-b)})))
      (is (= :cancel (deref (:response prompt-b) 0 nil)))
      (finally
        (prompt/clear-pending-interaction! {:task-id (:task-id prompt-a)})
        (prompt/clear-pending-interaction! {:task-id (:task-id prompt-b)})))))

(deftest shared-reply-correlation-and-coercion-work-across-task-and-session-selectors
  (let [session-id (db/create-session! :terminal)
        prompt-a   (prompt/register-interaction! {:kind :prompt
                                                  :session-id session-id
                                                  :task-id (random-uuid)
                                                  :prompt-id "prompt-a"
                                                  :label "OTP Code"
                                                  :response (promise)})
        prompt-b   (prompt/register-interaction! {:kind :approval
                                                  :session-id session-id
                                                  :task-id (random-uuid)
                                                  :approval-id "approval-b"
                                                  :tool-id :browser-open
                                                  :response (promise)})]
    (try
      (is (= "prompt-a"
             (:prompt-id (prompt/resolve-pending-interaction {:task-id (:task-id prompt-a)
                                                              :session-id session-id
                                                              :kind :prompt}))))
      (is (= "approval-b"
             (:approval-id (prompt/resolve-pending-interaction {:session-id session-id
                                                                :kind :approval}))))
      (is (= :allow
             (prompt/coerce-interaction-reply prompt-b "YES")))
      (is (= :deny
             (prompt/coerce-interaction-reply prompt-b "deny")))
      (is (= :cancel
             (prompt/coerce-interaction-reply prompt-b "cancel")))
      (is (= "123456"
             (prompt/coerce-interaction-reply prompt-a " 123456 ")))
      (is (= :cancel
             (prompt/coerce-interaction-reply prompt-a "never mind")))
      (is (nil? (prompt/coerce-interaction-reply prompt-b "maybe later")))
      (is (= {:status :stale
              :interaction prompt-a}
             (select-keys (prompt/deliver-validated-interaction! {:task-id (:task-id prompt-a)
                                                                  :kind :prompt}
                                                                 "wrong-id"
                                                                 "654321")
                          [:status :interaction])))
      (is (= {:status :delivered
              :value "654321"}
             (select-keys (prompt/deliver-validated-interaction! {:task-id (:task-id prompt-a)
                                                                  :kind :prompt}
                                                                 "prompt-a"
                                                                 "654321")
                          [:status :value])))
      (is (= "654321" (deref (:response prompt-a) 0 nil)))
      (is (= {:status :delivered
              :value :allow}
             (select-keys (prompt/submit-freeform-interaction-reply! {:session-id session-id
                                                                      :kind :approval}
                                                                     "yes")
                          [:status :value])))
      (is (= :allow (deref (:response prompt-b) 0 nil)))
      (is (= :interrupt (prompt/parse-control-intent "cancel")))
      (is (= :pause (prompt/parse-control-intent "pause")))
      (is (= :resume (prompt/parse-control-intent "resume")))
      (is (= :stop (prompt/parse-control-intent "stop")))
      (is (= "Interrupting the current task."
             (prompt/control-result-text :interrupt {:status :interrupting})))
      (is (= "No current task to control."
             (prompt/control-result-text :pause {:status :missing})))
      (finally
        (prompt/clear-pending-interaction! {:task-id (:task-id prompt-a)})
        (prompt/clear-pending-interaction! {:task-id (:task-id prompt-b)})))))
