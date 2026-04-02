(ns xia.channel.messaging-test
  (:require [charred.api :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.agent :as agent]
            [xia.channel.messaging :as messaging]
            [xia.db :as db]
            [xia.http-client :as http-client]
            [xia.prompt :as prompt]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(deftest slack-events-reuse-thread-session-and-persist-history
  (let [delivered (atom [])]
    (with-redefs [xia.channel.messaging/slack-enabled? (constantly true)
                  xia.async/submit-background! (fn [_description f]
                                                 (f))
                  agent/process-message (fn [session-id user-message & {:keys [channel persist-message?]}]
                                          (is (= :slack channel))
                                          (is (false? persist-message?))
                                          (binding [prompt/*interaction-context* {:channel channel
                                                                                  :session-id session-id}]
                                            (prompt/runtime-event! {:type :message.assistant
                                                                    :task-id (random-uuid)
                                                                    :summary (str "ack: " user-message "@" session-id)
                                                                    :data {:text (str "ack: " user-message "@" session-id)}}))
                                          (str "ack: " user-message "@" session-id))
                  messaging/send-session-message! (fn [channel session-id text]
                                                    (swap! delivered conj {:channel channel
                                                                           :session-id session-id
                                                                           :text text})
                                                    true)]
      (messaging/start!)
      (try
        (messaging/handle-slack-event!
         {"team_id" "T1"
          "event_id" "EVT-1"
          "event" {"type" "app_mention"
                   "channel" "C1"
                   "thread_ts" "1710000000.100"
                   "text" "<@U123ABC> first task"
                   "user" "U1"}})
        (messaging/handle-slack-event!
         {"team_id" "T1"
          "event_id" "EVT-2"
          "event" {"type" "app_mention"
                   "channel" "C1"
                   "thread_ts" "1710000000.100"
                   "text" "<@U123ABC> follow up"
                   "user" "U1"}})
        (finally
          (messaging/stop!))))
    (let [session (db/find-session-by-external-key "slack:T1:C1:1710000000.100")
          messages (db/session-messages (:id session))]
      (is (= :slack (:channel session)))
      (is (= "Slack C1" (:label session)))
      (is (= {:team-id "T1"
              :channel-id "C1"
              :thread-ts "1710000000.100"
              :user-id "U1"}
             (db/session-external-meta (:id session))))
      (is (= [:user :user]
             (mapv :role messages)))
      (is (= ["first task"
              "follow up"]
             (mapv :content messages)))
      (is (= [{:channel :slack
               :session-id (:id session)
               :text (str "ack: first task@" (:id session))}
              {:channel :slack
               :session-id (:id session)
               :text (str "ack: follow up@" (:id session))}]
             @delivered)))))

(deftest telegram-updates-create-session-and-deliver-reply
  (let [delivered (atom [])]
    (with-redefs [xia.channel.messaging/telegram-enabled? (constantly true)
                  xia.async/submit-background! (fn [_description f]
                                                 (f))
                  agent/process-message (fn [session-id user-message & {:keys [channel]}]
                                          (is (= :telegram channel))
                                          (binding [prompt/*interaction-context* {:channel channel
                                                                                  :session-id session-id}]
                                            (prompt/runtime-event! {:type :message.assistant
                                                                    :task-id (random-uuid)
                                                                    :summary (str "handled: " user-message "@" session-id)
                                                                    :data {:text (str "handled: " user-message "@" session-id)}}))
                                          (str "handled: " user-message "@" session-id))
                  messaging/send-session-message! (fn [channel session-id text]
                                                    (swap! delivered conj {:channel channel
                                                                           :session-id session-id
                                                                           :text text})
                                                    true)]
      (messaging/start!)
      (try
        (messaging/handle-telegram-update!
         {"update_id" 42
          "message" {"message_id" 7
                      "message_thread_id" 9
                      "text" "status?"
                      "chat" {"id" 1001
                              "title" "Ops"}
                      "from" {"id" 55
                              "is_bot" false
                              "first_name" "Alex"}}})
        (finally
          (messaging/stop!))))
    (let [session (db/find-session-by-external-key "telegram:1001:9")
          messages (db/session-messages (:id session))]
      (is (= :telegram (:channel session)))
      (is (= "Ops" (:label session)))
      (is (= {:chat-id 1001
              :message-thread-id 9
              :reply-to-message-id 7}
             (db/session-external-meta (:id session))))
      (is (= [:user]
             (mapv :role messages)))
      (is (= ["status?"]
             (mapv :content messages)))
      (is (= [{:channel :telegram
               :session-id (:id session)
               :text (str "handled: status?@" (:id session))}]
             @delivered)))))

(deftest send-session-message-uses-channel-specific-delivery
  (db/set-config! :secret/messaging-slack-bot-token "slack-token")
  (db/set-config! :secret/messaging-telegram-bot-token "telegram-token")
  (let [slack-session (db/create-session! :slack {:external-meta {:channel-id "C1"
                                                                  :thread-ts "1710.1"}})
        telegram-session (db/create-session! :telegram {:external-meta {:chat-id 2002
                                                                        :message-thread-id 12
                                                                        :reply-to-message-id 99}})
        imessage-session (db/create-session! :imessage {:external-meta {:chat-guid "chat-guid-1"}})
        requests (atom [])
        shells   (atom [])]
    (with-redefs [http-client/request (fn [req]
                                        (swap! requests conj req)
                                        {:status 200
                                         :body (json/write-json-str {"ok" true})})
                  clojure.java.shell/sh (fn [& argv]
                                          (swap! shells conj argv)
                                          {:exit 0 :out "" :err ""})
                  xia.channel.messaging/mac-os? (constantly true)]
      (is (true? (messaging/send-session-message! :slack slack-session "Slack hello")))
      (is (true? (messaging/send-session-message! :telegram telegram-session "Telegram hello")))
      (is (true? (messaging/send-session-message! :imessage imessage-session "iMessage hello"))))
    (is (= "https://slack.com/api/chat.postMessage"
           (:url (first @requests))))
    (is (= {"channel" "C1"
            "text" "Slack hello"
            "thread_ts" "1710.1"}
           (json/read-json (:body (first @requests)))))
    (is (= "https://api.telegram.org/bottelegram-token/sendMessage"
           (:url (second @requests))))
    (is (= {"chat_id" 2002
            "message_thread_id" 12
            "reply_to_message_id" 99
            "text" "Telegram hello"}
           (json/read-json (:body (second @requests)))))
    (is (= ["osascript"
            "-e" "on run argv"
            "-e" "set messageText to item 1 of argv"
            "-e" "set chatGuid to item 2 of argv"
            "-e" "tell application \"Messages\" to send messageText to chat id chatGuid"
            "-e" "end run"
            "--"
            "iMessage hello"
            "chat-guid-1"]
           (first @shells)))))

(deftest messaging-prompt-resumes-from-telegram-reply
  (let [session-id (db/create-session! :telegram {:external-key "telegram:1001:main"
                                                  :external-meta {:chat-id 1001}})
        delivered  (atom [])
        process-calls (atom 0)]
    (with-redefs [xia.channel.messaging/telegram-enabled? (constantly true)
                  xia.async/submit-background! (fn [_description f]
                                                 (f))
                  agent/process-message (fn [& _]
                                          (swap! process-calls inc)
                                          "unexpected")
                  messaging/send-session-message! (fn [channel sid text]
                                                    (swap! delivered conj {:channel channel
                                                                           :session-id sid
                                                                           :text text})
                                                    true)]
      (messaging/start!)
      (try
        (let [result-future
              (future
                (binding [prompt/*interaction-context* {:channel :telegram
                                                        :session-id session-id}]
                  (prompt/prompt! "OTP Code" :mask? true)))]
          (Thread/sleep 50)
          (messaging/handle-telegram-update!
           {"update_id" 99
            "message" {"message_id" 17
                        "text" "123456"
                        "chat" {"id" 1001}
                        "from" {"id" 55
                                "is_bot" false
                                "first_name" "Alex"}}})
          (is (= "123456" (deref result-future 2000 ::timeout)))
          (is (= 0 @process-calls))
          (is (= [] (db/session-messages session-id)))
          (is (= [{:channel :telegram
                   :session-id session-id
                   :text "Need input: OTP Code\nReply with the secret value for this run. Xia will not save it to internal chat history.\nReply CANCEL to stop this run."}]
                 @delivered)))
        (finally
          (messaging/stop!))))))

(deftest messaging-approval-resumes-from-slack-reply
  (let [session-id (db/create-session! :slack {:external-key "slack:T1:C1:1710000000.100"
                                               :external-meta {:team-id "T1"
                                                               :channel-id "C1"
                                                               :thread-ts "1710000000.100"}})
        delivered  (atom [])
        process-calls (atom 0)]
    (with-redefs [xia.channel.messaging/slack-enabled? (constantly true)
                  xia.async/submit-background! (fn [_description f]
                                                 (f))
                  agent/process-message (fn [& _]
                                          (swap! process-calls inc)
                                          "unexpected")
                  messaging/send-session-message! (fn [channel sid text]
                                                    (swap! delivered conj {:channel channel
                                                                           :session-id sid
                                                                           :text text})
                                                    true)]
      (messaging/start!)
      (try
        (let [result-future
              (future
                (binding [prompt/*interaction-context* {:channel :slack
                                                        :session-id session-id}]
                  (prompt/approve! {:tool-id :email-send
                                    :tool-name "email-send"
                                    :description "Send the draft email"
                                    :arguments {:to "ops@example.com"}
                                    :reason "The draft is ready"})))]
          (Thread/sleep 50)
          (messaging/handle-slack-event!
           {"team_id" "T1"
            "event_id" "EVT-approve"
            "event" {"type" "message"
                     "channel" "C1"
                     "thread_ts" "1710000000.100"
                     "ts" "1710000000.200"
                     "text" "yes"
                     "user" "U1"}})
          (is (= true (deref result-future 2000 ::timeout)))
          (is (= 0 @process-calls))
          (is (= [] (db/session-messages session-id)))
          (is (= [{:channel :slack
                   :session-id session-id
                   :text "Approval needed for email-send.\nDescription: Send the draft email\nReason: The draft is ready\nArguments: {:to \"ops@example.com\"}\nReply YES or NO. Reply CANCEL to stop this run."}]
                 @delivered)))
        (finally
          (messaging/stop!))))))

(deftest messaging-control-intent-interrupts-the-current-task
  (let [session-id     (db/create-session! :telegram {:external-key "telegram:1001:main"
                                                      :external-meta {:chat-id 1001}})
        task-id        (db/create-task! {:session-id session-id
                                         :channel :telegram
                                         :type :interactive
                                         :state :running
                                         :title "Investigate outage"})
        delivered      (atom [])
        process-calls  (atom 0)
        interrupt-calls (atom [])]
    (with-redefs [xia.channel.messaging/telegram-enabled? (constantly true)
                  xia.async/submit-background! (fn [_description f]
                                                 (f))
                  agent/process-message (fn [& _]
                                          (swap! process-calls inc)
                                          "unexpected")
                  agent/interrupt-task! (fn [id]
                                          (swap! interrupt-calls conj id)
                                          {:status :interrupting
                                           :task-id id
                                           :session-id session-id})
                  messaging/send-session-message! (fn [channel sid text]
                                                    (swap! delivered conj {:channel channel
                                                                           :session-id sid
                                                                           :text text})
                                                    true)]
      (messaging/start!)
      (try
        (messaging/handle-telegram-update!
         {"update_id" 101
          "message" {"message_id" 18
                      "text" "cancel"
                      "chat" {"id" 1001}
                      "from" {"id" 55
                              "is_bot" false
                              "first_name" "Alex"}}})
        (is (= 0 @process-calls))
        (is (= [task-id] @interrupt-calls))
        (is (= [] (db/session-messages session-id)))
        (is (= [{:channel :telegram
                 :session-id session-id
                 :text "Interrupting the current task."}]
               @delivered))
        (finally
          (messaging/stop!))))))

(deftest messaging-control-intent-cancels-the-current-session-when-no-task-exists
  (let [session-id      (db/create-session! :telegram {:external-key "telegram:1001:main"
                                                       :external-meta {:chat-id 1001}})
        delivered       (atom [])
        process-calls   (atom 0)
        cancel-calls    (atom [])]
    (with-redefs [xia.channel.messaging/telegram-enabled? (constantly true)
                  xia.async/submit-background! (fn [_description f]
                                                 (f))
                  agent/process-message (fn [& _]
                                          (swap! process-calls inc)
                                          "unexpected")
                  agent/cancel-session! (fn [sid reason]
                                          (swap! cancel-calls conj [sid reason])
                                          true)
                  messaging/send-session-message! (fn [channel sid text]
                                                    (swap! delivered conj {:channel channel
                                                                           :session-id sid
                                                                           :text text})
                                                    true)]
      (messaging/start!)
      (try
        (messaging/handle-telegram-update!
         {"update_id" 102
          "message" {"message_id" 19
                      "text" "cancel"
                      "chat" {"id" 1001}
                      "from" {"id" 55
                              "is_bot" false
                              "first_name" "Alex"}}})
        (is (= 0 @process-calls))
        (is (= [[session-id "session cancel requested"]] @cancel-calls))
        (is (= [] (db/session-messages session-id)))
        (is (= [{:channel :telegram
                 :session-id session-id
                 :text "Cancelling the current session."}]
               @delivered))
        (finally
          (messaging/stop!))))))

(deftest messaging-status-intent-replies-with-current-task-summary
  (let [session-id    (db/create-session! :telegram {:external-key "telegram:1001:main"
                                                     :external-meta {:chat-id 1001}})
        _task-id      (db/create-task! {:session-id session-id
                                        :channel :telegram
                                        :type :interactive
                                        :state :running
                                        :title "Investigate outage"
                                        :summary "Checking the invoice and alert trail"
                                        :autonomy-state {:stack [{:title "Investigate outage"
                                                                  :summary "Find why the billing alert fired."
                                                                  :next-step "Compare the invoice notes"
                                                                  :progress-status :in-progress
                                                                  :agenda [{:item "Compare the invoice notes"
                                                                            :status :in-progress}]}]}})
        turn-id       (db/start-task-turn! (-> (db/current-session-task session-id) :id)
                                           {:operation :start
                                            :state :running
                                            :input "investigate outage"
                                            :summary "Started outage investigation"})
        delivered     (atom [])
        process-calls (atom 0)]
    (db/add-task-item! turn-id
                       {:type :assistant-message
                        :role :assistant
                        :summary "Compared the invoice notes with the alert thread."
                        :data {:text "Compared the invoice notes with the alert thread."}})
    (with-redefs [xia.channel.messaging/telegram-enabled? (constantly true)
                  xia.async/submit-background! (fn [_description f]
                                                 (f))
                  agent/process-message (fn [& _]
                                          (swap! process-calls inc)
                                          "unexpected")
                  messaging/send-session-message! (fn [channel sid text]
                                                    (swap! delivered conj {:channel channel
                                                                           :session-id sid
                                                                           :text text})
                                                    true)]
      (messaging/start!)
      (try
        (messaging/handle-telegram-update!
         {"update_id" 103
          "message" {"message_id" 20
                      "text" "status"
                      "chat" {"id" 1001}
                      "from" {"id" 55
                              "is_bot" false
                              "first_name" "Alex"}}})
        (is (= 0 @process-calls))
        (is (= [] (db/session-messages session-id)))
        (is (= 1 (count @delivered)))
        (is (str/includes? (get-in @delivered [0 :text]) "Task: Investigate outage"))
        (is (str/includes? (get-in @delivered [0 :text]) "State: Running"))
        (is (str/includes? (get-in @delivered [0 :text]) "Focus: Investigate outage"))
        (is (str/includes? (get-in @delivered [0 :text]) "Next: Compare the invoice notes"))
        (is (str/includes? (get-in @delivered [0 :text]) "Recent: Compared the invoice notes with the alert thread."))
        (is (str/includes? (get-in @delivered [0 :text]) "Reply PAUSE or STOP to control it."))
        (finally
          (messaging/stop!))))))

(deftest messaging-help-intent-replies-with-current-task-commands
  (let [session-id   (db/create-session! :telegram {:external-key "telegram:1001:main"
                                                    :external-meta {:chat-id 1001}})
        _task-id     (db/create-task! {:session-id session-id
                                       :channel :telegram
                                       :type :interactive
                                       :state :paused
                                       :title "Investigate outage"})
        delivered    (atom [])
        process-calls (atom 0)]
    (with-redefs [xia.channel.messaging/telegram-enabled? (constantly true)
                  xia.async/submit-background! (fn [_description f]
                                                 (f))
                  agent/process-message (fn [& _]
                                          (swap! process-calls inc)
                                          "unexpected")
                  messaging/send-session-message! (fn [channel sid text]
                                                    (swap! delivered conj {:channel channel
                                                                           :session-id sid
                                                                           :text text})
                                                    true)]
      (messaging/start!)
      (try
        (messaging/handle-telegram-update!
         {"update_id" 104
          "message" {"message_id" 21
                      "text" "help"
                      "chat" {"id" 1001}
                      "from" {"id" 55
                              "is_bot" false
                              "first_name" "Alex"}}})
        (is (= 0 @process-calls))
        (is (= [] (db/session-messages session-id)))
        (is (= 1 (count @delivered)))
        (is (str/includes? (get-in @delivered [0 :text]) "Commands:"))
        (is (str/includes? (get-in @delivered [0 :text]) "STATUS: show the current task summary"))
        (is (str/includes? (get-in @delivered [0 :text]) "OUTPUT: show the latest assistant output"))
        (is (str/includes? (get-in @delivered [0 :text]) "ACTIVITY: show recent task actions"))
        (is (str/includes? (get-in @delivered [0 :text]) "RESUME: resume the current task"))
        (finally
          (messaging/stop!))))))

(deftest messaging-output-intent-replies-with-latest-task-output
  (let [session-id    (db/create-session! :telegram {:external-key "telegram:1001:main"
                                                     :external-meta {:chat-id 1001}})
        task-id       (db/create-task! {:session-id session-id
                                        :channel :telegram
                                        :type :interactive
                                        :state :running
                                        :title "Draft billing reply"
                                        :autonomy-state {:stack [{:title "Draft billing reply"
                                                                  :next-step "Confirm the refund amount"
                                                                  :progress-status :in-progress}]}})
        turn-id       (db/start-task-turn! task-id
                                           {:operation :start
                                            :state :running
                                            :input "draft billing reply"})
        delivered     (atom [])
        process-calls (atom 0)]
    (db/add-task-item! turn-id
                       {:type :assistant-message
                        :role :assistant
                        :summary "Here is the revised billing reply draft."
                        :data {:text "Here is the revised billing reply draft."}})
    (with-redefs [xia.channel.messaging/telegram-enabled? (constantly true)
                  xia.async/submit-background! (fn [_description f]
                                                 (f))
                  agent/process-message (fn [& _]
                                          (swap! process-calls inc)
                                          "unexpected")
                  messaging/send-session-message! (fn [channel sid text]
                                                    (swap! delivered conj {:channel channel
                                                                           :session-id sid
                                                                           :text text})
                                                    true)]
      (messaging/start!)
      (try
        (messaging/handle-telegram-update!
         {"update_id" 105
          "message" {"message_id" 22
                      "text" "output"
                      "chat" {"id" 1001}
                      "from" {"id" 55
                              "is_bot" false
                              "first_name" "Alex"}}})
        (is (= 0 @process-calls))
        (is (= [] (db/session-messages session-id)))
        (is (= 1 (count @delivered)))
        (is (str/includes? (get-in @delivered [0 :text]) "Task: Draft billing reply"))
        (is (str/includes? (get-in @delivered [0 :text]) "State: Running"))
        (is (str/includes? (get-in @delivered [0 :text]) "Latest output: Here is the revised billing reply draft."))
        (is (str/includes? (get-in @delivered [0 :text]) "Next: Confirm the refund amount"))
        (finally
          (messaging/stop!))))))

(deftest messaging-activity-intent-replies-with-recent-task-activity
  (let [session-id    (db/create-session! :telegram {:external-key "telegram:1001:main"
                                                     :external-meta {:chat-id 1001}})
        task-id       (db/create-task! {:session-id session-id
                                        :channel :telegram
                                        :type :interactive
                                        :state :running
                                        :title "Draft billing reply"})
        turn-id       (db/start-task-turn! task-id
                                           {:operation :start
                                            :state :running
                                            :input "draft billing reply"})
        delivered     (atom [])
        process-calls (atom 0)]
    (db/add-task-item! turn-id
                       {:type :tool-call
                        :tool-id "read-file"
                        :tool-call-id "call-1"
                        :summary "Requested read-file on invoice-notes.txt"})
    (db/add-task-item! turn-id
                       {:type :tool-result
                        :tool-id "read-file"
                        :tool-call-id "call-1"
                        :status :ok
                        :summary "Loaded invoice-notes.txt"})
    (db/add-task-item! turn-id
                       {:type :assistant-message
                        :role :assistant
                        :summary "Compared the invoice notes against the draft reply."
                        :data {:text "Compared the invoice notes against the draft reply."}})
    (with-redefs [xia.channel.messaging/telegram-enabled? (constantly true)
                  xia.async/submit-background! (fn [_description f]
                                                 (f))
                  agent/process-message (fn [& _]
                                          (swap! process-calls inc)
                                          "unexpected")
                  messaging/send-session-message! (fn [channel sid text]
                                                    (swap! delivered conj {:channel channel
                                                                           :session-id sid
                                                                           :text text})
                                                    true)]
      (messaging/start!)
      (try
        (messaging/handle-telegram-update!
         {"update_id" 106
          "message" {"message_id" 23
                      "text" "activity"
                      "chat" {"id" 1001}
                      "from" {"id" 55
                              "is_bot" false
                              "first_name" "Alex"}}})
        (is (= 0 @process-calls))
        (is (= [] (db/session-messages session-id)))
        (is (= 1 (count @delivered)))
        (is (str/includes? (get-in @delivered [0 :text]) "Task: Draft billing reply"))
        (is (str/includes? (get-in @delivered [0 :text]) "Recent activity:"))
        (is (str/includes? (get-in @delivered [0 :text]) "- Output: Compared the invoice notes against the draft reply."))
        (is (str/includes? (get-in @delivered [0 :text]) "- Tool completed: Loaded invoice-notes.txt"))
        (finally
          (messaging/stop!))))))
