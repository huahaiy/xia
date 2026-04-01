(ns xia.channel.messaging
  "Messaging channel runtime for Slack, Telegram, and local iMessage."
  (:require [charred.api :as json]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.agent :as agent]
            [xia.async :as async]
            [xia.audit :as audit]
            [xia.db :as db]
            [xia.http-client :as http-client]
            [xia.prompt :as prompt])
  (:import [java.nio.charset StandardCharsets]
           [java.util Date]
           [java.util.concurrent Executors ScheduledExecutorService TimeUnit]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]))

(def ^:private slack-bot-token-config-key :secret/messaging-slack-bot-token)
(def ^:private slack-signing-secret-config-key :secret/messaging-slack-signing-secret)
(def ^:private telegram-bot-token-config-key :secret/messaging-telegram-bot-token)
(def ^:private telegram-webhook-secret-config-key :secret/messaging-telegram-webhook-secret)
(def ^:private imessage-enabled-config-key :messaging/imessage-enabled?)
(def ^:private slack-enabled-config-key :messaging/slack-enabled?)
(def ^:private telegram-enabled-config-key :messaging/telegram-enabled?)
(def ^:private imessage-poll-interval-config-key :messaging/imessage-poll-interval-ms)

(def ^:private default-imessage-poll-interval-ms 5000)
(def ^:private dedupe-retention-ms (* 10 60 1000))
(def ^:private telegram-message-max-chars 3500)
(def ^:private default-interaction-timeout-ms (* 15 60 1000))
(def ^:private interaction-poll-ms 500)
(def ^:private imessage-chat-db-path
  (str (System/getProperty "user.home") "/Library/Messages/chat.db"))

(defonce ^:private recent-external-message-ids (atom {}))
(defonce ^:private pending-interactions (atom {}))
(defonce ^:private imessage-poller-executor (atom nil))
(defonce ^:private imessage-last-rowid (atom 0))

(declare send-session-message!)

(defn- nonblank-str
  [value]
  (some-> value str str/trim not-empty))

(defn- boolean-config
  [config-key default]
  (let [value (db/get-config config-key)]
    (cond
      (nil? value) default
      (boolean? value) value
      :else (contains? #{"true" "1" "yes" "on"}
                       (some-> value str str/trim str/lower-case)))))

(defn- positive-long-config
  [config-key default]
  (let [value (db/get-config config-key)]
    (try
      (let [parsed (some-> value str Long/parseLong)]
        (if (and parsed (pos? parsed))
          parsed
          default))
      (catch Exception _
        default))))

(defn slack-enabled?
  []
  (boolean-config slack-enabled-config-key false))

(defn telegram-enabled?
  []
  (boolean-config telegram-enabled-config-key false))

(defn imessage-enabled?
  []
  (boolean-config imessage-enabled-config-key false))

(defn- imessage-poll-interval-ms
  []
  (positive-long-config imessage-poll-interval-config-key
                        default-imessage-poll-interval-ms))

(defn- slack-bot-token
  []
  (nonblank-str (db/get-config slack-bot-token-config-key)))

(defn- slack-signing-secret
  []
  (nonblank-str (db/get-config slack-signing-secret-config-key)))

(defn- telegram-bot-token
  []
  (nonblank-str (db/get-config telegram-bot-token-config-key)))

(defn- telegram-webhook-secret
  []
  (nonblank-str (db/get-config telegram-webhook-secret-config-key)))

(defn- mac-os?
  []
  (some-> (System/getProperty "os.name")
          str/lower-case
          (str/includes? "mac")))

(defn admin-body
  []
  {:slack {:enabled (slack-enabled?)
           :bot_token_configured (boolean (slack-bot-token))
           :signing_secret_configured (boolean (slack-signing-secret))}
   :telegram {:enabled (telegram-enabled?)
              :bot_token_configured (boolean (telegram-bot-token))
              :webhook_secret_configured (boolean (telegram-webhook-secret))}
   :imessage {:enabled (imessage-enabled?)
              :available (boolean (and (mac-os?)
                                       (.exists (java.io.File. imessage-chat-db-path))))
              :poll_interval_ms (imessage-poll-interval-ms)}})

(defn save-admin-config!
  [{:keys [slack telegram imessage]}]
  (when (map? slack)
    (when (contains? slack :enabled)
      (db/set-config! slack-enabled-config-key (if (:enabled slack) "true" "false")))
    (when (contains? slack :bot-token)
      (db/set-config! slack-bot-token-config-key (or (nonblank-str (:bot-token slack)) "")))
    (when (contains? slack :signing-secret)
      (db/set-config! slack-signing-secret-config-key (or (nonblank-str (:signing-secret slack)) ""))))
  (when (map? telegram)
    (when (contains? telegram :enabled)
      (db/set-config! telegram-enabled-config-key (if (:enabled telegram) "true" "false")))
    (when (contains? telegram :bot-token)
      (db/set-config! telegram-bot-token-config-key (or (nonblank-str (:bot-token telegram)) "")))
    (when (contains? telegram :webhook-secret)
      (db/set-config! telegram-webhook-secret-config-key (or (nonblank-str (:webhook-secret telegram)) ""))))
  (when (map? imessage)
    (when (contains? imessage :enabled)
      (db/set-config! imessage-enabled-config-key (if (:enabled imessage) "true" "false")))
    (when (contains? imessage :poll-interval-ms)
      (let [value (:poll-interval-ms imessage)]
        (db/set-config! imessage-poll-interval-config-key
                        (when (some? value)
                          (str value))))))
  (admin-body))

(defn- now-ms
  []
  (System/currentTimeMillis))

(defn- messaging-channel?
  [channel]
  (contains? #{:slack :telegram :imessage} channel))

(defn- prune-recent-message-ids
  [entries]
  (let [cutoff (- (now-ms) dedupe-retention-ms)]
    (into {}
          (filter (fn [[_ seen-at]]
                    (<= cutoff (long seen-at))))
          entries)))

(defn- duplicate-external-message?
  [external-message-id]
  (when external-message-id
    (loop []
      (let [state @recent-external-message-ids
            state* (prune-recent-message-ids state)]
        (if (not= state state*)
          (if (compare-and-set! recent-external-message-ids state state*)
            (recur)
            (recur))
          (contains? state* external-message-id))))))

(defn- mark-external-message!
  [external-message-id]
  (when external-message-id
    (swap! recent-external-message-ids
           (fn [entries]
             (assoc (prune-recent-message-ids entries)
                    external-message-id
                    (now-ms))))))

(defn- interaction-timeout-ms
  []
  default-interaction-timeout-ms)

(defn- clear-pending-interaction!
  [session-id interaction-id]
  (swap! pending-interactions
         (fn [pending]
           (let [current (get pending session-id)]
             (if (= interaction-id (:interaction-id current))
               (dissoc pending session-id)
               pending)))))

(defn- current-pending-interaction
  [session-id]
  (get @pending-interactions session-id))

(defn- interaction-cancelled?
  [text]
  (contains? #{"cancel" "/cancel" "stop" "abort" "nevermind" "never mind"}
             (some-> text str/trim str/lower-case)))

(defn- parse-approval-decision
  [text]
  (let [value (some-> text str/trim str/lower-case)]
    (cond
      (contains? #{"yes" "y" "allow" "approve" "approved" "ok"} value) :allow
      (contains? #{"no" "n" "deny" "reject" "denied"} value) :deny
      :else nil)))

(defn- prompt-request-text
  [{:keys [label mask?]}]
  (str "Need input: " (or (nonblank-str label) "required value") "\n"
       (if mask?
         "Reply with the secret value for this run. Xia will not save it to internal chat history."
         "Reply with the requested value.")
       "\nReply CANCEL to stop this run."))

(defn- approval-request-text
  [{:keys [tool-name description reason arguments]}]
  (str "Approval needed for "
       (or (nonblank-str tool-name) "a privileged action")
       ".\n"
       (when-let [description* (nonblank-str description)]
         (str "Description: " description* "\n"))
       (when-let [reason* (nonblank-str reason)]
         (str "Reason: " reason* "\n"))
       (when (some? arguments)
         (str "Arguments: " (pr-str arguments) "\n"))
       "Reply YES or NO. Reply CANCEL to stop this run."))

(defn- interaction-cancelled-ex
  [session-id channel]
  (ex-info "Request cancelled"
           {:type :request-cancelled
            :session-id session-id
            :channel channel}))

(defn- await-interaction-result
  [session-id channel interaction]
  (let [deadline (+ (now-ms) (interaction-timeout-ms))
        response (:response interaction)
        kind (:kind interaction)]
    (loop []
      (when (agent/session-cancelled? session-id)
        (throw (interaction-cancelled-ex session-id channel)))
      (let [result (deref response interaction-poll-ms ::pending)]
        (cond
          (= ::pending result)
          (if (< (now-ms) deadline)
            (recur)
            (throw (ex-info (str "Timed out waiting for "
                                 (case kind
                                   :prompt "input"
                                   :approval "approval"
                                   "interaction"))
                            {:type :messaging-interaction-timeout
                             :kind kind
                             :session-id session-id
                             :channel channel})))

          (= :cancel result)
          (throw (interaction-cancelled-ex session-id channel))

          :else
          result)))))

(defn- register-interaction!
  [session-id interaction]
  (swap! pending-interactions assoc session-id interaction)
  interaction)

(defn- messaging-prompt
  [label & {:keys [mask?] :or {mask? false}}]
  (let [{:keys [channel session-id]} prompt/*interaction-context*]
    (when-not (and session-id (messaging-channel? channel))
      (throw (ex-info "Messaging prompt requires a messaging session"
                      {:channel channel
                       :session-id session-id
                       :label label})))
    (let [interaction {:interaction-id (str (random-uuid))
                       :kind :prompt
                       :label label
                       :mask? (boolean mask?)
                       :created-at (Date.)
                       :response (promise)}]
      (register-interaction! session-id interaction)
      (try
        (send-session-message! channel session-id (prompt-request-text interaction))
        (str (await-interaction-result session-id channel interaction))
        (finally
          (clear-pending-interaction! session-id (:interaction-id interaction)))))))

(defn- messaging-approval
  [{:keys [tool-id tool-name description arguments reason]}]
  (let [{:keys [channel session-id]} prompt/*interaction-context*]
    (when-not (and session-id (messaging-channel? channel))
      (throw (ex-info "Messaging approval requires a messaging session"
                      {:channel channel
                       :session-id session-id
                       :tool-id tool-id})))
    (let [interaction {:interaction-id (str (random-uuid))
                       :kind :approval
                       :tool-id tool-id
                       :tool-name tool-name
                       :description description
                       :arguments arguments
                       :reason reason
                       :created-at (Date.)
                       :response (promise)}]
      (register-interaction! session-id interaction)
      (try
        (send-session-message! channel session-id (approval-request-text interaction))
        (= :allow (await-interaction-result session-id channel interaction))
        (finally
          (clear-pending-interaction! session-id (:interaction-id interaction)))))))

(defn valid-slack-signature?
  [body-bytes timestamp signature]
  (let [secret (slack-signing-secret)]
    (when (and secret
               (seq body-bytes)
               (seq timestamp)
               (seq signature))
      (try
        (let [ts-long (Long/parseLong timestamp)
              age     (Math/abs (- (quot (System/currentTimeMillis) 1000)
                                   ts-long))]
          (when (<= age 300)
            (let [payload  (str "v0:" timestamp ":" (String. ^bytes body-bytes StandardCharsets/UTF_8))
                  mac      (Mac/getInstance "HmacSHA256")
                  _        (.init mac (SecretKeySpec. (.getBytes secret StandardCharsets/UTF_8)
                                                      "HmacSHA256"))
                  digest   (.doFinal mac (.getBytes payload StandardCharsets/UTF_8))
                  expected (str "v0="
                                (apply str (map #(format "%02x" (bit-and % 0xff)) digest)))]
              (= expected signature))))
        (catch Exception e
          (log/warn e "Failed to verify Slack signature")
          false)))))

(defn telegram-secret-valid?
  [header-value]
  (let [secret (telegram-webhook-secret)]
    (or (str/blank? (or secret ""))
        (= secret (nonblank-str header-value)))))

(defn- session-meta
  [session-id]
  (or (db/session-external-meta session-id) {}))

(defn- slack-conversation-key
  [{:keys [team-id channel-id thread-ts]}]
  (str "slack:" team-id ":" channel-id ":" (or thread-ts "main")))

(defn- telegram-conversation-key
  [{:keys [chat-id message-thread-id]}]
  (str "telegram:" chat-id ":" (or message-thread-id "main")))

(defn- imessage-conversation-key
  [{:keys [chat-guid]}]
  (str "imessage:" chat-guid))

(defn- ensure-external-session!
  [channel external-key external-meta & {:keys [label]}]
  (let [existing (db/find-session-by-external-key external-key)]
    (if-let [session-id (:id existing)]
      (do
        (db/set-session-active! session-id true)
        (db/save-session-external-meta! session-id external-meta)
        session-id)
      (db/create-session! channel {:label label
                                   :external-key external-key
                                   :external-meta external-meta}))))

(defn- persist-external-user-message!
  [session-id channel user-message external-message-id]
  (let [message-id (db/add-message! session-id :user user-message)]
    (audit/log! {:session-id session-id
                 :channel channel}
                {:actor :user
                 :type :user-message
                 :message-id message-id
                 :data (cond-> {:external_message_id external-message-id}
                         true (assoc :messaging true))})
    message-id))

(defn- normalize-outbound-text
  [text]
  (some-> text str str/trim not-empty))

(defn- split-telegram-text
  [text]
  (let [text* (normalize-outbound-text text)]
    (when text*
      (loop [remaining text*
             acc []]
        (if (<= (count remaining) telegram-message-max-chars)
          (conj acc remaining)
          (let [cut (or (some-> (subs remaining 0 telegram-message-max-chars)
                                (str/last-index-of "\n"))
                        telegram-message-max-chars)]
            (recur (str/trim (subs remaining cut))
                   (conj acc (str/trim (subs remaining 0 cut))))))))))

(defn- slack-response-json
  [response]
  (try
    (json/read-json (:body response))
    (catch Exception _
      nil)))

(defn- send-slack-message!
  [route text]
  (when-let [token (slack-bot-token)]
    (let [payload (cond-> {:channel (:channel-id route)
                           :text text}
                    (nonblank-str (:thread-ts route))
                    (assoc :thread_ts (:thread-ts route)))
          response (http-client/request {:method :post
                                         :url "https://slack.com/api/chat.postMessage"
                                         :headers {"Authorization" (str "Bearer " token)
                                                   "Content-Type" "application/json; charset=utf-8"}
                                         :body (json/write-json-str payload)})
          body     (slack-response-json response)]
      (when-not (and (= 200 (:status response))
                     (true? (get body "ok")))
        (throw (ex-info "Slack message delivery failed"
                        {:status (:status response)
                         :body body})))
      true)))

(defn- send-telegram-message!
  [route text]
  (when-let [token (telegram-bot-token)]
    (doseq [chunk (split-telegram-text text)]
      (let [payload  (cond-> {:chat_id (:chat-id route)
                              :text chunk}
                       (:message-thread-id route)
                       (assoc :message_thread_id (:message-thread-id route))
                       (:reply-to-message-id route)
                       (assoc :reply_to_message_id (:reply-to-message-id route)))
            response (http-client/request {:method :post
                                           :url (str "https://api.telegram.org/bot"
                                                     token
                                                     "/sendMessage")
                                           :headers {"Content-Type" "application/json; charset=utf-8"}
                                           :body (json/write-json-str payload)})
            body     (try
                       (json/read-json (:body response))
                       (catch Exception _
                         nil))]
        (when-not (and (= 200 (:status response))
                       (true? (get body "ok")))
          (throw (ex-info "Telegram message delivery failed"
                          {:status (:status response)
                           :body body})))))
    true))

(defn- send-imessage-message!
  [route text]
  (when-not (mac-os?)
    (throw (ex-info "iMessage delivery is only available on macOS" {})))
  (let [chat-guid (:chat-guid route)
        text*     (normalize-outbound-text text)]
    (when (and chat-guid text*)
      (let [{:keys [exit err]} (apply shell/sh
                                      "osascript"
                                      (concat ["-e" "on run argv"
                                               "-e" "set messageText to item 1 of argv"
                                               "-e" "set chatGuid to item 2 of argv"
                                               "-e" "tell application \"Messages\" to send messageText to chat id chatGuid"
                                               "-e" "end run"
                                               "--"
                                               text*
                                               chat-guid]))]
        (when-not (zero? exit)
          (throw (ex-info "iMessage delivery failed"
                          {:exit exit
                           :error err})))
        true))))

(defn send-session-message!
  [channel session-id text]
  (when-let [text* (normalize-outbound-text text)]
    (let [route (session-meta session-id)]
      (case channel
        :slack (send-slack-message! route text*)
        :telegram (send-telegram-message! route text*)
        :imessage (send-imessage-message! route text*)
        nil))))

(defn- messaging-assistant-message
  [{:keys [session-id channel text]}]
  (when (contains? #{:slack :telegram :imessage} channel)
    (try
      (send-session-message! channel session-id text)
      (catch Exception e
        (log/warn e "Failed to deliver intermediate assistant message"
                  {:session-id session-id
                   :channel channel})))))

(defn- messaging-runtime-event
  [{:keys [session-id channel type data summary]}]
  (when (contains? #{:slack :telegram :imessage} channel)
    (when (= :message.assistant type)
      (let [text (or (nonblank-str (:text data))
                     (nonblank-str summary))]
        (when text
          (try
            (send-session-message! channel session-id text)
            (catch Exception e
              (log/warn e "Failed to deliver messaging runtime event"
                        {:session-id session-id
                         :channel channel
                         :type type}))))))))

(defn- handle-pending-interaction-reply!
  [session-id channel user-message]
  (when-let [interaction (current-pending-interaction session-id)]
    (let [text (some-> user-message str str/trim)
          result (cond
                   (interaction-cancelled? text) :cancel
                   (= :approval (:kind interaction)) (parse-approval-decision text)
                   (seq text) text
                   :else nil)]
      (if result
        (do
          (deliver (:response interaction) result)
          true)
        (do
          (send-session-message! channel
                                 session-id
                                 (case (:kind interaction)
                                   :approval "Still waiting for approval. Reply YES, NO, or CANCEL."
                                   "Still waiting for input. Reply with the requested value or CANCEL."))
          true)))))

(defn- handle-external-message!
  [{:keys [channel external-key external-meta external-message-id user-message label]}]
  (when-not (duplicate-external-message? external-message-id)
    (mark-external-message! external-message-id)
    (let [session-id (ensure-external-session! channel
                                               external-key
                                               external-meta
                                               :label label)]
      (if (handle-pending-interaction-reply! session-id channel user-message)
        true
        (do
          (persist-external-user-message! session-id channel user-message external-message-id)
          (try
            (agent/process-message session-id
                                   user-message
                                   :channel channel
                                   :persist-message? false)
            (catch Exception e
              (log/error e "Messaging channel processing failed"
                         {:channel channel
                          :session-id session-id
                          :external-key external-key})
              (try
                (send-session-message! channel session-id
                                       (str "I hit an error while processing that message: "
                                            (or (.getMessage e) "unknown error")))
                (catch Exception send-error
                  (log/warn send-error "Failed to deliver messaging error reply"
                            {:channel channel
                             :session-id session-id}))))))))))

(defn handle-slack-event!
  [body]
  (let [event      (or (get body "event") {})
        event-type (or (get event "type") "")
        subtype    (nonblank-str (get event "subtype"))
        text       (some-> (get event "text")
                           str
                           (str/replace #"<@[A-Z0-9]+>\s*" "")
                           str/trim
                           not-empty)
        team-id    (or (get body "team_id")
                       (get-in body ["authorizations" 0 "team_id"]))
        channel-id (get event "channel")
        thread-ts  (or (get event "thread_ts")
                       (get event "ts"))
        user-id    (get event "user")
        event-id   (get body "event_id")]
    (when (and (slack-enabled?)
               (#{"message" "app_mention"} event-type)
               (nil? subtype)
               text
               team-id
               channel-id
               user-id)
      (async/submit-background!
       "slack-message"
       #(handle-external-message!
         {:channel :slack
          :external-key (slack-conversation-key {:team-id team-id
                                                 :channel-id channel-id
                                                 :thread-ts thread-ts})
          :external-meta {:team-id team-id
                          :channel-id channel-id
                          :thread-ts thread-ts
                          :user-id user-id}
          :external-message-id (str "slack:" event-id)
          :user-message text
          :label (str "Slack " channel-id)})))))

(defn handle-telegram-update!
  [update]
  (let [message           (or (get update "message")
                              (get update "edited_message"))
        text              (or (nonblank-str (get message "text"))
                              (nonblank-str (get message "caption")))
        chat-id           (get-in message ["chat" "id"])
        message-id        (get message "message_id")
        message-thread-id (get message "message_thread_id")
        update-id         (get update "update_id")
        sender-is-bot?    (true? (get-in message ["from" "is_bot"]))
        label             (or (nonblank-str (get-in message ["chat" "title"]))
                              (nonblank-str (get-in message ["from" "username"]))
                              (when-let [first-name (nonblank-str (get-in message ["from" "first_name"]))]
                                (str "Telegram " first-name))
                              "Telegram")]
    (when (and (telegram-enabled?)
               message
               text
               chat-id
               message-id
               (not sender-is-bot?))
      (async/submit-background!
       "telegram-message"
       #(handle-external-message!
         {:channel :telegram
          :external-key (telegram-conversation-key {:chat-id chat-id
                                                    :message-thread-id message-thread-id})
          :external-meta {:chat-id chat-id
                          :message-thread-id message-thread-id
                          :reply-to-message-id message-id}
          :external-message-id (str "telegram:" update-id)
          :user-message text
          :label label})))))

(defn- sqlite3-query
  [sql]
  (let [{:keys [exit out err]} (shell/sh "sqlite3" imessage-chat-db-path sql)]
    (when-not (zero? exit)
      (throw (ex-info "sqlite3 query failed"
                      {:exit exit
                       :error err
                       :sql sql})))
    out))

(defn- current-imessage-max-rowid
  []
  (some-> (sqlite3-query "select coalesce(max(ROWID), 0) from message;")
          str/trim
          not-empty
          Long/parseLong
          long))

(defn- parse-json-lines
  [text]
  (->> (str/split-lines (or text ""))
       (keep nonblank-str)
       (mapv json/read-json)))

(defn- fetch-new-imessages
  [after-rowid]
  (let [sql (str "select json_object("
                 "'rowid', m.ROWID,"
                 "'message_guid', m.guid,"
                 "'chat_guid', c.guid,"
                 "'text', coalesce(m.text, ''),"
                 "'handle', coalesce(h.id, ''),"
                 "'service', c.service_name"
                 ") "
                 "from message m "
                 "join chat_message_join cmj on cmj.message_id = m.ROWID "
                 "join chat c on c.ROWID = cmj.chat_id "
                 "left join handle h on h.ROWID = m.handle_id "
                 "where m.ROWID > " (long after-rowid) " "
                 "and coalesce(m.is_from_me, 0) = 0 "
                 "and coalesce(m.is_system_message, 0) = 0 "
                 "and coalesce(m.text, '') <> '' "
                 "and c.service_name = 'iMessage' "
                 "order by m.ROWID asc "
                 "limit 100;")]
    (parse-json-lines (sqlite3-query sql))))

(defn- poll-imessage-once!
  []
  (when (and (imessage-enabled?)
             (mac-os?)
             (.exists (java.io.File. imessage-chat-db-path)))
    (try
      (let [after-rowid @imessage-last-rowid
            rows        (fetch-new-imessages after-rowid)]
        (when-let [max-rowid (some->> rows
                                      (map #(long (or (get % "rowid") 0)))
                                      seq
                                      (apply max))]
          (reset! imessage-last-rowid max-rowid))
        (doseq [row rows
                :let [text      (nonblank-str (get row "text"))
                      chat-guid (nonblank-str (get row "chat_guid"))
                      rowid     (get row "rowid")]
                :when (and text chat-guid rowid)]
          (async/submit-background!
           "imessage-message"
           #(handle-external-message!
             {:channel :imessage
              :external-key (imessage-conversation-key {:chat-guid chat-guid})
              :external-meta {:chat-guid chat-guid
                              :handle (nonblank-str (get row "handle"))
                              :service (nonblank-str (get row "service"))}
              :external-message-id (str "imessage:" (get row "message_guid"))
              :user-message text
              :label (or (nonblank-str (get row "handle"))
                          chat-guid)}))))
      (catch Exception e
        (log/warn e "iMessage poll failed")))))

(defn- stop-imessage-poller!
  []
  (when-let [^ScheduledExecutorService exec @imessage-poller-executor]
    (.shutdown exec)
    (try
      (.awaitTermination exec 2 TimeUnit/SECONDS)
      (catch InterruptedException _
        (.shutdownNow exec)))
    (reset! imessage-poller-executor nil)))

(defn- start-imessage-poller!
  []
  (stop-imessage-poller!)
  (when (and (imessage-enabled?)
             (mac-os?)
             (.exists (java.io.File. imessage-chat-db-path)))
    (reset! imessage-last-rowid (or (current-imessage-max-rowid) 0))
    (let [exec (Executors/newSingleThreadScheduledExecutor)]
      (.scheduleWithFixedDelay exec
                               ^Runnable poll-imessage-once!
                               (long (imessage-poll-interval-ms))
                               (long (imessage-poll-interval-ms))
                               TimeUnit/MILLISECONDS)
      (reset! imessage-poller-executor exec)
      true)))

(defn start!
  []
  (prompt/register-prompt! :slack messaging-prompt)
  (prompt/register-prompt! :telegram messaging-prompt)
  (prompt/register-prompt! :imessage messaging-prompt)
  (prompt/register-approval! :slack messaging-approval)
  (prompt/register-approval! :telegram messaging-approval)
  (prompt/register-approval! :imessage messaging-approval)
  (prompt/register-runtime-event! :slack messaging-runtime-event)
  (prompt/register-runtime-event! :telegram messaging-runtime-event)
  (prompt/register-runtime-event! :imessage messaging-runtime-event)
  (start-imessage-poller!))

(defn stop!
  []
  (stop-imessage-poller!)
  (prompt/register-prompt! :slack nil)
  (prompt/register-prompt! :telegram nil)
  (prompt/register-prompt! :imessage nil)
  (prompt/register-approval! :slack nil)
  (prompt/register-approval! :telegram nil)
  (prompt/register-approval! :imessage nil)
  (prompt/register-runtime-event! :slack nil)
  (prompt/register-runtime-event! :telegram nil)
  (prompt/register-runtime-event! :imessage nil)
  (reset! pending-interactions {}))
