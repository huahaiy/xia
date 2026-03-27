(ns xia.email
  "Gmail-backed email helpers for bundled tools.

   This namespace intentionally wraps the generic service proxy so email tools
   can offer a first-class UX without exposing raw credentials to tool code."
  (:require [clojure.string :as str]
            [xia.db :as db]
            [xia.service :as service])
  (:import [java.nio.charset StandardCharsets]
           [java.util Base64]
           [org.jsoup Jsoup]))

(def default-service-id :gmail)
(def ^:private default-max-results 10)
(def ^:private gmail-api-base-url "https://gmail.googleapis.com")

(defn- nonblank-str
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn- normalize-service-id
  [service-id]
  (cond
    (nil? service-id) nil
    (keyword? service-id) service-id
    (string? service-id) (some-> service-id nonblank-str keyword)
    :else (keyword (str service-id))))

(defn- service-id-text?
  [value]
  (boolean (re-matches #"[A-Za-z0-9._-]+"
                       (or (nonblank-str value) ""))))

(defn- normalize-base-url
  [base-url]
  (some-> base-url nonblank-str (str/replace #"/+$" "")))

(defn- gmail-service?
  [service]
  (= gmail-api-base-url
     (normalize-base-url (:service/base-url service))))

(defn- connected-oauth-account?
  [account]
  (boolean (or (nonblank-str (:oauth.account/access-token account))
               (nonblank-str (:oauth.account/refresh-token account)))))

(defn- gmail-oauth-account?
  [account]
  (= :gmail (:oauth.account/provider-template account)))

(defn- auto-gmail-oauth-account
  []
  (let [accounts   (->> (db/list-oauth-accounts)
                        (filter gmail-oauth-account?)
                        (sort-by #(some-> % :oauth.account/id name))
                        vec)
        connected  (into [] (filter connected-oauth-account?) accounts)]
    (cond
      (= 1 (count connected)) (first connected)
      (= 1 (count accounts))   (first accounts)
      :else                    nil)))

(defn- ensure-auto-gmail-service!
  []
  (when-let [account (auto-gmail-oauth-account)]
    (db/save-service! {:id            default-service-id
                       :name          "Gmail"
                       :base-url      gmail-api-base-url
                       :auth-type     :oauth-account
                       :oauth-account (:oauth.account/id account)})
    default-service-id))

(defn- detect-gmail-service-id
  []
  (or (when (some-> (db/get-service default-service-id) gmail-service?)
        default-service-id)
      (some->> (db/list-services)
               (filter gmail-service?)
               (sort-by #(some-> % :service/id name))
               first
               :service/id)
      (ensure-auto-gmail-service!)))

(defn- resolve-service-id
  [service-id]
  (let [candidate (normalize-service-id service-id)]
    (cond
      (nil? candidate)
      (or (detect-gmail-service-id)
          default-service-id)

      :else
      (let [service (db/get-service candidate)]
        (when (and service (not (gmail-service? service)))
          (throw (ex-info (str "Service " (name candidate)
                               " is not configured for Gmail")
                          {:service-id candidate
                           :base-url   (:service/base-url service)})))
        candidate))))

(defn- pad-base64url
  [s]
  (let [text      (or s "")
        remainder (int (mod (count text) 4))]
    (str text
         (case remainder
           0 ""
           2 "=="
           3 "="
           1 "==="
           ""))))

(defn- decode-base64url
  [data]
  (when-let [text (nonblank-str data)]
    (try
      (String. (.decode (Base64/getUrlDecoder)
                        ^String (pad-base64url text))
               StandardCharsets/UTF_8)
      (catch Exception _
        nil))))

(defn- encode-base64url
  [text]
  (let [text (str (or text ""))]
    (.encodeToString (.withoutPadding (Base64/getUrlEncoder))
                     (.getBytes ^String text StandardCharsets/UTF_8))))

(defn- sanitize-header-value
  [value]
  (some-> value
          str
          (str/replace #"\r?\n+" " ")
          str/trim))

(defn- html->text
  [html]
  (some-> html str Jsoup/parse (.text) nonblank-str))

(defn- part-tree
  [payload]
  (tree-seq map?
            #(seq (get % "parts"))
            payload))

(defn- payload-text-part
  [payload mime-type]
  (some->> (part-tree payload)
           (keep (fn [part]
                   (when (= mime-type
                            (some-> (get part "mimeType")
                                    str
                                    str/lower-case))
                     (some-> (get-in part ["body" "data"])
                             decode-base64url))))
           (map nonblank-str)
           (some identity)))

(defn- payload-body
  [message]
  (let [payload   (get message "payload")
        plain     (some-> (payload-text-part payload "text/plain")
                          nonblank-str)
        html-text (some-> (payload-text-part payload "text/html")
                          html->text)
        snippet   (some-> (get message "snippet") nonblank-str)]
    (cond
      plain     {:kind :plain :text plain}
      html-text {:kind :html :text html-text}
      snippet   {:kind :snippet :text snippet}
      :else     nil)))

(defn- header-map
  [message]
  (reduce (fn [m header]
            (let [header-name  (some-> (get header "name") nonblank-str str/lower-case)
                  header-value (some-> (get header "value") sanitize-header-value nonblank-str)]
              (if (and header-name header-value)
                (assoc m header-name header-value)
                m)))
          {}
          (or (get-in message ["payload" "headers"]) [])))

(defn- header-value
  [message header-name]
  (get (header-map message) (str/lower-case (name header-name))))

(defn- parse-long-safe
  [value]
  (when-let [text (nonblank-str value)]
    (try
      (Long/parseLong text)
      (catch Exception _
        nil))))

(defn- message-summary
  [message]
  {:id             (get message "id")
   :thread-id      (get message "threadId")
   :subject        (header-value message "subject")
   :from           (header-value message "from")
   :to             (header-value message "to")
   :cc             (header-value message "cc")
   :date           (header-value message "date")
   :message-id     (header-value message "message-id")
   :snippet        (or (get message "snippet") "")
   :labels         (vec (or (get message "labelIds") []))
   :unread?        (boolean (some #{"UNREAD"} (get message "labelIds")))
   :received-at-ms (parse-long-safe (get message "internalDate"))})

(defn- message-detail
  [message]
  (let [body (payload-body message)]
    (cond-> (message-summary message)
      body
      (assoc :body (:text body)
             :body-kind (:kind body)))))

(defn- format-query
  [query unread-only? inbox-only?]
  (let [terms [(when inbox-only? "in:inbox")
               (when unread-only? "is:unread")
               (nonblank-str query)]]
    (some->> terms
             (remove nil?)
             seq
             (str/join " "))))

(defn- list-request
  [{:keys [service-id query] :as opts}]
  (let [query* (nonblank-str query)
        service-id-text (when (string? service-id)
                          (nonblank-str service-id))
        service-id* (cond
                      (keyword? service-id)
                      service-id

                      (and service-id-text
                           (service-id-text? service-id-text))
                      (normalize-service-id service-id-text)

                      :else
                      nil)
        query** (if (and (nil? query*)
                         service-id-text
                         (nil? service-id*))
                  service-id-text
                  query*)]
    (assoc opts
           :service-id service-id*
           :query query**)))

(defn- gmail-request
  [service-id method path & {:as opts}]
  (apply service/request service-id method path (mapcat identity opts)))

(defn- fetch-message
  [service-id message-id format]
  (let [response (gmail-request service-id
                                :get
                                (str "/gmail/v1/users/me/messages/" message-id)
                                :query-params {"format" format})]
    (:body response)))

(defn list-messages
  "List recent Gmail messages. Returns summaries suitable for inbox/search use."
  [& {:keys [service-id query max-results unread-only? inbox-only?]
      :or   {max-results  default-max-results
             unread-only? false
             inbox-only?  true}}]
  (let [{service-id* :service-id
         query*      :query} (list-request {:service-id service-id
                                            :query query})
        service-id  (resolve-service-id service-id*)
        query-text  (format-query query* unread-only? inbox-only?)
        response    (gmail-request service-id
                                   :get
                                   "/gmail/v1/users/me/messages"
                                   :query-params (cond-> {"maxResults" (long max-results)}
                                                   query-text
                                                   (assoc "q" query-text)))
        ids         (or (get-in response [:body "messages"]) [])
        messages    (mapv (fn [entry]
                            (-> (fetch-message service-id (get entry "id") "metadata")
                                message-summary))
                          ids)]
    {:service-id            (name service-id)
     :query                 query-text
     :returned-count        (count messages)
     :result-size-estimate  (or (get-in response [:body "resultSizeEstimate"])
                                (count messages))
     :messages              messages}))

(defn read-message
  "Read a Gmail message by id, including the best available plain-text body."
  [message-id & {:keys [service-id]}]
  (let [service-id (resolve-service-id service-id)
        message    (fetch-message service-id message-id "full")]
    (assoc (message-detail message)
           :service-id (name service-id))))

(defn- address-header
  [label value]
  (when-let [text (some-> value sanitize-header-value nonblank-str)]
    (str label ": " text)))

(defn- references-header
  [value]
  (let [text (cond
               (string? value) (nonblank-str value)
               (sequential? value) (some->> value
                                            (map sanitize-header-value)
                                            (remove nil?)
                                            seq
                                            (str/join " "))
               :else (some-> value str nonblank-str))]
    (address-header "References" text)))

(defn- raw-message
  [{:keys [to cc bcc subject body reply-to in-reply-to references]}]
  (str/join "\r\n"
            (concat
              (remove nil?
                      [(address-header "To" to)
                       (address-header "Cc" cc)
                       (address-header "Bcc" bcc)
                       (address-header "Reply-To" reply-to)
                       (address-header "In-Reply-To" in-reply-to)
                       (references-header references)
                       (address-header "Subject" subject)
                       "MIME-Version: 1.0"
                       "Content-Type: text/plain; charset=UTF-8"
                       "Content-Transfer-Encoding: 8bit"])
              ["" (or body "")])))

(defn send-message
  "Send an email through Gmail using the configured Gmail service."
  [to subject body & {:keys [cc bcc reply-to in-reply-to references thread-id service-id]}]
  (let [service-id (resolve-service-id service-id)
        payload    (cond-> {:raw (encode-base64url
                                   (raw-message {:to          to
                                                 :cc          cc
                                                 :bcc         bcc
                                                 :subject     subject
                                                 :body        body
                                                 :reply-to    reply-to
                                                 :in-reply-to in-reply-to
                                                 :references  references}))}
                     (nonblank-str thread-id)
                     (assoc :threadId (nonblank-str thread-id)))
        response   (gmail-request service-id
                                  :post
                                  "/gmail/v1/users/me/messages/send"
                                  :body payload)]
    {:service-id (name service-id)
     :status     "sent"
     :id         (get-in response [:body "id"])
     :thread-id  (get-in response [:body "threadId"])
     :labels     (vec (or (get-in response [:body "labelIds"]) []))}))

(defn delete-message
  "Delete a Gmail message.

   By default this removes the message from the inbox and adds the Trash label.
   Pass :permanent? true to permanently delete it instead."
  [message-id & {:keys [service-id permanent?]}]
  (let [service-id (resolve-service-id service-id)
        message-id (nonblank-str message-id)]
    (when-not message-id
      (throw (ex-info "message-id is required"
                      {:type :email/missing-message-id})))
    (let [status   (if permanent? "deleted" "trashed")
          method   (if permanent? :delete :post)
          path     (if permanent?
                     (str "/gmail/v1/users/me/messages/" message-id)
                     (str "/gmail/v1/users/me/messages/" message-id "/modify"))
          opts     (when-not permanent?
                     {:body {"addLabelIds"    ["TRASH"]
                             "removeLabelIds" ["INBOX" "UNREAD"]}})
          response (apply gmail-request service-id method path (mapcat identity opts))
          body     (:body response)]
      {:service-id (name service-id)
       :status     status
       :id         (or (get body "id") message-id)
       :thread-id  (get body "threadId")
       :labels     (vec (or (get body "labelIds") []))})))
