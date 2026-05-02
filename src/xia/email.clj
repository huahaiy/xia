(ns xia.email
  "Email helpers for bundled tools.

   Public email operations are provider-neutral and dispatch through an
   `EmailBackend` protocol. Gmail, Microsoft Graph mail, and IMAP/SMTP are
   supported today, and the contract is
   designed so additional API-backed or protocol-backed mail providers can be
   added without changing the tool-facing call surface."
  (:require [charred.api :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [xia.artifact :as artifact]
            [xia.db :as db]
            [xia.service :as service])
  (:import [jakarta.mail Address Flags Flags$Flag Folder Message Message$RecipientType Multipart Part Session Transport UIDFolder]
           [jakarta.mail.internet InternetAddress MimeMessage MimeUtility]
           [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.net URI URLEncoder]
           [java.nio.charset StandardCharsets]
           [java.time Instant]
           [java.util Base64 Date Properties UUID]
           [org.jsoup Jsoup]))

(def default-service-id :gmail)
(def ^:private microsoft-mail-service-id :microsoft-mail)
(def ^:private imap-smtp-service-id :imap-smtp-mail)
(def ^:private default-max-results 10)
(def ^:private default-max-attachment-bytes (* 256 1024))
(def ^:private default-max-saved-attachment-bytes (* 25 1024 1024))
(def ^:private gmail-api-base-url "https://gmail.googleapis.com")
(def ^:private microsoft-graph-root-url "https://graph.microsoft.com")
(def ^:private microsoft-graph-api-base-url "https://graph.microsoft.com/v1.0")
(def ^:private microsoft-max-inline-attachment-bytes (* 3 1024 1024))
(def ^:private microsoft-immutable-id-prefer "IdType=\"ImmutableId\"")
(def ^:private microsoft-filtered-page-token-prefix "msmail:")
(def ^:private imap-message-page-token-prefix "imapmsg:")
(def ^:private imap-draft-page-token-prefix "imapdraft:")
(def ^:private imap-id-prefix "imap:")
(def ^:private imap-schemes #{"imap" "imaps"})
(def ^:private smtp-schemes #{"smtp" "smtps"})
(def ^:private default-attachment-media-type "application/octet-stream")
(def ^:private textual-attachment-media-types
  #{"application/json"
    "application/xml"
    "application/javascript"
    "application/x-javascript"
    "application/x-sh"
    "application/x-yaml"
    "application/yaml"})
(def ^:private media-type->extension
  {"application/json" "json"
   "application/octet-stream" "bin"
   "application/pdf" "pdf"
   "application/xml" "xml"
   "application/zip" "zip"
   "image/gif" "gif"
   "image/jpeg" "jpg"
   "image/png" "png"
   "image/svg+xml" "svg"
   "text/calendar" "ics"
   "text/csv" "csv"
   "text/html" "html"
   "text/markdown" "md"
   "text/plain" "txt"})

(def ^:private imap-default-folders
  {:inbox   ["INBOX"]
   :drafts  ["Drafts" "INBOX/Drafts" "INBOX.Drafts" "[Gmail]/Drafts"]
   :sent    ["Sent" "Sent Items" "INBOX/Sent" "INBOX.Sent" "[Gmail]/Sent Mail"]
   :archive ["Archive" "Archives" "INBOX/Archive" "INBOX.Archive" "[Gmail]/All Mail"]
   :trash   ["Trash" "Deleted Items" "Deleted Messages" "INBOX/Trash" "INBOX.Trash" "[Gmail]/Trash"]})

(defprotocol EmailBackend
  (backend-key [backend]
    "Stable backend identifier keyword.")
  (backend-label [backend]
    "Human-readable backend label for errors and diagnostics.")
  (backend-default-service-id [backend]
    "Default service id for the backend when no configured service is detected.")
  (supports-service? [backend service]
    "Whether a saved service belongs to this backend.")
  (auto-detect-service-id [backend]
    "Return a configured service id for this backend, creating one if the backend
     supports an auto-managed service and the current account state allows it.")
  (backend-list-labels [backend service-id opts]
    "List labels for the backend.")
  (backend-list-messages [backend service-id opts]
    "List recent messages for the backend.")
  (backend-read-message [backend service-id message-id opts]
    "Read one message from the backend.")
  (backend-send-message [backend service-id to subject body opts]
    "Send one message through the backend.")
  (backend-delete-message [backend service-id message-id opts]
    "Delete or trash one message through the backend.")
  (backend-update-message [backend service-id message-id opts]
    "Update message or thread labels/state through the backend.")
  (backend-list-drafts [backend service-id opts]
    "List drafts for the backend.")
  (backend-read-draft [backend service-id draft-id opts]
    "Read one draft from the backend.")
  (backend-save-draft [backend service-id to subject body opts]
    "Create or update one draft through the backend.")
  (backend-send-draft [backend service-id draft-id opts]
    "Send one existing draft through the backend.")
  (backend-delete-draft [backend service-id draft-id opts]
    "Delete one draft through the backend."))

(defn- nonblank-str
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn- nonblank-text
  [value]
  (let [s (some-> value str)]
    (when (seq (str/trim (or s "")))
      s)))

(defn- value-of
  [m key]
  (or (get m key)
      (when (keyword? key)
        (get m (name key)))
      (when (string? key)
        (get m (keyword key)))))

(defn- present?
  [m key]
  (or (contains? m key)
      (when (keyword? key)
        (contains? m (name key)))
      (when (string? key)
        (contains? m (keyword key)))))

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

(defn- uri-scheme
  [value]
  (some-> value nonblank-str URI. .getScheme str/lower-case))

(defn- gmail-service?
  [service]
  (= gmail-api-base-url
     (normalize-base-url (:service/base-url service))))

(defn- microsoft-graph-service?
  [service]
  (contains? #{microsoft-graph-root-url
               microsoft-graph-api-base-url}
             (normalize-base-url (:service/base-url service))))

(defn- service-id-value
  [service]
  (or (:service/id service)
      (:id service)))

(defn- service-oauth-account-id
  [service]
  (or (:service/oauth-account service)
      (:oauth-account service)))

(defn- service-oauth-provider-template
  [service]
  (some-> (service-oauth-account-id service)
          db/get-oauth-account
          :oauth.account/provider-template))

(defn- service-email-backend
  [service]
  (or (:service/email-backend service)
      (:email-backend service)))

(defn- service-smtp-url
  [service]
  (or (:service/smtp-url service)
      (:smtp-url service)))

(defn- service-auth-username
  [service]
  (or (:service/auth-username service)
      (:auth-username service)))

(defn- service-email-address
  [service]
  (or (:service/email-address service)
      (:email-address service)))

(defn- microsoft-mail-service?
  [service]
  (and (microsoft-graph-service? service)
       (or (= microsoft-mail-service-id
              (normalize-service-id (service-id-value service)))
           (= :microsoft-mail
              (service-oauth-provider-template service)))))

(defn- imap-service?
  [service]
  (let [backend     (service-email-backend service)
        imap-scheme (some-> (:service/base-url service) uri-scheme)
        smtp-scheme (some-> (service-smtp-url service) uri-scheme)]
    (or (= :imap-smtp backend)
        (and (contains? imap-schemes imap-scheme)
             (contains? smtp-schemes smtp-scheme)))))

(defn- connected-oauth-account?
  [account]
  (boolean (or (nonblank-str (:oauth.account/access-token account))
               (nonblank-str (:oauth.account/refresh-token account)))))

(defn- gmail-oauth-account?
  [account]
  (= :gmail (:oauth.account/provider-template account)))

(defn- microsoft-mail-oauth-account?
  [account]
  (= :microsoft-mail (:oauth.account/provider-template account)))

(defn- auto-gmail-oauth-account
  []
  (let [accounts  (->> (db/list-oauth-accounts)
                       (filter gmail-oauth-account?)
                       (sort-by #(some-> % :oauth.account/id name))
                       vec)
        connected (into [] (filter connected-oauth-account?) accounts)]
    (cond
      (= 1 (count connected)) (first connected)
      (= 1 (count accounts))  (first accounts)
      :else                   nil)))

(defn- ensure-auto-gmail-service!
  []
  (when-let [account (auto-gmail-oauth-account)]
    (db/save-service! {:id            default-service-id
                       :name          "Gmail"
                       :base-url      gmail-api-base-url
                       :auth-type     :oauth-account
                       :oauth-account (:oauth.account/id account)
                       :rate-limit-per-minute service/gmail-rate-limit-per-minute
                       :autonomous-approved? true})
    default-service-id))

(defn- auto-microsoft-mail-oauth-account
  []
  (let [accounts  (->> (db/list-oauth-accounts)
                       (filter microsoft-mail-oauth-account?)
                       (sort-by #(some-> % :oauth.account/id name))
                       vec)
        connected (into [] (filter connected-oauth-account?) accounts)]
    (cond
      (= 1 (count connected)) (first connected)
      (= 1 (count accounts))  (first accounts)
      :else                   nil)))

(defn- ensure-auto-microsoft-mail-service!
  []
  (when-let [account (auto-microsoft-mail-oauth-account)]
    (db/save-service! {:id            microsoft-mail-service-id
                       :name          "Microsoft Mail"
                       :base-url      microsoft-graph-api-base-url
                       :auth-type     :oauth-account
                       :oauth-account (:oauth.account/id account)
                       :autonomous-approved? true})
    microsoft-mail-service-id))

(defn- gmail-detect-service-id
  []
  (or (when (some-> (db/get-service default-service-id) gmail-service?)
        default-service-id)
      (some->> (db/list-services)
               (filter gmail-service?)
               (sort-by #(some-> % :service/id name))
               first
               :service/id)
      (ensure-auto-gmail-service!)))

(defn- microsoft-detect-service-id
  []
  (or (when (some-> (db/get-service microsoft-mail-service-id) microsoft-mail-service?)
        microsoft-mail-service-id)
      (some->> (db/list-services)
               (filter microsoft-mail-service?)
               (sort-by #(some-> % :service/id name))
               first
               :service/id)
      (ensure-auto-microsoft-mail-service!)))

(defn- imap-smtp-detect-service-id
  []
  (let [services (->> (db/list-services)
                      (filter imap-service?)
                      (sort-by #(some-> % :service/id name))
                      vec)]
    (when (= 1 (count services))
      (:service/id (first services)))))

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

(defn- decode-base64url-bytes
  [data]
  (when-let [text (nonblank-str data)]
    (try
      (.decode (Base64/getUrlDecoder) ^String (pad-base64url text))
      (catch Exception _
        nil))))

(defn- encode-base64url
  [text]
  (let [text (str (or text ""))]
    (.encodeToString (.withoutPadding (Base64/getUrlEncoder))
                     (.getBytes ^String text StandardCharsets/UTF_8))))

(defn- decode-base64
  [text]
  (try
    (.decode (Base64/getDecoder) ^String text)
    (catch IllegalArgumentException e
      (throw (ex-info "attachment bytes_base64 must be valid base64"
                      {:type :email/invalid-attachment-bytes-base64}
                      e)))))

(defn- encode-base64-bytes
  [^bytes data]
  (.encodeToString (Base64/getEncoder) data))

(defn- encode-mime-base64
  [^bytes data]
  (.encodeToString
    (Base64/getMimeEncoder 76 (.getBytes "\r\n" StandardCharsets/UTF_8))
    data))

(defn- sanitize-header-value
  [value]
  (some-> value
          str
          (str/replace #"\r?\n+" " ")
          str/trim))

(def ^:private structured-body-keys
  #{"bcc"
    "body"
    "bodyText"
    "body_text"
    "cc"
    "content"
    "draft"
    "email"
    "inReplyTo"
    "in_reply_to"
    "mail"
    "message"
    "parts"
    "plainText"
    "plain_text"
    "recipient"
    "recipients"
    "references"
    "replyTo"
    "reply_to"
    "serviceId"
    "service_id"
    "subject"
    "text"
    "threadId"
    "thread_id"
    "to"})

(def ^:private extractable-body-keys
  ["body"
   "bodyText"
   "body_text"
   "content"
   "draft"
   "email"
   "mail"
   "message"
   "plainText"
   "plain_text"
   "text"])

(declare extract-structured-body-text)

(defn- key-text
  [value]
  (cond
    (string? value) value
    (keyword? value) (name value)
    :else (some-> value str nonblank-str)))

(defn- structured-body-payload?
  [value]
  (cond
    (map? value)
    (let [keys* (into #{} (keep key-text) (keys value))]
      (or (some structured-body-keys keys*)
          (some structured-body-payload? (vals value))))

    (sequential? value)
    (boolean (some structured-body-payload? (take 5 value)))

    :else
    false))

(defn- extract-structured-body-text
  [value]
  (cond
    (nil? value)
    nil

    (string? value)
    (nonblank-text value)

    (map? value)
    (some->> extractable-body-keys
             (map #(value-of value %))
             (map extract-structured-body-text)
             (remove nil?)
             first)

    (sequential? value)
    (let [parts (->> value
                     (map extract-structured-body-text)
                     (remove nil?)
                     vec)]
      (when (seq parts)
        (str/join "\n\n" parts)))

    :else
    (nonblank-text value)))

(defn- maybe-json-body-payload
  [text]
  (let [trimmed   (some-> text str/trim)
        candidate (or (second (re-matches #"(?is)```(?:json)?\s*(.+?)\s*```" (or trimmed "")))
                      (when (or (str/starts-with? (or trimmed "") "{")
                                (str/starts-with? (or trimmed "") "["))
                        trimmed))]
    (when candidate
      (try
        (json/read-json candidate)
        (catch Exception _
          nil)))))

(defn- body-preview
  [body]
  (let [text (pr-str body)]
    (subs text 0 (min 160 (count text)))))

(defn- invalid-body-error
  [body]
  (throw (ex-info "email body must be plain text, not a JSON wrapper or structured tool payload"
                  {:type         :email/invalid-body
                   :body-preview (body-preview body)})))

(defn- normalize-message-body
  [body]
  (loop [value body
         depth 0]
    (when (> depth 4)
      (invalid-body-error body))
    (cond
      (nil? value)
      ""

      (string? value)
      (if-let [parsed (some-> value maybe-json-body-payload)]
        (if (structured-body-payload? parsed)
          (if-let [text (extract-structured-body-text parsed)]
            (recur text (inc depth))
            (invalid-body-error body))
          value)
        value)

      (or (map? value) (sequential? value))
      (if-let [text (extract-structured-body-text value)]
        (recur text (inc depth))
        (invalid-body-error body))

      :else
      (str value))))

(defn- html->text
  [html]
  (some-> html str Jsoup/parse (.text) nonblank-str))

(defn- normalize-html-body
  [html-body]
  (some-> html-body str))

(defn- utf8-bytes
  [text]
  (.getBytes ^String (str (or text "")) StandardCharsets/UTF_8))

(defn- normalize-crlf
  [text]
  (-> (or text "")
      (str/replace #"\r?\n" "\r\n")))

(defn- base-media-type
  [media-type]
  (some-> media-type
          str
          str/lower-case
          (str/split #";")
          first
          str/trim
          not-empty))

(defn- textual-media-type?
  [media-type]
  (let [base (base-media-type media-type)]
    (or (some-> base (str/starts-with? "text/"))
        (contains? textual-attachment-media-types base))))

(defn- filename-extension
  [filename]
  (let [name (some-> filename str str/trim)
        idx  (when (seq name) (.lastIndexOf ^String name "."))]
    (when (and (some? idx) (pos? (long idx)) (< (long idx) (dec (long (count name)))))
      (-> (.substring ^String name (inc (long idx)))
          str/lower-case
          str/trim
          not-empty))))

(defn- default-filename
  [idx media-type]
  (let [ext (or (some-> media-type base-media-type media-type->extension)
                "bin")]
    (str "attachment-" (inc (long idx)) "." ext)))

(defn- escape-header-param
  [text]
  (-> (or text "")
      (str/replace #"\\" "\\\\")
      (str/replace #"\"" "\\\"")))

(defn- attachment-bytes
  [spec]
  (cond
    (present? spec :bytes)
    (let [value (value-of spec :bytes)]
      (cond
        (instance? (class (byte-array 0)) value) value
        (string? value) (utf8-bytes value)
        :else (throw (ex-info "attachment bytes must be a byte array or string"
                              {:type :email/invalid-attachment-bytes}))))

    (present? spec :bytes-base64)
    (let [value (value-of spec :bytes-base64)]
      (when-not (string? value)
        (throw (ex-info "attachment bytes_base64 must be a base64 string"
                        {:type :email/invalid-attachment-bytes-base64})))
      (decode-base64 value))

    (present? spec :content)
    (utf8-bytes (str (value-of spec :content)))

    :else
    nil))

(defn- load-artifact-attachment
  [artifact-id]
  (or (artifact/visible-artifact-download-data artifact-id)
      (throw (ex-info "attachment artifact not found"
                      {:type :email/attachment-artifact-not-found
                       :artifact-id (str artifact-id)}))))

(defn- normalize-attachment
  [spec idx]
  (let [artifact-id (or (value-of spec :artifact-id)
                        (value-of spec "artifact_id"))
        artifact*   (when artifact-id
                      (load-artifact-attachment artifact-id))
        ^bytes data (or (some-> artifact* :bytes)
                        (attachment-bytes spec))]
    (when-not data
      (throw (ex-info "attachment requires artifact_id, content, bytes, or bytes_base64"
                      {:type :email/missing-attachment-content
                       :attachment spec})))
    (let [media-type (or (base-media-type (value-of spec :media-type))
                         (base-media-type (value-of spec "media_type"))
                         (some-> artifact* :media-type base-media-type)
                         (when (present? spec :content)
                           "text/plain")
                         default-attachment-media-type)
          filename   (or (nonblank-str (value-of spec :filename))
                         (nonblank-str (value-of spec "file_name"))
                         (some-> artifact* :name nonblank-str)
                         (default-filename idx media-type))
          inline?    (boolean (or (value-of spec :inline?)
                                  (value-of spec "inline")
                                  false))]
      {:filename   filename
       :media-type media-type
       :inline?    inline?
       :bytes      data
       :size-bytes (long (alength data))})))

(defn- normalize-attachments
  [attachments]
  (when (seq attachments)
    (mapv normalize-attachment attachments (range))))

(defn- mime-boundary
  [prefix]
  (str "xia-" prefix "-" (UUID/randomUUID)))

(defn- part-header
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
    (part-header "References" text)))

(defn- render-leaf-part
  [{:keys [content-type transfer-encoding disposition filename body extra-headers]}]
  (str/join
    "\r\n"
    (concat
      [(str "Content-Type: " content-type)]
      (when transfer-encoding
        [(str "Content-Transfer-Encoding: " transfer-encoding)])
      (when disposition
        [(str "Content-Disposition: " disposition
              (when filename
                (str "; filename=\"" (escape-header-param filename) "\"")))])
      extra-headers
      ["" body])))

(declare render-part)

(defn- render-multipart-part
  [{:keys [content-type boundary parts extra-headers]}]
  (let [body (str/join
               "\r\n"
               (concat
                 (mapcat (fn [part]
                           [(str "--" boundary)
                            (render-part part)])
                         parts)
                 [(str "--" boundary "--")]))]
    (str/join
      "\r\n"
      (concat
        [(str "Content-Type: " content-type "; boundary=\"" boundary "\"")]
        extra-headers
        ["" body]))))

(defn- render-part
  [part]
  (if (:parts part)
    (render-multipart-part part)
    (render-leaf-part part)))

(defn- text-part
  [content-type body]
  {:content-type content-type
   :transfer-encoding "8bit"
   :body (or body "")})

(defn- attachment-part
  [{:keys [filename media-type inline? bytes]}]
  {:content-type (str media-type "; name=\"" (escape-header-param filename) "\"")
   :transfer-encoding "base64"
   :disposition (if inline? "inline" "attachment")
   :filename filename
   :body (encode-mime-base64 bytes)})

(defn- message-body-part
  [body html-body attachments]
  (let [plain-body    (normalize-message-body body)
        html-body*    (normalize-html-body html-body)
        plain-fallback (if (and (str/blank? plain-body) (seq html-body*))
                         (or (html->text html-body*) "")
                         plain-body)
        content-part  (cond
                        (and (seq html-body*) (seq plain-fallback))
                        {:content-type "multipart/alternative"
                         :boundary     (mime-boundary "alt")
                         :parts        [(text-part "text/plain; charset=UTF-8" plain-fallback)
                                        (text-part "text/html; charset=UTF-8" html-body*)]}

                        (seq html-body*)
                        (text-part "text/html; charset=UTF-8" html-body*)

                        :else
                        (text-part "text/plain; charset=UTF-8" plain-fallback))]
    (if (seq attachments)
      {:content-type "multipart/mixed"
       :boundary     (mime-boundary "mixed")
       :parts        (into [content-part]
                           (map attachment-part attachments))}
      content-part)))

(defn- raw-message
  [{:keys [to cc bcc subject body reply-to in-reply-to references html-body attachments]}]
  (let [body-part (message-body-part body html-body attachments)
        headers   (remove nil?
                          [(part-header "To" to)
                           (part-header "Cc" cc)
                           (part-header "Bcc" bcc)
                           (part-header "Reply-To" reply-to)
                           (part-header "In-Reply-To" in-reply-to)
                           (references-header references)
                           (part-header "Subject" subject)
                           "MIME-Version: 1.0"])]
    (str (str/join "\r\n" headers)
         "\r\n"
         (render-part body-part))))

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

(defn- payload-html-part
  [payload]
  (some-> (payload-text-part payload "text/html")
          nonblank-text))

(defn- payload-body
  [message]
  (let [payload    (get message "payload")
        plain      (some-> (payload-text-part payload "text/plain")
                           nonblank-str)
        html       (payload-html-part payload)
        html-text  (some-> html html->text)
        snippet    (some-> (get message "snippet") nonblank-str)]
    (cond
      plain
      {:kind :plain
       :text plain
       :html html}

      html-text
      {:kind :html
       :text html-text
       :html html}

      snippet
      {:kind :snippet
       :text snippet}

      :else
      nil)))

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

(defn- part-header-map
  [part]
  (reduce (fn [m header]
            (let [header-name  (some-> (get header "name") nonblank-str str/lower-case)
                  header-value (some-> (get header "value") sanitize-header-value nonblank-str)]
              (if (and header-name header-value)
                (assoc m header-name header-value)
                m)))
          {}
          (or (get part "headers") [])))

(defn- header-value
  [message header-name]
  (get (header-map message) (str/lower-case (name header-name))))

(defn- parse-long-safe
  [value]
  (cond
    (integer? value)
    (long value)

    (number? value)
    (long value)

    :else
    (when-let [text (nonblank-str value)]
      (try
        (Long/parseLong text)
        (catch Exception _
          nil)))))

(defn- parse-instant-ms
  [value]
  (when-let [text (nonblank-str value)]
    (try
      (.toEpochMilli (Instant/parse text))
      (catch Exception _
        nil))))

(defn- recipient-address
  [recipient]
  (let [email-address (or (get-in recipient ["emailAddress" "address"])
                          (get-in recipient [:emailAddress :address]))
        display-name  (or (get-in recipient ["emailAddress" "name"])
                          (get-in recipient [:emailAddress :name]))
        address       (nonblank-str email-address)
        name          (nonblank-str display-name)]
    (cond
      (and name address (not= (str/lower-case name)
                              (str/lower-case address)))
      (str name " <" address ">")

      address
      address

      :else
      name)))

(defn- recipient-header-address
  [recipient]
  (let [email-address (or (get-in recipient ["emailAddress" "address"])
                          (get-in recipient [:emailAddress :address]))
        display-name  (or (get-in recipient ["emailAddress" "name"])
                          (get-in recipient [:emailAddress :name]))
        address       (nonblank-str email-address)
        name          (nonblank-str display-name)
        rendered-name (when name
                        (if (re-find #"[;,]" name)
                          (str "\"" (escape-header-param name) "\"")
                          name))]
    (cond
      (and rendered-name address
           (not= (str/lower-case name)
                 (str/lower-case address)))
      (str rendered-name " <" address ">")

      address
      address

      :else
      rendered-name)))

(defn- recipient-list
  [recipients]
  (some->> recipients
           (keep recipient-address)
           seq
           (str/join ", ")))

(defn- recipient-header-list
  [recipients]
  (some->> recipients
           (keep recipient-header-address)
           seq
           (str/join ", ")))

(defn- provided-value?
  [value]
  (cond
    (string? value)
    (boolean (nonblank-str value))

    (sequential? value)
    (boolean (seq value))

    :else
    (some? value)))

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

(defn- gmail-request
  [service-id method path & {:as opts}]
  (apply service/request (normalize-service-id service-id) method path (mapcat identity opts)))

(defn- microsoft-api-prefix
  [service-id]
  (let [base-url (some-> service-id normalize-service-id db/get-service :service/base-url normalize-base-url)]
    (if (= base-url microsoft-graph-root-url)
      "/v1.0"
      "")))

(defn- microsoft-request-path
  [service-id path]
  (str (microsoft-api-prefix service-id) path))

(defn- microsoft-request
  [service-id method path & {:as opts}]
  (let [headers (cond
                  (nil? (:headers opts))
                  {"Prefer" microsoft-immutable-id-prefer}

                  (contains? (:headers opts) "Prefer")
                  (update (:headers opts) "Prefer"
                          (fn [value]
                            (let [parts (->> (str/split (str value) #"\s*,\s*")
                                             (remove str/blank?)
                                             vec)]
                              (if (some #{microsoft-immutable-id-prefer} parts)
                                value
                                (str value ", " microsoft-immutable-id-prefer)))))

                  :else
                  (assoc (:headers opts) "Prefer" microsoft-immutable-id-prefer))]
    (apply service/request
           (normalize-service-id service-id)
           method
           (microsoft-request-path service-id path)
           (mapcat identity (assoc opts :headers headers)))))

(defn- encode-query-param
  [value]
  (URLEncoder/encode (str value) "UTF-8"))

(defn- path-with-query-params
  [path query-params]
  (let [pairs (->> (or query-params {})
                   (map (fn [[k v]]
                          (str (encode-query-param k)
                               "="
                               (encode-query-param v))))
                   seq)]
    (if pairs
      (str path "?" (str/join "&" pairs))
      path)))

(defn- microsoft-filtered-page-token
  [page-path skip-filtered next-page-path]
  (str microsoft-filtered-page-token-prefix
       (encode-base64url
         (json/write-json-str {"page_path"       page-path
                               "skip_filtered"  (long skip-filtered)
                               "next_page_path" next-page-path}))))

(defn- microsoft-page-path
  [service-id page-token]
  (when-let [token (nonblank-str page-token)]
    (if (str/starts-with? token microsoft-filtered-page-token-prefix)
      (throw (ex-info "Filtered Microsoft page tokens must be decoded before path resolution"
                      {:page-token token}))
      (if (str/starts-with? token "/")
        token
        (let [uri           (java.net.URI. token)
              host          (some-> (.getHost uri) str/lower-case)
              request-path  (or (.getRawPath uri) "")
              request-query (.getRawQuery uri)
              base-path     (or (some-> service-id normalize-service-id db/get-service :service/base-url java.net.URI. .getPath)
                                "")
              strip-path    (if (seq base-path) base-path "/v1.0")
              relative-path (cond
                              (str/starts-with? request-path strip-path)
                              (subs request-path (count strip-path))

                              :else
                              request-path)
              relative-path (if (seq relative-path) relative-path "/")]
          (when-not (= "graph.microsoft.com" host)
            (throw (ex-info "Unsupported Microsoft Graph page token host"
                            {:page-token token
                             :host       host})))
          (str relative-path
               (when (seq request-query)
                 (str "?" request-query))))))))

(defn- microsoft-page-state
  [service-id initial-page-path page-token]
  (if-let [token (nonblank-str page-token)]
    (if (str/starts-with? token microsoft-filtered-page-token-prefix)
      (let [payload (some-> token
                            (subs (count microsoft-filtered-page-token-prefix))
                            decode-base64url
                            json/read-json)
            page-path (some-> (get payload "page_path")
                              nonblank-str
                              (#(microsoft-page-path service-id %)))
            skip-filtered (or (parse-long-safe (get payload "skip_filtered")) 0)
            next-page-path (some-> (get payload "next_page_path")
                                   nonblank-str
                                   (#(microsoft-page-path service-id %)))]
        (when-not page-path
          (throw (ex-info "Invalid Microsoft filtered page token"
                          {:page-token token})))
        {:page-path       page-path
         :skip-filtered   (long skip-filtered)
         :next-page-path  next-page-path})
      {:page-path      (microsoft-page-path service-id token)
       :skip-filtered  0
       :next-page-path nil})
    {:page-path      initial-page-path
     :skip-filtered  0
     :next-page-path nil}))

(defn- gmail-fetch-message
  [service-id message-id format]
  (let [response (gmail-request service-id
                                :get
                                (str "/gmail/v1/users/me/messages/" message-id)
                                :query-params {"format" format})]
    (:body response)))

(defn- gmail-fetch-draft
  [service-id draft-id format]
  (let [response (gmail-request service-id
                                :get
                                (str "/gmail/v1/users/me/drafts/" draft-id)
                                :query-params {"format" format})]
    (:body response)))

(defn- gmail-fetch-attachment
  [service-id message-id attachment-id]
  (let [response (gmail-request service-id
                                :get
                                (str "/gmail/v1/users/me/messages/" message-id "/attachments/" attachment-id))]
    (:body response)))

(defn- message-attachment-parts
  [message]
  (let [payload (get message "payload")]
    (->> (part-tree payload)
         (keep (fn [part]
                 (let [filename (nonblank-str (get part "filename"))
                       body     (get part "body")]
                   (when (or filename
                             (nonblank-str (get body "attachmentId")))
                     part)))))))

(defn- attachment-content
  [service-id message-id part max-attachment-bytes]
  (let [body          (or (get part "body") {})
        attachment-id (nonblank-str (get body "attachmentId"))
        inline-data   (decode-base64url-bytes (get body "data"))
        data          (or inline-data
                          (when attachment-id
                            (some-> (gmail-fetch-attachment service-id message-id attachment-id)
                                    (get "data")
                                    decode-base64url-bytes)))
        size          (long (or (parse-long-safe (get body "size"))
                                (some-> data alength)
                                0))]
    (cond
      (nil? data)
      {:content-available? false}

      (> size (long max-attachment-bytes))
      {:content-available? true
       :content-included?  false
       :content-reason     "attachment exceeds max_attachment_bytes"}

      :else
      {:content-available? true
       :content-included?  true
       :bytes              data})))

(defn- attachment-summary
  [service-id message-id part {:keys [include-attachment-data? max-attachment-bytes]}]
  (let [headers         (part-header-map part)
        mime-type       (some-> (get part "mimeType") nonblank-str)
        filename        (nonblank-str (get part "filename"))
        body            (or (get part "body") {})
        attachment-id   (nonblank-str (get body "attachmentId"))
        size            (or (parse-long-safe (get body "size")) 0)
        disposition     (get headers "content-disposition")
        inline?         (or (boolean (and disposition
                                          (str/starts-with? (str/lower-case disposition) "inline")))
                            (boolean (re-find #"(?i)\binline\b" (or disposition ""))))
        base-summary    {:part-id        (get part "partId")
                         :filename       filename
                         :mime-type      mime-type
                         :size-bytes     size
                         :attachment-id  attachment-id
                         :inline?        inline?
                         :content-id     (get headers "content-id")
                         :disposition    disposition}]
    (if-not include-attachment-data?
      base-summary
      (let [{:keys [content-available? content-included? content-reason bytes]}
            (attachment-content service-id message-id part max-attachment-bytes)]
        (cond-> base-summary
          (some? content-available?)
          (assoc :content-available? content-available?)

          (some? content-included?)
          (assoc :content-included? content-included?)

          content-reason
          (assoc :content-reason content-reason)

          (and bytes (textual-media-type? mime-type))
          (assoc :content-text (String. ^bytes bytes StandardCharsets/UTF_8))

          (and bytes (not (textual-media-type? mime-type)))
          (assoc :content-bytes-base64 (.encodeToString (Base64/getEncoder) ^bytes bytes)))))))

(defn- message-detail
  [service-id message opts]
  (let [body        (payload-body message)
        attachments (->> (message-attachment-parts message)
                         (mapv #(attachment-summary service-id (get message "id") % opts)))]
    (cond-> (message-summary message)
      body
      (assoc :body (:text body)
             :body-kind (:kind body))

      (seq (:html body))
      (assoc :html-body (:html body))

      (seq attachments)
      (assoc :attachments attachments))))

(def ^:private attachment-content-result-keys
  [:content-text :content-bytes-base64])

(defn- attachment-result-bytes
  [attachment]
  (cond
    (string? (:content-text attachment))
    (utf8-bytes (:content-text attachment))

    (string? (:content-bytes-base64 attachment))
    (decode-base64 (:content-bytes-base64 attachment))

    :else
    nil))

(defn- attachment-artifact-name
  [attachment idx]
  (or (nonblank-str (:filename attachment))
      (default-filename idx (:mime-type attachment))))

(defn- save-attachment-artifact!
  [attachment service-id source-id source-kind idx]
  (if-let [bytes (attachment-result-bytes attachment)]
    (let [name      (attachment-artifact-name attachment idx)
          artifact* (artifact/create-artifact!
                      {:name       name
                       :title      name
                       :media-type (:mime-type attachment)
                       :bytes      bytes
                       :source     :email-attachment
                       :meta       {:email/service-id   (clojure.core/name service-id)
                                    :email/source-kind  (clojure.core/name source-kind)
                                    :email/source-id    source-id
                                    :email/attachment-id (:attachment-id attachment)
                                    :email/part-id      (:part-id attachment)
                                    :email/filename     (:filename attachment)
                                    :email/mime-type    (:mime-type attachment)}})]
      (assoc attachment
             :artifact-id (str (:id artifact*))
             :artifact-name (:name artifact*)
             :artifact-status "saved"
             :artifact-kind (:kind artifact*)
             :artifact-size-bytes (:size-bytes artifact*)))
    (assoc attachment
           :artifact-status "not-saved"
           :artifact-reason (or (:content-reason attachment)
                                (if (false? (:content-available? attachment))
                                  "attachment content is unavailable"
                                  "attachment content was not included")))))

(defn- strip-inline-attachment-content
  [attachment include-attachment-data? max-attachment-bytes]
  (let [has-inline-content? (some #(contains? attachment %) attachment-content-result-keys)
        size-bytes          (long (or (parse-long-safe (:size-bytes attachment)) 0))
        inline-too-large?   (> size-bytes (long max-attachment-bytes))]
    (if (and include-attachment-data?
             (not inline-too-large?))
      attachment
      (cond-> (apply dissoc attachment attachment-content-result-keys)
        has-inline-content?
        (assoc :content-included? false
               :content-reason (if (:artifact-id attachment)
                                 (if inline-too-large?
                                   "attachment saved as artifact; inline content exceeds max_attachment_bytes"
                                   "attachment saved as artifact; inline content not requested")
                                 (if inline-too-large?
                                   "attachment exceeds max_attachment_bytes"
                                   "attachment content not requested")))))))

(defn- saved-attachment-artifact-summary
  [attachment]
  (when-let [artifact-id (:artifact-id attachment)]
    {:artifact-id   artifact-id
     :artifact-name (:artifact-name attachment)
     :filename      (:filename attachment)
     :mime-type     (:mime-type attachment)
     :size-bytes    (:artifact-size-bytes attachment)}))

(defn- attachment-fetch-opts
  [{:keys [save-attachments?
           include-attachment-data?
           max-attachment-bytes
           max-saved-attachment-bytes] :as opts}]
  (if save-attachments?
    (assoc opts
           :include-attachment-data? true
           :max-attachment-bytes (max (long max-attachment-bytes)
                                      (long max-saved-attachment-bytes)))
    (assoc opts
           :include-attachment-data? include-attachment-data?
           :max-attachment-bytes max-attachment-bytes)))

(defn- finalize-attachment-artifacts
  [detail service-id source-id source-kind
   {:keys [save-attachments?
           include-attachment-data?
           max-attachment-bytes]}]
  (if-not (seq (:attachments detail))
    detail
    (let [attachments* (->> (:attachments detail)
                            (map-indexed
                              (fn [idx attachment]
                                (cond-> attachment
                                  save-attachments?
                                  (save-attachment-artifact! service-id
                                                             source-id
                                                             source-kind
                                                             idx))))
                            (mapv #(strip-inline-attachment-content
                                     %
                                     include-attachment-data?
                                     max-attachment-bytes)))
          saved        (into [] (keep saved-attachment-artifact-summary) attachments*)]
      (cond-> (assoc detail :attachments attachments*)
        (seq saved)
        (assoc :saved-attachment-artifacts saved)))))

(defn- draft-summary
  [draft message]
  (assoc (message-summary message)
         :draft-id (get draft "id")))

(defn- draft-detail
  [service-id draft message opts]
  (assoc (message-detail service-id message opts)
         :draft-id (get draft "id")))

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
  (let [query*          (nonblank-str query)
        service-id-text (when (string? service-id)
                          (nonblank-str service-id))
        service-id*     (cond
                          (keyword? service-id)
                          service-id

                          (and service-id-text
                               (service-id-text? service-id-text))
                          (normalize-service-id service-id-text)

                          :else
                          nil)
        query**         (if (and (nil? query*)
                                 service-id-text
                                 (nil? service-id*))
                          service-id-text
                          query*)]
    (assoc opts
           :service-id service-id*
           :query query**)))

(defn- list-query-params
  [{:keys [max-results page-token include-spam-trash?]} query-text]
  (cond-> {"maxResults" (long max-results)}
    query-text
    (assoc "q" query-text)

    (seq (nonblank-str page-token))
    (assoc "pageToken" (nonblank-str page-token))

    (true? include-spam-trash?)
    (assoc "includeSpamTrash" true)))

(defn- compose-payload
  [to subject body {:keys [cc bcc reply-to in-reply-to references thread-id html-body attachments]}]
  (let [attachments* (normalize-attachments attachments)]
    (cond-> {:raw (encode-base64url
                    (raw-message {:to          to
                                  :cc          cc
                                  :bcc         bcc
                                  :subject     subject
                                  :body        body
                                  :reply-to    reply-to
                                  :in-reply-to in-reply-to
                                  :references  references
                                  :html-body   html-body
                                  :attachments attachments*}))}
      (nonblank-str thread-id)
      (assoc :threadId (nonblank-str thread-id)))))

(defn- modify-labels
  [{:keys [archive? read? add-labels remove-labels]}]
  (let [add-set    (into #{} (keep nonblank-str) add-labels)
        remove-set (into #{} (keep nonblank-str) remove-labels)
        add-set    (cond-> add-set
                     (false? archive?) (conj "INBOX")
                     (false? read?) (conj "UNREAD"))
        remove-set (cond-> remove-set
                     (true? archive?) (conj "INBOX")
                     (true? read?) (conj "UNREAD"))]
    {:add-labels    (vec (sort add-set))
     :remove-labels (vec (sort remove-set))}))

(defn- label-summary
  [label]
  {:id                     (get label "id")
   :name                   (get label "name")
   :type                   (some-> (get label "type") nonblank-str str/lower-case keyword)
   :message-list-visibility (get label "messageListVisibility")
   :label-list-visibility  (get label "labelListVisibility")
   :messages-total         (get label "messagesTotal")
   :messages-unread        (get label "messagesUnread")
   :threads-total          (get label "threadsTotal")
   :threads-unread         (get label "threadsUnread")
   :color                  (get label "color")})

(def ^:private microsoft-message-select
  "id,conversationId,subject,from,toRecipients,ccRecipients,internetMessageId,bodyPreview,categories,isRead,receivedDateTime,sentDateTime,parentFolderId,hasAttachments")

(def ^:private microsoft-fetch-message-select
  (str microsoft-message-select ",body,replyTo,internetMessageHeaders"))

(defn- microsoft-search-query
  [query]
  (when-let [text (nonblank-str query)]
    (str "\"" (str/replace text "\"" "\\\"") "\"")))

(defn- microsoft-list-query-params
  [{:keys [query max-results unread-only?]}]
  (cond-> {"$top"    (long max-results)
           "$select" microsoft-message-select}
    (and (not query) unread-only?)
    (assoc "$filter" "isRead eq false")

    (not query)
    (assoc "$orderby" "receivedDateTime DESC")

    query
    (assoc "$search" (microsoft-search-query query))))

(defn- parse-recipient-text
  [text]
  (when-let [value (nonblank-str text)]
    (if-let [[_ display-name address] (re-matches #"(?s)(.+?)\s*<([^>]+)>" value)]
      {"emailAddress" (cond-> {"address" (str/trim address)}
                        (seq (str/trim display-name))
                        (assoc "name" (-> display-name
                                          str/trim
                                          (str/replace #"^\"|\"$" ""))))}
      {"emailAddress" {"address" value}})))

(defn- split-recipient-header
  [value]
  (when-let [text (nonblank-str value)]
    (let [flush-part (fn [parts current]
                       (if-let [part (some-> (.toString ^StringBuilder current) nonblank-str)]
                         (conj parts part)
                         parts))]
      (loop [chars       (seq text)
             current     (StringBuilder.)
             parts       []
             in-quote?   false
             escaped?    false
             angle-depth 0]
        (if-let [ch (first chars)]
          (cond
            escaped?
            (do
              (.append current ^char ch)
              (recur (next chars) current parts in-quote? false angle-depth))

            (= ch \\)
            (do
              (.append current ^char ch)
              (recur (next chars) current parts in-quote? true angle-depth))

            (= ch \")
            (do
              (.append current ^char ch)
              (recur (next chars) current parts (not in-quote?) false angle-depth))

            (and (not in-quote?) (= ch \<))
            (do
              (.append current ^char ch)
              (recur (next chars) current parts in-quote? false (inc angle-depth)))

            (and (not in-quote?) (= ch \>) (pos? angle-depth))
            (do
              (.append current ^char ch)
              (recur (next chars) current parts in-quote? false (dec angle-depth)))

            (and (not in-quote?)
                 (zero? angle-depth)
                 (or (= ch \,) (= ch \;)))
            (recur (next chars) (StringBuilder.) (flush-part parts current) in-quote? false angle-depth)

            :else
            (do
              (.append current ^char ch)
              (recur (next chars) current parts in-quote? false angle-depth)))
          (let [parts* (flush-part parts current)]
            (when (seq parts*)
              parts*)))))))

(defn- graph-recipient-objects
  [value]
  (let [items (cond
                (nil? value)
                nil

                (sequential? value)
                value

                :else
                (split-recipient-header (str value)))]
    (some->> items
             (keep (fn [item]
                     (cond
                       (map? item)
                       (let [address (recipient-address item)]
                         (parse-recipient-text address))

                       :else
                       (parse-recipient-text item))))
             seq
             vec)))

(defn- microsoft-internet-message-headers
  [{:keys [in-reply-to references]}]
  (let [headers (remove nil?
                        [(when-let [value (nonblank-str in-reply-to)]
                           {"name" "In-Reply-To"
                            "value" value})
                         (when-let [value (some-> references references-header (str/replace #"^References:\s*" "") nonblank-str)]
                           {"name" "References"
                            "value" value})])]
    (when (seq headers)
      (vec headers))))

(defn- microsoft-message-header-value
  [message header-name]
  (let [target (some-> header-name name str/lower-case)]
    (some->> (or (get message "internetMessageHeaders") [])
             (keep (fn [header]
                     (let [name*  (some-> (get header "name") nonblank-str str/lower-case)
                           value* (some-> (get header "value") sanitize-header-value nonblank-str)]
                       (when (and (= target name*) value*)
                         value*))))
             first)))

(defn- microsoft-compose-requires-mime?
  [{:keys [html-body]}]
  (boolean (seq (normalize-html-body html-body))))

(defn- microsoft-attachment-payloads
  [attachments]
  (some->> (normalize-attachments attachments)
           (mapv (fn [{:keys [filename media-type inline? bytes size-bytes]}]
                   (when (>= (long size-bytes) microsoft-max-inline-attachment-bytes)
                     (throw (ex-info "Microsoft Graph inline attachments must be smaller than 3 MB"
                                     {:type       :email/attachment-too-large
                                      :filename   filename
                                      :size-bytes size-bytes
                                      :backend    :microsoft-mail})))
                   {"@odata.type" "#microsoft.graph.fileAttachment"
                    "name"        filename
                    "contentType" media-type
                    "isInline"    inline?
                    "contentBytes" (.encodeToString (Base64/getEncoder) ^bytes bytes)}))))

(defn- microsoft-message-payload
  [to subject body {:keys [cc bcc reply-to in-reply-to references html-body attachments]}]
  (let [plain-body          (normalize-message-body body)
        html-body*          (normalize-html-body html-body)
        content-type        (if (seq html-body*) "HTML" "Text")
        content             (if (seq html-body*) html-body* plain-body)
        attachment-payloads (microsoft-attachment-payloads attachments)]
    (cond-> {"subject" subject
             "body"    {"contentType" content-type
                        "content"     content}
             "toRecipients" (or (graph-recipient-objects to) [])}
      (graph-recipient-objects cc)
      (assoc "ccRecipients" (graph-recipient-objects cc))

      (graph-recipient-objects bcc)
      (assoc "bccRecipients" (graph-recipient-objects bcc))

      (graph-recipient-objects reply-to)
      (assoc "replyTo" (graph-recipient-objects reply-to))

      (microsoft-internet-message-headers {:in-reply-to in-reply-to
                                           :references  references})
      (assoc "internetMessageHeaders" (microsoft-internet-message-headers {:in-reply-to in-reply-to
                                                                           :references  references}))

      (seq attachment-payloads)
      (assoc "attachments" attachment-payloads))))

(defn- microsoft-mime-message-body
  [to subject body {:keys [cc bcc reply-to in-reply-to references html-body attachments]}]
  (let [attachments* (normalize-attachments attachments)
        raw          (raw-message {:to          to
                                   :cc          cc
                                   :bcc         bcc
                                   :subject     subject
                                   :body        body
                                   :reply-to    reply-to
                                   :in-reply-to in-reply-to
                                   :references  references
                                   :html-body   html-body
                                   :attachments attachments*})]
    (encode-base64-bytes (utf8-bytes raw))))

(defn- microsoft-message-summary
  [message]
  {:id             (get message "id")
   :thread-id      (get message "conversationId")
   :subject        (nonblank-str (get message "subject"))
   :from           (recipient-address (get message "from"))
   :to             (recipient-list (get message "toRecipients"))
   :cc             (recipient-list (get message "ccRecipients"))
   :date           (or (nonblank-str (get message "sentDateTime"))
                       (nonblank-str (get message "receivedDateTime")))
   :message-id     (nonblank-str (get message "internetMessageId"))
   :snippet        (or (nonblank-str (get message "bodyPreview")) "")
   :labels         (vec (or (get message "categories") []))
   :unread?        (not (boolean (get message "isRead")))
   :received-at-ms (parse-instant-ms (get message "receivedDateTime"))})

(defn- microsoft-body
  [message]
  (let [body         (get message "body")
        content-type (some-> (get body "contentType") nonblank-str str/lower-case)
        content      (some-> (get body "content") nonblank-text)
        preview      (some-> (get message "bodyPreview") nonblank-text)]
    (cond
      (and (= "text" content-type) content)
      {:kind :plain
       :text content}

      (and (= "html" content-type) content)
      {:kind :html
       :text (or (html->text content) preview "")
       :html content}

      preview
      {:kind :snippet
       :text preview}

      :else
      nil)))

(defn- microsoft-fetch-message
  [service-id message-id]
  (let [response (microsoft-request service-id
                                    :get
                                    (str "/me/messages/" message-id)
                                    :query-params {"$select" microsoft-fetch-message-select})]
    (:body response)))

(defn- microsoft-list-attachments
  [service-id message-id]
  (let [response (microsoft-request service-id
                                    :get
                                    (str "/me/messages/" message-id "/attachments"))]
    (or (get-in response [:body "value"]) [])))

(defn- microsoft-fetch-attachment
  [service-id message-id attachment-id]
  (let [response (microsoft-request service-id
                                    :get
                                    (str "/me/messages/" message-id "/attachments/" attachment-id))]
    (:body response)))

(defn- microsoft-attachment-bytes
  [service-id message-id attachment]
  (or (some-> (get attachment "contentBytes")
              decode-base64)
      (some-> (microsoft-fetch-attachment service-id message-id (get attachment "id"))
              (get "contentBytes")
              decode-base64)))

(defn- microsoft-existing-attachment-specs
  [service-id message-id]
  (mapv (fn [attachment]
          (let [kind (some-> (get attachment "@odata.type") nonblank-str (str/replace #"^#" ""))]
            (when-not (str/ends-with? (or kind "") "fileAttachment")
              (throw (ex-info "Only file attachments can be preserved when updating Microsoft drafts"
                              {:type          :email/unsupported-microsoft-attachment
                               :backend       :microsoft-mail
                               :message-id    message-id
                               :attachment-id (get attachment "id")
                               :attachment-type kind})))
            {:filename   (nonblank-str (get attachment "name"))
             :media-type (nonblank-str (get attachment "contentType"))
             :inline?    (boolean (get attachment "isInline"))
             :bytes      (or (microsoft-attachment-bytes service-id message-id attachment)
                             (throw (ex-info "Microsoft attachment content is unavailable for draft preservation"
                                             {:type          :email/microsoft-attachment-content-unavailable
                                              :backend       :microsoft-mail
                                              :message-id    message-id
                                              :attachment-id (get attachment "id")})))}))
        (microsoft-list-attachments service-id message-id)))

(defn- microsoft-attachment-summary
  [service-id message-id attachment {:keys [include-attachment-data? max-attachment-bytes]}]
  (let [kind            (some-> (get attachment "@odata.type") nonblank-str (str/replace #"^#" ""))
        filename        (nonblank-str (get attachment "name"))
        mime-type       (nonblank-str (get attachment "contentType"))
        size            (or (parse-long-safe (get attachment "size")) 0)
        attachment-id   (nonblank-str (get attachment "id"))
        inline?         (boolean (get attachment "isInline"))
        base-summary    {:part-id        attachment-id
                         :filename       filename
                         :mime-type      mime-type
                         :size-bytes     size
                         :attachment-id  attachment-id
                         :inline?        inline?
                         :content-id     (get attachment "contentId")
                         :disposition    nil}]
    (if-not include-attachment-data?
      base-summary
      (let [bytes (when (str/ends-with? (or kind "") "fileAttachment")
                    (microsoft-attachment-bytes service-id message-id attachment))]
        (cond
          (nil? bytes)
          (assoc base-summary :content-available? false)

          (> (long (alength ^bytes bytes)) (long max-attachment-bytes))
          (assoc base-summary
                 :content-available? true
                 :content-included? false
                 :content-reason "attachment exceeds max_attachment_bytes")

          (textual-media-type? mime-type)
          (assoc base-summary
                 :content-available? true
                 :content-included? true
                 :content-text (String. ^bytes bytes StandardCharsets/UTF_8))

          :else
          (assoc base-summary
                 :content-available? true
                 :content-included? true
                 :content-bytes-base64 (.encodeToString (Base64/getEncoder) ^bytes bytes)))))))

(defn- microsoft-message-detail
  [service-id message opts]
  (let [body        (microsoft-body message)
        attachments (when (boolean (get message "hasAttachments"))
                      (->> (microsoft-list-attachments service-id (get message "id"))
                           (mapv #(microsoft-attachment-summary service-id (get message "id") % opts))))]
    (cond-> (microsoft-message-summary message)
      body
      (assoc :body (:text body)
             :body-kind (:kind body))

      (seq (:html body))
      (assoc :html-body (:html body))

      (seq attachments)
      (assoc :attachments attachments))))

(defn- microsoft-category-summary
  [category]
  {:id                      (get category "displayName")
   :name                    (get category "displayName")
   :type                    :user
   :message-list-visibility nil
   :label-list-visibility   nil
   :messages-total          nil
   :messages-unread         nil
   :threads-total           nil
   :threads-unread          nil
   :color                   (get category "color")})

(defn- microsoft-mail-folder-id
  [service-id well-known-name]
  (some-> (microsoft-request service-id
                             :get
                             (str "/me/mailFolders/" well-known-name)
                             :query-params {"$select" "id"})
          :body
          (get "id")))

(defn- microsoft-move-message
  [service-id message-id destination-id]
  (:body (microsoft-request service-id
                            :post
                            (str "/me/messages/" message-id "/move")
                            :body {"destinationId" destination-id})))

(defn- microsoft-delete-existing-attachments!
  [service-id message-id]
  (doseq [attachment (microsoft-list-attachments service-id message-id)]
    (when-let [attachment-id (nonblank-str (get attachment "id"))]
      (microsoft-request service-id
                         :delete
                         (str "/me/messages/" message-id "/attachments/" attachment-id)))))

(defn- microsoft-add-attachments!
  [service-id message-id attachments]
  (doseq [attachment (or (microsoft-attachment-payloads attachments) [])]
    (microsoft-request service-id
                       :post
                       (str "/me/messages/" message-id "/attachments")
                       :body attachment)))

(defn- microsoft-create-draft-message
  [service-id to subject body opts]
  (if (microsoft-compose-requires-mime? opts)
    (:body (microsoft-request service-id
                              :post
                              "/me/messages"
                              :headers {"Content-Type" "text/plain"}
                              :body (microsoft-mime-message-body to subject body opts)))
    (:body (microsoft-request service-id
                              :post
                              "/me/messages"
                              :body (microsoft-message-payload to subject body opts)))))

(defn- microsoft-replacement-draft-opts
  [service-id draft-id opts]
  (let [current-message (microsoft-fetch-message service-id draft-id)
        attachments     (if (some? (:attachments opts))
                          (:attachments opts)
                          (when (boolean (get current-message "hasAttachments"))
                            (microsoft-existing-attachment-specs service-id draft-id)))]
    {:cc          (if (provided-value? (:cc opts))
                    (:cc opts)
                    (recipient-header-list (get current-message "ccRecipients")))
     :bcc         (if (provided-value? (:bcc opts))
                    (:bcc opts)
                    (recipient-header-list (get current-message "bccRecipients")))
     :reply-to    (if (provided-value? (:reply-to opts))
                    (:reply-to opts)
                    (recipient-header-list (get current-message "replyTo")))
     :in-reply-to (if (provided-value? (:in-reply-to opts))
                    (:in-reply-to opts)
                    (microsoft-message-header-value current-message :in-reply-to))
     :references  (if (provided-value? (:references opts))
                    (:references opts)
                    (microsoft-message-header-value current-message :references))
     :html-body   (:html-body opts)
     :attachments attachments}))

(defn- microsoft-save-draft-message
  [service-id draft-id to subject body opts]
  (let [payload  (microsoft-message-payload to subject body opts)
        draft-id (nonblank-str draft-id)
        replace-attachments? (some? (:attachments opts))
        replacement-required? (and draft-id
                                   (or replace-attachments?
                                       (microsoft-compose-requires-mime? opts)))]
    (cond
      replacement-required?
      (let [replacement (microsoft-create-draft-message service-id
                                                        to
                                                        subject
                                                        body
                                                        (microsoft-replacement-draft-opts service-id draft-id opts))]
        (microsoft-request service-id
                           :delete
                           (str "/me/messages/" draft-id))
        replacement)

      draft-id
      (do
        (microsoft-request service-id
                           :patch
                           (str "/me/messages/" draft-id)
                           :body (dissoc payload "attachments"))
        (when replace-attachments?
          (microsoft-delete-existing-attachments! service-id draft-id)
          (microsoft-add-attachments! service-id draft-id (:attachments opts)))
        (microsoft-fetch-message service-id draft-id))

      :else
      (microsoft-create-draft-message service-id to subject body opts))))

(defn- parse-mail-uri!
  [value field]
  (try
    (URI. (or value ""))
    (catch Exception e
      (throw (ex-info "Mail service URL must be a valid absolute URL"
                      {:field field
                       :value value}
                      e)))))

(defn- normalize-mail-security
  [value scheme default-security]
  (let [value* (cond
                 (keyword? value) value
                 (string? value) (some-> value nonblank-str keyword)
                 :else nil)
        inferred (case (some-> scheme nonblank-str str/lower-case)
                   ("imaps" "smtps") :ssl
                   ("imap" "smtp")   default-security
                   default-security)
        resolved (or value* inferred)]
    (when-not (contains? #{:ssl :starttls :none} resolved)
      (throw (ex-info "Unsupported mail security mode"
                      {:value value
                       :scheme scheme})))
    resolved))

(defn- default-port
  [scheme security]
  (case [(some-> scheme nonblank-str str/lower-case) security]
    ["imaps" :ssl] 993
    ["imap" :ssl] 993
    ["imap" :starttls] 143
    ["imap" :none] 143
    ["smtps" :ssl] 465
    ["smtp" :ssl] 465
    ["smtp" :starttls] 587
    ["smtp" :none] 25
    nil))

(defn- service-mail-config
  [service-id]
  (let [service          (or (db/get-service service-id)
                             (throw (ex-info (str "Unknown mail service: " (name service-id))
                                             {:service-id service-id})))
        imap-uri         (parse-mail-uri! (:service/base-url service) "base_url")
        smtp-url         (service-smtp-url service)
        smtp-uri         (parse-mail-uri! smtp-url "smtp_url")
        username         (or (nonblank-str (service-auth-username service))
                             (nonblank-str (service-email-address service))
                             (throw (ex-info "auth_username or email_address is required for IMAP/SMTP mail"
                                             {:service-id service-id
                                              :field "auth_username"})))
        password         (or (nonblank-str (:service/auth-key service))
                             (throw (ex-info "auth_key is required for IMAP/SMTP mail"
                                             {:service-id service-id
                                              :field "auth_key"})))
        email-address    (or (nonblank-str (service-email-address service))
                             username)
        imap-scheme      (some-> (.getScheme imap-uri) str/lower-case)
        smtp-scheme      (some-> (.getScheme smtp-uri) str/lower-case)
        imap-security    (normalize-mail-security (:service/imap-security service)
                                                  imap-scheme
                                                  :starttls)
        smtp-security    (normalize-mail-security (:service/smtp-security service)
                                                  smtp-scheme
                                                  :starttls)
        imap-port        (let [port (.getPort imap-uri)]
                           (if (neg? (long port))
                             (default-port imap-scheme imap-security)
                             port))
        smtp-port        (let [port (.getPort smtp-uri)]
                           (if (neg? (long port))
                             (default-port smtp-scheme smtp-security)
                             port))]
    {:service-id      service-id
     :service         service
     :username        username
     :password        password
     :email-address   email-address
     :imap-host       (.getHost imap-uri)
     :imap-port       imap-port
     :imap-security   imap-security
     :smtp-host       (.getHost smtp-uri)
     :smtp-port       smtp-port
     :smtp-security   smtp-security
     :folders         {:inbox   (nonblank-str (:service/inbox-folder service))
                       :drafts  (nonblank-str (:service/drafts-folder service))
                       :sent    (nonblank-str (:service/sent-folder service))
                       :archive (nonblank-str (:service/archive-folder service))
                       :trash   (nonblank-str (:service/trash-folder service))}}))

(defn- imap-protocol
  [{:keys [imap-security]}]
  (if (= :ssl imap-security) "imaps" "imap"))

(defn- smtp-protocol
  [{:keys [smtp-security]}]
  (if (= :ssl smtp-security) "smtps" "smtp"))

(defn- mail-properties
  [{:keys [imap-host imap-port imap-security smtp-host smtp-port smtp-security username email-address]
    :as config}]
  (let [props         (Properties.)
        imap-proto    (imap-protocol config)
        smtp-proto    (smtp-protocol config)
        imap-prefix   (str "mail." imap-proto)
        smtp-prefix   (str "mail." smtp-proto)]
    (doto props
      (.put "mail.store.protocol" imap-proto)
      (.put "mail.transport.protocol" smtp-proto)
      (.put "mail.mime.address.strict" "false")
      (.put "mail.mime.decodefilename" "true")
      (.put "mail.mime.encodefilename" "true")
      (.put "mail.debug" "false")
      (.put "mail.from" email-address)
      (.put (str imap-prefix ".host") imap-host)
      (.put (str imap-prefix ".port") (str imap-port))
      (.put (str imap-prefix ".connectiontimeout") "30000")
      (.put (str imap-prefix ".timeout") "30000")
      (.put (str imap-prefix ".writetimeout") "30000")
      (.put (str imap-prefix ".ssl.enable") (str (= :ssl imap-security)))
      (.put (str imap-prefix ".starttls.enable") (str (= :starttls imap-security)))
      (.put (str smtp-prefix ".host") smtp-host)
      (.put (str smtp-prefix ".port") (str smtp-port))
      (.put (str smtp-prefix ".auth") "true")
      (.put (str smtp-prefix ".connectiontimeout") "30000")
      (.put (str smtp-prefix ".timeout") "30000")
      (.put (str smtp-prefix ".writetimeout") "30000")
      (.put (str smtp-prefix ".ssl.enable") (str (= :ssl smtp-security)))
      (.put (str smtp-prefix ".starttls.enable") (str (= :starttls smtp-security)))
      (.put (str smtp-prefix ".user") username))))

(defn- mail-session
  [config]
  (Session/getInstance (mail-properties config)))

(defn- generated-message-id
  [email-address]
  (let [domain (or (some-> email-address nonblank-str (str/split #"@") second nonblank-str)
                   "xia.local")]
    (str "<" (UUID/randomUUID) "@" domain ">")))

(defn- message-header
  [^Message message header-name]
  (some-> (.getHeader message header-name nil)
          sanitize-header-value
          nonblank-str))

(defn- part-header-value
  [^Part part header-name]
  (some-> (.getHeader part header-name)
          seq
          first
          sanitize-header-value
          nonblank-str))

(defn- ensure-mime-message-metadata!
  [config ^MimeMessage message]
  (when-not (.getFrom message)
    (.setFrom message (InternetAddress. ^String (:email-address config))))
  (when-not (.getSentDate message)
    (.setSentDate message (Date.)))
  (when-not (message-header message "Message-ID")
    (.setHeader message "Message-ID" (generated-message-id (:email-address config))))
  (.saveChanges message)
  message)

(defn- mime-message-from-raw
  [config raw]
  (let [session (mail-session config)
        message (MimeMessage. session (ByteArrayInputStream. (utf8-bytes raw)))]
    (ensure-mime-message-metadata! config message)))

(defn- with-imap-store
  [config f]
  (let [session (mail-session config)
        store   (.getStore session (imap-protocol config))]
    (try
      (.connect store ^String (:imap-host config) (int (:imap-port config)) ^String (:username config) ^String (:password config))
      (f session store)
      (finally
        (when (.isConnected store)
          (.close store))))))

(defn- folder-leaf-name
  [folder-name]
  (some-> folder-name
          nonblank-str
          (str/split #"[/.]")
          last
          nonblank-str))

(defn- folder-name-matches?
  [folder-name candidate]
  (let [folder*    (some-> folder-name nonblank-str str/lower-case)
        candidate* (some-> candidate nonblank-str str/lower-case)
        leaf*      (some-> folder-name folder-leaf-name str/lower-case)
        cand-leaf* (some-> candidate folder-leaf-name str/lower-case)]
    (or (= folder* candidate*)
        (= leaf* candidate*)
        (= leaf* cand-leaf*)
        (= folder* cand-leaf*))))

(defn- selectable-folder?
  [^Folder folder]
  (and (.exists folder)
       (pos? (bit-and (.getType folder) Folder/HOLDS_MESSAGES))))

(defn- store-folders
  [store]
  (->> (.list (.getDefaultFolder store) "*")
       (filter selectable-folder?)
       vec))

(defn- folder-candidates
  [config folder-kind]
  (cond-> []
    (get-in config [:folders folder-kind])
    (conj (get-in config [:folders folder-kind]))

    (seq (get imap-default-folders folder-kind))
    (into (get imap-default-folders folder-kind))))

(defn- resolve-folder
  [store config folder-kind]
  (let [folders     (store-folders store)
        candidates  (folder-candidates config folder-kind)]
    (or (some (fn [candidate]
                (some (fn [^Folder folder]
                        (when (folder-name-matches? (.getFullName folder) candidate)
                          folder))
                      folders))
              candidates)
        (throw (ex-info (str "Mail folder for " (name folder-kind) " was not found")
                        {:service-id  (:service-id config)
                         :folder-kind folder-kind
                         :candidates  candidates})))))

(defn- open-folder!
  [^Folder folder mode]
  (when-not (.isOpen folder)
    (.open folder mode))
  folder)

(defn- close-folder!
  [^Folder folder expunge?]
  (when (and folder (.isOpen folder))
    (.close folder expunge?)))

(defn- encode-page-token
  [prefix payload]
  (str prefix (encode-base64url (json/write-json-str payload))))

(defn- decode-page-token
  [prefix token]
  (when-let [text (nonblank-str token)]
    (when-not (str/starts-with? text prefix)
      (throw (ex-info "Unexpected mail page token"
                      {:page-token token
                       :expected-prefix prefix})))
    (some-> text
            (subs (count prefix))
            decode-base64url
            json/read-json)))

(defn- encode-imap-message-id
  [folder-name uid]
  (str imap-id-prefix
       (encode-base64url (json/write-json-str {"folder" folder-name
                                               "uid"    (long uid)}))))

(defn- decode-imap-message-id
  [message-id]
  (let [text (or (nonblank-str message-id)
                 (throw (ex-info "message-id is required"
                                 {:type :email/missing-message-id})))]
    (when-not (str/starts-with? text imap-id-prefix)
      (throw (ex-info "Unsupported IMAP message id"
                      {:message-id message-id})))
    (let [payload (some-> text
                          (subs (count imap-id-prefix))
                          decode-base64url
                          json/read-json)
          folder-name (nonblank-str (get payload "folder"))
          uid         (parse-long-safe (get payload "uid"))]
      (when-not (and folder-name uid)
        (throw (ex-info "Invalid IMAP message id"
                        {:message-id message-id})))
      {:folder-name folder-name
       :uid         uid})))

(defn- message-uid
  [^Folder folder ^Message message]
  (let [uid (.getUID ^UIDFolder folder message)]
    (when (neg? (long uid))
      (throw (ex-info "IMAP server did not return a stable UID for the message"
                      {:folder (.getFullName folder)})))
    uid))

(defn- address->text
  [^Address address]
  (cond
    (instance? InternetAddress address)
    (.toUnicodeString ^InternetAddress address)

    (nil? address)
    nil

    :else
    (str address)))

(defn- addresses->text
  [addresses]
  (some->> addresses
           seq
           (map address->text)
           (remove str/blank?)
           seq
           (str/join ", ")))

(defn- date->instant-text
  [^Date date]
  (some-> date .toInstant str))

(defn- date->epoch-ms
  [^Date date]
  (some-> date .toInstant .toEpochMilli))

(defn- part-disposition
  [^Part part]
  (some-> (.getDisposition part) sanitize-header-value nonblank-str))

(defn- decode-filename
  [filename]
  (when-let [text (nonblank-str filename)]
    (try
      (MimeUtility/decodeText text)
      (catch Exception _
        text))))

(defn- read-stream-bytes
  [stream]
  (with-open [in stream
              out (ByteArrayOutputStream.)]
    (io/copy in out)
    (.toByteArray out)))

(defn- message-preview-text
  [part]
  (cond
    (.isMimeType part "text/plain")
    (some-> (.getContent part) str nonblank-text)

    (.isMimeType part "text/html")
    (some-> (.getContent part) str html->text nonblank-text)

    (.isMimeType part "multipart/*")
    (let [content (.getContent part)]
      (when (instance? Multipart content)
        (loop [idx 0]
          (when (< idx (.getCount ^Multipart content))
            (or (message-preview-text (.getBodyPart ^Multipart content idx))
                (recur (inc idx)))))))

    (.isMimeType part "message/rfc822")
    (some-> (.getContent part) message-preview-text)

    :else
    nil))

(defn- attachment-part?
  [^Part part]
  (let [filename    (decode-filename (.getFileName part))
        disposition (some-> (part-disposition part) str/lower-case)]
    (or (seq filename)
        (contains? #{"attachment" "inline"} disposition))))

(declare collect-message-parts)

(defn- attachment-summary-from-part
  [^Part part {:keys [include-attachment-data? max-attachment-bytes]}]
  (let [filename        (decode-filename (.getFileName part))
        mime-type       (some-> (.getContentType part) base-media-type)
        size            (let [raw-size (.getSize part)]
                          (if (neg? (long raw-size)) 0 raw-size))
        disposition     (part-disposition part)
        inline?         (or (boolean (re-find #"(?i)\binline\b" (or disposition "")))
                            (boolean (part-header-value part "Content-ID")))
        content-id      (part-header-value part "Content-ID")
        base-summary    {:part-id        (or content-id filename (str (UUID/randomUUID)))
                         :filename       filename
                         :mime-type      mime-type
                         :size-bytes     size
                         :attachment-id  nil
                         :inline?        inline?
                         :content-id     content-id
                         :disposition    disposition}]
    (if-not include-attachment-data?
      base-summary
      (let [bytes (read-stream-bytes (.getInputStream part))
            size* (long (alength ^bytes bytes))]
        (cond
          (> size* (long max-attachment-bytes))
          (assoc base-summary
                 :size-bytes size*
                 :content-available? true
                 :content-included? false
                 :content-reason "attachment exceeds max_attachment_bytes")

          (textual-media-type? mime-type)
          (assoc base-summary
                 :size-bytes size*
                 :content-available? true
                 :content-included? true
                 :content-text (String. ^bytes bytes StandardCharsets/UTF_8))

          :else
          (assoc base-summary
                 :size-bytes size*
                 :content-available? true
                 :content-included? true
                 :content-bytes-base64 (.encodeToString (Base64/getEncoder) ^bytes bytes)))))))

(defn- collect-message-parts
  [^Part part opts state]
  (cond
    (and (.isMimeType part "text/plain")
         (not (attachment-part? part)))
    (update state :plain-body #(or % (some-> (.getContent part) str nonblank-text)))

    (and (.isMimeType part "text/html")
         (not (attachment-part? part)))
    (update state :html-body #(or % (some-> (.getContent part) str nonblank-text)))

    (.isMimeType part "multipart/*")
    (let [content (.getContent part)]
      (if (instance? Multipart content)
        (reduce (fn [acc idx]
                  (collect-message-parts (.getBodyPart ^Multipart content idx) opts acc))
                state
                (range (.getCount ^Multipart content)))
        state))

    (.isMimeType part "message/rfc822")
    (if-let [content (.getContent part)]
      (collect-message-parts content opts state)
      state)

    (attachment-part? part)
    (update state :attachments conj (attachment-summary-from-part part opts))

    :else
    state))

(defn- message-body-data
  [^Message message opts]
  (let [state (collect-message-parts message opts {:plain-body nil
                                                   :html-body nil
                                                   :attachments []})
        plain (:plain-body state)
        html  (:html-body state)
        text  (cond
                (seq plain) plain
                (seq html) (or (html->text html) "")
                :else nil)
        kind  (cond
                (seq plain) :plain
                (seq html)  :html
                :else nil)]
    {:body-kind   kind
     :body        text
     :html-body   html
     :attachments (vec (:attachments state))}))

(defn- message-date*
  [^Message message]
  (or (.getReceivedDate message)
      (.getSentDate message)))

(defn- imap-message-summary
  [^Folder folder ^Message message]
  (let [folder-name (.getFullName folder)
        uid         (message-uid folder message)
        date        (message-date* message)
        snippet     (or (message-preview-text message) "")]
    {:id             (encode-imap-message-id folder-name uid)
     :thread-id      (message-header message "References")
     :subject        (some-> (.getSubject message) sanitize-header-value nonblank-str)
     :from           (addresses->text (.getFrom message))
     :to             (addresses->text (.getRecipients message Message$RecipientType/TO))
     :cc             (addresses->text (.getRecipients message Message$RecipientType/CC))
     :date           (date->instant-text date)
     :message-id     (message-header message "Message-ID")
     :snippet        snippet
     :labels         [folder-name]
     :unread?        (not (.isSet message Flags$Flag/SEEN))
     :received-at-ms (date->epoch-ms date)}))

(defn- imap-message-detail
  [^Folder folder ^Message message opts]
  (let [body-data (message-body-data message opts)]
    (cond-> (imap-message-summary folder message)
      (:body body-data)
      (assoc :body (:body body-data)
             :body-kind (:body-kind body-data))

      (seq (:html-body body-data))
      (assoc :html-body (:html-body body-data))

      (seq (:attachments body-data))
      (assoc :attachments (:attachments body-data)))))

(defn- query-tokens
  [query]
  (mapv #(or (second %) (nth % 2))
        (re-seq #"\"([^\"]+)\"|(\S+)" (or query ""))))

(defn- token->matcher
  [token]
  (let [token* (or (nonblank-str token) "")
        token* (str/lower-case token*)]
    (cond
      (str/starts-with? token* "from:")
      [:from (subs token* 5)]

      (str/starts-with? token* "to:")
      [:to (subs token* 3)]

      (str/starts-with? token* "cc:")
      [:cc (subs token* 3)]

      (str/starts-with? token* "subject:")
      [:subject (subs token* 8)]

      (str/starts-with? token* "body:")
      [:body (subs token* 5)]

      :else
      [:any token*])))

(defn- summary-matches-token?
  [summary [field needle]]
  (let [needle* (some-> needle nonblank-str str/lower-case)]
    (or (nil? needle*)
        (case field
          :from    (str/includes? (str/lower-case (or (:from summary) "")) needle*)
          :to      (str/includes? (str/lower-case (or (:to summary) "")) needle*)
          :cc      (str/includes? (str/lower-case (or (:cc summary) "")) needle*)
          :subject (str/includes? (str/lower-case (or (:subject summary) "")) needle*)
          :body    (str/includes? (str/lower-case (or (:snippet summary) "")) needle*)
          :any     (some #(str/includes? (str/lower-case (or % "")) needle*)
                         [(:subject summary) (:from summary) (:to summary) (:cc summary) (:snippet summary)])
          false))))

(defn- summary-matches-query?
  [summary query]
  (let [matchers (->> (query-tokens query)
                      (map token->matcher)
                      vec)]
    (every? #(summary-matches-token? summary %) matchers)))

(defn- folder-message-seq
  [^Folder folder]
  (let [count (.getMessageCount folder)]
    (when (pos? (long count))
      (->> (.getMessages folder 1 count)
           seq
           (sort-by (fn [^Message message]
                      [(or (date->epoch-ms (message-date* message)) 0)
                       (.getMessageNumber message)])
                    #(compare %2 %1))))))

(defn- list-folder-entries
  [^Folder folder query unread-only? offset max-results]
  (let [matches (->> (folder-message-seq folder)
                     (map (fn [^Message message]
                            (let [summary (imap-message-summary folder message)]
                              {:summary summary
                               :message message})))
                     (filter (fn [{:keys [summary]}]
                               (and (if unread-only? (:unread? summary) true)
                                    (summary-matches-query? summary query))))
                     vec)
        page    (->> matches
                     (drop offset)
                     (take max-results)
                     (mapv :summary))
        next-offset (+ offset (count page))]
    {:entries         page
     :next-page-token (when (< next-offset (count matches))
                        next-offset)}))

(defn- list-page-offset
  [prefix page-token]
  (or (some-> (decode-page-token prefix page-token)
              (get "offset")
              parse-long-safe)
      0))

(defn- page-token-for-offset
  [prefix offset]
  (encode-page-token prefix {"offset" offset}))

(defn- folder-label-type
  [folder-name]
  (let [name* (str/lower-case (or folder-name ""))]
    (cond
      (some #(folder-name-matches? folder-name %) (mapcat val imap-default-folders))
      :system

      :else
      :user)))

(defn- imap-list-labels*
  [config]
  (with-imap-store config
    (fn [_ store]
      (let [labels (->> (store-folders store)
                        (mapv (fn [^Folder folder]
                                {:id                      (.getFullName folder)
                                 :name                    (.getFullName folder)
                                 :type                    (folder-label-type (.getFullName folder))
                                 :message-list-visibility nil
                                 :label-list-visibility   nil
                                 :messages-total          (let [count (.getMessageCount folder)]
                                                            (when (not (neg? (long count))) count))
                                 :messages-unread         nil
                                 :threads-total           nil
                                 :threads-unread          nil
                                 :color                   nil})))]
        {:service-id     (name (:service-id config))
         :returned-count (count labels)
         :labels         labels}))))

(defn- imap-list-messages*
  [config {:keys [query max-results unread-only? inbox-only? page-token]}]
  (with-imap-store config
    (fn [_ store]
      (let [folder      (resolve-folder store config :inbox)
            offset      (list-page-offset imap-message-page-token-prefix page-token)]
        (try
          (open-folder! folder Folder/READ_ONLY)
          (let [{:keys [entries next-page-token]}
                (list-folder-entries folder query unread-only? offset max-results)]
            {:service-id           (name (:service-id config))
             :query                (nonblank-str query)
             :page-token           (nonblank-str page-token)
             :next-page-token      (some-> next-page-token
                                           (page-token-for-offset imap-message-page-token-prefix))
             :returned-count       (count entries)
             :result-size-estimate (count entries)
             :messages             entries})
          (finally
            (close-folder! folder false)))))))

(defn- with-message-ref
  [config message-id mode f]
  (with-imap-store config
    (fn [_ store]
      (let [{:keys [folder-name uid]} (decode-imap-message-id message-id)
            folder (or (some (fn [^Folder candidate]
                               (when (= (.getFullName candidate) folder-name)
                                 candidate))
                             (store-folders store))
                       (throw (ex-info "Mail folder for message id was not found"
                                       {:service-id (:service-id config)
                                        :folder folder-name})))]
        (try
          (open-folder! folder mode)
          (let [message (.getMessageByUID ^UIDFolder folder uid)]
            (when-not message
              (throw (ex-info "Mail message was not found"
                              {:service-id (:service-id config)
                               :message-id message-id
                               :folder folder-name
                               :uid uid})))
            (f folder message))
          (finally
            (close-folder! folder false)))))))

(defn- imap-read-message*
  [config message-id opts]
  (with-message-ref config message-id Folder/READ_ONLY
    (fn [folder message]
      (assoc (imap-message-detail folder message opts)
             :service-id (name (:service-id config))))))

(defn- find-message-in-folder-by-header
  [^Folder folder header-name header-value]
  (let [count (.getMessageCount folder)
        start (max 1 (- count 50))]
    (some (fn [^Message message]
            (when (= header-value (message-header message header-name))
              message))
          (reverse (seq (.getMessages folder start count))))))

(defn- append-message-to-folder!
  [store config folder-kind ^MimeMessage message]
  (let [folder (resolve-folder store config folder-kind)
        header-value (or (message-header message "Message-ID")
                         (generated-message-id (:email-address config)))]
    (.setHeader message "Message-ID" header-value)
    (.saveChanges message)
    (try
      (open-folder! folder Folder/READ_WRITE)
      (.appendMessages folder (into-array Message [message]))
      (let [saved (or (find-message-in-folder-by-header folder "Message-ID" header-value)
                      (let [count (.getMessageCount folder)]
                        (when (pos? (long count))
                          (.getMessage folder count))))]
        (when-not saved
          (throw (ex-info "Unable to locate appended mail message"
                          {:service-id (:service-id config)
                           :folder-kind folder-kind})))
        {:id        (encode-imap-message-id (.getFullName folder) (message-uid folder saved))
         :folder    (.getFullName folder)
         :summary   (imap-message-summary folder saved)
         :message-id header-value})
      (finally
        (close-folder! folder false)))))

(defn- smtp-send-message!
  [config ^MimeMessage message]
  (let [session   (mail-session config)
        transport (.getTransport session (smtp-protocol config))]
    (try
      (.connect transport ^String (:smtp-host config) (int (:smtp-port config)) ^String (:username config) ^String (:password config))
      (.sendMessage transport message (.getAllRecipients message))
      {:message-id (message-header message "Message-ID")}
      (finally
        (.close transport)))))

(defn- imap-send-message*
  [config to subject body opts]
  (let [raw          (raw-message {:to          to
                                   :cc          (:cc opts)
                                   :bcc         (:bcc opts)
                                   :subject     subject
                                   :body        body
                                   :reply-to    (:reply-to opts)
                                   :in-reply-to (:in-reply-to opts)
                                   :references  (:references opts)
                                   :html-body   (:html-body opts)
                                   :attachments (normalize-attachments (:attachments opts))})
        message      (mime-message-from-raw config raw)
        _            (smtp-send-message! config message)
        stored       (with-imap-store config
                       (fn [_ store]
                         (try
                           (append-message-to-folder! store config :sent message)
                           (catch Exception _
                             nil))))]
    {:service-id (name (:service-id config))
     :status     "sent"
     :id         (or (:id stored) (message-header message "Message-ID"))
     :thread-id  (message-header message "References")
     :labels     (vec (cond-> []
                        (:folder stored) (conj (:folder stored))))}))

(defn- move-message-to-folder!
  [store config ^Folder source-folder ^Message message folder-kind]
  (let [target-folder (resolve-folder store config folder-kind)
        source-name   (.getFullName source-folder)
        target-name   (.getFullName target-folder)
        header-value  (or (message-header message "Message-ID")
                          (generated-message-id (:email-address config)))]
    (if (= source-name target-name)
      {:id      (encode-imap-message-id source-name (message-uid source-folder message))
       :folder  source-name
       :summary (imap-message-summary source-folder message)}
      (do
        (.copyMessages source-folder (into-array Message [message]) target-folder)
        (.setFlag message Flags$Flag/DELETED true)
        (close-folder! source-folder true)
        (try
          (open-folder! target-folder Folder/READ_WRITE)
          (let [copied (or (find-message-in-folder-by-header target-folder "Message-ID" header-value)
                           (let [count (.getMessageCount target-folder)]
                             (when (pos? (long count))
                               (.getMessage target-folder count))))]
            (when-not copied
              (throw (ex-info "Unable to locate moved mail message"
                              {:service-id (:service-id config)
                               :folder-kind folder-kind})))
            {:id      (encode-imap-message-id target-name (message-uid target-folder copied))
             :folder  target-name
             :summary (imap-message-summary target-folder copied)})
          (finally
            (close-folder! target-folder false)))))))

(defn- imap-delete-message*
  [config message-id {:keys [permanent?]}]
  (with-imap-store config
    (fn [_ store]
      (let [{:keys [folder-name uid]} (decode-imap-message-id message-id)
            folder (or (some (fn [^Folder candidate]
                               (when (= (.getFullName candidate) folder-name)
                                 candidate))
                             (store-folders store))
                       (throw (ex-info "Mail folder for message id was not found"
                                       {:service-id (:service-id config)
                                        :folder folder-name})))]
        (open-folder! folder Folder/READ_WRITE)
        (let [message (.getMessageByUID ^UIDFolder folder uid)]
          (when-not message
            (throw (ex-info "Mail message was not found"
                            {:service-id (:service-id config)
                             :message-id message-id})))
          (if permanent?
            (do
              (.setFlag message Flags$Flag/DELETED true)
              (close-folder! folder true)
              {:service-id (name (:service-id config))
               :status     "deleted"
               :id         message-id
               :thread-id  nil
               :labels     []})
            (let [moved (try
                          (move-message-to-folder! store config folder message :trash)
                          (catch Exception _
                            (.setFlag message Flags$Flag/DELETED true)
                            (close-folder! folder true)
                            nil))]
              (if moved
                {:service-id (name (:service-id config))
                 :status     "trashed"
                 :id         (:id moved)
                 :thread-id  nil
                 :labels     [(:folder moved)]}
                {:service-id (name (:service-id config))
                 :status     "deleted"
                 :id         message-id
                 :thread-id  nil
                 :labels     []}))))))))

(defn- imap-update-message*
  [config message-id {:keys [archive? read? add-labels remove-labels]}]
  (when (or (seq add-labels) (seq remove-labels))
    (throw (ex-info "IMAP/SMTP mail does not support generic label mutation"
                    {:service-id (:service-id config)
                     :backend    :imap-smtp
                     :type       :email/unsupported-label-update})))
  (with-imap-store config
    (fn [_ store]
      (let [{:keys [folder-name uid]} (decode-imap-message-id message-id)
            folder (or (some (fn [^Folder candidate]
                               (when (= (.getFullName candidate) folder-name)
                                 candidate))
                             (store-folders store))
                       (throw (ex-info "Mail folder for message id was not found"
                                       {:service-id (:service-id config)
                                        :folder folder-name})))]
        (open-folder! folder Folder/READ_WRITE)
        (let [message (.getMessageByUID ^UIDFolder folder uid)]
          (when-not message
            (throw (ex-info "Mail message was not found"
                            {:service-id (:service-id config)
                             :message-id message-id})))
          (when (some? read?)
            (.setFlag message Flags$Flag/SEEN (boolean read?)))
          (let [moved (when (some? archive?)
                        (move-message-to-folder! store
                                                 config
                                                 folder
                                                 message
                                                 (if archive? :archive :inbox)))
                summary (if moved
                          (:summary moved)
                          (imap-message-summary folder message))]
            (assoc summary
                   :service-id (name (:service-id config))
                   :status "updated"
                   :archived? (boolean archive?))))))))

(defn- imap-list-drafts*
  [config {:keys [query max-results page-token]}]
  (with-imap-store config
    (fn [_ store]
      (let [folder (resolve-folder store config :drafts)
            offset (list-page-offset imap-draft-page-token-prefix page-token)]
        (try
          (open-folder! folder Folder/READ_ONLY)
          (let [{:keys [entries next-page-token]}
                (list-folder-entries folder query false offset max-results)
                drafts (mapv #(assoc % :draft-id (:id %)) entries)]
            {:service-id           (name (:service-id config))
             :query                (nonblank-str query)
             :page-token           (nonblank-str page-token)
             :next-page-token      (some-> next-page-token
                                           (page-token-for-offset imap-draft-page-token-prefix))
             :returned-count       (count drafts)
             :result-size-estimate (count drafts)
             :drafts               drafts})
          (finally
            (close-folder! folder false)))))))

(defn- imap-read-draft*
  [config draft-id opts]
  (with-message-ref config draft-id Folder/READ_ONLY
    (fn [folder message]
      (assoc (imap-message-detail folder message opts)
             :service-id (name (:service-id config))
             :draft-id draft-id))))

(defn- imap-save-draft*
  [config to subject body {:keys [draft-id] :as opts}]
  (let [raw        (raw-message {:to          to
                                 :cc          (:cc opts)
                                 :bcc         (:bcc opts)
                                 :subject     subject
                                 :body        body
                                 :reply-to    (:reply-to opts)
                                 :in-reply-to (:in-reply-to opts)
                                 :references  (:references opts)
                                 :html-body   (:html-body opts)
                                 :attachments (normalize-attachments (:attachments opts))})
        message    (mime-message-from-raw config raw)]
    (with-imap-store config
      (fn [_ store]
        (when-let [draft-id* (nonblank-str draft-id)]
          (with-message-ref config draft-id* Folder/READ_WRITE
            (fn [folder existing]
              (.setFlag existing Flags$Flag/DELETED true)
              (close-folder! folder true))))
        (let [stored (append-message-to-folder! store config :drafts message)]
          {:service-id (name (:service-id config))
           :status     "saved"
           :draft-id   (:id stored)
           :id         (:id stored)
           :thread-id  (message-header message "References")
           :labels     [(:folder stored)]})))))

(defn- imap-send-draft*
  [config draft-id]
  (with-message-ref config draft-id Folder/READ_WRITE
    (fn [folder message]
      (let [out (ByteArrayOutputStream.)]
        (.writeTo message out)
        (let [prepared (mime-message-from-raw config (String. (.toByteArray out) StandardCharsets/UTF_8))
              _        (smtp-send-message! config prepared)
              stored   (with-imap-store config
                         (fn [_ store]
                           (try
                             (append-message-to-folder! store config :sent prepared)
                             (catch Exception _
                               nil))))]
          (.setFlag message Flags$Flag/DELETED true)
          (close-folder! folder true)
          {:service-id (name (:service-id config))
           :status     "sent"
           :draft-id   draft-id
           :id         (or (:id stored) (message-header prepared "Message-ID"))
           :thread-id  (message-header prepared "References")
           :labels     (vec (cond-> []
                              (:folder stored) (conj (:folder stored))))})))))

(defn- imap-delete-draft*
  [config draft-id]
  (with-message-ref config draft-id Folder/READ_WRITE
    (fn [folder message]
      (.setFlag message Flags$Flag/DELETED true)
      (close-folder! folder true)
      {:service-id (name (:service-id config))
       :status     "deleted"
       :draft-id   draft-id})))

(defn- microsoft-list-message-page
  [service-id page-path unread-only?]
  (let [response       (microsoft-request service-id :get page-path)
        messages       (or (get-in response [:body "value"]) [])
        next-page-token (get-in response [:body "@odata.nextLink"])
        filtered       (cond->> messages
                         unread-only?
                         (filter #(not (boolean (get % "isRead"))))
                         true
                         vec)
        next-page-path (some-> next-page-token
                               (#(microsoft-page-path service-id %)))]
    {:messages       filtered
     :next-page-token next-page-token
     :next-page-path next-page-path}))

(defrecord GmailBackend []
  EmailBackend
  (backend-key [_]
    :gmail)
  (backend-label [_]
    "Gmail")
  (backend-default-service-id [_]
    default-service-id)
  (supports-service? [_ service]
    (gmail-service? service))
  (auto-detect-service-id [_]
    (gmail-detect-service-id))
  (backend-list-labels [_ service-id _]
    (let [response (gmail-request service-id
                                  :get
                                  "/gmail/v1/users/me/labels")
          labels   (mapv label-summary (or (get-in response [:body "labels"]) []))]
      {:service-id     (name service-id)
       :returned-count (count labels)
       :labels         labels}))
  (backend-list-messages [_ service-id {:keys [query max-results unread-only? inbox-only? page-token include-spam-trash?]}]
    (let [query-text (format-query query unread-only? inbox-only?)
          response   (gmail-request service-id
                                    :get
                                    "/gmail/v1/users/me/messages"
                                    :query-params (list-query-params {:max-results         max-results
                                                                      :page-token          page-token
                                                                      :include-spam-trash? include-spam-trash?}
                                                                     query-text))
          ids        (or (get-in response [:body "messages"]) [])
          messages   (mapv (fn [entry]
                             (-> (gmail-fetch-message service-id (get entry "id") "metadata")
                                 message-summary))
                           ids)]
      {:service-id           (name service-id)
       :query                query-text
       :page-token           (nonblank-str page-token)
       :next-page-token      (get-in response [:body "nextPageToken"])
       :returned-count       (count messages)
       :result-size-estimate (or (get-in response [:body "resultSizeEstimate"])
                                 (count messages))
       :messages             messages}))
  (backend-read-message [_ service-id message-id opts]
    (let [message (gmail-fetch-message service-id message-id "full")]
      (assoc (message-detail service-id message opts)
             :service-id (name service-id))))
  (backend-send-message [_ service-id to subject body {:keys [cc bcc reply-to in-reply-to references thread-id html-body attachments]}]
    (let [payload  (compose-payload to subject body {:cc          cc
                                                     :bcc         bcc
                                                     :reply-to    reply-to
                                                     :in-reply-to in-reply-to
                                                     :references  references
                                                     :thread-id   thread-id
                                                     :html-body   html-body
                                                     :attachments attachments})
          response (gmail-request service-id
                                  :post
                                  "/gmail/v1/users/me/messages/send"
                                  :body payload)]
      {:service-id       (name service-id)
       :status           "sent"
       :id               (get-in response [:body "id"])
       :thread-id        (get-in response [:body "threadId"])
       :labels           (vec (or (get-in response [:body "labelIds"]) []))}))
  (backend-delete-message [_ service-id message-id {:keys [permanent?]}]
    (let [message-id (nonblank-str message-id)]
      (when-not message-id
        (throw (ex-info "message-id is required"
                        {:type :email/missing-message-id})))
      (let [message   (gmail-fetch-message service-id message-id "metadata")
            thread-id (nonblank-str (get message "threadId"))
            status    (if permanent? "deleted" "trashed")
            method    (if permanent? :delete :post)
            path      (cond
                        thread-id
                        (if permanent?
                          (str "/gmail/v1/users/me/threads/" thread-id)
                          (str "/gmail/v1/users/me/threads/" thread-id "/trash"))

                        permanent?
                        (str "/gmail/v1/users/me/messages/" message-id)

                        :else
                        (str "/gmail/v1/users/me/messages/" message-id "/modify"))
            opts      (when (and (not permanent?) (nil? thread-id))
                        {:body {"addLabelIds"    ["TRASH"]
                                "removeLabelIds" ["INBOX" "UNREAD"]}})
            response  (apply gmail-request service-id method path (mapcat identity opts))
            body      (:body response)
            labels    (or (get body "labelIds")
                          (some->> (get body "messages")
                                   (filter #(= message-id (get % "id")))
                                   first
                                   (#(get % "labelIds"))))]
        {:service-id (name service-id)
         :status     status
         :id         message-id
         :thread-id  (or (get body "id")
                         (get body "threadId")
                         thread-id)
         :labels     (vec (or labels []))})))
  (backend-update-message [_ service-id message-id opts]
    (let [message-id                    (nonblank-str message-id)
          _                             (when-not message-id
                                          (throw (ex-info "message-id is required"
                                                          {:type :email/missing-message-id})))
          original-message              (gmail-fetch-message service-id message-id "metadata")
          thread-id                     (nonblank-str (get original-message "threadId"))
          {:keys [add-labels remove-labels]} (modify-labels opts)
          _                             (when (or (seq add-labels) (seq remove-labels))
                                          (if thread-id
                                            (gmail-request service-id
                                                           :post
                                                           (str "/gmail/v1/users/me/threads/" thread-id "/modify")
                                                           :body {"addLabelIds" add-labels
                                                                  "removeLabelIds" remove-labels})
                                            (gmail-request service-id
                                                           :post
                                                           (str "/gmail/v1/users/me/messages/" message-id "/modify")
                                                           :body {"addLabelIds" add-labels
                                                                  "removeLabelIds" remove-labels})))
          updated-message               (gmail-fetch-message service-id message-id "metadata")]
      (assoc (message-summary updated-message)
             :service-id (name service-id)
             :status "updated"
             :archived? (not (boolean (some #{"INBOX"} (get updated-message "labelIds")))))))
  (backend-list-drafts [_ service-id {:keys [query max-results page-token include-spam-trash?]}]
    (let [query-text (nonblank-str query)
          response   (gmail-request service-id
                                    :get
                                    "/gmail/v1/users/me/drafts"
                                    :query-params (list-query-params {:max-results         max-results
                                                                      :page-token          page-token
                                                                      :include-spam-trash? include-spam-trash?}
                                                                     query-text))
          drafts     (or (get-in response [:body "drafts"]) [])
          entries    (mapv (fn [draft]
                             (let [message-id (get-in draft ["message" "id"])
                                   message    (gmail-fetch-message service-id message-id "metadata")]
                               (draft-summary draft message)))
                           drafts)]
      {:service-id           (name service-id)
       :query                query-text
       :page-token           (nonblank-str page-token)
       :next-page-token      (get-in response [:body "nextPageToken"])
       :returned-count       (count entries)
       :result-size-estimate (or (get-in response [:body "resultSizeEstimate"])
                                 (count entries))
       :drafts               entries}))
  (backend-read-draft [_ service-id draft-id opts]
    (let [draft   (gmail-fetch-draft service-id draft-id "full")
          message (get draft "message")]
      (assoc (draft-detail service-id draft message opts)
             :service-id (name service-id))))
  (backend-save-draft [_ service-id to subject body {:keys [draft-id cc bcc reply-to in-reply-to references thread-id html-body attachments]}]
    (let [message-payload (compose-payload to subject body {:cc          cc
                                                            :bcc         bcc
                                                            :reply-to    reply-to
                                                            :in-reply-to in-reply-to
                                                            :references  references
                                                            :thread-id   thread-id
                                                            :html-body   html-body
                                                            :attachments attachments})
          response        (if-let [draft-id* (nonblank-str draft-id)]
                            (gmail-request service-id
                                           :put
                                           (str "/gmail/v1/users/me/drafts/" draft-id*)
                                           :body {:id draft-id*
                                                  :message message-payload})
                            (gmail-request service-id
                                           :post
                                           "/gmail/v1/users/me/drafts"
                                           :body {:message message-payload}))
          draft           (:body response)
          message         (get draft "message")]
      {:service-id        (name service-id)
       :status            "saved"
       :draft-id          (get draft "id")
       :id                (get message "id")
       :thread-id         (get message "threadId")
       :labels            (vec (or (get message "labelIds") []))}))
  (backend-send-draft [_ service-id draft-id _]
    (let [draft-id* (nonblank-str draft-id)]
      (when-not draft-id*
        (throw (ex-info "draft-id is required"
                        {:type :email/missing-draft-id})))
      (let [response (gmail-request service-id
                                    :post
                                    "/gmail/v1/users/me/drafts/send"
                                    :body {:id draft-id*})]
        {:service-id (name service-id)
         :status     "sent"
         :draft-id   draft-id*
         :id         (get-in response [:body "id"])
         :thread-id  (get-in response [:body "threadId"])
         :labels     (vec (or (get-in response [:body "labelIds"]) []))})))
  (backend-delete-draft [_ service-id draft-id _]
    (let [draft-id* (nonblank-str draft-id)]
      (when-not draft-id*
        (throw (ex-info "draft-id is required"
                        {:type :email/missing-draft-id})))
      (gmail-request service-id
                     :delete
                     (str "/gmail/v1/users/me/drafts/" draft-id*))
      {:service-id (name service-id)
       :status     "deleted"
       :draft-id   draft-id*})))

(defrecord MicrosoftGraphBackend []
  EmailBackend
  (backend-key [_]
    :microsoft-mail)
  (backend-label [_]
    "Microsoft Mail")
  (backend-default-service-id [_]
    microsoft-mail-service-id)
  (supports-service? [_ service]
    (microsoft-mail-service? service))
  (auto-detect-service-id [_]
    (microsoft-detect-service-id))
  (backend-list-labels [_ service-id _]
    (let [response   (microsoft-request service-id
                                        :get
                                        "/me/outlook/masterCategories")
          categories (mapv microsoft-category-summary
                           (or (get-in response [:body "value"]) []))]
      {:service-id     (name service-id)
       :returned-count (count categories)
       :labels         categories}))
  (backend-list-messages [_ service-id {:keys [query max-results unread-only? inbox-only? page-token]}]
    (let [path             (if inbox-only?
                             "/me/mailFolders/inbox/messages"
                             "/me/messages")
          unread-filter?   (and query unread-only?)
          initial-page-path (path-with-query-params path
                                                    (microsoft-list-query-params {:query        query
                                                                                 :max-results  max-results
                                                                                 :unread-only? unread-only?}))
          {:keys [page-path skip-filtered next-page-path]}
          (microsoft-page-state service-id initial-page-path page-token)]
      (loop [current-page-path page-path
             current-skip      skip-filtered
             fallback-next-path next-page-path
             acc               []]
        (if-not current-page-path
          {:service-id           (name service-id)
           :query                (nonblank-str query)
           :page-token           (nonblank-str page-token)
           :next-page-token      nil
           :returned-count       (count acc)
           :result-size-estimate (count acc)
           :messages             (mapv microsoft-message-summary acc)}
          (let [{:keys [messages next-page-token next-page-path]}
                (microsoft-list-message-page service-id current-page-path unread-filter?)
                messages*       (vec (drop current-skip messages))
                remaining       (max 0 (- (long max-results) (count acc)))
                taken           (vec (take remaining messages*))
                acc*            (into acc taken)
                filtered-count  (count messages*)
                consumed-count  (+ current-skip (count taken))
                effective-next-path (or next-page-path fallback-next-path)
                effective-next-token (or next-page-token fallback-next-path)
                partial-page?   (> filtered-count remaining)
                next-token*     (cond
                                  partial-page?
                                  (microsoft-filtered-page-token current-page-path
                                                                 consumed-count
                                                                 effective-next-path)

                                  (seq effective-next-token)
                                  effective-next-token

                                  :else
                                  nil)]
            (if (or partial-page?
                    (>= (count acc*) (long max-results))
                    (nil? effective-next-path))
              {:service-id           (name service-id)
               :query                (nonblank-str query)
               :page-token           (nonblank-str page-token)
               :next-page-token      next-token*
               :returned-count       (count acc*)
               :result-size-estimate (count acc*)
               :messages             (mapv microsoft-message-summary acc*)}
              (recur effective-next-path
                     0
                     nil
                     acc*)))))))
  (backend-read-message [_ service-id message-id opts]
    (assoc (microsoft-message-detail service-id
                                     (microsoft-fetch-message service-id message-id)
                                     opts)
           :service-id (name service-id)))
  (backend-send-message [_ service-id to subject body opts]
    (let [draft   (microsoft-save-draft-message service-id nil to subject body opts)
          draft-id (get draft "id")]
      (microsoft-request service-id
                         :post
                         (str "/me/messages/" draft-id "/send"))
      {:service-id (name service-id)
       :status     "sent"
       :id         draft-id
       :thread-id  (get draft "conversationId")
       :labels     (vec (or (get draft "categories") []))}))
  (backend-delete-message [_ service-id message-id {:keys [permanent?]}]
    (let [message-id* (nonblank-str message-id)]
      (when-not message-id*
        (throw (ex-info "message-id is required"
                        {:type :email/missing-message-id})))
      (if permanent?
        (do
          (microsoft-request service-id
                             :post
                             (str "/me/messages/" message-id* "/permanentDelete"))
          {:service-id (name service-id)
           :status     "deleted"
           :id         message-id*
           :thread-id  nil
           :labels     []})
        (let [moved (microsoft-move-message service-id message-id* "deleteditems")]
          {:service-id (name service-id)
           :status     "trashed"
           :id         (or (get moved "id") message-id*)
           :thread-id  (get moved "conversationId")
           :labels     (vec (or (get moved "categories") []))}))))
  (backend-update-message [_ service-id message-id {:keys [archive? read? add-labels remove-labels]}]
    (let [message-id*      (nonblank-str message-id)
          _                (when-not message-id*
                             (throw (ex-info "message-id is required"
                                             {:type :email/missing-message-id})))
          moved-message    (when (some? archive?)
                             (microsoft-move-message service-id
                                                     message-id*
                                                     (if archive? "archive" "inbox")))
          current-id       (or (get moved-message "id") message-id*)
          current-message  (or moved-message
                               (microsoft-fetch-message service-id current-id))
          current-cats     (into #{} (keep nonblank-str) (or (get current-message "categories") []))
          updated-cats     (-> current-cats
                               (into (keep nonblank-str) add-labels)
                               (#(apply disj % (keep nonblank-str remove-labels))))
          patch-body       (cond-> {}
                             (some? read?)
                             (assoc "isRead" (boolean read?))

                             (or (seq add-labels) (seq remove-labels))
                             (assoc "categories" (vec (sort updated-cats))))
          _                (when (seq patch-body)
                             (microsoft-request service-id
                                                :patch
                                                (str "/me/messages/" current-id)
                                                :body patch-body))
          updated-message  (microsoft-fetch-message service-id current-id)
          archive-folder-id (microsoft-mail-folder-id service-id "archive")]
      (assoc (microsoft-message-summary updated-message)
             :service-id (name service-id)
             :status "updated"
             :archived? (if archive-folder-id
                          (= archive-folder-id (get updated-message "parentFolderId"))
                          (boolean archive?)))))
  (backend-list-drafts [_ service-id {:keys [query max-results page-token]}]
    (let [response (if-let [page-path (microsoft-page-path service-id page-token)]
                     (microsoft-request service-id :get page-path)
                     (microsoft-request service-id
                                        :get
                                        "/me/mailFolders/drafts/messages"
                                        :query-params (microsoft-list-query-params {:query        query
                                                                                    :max-results  max-results
                                                                                    :unread-only? false})))
          drafts   (->> (or (get-in response [:body "value"]) [])
                        (map #(assoc (microsoft-message-summary %) :draft-id (get % "id")))
                        vec)]
      {:service-id           (name service-id)
       :query                (nonblank-str query)
       :page-token           (nonblank-str page-token)
       :next-page-token      (get-in response [:body "@odata.nextLink"])
       :returned-count       (count drafts)
       :result-size-estimate (count drafts)
       :drafts               drafts}))
  (backend-read-draft [_ service-id draft-id opts]
    (assoc (microsoft-message-detail service-id
                                     (microsoft-fetch-message service-id draft-id)
                                     opts)
           :service-id (name service-id)
           :draft-id draft-id))
  (backend-save-draft [_ service-id to subject body {:keys [draft-id] :as opts}]
    (let [draft (microsoft-save-draft-message service-id draft-id to subject body opts)]
      {:service-id (name service-id)
       :status     "saved"
       :draft-id   (get draft "id")
       :id         (get draft "id")
       :thread-id  (get draft "conversationId")
       :labels     (vec (or (get draft "categories") []))}))
  (backend-send-draft [_ service-id draft-id _]
    (let [draft-id* (nonblank-str draft-id)]
      (when-not draft-id*
        (throw (ex-info "draft-id is required"
                        {:type :email/missing-draft-id})))
      (let [draft (microsoft-fetch-message service-id draft-id*)]
        (microsoft-request service-id
                           :post
                           (str "/me/messages/" draft-id* "/send"))
        {:service-id (name service-id)
         :status     "sent"
         :draft-id   draft-id*
         :id         draft-id*
         :thread-id  (get draft "conversationId")
         :labels     (vec (or (get draft "categories") []))})))
  (backend-delete-draft [_ service-id draft-id _]
    (let [draft-id* (nonblank-str draft-id)]
      (when-not draft-id*
        (throw (ex-info "draft-id is required"
                        {:type :email/missing-draft-id})))
      (microsoft-request service-id
                         :delete
                         (str "/me/messages/" draft-id*))
      {:service-id (name service-id)
       :status     "deleted"
       :draft-id   draft-id*})))

(defrecord ImapSmtpBackend []
  EmailBackend
  (backend-key [_]
    :imap-smtp)
  (backend-label [_]
    "IMAP/SMTP")
  (backend-default-service-id [_]
    imap-smtp-service-id)
  (supports-service? [_ service]
    (imap-service? service))
  (auto-detect-service-id [_]
    (imap-smtp-detect-service-id))
  (backend-list-labels [_ service-id _]
    (imap-list-labels* (service-mail-config service-id)))
  (backend-list-messages [_ service-id opts]
    (imap-list-messages* (service-mail-config service-id) opts))
  (backend-read-message [_ service-id message-id opts]
    (imap-read-message* (service-mail-config service-id) message-id opts))
  (backend-send-message [_ service-id to subject body opts]
    (imap-send-message* (service-mail-config service-id) to subject body opts))
  (backend-delete-message [_ service-id message-id opts]
    (imap-delete-message* (service-mail-config service-id) message-id opts))
  (backend-update-message [_ service-id message-id opts]
    (imap-update-message* (service-mail-config service-id) message-id opts))
  (backend-list-drafts [_ service-id opts]
    (imap-list-drafts* (service-mail-config service-id) opts))
  (backend-read-draft [_ service-id draft-id opts]
    (imap-read-draft* (service-mail-config service-id) draft-id opts))
  (backend-save-draft [_ service-id to subject body opts]
    (imap-save-draft* (service-mail-config service-id) to subject body opts))
  (backend-send-draft [_ service-id draft-id _]
    (imap-send-draft* (service-mail-config service-id) draft-id))
  (backend-delete-draft [_ service-id draft-id _]
    (imap-delete-draft* (service-mail-config service-id) draft-id)))

(def ^:private backends
  [(->GmailBackend)
   (->MicrosoftGraphBackend)
   (->ImapSmtpBackend)])

(defn- primary-backend
  []
  (first backends))

(defn- backend-target-for-existing-service
  [candidate]
  (when-let [service (db/get-service candidate)]
    (if-let [backend (some #(when (supports-service? % service) %) backends)]
      {:backend backend
       :service-id candidate}
      (let [label (if (= 1 (count backends))
                    (backend-label (primary-backend))
                    "a supported email backend")]
        (throw (ex-info (str "Service " (name candidate)
                             " is not configured for " label)
                        {:service-id candidate
                         :base-url   (:service/base-url service)}))))))

(defn- detect-email-target
  []
  (some (fn [backend]
          (when-let [service-id (auto-detect-service-id backend)]
            {:backend backend
             :service-id service-id}))
        backends))

(defn- resolve-email-target
  [service-id]
  (let [candidate (normalize-service-id service-id)]
    (cond
      candidate
      (or (backend-target-for-existing-service candidate)
          {:backend (primary-backend)
           :service-id candidate})

      :else
      (or (detect-email-target)
          (let [backend (primary-backend)]
            {:backend backend
             :service-id (backend-default-service-id backend)})))))

(defn list-labels
  "List labels using the detected email backend."
  [& {:keys [service-id]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend-list-labels backend service-id {})))

(defn list-messages
  "List recent messages using the detected email backend."
  [& {:keys [service-id query max-results unread-only? inbox-only? page-token include-spam-trash?]
      :or   {max-results  default-max-results
             unread-only? false
             inbox-only?  true}}]
  (let [{service-id* :service-id
         query*      :query} (list-request {:service-id service-id
                                            :query query})
        {:keys [backend service-id]} (resolve-email-target service-id*)]
    (backend-list-messages backend
                           service-id
                           {:query               query*
                            :max-results         max-results
                            :unread-only?        unread-only?
                            :inbox-only?         inbox-only?
                            :page-token          page-token
                            :include-spam-trash? include-spam-trash?})))

(defn read-message
  "Read a message by id using the detected email backend."
  [message-id & {:keys [service-id include-attachment-data? max-attachment-bytes
                        save-attachments? max-saved-attachment-bytes]
                 :or   {include-attachment-data? false
                        max-attachment-bytes default-max-attachment-bytes
                        save-attachments? false
                        max-saved-attachment-bytes default-max-saved-attachment-bytes}}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)
        opts {:include-attachment-data? include-attachment-data?
              :max-attachment-bytes max-attachment-bytes
              :save-attachments? save-attachments?
              :max-saved-attachment-bytes max-saved-attachment-bytes}]
    (finalize-attachment-artifacts
      (backend-read-message backend
                            service-id
                            message-id
                            (attachment-fetch-opts opts))
      service-id
      message-id
      :message
      opts)))

(defn send-message
  "Send an email through the detected email backend."
  [to subject body & {:keys [cc bcc reply-to in-reply-to references thread-id service-id html-body attachments]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend-send-message backend
                          service-id
                          to
                          subject
                          body
                          {:cc          cc
                           :bcc         bcc
                           :reply-to    reply-to
                           :in-reply-to in-reply-to
                           :references  references
                           :thread-id   thread-id
                           :html-body   html-body
                           :attachments attachments})))

(defn delete-message
  "Delete a message using the detected email backend.

   Backend-specific trash vs permanent-delete semantics are handled by the
   backend implementation."
  [message-id & {:keys [service-id permanent?]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend-delete-message backend
                            service-id
                            message-id
                            {:permanent? permanent?})))

(defn update-message
  "Update message or thread labels/state using the detected email backend."
  [message-id & {:keys [service-id archive? read? add-labels remove-labels]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend-update-message backend
                            service-id
                            message-id
                            {:archive?      archive?
                             :read?         read?
                             :add-labels    add-labels
                             :remove-labels remove-labels})))

(defn list-drafts
  "List drafts using the detected email backend."
  [& {:keys [service-id query max-results page-token include-spam-trash?]
      :or   {max-results default-max-results}}]
  (let [{service-id* :service-id
         query*      :query} (list-request {:service-id service-id
                                            :query query})
        {:keys [backend service-id]} (resolve-email-target service-id*)]
    (backend-list-drafts backend
                         service-id
                         {:query               query*
                          :max-results         max-results
                          :page-token          page-token
                          :include-spam-trash? include-spam-trash?})))

(defn read-draft
  "Read a draft by id using the detected email backend."
  [draft-id & {:keys [service-id include-attachment-data? max-attachment-bytes
                      save-attachments? max-saved-attachment-bytes]
               :or   {include-attachment-data? false
                      max-attachment-bytes default-max-attachment-bytes
                      save-attachments? false
                      max-saved-attachment-bytes default-max-saved-attachment-bytes}}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)
        opts {:include-attachment-data? include-attachment-data?
              :max-attachment-bytes max-attachment-bytes
              :save-attachments? save-attachments?
              :max-saved-attachment-bytes max-saved-attachment-bytes}]
    (finalize-attachment-artifacts
      (backend-read-draft backend
                          service-id
                          draft-id
                          (attachment-fetch-opts opts))
      service-id
      draft-id
      :draft
      opts)))

(defn save-draft
  "Create or update a draft using the detected email backend."
  [to subject body & {:keys [draft-id cc bcc reply-to in-reply-to references thread-id service-id html-body attachments]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend-save-draft backend
                        service-id
                        to
                        subject
                        body
                        {:draft-id     draft-id
                         :cc           cc
                         :bcc          bcc
                         :reply-to     reply-to
                         :in-reply-to  in-reply-to
                         :references   references
                         :thread-id    thread-id
                         :html-body    html-body
                         :attachments  attachments})))

(defn send-draft
  "Send an existing draft using the detected email backend."
  [draft-id & {:keys [service-id]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend-send-draft backend service-id draft-id {})))

(defn delete-draft
  "Delete a draft using the detected email backend."
  [draft-id & {:keys [service-id]}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend-delete-draft backend service-id draft-id {})))
