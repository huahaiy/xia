(ns xia.email
  "Email helpers for bundled tools.

   Public email operations are provider-neutral and dispatch through an
   `EmailBackend` protocol. Gmail is the only backend today, but the contract is
   designed so additional API-backed or protocol-backed mail providers can be
   added without changing the tool-facing call surface."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [xia.artifact :as artifact]
            [xia.db :as db]
            [xia.service :as service])
  (:import [java.nio.charset StandardCharsets]
           [java.time Instant]
           [java.util Base64 UUID]
           [org.jsoup Jsoup]))

(def default-service-id :gmail)
(def ^:private microsoft-mail-service-id :microsoft-mail)
(def ^:private default-max-results 10)
(def ^:private default-max-attachment-bytes (* 256 1024))
(def ^:private gmail-api-base-url "https://gmail.googleapis.com")
(def ^:private microsoft-graph-root-url "https://graph.microsoft.com")
(def ^:private microsoft-graph-api-base-url "https://graph.microsoft.com/v1.0")
(def ^:private microsoft-max-inline-attachment-bytes (* 3 1024 1024))
(def ^:private microsoft-immutable-id-prefer "IdType=\"ImmutableId\"")
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

(defn- microsoft-mail-service?
  [service]
  (and (microsoft-graph-service? service)
       (or (= microsoft-mail-service-id
              (normalize-service-id (service-id-value service)))
           (= :microsoft-mail
              (service-oauth-provider-template service)))))

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
                       :oauth-account (:oauth.account/id account)})
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
                       :oauth-account (:oauth.account/id account)})
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

(defn- recipient-list
  [recipients]
  (some->> recipients
           (keep recipient-address)
           seq
           (str/join ", ")))

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

(defn- microsoft-page-path
  [service-id page-token]
  (when-let [token (nonblank-str page-token)]
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
               (str "?" request-query)))))))

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
  "id,conversationId,subject,from,toRecipients,ccRecipients,internetMessageId,bodyPreview,categories,isRead,receivedDateTime,sentDateTime,parentFolderId")

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

(defn- graph-recipient-objects
  [value]
  (let [items (cond
                (nil? value)
                nil

                (sequential? value)
                value

                :else
                (str/split (str value) #"\s*[;,]\s*"))]
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
                                    :query-params {"$select" (str microsoft-message-select ",body")})]
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
        attachments (->> (microsoft-list-attachments service-id (get message "id"))
                         (mapv #(microsoft-attachment-summary service-id (get message "id") % opts)))]
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

(defn- microsoft-save-draft-message
  [service-id draft-id to subject body opts]
  (let [payload  (microsoft-message-payload to subject body opts)
        draft-id (nonblank-str draft-id)
        replace-attachments? (some? (:attachments opts))]
    (if draft-id
      (do
        (microsoft-request service-id
                           :patch
                           (str "/me/messages/" draft-id)
                           :body (dissoc payload "attachments"))
        (when replace-attachments?
          (microsoft-delete-existing-attachments! service-id draft-id)
          (microsoft-add-attachments! service-id draft-id (:attachments opts)))
        (microsoft-fetch-message service-id draft-id))
      (:body (microsoft-request service-id
                                :post
                                "/me/messages"
                                :body payload)))))

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
    (let [path       (if inbox-only?
                       "/me/mailFolders/inbox/messages"
                       "/me/messages")
          response   (if-let [page-path (microsoft-page-path service-id page-token)]
                       (microsoft-request service-id :get page-path)
                       (microsoft-request service-id
                                          :get
                                          path
                                          :query-params (microsoft-list-query-params {:query        query
                                                                                      :max-results  max-results
                                                                                      :unread-only? unread-only?})))
          messages*  (or (get-in response [:body "value"]) [])
          messages   (cond->> messages*
                       (and query unread-only?)
                       (filter #(not (boolean (get % "isRead"))))

                       true
                       (take max-results))
          messages   (mapv microsoft-message-summary messages)]
      {:service-id           (name service-id)
       :query                (nonblank-str query)
       :page-token           (nonblank-str page-token)
       :next-page-token      (get-in response [:body "@odata.nextLink"])
       :returned-count       (count messages)
       :result-size-estimate (count messages)
       :messages             messages}))
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

(def ^:private backends
  [(->GmailBackend)
   (->MicrosoftGraphBackend)])

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
  [message-id & {:keys [service-id include-attachment-data? max-attachment-bytes]
                 :or   {include-attachment-data? false
                        max-attachment-bytes default-max-attachment-bytes}}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend-read-message backend
                          service-id
                          message-id
                          {:include-attachment-data? include-attachment-data?
                           :max-attachment-bytes     max-attachment-bytes})))

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
  [draft-id & {:keys [service-id include-attachment-data? max-attachment-bytes]
               :or   {include-attachment-data? false
                      max-attachment-bytes default-max-attachment-bytes}}]
  (let [{:keys [backend service-id]} (resolve-email-target service-id)]
    (backend-read-draft backend
                        service-id
                        draft-id
                        {:include-attachment-data? include-attachment-data?
                         :max-attachment-bytes     max-attachment-bytes})))

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
