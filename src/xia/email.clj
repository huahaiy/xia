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
           [java.util Base64 UUID]
           [org.jsoup Jsoup]))

(def default-service-id :gmail)
(def ^:private default-max-results 10)
(def ^:private default-max-attachment-bytes (* 256 1024))
(def ^:private gmail-api-base-url "https://gmail.googleapis.com")
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

(defn- connected-oauth-account?
  [account]
  (boolean (or (nonblank-str (:oauth.account/access-token account))
               (nonblank-str (:oauth.account/refresh-token account)))))

(defn- gmail-oauth-account?
  [account]
  (= :gmail (:oauth.account/provider-template account)))

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

(def ^:private backends
  [(->GmailBackend)])

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
