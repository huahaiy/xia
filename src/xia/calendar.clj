(ns xia.calendar
  "Provider-neutral calendar helpers for bundled tools.

   Google Calendar, Microsoft Graph Calendar, generic CalDAV collections, and
   read-only iCalendar feeds are supported today. The public functions expose a
   single tool-facing surface while provider-specific API details stay behind a
   small backend protocol."
  (:require [clojure.string :as str]
            [xia.db :as db]
            [xia.service :as service])
  (:import [java.io ByteArrayInputStream]
           [java.net URI URLEncoder]
           [java.nio.charset StandardCharsets]
           [java.time Instant LocalDateTime OffsetDateTime ZoneOffset ZonedDateTime]
           [java.time.format DateTimeFormatter]
           [java.time.temporal ChronoUnit]
           [javax.xml XMLConstants]
           [javax.xml.parsers DocumentBuilderFactory]
           [org.w3c.dom Node NodeList]))

(def default-service-id :google-calendar)
(def ^:private microsoft-calendar-service-id :microsoft-calendar)
(def ^:private caldav-calendar-service-id :caldav-calendar)
(def ^:private ical-calendar-service-id :ical-calendar)
(def ^:private caldav-service-ids
  #{:caldav :caldav-calendar :ical :ical-calendar :webcal-calendar})
(def ^:private default-max-results 10)
(def ^:private default-availability-interval-minutes 30)
(def ^:private google-calendar-api-base-url "https://www.googleapis.com/calendar/v3")
(def ^:private google-api-root-url "https://www.googleapis.com")
(def ^:private microsoft-graph-root-url "https://graph.microsoft.com")
(def ^:private microsoft-graph-api-base-url "https://graph.microsoft.com/v1.0")

(defprotocol CalendarBackend
  (backend-key [backend]
    "Stable backend identifier keyword.")
  (backend-label [backend]
    "Human-readable backend label for errors and diagnostics.")
  (backend-default-service-id [backend]
    "Default service id for the backend when no configured service is detected.")
  (supports-service? [backend service]
    "Whether a saved service belongs to this backend.")
  (auto-detect-service-id [backend]
    "Return a configured service id for this backend, creating one when safe.")
  (backend-list-calendars [backend service-id opts]
    "List calendars for the backend.")
  (backend-list-events [backend service-id opts]
    "List events for the backend.")
  (backend-read-event [backend service-id calendar-id event-id opts]
    "Read one event for the backend.")
  (backend-create-event [backend service-id opts]
    "Create one event for the backend.")
  (backend-update-event [backend service-id calendar-id event-id opts]
    "Patch one event for the backend.")
  (backend-delete-event [backend service-id calendar-id event-id opts]
    "Delete one event for the backend.")
  (backend-find-availability [backend service-id opts]
    "Return free/busy information for the backend."))

(defn- nonblank-str
  [value]
  (some-> value str str/trim not-empty))

(defn- normalize-service-id
  [service-id]
  (cond
    (nil? service-id) nil
    (keyword? service-id) service-id
    (string? service-id) (some-> service-id nonblank-str keyword)
    :else (keyword (str service-id))))

(defn- value-of
  [m key]
  (or (get m key)
      (when (keyword? key)
        (get m (name key)))
      (when (string? key)
        (get m (keyword key)))))

(defn- service-id-value
  [service]
  (or (:service/id service)
      (:id service)))

(defn- normalize-base-url
  [value]
  (some-> value nonblank-str (str/replace #"/+$" "")))

(defn- uri-host
  [value]
  (try
    (some-> (URI. (or value "")) .getHost str/lower-case)
    (catch Exception _
      nil)))

(defn- uri-path
  [value]
  (try
    (some-> (URI. (or value "")) .getPath nonblank-str)
    (catch Exception _
      nil)))

(defn- service-oauth-provider-template
  [service]
  (some-> service
          :service/oauth-account
          db/get-oauth-account
          :oauth.account/provider-template))

(defn- google-calendar-service?
  [service]
  (let [service-id (normalize-service-id (service-id-value service))
        template   (service-oauth-provider-template service)
        base-url   (normalize-base-url (:service/base-url service))
        host       (uri-host base-url)
        path       (or (uri-path base-url) "")]
    (or (= default-service-id service-id)
        (= :google-calendar template)
        (= google-calendar-api-base-url base-url)
        (and (= "www.googleapis.com" host)
             (str/starts-with? path "/calendar")))))

(defn- microsoft-calendar-service?
  [service]
  (let [service-id (normalize-service-id (service-id-value service))
        template   (service-oauth-provider-template service)]
    (or (= microsoft-calendar-service-id service-id)
        (= :microsoft-calendar template))))

(defn- caldav-calendar-service?
  [service]
  (let [service-id (normalize-service-id (service-id-value service))
        id-name    (some-> service-id name str/lower-case)
        base-url   (normalize-base-url (:service/base-url service))
        path       (some-> (or (uri-path base-url) "") str/lower-case)]
    (and (not (google-calendar-service? service))
         (not (microsoft-calendar-service? service))
         (or (contains? caldav-service-ids service-id)
             (some-> id-name (str/includes? "caldav"))
             (some-> id-name (str/includes? "ical"))
             (some-> id-name (str/includes? "webcal"))
             (str/includes? path "caldav")
             (str/ends-with? path ".ics")))))

(defn- ical-feed-service?
  [service]
  (let [service-id (normalize-service-id (service-id-value service))
        id-name    (some-> service-id name str/lower-case)
        base-url   (normalize-base-url (:service/base-url service))
        path       (some-> (or (uri-path base-url) "") str/lower-case)]
    (or (str/ends-with? path ".ics")
        (and (or (= ical-calendar-service-id service-id)
                 (some-> id-name (str/includes? "ical"))
                 (some-> id-name (str/includes? "webcal")))
             (not (some-> id-name (str/includes? "caldav")))))))

(defn- connected-oauth-account?
  [account]
  (boolean (or (nonblank-str (:oauth.account/access-token account))
               (nonblank-str (:oauth.account/refresh-token account)))))

(defn- oauth-account-for-template
  [template-id]
  (let [accounts  (->> (db/list-oauth-accounts)
                       (filter #(= template-id (:oauth.account/provider-template %)))
                       (sort-by #(some-> % :oauth.account/id name))
                       vec)
        connected (into [] (filter connected-oauth-account?) accounts)]
    (cond
      (= 1 (count connected)) (first connected)
      (= 1 (count accounts))  (first accounts)
      :else                   nil)))

(defn- ensure-auto-google-calendar-service!
  []
  (when-let [account (oauth-account-for-template :google-calendar)]
    (db/save-service! {:id            default-service-id
                       :name          "Google Calendar"
                       :base-url      google-calendar-api-base-url
                       :auth-type     :oauth-account
                       :oauth-account (:oauth.account/id account)})
    default-service-id))

(defn- ensure-auto-microsoft-calendar-service!
  []
  (when-let [account (oauth-account-for-template :microsoft-calendar)]
    (db/save-service! {:id            microsoft-calendar-service-id
                       :name          "Microsoft Calendar"
                       :base-url      microsoft-graph-api-base-url
                       :auth-type     :oauth-account
                       :oauth-account (:oauth.account/id account)})
    microsoft-calendar-service-id))

(defn- google-detect-service-id
  []
  (or (when (some-> (db/get-service default-service-id) google-calendar-service?)
        default-service-id)
      (some->> (db/list-services)
               (filter google-calendar-service?)
               (sort-by #(some-> % :service/id name))
               first
               :service/id)
      (ensure-auto-google-calendar-service!)))

(defn- microsoft-detect-service-id
  []
  (or (when (some-> (db/get-service microsoft-calendar-service-id) microsoft-calendar-service?)
        microsoft-calendar-service-id)
      (some->> (db/list-services)
               (filter microsoft-calendar-service?)
               (sort-by #(some-> % :service/id name))
               first
               :service/id)
      (ensure-auto-microsoft-calendar-service!)))

(defn- caldav-detect-service-id
  []
  (let [services (->> (db/list-services)
                      (filter caldav-calendar-service?)
                      (sort-by #(some-> % :service/id name))
                      vec)]
    (or (some (fn [service]
                (let [service-id (:service/id service)]
                  (when (contains? caldav-service-ids service-id)
                    service-id)))
              services)
        (when (= 1 (count services))
          (:service/id (first services))))))

(defn- encode-path-segment
  [value]
  (-> (URLEncoder/encode (str value) "UTF-8")
      (str/replace "+" "%20")))

(defn- encode-query-param
  [value]
  (URLEncoder/encode (str value) "UTF-8"))

(defn- path-with-query-params
  [path query-params]
  (let [pairs (->> (or query-params {})
                   (keep (fn [[k v]]
                           (when (some? v)
                             (str (encode-query-param k)
                                  "="
                                  (encode-query-param v)))))
                   seq)]
    (if pairs
      (str path "?" (str/join "&" pairs))
      path)))

(defn- clean-query-params
  [m]
  (into {}
        (keep (fn [[k v]]
                (when (and (some? v)
                           (not (and (string? v) (str/blank? v))))
                  [k v])))
        m))

(defn- list-query-params
  [{:keys [max-results page-token]} extra]
  (clean-query-params
   (merge extra
          {"maxResults" (or max-results default-max-results)
           "pageToken"  (nonblank-str page-token)})))

(defn- google-api-prefix
  [service-id]
  (let [base-url (some-> service-id normalize-service-id db/get-service :service/base-url normalize-base-url)]
    (if (= base-url google-api-root-url)
      "/calendar/v3"
      "")))

(defn- google-request-path
  [service-id path]
  (str (google-api-prefix service-id) path))

(defn- google-request
  [service-id method path & {:as opts}]
  (apply service/request
         (normalize-service-id service-id)
         method
         (google-request-path service-id path)
         (mapcat identity opts)))

(defn- microsoft-api-prefix
  [service-id]
  (let [base-url (some-> service-id normalize-service-id db/get-service :service/base-url normalize-base-url)]
    (if (= base-url microsoft-graph-root-url)
      "/v1.0"
      "")))

(defn- microsoft-request-path
  [service-id path]
  (str (microsoft-api-prefix service-id) path))

(defn- prefer-time-zone
  [time-zone]
  (str "outlook.timezone=\"" (or (nonblank-str time-zone) "UTC") "\""))

(defn- add-prefer-header
  [headers prefer-value]
  (cond
    (nil? headers)
    {"Prefer" prefer-value}

    (contains? headers "Prefer")
    (update headers "Prefer"
            (fn [value]
              (let [parts (->> (str/split (str value) #"\s*,\s*")
                               (remove str/blank?)
                               vec)]
                (if (some #{prefer-value} parts)
                  value
                  (str value ", " prefer-value)))))

    :else
    (assoc headers "Prefer" prefer-value)))

(defn- microsoft-request
  [service-id method path & {:as opts}]
  (let [time-zone (:time-zone opts)
        opts      (dissoc opts :time-zone)
        headers   (add-prefer-header (:headers opts) (prefer-time-zone time-zone))]
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
      (let [uri           (URI. token)
            host          (some-> (.getHost uri) str/lower-case)
            request-path  (or (.getRawPath uri) "")
            request-query (.getRawQuery uri)
            base-path     (or (some-> service-id normalize-service-id db/get-service :service/base-url URI. .getPath)
                              "")
            strip-path    (if (seq base-path) base-path "/v1.0")
            relative-path (if (str/starts-with? request-path strip-path)
                            (subs request-path (count strip-path))
                            request-path)
            relative-path (if (seq relative-path) relative-path "/")]
        (when-not (= "graph.microsoft.com" host)
          (throw (ex-info "Unsupported Microsoft Graph page token host"
                          {:page-token token
                           :host       host})))
        (str relative-path
             (when (seq request-query)
               (str "?" request-query)))))))

(defn- date-only?
  [value]
  (boolean (re-matches #"\d{4}-\d{2}-\d{2}" (str value))))

(defn- time-map
  [value]
  (when (map? value)
    {:date-time (or (value-of value :date-time)
                    (value-of value :dateTime))
     :date      (value-of value :date)
     :time-zone (or (value-of value :time-zone)
                    (value-of value :timeZone))
     :raw       value}))

(defn- google-time
  [value time-zone all-day?]
  (when-let [value* (nonblank-str value)]
    (if (or all-day? (date-only? value*))
      {"date" value*}
      (cond-> {"dateTime" value*}
        (nonblank-str time-zone) (assoc "timeZone" (nonblank-str time-zone))))))

(defn- microsoft-date-time-value
  [value]
  (let [value* (nonblank-str value)]
    (cond
      (nil? value*) nil
      (date-only? value*) (str value* "T00:00:00")
      :else value*)))

(defn- microsoft-time
  [value time-zone]
  (when-let [value* (microsoft-date-time-value value)]
    {"dateTime" value*
     "timeZone" (or (nonblank-str time-zone) "UTC")}))

(defn- attendee-inputs
  [attendees]
  (cond
    (nil? attendees) []
    (string? attendees) (->> (str/split attendees #",")
                             (map str/trim)
                             (remove str/blank?)
                             vec)
    (sequential? attendees) (vec attendees)
    :else [attendees]))

(defn- attendee-email
  [attendee]
  (cond
    (string? attendee) (nonblank-str attendee)
    (map? attendee)    (or (nonblank-str (value-of attendee :email))
                           (nonblank-str (value-of attendee :address)))
    :else              (nonblank-str attendee)))

(defn- google-attendee
  [attendee]
  (when-let [email (attendee-email attendee)]
    (if (map? attendee)
      (cond-> {"email" email}
        (nonblank-str (value-of attendee :name))
        (assoc "displayName" (nonblank-str (value-of attendee :name)))
        (some? (value-of attendee :optional))
        (assoc "optional" (boolean (value-of attendee :optional))))
      {"email" email})))

(defn- microsoft-attendee
  [attendee]
  (when-let [email (attendee-email attendee)]
    (let [name (when (map? attendee)
                 (nonblank-str (value-of attendee :name)))
          type (if (and (map? attendee)
                        (or (= "optional" (some-> (value-of attendee :type) str str/lower-case))
                            (true? (value-of attendee :optional))))
                 "optional"
                 "required")]
      {"emailAddress" (cond-> {"address" email}
                        name (assoc "name" name))
       "type" type})))

(defn- recurrence-lines
  [recurrence]
  (cond
    (nil? recurrence) []
    (string? recurrence) (if-let [line (nonblank-str recurrence)] [line] [])
    (sequential? recurrence) (->> recurrence (keep nonblank-str) vec)
    :else [(str recurrence)]))

(declare require-event-id!)

;; ---------------------------------------------------------------------------
;; Generic CalDAV / iCalendar support
;; ---------------------------------------------------------------------------

(def ^:private ical-utc-date-time-formatter
  (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'"))

(def ^:private ical-local-date-time-formatter
  (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss"))

(defn- xml-escape
  [value]
  (-> (str value)
      (str/replace "&" "&amp;")
      (str/replace "<" "&lt;")
      (str/replace ">" "&gt;")
      (str/replace "\"" "&quot;")
      (str/replace "'" "&apos;")))

(defn- secure-document-builder-factory
  []
  (let [factory (DocumentBuilderFactory/newInstance)]
    (.setNamespaceAware factory true)
    (doseq [feature ["http://apache.org/xml/features/disallow-doctype-decl"
                     "http://xml.org/sax/features/external-general-entities"
                     "http://xml.org/sax/features/external-parameter-entities"
                     "http://apache.org/xml/features/nonvalidating/load-external-dtd"]]
      (try
        (.setFeature factory feature (not (str/includes? feature "external")))
        (catch Exception _ nil)))
    (try
      (.setAttribute factory XMLConstants/ACCESS_EXTERNAL_DTD "")
      (catch Exception _ nil))
    (try
      (.setAttribute factory XMLConstants/ACCESS_EXTERNAL_SCHEMA "")
      (catch Exception _ nil))
    factory))

(defn- parse-xml-text
  [text]
  (let [bytes (.getBytes (str text) StandardCharsets/UTF_8)]
    (.parse (.newDocumentBuilder (secure-document-builder-factory))
            (ByteArrayInputStream. bytes))))

(defn- child-nodes
  [node]
  (let [^NodeList nodes (.getChildNodes node)]
    (map (fn [idx]
           (.item nodes idx))
         (range (.getLength nodes)))))

(defn- element-node?
  [node]
  (= Node/ELEMENT_NODE (.getNodeType node)))

(defn- node-local-name
  [node]
  (or (.getLocalName node)
      (some-> (.getNodeName node)
              (str/split #":")
              last)))

(defn- direct-child-elements-by-local-name
  [node local-name]
  (->> (child-nodes node)
       (filter element-node?)
       (filter #(= local-name (node-local-name %)))))

(defn- descendant-elements-by-local-name
  [node local-name]
  (->> (tree-seq (constantly true) child-nodes node)
       rest
       (filter element-node?)
       (filter #(= local-name (node-local-name %)))))

(defn- node-text
  [node]
  (some-> node .getTextContent nonblank-str))

(defn- first-descendant-text
  [node local-name]
  (some-> node
          (descendant-elements-by-local-name local-name)
          first
          node-text))

(defn- ensure-leading-slash
  [path]
  (let [path* (or (nonblank-str path) "/")]
    (if (str/starts-with? path* "/")
      path*
      (str "/" path*))))

(defn- ensure-trailing-slash
  [path]
  (let [path* (ensure-leading-slash path)]
    (if (str/ends-with? path* "/")
      path*
      (str path* "/"))))

(defn- uri-raw-path
  [value]
  (try
    (some-> (URI. (or value "")) .getRawPath nonblank-str)
    (catch Exception _
      nil)))

(defn- service-base-raw-path
  [service-id]
  (some-> service-id
          normalize-service-id
          db/get-service
          :service/base-url
          uri-raw-path))

(defn- caldav-href->path
  [service-id href]
  (let [href*      (or (nonblank-str href) "/")
        href-path  (try
                     (let [uri (URI. href*)]
                       (or (.getRawPath uri) href*))
                     (catch Exception _
                       href*))
        href-path  (ensure-leading-slash href-path)
        base-path  (some-> (service-base-raw-path service-id)
                           (str/replace #"/+$" ""))
        relative   (if (and (seq base-path)
                            (str/starts-with? href-path base-path))
                     (subs href-path (count base-path))
                     href-path)]
    (ensure-leading-slash relative)))

(defn- caldav-calendar-path
  [calendar-id]
  (ensure-trailing-slash (or (nonblank-str calendar-id) "/")))

(defn- caldav-event-path
  [calendar-id event-id]
  (let [event-id* (require-event-id! event-id)]
    (if (str/starts-with? event-id* "/")
      event-id*
      (str (caldav-calendar-path calendar-id) event-id*))))

(defn- caldav-resource-name
  [uid]
  (let [uid* (or (nonblank-str uid) (str (random-uuid)))]
    (if (str/ends-with? (str/lower-case uid*) ".ics")
      uid*
      (str (encode-path-segment uid*) ".ics"))))

(defn- caldav-calendar-resource-path
  [calendar-id uid]
  (str (caldav-calendar-path calendar-id) (caldav-resource-name uid)))

(defn- caldav-request
  [service-id method path & {:as opts}]
  (apply service/request
         (normalize-service-id service-id)
         method
         path
         (mapcat identity opts)))

(defn- caldav-propfind-body
  []
  "<?xml version=\"1.0\" encoding=\"utf-8\"?>
<d:propfind xmlns:d=\"DAV:\" xmlns:c=\"urn:ietf:params:xml:ns:caldav\" xmlns:cs=\"http://calendarserver.org/ns/\">
  <d:prop>
    <d:displayname/>
    <d:resourcetype/>
    <c:calendar-description/>
    <c:calendar-timezone/>
    <cs:getctag/>
  </d:prop>
</d:propfind>")

(defn- parse-instant
  [value]
  (try
    (Instant/parse (str value))
    (catch Exception _
      nil)))

(defn- parse-offset-date-time
  [value]
  (try
    (OffsetDateTime/parse (str value))
    (catch Exception _
      nil)))

(defn- parse-local-date-time
  [value]
  (try
    (LocalDateTime/parse (str value))
    (catch Exception _
      nil)))

(defn- compact-date
  [value]
  (str/replace (str value) "-" ""))

(defn- compact-local-date-time-value
  [value]
  (let [value* (-> (str value)
                   (str/replace #"\.\d+" "")
                   (str/replace #"([+-]\d{2}:?\d{2}|Z)$" ""))]
    (-> value*
        (str/replace "-" "")
        (str/replace ":" ""))))

(defn- format-utc-ical-time
  [instant]
  (.format ical-utc-date-time-formatter (.atZone instant ZoneOffset/UTC)))

(defn- caldav-range-time
  [value]
  (let [value* (nonblank-str value)]
    (cond
      (nil? value*) nil
      (date-only? value*) (str (compact-date value*) "T000000Z")
      (parse-instant value*) (format-utc-ical-time (parse-instant value*))
      (parse-offset-date-time value*) (format-utc-ical-time (.toInstant (parse-offset-date-time value*)))
      :else (str (compact-local-date-time-value value*) "Z"))))

(defn- caldav-calendar-query-body
  [time-min time-max]
  (let [start (caldav-range-time time-min)
        end   (caldav-range-time time-max)
        range (when (and start end)
                (str "<c:time-range start=\"" (xml-escape start)
                     "\" end=\"" (xml-escape end) "\"/>"))]
    (str "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
         "<c:calendar-query xmlns:d=\"DAV:\" xmlns:c=\"urn:ietf:params:xml:ns:caldav\">\n"
         "  <d:prop><d:getetag/><c:calendar-data/></d:prop>\n"
         "  <c:filter><c:comp-filter name=\"VCALENDAR\"><c:comp-filter name=\"VEVENT\">"
         (or range "")
         "</c:comp-filter></c:comp-filter></c:filter>\n"
         "</c:calendar-query>")))

(defn- caldav-multistatus-responses
  [xml]
  (descendant-elements-by-local-name (parse-xml-text xml) "response"))

(defn- caldav-calendar-summary
  [service-id response]
  (let [href        (first-descendant-text response "href")
        path        (caldav-href->path service-id href)
        displayname (first-descendant-text response "displayname")
        description (first-descendant-text response "calendar-description")
        timezone    (first-descendant-text response "calendar-timezone")
        ctag        (first-descendant-text response "getctag")
        calendar?   (boolean (seq (descendant-elements-by-local-name response "calendar")))]
    {:id          path
     :summary     (or displayname path)
     :description description
     :time-zone   timezone
     :ctag        ctag
     :calendar?   calendar?
     :read-only?  false}))

(defn- ical-unfold-lines
  [text]
  (reduce (fn [lines line]
            (if (and (seq lines)
                     (or (str/starts-with? line " ")
                         (str/starts-with? line "\t")))
              (conj (pop lines) (str (peek lines) (subs line 1)))
              (conj lines line)))
          []
          (str/split (str text) #"\r\n|\n|\r")))

(defn- ical-unquote-param-value
  [value]
  (let [value* (str value)]
    (if (and (str/starts-with? value* "\"")
             (str/ends-with? value* "\"")
             (<= 2 (count value*)))
      (subs value* 1 (dec (count value*)))
      value*)))

(defn- parse-ical-line
  [line]
  (let [idx (.indexOf (str line) ":")]
    (when (pos? idx)
      (let [head   (subs line 0 idx)
            value  (subs line (inc idx))
            parts  (str/split head #";")
            name   (some-> (first parts) str/upper-case)
            params (into {}
                         (keep (fn [part]
                                 (let [[k v] (str/split part #"=" 2)]
                                   (when (and k v)
                                     [(str/upper-case k)
                                      (ical-unquote-param-value v)]))))
                         (rest parts))]
        {:name name
         :params params
         :value value}))))

(defn- ical-content-lines
  [text]
  (into [] (keep parse-ical-line) (ical-unfold-lines text)))

(defn- ical-vevent-blocks
  [props]
  (loop [remaining props
         in-event? false
         current []
         events []]
    (if-let [prop (first remaining)]
      (let [begin-event? (and (= "BEGIN" (:name prop))
                              (= "VEVENT" (str/upper-case (:value prop))))
            end-event?   (and (= "END" (:name prop))
                              (= "VEVENT" (str/upper-case (:value prop))))]
        (cond
          begin-event?
          (recur (rest remaining) true [] events)

          end-event?
          (recur (rest remaining) false [] (conj events current))

          in-event?
          (recur (rest remaining) true (conj current prop) events)

          :else
          (recur (rest remaining) false current events)))
      events)))

(defn- ical-unescape-text
  [value]
  (some-> (str value)
          (str/replace #"\\[nN]" "\n")
          (str/replace "\\," ",")
          (str/replace "\\;" ";")
          (str/replace "\\\\" "\\")))

(defn- ical-prop
  [props name]
  (let [name* (str/upper-case (str name))]
    (first (filter #(= name* (:name %)) props))))

(defn- ical-props
  [props name]
  (let [name* (str/upper-case (str name))]
    (filter #(= name* (:name %)) props)))

(defn- ical-prop-value
  [props name]
  (some-> (ical-prop props name) :value ical-unescape-text nonblank-str))

(defn- ical-param
  [prop name]
  (get-in prop [:params (str/upper-case (str name))]))

(defn- format-ical-read-time
  [value]
  (let [value* (str value)]
    (if-let [[_ y m d h minute s z] (re-matches #"(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})(Z?)" value*)]
      (str y "-" m "-" d "T" h ":" minute ":" s z)
      (if-let [[_ y m d] (re-matches #"(\d{4})(\d{2})(\d{2})" value*)]
        (str y "-" m "-" d)
        value*))))

(defn- ical-time-map
  [prop]
  (when prop
    (let [value      (nonblank-str (:value prop))
          value-type (some-> (ical-param prop "VALUE") str/upper-case)
          time-zone  (ical-param prop "TZID")
          formatted  (some-> value format-ical-read-time)]
      (cond
        (nil? value) nil
        (or (= "DATE" value-type)
            (re-matches #"\d{8}" value))
        {:date formatted
         :raw  {:value value
                :params (:params prop)}}

        :else
        {:date-time formatted
         :time-zone time-zone
         :raw       {:value value
                     :params (:params prop)}}))))

(defn- ical-mailto-email
  [value]
  (some-> value
          ical-unescape-text
          (str/replace #"(?i)^mailto:" "")
          nonblank-str))

(defn- ical-person-summary
  [prop]
  (when prop
    {:email        (ical-mailto-email (:value prop))
     :display-name (some-> (ical-param prop "CN") ical-unescape-text)}))

(defn- ical-attendee-summary
  [prop]
  (when prop
    (let [role     (some-> (ical-param prop "ROLE") str/upper-case)
          partstat (some-> (ical-param prop "PARTSTAT") str/lower-case)]
      {:email           (ical-mailto-email (:value prop))
       :display-name    (some-> (ical-param prop "CN") ical-unescape-text)
       :response-status partstat
       :optional?       (= "OPT-PARTICIPANT" role)
       :organizer?      false
       :self?           false})))

(defn- ical-recurrence-summary
  [props]
  (->> (concat (ical-props props "RRULE")
               (ical-props props "RDATE")
               (ical-props props "EXDATE"))
       (mapv #(str (:name %) ":" (:value %)))))

(defn- ical-event-summary
  [provider service-id calendar-id event-path props]
  (let [uid   (ical-prop-value props "UID")
        start (ical-time-map (ical-prop props "DTSTART"))
        end   (ical-time-map (ical-prop props "DTEND"))]
    {:provider     provider
     :service-id   (name service-id)
     :calendar-id  calendar-id
     :id           (or (nonblank-str event-path) uid)
     :uid          uid
     :summary      (ical-prop-value props "SUMMARY")
     :description  (ical-prop-value props "DESCRIPTION")
     :location     (ical-prop-value props "LOCATION")
     :status       (some-> (ical-prop-value props "STATUS") str/lower-case)
     :created      (some-> (ical-prop props "CREATED") :value format-ical-read-time)
     :updated      (some-> (ical-prop props "LAST-MODIFIED") :value format-ical-read-time)
     :start        start
     :end          end
     :all-day?     (boolean (:date start))
     :attendees    (into [] (keep ical-attendee-summary) (ical-props props "ATTENDEE"))
     :organizer    (ical-person-summary (ical-prop props "ORGANIZER"))
     :recurrence   (ical-recurrence-summary props)
     :transparency (some-> (ical-prop-value props "TRANSP") str/lower-case)}))

(defn- ical-text->event-summaries
  [provider service-id calendar-id text & {:keys [event-path]}]
  (let [props  (ical-content-lines text)
        blocks (ical-vevent-blocks props)]
    (mapv #(ical-event-summary provider service-id calendar-id event-path %) blocks)))

(defn- event-time-value
  [event key]
  (or (get-in event [key :date-time])
      (get-in event [key :date])))

(defn- event-time-zone
  [event key]
  (get-in event [key :time-zone]))

(defn- event-matches-query?
  [event query]
  (if-let [query* (some-> query nonblank-str str/lower-case)]
    (str/includes? (str/lower-case
                    (str (:summary event) " "
                         (:description event) " "
                         (:location event)))
                   query*)
    true))

(defn- before-time?
  [a b]
  (and (nonblank-str a)
       (nonblank-str b)
       (neg? (compare (str a) (str b)))))

(defn- after-time?
  [a b]
  (and (nonblank-str a)
       (nonblank-str b)
       (pos? (compare (str a) (str b)))))

(defn- event-overlaps-range?
  [event time-min time-max]
  (let [start (event-time-value event :start)
        end   (or (event-time-value event :end) start)]
    (and (or (nil? (nonblank-str time-max))
             (nil? (nonblank-str start))
             (before-time? start time-max))
         (or (nil? (nonblank-str time-min))
             (nil? (nonblank-str end))
             (after-time? end time-min)))))

(defn- filter-calendar-events
  [events {:keys [query time-min time-max max-results]}]
  (->> events
       (filter #(event-matches-query? % query))
       (filter #(event-overlaps-range? % time-min time-max))
       (take (or max-results default-max-results))
       vec))

(defn- ical-escape-text
  [value]
  (some-> (str value)
          (str/replace "\\" "\\\\")
          (str/replace ";" "\\;")
          (str/replace "," "\\,")
          (str/replace "\r\n" "\n")
          (str/replace "\r" "\n")
          (str/replace "\n" "\\n")))

(defn- ical-param-value
  [value]
  (let [value* (str value)]
    (if (re-find #"[,:;]" value*)
      (str "\"" (str/replace value* "\"" "") "\"")
      value*)))

(defn- format-ical-write-time
  [name value time-zone all-day?]
  (when-let [value* (nonblank-str value)]
    (cond
      (or all-day? (date-only? value*))
      (str name ";VALUE=DATE:" (compact-date value*))

      (parse-offset-date-time value*)
      (str name ":" (format-utc-ical-time (.toInstant (parse-offset-date-time value*))))

      (parse-local-date-time value*)
      (str name
           (when-let [tz (nonblank-str time-zone)]
             (str ";TZID=" (ical-param-value tz)))
           ":"
           (.format ical-local-date-time-formatter (parse-local-date-time value*)))

      :else
      (str name
           (when-let [tz (nonblank-str time-zone)]
             (str ";TZID=" (ical-param-value tz)))
           ":"
           (compact-local-date-time-value value*)))))

(defn- ical-content-line
  [name value]
  (when-let [value* (nonblank-str value)]
    (str name ":" (ical-escape-text value*))))

(defn- ical-attendee-line
  [attendee]
  (when-let [email (attendee-email attendee)]
    (let [name      (when (map? attendee)
                      (or (nonblank-str (value-of attendee :display-name))
                          (nonblank-str (value-of attendee :name))))
          optional? (and (map? attendee)
                         (or (true? (value-of attendee :optional))
                             (true? (value-of attendee :optional?))))
          params    (cond-> []
                      name (conj (str "CN=" (ical-param-value name)))
                      optional? (conj "ROLE=OPT-PARTICIPANT"))]
      (str "ATTENDEE"
           (when (seq params)
             (str ";" (str/join ";" params)))
           ":mailto:" email))))

(defn- normalize-ical-recurrence-line
  [line]
  (when-let [line* (nonblank-str line)]
    (if (re-find #"^[A-Z-]+:" (str/upper-case line*))
      line*
      (str "RRULE:" line*))))

(defn- build-ical-event
  [{:keys [uid summary description location start end time-zone all-day?
           attendees recurrence transparency status]}]
  (let [uid*      (or (nonblank-str uid) (str (random-uuid) "@xia.local"))
        all-day?* (boolean (or all-day? (date-only? start) (date-only? end)))
        dtstamp   (format-utc-ical-time (Instant/now))
        lines     (concat
                   ["BEGIN:VCALENDAR"
                    "VERSION:2.0"
                    "PRODID:-//Xia//Calendar//EN"
                    "CALSCALE:GREGORIAN"
                    "BEGIN:VEVENT"
                    (str "UID:" uid*)
                    (str "DTSTAMP:" dtstamp)]
                   (keep identity
                         [(ical-content-line "SUMMARY" summary)
                          (ical-content-line "DESCRIPTION" description)
                          (ical-content-line "LOCATION" location)
                          (format-ical-write-time "DTSTART" start time-zone all-day?*)
                          (format-ical-write-time "DTEND" end time-zone all-day?*)
                          (ical-content-line "STATUS" (some-> status str/upper-case))
                          (ical-content-line "TRANSP" (some-> transparency str/upper-case))])
                   (keep ical-attendee-line (attendee-inputs attendees))
                   (keep normalize-ical-recurrence-line (recurrence-lines recurrence))
                   ["END:VEVENT"
                    "END:VCALENDAR"])]
    {:uid  uid*
     :body (str (str/join "\r\n" lines) "\r\n")}))

(defn- google-event-payload
  [{:keys [summary description location start end time-zone all-day?
           attendees recurrence transparency visibility]}]
  (cond-> {}
    (some? summary) (assoc "summary" summary)
    (some? description) (assoc "description" description)
    (some? location) (assoc "location" location)
    start (assoc "start" (google-time start time-zone all-day?))
    end (assoc "end" (google-time end time-zone all-day?))
    (seq (attendee-inputs attendees))
    (assoc "attendees" (into [] (keep google-attendee) (attendee-inputs attendees)))
    (seq (recurrence-lines recurrence))
    (assoc "recurrence" (recurrence-lines recurrence))
    (nonblank-str transparency) (assoc "transparency" (nonblank-str transparency))
    (nonblank-str visibility) (assoc "visibility" (nonblank-str visibility))))

(defn- microsoft-event-payload
  [{:keys [summary description location start end time-zone all-day?
           attendees recurrence show-as sensitivity html?]}]
  (when (seq (recurrence-lines recurrence))
    (throw (ex-info "recurrence is currently supported for Google Calendar only"
                    {:type :calendar/unsupported-recurrence})))
  (cond-> {}
    (some? summary) (assoc "subject" summary)
    (some? description) (assoc "body" {"contentType" (if html? "HTML" "Text")
                                       "content" description})
    (some? location) (assoc "location" {"displayName" location})
    start (assoc "start" (microsoft-time start time-zone))
    end (assoc "end" (microsoft-time end time-zone))
    (some? all-day?) (assoc "isAllDay" (boolean all-day?))
    (seq (attendee-inputs attendees))
    (assoc "attendees" (into [] (keep microsoft-attendee) (attendee-inputs attendees)))
    (nonblank-str show-as) (assoc "showAs" (nonblank-str show-as))
    (nonblank-str sensitivity) (assoc "sensitivity" (nonblank-str sensitivity))))

(defn- google-calendar-summary
  [calendar]
  {:id               (get calendar "id")
   :summary          (get calendar "summary")
   :description      (get calendar "description")
   :time-zone        (get calendar "timeZone")
   :primary?         (boolean (get calendar "primary"))
   :selected?        (boolean (get calendar "selected"))
   :access-role      (get calendar "accessRole")
   :background-color (get calendar "backgroundColor")
   :foreground-color (get calendar "foregroundColor")})

(defn- google-person
  [person]
  (when (map? person)
    {:email        (get person "email")
     :display-name (get person "displayName")
     :self?        (boolean (get person "self"))}))

(defn- google-attendee-summary
  [attendee]
  (when (map? attendee)
    {:email           (get attendee "email")
     :display-name    (get attendee "displayName")
     :response-status (get attendee "responseStatus")
     :optional?       (boolean (get attendee "optional"))
     :organizer?      (boolean (get attendee "organizer"))
     :self?           (boolean (get attendee "self"))}))

(defn- google-event-summary
  [service-id calendar-id event]
  (let [start (time-map (get event "start"))
        end   (time-map (get event "end"))]
    {:provider           "google-calendar"
     :service-id         (name service-id)
     :calendar-id        calendar-id
     :id                 (get event "id")
     :summary            (get event "summary")
     :description        (get event "description")
     :location           (get event "location")
     :status             (get event "status")
     :html-link          (get event "htmlLink")
     :created            (get event "created")
     :updated            (get event "updated")
     :start              start
     :end                end
     :all-day?           (boolean (:date start))
     :attendees          (into [] (keep google-attendee-summary) (or (get event "attendees") []))
     :organizer          (google-person (get event "organizer"))
     :creator            (google-person (get event "creator"))
     :recurrence         (vec (or (get event "recurrence") []))
     :recurring-event-id (get event "recurringEventId")
     :transparency       (get event "transparency")
     :visibility         (get event "visibility")}))

(defn- microsoft-calendar-summary
  [calendar]
  {:id              (get calendar "id")
   :summary         (get calendar "name")
   :name            (get calendar "name")
   :color           (get calendar "color")
   :can-share?      (boolean (get calendar "canShare"))
   :can-edit?       (boolean (get calendar "canEdit"))
   :owner           (get-in calendar ["owner" "address"])
   :owner-name      (get-in calendar ["owner" "name"])
   :is-default?     (boolean (get calendar "isDefaultCalendar"))
   :is-removable?   (boolean (get calendar "isRemovable"))})

(defn- microsoft-email-address
  [value]
  (when (map? value)
    {:email (get value "address")
     :name  (get value "name")}))

(defn- microsoft-recipient-summary
  [recipient]
  (when (map? recipient)
    (assoc (microsoft-email-address (get recipient "emailAddress"))
           :type (get recipient "type")
           :status (get-in recipient ["status" "response"]))))

(defn- microsoft-event-summary
  [service-id calendar-id event]
  {:provider           "microsoft-calendar"
   :service-id         (name service-id)
   :calendar-id        calendar-id
   :id                 (get event "id")
   :summary            (get event "subject")
   :description        (or (get-in event ["body" "content"])
                           (get event "bodyPreview"))
   :body-preview       (get event "bodyPreview")
   :location           (get-in event ["location" "displayName"])
   :status             (get event "responseStatus")
   :web-link           (get event "webLink")
   :created            (get event "createdDateTime")
   :updated            (get event "lastModifiedDateTime")
   :start              (time-map (get event "start"))
   :end                (time-map (get event "end"))
   :all-day?           (boolean (get event "isAllDay"))
   :attendees          (into [] (keep microsoft-recipient-summary) (or (get event "attendees") []))
   :organizer          (microsoft-recipient-summary (get event "organizer"))
   :recurring-event-id (get event "seriesMasterId")
   :show-as            (get event "showAs")
   :sensitivity        (get event "sensitivity")
   :categories         (vec (or (get event "categories") []))})

(defn- google-calendar-id
  [calendar-id]
  (or (nonblank-str calendar-id) "primary"))

(defn- google-event-path
  [calendar-id event-id]
  (str "/calendars/"
       (encode-path-segment (google-calendar-id calendar-id))
       "/events/"
       (encode-path-segment event-id)))

(defn- microsoft-calendar-events-path
  [calendar-id collection]
  (if-let [calendar-id* (nonblank-str calendar-id)]
    (str "/me/calendars/" (encode-path-segment calendar-id*) "/" collection)
    (str "/me/" collection)))

(defn- microsoft-event-path
  [calendar-id event-id]
  (if-let [calendar-id* (nonblank-str calendar-id)]
    (str "/me/calendars/"
         (encode-path-segment calendar-id*)
         "/events/"
         (encode-path-segment event-id))
    (str "/me/events/" (encode-path-segment event-id))))

(defn- require-event-id!
  [event-id]
  (or (nonblank-str event-id)
      (throw (ex-info "event-id is required"
                      {:type :calendar/missing-event-id}))))

(defn- require-time-range!
  [time-min time-max]
  (when-not (and (nonblank-str time-min)
                 (nonblank-str time-max))
    (throw (ex-info "time-min and time-max are required"
                    {:type :calendar/missing-time-range}))))

(defn- default-time-min
  []
  (str (Instant/now)))

(defn- default-time-max
  []
  (str (.plus (Instant/now) 7 ChronoUnit/DAYS)))

(defrecord GoogleCalendarBackend []
  CalendarBackend
  (backend-key [_]
    :google-calendar)
  (backend-label [_]
    "Google Calendar")
  (backend-default-service-id [_]
    default-service-id)
  (supports-service? [_ service]
    (google-calendar-service? service))
  (auto-detect-service-id [_]
    (google-detect-service-id))
  (backend-list-calendars [_ service-id {:keys [max-results page-token include-hidden?]}]
    (let [response  (google-request service-id
                                    :get
                                    "/users/me/calendarList"
                                    :query-params (list-query-params
                                                   {:max-results max-results
                                                    :page-token  page-token}
                                                   {"showHidden" (when include-hidden? true)}))
          calendars (mapv google-calendar-summary (or (get-in response [:body "items"]) []))]
      {:provider       "google-calendar"
       :service-id     (name service-id)
       :page-token     (nonblank-str page-token)
       :next-page-token (get-in response [:body "nextPageToken"])
       :returned-count (count calendars)
       :calendars      calendars}))
  (backend-list-events [_ service-id {:keys [calendar-id time-min time-max query max-results
                                             page-token include-cancelled? time-zone]}]
    (let [calendar-id* (google-calendar-id calendar-id)
          response     (google-request service-id
                                       :get
                                       (str "/calendars/"
                                            (encode-path-segment calendar-id*)
                                            "/events")
                                       :query-params (list-query-params
                                                      {:max-results max-results
                                                       :page-token  page-token}
                                                      {"timeMin"      time-min
                                                       "timeMax"      time-max
                                                       "q"            (nonblank-str query)
                                                       "singleEvents" true
                                                       "orderBy"      "startTime"
                                                       "showDeleted"  (when include-cancelled? true)
                                                       "timeZone"     (nonblank-str time-zone)}))
          events       (mapv #(google-event-summary service-id calendar-id* %)
                             (or (get-in response [:body "items"]) []))]
      {:provider        "google-calendar"
       :service-id      (name service-id)
       :calendar-id     calendar-id*
       :time-min        time-min
       :time-max        time-max
       :query           (nonblank-str query)
       :page-token      (nonblank-str page-token)
       :next-page-token (get-in response [:body "nextPageToken"])
       :returned-count  (count events)
       :events          events}))
  (backend-read-event [_ service-id calendar-id event-id _]
    (let [calendar-id* (google-calendar-id calendar-id)
          event-id*    (require-event-id! event-id)
          response     (google-request service-id
                                       :get
                                       (google-event-path calendar-id* event-id*))]
      (google-event-summary service-id calendar-id* (:body response))))
  (backend-create-event [_ service-id {:keys [calendar-id send-updates] :as opts}]
    (let [calendar-id* (google-calendar-id calendar-id)
          response     (google-request service-id
                                       :post
                                       (str "/calendars/"
                                            (encode-path-segment calendar-id*)
                                            "/events")
                                       :query-params (clean-query-params
                                                      {"sendUpdates" (nonblank-str send-updates)})
                                       :body (google-event-payload opts))]
      (assoc (google-event-summary service-id calendar-id* (:body response))
             :status "created")))
  (backend-update-event [_ service-id calendar-id event-id {:keys [send-updates] :as opts}]
    (let [calendar-id* (google-calendar-id calendar-id)
          event-id*    (require-event-id! event-id)
          response     (google-request service-id
                                       :patch
                                       (google-event-path calendar-id* event-id*)
                                       :query-params (clean-query-params
                                                      {"sendUpdates" (nonblank-str send-updates)})
                                       :body (google-event-payload opts))]
      (assoc (google-event-summary service-id calendar-id* (:body response))
             :status "updated")))
  (backend-delete-event [_ service-id calendar-id event-id {:keys [send-updates]}]
    (let [calendar-id* (google-calendar-id calendar-id)
          event-id*    (require-event-id! event-id)]
      (google-request service-id
                      :delete
                      (google-event-path calendar-id* event-id*)
                      :query-params (clean-query-params
                                     {"sendUpdates" (nonblank-str send-updates)}))
      {:provider    "google-calendar"
       :service-id  (name service-id)
       :calendar-id calendar-id*
       :event-id    event-id*
       :status      "deleted"}))
  (backend-find-availability [_ service-id {:keys [calendars time-min time-max time-zone]}]
    (require-time-range! time-min time-max)
    (let [calendar-ids (or (seq (mapv str (or calendars [])))
                           ["primary"])
          response     (google-request service-id
                                       :post
                                       "/freeBusy"
                                       :body (cond-> {"timeMin" time-min
                                                      "timeMax" time-max
                                                      "items"   (mapv (fn [id] {"id" id})
                                                                       calendar-ids)}
                                               (nonblank-str time-zone)
                                               (assoc "timeZone" (nonblank-str time-zone))))
          calendars*   (get-in response [:body "calendars"])]
      {:provider    "google-calendar"
       :service-id  (name service-id)
       :time-min    time-min
       :time-max    time-max
       :time-zone   (nonblank-str time-zone)
       :calendars   (mapv (fn [id]
                             {:calendar-id id
                              :busy        (mapv (fn [entry]
                                                   {:start (get entry "start")
                                                    :end   (get entry "end")})
                                                 (or (get-in calendars* [id "busy"]) []))
                              :errors      (vec (or (get-in calendars* [id "errors"]) []))})
                           calendar-ids)})))

(defrecord MicrosoftCalendarBackend []
  CalendarBackend
  (backend-key [_]
    :microsoft-calendar)
  (backend-label [_]
    "Microsoft Calendar")
  (backend-default-service-id [_]
    microsoft-calendar-service-id)
  (supports-service? [_ service]
    (microsoft-calendar-service? service))
  (auto-detect-service-id [_]
    (microsoft-detect-service-id))
  (backend-list-calendars [_ service-id {:keys [max-results page-token time-zone]}]
    (let [initial-path (path-with-query-params
                        "/me/calendars"
                        {"$top" (or max-results default-max-results)})
          path         (or (microsoft-page-path service-id page-token)
                           initial-path)
          response     (microsoft-request service-id :get path :time-zone time-zone)
          calendars    (mapv microsoft-calendar-summary (or (get-in response [:body "value"]) []))]
      {:provider        "microsoft-calendar"
       :service-id      (name service-id)
       :page-token      (nonblank-str page-token)
       :next-page-token (get-in response [:body "@odata.nextLink"])
       :returned-count  (count calendars)
       :calendars       calendars}))
  (backend-list-events [_ service-id {:keys [calendar-id time-min time-max query max-results
                                             page-token time-zone]}]
    (let [calendar-view? (and (nonblank-str time-min) (nonblank-str time-max))
          collection     (if calendar-view? "calendarView" "events")
          base-path      (microsoft-calendar-events-path calendar-id collection)
          params         (clean-query-params
                          (merge {"$top" (or max-results default-max-results)}
                                 (when calendar-view?
                                   {"startDateTime" time-min
                                    "endDateTime"   time-max
                                    "$orderby"      "start/dateTime"})))
          initial-path   (path-with-query-params base-path params)
          path           (or (microsoft-page-path service-id page-token)
                             initial-path)
          response       (microsoft-request service-id :get path :time-zone time-zone)
          query*         (some-> query nonblank-str str/lower-case)
          events         (->> (or (get-in response [:body "value"]) [])
                              (filter (fn [event]
                                        (or (nil? query*)
                                            (str/includes? (str/lower-case
                                                            (str (get event "subject") " "
                                                                 (get event "bodyPreview") " "
                                                                 (get-in event ["location" "displayName"])))
                                                           query*))))
                              (mapv #(microsoft-event-summary service-id calendar-id %)))]
      {:provider        "microsoft-calendar"
       :service-id      (name service-id)
       :calendar-id     (nonblank-str calendar-id)
       :time-min        time-min
       :time-max        time-max
       :query           (nonblank-str query)
       :page-token      (nonblank-str page-token)
       :next-page-token (get-in response [:body "@odata.nextLink"])
       :returned-count  (count events)
       :events          events}))
  (backend-read-event [_ service-id calendar-id event-id {:keys [time-zone]}]
    (let [event-id* (require-event-id! event-id)
          response  (microsoft-request service-id
                                       :get
                                       (microsoft-event-path calendar-id event-id*)
                                       :time-zone time-zone)]
      (microsoft-event-summary service-id calendar-id (:body response))))
  (backend-create-event [_ service-id {:keys [calendar-id time-zone] :as opts}]
    (let [response (microsoft-request service-id
                                      :post
                                      (microsoft-calendar-events-path calendar-id "events")
                                      :time-zone time-zone
                                      :body (microsoft-event-payload opts))]
      (assoc (microsoft-event-summary service-id calendar-id (:body response))
             :status "created")))
  (backend-update-event [_ service-id calendar-id event-id {:keys [time-zone] :as opts}]
    (let [event-id* (require-event-id! event-id)
          response  (microsoft-request service-id
                                       :patch
                                       (microsoft-event-path calendar-id event-id*)
                                       :time-zone time-zone
                                       :body (microsoft-event-payload opts))]
      (assoc (microsoft-event-summary service-id calendar-id (:body response))
             :status "updated")))
  (backend-delete-event [_ service-id calendar-id event-id _]
    (let [event-id* (require-event-id! event-id)]
      (microsoft-request service-id
                         :delete
                         (microsoft-event-path calendar-id event-id*))
      {:provider    "microsoft-calendar"
       :service-id  (name service-id)
       :calendar-id (nonblank-str calendar-id)
       :event-id    event-id*
       :status      "deleted"}))
  (backend-find-availability [_ service-id {:keys [calendars time-min time-max time-zone
                                                   interval-minutes]}]
    (require-time-range! time-min time-max)
    (let [schedules (or (seq (mapv str (or calendars [])))
                        ["me"])
          time-zone* (or (nonblank-str time-zone) "UTC")
          response  (microsoft-request service-id
                                       :post
                                       "/me/calendar/getSchedule"
                                       :time-zone time-zone*
                                       :body {"schedules" schedules
                                              "startTime" {"dateTime" (microsoft-date-time-value time-min)
                                                           "timeZone" time-zone*}
                                              "endTime"   {"dateTime" (microsoft-date-time-value time-max)
                                                           "timeZone" time-zone*}
                                              "availabilityViewInterval" (or interval-minutes
                                                                             default-availability-interval-minutes)})
          values    (or (get-in response [:body "value"]) [])]
      {:provider   "microsoft-calendar"
       :service-id (name service-id)
       :time-min   time-min
       :time-max   time-max
       :time-zone  time-zone*
       :calendars  (mapv (fn [entry]
                            {:calendar-id        (get entry "scheduleId")
                             :availability-view  (get entry "availabilityView")
                             :busy               (mapv (fn [item]
                                                         {:status (get item "status")
                                                          :start  (time-map (get item "start"))
                                                          :end    (time-map (get item "end"))})
                                                       (or (get entry "scheduleItems") []))
                             :errors             (if-let [error (get entry "error")]
                                                   [error]
                                                   [])})
                          values)})))

(defn- calendar-service-provider
  [service]
  (if (ical-feed-service? service)
    "ical-calendar"
    "caldav-calendar"))

(defn- ical-feed-calendar-summary
  [service-id service]
  {:id         "default"
   :summary    (or (nonblank-str (:service/name service))
                   (name service-id))
   :read-only? true})

(defn- fetch-ical-feed-events
  [service-id calendar-id]
  (let [response (caldav-request service-id
                                 :get
                                 ""
                                 :headers {"Accept" "text/calendar"}
                                 :as :string)]
    (ical-text->event-summaries "ical-calendar"
                                service-id
                                (or (nonblank-str calendar-id) "default")
                                (:body response))))

(defn- find-ical-feed-event
  [service-id calendar-id event-id]
  (let [event-id* (require-event-id! event-id)]
    (or (some (fn [event]
                (when (or (= event-id* (:id event))
                          (= event-id* (:uid event)))
                  event))
              (fetch-ical-feed-events service-id calendar-id))
        (throw (ex-info "iCalendar event not found"
                        {:type       :calendar/event-not-found
                         :event-id   event-id*
                         :service-id service-id})))))

(defn- caldav-calendar-summaries
  [service-id {:keys [max-results]}]
  (let [response  (caldav-request service-id
                                  :propfind
                                  "/"
                                  :headers {"Depth" "1"
                                            "Content-Type" "application/xml; charset=utf-8"}
                                  :body (caldav-propfind-body)
                                  :as :string)
        summaries (mapv #(caldav-calendar-summary service-id %)
                        (caldav-multistatus-responses (:body response)))
        calendars (let [calendar-resources (filterv :calendar? summaries)]
                    (or (seq calendar-resources)
                        (seq summaries)
                        [{:id "/" :summary "Calendar" :calendar? true :read-only? false}]))]
    (->> calendars
         (take (or max-results default-max-results))
         vec)))

(defn- caldav-events-from-report
  [service-id calendar-id xml]
  (->> (caldav-multistatus-responses xml)
       (mapcat (fn [response]
                 (let [href          (first-descendant-text response "href")
                       event-path    (caldav-href->path service-id href)
                       calendar-data (first-descendant-text response "calendar-data")]
                   (when (nonblank-str calendar-data)
                     (ical-text->event-summaries "caldav-calendar"
                                                 service-id
                                                 (caldav-calendar-path calendar-id)
                                                 calendar-data
                                                 :event-path event-path)))))
       vec))

(defn- fetch-caldav-events
  [service-id {:keys [calendar-id time-min time-max] :as opts}]
  (let [calendar-path (caldav-calendar-path calendar-id)
        response      (caldav-request service-id
                                      :report
                                      calendar-path
                                      :headers {"Depth" "1"
                                                "Content-Type" "application/xml; charset=utf-8"}
                                      :body (caldav-calendar-query-body time-min time-max)
                                      :as :string)]
    (filter-calendar-events (caldav-events-from-report service-id calendar-path (:body response))
                            opts)))

(defn- calendar-list-events*
  [service-id {:keys [calendar-id] :as opts}]
  (let [service  (db/get-service service-id)
        provider (calendar-service-provider service)]
    (if (= "ical-calendar" provider)
      (filter-calendar-events (fetch-ical-feed-events service-id calendar-id)
                              opts)
      (fetch-caldav-events service-id opts))))

(defn- caldav-read-event*
  [service-id calendar-id event-id]
  (let [event-path (caldav-event-path calendar-id event-id)
        response   (caldav-request service-id
                                   :get
                                   event-path
                                   :headers {"Accept" "text/calendar"}
                                   :as :string)]
    (or (first (ical-text->event-summaries "caldav-calendar"
                                           service-id
                                           (caldav-calendar-path calendar-id)
                                           (:body response)
                                           :event-path event-path))
        (throw (ex-info "CalDAV event not found"
                        {:type       :calendar/event-not-found
                         :event-id   event-id
                         :event-path event-path
                         :service-id service-id})))))

(defn- read-only-calendar-error
  []
  (throw (ex-info "iCalendar feeds are read-only; use a CalDAV collection to create, update, or delete events"
                  {:type :calendar/read-only-backend})))

(defn- non-nil-entries
  [m]
  (into {} (remove (comp nil? val)) m))

(defn- caldav-existing-event->ical-input
  [event]
  {:uid          (:uid event)
   :summary      (:summary event)
   :description  (:description event)
   :location     (:location event)
   :start        (event-time-value event :start)
   :end          (event-time-value event :end)
   :time-zone    (or (event-time-zone event :start)
                     (event-time-zone event :end))
   :all-day?     (:all-day? event)
   :attendees    (:attendees event)
   :recurrence   (:recurrence event)
   :transparency (:transparency event)
   :status       (:status event)})

(defn- caldav-create-event*
  [service-id {:keys [calendar-id summary start end] :as opts}]
  (let [uid        (str (random-uuid) "@xia.local")
        event-path (caldav-calendar-resource-path calendar-id uid)
        built      (build-ical-event (assoc opts
                                            :uid uid
                                            :summary summary
                                            :start start
                                            :end end))]
    (caldav-request service-id
                    :put
                    event-path
                    :headers {"Content-Type" "text/calendar; charset=utf-8"}
                    :body (:body built)
                    :as :string)
    (assoc (first (ical-text->event-summaries "caldav-calendar"
                                             service-id
                                             (caldav-calendar-path calendar-id)
                                             (:body built)
                                             :event-path event-path))
           :status "created")))

(defn- caldav-update-event*
  [service-id calendar-id event-id opts]
  (let [event-path (caldav-event-path calendar-id event-id)
        existing   (caldav-read-event* service-id calendar-id event-path)
        merged     (merge (caldav-existing-event->ical-input existing)
                          (non-nil-entries opts))
        built      (build-ical-event merged)]
    (caldav-request service-id
                    :put
                    event-path
                    :headers {"Content-Type" "text/calendar; charset=utf-8"}
                    :body (:body built)
                    :as :string)
    (assoc (first (ical-text->event-summaries "caldav-calendar"
                                             service-id
                                             (caldav-calendar-path calendar-id)
                                             (:body built)
                                             :event-path event-path))
           :status "updated")))

(defn- busy-block
  [event]
  (when-not (= "transparent" (:transparency event))
    {:event-id (:id event)
     :summary  (:summary event)
     :start    (:start event)
     :end      (:end event)}))

(defrecord CalDAVCalendarBackend []
  CalendarBackend
  (backend-key [_]
    :caldav-calendar)
  (backend-label [_]
    "CalDAV or iCalendar")
  (backend-default-service-id [_]
    caldav-calendar-service-id)
  (supports-service? [_ service]
    (caldav-calendar-service? service))
  (auto-detect-service-id [_]
    (caldav-detect-service-id))
  (backend-list-calendars [_ service-id {:keys [max-results page-token] :as opts}]
    (let [service   (db/get-service service-id)
          provider  (calendar-service-provider service)
          calendars (if (= "ical-calendar" provider)
                      [(ical-feed-calendar-summary service-id service)]
                      (caldav-calendar-summaries service-id opts))]
      {:provider        provider
       :service-id      (name service-id)
       :page-token      (nonblank-str page-token)
       :next-page-token nil
       :returned-count  (count calendars)
       :calendars       (if (= "ical-calendar" provider)
                          calendars
                          (vec (take (or max-results default-max-results) calendars)))}))
  (backend-list-events [_ service-id {:keys [calendar-id time-min time-max query max-results
                                             page-token] :as opts}]
    (let [service      (db/get-service service-id)
          provider     (calendar-service-provider service)
          calendar-id* (if (= "ical-calendar" provider)
                         (or (nonblank-str calendar-id) "default")
                         (caldav-calendar-path calendar-id))
          events       (calendar-list-events* service-id opts)]
      {:provider        provider
       :service-id      (name service-id)
       :calendar-id     calendar-id*
       :time-min        time-min
       :time-max        time-max
       :query           (nonblank-str query)
       :page-token      (nonblank-str page-token)
       :next-page-token nil
       :returned-count  (count events)
       :events          (vec (take (or max-results default-max-results) events))}))
  (backend-read-event [_ service-id calendar-id event-id _]
    (let [service  (db/get-service service-id)
          provider (calendar-service-provider service)]
      (if (= "ical-calendar" provider)
        (find-ical-feed-event service-id calendar-id event-id)
        (caldav-read-event* service-id calendar-id event-id))))
  (backend-create-event [_ service-id opts]
    (let [service (db/get-service service-id)]
      (if (ical-feed-service? service)
        (read-only-calendar-error)
        (caldav-create-event* service-id opts))))
  (backend-update-event [_ service-id calendar-id event-id opts]
    (let [service (db/get-service service-id)]
      (if (ical-feed-service? service)
        (read-only-calendar-error)
        (caldav-update-event* service-id calendar-id event-id opts))))
  (backend-delete-event [_ service-id calendar-id event-id _]
    (let [service (db/get-service service-id)]
      (if (ical-feed-service? service)
        (read-only-calendar-error)
        (let [event-path (caldav-event-path calendar-id event-id)]
          (caldav-request service-id :delete event-path :as :string)
          {:provider    "caldav-calendar"
           :service-id  (name service-id)
           :calendar-id (caldav-calendar-path calendar-id)
           :event-id    event-path
           :status      "deleted"}))))
  (backend-find-availability [_ service-id {:keys [calendars time-min time-max] :as opts}]
    (require-time-range! time-min time-max)
    (let [service      (db/get-service service-id)
          provider     (calendar-service-provider service)
          calendar-ids (if (= "ical-calendar" provider)
                         [(or (first (seq (mapv str (or calendars []))))
                              "default")]
                         (or (seq (mapv str (or calendars [])))
                             ["/"]))]
      {:provider   provider
       :service-id (name service-id)
       :time-min   time-min
       :time-max   time-max
       :calendars  (mapv (fn [calendar-id]
                            (let [events (calendar-list-events* service-id
                                                                (assoc opts
                                                                       :calendar-id calendar-id
                                                                       :max-results 250))]
                              {:calendar-id calendar-id
                               :busy        (into [] (keep busy-block) events)
                               :errors      []}))
                          calendar-ids)})))

(def ^:private backends
  [(->GoogleCalendarBackend)
   (->MicrosoftCalendarBackend)
   (->CalDAVCalendarBackend)])

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
                    "a supported calendar backend")]
        (throw (ex-info (str "Service " (name candidate)
                             " is not configured for " label)
                        {:service-id candidate
                         :base-url   (:service/base-url service)}))))))

(defn- detect-calendar-target
  []
  (some (fn [backend]
          (when-let [service-id (auto-detect-service-id backend)]
            {:backend backend
             :service-id service-id}))
        backends))

(defn- resolve-calendar-target
  [service-id]
  (let [candidate (normalize-service-id service-id)]
    (cond
      candidate
      (or (backend-target-for-existing-service candidate)
          {:backend (primary-backend)
           :service-id candidate})

      :else
      (or (detect-calendar-target)
          (let [backend (primary-backend)]
            {:backend backend
             :service-id (backend-default-service-id backend)})))))

(defn list-calendars
  "List calendars using the detected calendar backend."
  [& {:keys [service-id max-results page-token include-hidden? time-zone]
      :or   {max-results default-max-results}}]
  (let [{:keys [backend service-id]} (resolve-calendar-target service-id)]
    (backend-list-calendars backend
                            service-id
                            {:max-results     max-results
                             :page-token      page-token
                             :include-hidden? include-hidden?
                             :time-zone       time-zone})))

(defn list-events
  "List upcoming or bounded events using the detected calendar backend.

   When no range is supplied, the default range is now through seven days from
   now so the tool returns useful upcoming events instead of an unbounded feed."
  [& {:keys [service-id calendar-id time-min time-max query max-results page-token
             include-cancelled? time-zone]
      :or   {max-results default-max-results}}]
  (let [time-min* (or (nonblank-str time-min) (default-time-min))
        time-max* (or (nonblank-str time-max) (default-time-max))
        {:keys [backend service-id]} (resolve-calendar-target service-id)]
    (backend-list-events backend
                         service-id
                         {:calendar-id        calendar-id
                          :time-min           time-min*
                          :time-max           time-max*
                          :query              query
                          :max-results        max-results
                          :page-token         page-token
                          :include-cancelled? include-cancelled?
                          :time-zone          time-zone})))

(defn read-event
  "Read a calendar event by id."
  [event-id & {:keys [service-id calendar-id time-zone]}]
  (let [{:keys [backend service-id]} (resolve-calendar-target service-id)]
    (backend-read-event backend
                        service-id
                        calendar-id
                        event-id
                        {:time-zone time-zone})))

(defn create-event
  "Create a calendar event.

   `start` and `end` should be ISO date-time strings, or YYYY-MM-DD strings for
   all-day events. For all-day events, providers expect `end` to be the
   exclusive ending date."
  [summary start end & {:keys [service-id calendar-id description location attendees
                               time-zone all-day? recurrence transparency visibility
                               show-as sensitivity send-updates html?]}]
  (when-not (nonblank-str summary)
    (throw (ex-info "summary is required"
                    {:type :calendar/missing-summary})))
  (when-not (and (nonblank-str start) (nonblank-str end))
    (throw (ex-info "start and end are required"
                    {:type :calendar/missing-event-time})))
  (let [{:keys [backend service-id]} (resolve-calendar-target service-id)]
    (backend-create-event backend
                          service-id
                          {:calendar-id  calendar-id
                           :summary      summary
                           :description  description
                           :location     location
                           :start        start
                           :end          end
                           :attendees    attendees
                           :time-zone    time-zone
                           :all-day?     all-day?
                           :recurrence   recurrence
                           :transparency transparency
                           :visibility   visibility
                           :show-as      show-as
                           :sensitivity  sensitivity
                           :send-updates send-updates
                           :html?        html?})))

(defn update-event
  "Patch a calendar event by id."
  [event-id & {:keys [service-id calendar-id summary description location start end
                      attendees time-zone all-day? recurrence transparency visibility
                      show-as sensitivity send-updates html?]}]
  (let [{:keys [backend service-id]} (resolve-calendar-target service-id)]
    (backend-update-event backend
                          service-id
                          calendar-id
                          event-id
                          {:summary      summary
                           :description  description
                           :location     location
                           :start        start
                           :end          end
                           :attendees    attendees
                           :time-zone    time-zone
                           :all-day?     all-day?
                           :recurrence   recurrence
                           :transparency transparency
                           :visibility   visibility
                           :show-as      show-as
                           :sensitivity  sensitivity
                           :send-updates send-updates
                           :html?        html?})))

(defn delete-event
  "Delete a calendar event by id."
  [event-id & {:keys [service-id calendar-id send-updates]}]
  (let [{:keys [backend service-id]} (resolve-calendar-target service-id)]
    (backend-delete-event backend
                          service-id
                          calendar-id
                          event-id
                          {:send-updates send-updates})))

(defn find-availability
  "Find busy blocks for one or more calendars over a required time range."
  [& {:keys [service-id calendars time-min time-max time-zone interval-minutes]}]
  (let [{:keys [backend service-id]} (resolve-calendar-target service-id)]
    (backend-find-availability backend
                               service-id
                               {:calendars        calendars
                                :time-min         time-min
                                :time-max         time-max
                                :time-zone        time-zone
                                :interval-minutes interval-minutes})))
