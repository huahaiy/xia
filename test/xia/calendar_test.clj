(ns xia.calendar-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [xia.calendar :as calendar]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.service :as service]
            [xia.test-helpers :refer [with-test-db]]
            [xia.tool :as tool])
  (:import [java.util Date]))

(use-fixtures :each with-test-db)

(defn- register-google-calendar-service!
  []
  (db/register-service! {:id        :google-calendar
                         :name      "Google Calendar"
                         :base-url  "https://www.googleapis.com/calendar/v3"
                         :auth-type :bearer
                         :auth-key  "token"}))

(defn- register-microsoft-calendar-service!
  []
  (db/register-service! {:id        :microsoft-calendar
                         :name      "Microsoft Calendar"
                         :base-url  "https://graph.microsoft.com/v1.0"
                         :auth-type :bearer
                         :auth-key  "token"}))

(defn- register-caldav-calendar-service!
  []
  (db/register-service! {:id        :caldav-calendar
                         :name      "CalDAV Calendar"
                         :base-url  "https://calendar.example/dav/user/calendar"
                         :auth-type :basic
                         :auth-key  "user:pass"}))

(defn- register-ical-calendar-service!
  []
  (db/register-service! {:id        :ical-calendar
                         :name      "Holidays"
                         :base-url  "https://calendar.example/holidays.ics"
                         :auth-type :bearer
                         :auth-key  "token"}))

(defn- oauth-account
  [id provider-template]
  {:id                id
   :name              (name id)
   :provider-template provider-template
   :authorize-url     "https://example.com/oauth/authorize"
   :token-url         "https://example.com/oauth/token"
   :client-id         "client"
   :client-secret     "secret"
   :scopes            "calendar"
   :access-token      "access"
   :refresh-token     "refresh"
   :token-type        "Bearer"
   :connected-at      (Date.)})

(deftest google-list-events-uses-calendar-api-and-normalizes-events
  (register-google-calendar-service!)
  (let [calls (atom [])]
    (with-redefs [service/request
                  (fn [service-id method path & {:as opts}]
                    (swap! calls conj {:service-id service-id
                                       :method     method
                                       :path       path
                                       :opts       opts})
                    {:status 200
                     :body   {"nextPageToken" "next"
                              "items" [{"id" "evt-1"
                                        "summary" "Standup"
                                        "description" "Daily sync"
                                        "location" "Room 4"
                                        "htmlLink" "https://calendar.example/evt-1"
                                        "start" {"dateTime" "2026-04-29T09:00:00-07:00"
                                                 "timeZone" "America/Los_Angeles"}
                                        "end" {"dateTime" "2026-04-29T09:30:00-07:00"
                                               "timeZone" "America/Los_Angeles"}
                                        "attendees" [{"email" "a@example.com"
                                                      "responseStatus" "accepted"}]}]}})]
      (let [result (calendar/list-events
                    :calendar-id "primary"
                    :time-min "2026-04-29T00:00:00Z"
                    :time-max "2026-04-30T00:00:00Z"
                    :max-results 5)
            call   (first @calls)
            event  (first (:events result))]
        (is (= :google-calendar (:service-id call)))
        (is (= :get (:method call)))
        (is (= "/calendars/primary/events" (:path call)))
        (is (= {"maxResults" 5
                "timeMin" "2026-04-29T00:00:00Z"
                "timeMax" "2026-04-30T00:00:00Z"
                "singleEvents" true
                "orderBy" "startTime"}
               (select-keys (get-in call [:opts :query-params])
                            ["maxResults" "timeMin" "timeMax" "singleEvents" "orderBy"])))
        (is (= "next" (:next-page-token result)))
        (is (= "evt-1" (:id event)))
        (is (= "Standup" (:summary event)))
        (is (= "2026-04-29T09:00:00-07:00" (get-in event [:start :date-time])))
        (is (= [{:email "a@example.com"
                 :display-name nil
                 :response-status "accepted"
                 :optional? false
                 :organizer? false
                 :self? false}]
               (:attendees event)))))))

(deftest google-create-event-builds-provider-payload
  (register-google-calendar-service!)
  (let [calls (atom [])]
    (with-redefs [service/request
                  (fn [service-id method path & {:as opts}]
                    (swap! calls conj {:service-id service-id
                                       :method     method
                                       :path       path
                                       :opts       opts})
                    {:status 200
                     :body   (assoc (:body opts)
                                    "id" "created-1"
                                    "start" (get (:body opts) "start")
                                    "end" (get (:body opts) "end"))})]
      (let [result (calendar/create-event
                    "Planning"
                    "2026-04-29T10:00:00-07:00"
                    "2026-04-29T11:00:00-07:00"
                    :time-zone "America/Los_Angeles"
                    :attendees ["a@example.com"
                                {"email" "b@example.com"
                                 "name" "Bee"
                                 "optional" true}]
                    :send-updates "all")
            call   (first @calls)
            body   (get-in call [:opts :body])]
        (is (= :post (:method call)))
        (is (= "/calendars/primary/events" (:path call)))
        (is (= {"sendUpdates" "all"}
               (get-in call [:opts :query-params])))
        (is (= "Planning" (get body "summary")))
        (is (= {"dateTime" "2026-04-29T10:00:00-07:00"
                "timeZone" "America/Los_Angeles"}
               (get body "start")))
        (is (= [{"email" "a@example.com"}
                {"email" "b@example.com"
                 "displayName" "Bee"
                 "optional" true}]
               (get body "attendees")))
        (is (= "created" (:status result)))
        (is (= "created-1" (:id result)))))))

(deftest microsoft-find-availability-normalizes-schedule-items
  (register-microsoft-calendar-service!)
  (let [calls (atom [])]
    (with-redefs [service/request
                  (fn [service-id method path & {:as opts}]
                    (swap! calls conj {:service-id service-id
                                       :method     method
                                       :path       path
                                       :opts       opts})
                    {:status 200
                     :body   {"value" [{"scheduleId" "me@example.com"
                                        "availabilityView" "2"
                                        "scheduleItems" [{"status" "busy"
                                                          "start" {"dateTime" "2026-04-29T09:00:00"
                                                                   "timeZone" "Pacific Standard Time"}
                                                          "end" {"dateTime" "2026-04-29T09:30:00"
                                                                 "timeZone" "Pacific Standard Time"}}]}]}})]
      (let [result (calendar/find-availability
                    :service-id :microsoft-calendar
                    :calendars ["me@example.com"]
                    :time-min "2026-04-29T00:00:00"
                    :time-max "2026-04-30T00:00:00"
                    :time-zone "Pacific Standard Time"
                    :interval-minutes 15)
            call   (first @calls)
            body   (get-in call [:opts :body])]
        (is (= :microsoft-calendar (:service-id call)))
        (is (= :post (:method call)))
        (is (= "/me/calendar/getSchedule" (:path call)))
        (is (= {"Prefer" "outlook.timezone=\"Pacific Standard Time\""}
               (get-in call [:opts :headers])))
        (is (= ["me@example.com"] (get body "schedules")))
        (is (= 15 (get body "availabilityViewInterval")))
        (is (= "busy" (get-in result [:calendars 0 :busy 0 :status])))
        (is (= "2026-04-29T09:00:00"
               (get-in result [:calendars 0 :busy 0 :start :date-time])))))))

(deftest microsoft-mail-service-is-not-treated-as-calendar
  (db/register-service! {:id        :microsoft-mail
                         :name      "Microsoft Mail"
                         :base-url  "https://graph.microsoft.com/v1.0"
                         :auth-type :bearer
                         :auth-key  "token"})
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"not configured for .*calendar"
       (calendar/list-calendars :service-id :microsoft-mail))))

(deftest caldav-list-events-uses-calendar-query-report
  (register-caldav-calendar-service!)
  (let [calls (atom [])
        calendar-data "BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:evt-1@example.com
SUMMARY:Standup
LOCATION:Room 4
DTSTART:20260429T160000Z
DTEND:20260429T163000Z
ATTENDEE;CN=Ana;PARTSTAT=ACCEPTED:mailto:a@example.com
END:VEVENT
END:VCALENDAR"
        xml (str "<d:multistatus xmlns:d=\"DAV:\" xmlns:c=\"urn:ietf:params:xml:ns:caldav\">"
                 "<d:response>"
                 "<d:href>/dav/user/calendar/evt-1.ics</d:href>"
                 "<d:propstat><d:prop><c:calendar-data><![CDATA["
                 calendar-data
                 "]]></c:calendar-data></d:prop></d:propstat>"
                 "</d:response>"
                 "</d:multistatus>")]
    (with-redefs [service/request
                  (fn [service-id method path & {:as opts}]
                    (swap! calls conj {:service-id service-id
                                       :method     method
                                       :path       path
                                       :opts       opts})
                    {:status 207
                     :body   xml})]
      (let [result (calendar/list-events
                    :service-id :caldav-calendar
                    :calendar-id "/"
                    :time-min "2026-04-29T00:00:00Z"
                    :time-max "2026-04-30T00:00:00Z")
            call   (first @calls)
            event  (first (:events result))]
        (is (= :caldav-calendar (:service-id call)))
        (is (= :report (:method call)))
        (is (= "/" (:path call)))
        (is (= "1" (get-in call [:opts :headers "Depth"])))
        (is (str/includes? (get-in call [:opts :body])
                           "<c:time-range start=\"20260429T000000Z\" end=\"20260430T000000Z\"/>"))
        (is (= "caldav-calendar" (:provider result)))
        (is (= "/evt-1.ics" (:id event)))
        (is (= "evt-1@example.com" (:uid event)))
        (is (= "Standup" (:summary event)))
        (is (= "2026-04-29T16:00:00Z" (get-in event [:start :date-time])))
        (is (= [{:email "a@example.com"
                 :display-name "Ana"
                 :response-status "accepted"
                 :optional? false
                 :organizer? false
                 :self? false}]
               (:attendees event)))))))

(deftest caldav-create-event-puts-icalendar-resource
  (register-caldav-calendar-service!)
  (let [calls (atom [])]
    (with-redefs [service/request
                  (fn [service-id method path & {:as opts}]
                    (swap! calls conj {:service-id service-id
                                       :method     method
                                       :path       path
                                       :opts       opts})
                    {:status 201
                     :body   ""})]
      (let [result (calendar/create-event
                    "Planning"
                    "2026-04-29T10:00:00-07:00"
                    "2026-04-29T11:00:00-07:00"
                    :service-id :caldav-calendar
                    :calendar-id "/"
                    :attendees ["a@example.com"])
            call   (first @calls)
            body   (get-in call [:opts :body])]
        (is (= :put (:method call)))
        (is (re-matches #"/.+\.ics" (:path call)))
        (is (= "text/calendar; charset=utf-8"
               (get-in call [:opts :headers "Content-Type"])))
        (is (str/includes? body "BEGIN:VCALENDAR"))
        (is (str/includes? body "SUMMARY:Planning"))
        (is (str/includes? body "DTSTART:20260429T170000Z"))
        (is (str/includes? body "ATTENDEE:mailto:a@example.com"))
        (is (= "created" (:status result)))
        (is (= "caldav-calendar" (:provider result)))))))

(deftest ical-feed-lists-events-and-rejects-writes
  (register-ical-calendar-service!)
  (let [calls (atom [])
        feed "BEGIN:VCALENDAR
VERSION:2.0
X-WR-CALNAME:Holidays
BEGIN:VEVENT
UID:holiday-1
SUMMARY:Founders Day
DTSTART;VALUE=DATE:20260429
DTEND;VALUE=DATE:20260430
END:VEVENT
END:VCALENDAR"]
    (with-redefs [service/request
                  (fn [service-id method path & {:as opts}]
                    (swap! calls conj {:service-id service-id
                                       :method     method
                                       :path       path
                                       :opts       opts})
                    {:status 200
                     :body   feed})]
      (let [result (calendar/list-events
                    :service-id :ical-calendar
                    :time-min "2026-04-29"
                    :time-max "2026-04-30")
            call   (first @calls)
            event  (first (:events result))]
        (is (= :ical-calendar (:service-id call)))
        (is (= :get (:method call)))
        (is (= "" (:path call)))
        (is (= "ical-calendar" (:provider result)))
        (is (= "default" (:calendar-id result)))
        (is (= "holiday-1" (:id event)))
        (is (= "2026-04-29" (get-in event [:start :date])))
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"read-only"
             (calendar/create-event
              "Planning"
              "2026-04-29T10:00:00-07:00"
              "2026-04-29T11:00:00-07:00"
              :service-id :ical-calendar)))))))

(deftest auto-detects-google-calendar-oauth-account
  (db/register-oauth-account! (oauth-account :calendar-account :google-calendar))
  (let [calls (atom [])]
    (with-redefs [service/request
                  (fn [service-id method path & {:as opts}]
                    (swap! calls conj {:service-id service-id
                                       :method     method
                                       :path       path
                                       :opts       opts})
                    {:status 200
                     :body   {"items" []}})]
      (let [result  (calendar/list-calendars)
            service (db/get-service :google-calendar)]
        (is (= "google-calendar" (:service-id result)))
        (is (= :calendar-account (:service/oauth-account service)))
        (is (= :oauth-account (:service/auth-type service)))
        (is (= :google-calendar (:service-id (first @calls))))))))

(deftest calendar-list-tool-executes-through-sci
  (register-google-calendar-service!)
  (tool/ensure-bundled-tools!)
  (with-redefs [service/request
                (fn [service-id method path & {:as _opts}]
                  (is (= :google-calendar service-id))
                  (is (= :get method))
                  (is (= "/users/me/calendarList" path))
                  {:status 200
                   :body   {"items" [{"id" "primary"
                                      "summary" "Main"
                                      "primary" true}]}})]
    (let [session-id (random-uuid)]
      (prompt/register-approval! :terminal (constantly true))
      (try
        (let [result (tool/execute-tool :calendar-list
                                        {"max_results" 3}
                                        {:channel :terminal
                                         :session-id session-id})]
          (is (= "google-calendar" (:service-id result)))
          (is (= "primary" (get-in result [:calendars 0 :id]))))
        (finally
          (prompt/register-approval! :terminal nil)
          (tool/clear-session-approvals! session-id))))))
