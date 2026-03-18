(ns xia.channel.http-test
  (:require [charred.api :as json]
            [clojure.test :refer :all]
            [clojure.string :as str]
            [xia.agent]
            [xia.artifact :as artifact]
            [xia.channel.http :as http]
            [xia.db :as db]
            [xia.local-doc :as local-doc]
            [xia.schedule :as schedule]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.nio.charset StandardCharsets]
           [java.util Arrays Base64 UUID]
           [org.apache.pdfbox.pdmodel PDDocument PDPage PDPageContentStream]
           [org.apache.pdfbox.pdmodel.font PDType1Font Standard14Fonts$FontName]))

(use-fixtures :each with-test-db)

(defn- request-body [payload]
  (ByteArrayInputStream.
    (.getBytes (json/write-json-str payload) StandardCharsets/UTF_8)))

(declare ui-headers)

(defn- multipart-body
  [boundary parts]
  (let [encoding StandardCharsets/UTF_8]
    (with-open [out (ByteArrayOutputStream.)]
      (doseq [{:keys [field-name filename content-type body-bytes]} parts]
        (.write out (.getBytes (str "--" boundary "\r\n") encoding))
        (.write out (.getBytes (str "Content-Disposition: form-data; name=\""
                                    field-name
                                    "\"; filename=\""
                                    filename
                                    "\"\r\n")
                               encoding))
        (when content-type
          (.write out (.getBytes (str "Content-Type: " content-type "\r\n") encoding)))
        (.write out (.getBytes "\r\n" encoding))
        (.write out ^bytes body-bytes)
        (.write out (.getBytes "\r\n" encoding)))
      (.write out (.getBytes (str "--" boundary "--\r\n") encoding))
      (.toByteArray out))))

(defn- multipart-request
  [uri parts]
  (let [boundary (str "----xia-test-boundary-" (random-uuid))]
    {:uri            uri
     :request-method :post
     :headers        (assoc (ui-headers)
                            "content-type" (str "multipart/form-data; boundary=" boundary))
     :body           (ByteArrayInputStream. (multipart-body boundary parts))}))

(defn- oversized-request-body []
  (let [size (inc (var-get #'http/max-request-body-bytes))
        sb   (StringBuilder. size)]
    (dotimes [_ size]
      (.append sb "x"))
    (ByteArrayInputStream.
      (.getBytes (.toString sb) StandardCharsets/UTF_8))))

(defn- response-json [response]
  (json/read-json (:body response)))

(defn- local-session-cookie []
  (let [response (#'http/router {:uri "/" :request-method :get})]
    (first (str/split (get-in response [:headers "Set-Cookie"]) #";"))))

(defn- ui-headers []
  {"origin" "http://localhost:3008"
   "cookie" (local-session-cookie)})

(defn- sample-pdf-bytes
  [text]
  (with-open [doc (PDDocument.)
              out (ByteArrayOutputStream.)]
    (let [page (PDPage.)]
      (.addPage doc page)
      (with-open [content (PDPageContentStream. doc page)]
        (.beginText content)
        (.setFont content (PDType1Font. Standard14Fonts$FontName/HELVETICA) 12)
        (.newLineAtOffset content 72 720)
        (.showText content text)
        (.endText content))
      (.save doc out)
      (.toByteArray out))))

(defn- wait-for
  [f]
  (loop [attempt 0]
    (let [result (f)]
      (cond
        result result
        (>= attempt 49) nil
        :else (do (Thread/sleep 20)
                  (recur (inc attempt)))))))

(deftest root-route-serves-local-web-interface
  (let [response (#'http/router {:uri "/" :request-method :get})]
    (is (= 200 (:status response)))
    (is (= "text/html; charset=utf-8" (get-in response [:headers "Content-Type"])))
    (is (re-find #"xia-local-session=" (get-in response [:headers "Set-Cookie"])))
    (is (re-find #"HttpOnly" (get-in response [:headers "Set-Cookie"])))
    (is (re-find #"SameSite=Strict" (get-in response [:headers "Set-Cookie"])))
    (is (re-find #"<title>Xia</title>" (:body response)))
    (is (re-find #"Approval Required" (:body response)))
    (is (re-find #"Copy transcript" (:body response)))
    (is (re-find #"Local Documents" (:body response)))
    (is (re-find #"Artifacts" (:body response)))
    (is (re-find #"Notes" (:body response)))
    (is (re-find #"History" (:body response)))
    (is (re-find #"Scheduled Runs" (:body response)))
    (is (re-find #"Settings" (:body response)))
    (is (re-find #"AI Models" (:body response)))
    (is (re-find #"Episode Retention" (:body response)))
    (is (re-find #"Knowledge Decay" (:body response)))
    (is (re-find #"Archive After Bottom \(Days\)" (:body response)))
    (is (re-find #"Workloads" (:body response)))
    (is (re-find #"System Prompt Budget" (:body response)))
    (is (re-find #"App Connections" (:body response)))
    (is (re-find #"Service Preset" (:body response)))
    (is (re-find #"Apply preset" (:body response)))
    (is (re-find #"Add to API list" (:body response)))
    (is (re-find #"Site Logins" (:body response)))
    (is (re-find #"Rate Limit \(req/min\)" (:body response)))
    (is (re-find #"<textarea" (:body response)))
    (is (re-find #"src=\"app.js\"" (:body response)))
    (is (re-find #"href=\"style.css\"" (:body response)))
    (is (re-find #"rel=\"icon\" type=\"image/x-icon\" href=\"favicon.ico\"" (:body response)))
    (is (re-find #"href=\"favicon/favicon-32x32.png\"" (:body response)))
    (is (re-find #"href=\"favicon/site.webmanifest\"" (:body response)))
    (is (re-find #"src=\"favicon/android-chrome-192x192.png\"" (:body response)))
    (is (re-find #"alt=\"Xia logo\"" (:body response)))))

(deftest serves-static-resources
  (testing "serves style.css"
    (let [response (#'http/router {:uri "/style.css" :request-method :get})]
      (is (= 200 (:status response)))
      (is (= "text/css" (get-in response [:headers "Content-Type"])))
      (is (re-find #":root" (:body response)))))
  (testing "serves app.js"
    (let [response (#'http/router {:uri "/app.js" :request-method :get})]
      (is (= 200 (:status response)))
      (is (= "text/javascript" (get-in response [:headers "Content-Type"])))
      (is (re-find #"sessionStorage\.getItem" (:body response)))))
  (testing "serves favicon.ico"
    (let [response (#'http/router {:uri "/favicon.ico" :request-method :get})]
      (is (= 200 (:status response)))
      (is (= "image/x-icon" (get-in response [:headers "Content-Type"])))
      (is (instance? (Class/forName "[B") (:body response)))
      (is (pos? (alength ^bytes (:body response))))))
  (testing "serves favicon manifest"
    (let [response (#'http/router {:uri "/favicon/site.webmanifest" :request-method :get})]
      (is (= 200 (:status response)))
      (is (= "application/manifest+json; charset=utf-8"
             (get-in response [:headers "Content-Type"])))
      (is (re-find #"/favicon/android-chrome-192x192.png" (:body response)))))
  (testing "serves favicon png"
    (let [response (#'http/router {:uri "/favicon/favicon-32x32.png" :request-method :get})]
      (is (= 200 (:status response)))
      (is (= "image/png" (get-in response [:headers "Content-Type"])))
      (is (instance? (Class/forName "[B") (:body response)))
      (is (pos? (alength ^bytes (:body response)))))))

(deftest create-session-route-returns-session-id
  (let [response (#'http/router {:uri            "/sessions"
                                 :request-method :post
                                 :headers        (ui-headers)})
        body     (response-json response)
        sid      (UUID/fromString (get body "session_id"))]
    (is (= 200 (:status response)))
    (is (= :http
           (ffirst (db/q '[:find ?channel :in $ ?sid
                           :where
                           [?s :session/id ?sid]
                           [?s :session/channel ?channel]]
                         sid))))))

(deftest create-session-route-schedules-rest-session-finalizer
  (let [scheduled (atom nil)]
    (with-redefs [xia.channel.http/schedule-rest-session-finalizer! (fn [sid]
                                                                      (reset! scheduled sid))]
      (let [response (#'http/router {:uri            "/sessions"
                                     :request-method :post
                                     :headers        (ui-headers)})
            body     (response-json response)
            sid      (UUID/fromString (get body "session_id"))]
        (is (= 200 (:status response)))
        (is (= sid @scheduled))))))

(deftest chat-route-creates-http-session
  (with-redefs [xia.agent/process-message (fn [_session-id user-message & _]
                                            (str "echo: " user-message))]
    (let [response (#'http/router {:uri            "/chat"
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"message" "hello"})})
          body     (response-json response)
          sid      (UUID/fromString (get body "session_id"))]
      (is (= 200 (:status response)))
      (is (= "application/json" (get-in response [:headers "Content-Type"])))
      (is (= "assistant" (get body "role")))
      (is (= "echo: hello" (get body "content")))
      (is (= :http
             (ffirst (db/q '[:find ?channel :in $ ?sid
                             :where
                             [?s :session/id ?sid]
                             [?s :session/channel ?channel]]
                           sid)))))))

(deftest chat-route-schedules-rest-session-finalizer
  (let [scheduled (atom nil)]
    (with-redefs [xia.agent/process-message (fn [_session-id user-message & _]
                                              (str "echo: " user-message))
                  xia.channel.http/schedule-rest-session-finalizer! (fn [sid]
                                                                      (reset! scheduled sid))]
      (let [response (#'http/router {:uri            "/chat"
                                     :request-method :post
                                     :headers        (ui-headers)
                                     :body           (request-body {"message" "hello"})})
            body     (response-json response)
            sid      (UUID/fromString (get body "session_id"))]
        (is (= 200 (:status response)))
        (is (= sid @scheduled))))))

(deftest chat-route-reuses-provided-session-id
  (let [seen-session (atom nil)
        sid          (db/create-session! :http)]
    (with-redefs [xia.agent/process-message (fn [session-id _user-message & _]
                                              (reset! seen-session session-id)
                                              "ok")]
      (let [response (#'http/router {:uri            "/chat"
                                     :request-method :post
                                     :headers        (ui-headers)
                                     :body           (request-body {"message" "hello"
                                                                    "session_id" (str sid)})})
            body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= (str sid) (get body "session_id")))
        (is (= sid @seen-session))))))

(deftest chat-route-rejects-closed-session
  (let [sid (db/create-session! :http)]
    (#'http/set-session-active! sid false)
    (let [response (#'http/router {:uri            "/chat"
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"message" "hello"
                                                                  "session_id" (str sid)})})
          body     (response-json response)]
      (is (= 409 (:status response)))
      (is (= "session closed" (get body "error"))))))

(deftest chat-route-blocks-non-local-origins
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :headers        {"origin" "https://evil.example"}
                                 :body           (request-body {"message" "hello"})})
        body     (response-json response)]
    (is (= 403 (:status response)))
    (is (= "forbidden origin" (get body "error")))))

(deftest chat-route-blocks-missing-session-secret
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :headers        {"origin" "http://localhost:3008"}
                                 :body           (request-body {"message" "hello"})})
        body     (response-json response)]
    (is (= 401 (:status response)))
    (is (= "missing or invalid local session secret" (get body "error")))))

(deftest chat-route-blocks-no-origin-without-session-secret
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :body           (request-body {"message" "hello"})})
        body     (response-json response)]
    (is (= 401 (:status response)))
    (is (= "missing or invalid local session secret" (get body "error")))))

(deftest chat-route-allows-local-origins-with-session-secret
  (with-redefs [xia.agent/process-message (fn [_session-id _user-message & _] "ok")]
    (let [response (#'http/router {:uri            "/chat"
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"message" "hello"})})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= "ok" (get body "content"))))))

(deftest chat-route-allows-direct-local-client-with-session-secret
  (with-redefs [xia.agent/process-message (fn [_session-id _user-message & _] "ok")]
    (let [response (#'http/router {:uri            "/chat"
                                   :request-method :post
                                   :headers        {"cookie" (local-session-cookie)}
                                   :body           (request-body {"message" "hello"})})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= "ok" (get body "content"))))))

(deftest chat-route-offloads-async-http-requests
  (let [release-turn (promise)
        seen-turn    (promise)
        sent-response (promise)
        req          {:uri            "/chat"
                      :request-method :post
                      :headers        (ui-headers)
                      :body           (request-body {"message" "hello"})
                      :async-channel  ::chat-channel}]
    (with-redefs [org.httpkit.server/as-channel (fn [ring-req handlers]
                                                  (is (= ::chat-channel (:async-channel ring-req)))
                                                  ((:on-open handlers) ::chat-channel)
                                                  ::async-response)
                  org.httpkit.server/send! (fn [ch response]
                                             (deliver sent-response [ch response])
                                             true)
                  xia.agent/process-message (fn [session-id user-message & _]
                                              (deliver seen-turn [session-id user-message])
                                              (deref release-turn 2000 ::timeout)
                                              (str "echo: " user-message))]
      (let [route-result (deref (future (#'http/router req)) 200 ::timeout)
            [sid user-message] (deref seen-turn 200 ::timeout)]
        (is (= ::async-response route-result))
        (is (instance? UUID sid))
        (is (= "hello" user-message))
        (is (= ::pending (deref sent-response 100 ::pending)))
        (deliver release-turn true)
        (let [[ch response] (deref sent-response 2000 ::timeout)
              body          (response-json response)]
          (is (= ::chat-channel ch))
          (is (= 200 (:status response)))
          (is (= (str sid) (get body "session_id")))
          (is (= "assistant" (get body "role")))
          (is (= "echo: hello" (get body "content"))))))))

(deftest chat-route-sends-async-json-errors
  (let [sent-response (promise)
        req           {:uri            "/chat"
                       :request-method :post
                       :headers        (ui-headers)
                       :body           (request-body {"message" "hello"})
                       :async-channel  ::chat-channel}]
    (with-redefs [org.httpkit.server/as-channel (fn [_req handlers]
                                                  ((:on-open handlers) ::chat-channel)
                                                  ::async-response)
                  org.httpkit.server/send! (fn [_ch response]
                                             (deliver sent-response response)
                                             true)
                  xia.agent/process-message (fn [_session-id _user-message & _]
                                              (throw (ex-info "rate limited"
                                                              {:status 429
                                                               :error  "rate limited"})))]
      (is (= ::async-response
             (deref (future (#'http/router req)) 200 ::timeout)))
      (let [response (deref sent-response 2000 ::timeout)
            body     (response-json response)]
        (is (= 429 (:status response)))
        (is (= "rate limited" (get body "error")))))))

(deftest websocket-route-blocks-non-local-origins
  (let [as-channel-called? (atom false)]
    (with-redefs [org.httpkit.server/websocket-handshake-check (constantly true)
                  org.httpkit.server/as-channel (fn [_req _handlers]
                                                  (reset! as-channel-called? true)
                                                  ::websocket-upgraded)]
      (let [response (#'http/router {:uri            "/ws"
                                     :request-method :get
                                     :headers        {"origin" "https://evil.example"}})
            body     (response-json response)]
        (is (= 403 (:status response)))
        (is (= "forbidden origin" (get body "error")))
        (is (false? @as-channel-called?))))))

(deftest websocket-route-blocks-missing-session-secret
  (let [as-channel-called? (atom false)]
    (with-redefs [org.httpkit.server/websocket-handshake-check (constantly true)
                  org.httpkit.server/as-channel (fn [_req _handlers]
                                                  (reset! as-channel-called? true)
                                                  ::websocket-upgraded)]
      (let [response (#'http/router {:uri            "/ws"
                                     :request-method :get
                                     :headers        {"origin" "http://localhost:3008"}})
            body     (response-json response)]
        (is (= 401 (:status response)))
        (is (= "missing or invalid local session secret" (get body "error")))
        (is (false? @as-channel-called?))))))

(deftest websocket-route-allows-local-origin-with-session-secret
  (with-redefs [org.httpkit.server/websocket-handshake-check (constantly true)
                org.httpkit.server/as-channel (fn [_req _handlers]
                                                ::websocket-upgraded)]
    (is (= ::websocket-upgraded
           (#'http/router {:uri            "/ws"
                           :request-method :get
                           :headers        (ui-headers)})))))

(deftest websocket-disconnect-records-conversation-before-clearing-wm
  (let [handlers  (atom nil)
        sessions  (atom {})
        sid       (random-uuid)
        ch        (Object.)
        lifecycle (atom [])]
    (with-redefs [xia.channel.http/protected-route-response (fn [_req handler]
                                                              (handler))
                  xia.channel.http/ws-sessions sessions
                  org.httpkit.server/as-channel (fn [_req ws-handlers]
                                                  (reset! handlers ws-handlers)
                                                  ::websocket-upgraded)
                  org.httpkit.server/send! (fn [_ch _msg] nil)
                  xia.db/create-session! (fn [channel]
                                           (is (= :websocket channel))
                                           sid)
                  xia.working-memory/ensure-wm! (fn [session-id]
                                                  (swap! lifecycle conj [:ensure session-id]))
                  xia.working-memory/get-wm (fn [session-id]
                                              (is (= sid session-id))
                                              {:topics "release planning"})
                  xia.working-memory/snapshot! (fn [session-id]
                                                (swap! lifecycle conj [:snapshot session-id]))
                  xia.hippocampus/record-conversation! (fn [session-id channel & {:keys [topics]}]
                                                         (swap! lifecycle conj [:record session-id channel topics]))
                  xia.working-memory/clear-wm! (fn [session-id]
                                                 (swap! lifecycle conj [:clear session-id]))]
      (is (= ::websocket-upgraded (#'http/ws-handler {:headers (ui-headers)})))
      ((:on-open @handlers) ch)
      (is (= sid (get @sessions ch)))
      ((:on-close @handlers) ch 1000)
      (is (= [[:ensure sid]
              [:snapshot sid]
              [:record sid :websocket "release planning"]
              [:clear sid]]
             @lifecycle))
      (is (nil? (get @sessions ch))))))

(deftest close-session-route-finalizes-rest-session
  (let [sid       (db/create-session! :http)
        lifecycle (atom [])]
    (with-redefs [xia.channel.http/cancel-rest-session-finalizer! (fn [session-id]
                                                                    (swap! lifecycle conj [:cancel session-id]))
                  xia.working-memory/get-wm (fn [session-id]
                                              (is (= sid session-id))
                                              {:topics "release planning"})
                  xia.working-memory/snapshot! (fn [session-id]
                                                (swap! lifecycle conj [:snapshot session-id]))
                  xia.hippocampus/record-conversation! (fn [session-id channel & {:keys [topics]}]
                                                         (swap! lifecycle conj [:record session-id channel topics]))
                  xia.working-memory/clear-wm! (fn [session-id]
                                                 (swap! lifecycle conj [:clear session-id]))]
      (let [response (#'http/router {:uri            (str "/sessions/" sid)
                                     :request-method :delete
                                     :headers        (ui-headers)})
            body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= "closed" (get body "status")))
        (is (= false (get body "already_closed")))
        (is (= [[:snapshot sid]
                [:record sid :http "release planning"]
                [:clear sid]
                [:cancel (str sid)]]
               @lifecycle))
        (is (= false
               (ffirst (db/q '[:find ?active :in $ ?sid
                               :where
                               [?s :session/id ?sid]
                               [(get-else $ ?s :session/active? false) ?active]]
                             sid))))))))

(deftest chat-route-validates-required-message
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"session_id" (str (random-uuid))})})
        body     (response-json response)]
    (is (= 400 (:status response)))
    (is (= "missing 'message' field" (get body "error")))))

(deftest chat-route-rejects-oversized-request-body
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (oversized-request-body)})
        body     (response-json response)]
    (is (= 413 (:status response)))
    (is (= "request body too large" (get body "error")))))

(deftest session-messages-route-returns-transcript
  (let [sid (db/create-session! :http)
        doc (local-doc/save-upload! {:session-id sid
                                     :name "notes.md"
                                     :media-type "text/markdown"
                                     :text "# notes"})
        report (artifact/create-artifact! {:session-id sid
                                           :name "report.json"
                                           :title "Summary Report"
                                           :kind :json
                                           :data {"topic" "notes"}})]
    (db/add-message! sid :user "hello"
                     :local-doc-ids [(:id doc)]
                     :artifact-ids [(:id report)])
    (db/add-message! sid :assistant "hi there"
                     :local-doc-ids [(:id doc)]
                     :artifact-ids [(:id report)])
    (db/add-message! sid :tool "internal")
    (let [response (#'http/router {:uri            (str "/sessions/" sid "/messages")
                                   :request-method :get
                                   :headers        (ui-headers)})
          body     (response-json response)
          messages (get body "messages")]
      (is (= 200 (:status response)))
      (is (= 2 (count messages)))
      (is (= "user" (get (first messages) "role")))
      (is (= "hello" (get (first messages) "content")))
      (is (= "notes.md" (get-in (first messages) ["local_docs" 0 "name"])))
      (is (= "Summary Report" (get-in (first messages) ["artifacts" 0 "title"])))
      (is (= "assistant" (get (second messages) "role")))
      (is (= "hi there" (get (second messages) "content")))
      (is (= (str (:id doc)) (get-in (second messages) ["local_docs" 0 "id"])))
      (is (= (str (:id report)) (get-in (second messages) ["artifacts" 0 "id"]))))))

(deftest session-messages-route-blocks-non-local-origins
  (let [response (#'http/router {:uri            (str "/sessions/" (random-uuid) "/messages")
                                 :request-method :get
                                 :headers        {"origin" "https://evil.example"}})
        body     (response-json response)]
    (is (= 403 (:status response)))
    (is (= "forbidden origin" (get body "error")))))

(deftest session-messages-route-blocks-missing-session-secret
  (let [response (#'http/router {:uri            (str "/sessions/" (random-uuid) "/messages")
                                 :request-method :get
                                 :headers        {"origin" "http://localhost:3008"}})
        body     (response-json response)]
    (is (= 401 (:status response)))
    (is (= "missing or invalid local session secret" (get body "error")))))

(deftest history-sessions-route-returns-chat-session-summaries
  (let [visible-sid (db/create-session! :http)
        hidden-sid  (db/create-session! :api)]
    (db/add-message! visible-sid :user "hello")
    (db/add-message! visible-sid :assistant "latest reply")
    (db/add-message! hidden-sid :user "should stay hidden")
    (let [response (#'http/router {:uri            "/history/sessions"
                                   :request-method :get
                                   :headers        (ui-headers)})
          body     (response-json response)
          sessions (get body "sessions")
          session  (first sessions)]
      (is (= 200 (:status response)))
      (is (= 1 (count sessions)))
      (is (= (str visible-sid) (get session "id")))
      (is (= "http" (get session "channel")))
      (is (= true (get session "active")))
      (is (= 2 (get session "message_count")))
      (is (= "latest reply" (get session "preview")))
      (is (string? (get session "created_at")))
      (is (string? (get session "last_message_at"))))))

(deftest history-schedules-route-returns-schedule-summaries
  (schedule/create-schedule!
    {:id :daily-brief
     :name "Daily Brief"
     :spec {:interval-minutes 5}
     :type :prompt
     :prompt "Summarize the morning inbox."})
  (schedule/record-run! :daily-brief
    {:started-at  (java.util.Date. 1000)
     :finished-at (java.util.Date. 2000)
     :status      :error
     :error       "temporary upstream failure"})
  (let [response (#'http/router {:uri            "/history/schedules"
                                 :request-method :get
                                 :headers        (ui-headers)})
        body     (response-json response)
        schedules (get body "schedules")
        sched    (first schedules)]
    (is (= 200 (:status response)))
    (is (= 1 (count schedules)))
    (is (= "daily-brief" (get sched "id")))
    (is (= "Daily Brief" (get sched "name")))
    (is (= "prompt" (get sched "type")))
    (is (= true (get sched "trusted")))
    (is (= true (get sched "enabled")))
    (is (= "error" (get sched "latest_status")))
    (is (= "temporary upstream failure" (get sched "latest_error")))
    (is (string? (get sched "last_run")))
    (is (string? (get sched "next_run")))))

(deftest history-schedule-runs-route-returns-audit-and-result
  (schedule/create-schedule!
    {:id :weekly-review
     :name "Weekly Review"
     :spec {:interval-minutes 5}
     :type :tool
     :tool-id :schedule-list})
  (schedule/record-run! :weekly-review
    {:started-at  (java.util.Date. 1000)
     :finished-at (java.util.Date. 2000)
     :status      :success
     :actions     [{:tool-id "schedule-list" :status "success"}]
     :result      "done"})
  (schedule/record-run! :weekly-review
    {:started-at  (java.util.Date. 3000)
     :finished-at (java.util.Date. 4000)
     :status      :error
     :actions     [{:tool-id "schedule-list" :status "error"}]
     :error       "boom"})
  (let [response (#'http/router {:uri            "/history/schedules/weekly-review/runs"
                                 :request-method :get
                                 :headers        (ui-headers)})
        body     (response-json response)
        sched    (get body "schedule")
        runs     (get body "runs")
        latest   (first runs)
        older    (second runs)]
    (is (= 200 (:status response)))
    (is (= "weekly-review" (get sched "id")))
    (is (= "error" (get sched "latest_status")))
    (is (= 2 (count runs)))
    (is (= "error" (get latest "status")))
    (is (= "boom" (get latest "error")))
    (is (= [{"tool-id" "schedule-list" "status" "error"}]
           (get latest "actions")))
    (is (= "success" (get older "status")))
    (is (= "done" (get older "result")))
    (is (= [{"tool-id" "schedule-list" "status" "success"}]
           (get older "actions")))))

(deftest session-status-route-returns-live-status
  (let [sid (str (db/create-session! :http))]
    (#'http/http-status-handler {:session-id sid
                                 :state      :running
                                 :phase      :llm
                                 :message    "Calling model"})
    (let [response (#'http/router {:uri            (str "/sessions/" sid "/status")
                                   :request-method :get
                                   :headers        (ui-headers)})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= sid (get body "session_id")))
      (is (= "running" (get-in body ["status" "state"])))
      (is (= "llm" (get-in body ["status" "phase"])))
      (is (= "Calling model" (get-in body ["status" "message"])))
      (is (string? (get-in body ["status" "updated_at"]))))
    (#'http/http-status-handler {:session-id sid
                                 :state      :done
                                 :message    "Ready"})
    (let [response (#'http/router {:uri            (str "/sessions/" sid "/status")
                                   :request-method :get
                                   :headers        (ui-headers)})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (nil? (get body "status"))))))

(deftest session-status-route-clears-terminal-error-status
  (let [sid (str (db/create-session! :http))]
    (#'http/http-status-handler {:session-id sid
                                 :state      :running
                                 :phase      :tool
                                 :message    "Calling upstream"})
    (#'http/http-status-handler {:session-id sid
                                 :state      :error
                                 :phase      :error
                                 :message    "Request failed: boom"})
    (let [response (#'http/router {:uri            (str "/sessions/" sid "/status")
                                   :request-method :get
                                   :headers        (ui-headers)})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (nil? (get body "status"))))))

(deftest scratch-pad-routes-round-trip
  (let [sid         (str (db/create-session! :http))
        create-res  (#'http/router {:uri            (str "/sessions/" sid "/scratch-pads")
                                    :request-method :post
                                    :headers        (ui-headers)
                                    :body           (request-body {"title" "Draft"
                                                                   "content" "alpha"})})
        create-body (response-json create-res)
        pad-id      (get-in create-body ["pad" "id"])]
    (is (= 201 (:status create-res)))
    (is (= "Draft" (get-in create-body ["pad" "title"])))
    (is (= "alpha" (get-in create-body ["pad" "content"])))
    (let [list-res  (#'http/router {:uri            (str "/sessions/" sid "/scratch-pads")
                                    :request-method :get
                                    :headers        (ui-headers)})
          list-body (response-json list-res)]
      (is (= 200 (:status list-res)))
      (is (= 1 (count (get list-body "pads"))))
      (is (= pad-id (get-in list-body ["pads" 0 "id"]))))
    (let [get-res  (#'http/router {:uri            (str "/sessions/" sid "/scratch-pads/" pad-id)
                                   :request-method :get
                                   :headers        (ui-headers)})
          get-body (response-json get-res)]
      (is (= 200 (:status get-res)))
      (is (= "alpha" (get-in get-body ["pad" "content"]))))
    (let [save-res  (#'http/router {:uri            (str "/sessions/" sid "/scratch-pads/" pad-id)
                                    :request-method :put
                                    :headers        (ui-headers)
                                    :body           (request-body {"title" "Draft 2"
                                                                   "content" "beta"
                                                                   "expected_version" 1})})
          save-body (response-json save-res)]
      (is (= 200 (:status save-res)))
      (is (= "Draft 2" (get-in save-body ["pad" "title"])))
      (is (= "beta" (get-in save-body ["pad" "content"])))
      (is (= 2 (get-in save-body ["pad" "version"]))))
    (let [edit-res  (#'http/router {:uri            (str "/sessions/" sid "/scratch-pads/" pad-id "/edit")
                                    :request-method :post
                                    :headers        (ui-headers)
                                    :body           (request-body {"operation" {"op" "append"
                                                                                 "separator" "\n"
                                                                                 "text" "gamma"}
                                                                   "expected_version" 2})})
          edit-body (response-json edit-res)]
      (is (= 200 (:status edit-res)))
      (is (= "beta\ngamma" (get-in edit-body ["pad" "content"]))))
    (let [delete-res  (#'http/router {:uri            (str "/sessions/" sid "/scratch-pads/" pad-id)
                                      :request-method :delete
                                      :headers        (ui-headers)})
          delete-body (response-json delete-res)]
      (is (= 200 (:status delete-res)))
      (is (= "deleted" (get delete-body "status"))))
    (let [list-res  (#'http/router {:uri            (str "/sessions/" sid "/scratch-pads")
                                    :request-method :get
                                    :headers        (ui-headers)})
          list-body (response-json list-res)]
      (is (= [] (get list-body "pads"))))))

(deftest local-document-routes-round-trip
  (let [sid         (str (db/create-session! :http))
        create-res  (#'http/router {:uri            (str "/sessions/" sid "/local-documents")
                                    :request-method :post
                                    :headers        (ui-headers)
                                    :body           (request-body {"documents" [{"name" "notes.md"
                                                                                 "media_type" "text/markdown"
                                                                                 "size_bytes" 22
                                                                                 "text" "# Local\n\ncontent"}]})})
        create-body (response-json create-res)
        doc-id      (get-in create-body ["documents" 0 "id"])]
    (is (= 201 (:status create-res)))
    (is (= "notes.md" (get-in create-body ["documents" 0 "name"])))
    (is (= "text/markdown" (get-in create-body ["documents" 0 "media_type"])))
    (is (string? (get-in create-body ["documents" 0 "preview"])))
    (let [list-res  (#'http/router {:uri            (str "/sessions/" sid "/local-documents")
                                    :request-method :get
                                    :headers        (ui-headers)})
          list-body (response-json list-res)]
      (is (= 200 (:status list-res)))
      (is (= 1 (count (get list-body "documents"))))
      (is (= doc-id (get-in list-body ["documents" 0 "id"]))))
    (let [get-res  (#'http/router {:uri            (str "/sessions/" sid "/local-documents/" doc-id)
                                   :request-method :get
                                   :headers        (ui-headers)})
          get-body (response-json get-res)]
      (is (= 200 (:status get-res)))
      (is (= "# Local\n\ncontent" (get-in get-body ["document" "text"]))))
    (let [note-res  (#'http/router {:uri            (str "/sessions/" sid "/local-documents/" doc-id "/scratch-pads")
                                    :request-method :post
                                    :headers        (ui-headers)})
          note-body (response-json note-res)]
      (is (= 201 (:status note-res)))
      (is (= "notes.md" (get-in note-body ["pad" "title"])))
      (is (= "# Local\n\ncontent" (get-in note-body ["pad" "content"]))))
    (let [delete-res  (#'http/router {:uri            (str "/sessions/" sid "/local-documents/" doc-id)
                                      :request-method :delete
                                      :headers        (ui-headers)})
          delete-body (response-json delete-res)]
      (is (= 200 (:status delete-res)))
      (is (= "deleted" (get delete-body "status"))))
    (let [list-res  (#'http/router {:uri            (str "/sessions/" sid "/local-documents")
                                    :request-method :get
                                    :headers        (ui-headers)})
          list-body (response-json list-res)]
      (is (= [] (get list-body "documents"))))))

(deftest local-document-create-route-reports-partial-upload-errors
  (let [sid        (str (db/create-session! :http))
        response   (#'http/router {:uri            (str "/sessions/" sid "/local-documents")
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"documents" [{"name" "notes.txt"
                                                                                "media_type" "text/plain"
                                                                                "text" "ok"}
                                                                               {"name" "paper.pdf"
                                                                                "media_type" "application/pdf"
                                                                                "text" "ignored"}]})})
        body       (response-json response)]
    (is (= 201 (:status response)))
    (is (= 2 (count (get body "documents"))))
    (is (= 1 (count (get body "errors"))))
    (is (= "failed" (get-in body ["documents" 1 "status"])))
    (is (= "paper.pdf" (get-in body ["errors" 0 "name"])))))

(deftest local-document-multipart-route-round-trip
  (let [sid         (str (db/create-session! :http))
        create-res  (#'http/router
                      (multipart-request
                        (str "/sessions/" sid "/local-documents")
                        [{:field-name   "documents"
                          :filename     "notes.md"
                          :content-type "text/markdown"
                          :body-bytes   (.getBytes "# Local\n\ncontent"
                                                  StandardCharsets/UTF_8)}]))
        create-body (response-json create-res)
        doc-id      (get-in create-body ["documents" 0 "id"])]
    (is (= 201 (:status create-res)))
    (is (= "notes.md" (get-in create-body ["documents" 0 "name"])))
    (is (= "text/markdown" (get-in create-body ["documents" 0 "media_type"])))
    (let [get-res  (#'http/router {:uri            (str "/sessions/" sid "/local-documents/" doc-id)
                                   :request-method :get
                                   :headers        (ui-headers)})
          get-body (response-json get-res)]
      (is (= 200 (:status get-res)))
      (is (= "# Local\n\ncontent" (get-in get-body ["document" "text"]))))))

(deftest local-document-multipart-route-extracts-pdf
  (let [sid         (str (db/create-session! :http))
        create-res  (#'http/router
                      (multipart-request
                        (str "/sessions/" sid "/local-documents")
                        [{:field-name   "documents"
                          :filename     "paper.pdf"
                          :content-type "application/pdf"
                          :body-bytes   (sample-pdf-bytes "Hello multipart PDF")}]))
        create-body (response-json create-res)
        doc-id      (get-in create-body ["documents" 0 "id"])
        get-res     (#'http/router {:uri            (str "/sessions/" sid "/local-documents/" doc-id)
                                    :request-method :get
                                    :headers        (ui-headers)})
        get-body    (response-json get-res)]
    (is (= 201 (:status create-res)))
    (is (= "application/pdf" (get-in create-body ["documents" 0 "media_type"])))
    (is (= 200 (:status get-res)))
    (is (.contains ^String (get-in get-body ["document" "text"]) "Hello multipart PDF"))))

(deftest local-document-multipart-route-reports-partial-upload-errors
  (let [sid      (str (db/create-session! :http))
        response (#'http/router
                   (multipart-request
                     (str "/sessions/" sid "/local-documents")
                     [{:field-name   "documents"
                       :filename     "notes.txt"
                       :content-type "text/plain"
                       :body-bytes   (.getBytes "ok" StandardCharsets/UTF_8)}
                      {:field-name   "documents"
                       :filename     "bad.bin"
                       :content-type "application/octet-stream"
                       :body-bytes   (.getBytes "raw" StandardCharsets/UTF_8)}]))
        body     (response-json response)]
    (is (= 201 (:status response)))
    (is (= 2 (count (get body "documents"))))
    (is (= 1 (count (get body "errors"))))
    (is (= "failed" (get-in body ["documents" 1 "status"])))
    (is (= "bad.bin" (get-in body ["errors" 0 "name"])))))

(deftest artifact-routes-round-trip
  (let [sid           (str (db/create-session! :http))
        create-res    (#'http/router {:uri            (str "/sessions/" sid "/artifacts")
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"name" "report.json"
                                                                     "kind" "json"
                                                                     "data" {"topic" "cars"
                                                                             "count" 2}})})
        create-body   (response-json create-res)
        artifact-id   (get-in create-body ["artifact" "id"])
        list-res      (#'http/router {:uri            (str "/sessions/" sid "/artifacts")
                                      :request-method :get
                                      :headers        (ui-headers)})
        list-body     (response-json list-res)
        get-res       (#'http/router {:uri            (str "/sessions/" sid "/artifacts/" artifact-id)
                                      :request-method :get
                                      :headers        (ui-headers)})
        get-body      (response-json get-res)
        note-res      (#'http/router {:uri            (str "/sessions/" sid "/artifacts/" artifact-id "/scratch-pads")
                                      :request-method :post
                                      :headers        (ui-headers)})
        note-body     (response-json note-res)
        download-res  (#'http/router {:uri            (str "/sessions/" sid "/artifacts/" artifact-id "/download")
                                      :request-method :get
                                      :headers        (ui-headers)})
        delete-res    (#'http/router {:uri            (str "/sessions/" sid "/artifacts/" artifact-id)
                                      :request-method :delete
                                      :headers        (ui-headers)})
        delete-body   (response-json delete-res)
        empty-list    (#'http/router {:uri            (str "/sessions/" sid "/artifacts")
                                      :request-method :get
                                      :headers        (ui-headers)})
        empty-body    (response-json empty-list)]
    (is (= 201 (:status create-res)))
    (is (= "report.json" (get-in create-body ["artifact" "name"])))
    (is (= "json" (get-in create-body ["artifact" "kind"])))
    (is (= "application/json" (get-in create-body ["artifact" "media_type"])))
    (is (= 200 (:status list-res)))
    (is (= 1 (count (get list-body "artifacts"))))
    (is (= artifact-id (get-in list-body ["artifacts" 0 "id"])))
    (is (= 200 (:status get-res)))
    (is (.contains ^String (get-in get-body ["artifact" "text"]) "\"topic\": \"cars\""))
    (is (= 201 (:status note-res)))
    (is (= "report" (get-in note-body ["pad" "title"])))
    (is (.contains ^String (get-in note-body ["pad" "content"]) "\"topic\": \"cars\""))
    (is (= 200 (:status download-res)))
    (is (= "application/json; charset=utf-8" (get-in download-res [:headers "Content-Type"])))
    (is (re-find #"attachment; filename=\"report\.json\"" (get-in download-res [:headers "Content-Disposition"])))
    (is (.contains ^String (String. ^bytes (:body download-res) StandardCharsets/UTF_8) "\"count\": 2"))
    (is (= 200 (:status delete-res)))
    (is (= "deleted" (get delete-body "status")))
    (is (= [] (get empty-body "artifacts")))))

(deftest binary-artifact-routes-round-trip
  (let [sid          (str (db/create-session! :http))
        payload      (.getBytes "%PDF-1.7\nbinary-http\n" StandardCharsets/UTF_8)
        encoded      (.encodeToString (Base64/getEncoder) payload)
        create-res   (#'http/router {:uri            (str "/sessions/" sid "/artifacts")
                                     :request-method :post
                                     :headers        (ui-headers)
                                     :body           (request-body {"name" "report.pdf"
                                                                    "kind" "pdf"
                                                                    "bytes_base64" encoded})})
        create-body  (response-json create-res)
        artifact-id  (get-in create-body ["artifact" "id"])
        get-res      (#'http/router {:uri            (str "/sessions/" sid "/artifacts/" artifact-id)
                                     :request-method :get
                                     :headers        (ui-headers)})
        get-body     (response-json get-res)
        download-res (#'http/router {:uri            (str "/sessions/" sid "/artifacts/" artifact-id "/download")
                                     :request-method :get
                                     :headers        (ui-headers)})]
    (is (= 201 (:status create-res)))
    (is (= "report.pdf" (get-in create-body ["artifact" "name"])))
    (is (= "pdf" (get-in create-body ["artifact" "kind"])))
    (is (= true (get-in create-body ["artifact" "has_blob"])))
    (is (= false (get-in create-body ["artifact" "text_available"])))
    (is (number? (get-in create-body ["artifact" "compressed_size_bytes"])))
    (is (= 200 (:status get-res)))
    (is (nil? (get-in get-body ["artifact" "text"])))
    (is (= true (get-in get-body ["artifact" "has_blob"])))
    (is (= false (get-in get-body ["artifact" "text_available"])))
    (is (= 200 (:status download-res)))
    (is (= "application/pdf" (get-in download-res [:headers "Content-Type"])))
    (is (Arrays/equals ^bytes payload ^bytes (:body download-res)))))

(deftest chat-route-forwards-explicit-local-document-and-artifact-refs
  (let [sid      (db/create-session! :http)
        doc      (local-doc/save-upload! {:session-id sid
                                          :name "paper.md"
                                          :media-type "text/markdown"
                                          :text "content"})
        report   (artifact/create-artifact! {:session-id sid
                                             :name "report.json"
                                             :title "Research Report"
                                             :kind :json
                                             :data {"topic" "cars"}})
        seen-opts (atom nil)]
    (with-redefs [xia.agent/process-message (fn [_session-id _user-message & {:as opts}]
                                              (reset! seen-opts opts)
                                              "ok")]
      (let [response (#'http/router {:uri            "/chat"
                                     :request-method :post
                                     :headers        (ui-headers)
                                     :body           (request-body {"message" "summarize this"
                                                                    "session_id" (str sid)
                                                                    "local_doc_ids" [(str (:id doc))]
                                                                    "artifact_ids" [(str (:id report))]})})
            body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= "ok" (get body "content")))
        (is (= :http (:channel @seen-opts)))
        (is (= [(str (:id doc))] (mapv str (:local-doc-ids @seen-opts))))
        (is (= [(str (:id report))] (mapv str (:artifact-ids @seen-opts))))))))

(deftest approval-route-allows-round-trip
  (let [sid    (str (db/create-session! :http))
        waiter (future
                 (#'http/http-approval-handler
                   {:session-id   sid
                    :tool-id      :browser-login
                    :tool-name    "browser-login"
                    :description  "Log into a site"
                    :arguments    {"site" "jira"}
                    :reason       "uses stored site credentials"
                    :policy       :session}))]
    (let [pending-body (wait-for
                         #(let [response (#'http/router {:uri            (str "/sessions/" sid "/approval")
                                                         :request-method :get
                                                         :headers        (ui-headers)})
                                body     (response-json response)]
                            (when (get body "pending")
                              body)))]
      (is (some? pending-body))
      (is (= "browser-login" (get-in pending-body ["approval" "tool_name"])))
      (let [approval-id (get-in pending-body ["approval" "approval_id"])
            submit      (#'http/router {:uri            (str "/sessions/" sid "/approval")
                                        :request-method :post
                                        :headers        (ui-headers)
                                        :body           (request-body {"approval_id" approval-id
                                                                       "decision" "allow"})})
            submit-body (response-json submit)]
        (is (= 200 (:status submit)))
        (is (= "recorded" (get submit-body "status")))))
    (is (= true (deref waiter 2000 ::timeout)))))

(deftest admin-config-route-returns-safe-summaries
  (db/upsert-provider! {:id       :openai
                        :name     "OpenAI"
                        :base-url "https://api.openai.com/v1"
                        :api-key  "sk-secret"
                        :model    "gpt-5"
                        :workloads #{:assistant :history-compaction}
                        :system-prompt-budget 16000
                        :history-budget 32000})
  (db/set-default-provider! :openai)
  (db/register-service! {:id          :github
                         :name        "GitHub"
                         :base-url    "https://api.github.com"
                         :auth-type   :api-key-header
                         :auth-key    "gh-secret"
                         :auth-header "X-API-Key"
                         :autonomous-approved? true
                         :rate-limit-per-minute 90})
  (db/register-oauth-account! {:id            :google
                               :name          "Google"
                               :authorize-url "https://accounts.google.com/o/oauth2/v2/auth"
                               :token-url     "https://oauth2.googleapis.com/token"
                               :client-id     "client-id"
                               :client-secret "client-secret"
                               :autonomous-approved? true
                               :access-token  "access-123"
                               :refresh-token "refresh-123"})
  (db/register-site-cred! {:id             :github
                           :name           "GitHub login"
                           :login-url      "https://github.com/login"
                           :username-field "login"
                           :password-field "password"
                           :username       "hyang"
                           :password       "pw-secret"
                           :autonomous-approved? true
                           :form-selector  "#login"
                           :extra-fields   "{\"remember_me\":\"1\"}"})
  (db/install-tool! {:id          :demo-tool
                     :name        "Demo tool"
                     :description "desc"
                     :parameters  {}
                     :handler     "(fn [_] {:ok true})"
                     :approval    :session})
  (db/install-skill! {:id          :demo-skill
                      :name        "Demo skill"
                      :description "skill desc"
                      :content     "content"})
  (let [response (#'http/router {:uri            "/admin/config"
                                 :request-method :get
                                 :headers        (ui-headers)})
        body     (response-json response)
        provider (first (filter #(= "openai" (get % "id")) (get body "providers")))
        memory-retention (get body "memory_retention")
        knowledge-decay (get body "knowledge_decay")
        llm-workloads (get body "llm_workloads")
        templates (get body "oauth_provider_templates")
        oauth    (first (filter #(= "google" (get % "id")) (get body "oauth_accounts")))
        service  (first (filter #(= "github" (get % "id")) (get body "services")))
        site     (first (filter #(= "github" (get % "id")) (get body "sites")))
        tool     (first (filter #(= "demo-tool" (get % "id")) (get body "tools")))
        skill    (first (filter #(= "demo-skill" (get % "id")) (get body "skills")))]
    (is (= 200 (:status response)))
    (is (= true (get provider "api_key_configured")))
    (is (not (contains? provider "api_key")))
    (is (= true (get provider "default")))
    (is (= ["assistant" "history-compaction"] (get provider "workloads")))
    (is (= 16000 (get provider "system_prompt_budget")))
    (is (= 32000 (get provider "history_budget")))
    (is (= 182 (get memory-retention "full_resolution_days")))
    (is (= 365 (get memory-retention "decay_half_life_days")))
    (is (= 8 (get memory-retention "retained_count")))
    (is (= 182 (get knowledge-decay "grace_period_days")))
    (is (= 730 (get knowledge-decay "half_life_days")))
    (is (= 0.1 (get knowledge-decay "min_confidence")))
    (is (= 1 (get knowledge-decay "maintenance_interval_days")))
    (is (= 365 (get knowledge-decay "archive_after_bottom_days")))
    (is (= "healthy" (get provider "health_status")))
    (is (= #{"assistant" "history-compaction" "topic-summary" "memory-summary" "memory-importance" "memory-extraction" "fact-utility"}
           (set (map #(get % "id") llm-workloads))))
    (is (= #{"github" "google" "microsoft"}
           (set (map #(get % "id") templates))))
    (is (= "{\"access_type\":\"offline\",\"prompt\":\"consent\"}"
           (get (first (filter #(= "google" (get % "id")) templates)) "auth_params")))
    (is (= "GitHub API"
           (get (first (filter #(= "github" (get % "id")) templates)) "service_name")))
    (is (= true (get oauth "client_secret_configured")))
    (is (= true (get oauth "access_token_configured")))
    (is (= true (get oauth "autonomous_approved")))
    (is (not (contains? oauth "client_secret")))
    (is (not (contains? oauth "access_token")))
    (is (= true (get service "auth_key_configured")))
    (is (not (contains? service "auth_key")))
    (is (= "api-key-header" (get service "auth_type")))
    (is (= true (get service "autonomous_approved")))
    (is (= false (get service "oauth_account_autonomous_approved")))
    (is (= 90 (get service "rate_limit_per_minute")))
    (is (= 90 (get service "effective_rate_limit_per_minute")))
    (is (= true (get site "autonomous_approved")))
    (is (= true (get site "username_configured")))
    (is (= true (get site "password_configured")))
    (is (not (contains? site "username")))
    (is (not (contains? site "password")))
    (is (= "session" (get tool "approval")))
    (is (= true (get tool "enabled")))
    (is (= true (get skill "enabled")))))

(deftest admin-memory-retention-route-saves-and-clears-settings
  (let [save-response (#'http/router {:uri            "/admin/memory-retention"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"full_resolution_days" "730"
                                                                     "decay_half_life_days" "180"
                                                                     "retained_count" "12"})})
        save-body     (response-json save-response)]
    (is (= 200 (:status save-response)))
    (is (= 730 (get-in save-body ["memory_retention" "full_resolution_days"])))
    (is (= 180 (get-in save-body ["memory_retention" "decay_half_life_days"])))
    (is (= 12 (get-in save-body ["memory_retention" "retained_count"])))
    (is (= (str (* 730 24 60 60 1000))
           (db/get-config :memory/episode-full-resolution-ms)))
    (is (= (str (* 180 24 60 60 1000))
           (db/get-config :memory/episode-decay-half-life-ms)))
    (is (= "12" (db/get-config :memory/episode-retained-decayed-count))))
  (let [clear-response (#'http/router {:uri            "/admin/memory-retention"
                                       :request-method :post
                                       :headers        (ui-headers)
                                       :body           (request-body {"full_resolution_days" ""
                                                                      "decay_half_life_days" ""
                                                                      "retained_count" ""})})
        clear-body     (response-json clear-response)]
    (is (= 200 (:status clear-response)))
    (is (= 182 (get-in clear-body ["memory_retention" "full_resolution_days"])))
    (is (= 365 (get-in clear-body ["memory_retention" "decay_half_life_days"])))
    (is (= 8 (get-in clear-body ["memory_retention" "retained_count"])))
    (is (nil? (db/get-config :memory/episode-full-resolution-ms)))
    (is (nil? (db/get-config :memory/episode-decay-half-life-ms)))
    (is (nil? (db/get-config :memory/episode-retained-decayed-count)))))

(deftest admin-knowledge-decay-route-saves-and-clears-settings
  (let [save-response (#'http/router {:uri            "/admin/knowledge-decay"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"grace_period_days" "14"
                                                                     "half_life_days" "120"
                                                                     "min_confidence" "0.25"
                                                                     "maintenance_interval_days" "3"
                                                                     "archive_after_bottom_days" "45"})})
        save-body     (response-json save-response)]
    (is (= 200 (:status save-response)))
    (is (= 14 (get-in save-body ["knowledge_decay" "grace_period_days"])))
    (is (= 120 (get-in save-body ["knowledge_decay" "half_life_days"])))
    (is (= 0.25 (get-in save-body ["knowledge_decay" "min_confidence"])))
    (is (= 3 (get-in save-body ["knowledge_decay" "maintenance_interval_days"])))
    (is (= 45 (get-in save-body ["knowledge_decay" "archive_after_bottom_days"])))
    (is (= (str (* 14 24 60 60 1000))
           (db/get-config :memory/knowledge-decay-grace-period-ms)))
    (is (= (str (* 120 24 60 60 1000))
           (db/get-config :memory/knowledge-decay-half-life-ms)))
    (is (= "0.25" (db/get-config :memory/knowledge-decay-min-confidence)))
    (is (= (str (* 3 24 60 60 1000))
           (db/get-config :memory/knowledge-decay-maintenance-step-ms)))
    (is (= (str (* 45 24 60 60 1000))
           (db/get-config :memory/knowledge-decay-archive-after-bottom-ms))))
  (let [clear-response (#'http/router {:uri            "/admin/knowledge-decay"
                                       :request-method :post
                                       :headers        (ui-headers)
                                       :body           (request-body {"grace_period_days" ""
                                                                      "half_life_days" ""
                                                                      "min_confidence" ""
                                                                      "maintenance_interval_days" ""
                                                                      "archive_after_bottom_days" ""})})
        clear-body     (response-json clear-response)]
    (is (= 200 (:status clear-response)))
    (is (= 182 (get-in clear-body ["knowledge_decay" "grace_period_days"])))
    (is (= 730 (get-in clear-body ["knowledge_decay" "half_life_days"])))
    (is (= 0.1 (get-in clear-body ["knowledge_decay" "min_confidence"])))
    (is (= 1 (get-in clear-body ["knowledge_decay" "maintenance_interval_days"])))
    (is (= 365 (get-in clear-body ["knowledge_decay" "archive_after_bottom_days"])))
    (is (nil? (db/get-config :memory/knowledge-decay-grace-period-ms)))
    (is (nil? (db/get-config :memory/knowledge-decay-half-life-ms)))
    (is (nil? (db/get-config :memory/knowledge-decay-min-confidence)))
    (is (nil? (db/get-config :memory/knowledge-decay-maintenance-step-ms)))
    (is (nil? (db/get-config :memory/knowledge-decay-archive-after-bottom-ms)))))

(deftest admin-oauth-account-routes-save-connect-and-delete
  (db/register-oauth-account! {:id            :google
                               :name          "Google"
                               :authorize-url "https://accounts.google.com/o/oauth2/v2/auth"
                               :token-url     "https://oauth2.googleapis.com/token"
                               :client-id     "client-id"
                               :client-secret "client-secret"})
  (let [save-response (#'http/router {:uri            "/admin/oauth-accounts"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"id" "google"
                                                                     "name" "Google Workspace"
                                                                     "authorize_url" "https://accounts.google.com/o/oauth2/v2/auth"
                                                                     "token_url" "https://oauth2.googleapis.com/token"
                                                                     "client_id" "client-id"
                                                                     "client_secret" ""
                                                                     "provider_template" "google"
                                                                     "scopes" "openid email"
                                                                     "redirect_uri" ""
                                                                     "auth_params" "{\"access_type\":\"offline\"}"
                                                                     "token_params" ""
                                                                     "autonomous_approved" true})})
        save-body     (response-json save-response)
        account       (db/get-oauth-account :google)]
    (is (= 200 (:status save-response)))
    (is (= "Google Workspace" (get-in save-body ["oauth_account" "name"])))
    (is (= "google" (get-in save-body ["oauth_account" "provider_template"])))
    (is (= true (get-in save-body ["oauth_account" "autonomous_approved"])))
    (is (= "client-secret" (:oauth.account/client-secret account)))
    (is (= true (:oauth.account/autonomous-approved? account)))
    (is (= :google (:oauth.account/provider-template account)))
    (is (= "{\"access_type\":\"offline\"}" (:oauth.account/auth-params account))))
  (with-redefs [xia.oauth/start-authorization!
                (fn [account-id callback-url]
                  (is (= :google account-id))
                  (is (= "http://localhost:3008/oauth/callback" callback-url))
                  {:authorization-url "https://accounts.google.com/o/oauth2/v2/auth?state=abc"
                   :redirect-uri callback-url})]
    (let [response (#'http/router {:uri            "/admin/oauth-accounts/google/connect"
                                   :request-method :post
                                   :headers        (ui-headers)})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= "https://accounts.google.com/o/oauth2/v2/auth?state=abc"
             (get body "authorization_url")))))
  (db/register-service! {:id            :gmail
                         :name          "Gmail"
                         :base-url      "https://gmail.googleapis.com"
                         :auth-type     :oauth-account
                         :oauth-account :google})
  (let [delete-response (#'http/router {:uri            "/admin/oauth-accounts/google"
                                        :request-method :delete
                                        :headers        (ui-headers)})
        delete-body     (response-json delete-response)]
    (is (= 409 (:status delete-response)))
    (is (= "oauth account is still referenced by a service" (get delete-body "error")))))

(deftest admin-resource-routes-default-autonomous-approval-to-true
  (let [oauth-response (#'http/router {:uri            "/admin/oauth-accounts"
                                       :request-method :post
                                       :headers        (ui-headers)
                                       :body           (request-body {"id" "google"
                                                                      "name" "Google"
                                                                      "authorize_url" "https://accounts.google.com/o/oauth2/v2/auth"
                                                                      "token_url" "https://oauth2.googleapis.com/token"
                                                                      "client_id" "client-id"
                                                                      "client_secret" "client-secret"
                                                                      "provider_template" ""
                                                                      "scopes" "openid email"
                                                                      "redirect_uri" ""
                                                                      "auth_params" ""
                                                                      "token_params" ""})})
        service-response (#'http/router {:uri            "/admin/services"
                                         :request-method :post
                                         :headers        (ui-headers)
                                         :body           (request-body {"id" "gmail"
                                                                        "name" "Gmail"
                                                                        "base_url" "https://gmail.googleapis.com"
                                                                        "auth_type" "oauth-account"
                                                                        "oauth_account" "google"
                                                                        "auth_header" ""
                                                                        "rate_limit_per_minute" ""
                                                                        "auth_key" ""
                                                                        "enabled" true})})
        site-response (#'http/router {:uri            "/admin/sites"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"id" "portal"
                                                                     "name" "Portal"
                                                                     "login_url" "https://portal.example/login"
                                                                     "username_field" "username"
                                                                     "password_field" "password"
                                                                     "username" "hyang"
                                                                     "password" "pw"
                                                                     "form_selector" ""
                                                                     "extra_fields" ""})})
        oauth-account (db/get-oauth-account :google)
        service       (db/get-service :gmail)
        site          (db/get-site-cred :portal)]
    (is (= 200 (:status oauth-response)))
    (is (= 200 (:status service-response)))
    (is (= 200 (:status site-response)))
    (is (= true (get-in (response-json oauth-response) ["oauth_account" "autonomous_approved"])))
    (is (= true (get-in (response-json service-response) ["service" "autonomous_approved"])))
    (is (= true (get-in (response-json service-response) ["service" "oauth_account_autonomous_approved"])))
    (is (= true (get-in (response-json site-response) ["site" "autonomous_approved"])))
    (is (= true (:oauth.account/autonomous-approved? oauth-account)))
    (is (= true (:service/autonomous-approved? service)))
    (is (= true (:site-cred/autonomous-approved? site)))))

(deftest oauth-callback-route-renders-success-page
  (with-redefs [xia.oauth/callback-account-id (fn [_state] :google)
                xia.oauth/complete-authorization!
                (fn [state code]
                  (is (= "abc" state))
                  (is (= "secret-code" code))
                  {:oauth.account/id :google})]
    (let [response (#'http/router {:uri            "/oauth/callback"
                                   :request-method :get
                                   :query-string   "state=abc&code=secret-code"})]
      (is (= 200 (:status response)))
      (is (= "text/html; charset=utf-8" (get-in response [:headers "Content-Type"])))
      (is (re-find #"OAuth connected" (:body response)))
      (is (re-find #"xia-oauth-complete" (:body response))))))

(deftest admin-provider-route-preserves-secret-and-switches-default
  (db/upsert-provider! {:id       :anthropic
                        :name     "Anthropic"
                        :base-url "https://example.com/a"
                        :api-key  "anthropic-key"
                        :model    "claude"})
  (db/upsert-provider! {:id       :openai
                        :name     "OpenAI"
                        :base-url "https://api.openai.com/v1"
                        :api-key  "openai-key"
                        :model    "gpt-5"})
  (db/set-default-provider! :anthropic)
  (let [response (#'http/router {:uri            "/admin/providers"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"id" "openai"
                                                                "name" "OpenAI"
                                                                "base_url" "https://api.openai.com/v1"
                                                                "model" "gpt-5-mini"
                                                                "vision" true
                                                                "workloads" ["assistant"
                                                                              "history-compaction"]
                                                                "system_prompt_budget" "16000"
                                                                "history_budget" "32000"
                                                                "api_key" ""
                                                                "default" true})})
        body     (response-json response)]
    (is (= 200 (:status response)))
    (is (= "openai" (get-in body ["provider" "id"])))
    (is (= "gpt-5-mini" (get-in body ["provider" "model"])))
    (is (= true (get-in body ["provider" "vision"])))
    (is (= ["assistant" "history-compaction"] (get-in body ["provider" "workloads"])))
    (is (= 16000 (get-in body ["provider" "system_prompt_budget"])))
    (is (= 32000 (get-in body ["provider" "history_budget"])))
    (is (= "openai-key" (:llm.provider/api-key (db/get-provider :openai))))
    (is (= #{:assistant :history-compaction}
           (set (:llm.provider/workloads (db/get-provider :openai)))))
    (is (= true (:llm.provider/vision? (db/get-provider :openai))))
    (is (= 16000 (:llm.provider/system-prompt-budget (db/get-provider :openai))))
    (is (= 32000 (:llm.provider/history-budget (db/get-provider :openai))))
    (is (= true (:llm.provider/default? (db/get-provider :openai))))
    (is (= false (:llm.provider/default? (db/get-provider :anthropic))))))

(deftest admin-provider-route-rejects-oversized-request-body
  (let [response (#'http/router {:uri            "/admin/providers"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (oversized-request-body)})
        body     (response-json response)]
    (is (= 413 (:status response)))
    (is (= "request body too large" (get body "error")))))

(deftest admin-provider-route-clears-model-budgets
  (db/upsert-provider! {:id                   :openai
                        :name                 "OpenAI"
                        :base-url             "https://api.openai.com/v1"
                        :api-key              "openai-key"
                        :model                "gpt-5"
                        :system-prompt-budget 16000
                        :history-budget       32000
                        :default?             true})
  (let [response (#'http/router {:uri            "/admin/providers"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"id" "openai"
                                                                "name" "OpenAI"
                                                                "base_url" "https://api.openai.com/v1"
                                                                "model" "gpt-5"
                                                                "workloads" []
                                                                "system_prompt_budget" ""
                                                                "history_budget" ""
                                                                "api_key" ""
                                                                "default" true})})
        provider (db/get-provider :openai)]
    (is (= 200 (:status response)))
    (is (empty? (or (:llm.provider/workloads provider) [])))
    (is (nil? (:llm.provider/system-prompt-budget provider)))
    (is (nil? (:llm.provider/history-budget provider)))))

(deftest admin-service-route-preserves-secret-and-clears-unused-header
  (db/register-service! {:id          :github
                         :name        "GitHub"
                         :base-url    "https://api.github.com"
                         :auth-type   :api-key-header
                         :auth-key    "gh-secret"
                         :auth-header "X-API-Key"})
  (let [response (#'http/router {:uri            "/admin/services"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"id" "github"
                                                                "name" "GitHub"
                                                                "base_url" "https://api.github.com"
                                                                "auth_type" "bearer"
                                                                "auth_header" ""
                                                                "rate_limit_per_minute" "120"
                                                                "auth_key" ""
                                                                "autonomous_approved" true
                                                                "enabled" false})})
        service  (db/get-service :github)]
    (is (= 200 (:status response)))
    (is (= :bearer (:service/auth-type service)))
    (is (= "gh-secret" (:service/auth-key service)))
    (is (nil? (:service/auth-header service)))
    (is (= 120 (:service/rate-limit-per-minute service)))
    (is (= true (:service/autonomous-approved? service)))
    (is (= false (:service/enabled? service)))))

(deftest admin-site-routes-preserve-secrets-and-delete
  (db/register-site-cred! {:id             :github
                           :name           "GitHub"
                           :login-url      "https://github.com/login"
                           :username-field "login"
                           :password-field "password"
                           :username       "hyang"
                           :password       "pw-secret"
                           :form-selector  "#login"
                           :extra-fields   "{\"remember_me\":\"1\"}"})
  (let [save-response (#'http/router {:uri            "/admin/sites"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"id" "github"
                                                                     "name" "GitHub"
                                                                     "login_url" "https://github.com/login"
                                                                     "username_field" "login"
                                                                     "password_field" "password"
                                                                     "username" ""
                                                                     "password" ""
                                                                     "form_selector" ""
                                                                     "extra_fields" ""
                                                                     "autonomous_approved" true})})
        save-body     (response-json save-response)
        site          (db/get-site-cred :github)]
    (is (= 200 (:status save-response)))
    (is (= true (get-in save-body ["site" "autonomous_approved"])))
    (is (= true (get-in save-body ["site" "username_configured"])))
    (is (= true (get-in save-body ["site" "password_configured"])))
    (is (= true (:site-cred/autonomous-approved? site)))
    (is (= "hyang" (:site-cred/username site)))
    (is (= "pw-secret" (:site-cred/password site)))
    (is (nil? (:site-cred/form-selector site)))
    (is (nil? (:site-cred/extra-fields site))))
  (let [delete-response (#'http/router {:uri            "/admin/sites/github"
                                        :request-method :delete
                                        :headers        (ui-headers)})
        delete-body     (response-json delete-response)]
    (is (= 200 (:status delete-response)))
    (is (= "deleted" (get delete-body "status")))
    (is (nil? (db/get-site-cred :github)))))

(deftest start-binds-to-loopback-by-default
  (let [captured (atom nil)]
    (with-redefs [org.httpkit.server/run-server
                  (fn [_handler opts]
                    (reset! captured opts)
                    (fn [] nil))]
      (http/start! 18790)
      (http/stop!)
      (is (= "127.0.0.1" (:ip @captured)))
      (is (= 18790 (:port @captured))))))

(deftest start-allows-explicit-bind-host
  (let [captured (atom nil)]
    (with-redefs [org.httpkit.server/run-server
                  (fn [_handler opts]
                    (reset! captured opts)
                    (fn [] nil))]
      (http/start! "0.0.0.0" 18790)
      (http/stop!)
      (is (= "0.0.0.0" (:ip @captured)))
      (is (= 18790 (:port @captured))))))
