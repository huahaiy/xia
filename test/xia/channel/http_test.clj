(ns xia.channel.http-test
  (:require [charred.api :as json]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.string :as str]
            [xia.agent]
            [xia.artifact :as artifact]
            [xia.backup :as backup]
            [xia.channel.http :as http]
            [xia.db :as db]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.local-doc :as local-doc]
            [xia.local-ocr :as local-ocr]
            [xia.paths :as paths]
            [xia.remote-bridge :as remote-bridge]
            [xia.runtime :as runtime]
            [xia.schedule :as schedule]
            [xia.test-helpers :as th :refer [minimal-pdf-bytes with-test-db]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream File]
           [java.net BindException]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.security KeyPairGenerator]
           [java.security.spec NamedParameterSpec]
           [java.util Arrays Base64 UUID]))

(use-fixtures :each with-test-db)

(def ^:private byte-array-class (class (byte-array 0)))

(defn- request-body [payload]
  (let [^String body (json/write-json-str payload)]
    (ByteArrayInputStream.
      (.getBytes body StandardCharsets/UTF_8))))

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
  (let [size      (inc (long (var-get #'http/max-request-body-bytes)))
        body-size (int size)
        body      (byte-array body-size)]
    (Arrays/fill body (byte (int \x)))
    (ByteArrayInputStream. body)))

(defn- response-json [response]
  (json/read-json (:body response)))

(defn- x25519-public-key []
  (let [generator (KeyPairGenerator/getInstance "X25519")]
    (.initialize generator (NamedParameterSpec. "X25519"))
    (-> (Base64/getUrlEncoder)
        (.withoutPadding)
        (.encodeToString (.getEncoded (.getPublic (.generateKeyPair generator)))))))

(defn- remote-pairing-token
  [{:keys [device-id device-name platform topics push-token]
    :or {device-id (random-uuid)
         device-name "Phone"
         platform "ios"
         topics ["schedule.failed" "schedule.recovered"]}}]
  (json/write-json-str
    {"device_id"   (str device-id)
     "device_name" device-name
     "public_key"  (x25519-public-key)
     "platform"    platform
     "topics"      topics
     "push_token"  push-token}))

(defn- local-session-cookie []
  (let [response (#'http/router {:uri "/" :request-method :get})]
    (first (str/split (get-in response [:headers "Set-Cookie"]) #";"))))

(defn- ui-headers []
  {"origin" "http://localhost:3008"
   "cookie" (local-session-cookie)})

(defn- command-headers
  ([] (command-headers "command-secret"))
  ([token]
   {"authorization" (str "Bearer " token)}))

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
    (is (re-find #"Knowledge Graph" (:body response)))
    (is (re-find #"Settings" (:body response)))
    (is (re-find #"AI Models" (:body response)))
    (is (re-find #"Notification Bridge" (:body response)))
    (is (re-find #"Episode Retention" (:body response)))
    (is (re-find #"Knowledge Decay" (:body response)))
    (is (re-find #"Archive After Bottom \(Days\)" (:body response)))
    (is (re-find #"Workloads" (:body response)))
    (is (re-find #"System Prompt Budget" (:body response)))
    (is (re-find #"API Auth" (:body response)))
    (is (re-find #"Service Preset" (:body response)))
    (is (re-find #"Apply preset" (:body response)))
    (is (re-find #"Add to API list" (:body response)))
    (is (re-find #"Site Logins" (:body response)))
    (is (re-find #"Rate Limit \(req/min\)" (:body response)))
    (is (re-find #"<textarea" (:body response)))
    (is (re-find #"src=\"app.js\"" (:body response)))
    (is (re-find #"href=\"style.css\"" (:body response)))
    (is (re-find #"rel=\"icon\" href=\"favicon.ico\" sizes=\"any\"" (:body response)))
    (is (re-find #"href=\"favicon/favicon.svg\"" (:body response)))
    (is (re-find #"href=\"favicon/site.webmanifest\"" (:body response)))
    (is (re-find #"src=\"favicon/favicon.svg\"" (:body response)))
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
      (is (instance? byte-array-class (:body response)))
      (is (pos? (alength ^bytes (:body response))))))
  (testing "serves favicon manifest"
    (let [response (#'http/router {:uri "/favicon/site.webmanifest" :request-method :get})]
      (is (= 200 (:status response)))
      (is (= "application/manifest+json; charset=utf-8"
             (get-in response [:headers "Content-Type"])))
      (is (re-find #"/favicon/web-app-manifest-192x192.png" (:body response)))))
  (testing "serves favicon svg"
    (let [response (#'http/router {:uri "/favicon/favicon.svg" :request-method :get})]
      (is (= 200 (:status response)))
      (is (= "image/svg+xml" (get-in response [:headers "Content-Type"])))
      (is (re-find #"<svg" (:body response)))))
  (testing "serves favicon png"
    (let [response (#'http/router {:uri "/favicon/favicon-96x96.png" :request-method :get})]
      (is (= 200 (:status response)))
      (is (= "image/png" (get-in response [:headers "Content-Type"])))
      (is (instance? byte-array-class (:body response)))
      (is (pos? (alength ^bytes (:body response)))))))

(deftest web-dev-reload-route-disabled-by-default
  (let [response (#'http/router {:uri "/__dev/web-reload" :request-method :get})]
    (is (= 404 (:status response)))))

(deftest web-dev-mode-serves-live-web-assets
  (#'http/configure-web-dev! true)
  (try
    (testing "injects live reload client into the root page"
      (let [response (#'http/router {:uri "/" :request-method :get})]
        (is (= 200 (:status response)))
        (is (= "no-store, no-cache, must-revalidate, max-age=0"
               (get-in response [:headers "Cache-Control"])))
        (is (re-find #"/__dev/web-reload" (:body response)))
        (is (re-find #"window\.location\.reload" (:body response)))))
    (testing "serves the web dev reload endpoint"
      (let [response (#'http/router {:uri "/__dev/web-reload" :request-method :get})
            body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= true (get body "enabled")))
        (is (string? (get body "version")))))
    (testing "disables browser caching for static assets"
      (let [response (#'http/router {:uri "/app.js" :request-method :get})]
        (is (= 200 (:status response)))
        (is (= "no-store, no-cache, must-revalidate, max-age=0"
               (get-in response [:headers "Cache-Control"])))))
    (finally
      (#'http/configure-web-dev! false))))

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

(deftest command-route-is-disabled-without-token
  (with-redefs [xia.channel.http/command-channel-token (constantly nil)]
    (let [response (#'http/router {:uri            "/command/sessions"
                                   :request-method :post})
          body     (response-json response)]
      (is (= 503 (:status response)))
      (is (= "command channel is not configured" (get body "error"))))))

(deftest command-route-blocks-missing-or-invalid-token
  (with-redefs [xia.channel.http/command-channel-token (constantly "command-secret")]
    (testing "missing token"
      (let [response (#'http/router {:uri            "/command/sessions"
                                     :request-method :post})
            body     (response-json response)]
        (is (= 401 (:status response)))
        (is (= "missing or invalid command token" (get body "error")))))
    (testing "wrong token"
      (let [response (#'http/router {:uri            "/command/sessions"
                                     :request-method :post
                                     :headers        (command-headers "wrong-secret")})
            body     (response-json response)]
        (is (= 401 (:status response)))
        (is (= "missing or invalid command token" (get body "error")))))))

(deftest command-create-session-route-returns-session-id
  (with-redefs [xia.channel.http/command-channel-token (constantly "command-secret")]
    (let [response (#'http/router {:uri            "/command/sessions"
                                   :request-method :post
                                   :headers        (command-headers)})
          body     (response-json response)
          sid      (UUID/fromString (get body "session_id"))]
      (is (= 200 (:status response)))
      (is (= :command
             (ffirst (db/q '[:find ?channel :in $ ?sid
                             :where
                             [?s :session/id ?sid]
                             [?s :session/channel ?channel]]
                           sid)))))))

(deftest command-chat-route-creates-command-session
  (let [seen-opts (atom nil)]
    (with-redefs [xia.channel.http/command-channel-token (constantly "command-secret")
                  xia.agent/process-message (fn [_session-id user-message & opts]
                                              (reset! seen-opts (apply hash-map opts))
                                              (str "echo: " user-message))]
      (let [response (#'http/router {:uri            "/command/chat"
                                     :request-method :post
                                     :headers        (command-headers)
                                     :body           (request-body {"message" "hello"})})
            body     (response-json response)
            sid      (UUID/fromString (get body "session_id"))]
        (is (= 200 (:status response)))
        (is (= "assistant" (get body "role")))
        (is (= "echo: hello" (get body "content")))
        (is (= :command (:channel @seen-opts)))
        (is (= :command
               (ffirst (db/q '[:find ?channel :in $ ?sid
                               :where
                               [?s :session/id ?sid]
                               [?s :session/channel ?channel]]
                             sid))))))))

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

(deftest command-close-session-route-finalizes-command-session
  (let [sid       (db/create-session! :command)
        lifecycle (atom [])]
    (with-redefs [xia.channel.http/command-channel-token (constantly "command-secret")
                  xia.channel.http/cancel-rest-session-finalizer! (fn [session-id]
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
      (let [response (#'http/router {:uri            (str "/command/sessions/" sid)
                                     :request-method :delete
                                     :headers        (command-headers)})
            body     (response-json response)]
        (is (= 200 (:status response)))
        (is (= "closed" (get body "status")))
        (is (= false (get body "already_closed")))
        (is (= [[:snapshot sid]
                [:record sid :command "release planning"]
                [:clear sid]
                [:cancel (str sid)]]
               @lifecycle))
        (is (= false
               (ffirst (db/q '[:find ?active :in $ ?sid
                               :where
                               [?s :session/id ?sid]
                               [(get-else $ ?s :session/active? false) ?active]]
                             sid))))))))

(deftest close-session-route-cancels-busy-rest-session
  (let [sid       (db/create-session! :http)
        cancelled (atom nil)
        finalized (atom false)]
    (swap! @#'xia.channel.http/session-statuses
           assoc
           (str sid)
           {:state :running
            :message "Calling model"})
    (try
      (with-redefs [xia.agent/cancel-session! (fn [session-id reason]
                                                (reset! cancelled [session-id reason])
                                                true)
                    xia.channel.http/finalize-rest-session! (fn [& _]
                                                              (reset! finalized true)
                                                              true)]
        (let [response (#'http/handle-close-session (str sid))
              body     (response-json response)]
          (is (= 202 (:status response)))
          (is (= [(str sid) "session close requested"] @cancelled))
          (is (= "cancelling" (get body "status")))
          (is (= true (get body "closing")))
          (is (false? @finalized))))
      (finally
        (swap! @#'xia.channel.http/session-statuses dissoc (str sid))))))

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
        hidden-sid  (db/create-session! :command)]
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
    (is (string? (get-in create-body ["documents" 0 "summary"])))
    (is (string? (get-in create-body ["documents" 0 "preview"])))
    (is (= 1 (get-in create-body ["documents" 0 "chunk_count"])))
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
      (is (string? (get-in get-body ["document" "summary"])))
      (is (= 1 (get-in get-body ["document" "chunk_count"])))
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
    (is (string? (get-in create-body ["documents" 0 "summary"])))
    (let [get-res  (#'http/router {:uri            (str "/sessions/" sid "/local-documents/" doc-id)
                                   :request-method :get
                                   :headers        (ui-headers)})
          get-body (response-json get-res)]
      (is (= 200 (:status get-res)))
      (is (= 1 (get-in get-body ["document" "chunk_count"])))
      (is (= "# Local\n\ncontent" (get-in get-body ["document" "text"]))))))

(deftest local-document-multipart-route-extracts-pdf
  (let [sid         (str (db/create-session! :http))
        create-res  (#'http/router
                      (multipart-request
                        (str "/sessions/" sid "/local-documents")
                        [{:field-name   "documents"
                          :filename     "paper.pdf"
                          :content-type "application/pdf"
                          :body-bytes   (minimal-pdf-bytes "Hello multipart PDF")}]))
        create-body (response-json create-res)
        doc-id      (get-in create-body ["documents" 0 "id"])
        get-res     (#'http/router {:uri            (str "/sessions/" sid "/local-documents/" doc-id)
                                    :request-method :get
                                    :headers        (ui-headers)})
        get-body    (response-json get-res)]
    (is (= 201 (:status create-res)))
    (is (= "application/pdf" (get-in create-body ["documents" 0 "media_type"])))
    (is (= 200 (:status get-res)))
    (is (= "ready" (get-in create-body ["documents" 0 "status"])))
    (is (= [] (get create-body "errors")))
    (is (= "ready" (get-in get-body ["document" "status"])))
    (is (.contains ^String (get-in get-body ["document" "text"]) "Hello multipart PDF"))))

(deftest local-document-create-route-supports-image-ocr-mode
  (let [sid   (str (db/create-session! :http))
        calls (atom [])]
    (with-redefs [local-ocr/ocr-image-bytes (fn [_bytes opts]
                                              (swap! calls conj opts)
                                              "<table><tr><td>A</td></tr></table>")]
      (let [response (#'http/router {:uri            (str "/sessions/" sid "/local-documents")
                                     :request-method :post
                                     :headers        (ui-headers)
                                     :body           (request-body {"documents" [{"name" "sheet.png"
                                                                                  "media_type" "image/png"
                                                                                  "ocr_mode" "table"
                                                                                  "bytes_base64" (.encodeToString (Base64/getEncoder)
                                                                                                                   (.getBytes "fake-image"
                                                                                                                              StandardCharsets/UTF_8))}]})})
            body     (response-json response)]
        (is (= 201 (:status response)))
        (is (= "<table><tr><td>A</td></tr></table>"
               (get-in body ["documents" 0 "summary"])))
        (is (= :table (:ocr-mode (first @calls))))))))

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

(deftest knowledge-routes-search-list-and-forget-facts
  (let [alice-eid      (th/seed-node! "Alice Johnson" "person")
        _bob-eid       (th/seed-node! "Bob Team" "person")
        forgotten-eid  (th/seed-fact! alice-eid "prefers green tea")
        kept-eid       (th/seed-fact! alice-eid "works on billing")
        _bob-fact-eid  (th/seed-fact! (th/seed-node! "Infra Project" "project")
                                      "tracks deployment incidents")
        search-res     (#'http/router {:uri            "/knowledge/nodes"
                                       :query-string   "query=Alice"
                                       :request-method :get
                                       :headers        (ui-headers)})
        search-body    (response-json search-res)
        node-id        (get-in search-body ["nodes" 0 "id"])
        facts-res      (#'http/router {:uri            (str "/knowledge/nodes/" node-id "/facts")
                                       :request-method :get
                                       :headers        (ui-headers)})
        facts-body     (response-json facts-res)
        delete-res     (#'http/router {:uri            (str "/knowledge/facts/" forgotten-eid)
                                       :request-method :delete
                                       :headers        (ui-headers)})
        delete-body    (response-json delete-res)
        facts-after    (#'http/router {:uri            (str "/knowledge/nodes/" node-id "/facts")
                                       :request-method :get
                                       :headers        (ui-headers)})
        facts-after-body (response-json facts-after)]
    (is (= 200 (:status search-res)))
    (is (= "Alice Johnson" (get-in search-body ["nodes" 0 "name"])))
    (is (= "person" (get-in search-body ["nodes" 0 "type"])))
    (is (= (str alice-eid) node-id))
    (is (= 200 (:status facts-res)))
    (is (= "Alice Johnson" (get-in facts-body ["node" "name"])))
    (is (= #{"prefers green tea" "works on billing"}
           (set (map #(get % "content") (get facts-body "facts")))))
    (is (= 200 (:status delete-res)))
    (is (= "forgotten" (get delete-body "status")))
    (is (= forgotten-eid (get-in delete-body ["fact" "eid"])))
    (is (empty? (into {} (db/entity forgotten-eid))))
    (is (= 200 (:status facts-after)))
    (is (= [kept-eid] (mapv #(get % "eid") (get facts-after-body "facts"))))))

(deftest knowledge-routes-validate-inputs
  (let [missing-query (#'http/router {:uri            "/knowledge/nodes"
                                      :request-method :get
                                      :headers        (ui-headers)})
        missing-body  (response-json missing-query)
        invalid-node  (#'http/router {:uri            "/knowledge/nodes/not-a-number/facts"
                                      :request-method :get
                                      :headers        (ui-headers)})
        invalid-node-body (response-json invalid-node)
        missing-fact  (#'http/router {:uri            "/knowledge/facts/999999"
                                      :request-method :delete
                                      :headers        (ui-headers)})
        missing-fact-body (response-json missing-fact)]
    (is (= 400 (:status missing-query)))
    (is (= "missing query" (get missing-body "error")))
    (is (= 400 (:status invalid-node)))
    (is (= "'node id' must be a positive integer" (get invalid-node-body "error")))
    (is (= 404 (:status missing-fact)))
    (is (= "fact not found" (get missing-fact-body "error")))))

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

(deftest command-approval-route-allows-round-trip
  (with-redefs [xia.channel.http/command-channel-token (constantly "command-secret")]
    (let [sid    (str (db/create-session! :command))
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
                           #(let [response (#'http/router {:uri            (str "/command/sessions/" sid "/approval")
                                                           :request-method :get
                                                           :headers        (command-headers)})
                                  body     (response-json response)]
                              (when (get body "pending")
                                body)))]
        (is (some? pending-body))
        (is (= "browser-login" (get-in pending-body ["approval" "tool_name"])))
        (let [approval-id (get-in pending-body ["approval" "approval_id"])
              submit      (#'http/router {:uri            (str "/command/sessions/" sid "/approval")
                                          :request-method :post
                                          :headers        (command-headers)
                                          :body           (request-body {"approval_id" approval-id
                                                                         "decision" "allow"})})
              submit-body (response-json submit)]
          (is (= 200 (:status submit)))
          (is (= "recorded" (get submit-body "status")))))
      (is (= true (deref waiter 2000 ::timeout))))))

(deftest session-message-routes-keep-http-and-command-sessions-separate
  (let [http-sid    (str (db/create-session! :http))
        command-sid (str (db/create-session! :command))]
    (with-redefs [xia.channel.http/command-channel-token (constantly "command-secret")]
      (let [http-response (#'http/router {:uri            (str "/sessions/" command-sid "/messages")
                                          :request-method :get
                                          :headers        (ui-headers)})
            http-body     (response-json http-response)
            cmd-response  (#'http/router {:uri            (str "/command/sessions/" http-sid "/messages")
                                          :request-method :get
                                          :headers        (command-headers)})
            cmd-body      (response-json cmd-response)]
        (is (= 404 (:status http-response)))
        (is (= "session not found" (get http-body "error")))
        (is (= 404 (:status cmd-response)))
        (is (= "session not found" (get cmd-body "error")))))))

(deftest admin-config-route-returns-safe-summaries
  (db/upsert-provider! {:id       :openai
                        :name     "OpenAI"
                        :template :openai
                        :base-url "https://api.openai.com/v1"
                        :api-key  "sk-secret"
                        :model    "gpt-5"
                        :access-mode :api
                        :credential-source :api-key
                        :workloads #{:assistant :history-compaction}
                        :system-prompt-budget 16000
                        :history-budget 32000
                        :rate-limit-per-minute 75})
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
                      :content     "content"
                      :source-format :openclaw-zip-url
                      :source-url "https://clawhub.ai/downloads/demo-skill.zip"
                      :source-name "demo-skill.zip"
                      :import-warnings ["Ignored unsupported frontmatter field `homepage`."]
                      :imported-from-openclaw? true})
  (instance-supervisor/configure! {:enabled? true
                                   :command "/opt/xia/bin/xia"})
  (instance-supervisor/set-instance-management-enabled! true)
  (try
    (let [response (#'http/router {:uri            "/admin/config"
                                   :request-method :get
                                   :headers        (ui-headers)})
          body     (response-json response)
          provider (first (filter #(= "openai" (get % "id")) (get body "providers")))
          openai-template (first (filter #(= "openai" (get % "id")) (get body "llm_provider_templates")))
          qwen-template (first (filter #(= "qwen" (get % "id")) (get body "llm_provider_templates")))
          glm-template (first (filter #(= "glm" (get % "id")) (get body "llm_provider_templates")))
          custom-template (first (filter #(= "custom" (get % "id")) (get body "llm_provider_templates")))
          conversation-context (get body "conversation_context")
          memory-retention (get body "memory_retention")
          knowledge-decay (get body "knowledge_decay")
          local-doc-summarization (get body "local_doc_summarization")
          local-doc-ocr (get body "local_doc_ocr")
          database-backup (get body "database_backup")
          llm-workloads (get body "llm_workloads")
          templates (get body "oauth_provider_templates")
          oauth    (first (filter #(= "google" (get % "id")) (get body "oauth_accounts")))
          service  (first (filter #(= "github" (get % "id")) (get body "services")))
          site     (first (filter #(= "github" (get % "id")) (get body "sites")))
          tool     (first (filter #(= "demo-tool" (get % "id")) (get body "tools")))
          skill    (first (filter #(= "demo-skill" (get % "id")) (get body "skills")))
          remote-bridge (get body "remote_bridge")
          remote-devices (get body "remote_devices")
          remote-events (get body "remote_events")
          remote-snapshot (get body "remote_snapshot")]
      (is (= 200 (:status response)))
      (is (= false (get body "setup_required")))
      (is (= "General personal assistant for everyday digital work."
             (get-in body ["identity" "role"])))
      (is (= "default" (get-in body ["instance" "id"])))
      (is (= true (get-in body ["capabilities" "instance_management_configured"])))
      (is (= true (get-in body ["capabilities" "instance_management_enabled"])))
      (is (= (paths/absolute-path (db/current-db-path))
             (get-in body ["storage" "db_path"])))
      (is (= (paths/absolute-path (paths/support-dir-path (db/current-db-path)))
             (get-in body ["storage" "support_dir"])))
    (is (= true (get provider "api_key_configured")))
    (is (not (contains? provider "api_key")))
    (is (= "openai" (get provider "template")))
    (is (= "api" (get provider "access_mode")))
    (is (= "api-key" (get provider "credential_source")))
    (is (= "api-key" (get provider "auth_type")))
    (is (= true (get provider "default")))
    (is (= ["assistant" "history-compaction"] (get provider "workloads")))
    (is (= 16000 (get provider "system_prompt_budget")))
	    (is (= 32000 (get provider "history_budget")))
	    (is (= 75 (get provider "rate_limit_per_minute")))
	    (is (= 75 (get provider "effective_rate_limit_per_minute")))
	    (is (= 24 (get conversation-context "recent_history_message_limit")))
	    (is (= 8000 (get conversation-context "history_budget")))
	    (is (= 182 (get memory-retention "full_resolution_days")))
    (is (= 365 (get memory-retention "decay_half_life_days")))
    (is (= 8 (get memory-retention "retained_count")))
    (is (= 182 (get knowledge-decay "grace_period_days")))
    (is (= 730 (get knowledge-decay "half_life_days")))
    (is (= 0.1 (get knowledge-decay "min_confidence")))
    (is (= 1 (get knowledge-decay "maintenance_interval_days")))
    (is (= 365 (get knowledge-decay "archive_after_bottom_days")))
    (is (= false (get local-doc-summarization "model_summaries_enabled")))
    (is (= "local" (get local-doc-summarization "model_summary_backend")))
    (is (nil? (get local-doc-summarization "model_summary_provider_id")))
    (is (= 96 (get local-doc-summarization "chunk_summary_max_tokens")))
    (is (= 160 (get local-doc-summarization "doc_summary_max_tokens")))
    (is (= false (get local-doc-ocr "enabled")))
    (is (= "local" (get local-doc-ocr "model_backend")))
    (is (nil? (get local-doc-ocr "external_provider_id")))
    (is (= "openai" (get local-doc-ocr "resolved_external_provider_id")))
    (is (= false (get local-doc-ocr "external_provider_vision")))
    (is (= true (get local-doc-ocr "configured")))
    (is (= "ocr" (get local-doc-ocr "default_mode")))
    (is (= 1605632 (get local-doc-ocr "spotting_image_max_pixels")))
    (is (= #{"ocr" "formula" "table" "chart" "seal" "spotting"}
           (set (map #(get % "id") (get local-doc-ocr "supported_modes")))))
    (is (= false (get database-backup "enabled")))
    (is (= (backup/backup-directory) (get database-backup "directory")))
    (is (= 24 (get database-backup "interval_hours")))
    (is (= 7 (get database-backup "retain_count")))
    (is (= false (get database-backup "running")))
    (is (nil? (get database-backup "last_success_at")))
    (is (nil? (get database-backup "last_error")))
    (is (= "healthy" (get provider "health_status")))
    (is (= #{"assistant" "history-compaction" "topic-summary" "memory-summary" "memory-importance" "memory-extraction" "fact-utility"}
           (set (map #(get % "id") llm-workloads))))
    (is (every? (set (map #(get % "id") (get body "llm_provider_templates")))
                ["claude" "custom" "deepseek" "gemini" "glm"
                 "minimax" "ollama" "openai" "openrouter" "qwen"]))
    (is (= #{"api-key"} (set (get openai-template "auth_types"))))
    (is (= "api" (get-in openai-template ["access_modes" 0 "id"])))
    (is (= true (get-in openai-template ["access_modes" 0 "default"])))
    (is (= "api-key" (get-in openai-template ["access_modes" 0 "credential_sources" 0])))
    (is (= #{"api-key"} (set (get qwen-template "auth_types"))))
    (is (= ["api-key"] (get-in qwen-template ["access_modes" 0 "credential_sources"])))
    (is (= #{"api-key"} (set (get glm-template "auth_types"))))
    (is (= ["api-key"] (get-in glm-template ["access_modes" 0 "credential_sources"])))
    (is (= #{"api-key" "none"} (set (get custom-template "auth_types"))))
    (is (= ["api-key"] (get-in custom-template ["access_modes" 0 "credential_sources"])))
    (is (nil? (get openai-template "account_connector")))
    (is (= "https://platform.openai.com/" (get openai-template "account_url")))
    (is (= "https://platform.openai.com/api-keys" (get openai-template "api_key_url")))
    (is (= "https://help.openai.com/en/articles/4936850-where-do-i-find-my-api-key"
           (get openai-template "docs_url")))
    (is (= [] (get openai-template "sign_in_options")))
    (is (= #{"github" "google" "gmail" "microsoft"}
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
    (is (= true (get skill "enabled")))
    (is (= "openclaw-zip-url" (get skill "source_format")))
    (is (= "https://clawhub.ai/downloads/demo-skill.zip" (get skill "source_url")))
    (is (= "demo-skill.zip" (get skill "source_name")))
    (is (= true (get skill "imported_from_openclaw")))
    (is (= ["Ignored unsupported frontmatter field `homepage`."] (get skill "import_warnings")))
    (is (= "primary" (get remote-bridge "id")))
    (is (= false (get remote-bridge "enabled")))
    (is (= true (get remote-bridge "keypair_ready")))
    (is (string? (get remote-bridge "public_key")))
    (is (= "disabled" (get remote-bridge "connection_state")))
      (is (= [] remote-devices))
      (is (= [] remote-events))
      (is (= [] (get remote-snapshot "attention")))
      (is (= "disabled" (get-in remote-snapshot ["connectivity" "connection_state"]))))
    (finally
      (instance-supervisor/set-instance-management-enabled! false)
      (instance-supervisor/configure! {:enabled? false}))))

(deftest admin-config-route-flags-first-run-onboarding-when-no-providers
  (let [response (#'http/router {:uri            "/admin/config"
                                 :request-method :get
                                 :headers        (ui-headers)})
        body     (response-json response)]
    (is (= 200 (:status response)))
    (is (= true (get body "setup_required")))
    (is (seq (get body "llm_provider_templates")))))

(deftest admin-identity-route-saves-role
  (instance-supervisor/configure! {:enabled? true
                                   :command "/opt/xia/bin/xia"})
  (try
    (let [response (#'http/router {:uri            "/admin/identity"
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"name" "Ops Xia"
                                                                  "role" "Release assistant"
                                                                  "description" "Coordinates release work."
                                                                  "controller_enabled" true})})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= "Ops Xia" (get-in body ["identity" "name"])))
      (is (= "Release assistant" (get-in body ["identity" "role"])))
      (is (= "Coordinates release work." (get-in body ["identity" "description"])))
      (is (= true (get-in body ["capabilities" "instance_management_configured"])))
      (is (= true (get-in body ["capabilities" "instance_management_enabled"])))
      (is (= "Release assistant" (db/get-identity :role))))
    (finally
      (instance-supervisor/set-instance-management-enabled! false)
      (instance-supervisor/configure! {:enabled? false}))))

(deftest admin-context-route-saves-and-clears-settings
  (let [save-response (#'http/router {:uri            "/admin/context"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"recent_history_message_limit" "40"
                                                                     "history_budget" "12000"})})
        save-body     (response-json save-response)]
    (is (= 200 (:status save-response)))
    (is (= 40 (get-in save-body ["conversation_context" "recent_history_message_limit"])))
    (is (= 12000 (get-in save-body ["conversation_context" "history_budget"])))
    (is (= "40" (db/get-config :context/recent-history-message-limit)))
    (is (= "12000" (db/get-config :context/history-budget))))
  (let [clear-response (#'http/router {:uri            "/admin/context"
                                       :request-method :post
                                       :headers        (ui-headers)
                                       :body           (request-body {"recent_history_message_limit" ""
                                                                      "history_budget" ""})})
        clear-body     (response-json clear-response)]
    (is (= 200 (:status clear-response)))
    (is (= 24 (get-in clear-body ["conversation_context" "recent_history_message_limit"])))
    (is (= 8000 (get-in clear-body ["conversation_context" "history_budget"])))
    (is (nil? (db/get-config :context/recent-history-message-limit)))
    (is (nil? (db/get-config :context/history-budget)))))

(deftest admin-remote-bridge-route-saves-config
  (let [response (#'http/router {:uri            "/admin/remote-bridge"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"enabled" true
                                                                "instance_label" "Desk Xia"
                                                                "relay_url" "https://relay.example.test"})})
        body     (response-json response)]
    (is (= 200 (:status response)))
    (is (= true (get-in body ["remote_bridge" "enabled"])))
    (is (= "Desk Xia" (get-in body ["remote_bridge" "instance_label"])))
    (is (= "https://relay.example.test" (get-in body ["remote_bridge" "relay_url"])))
    (is (= true (get-in body ["remote_snapshot" "connectivity" "enabled"])))
    (is (= "Desk Xia" (get-in body ["remote_snapshot" "instance" "label"])))
    (is (= true (:enabled? (remote-bridge/bridge-config))))))

(deftest admin-local-doc-summarization-route-saves-and-clears-settings
  (db/upsert-provider! {:id :openai
                        :name "OpenAI"
                        :base-url "https://api.openai.com/v1"
                        :model "gpt-4o-mini"})
  (let [save-response (#'http/router {:uri            "/admin/local-doc-summarization"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"model_summaries_enabled" true
                                                                     "model_summary_backend" "external"
                                                                     "model_summary_provider_id" "openai"
                                                                     "chunk_summary_max_tokens" "128"
                                                                     "doc_summary_max_tokens" "256"})})
        save-body     (response-json save-response)]
    (is (= 200 (:status save-response)))
    (is (= true (get-in save-body ["local_doc_summarization" "model_summaries_enabled"])))
    (is (= "external" (get-in save-body ["local_doc_summarization" "model_summary_backend"])))
    (is (= "openai" (get-in save-body ["local_doc_summarization" "model_summary_provider_id"])))
    (is (= 128 (get-in save-body ["local_doc_summarization" "chunk_summary_max_tokens"])))
    (is (= 256 (get-in save-body ["local_doc_summarization" "doc_summary_max_tokens"])))
    (is (= "true" (str (db/get-config :local-doc/model-summaries-enabled?))))
    (is (= "external" (db/get-config :local-doc/model-summary-backend)))
    (is (= "openai" (db/get-config :local-doc/model-summary-provider-id)))
    (is (= "128" (db/get-config :local-doc/chunk-summary-max-tokens)))
    (is (= "256" (db/get-config :local-doc/doc-summary-max-tokens))))
  (let [clear-response (#'http/router {:uri            "/admin/local-doc-summarization"
                                       :request-method :post
                                       :headers        (ui-headers)
                                       :body           (request-body {"model_summaries_enabled" false
                                                                      "model_summary_backend" ""
                                                                      "model_summary_provider_id" ""
                                                                      "chunk_summary_max_tokens" ""
                                                                      "doc_summary_max_tokens" ""})})
        clear-body     (response-json clear-response)]
    (is (= 200 (:status clear-response)))
    (is (= false (get-in clear-body ["local_doc_summarization" "model_summaries_enabled"])))
    (is (= "local" (get-in clear-body ["local_doc_summarization" "model_summary_backend"])))
    (is (nil? (get-in clear-body ["local_doc_summarization" "model_summary_provider_id"])))
    (is (= 96 (get-in clear-body ["local_doc_summarization" "chunk_summary_max_tokens"])))
    (is (= 160 (get-in clear-body ["local_doc_summarization" "doc_summary_max_tokens"])))
    (is (= "false" (str (db/get-config :local-doc/model-summaries-enabled?))))
    (is (nil? (db/get-config :local-doc/model-summary-backend)))
    (is (nil? (db/get-config :local-doc/model-summary-provider-id)))
    (is (nil? (db/get-config :local-doc/chunk-summary-max-tokens)))
    (is (nil? (db/get-config :local-doc/doc-summary-max-tokens)))))

(deftest admin-local-doc-ocr-route-saves-and-clears-settings
  (db/set-config! :local-doc/ocr-command "/usr/local/bin/llama-cli")
  (db/set-config! :local-doc/ocr-model-path "/models/PaddleOCR-VL-1.5.gguf")
  (db/set-config! :local-doc/ocr-mmproj-path "/models/PaddleOCR-VL-1.5-mmproj.gguf")
  (db/set-config! :local-doc/ocr-spotting-mmproj-path "/models/PaddleOCR-VL-1.5-mmproj-spotting.gguf")
  (let [save-response (#'http/router {:uri            "/admin/local-doc-ocr"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"enabled" true
                                                                     "model_backend" "local"
                                                                     "external_provider_id" ""
                                                                     "timeout_ms" "180000"
                                                                     "max_tokens" "1024"})})
        save-body     (response-json save-response)]
    (is (= 200 (:status save-response)))
    (is (= true (get-in save-body ["local_doc_ocr" "enabled"])))
    (is (= "local" (get-in save-body ["local_doc_ocr" "model_backend"])))
    (is (nil? (get-in save-body ["local_doc_ocr" "external_provider_id"])))
    (is (= 180000 (get-in save-body ["local_doc_ocr" "timeout_ms"])))
    (is (= 1024 (get-in save-body ["local_doc_ocr" "max_tokens"])))
    (is (= "true" (str (db/get-config :local-doc/ocr-enabled?))))
    (is (= "local" (db/get-config :local-doc/ocr-backend)))
    (is (nil? (db/get-config :local-doc/ocr-provider-id)))
    (is (nil? (db/get-config :local-doc/ocr-command)))
    (is (nil? (db/get-config :local-doc/ocr-model-path)))
    (is (nil? (db/get-config :local-doc/ocr-mmproj-path)))
    (is (nil? (db/get-config :local-doc/ocr-spotting-mmproj-path)))
    (is (= "180000" (db/get-config :local-doc/ocr-timeout-ms)))
    (is (= "1024" (db/get-config :local-doc/ocr-max-tokens))))
  (let [clear-response (#'http/router {:uri            "/admin/local-doc-ocr"
                                       :request-method :post
                                       :headers        (ui-headers)
                                       :body           (request-body {"enabled" false
                                                                      "model_backend" ""
                                                                      "external_provider_id" ""
                                                                      "timeout_ms" ""
                                                                      "max_tokens" ""})})
        clear-body     (response-json clear-response)]
    (is (= 200 (:status clear-response)))
    (is (= false (get-in clear-body ["local_doc_ocr" "enabled"])))
    (is (= "local" (get-in clear-body ["local_doc_ocr" "model_backend"])))
    (is (nil? (get-in clear-body ["local_doc_ocr" "external_provider_id"])))
    (is (= true (get-in clear-body ["local_doc_ocr" "configured"])))
    (is (= 120000 (get-in clear-body ["local_doc_ocr" "timeout_ms"])))
    (is (= 2048 (get-in clear-body ["local_doc_ocr" "max_tokens"])))
    (is (= "false" (str (db/get-config :local-doc/ocr-enabled?))))
    (is (nil? (db/get-config :local-doc/ocr-backend)))
    (is (nil? (db/get-config :local-doc/ocr-provider-id)))
    (is (nil? (db/get-config :local-doc/ocr-command)))
    (is (nil? (db/get-config :local-doc/ocr-model-path)))
    (is (nil? (db/get-config :local-doc/ocr-mmproj-path)))
    (is (nil? (db/get-config :local-doc/ocr-spotting-mmproj-path)))
    (is (nil? (db/get-config :local-doc/ocr-timeout-ms)))
    (is (nil? (db/get-config :local-doc/ocr-max-tokens)))))

(deftest admin-local-doc-ocr-route-saves-external-provider-selection
  (db/upsert-provider! {:id :vision
                        :name "Vision"
                        :base-url "https://api.example.com/v1"
                        :model "gpt-4.1-mini"
                        :vision? true})
  (let [response (#'http/router {:uri            "/admin/local-doc-ocr"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"enabled" true
                                                                "model_backend" "external"
                                                                "external_provider_id" "vision"})})
        body     (response-json response)]
    (is (= 200 (:status response)))
    (is (= "external" (get-in body ["local_doc_ocr" "model_backend"])))
    (is (= "vision" (get-in body ["local_doc_ocr" "external_provider_id"])))
    (is (= "vision" (get-in body ["local_doc_ocr" "resolved_external_provider_id"])))
    (is (= true (get-in body ["local_doc_ocr" "external_provider_vision"])))
    (is (= true (get-in body ["local_doc_ocr" "configured"])))
    (is (= "external" (db/get-config :local-doc/ocr-backend)))
    (is (= "vision" (db/get-config :local-doc/ocr-provider-id)))))

(deftest admin-database-backup-route-saves-and-clears-settings
  (let [save-response (#'http/router {:uri            "/admin/database-backup"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"enabled" true
                                                                     "directory" "/tmp/xia-backups"
                                                                     "interval_hours" "12"
                                                                     "retain_count" "5"})})
        save-body     (response-json save-response)]
    (is (= 200 (:status save-response)))
    (is (= true (get-in save-body ["database_backup" "enabled"])))
    (is (= "/tmp/xia-backups" (get-in save-body ["database_backup" "directory"])))
    (is (= 12 (get-in save-body ["database_backup" "interval_hours"])))
    (is (= 5 (get-in save-body ["database_backup" "retain_count"])))
    (is (= "true" (str (db/get-config :backup/enabled?))))
    (is (= "/tmp/xia-backups" (db/get-config :backup/directory)))
    (is (= "12" (db/get-config :backup/interval-hours)))
    (is (= "5" (db/get-config :backup/retain-count))))
  (let [clear-response (#'http/router {:uri            "/admin/database-backup"
                                       :request-method :post
                                       :headers        (ui-headers)
                                       :body           (request-body {"enabled" false
                                                                      "directory" ""
                                                                      "interval_hours" ""
                                                                      "retain_count" ""})})
        clear-body     (response-json clear-response)]
    (is (= 200 (:status clear-response)))
    (is (= false (get-in clear-body ["database_backup" "enabled"])))
    (is (= (backup/backup-directory) (get-in clear-body ["database_backup" "directory"])))
    (is (= 24 (get-in clear-body ["database_backup" "interval_hours"])))
    (is (= 7 (get-in clear-body ["database_backup" "retain_count"])))
    (is (= "false" (str (db/get-config :backup/enabled?))))
    (is (nil? (db/get-config :backup/directory)))
    (is (nil? (db/get-config :backup/interval-hours)))
    (is (nil? (db/get-config :backup/retain-count)))))

(deftest admin-remote-bridge-device-routes-pair-and-revoke
  (let [device-id (random-uuid)
        pair-response (#'http/router {:uri            "/admin/remote-bridge/pair"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"pairing_token"
                                                                     (remote-pairing-token {:device-id device-id
                                                                                            :device-name "Desk Phone"
                                                                                            :platform "ios"
                                                                                            :topics ["schedule.failed"
                                                                                                     "service.attention"]})})})
        pair-body     (response-json pair-response)
        revoke-response (#'http/router {:uri            (str "/admin/remote-bridge/devices/" device-id)
                                        :request-method :delete
                                        :headers        (ui-headers)})
        revoke-body     (response-json revoke-response)]
    (is (= 200 (:status pair-response)))
    (is (= (str device-id) (get-in pair-body ["device" "id"])))
    (is (= "Desk Phone" (get-in pair-body ["device" "name"])))
    (is (= "paired" (get-in pair-body ["device" "status"])))
    (is (= ["schedule.failed" "service.attention"]
           (get-in pair-body ["device" "topics"])))
    (is (= 1 (count (get pair-body "remote_devices"))))
    (is (= 200 (:status revoke-response)))
    (is (= (str device-id) (get-in revoke-body ["device" "id"])))
    (is (= "revoked" (get-in revoke-body ["device" "status"])))
    (is (= "revoked"
           (get-in (first (get revoke-body "remote_devices")) ["status"])))))

(deftest admin-openclaw-import-route-imports-skill-bundle
  (let [^File root (.toFile (Files/createTempDirectory "xia-http-openclaw" (into-array FileAttribute [])))]
    (try
      (spit (io/file root "SKILL.md")
            (str "---\n"
                 "name: HTTP Imported Skill\n"
                 "description: Imported through admin route.\n"
                 "tags: [browser]\n"
                 "---\n\n"
                 "Use the `browser` tool to inspect the page.\n"))
      (spit (io/file root "notes.txt")
            "Capture the title and the key facts.\n")
      (let [response (#'http/router {:uri            "/admin/skills/import-openclaw"
                                     :request-method :post
                                     :headers        (ui-headers)
                                     :body           (request-body {"source" (.getAbsolutePath root)})})
            body     (response-json response)
            imported (get body "import")
            skill    (get body "skill")
            stored   (db/get-skill :http-imported-skill)]
        (is (= 200 (:status response)))
        (is (= "imported" (get imported "status")))
        (is (= "http-imported-skill" (get imported "skill_id")))
        (is (= "openclaw-dir" (get-in imported ["source" "format"])))
        (is (= (.getAbsolutePath root) (get-in imported ["source" "path"])))
        (is (= "HTTP Imported Skill" (get skill "name")))
        (is (= ["browser"] (get skill "tags")))
        (is (= true (get skill "imported_from_openclaw")))
        (is (= "openclaw-dir" (get skill "source_format")))
        (is (empty? (get skill "import_warnings")))
        (is (= "HTTP Imported Skill" (:skill/name stored)))
        (is (str/includes? (:skill/content stored) "## Xia Compatibility"))
        (is (str/includes? (:skill/content stored) "## Bundled Resources")))
      (finally
        (doseq [file (reverse (file-seq root))]
          (Files/deleteIfExists (.toPath ^File file)))))))

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
                               :connection-mode :oauth-flow
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
    (is (= "oauth-flow" (get-in save-body ["oauth_account" "connection_mode"])))
    (is (= "google" (get-in save-body ["oauth_account" "provider_template"])))
    (is (= true (get-in save-body ["oauth_account" "autonomous_approved"])))
    (is (= "client-secret" (:oauth.account/client-secret account)))
    (is (= true (:oauth.account/autonomous-approved? account)))
    (is (= :oauth-flow (:oauth.account/connection-mode account)))
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
    (is (= "oauth account is still referenced by a provider or service" (get delete-body "error")))))

(deftest admin-oauth-account-delete-blocks-when-provider-linked
  (db/register-oauth-account! {:id            :openai-login
                               :name          "OpenAI Login"
                               :authorize-url "https://example.com/oauth/authorize"
                               :token-url     "https://example.com/oauth/token"
                               :client-id     "client-id"
                               :client-secret "client-secret"})
  (db/upsert-provider! {:id            :openai
                        :name          "OpenAI"
                        :template      :openai
                        :base-url      "https://api.openai.com/v1"
                        :model         "gpt-5"
                        :auth-type     :oauth-account
                        :oauth-account :openai-login})
  (let [response (#'http/router {:uri            "/admin/oauth-accounts/openai-login"
                                 :request-method :delete
                                 :headers        (ui-headers)})
        body     (response-json response)]
    (is (= 409 (:status response)))
    (is (= "oauth account is still referenced by a provider or service"
           (get body "error")))))

(deftest admin-oauth-account-route-supports-manual-token-connections
  (let [save-response (#'http/router {:uri            "/admin/oauth-accounts"
                                      :request-method :post
                                      :headers        (ui-headers)
                                      :body           (request-body {"id" "claude-token"
                                                                     "name" "Claude Token"
                                                                     "connection_mode" "manual-token"
                                                                     "provider_template" ""
                                                                     "access_token" "token-123"
                                                                     "token_type" "Bearer"
                                                                     "expires_at" "2027-03-22T00:00:00Z"
                                                                     "autonomous_approved" true})})
        save-body     (response-json save-response)
        account       (db/get-oauth-account :claude-token)]
    (is (= 200 (:status save-response)))
    (is (= "manual-token" (get-in save-body ["oauth_account" "connection_mode"])))
    (is (= true (get-in save-body ["oauth_account" "access_token_configured"])))
    (is (= true (get-in save-body ["oauth_account" "connected"])))
    (is (= "Bearer" (get-in save-body ["oauth_account" "token_type"])))
    (is (= :manual-token (:oauth.account/connection-mode account)))
    (is (= "token-123" (:oauth.account/access-token account)))
    (is (= "Bearer" (:oauth.account/token-type account)))
    (is (= "2027-03-22T00:00:00Z" (str (.toInstant ^java.util.Date (:oauth.account/expires-at account)))))
    (is (nil? (:oauth.account/authorize-url account))))
  (let [connect-response (#'http/router {:uri            "/admin/oauth-accounts/claude-token/connect"
                                         :request-method :post
                                         :headers        (ui-headers)})
        connect-body     (response-json connect-response)]
    (is (= 400 (:status connect-response)))
    (is (= "manual-token connections do not support Connect Now"
           (get connect-body "error")))))

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
                                                                "rate_limit_per_minute" "90"
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
    (is (= 90 (get-in body ["provider" "rate_limit_per_minute"])))
    (is (= 90 (get-in body ["provider" "effective_rate_limit_per_minute"])))
    (is (= "openai-key" (:llm.provider/api-key (db/get-provider :openai))))
    (is (= #{:assistant :history-compaction}
           (set (:llm.provider/workloads (db/get-provider :openai)))))
    (is (= true (:llm.provider/vision? (db/get-provider :openai))))
    (is (= 16000 (:llm.provider/system-prompt-budget (db/get-provider :openai))))
    (is (= 32000 (:llm.provider/history-budget (db/get-provider :openai))))
    (is (= 90 (:llm.provider/rate-limit-per-minute (db/get-provider :openai))))
    (is (= true (:llm.provider/default? (db/get-provider :openai))))
    (is (= false (:llm.provider/default? (db/get-provider :anthropic))))))

(deftest admin-provider-model-metadata-route-returns-inferred-vision
  (with-redefs [xia.llm/fetch-provider-model-metadata
                (fn [{:keys [base-url api-key model]}]
                  (is (= "https://api.example.com/v1" base-url))
                  (is (= "sk-test" api-key))
                  (is (= "gpt-4o" model))
                  {:id "gpt-4o"
                   :vision? true
                   :vision-source :metadata})]
    (let [response (#'http/router {:uri            "/admin/provider-model-metadata"
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"base_url" "https://api.example.com/v1"
                                                                  "api_key" "sk-test"
                                                                  "model" "gpt-4o"})})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= "gpt-4o" (get-in body ["model" "id"])))
      (is (= true (get-in body ["model" "vision"])))
      (is (= "metadata" (get-in body ["model" "vision_source"]))))))

(deftest admin-provider-model-metadata-route-returns-context-window-and-recommended-budgets
  (with-redefs [xia.llm/fetch-provider-model-metadata
                (fn [_]
                  {:id "gpt-5"
                   :vision? true
                   :vision-source :metadata
                   :context-window 128000
                   :context-window-source :metadata
                   :recommended-system-prompt-budget 24000
                   :recommended-history-budget 72000
                   :recommended-input-budget-cap 96000})]
    (let [response (#'http/router {:uri            "/admin/provider-model-metadata"
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"base_url" "https://api.example.com/v1"
                                                                  "api_key" "sk-test"
                                                                  "model" "gpt-5"})})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= 128000 (get-in body ["model" "context_window"])))
      (is (= "metadata" (get-in body ["model" "context_window_source"])))
      (is (= 24000 (get-in body ["model" "recommended_system_prompt_budget"])))
      (is (= 72000 (get-in body ["model" "recommended_history_budget"])))
      (is (= 96000 (get-in body ["model" "recommended_input_budget_cap"]))))))

(deftest admin-provider-models-route-uses-saved-provider-api-key
  (db/upsert-provider! {:id                :claude
                        :name              "Claude"
                        :base-url          "https://api.anthropic.com/v1"
                        :api-key           "sk-stored"
                        :model             "claude-sonnet-4-5"
                        :credential-source :api-key
                        :auth-type         :api-key})
  (with-redefs [xia.llm/fetch-provider-models
                (fn [{:keys [base-url api-key auth-header]}]
                  (is (= "https://api.anthropic.com/v1" base-url))
                  (is (= "sk-stored" api-key))
                  (is (nil? auth-header))
                  ["claude-haiku-4-5" "claude-sonnet-4-5"])]
    (let [response (#'http/router {:uri            "/admin/provider-models"
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"provider_id" "claude"})})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= ["claude-haiku-4-5" "claude-sonnet-4-5"]
             (get body "models"))))))

(deftest admin-provider-model-metadata-route-uses-linked-oauth-account
  (db/register-oauth-account! {:id            :qwen-login
                               :name          "Qwen Login"
                               :authorize-url "https://example.com/oauth/authorize"
                               :token-url     "https://example.com/oauth/token"
                               :client-id     "client-id"
                               :access-token  "oauth-token"
                               :token-type    "Bearer"})
  (db/upsert-provider! {:id                :qwen
                        :name              "Qwen"
                        :base-url          "https://dashscope.aliyuncs.com/compatible-mode/v1"
                        :model             "qwen-max"
                        :credential-source :oauth-account
                        :auth-type         :oauth-account
                        :oauth-account     :qwen-login})
  (with-redefs [xia.oauth/ensure-account-ready!
                (constantly {:oauth.account/access-token "oauth-token"
                             :oauth.account/token-type "Bearer"})
                xia.oauth/oauth-header
                (constantly "Bearer oauth-token")
                xia.llm/fetch-provider-model-metadata
                (fn [{:keys [base-url api-key auth-header model]}]
                  (is (= "https://dashscope.aliyuncs.com/compatible-mode/v1" base-url))
                  (is (nil? api-key))
                  (is (= "Bearer oauth-token" auth-header))
                  (is (= "qwen-max" model))
                  {:id "qwen-max"
                   :vision? false
                   :vision-source :metadata})]
    (let [response (#'http/router {:uri            "/admin/provider-model-metadata"
                                   :request-method :post
                                   :headers        (ui-headers)
                                   :body           (request-body {"provider_id" "qwen"
                                                                  "model" "qwen-max"})})
          body     (response-json response)]
      (is (= 200 (:status response)))
      (is (= "qwen-max" (get-in body ["model" "id"])))
      (is (= false (get-in body ["model" "vision"])))
      (is (= "metadata" (get-in body ["model" "vision_source"]))))))

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
                        :rate-limit-per-minute 90
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
                                                                "rate_limit_per_minute" ""
                                                                "api_key" ""
                                                                "default" true})})
        provider (db/get-provider :openai)]
    (is (= 200 (:status response)))
    (is (empty? (or (:llm.provider/workloads provider) [])))
    (is (nil? (:llm.provider/system-prompt-budget provider)))
    (is (nil? (:llm.provider/history-budget provider)))
    (is (nil? (:llm.provider/rate-limit-per-minute provider)))))

(deftest admin-provider-route-reuses-api-key-from-another-provider
  (db/upsert-provider! {:id                :openrouter-primary
                        :name              "OpenRouter Primary"
                        :template          :openrouter
                        :base-url          "https://openrouter.ai/api/v1"
                        :model             "openai/gpt-4.1"
                        :credential-source :api-key
                        :auth-type         :api-key
                        :api-key           "openrouter-key"})
  (let [response (#'http/router {:uri            "/admin/providers"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"id" "openrouter-gemini"
                                                                "name" "OpenRouter Gemini"
                                                                "template" "openrouter"
                                                                "base_url" "https://openrouter.ai/api/v1"
                                                                "model" "google/gemini-2.5-pro"
                                                                "credential_source" "api-key"
                                                                "reuse_api_key_provider_id" "openrouter-primary"})})
        body     (response-json response)
        provider (db/get-provider :openrouter-gemini)]
    (is (= 200 (:status response)))
    (is (= "openrouter-gemini" (get-in body ["provider" "id"])))
    (is (= "google/gemini-2.5-pro" (get-in body ["provider" "model"])))
    (is (= "openrouter-key" (:llm.provider/api-key provider)))))

(deftest admin-provider-route-links-oauth-account
  (db/register-oauth-account! {:id            :openai-login
                               :name          "OpenAI Login"
                               :authorize-url "https://example.com/oauth/authorize"
                               :token-url     "https://example.com/oauth/token"
                               :client-id     "client-id"
                               :client-secret "client-secret"
                               :access-token  "access-token"})
  (let [response (#'http/router {:uri            "/admin/providers"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"id" "openai"
                                                                "name" "OpenAI"
                                                                "template" "custom"
                                                                "base_url" "https://api.openai.com/v1"
                                                                "model" "gpt-5"
                                                                "access_mode" "account"
                                                                "credential_source" "oauth-account"
                                                                "oauth_account" "openai-login"
                                                                "default" true})})
        body     (response-json response)
        provider (db/get-provider :openai)]
    (is (= 200 (:status response)))
    (is (= "custom" (get-in body ["provider" "template"])))
    (is (= "api" (get-in body ["provider" "access_mode"])))
    (is (= "oauth-account" (get-in body ["provider" "credential_source"])))
    (is (= "oauth-account" (get-in body ["provider" "auth_type"])))
    (is (= "openai-login" (get-in body ["provider" "oauth_account"])))
    (is (= "OpenAI Login" (get-in body ["provider" "oauth_account_name"])))
    (is (= :custom (:llm.provider/template provider)))
    (is (= :api (:llm.provider/access-mode provider)))
    (is (= :oauth-account (:llm.provider/credential-source provider)))
    (is (= :oauth-account (:llm.provider/auth-type provider)))
    (is (= :openai-login (:llm.provider/oauth-account provider)))))

(deftest admin-provider-route-rejects-browser-session-field
  (let [response (#'http/router {:uri            "/admin/providers"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"id" "openai-account"
                                                                "name" "OpenAI Account"
                                                                "template" "openai"
                                                                "base_url" "https://api.openai.com/v1"
                                                                "model" "gpt-5"
                                                                "access_mode" "api"
                                                                "credential_source" "api-key"
                                                                "api_key" "openai-key"
                                                                "browser_session" "browser-session-1"
                                                                "default" true})})
        body     (response-json response)]
    (is (= 400 (:status response)))
    (is (str/includes? (or (get body "error") "")
                       "browser_session is no longer supported"))))

(deftest admin-provider-account-connector-routes-removed
  (let [start-response (#'http/router {:uri            "/admin/provider-account-connectors/openai-browser/start"
                                       :request-method :post
                                       :headers        (ui-headers)})
        complete-response (#'http/router {:uri            "/admin/provider-account-connectors/openai-browser/complete"
                                          :request-method :post
                                          :headers        (ui-headers)
                                          :body           (request-body {"browser_session" "browser-session-1"})})]
    (is (= 404 (:status start-response)))
    (is (= 404 (:status complete-response)))))

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
      (is (= 18790 (http/current-port)))
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

(deftest start-falls-forward-when-port-is-taken
  (let [attempts (atom [])]
    (with-redefs [org.httpkit.server/run-server
                  (fn [_handler opts]
                    (swap! attempts conj opts)
                    (if (= 18790 (:port opts))
                      (throw (BindException. "Address already in use"))
                      (fn [] nil)))]
      (http/start! 18790)
      (is (= [18790 18791] (mapv :port @attempts)))
      (is (= 18791 (http/current-port)))
      (http/stop!))))
