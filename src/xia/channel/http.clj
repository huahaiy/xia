(ns xia.channel.http
  "HTTP/WebSocket channel — enables remote clients and web UIs."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [org.httpkit.server :as http]
            [charred.api :as json]
            [clojure.tools.logging :as log]
            [xia.scratch :as scratch]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.oauth :as oauth]
            [xia.oauth-template :as oauth-template]
            [xia.service :as service-proxy]
            [xia.schedule :as schedule]
            [xia.agent :as agent]
            [xia.prompt :as prompt]
            [xia.working-memory :as wm])
  (:import [java.security SecureRandom]
           [java.util Base64]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defonce ^:private server-atom (atom nil))
(defonce ^:private ws-sessions (atom {})) ; channel → session-id
(defonce ^:private pending-approvals (atom {})) ; session-id string → approval map
(defonce ^:private session-statuses (atom {})) ; session-id string → latest status map

(def ^:private local-hosts #{"localhost" "127.0.0.1" "::1" "[::1]"})
(def ^:private approval-timeout-ms (* 5 60 1000))
(def ^:private local-session-cookie-name "xia-local-session")
(def ^:private service-auth-types #{:bearer :basic :api-key-header :query-param :oauth-account})
(defonce ^:private local-session-secret
  (delay
    (let [bytes (byte-array 32)
          _     (.nextBytes (SecureRandom.) bytes)]
      (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bytes))))

;; ---------------------------------------------------------------------------
;; Local web UI
;; ---------------------------------------------------------------------------

(def ^:private read-resource
  (memoize
    (fn [path]
      (some-> (str "web/" path)
              io/resource
              slurp))))

(defn- resource-response [path content-type]
  (if-let [content (read-resource path)]
    {:status  200
     :headers {"Content-Type" content-type}
     :body    content}
    {:status 404 :body "Not Found"}))

;; ---------------------------------------------------------------------------
;; WebSocket handler
;; ---------------------------------------------------------------------------

(defn- ws-handler [req]
  (http/as-channel req
    {:on-open
     (fn [ch]
       (let [sid (db/create-session! :websocket)]
         (swap! ws-sessions assoc ch sid)
         (wm/ensure-wm! sid)
         (log/info "WebSocket connected, session:" sid)
         (http/send! ch (json/write-json-str {:type "connected" :session-id (str sid)}))))

     :on-receive
     (fn [ch msg]
       (if-let [sid (get @ws-sessions ch)]
         (try
           (let [data     (json/read-json msg)
                 text     (get data "message" (get data "content" msg))
                 response (agent/process-message sid text :channel :websocket)]
             (http/send! ch (json/write-json-str {:type    "message"
                                                   :role    "assistant"
                                                   :content response})))
           (catch Exception e
             (log/error e "WebSocket message error")
             (http/send! ch (json/write-json-str {:type  "error"
                                                   :error (.getMessage e)}))))
         (http/send! ch (json/write-json-str {:type "error" :error "Session not found"}))))

     :on-close
     (fn [ch _status]
       (when-let [sid (get @ws-sessions ch)]
         (wm/snapshot! sid)
         (wm/clear-wm! sid))
       (swap! ws-sessions dissoc ch)
       (log/info "WebSocket disconnected"))}))

;; ---------------------------------------------------------------------------
;; REST endpoints
;; ---------------------------------------------------------------------------

(defn- read-body [req]
  (when-let [body (:body req)]
    (json/read-json (slurp body))))

(defn- request-header
  [req header-name]
  (let [target (str/lower-case header-name)]
    (or (get-in req [:headers header-name])
        (get-in req [:headers target])
        (some (fn [[k v]]
                (when (= target (str/lower-case (str k)))
                  v))
              (:headers req)))))

(declare nonblank-str parse-session-id session-exists?)

(defn- first-forwarded
  [value]
  (some-> value str (str/split #",") first str/trim nonblank-str))

(defn- request-base-url
  [req]
  (or (when-let [origin (nonblank-str (request-header req "origin"))]
        (let [uri (java.net.URI. origin)]
          (str (.getScheme uri) "://" (.getAuthority uri))))
      (let [scheme (or (first-forwarded (request-header req "x-forwarded-proto"))
                       (some-> (:scheme req) name)
                       "http")
            host   (or (first-forwarded (request-header req "x-forwarded-host"))
                       (nonblank-str (request-header req "host")))]
        (when host
          (str scheme "://" host)))))

(defn- parse-query-string
  [query-string]
  (into {}
        (keep (fn [part]
                (let [[k v] (str/split (str part) #"=" 2)]
                  (when (seq k)
                    [(java.net.URLDecoder/decode k "UTF-8")
                     (some-> v (java.net.URLDecoder/decode "UTF-8"))]))))
        (str/split (or query-string "") #"&")))

(defn- session-secret []
  @local-session-secret)

(defn- session-cookie-value []
  (str local-session-cookie-name "=" (session-secret)))

(defn- session-cookie-header []
  (str (session-cookie-value) "; Path=/; HttpOnly; SameSite=Strict"))

(defn- cookie-map
  [req]
  (let [cookie-header (request-header req "cookie")]
    (into {}
          (keep (fn [part]
                  (let [[k v] (str/split (str/trim part) #"=" 2)]
                    (when (and (seq k) (some? v))
                      [k v]))))
          (str/split (or cookie-header "") #";"))))

(defn- local-origin?
  [origin]
  (try
    (let [host (.getHost (java.net.URI. origin))]
      (contains? local-hosts host))
    (catch Exception _
      false)))

(defn- valid-session-secret?
  [req]
  (= (get (cookie-map req) local-session-cookie-name)
     (session-secret)))

(defn- trusted-local-origin?
  "Allow loopback browser origins and direct local clients with no origin
   headers. Origin checks prevent cross-site requests from using the cookie."
  [req]
  (let [origin  (request-header req "origin")
        referer (request-header req "referer")]
    (cond
      (seq origin)  (local-origin? origin)
      (seq referer) (local-origin? referer)
      :else         true)))

(defn- trusted-browser-request?
  "Stateful local UI/API routes require both a local browser origin (when
   present) and the per-process session secret cookie."
  [req]
  (and (trusted-local-origin? req)
       (valid-session-secret? req)))

(defn- json-response [status body]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/write-json-str body)})

(defn- html-response [body]
  {:status  200
   :headers {"Content-Type" "text/html; charset=utf-8"}
   :body    body})

(defn- escape-html
  [value]
  (-> (str (or value ""))
      (str/replace "&" "&amp;")
      (str/replace "<" "&lt;")
      (str/replace ">" "&gt;")
      (str/replace "\"" "&quot;")
      (str/replace "'" "&#39;")))

(defn- forbidden-response []
  (json-response 403 {:error "forbidden origin"}))

(defn- unauthorized-response []
  (json-response 401 {:error "missing or invalid local session secret"}))

(defn- protected-route-response
  [req allowed-fn]
  (cond
    (not (trusted-local-origin? req))
    (forbidden-response)

    (not (valid-session-secret? req))
    (unauthorized-response)

    :else
    (allowed-fn)))

(defn- approval->body
  [{:keys [approval-id tool-id tool-name description arguments reason policy created-at]}]
  {:approval_id approval-id
   :tool_id     (name tool-id)
   :tool_name   tool-name
   :description description
   :arguments   arguments
   :reason      reason
   :policy      (name policy)
   :created_at  (some-> created-at .toInstant str)})

(defn- clear-pending-approval!
  [session-id approval-id]
  (swap! pending-approvals
         (fn [pending]
           (let [current (get pending session-id)]
             (if (= approval-id (:approval-id current))
               (dissoc pending session-id)
               pending)))))

(defn- http-approval-handler
  [{:keys [session-id tool-id tool-name description arguments reason policy]}]
  (let [sid (some-> session-id str)]
    (when-not sid
      (throw (ex-info "HTTP approval requires a session id"
                      {:tool-id tool-id})))
    (let [approval-id (str (random-uuid))
          decision    (promise)
          approval    {:approval-id approval-id
                       :tool-id     tool-id
                       :tool-name   (or tool-name (name tool-id))
                       :description description
                       :arguments   arguments
                       :reason      reason
                       :policy      policy
                       :created-at  (java.util.Date.)
                       :decision    decision}]
      (swap! pending-approvals assoc sid approval)
      (try
        (let [result (deref decision approval-timeout-ms ::timeout)]
          (case result
            :allow true
            :deny  false
            (throw (ex-info "Timed out waiting for tool approval"
                            {:tool-id tool-id
                             :session-id sid}))))
        (finally
          (clear-pending-approval! sid approval-id))))))

(defn- parse-session-id
  [session-id]
  (try
    (let [uuid (java.util.UUID/fromString session-id)]
      (str uuid))
    (catch IllegalArgumentException _
      nil)))

(defn- session-exists?
  [session-id]
  (when-let [sid (parse-session-id session-id)]
    (boolean
      (ffirst (db/q '[:find ?e :in $ ?sid
                      :where
                      [?e :session/id ?sid]]
                    (java.util.UUID/fromString sid))))))

(defn- instant->str [value]
  (some-> value .toInstant str))

(def ^:private history-session-channels #{:http :websocket :terminal})

(defn- truncate-text
  [value limit]
  (let [text (some-> value str str/trim)]
    (when (seq text)
      (if (> (count text) limit)
        (str (subs text 0 (max 0 (- limit 1))) "…")
        text))))

(defn- history-run->body
  [run]
  {:id          (some-> (:id run) str)
   :schedule_id (some-> (:schedule-id run) name)
   :started_at  (instant->str (:started-at run))
   :finished_at (instant->str (:finished-at run))
   :status      (some-> (:status run) name)
   :actions     (:actions run)
   :result      (:result run)
   :error       (:error run)})

(defn- history-schedule->body
  [sched]
  (let [latest-run (first (schedule/schedule-history (:id sched) 1))]
    {:id            (some-> (:id sched) name)
     :name          (:name sched)
     :type          (some-> (:type sched) name)
     :trusted       (boolean (:trusted? sched))
     :enabled       (boolean (:enabled? sched))
     :last_run      (instant->str (:last-run sched))
     :next_run      (instant->str (:next-run sched))
     :latest_status (some-> (:status latest-run) name)
     :latest_error  (truncate-text (:error latest-run) 160)}))

(defn- history-session->body
  [session]
  (let [messages     (->> (db/session-messages (:id session))
                          (filter #(#{:user :assistant} (:role %)))
                          vec)
        last-message (last messages)]
    {:id              (some-> (:id session) str)
     :channel         (some-> (:channel session) name)
     :created_at      (instant->str (:created-at session))
     :active          (boolean (:active? session))
     :message_count   (count messages)
     :last_message_at (instant->str (:created-at last-message))
     :preview         (truncate-text (:content last-message) 160)}))

(defn- status->body
  [status]
  (when status
    {:state      (some-> (:state status) name)
     :phase      (some-> (:phase status) name)
     :message    (:message status)
     :tool_id    (some-> (:tool-id status) name)
     :tool_name  (:tool-name status)
     :round      (:round status)
     :tool_count (:tool-count status)
     :updated_at (instant->str (:updated-at status))}))

(defn- clear-session-status!
  [session-id]
  (when session-id
    (swap! session-statuses dissoc (str session-id))))

(defn- http-status-handler
  [{:keys [session-id state] :as status}]
  (when-let [sid (some-> session-id str)]
    (if (= :done state)
      (clear-session-status! sid)
      (swap! session-statuses assoc sid (assoc status :updated-at (java.util.Date.))))))

(defn- scratch-pad->body
  [pad]
  {:id         (:id pad)
   :scope      (name (:scope pad))
   :session_id (:session-id pad)
   :title      (:title pad)
   :content    (:content pad)
   :mime       (:mime pad)
   :version    (:version pad)
   :created_at (instant->str (:created-at pad))
   :updated_at (instant->str (:updated-at pad))})

(defn- scratch-metadata->body
  [pad]
  (dissoc (scratch-pad->body pad) :content))

(defn- nonblank-str
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn- parse-keyword-id
  [value field-name]
  (let [id-str (nonblank-str value)]
    (cond
      (nil? id-str)
      (throw (ex-info (str "missing '" field-name "' field") {:field field-name}))

      (re-find #"\s" id-str)
      (throw (ex-info (str "'" field-name "' must not contain whitespace")
                      {:field field-name
                       :value value}))

      :else
      (keyword id-str))))

(defn- parse-optional-positive-long
  [value field-name]
  (let [text (nonblank-str value)]
    (when text
      (try
        (let [parsed (Long/parseLong text)]
          (when-not (pos? parsed)
            (throw (ex-info (str "'" field-name "' must be a positive integer")
                            {:field field-name
                             :value value})))
          parsed)
        (catch NumberFormatException _
          (throw (ex-info (str "'" field-name "' must be a positive integer")
                          {:field field-name
                           :value value})))))))

(defn- parse-provider-workloads
  [value]
  (let [entries (cond
                  (nil? value) []
                  (sequential? value) value
                  :else (str/split (str value) #","))]
    (->> entries
         (map nonblank-str)
         (remove nil?)
         distinct
         (mapv (fn [entry]
                 (let [workload (keyword entry)]
                   (when-not (llm/known-workload? workload)
                     (throw (ex-info "invalid workload"
                                     {:field "workloads"
                                      :value entry})))
                   workload))))))

(defn- parse-extra-fields
  [value]
  (let [text (nonblank-str value)]
    (when text
      (try
        (json/write-json-str (json/read-json text))
        (catch Exception _
          (throw (ex-info "extra_fields must be valid JSON"
                          {:field "extra_fields"})))))))

(defn- parse-json-object-string
  [value field-name]
  (let [text (nonblank-str value)]
    (when text
      (try
        (let [parsed (json/read-json text)]
          (when-not (map? parsed)
            (throw (ex-info (str field-name " must be a JSON object")
                            {:field field-name})))
          (json/write-json-str parsed))
        (catch clojure.lang.ExceptionInfo e
          (throw e))
        (catch Exception _
          (throw (ex-info (str field-name " must be valid JSON")
                          {:field field-name})))))))

(defn- parse-auth-type
  [value]
  (let [auth-type (some-> value nonblank-str keyword)]
    (when-not (contains? service-auth-types auth-type)
      (throw (ex-info "invalid auth_type"
                      {:field "auth_type"
                       :value value})))
    auth-type))

(defn- sort-by-name
  [entries]
  (->> entries
       (sort-by (fn [entry]
                  (str/lower-case (or (:name entry) (:id entry) ""))))
       vec))

(defn- provider->admin-body
  [provider]
  (let [provider-id (some-> (:llm.provider/id provider) name)
        health      (llm/provider-health-summary (:llm.provider/id provider))]
    {:id                    provider-id
     :name                  (:llm.provider/name provider)
     :base_url              (:llm.provider/base-url provider)
     :model                 (:llm.provider/model provider)
     :workloads             (->> (:llm.provider/workloads provider)
                                 (map name)
                                 sort
                                 vec)
     :system_prompt_budget  (:llm.provider/system-prompt-budget provider)
     :history_budget        (:llm.provider/history-budget provider)
     :health_status         (name (:status health))
     :health_failures       (:consecutive-failures health)
     :health_cooldown_ms    (:cooldown-remaining-ms health)
     :health_last_error     (:last-error health)
     :default               (boolean (:llm.provider/default? provider))
     :api_key_configured    (boolean (nonblank-str (:llm.provider/api-key provider)))}))

(defn- service->admin-body
  [service]
  (let [oauth-account (some-> (:service/oauth-account service) db/get-oauth-account)]
    {:id                     (some-> (:service/id service) name)
     :name                   (:service/name service)
     :base_url               (:service/base-url service)
     :auth_type              (some-> (:service/auth-type service) name)
     :auth_header            (:service/auth-header service)
     :oauth_account          (some-> (:service/oauth-account service) name)
     :oauth_account_name     (:oauth.account/name oauth-account)
     :oauth_account_connected (boolean (nonblank-str (:oauth.account/access-token oauth-account)))
     :oauth_account_autonomous_approved (boolean (and oauth-account
                                                     (autonomous/oauth-account-autonomous-approved? oauth-account)))
     :rate_limit_per_minute  (:service/rate-limit-per-minute service)
     :effective_rate_limit_per_minute (service-proxy/effective-rate-limit-per-minute service)
     :autonomous_approved    (boolean (autonomous/service-autonomous-approved? service))
     :enabled                (boolean (:service/enabled? service))
     :auth_key_configured    (boolean (nonblank-str (:service/auth-key service)))}))

(defn- oauth-account->admin-body
  [account]
  {:id                       (some-> (:oauth.account/id account) name)
   :name                     (:oauth.account/name account)
   :authorize_url            (:oauth.account/authorize-url account)
   :token_url                (:oauth.account/token-url account)
   :client_id                (:oauth.account/client-id account)
   :provider_template        (some-> (:oauth.account/provider-template account) name)
   :scopes                   (:oauth.account/scopes account)
   :redirect_uri             (:oauth.account/redirect-uri account)
   :auth_params              (:oauth.account/auth-params account)
   :token_params             (:oauth.account/token-params account)
   :client_secret_configured (boolean (nonblank-str (:oauth.account/client-secret account)))
   :access_token_configured  (boolean (nonblank-str (:oauth.account/access-token account)))
   :refresh_token_configured (boolean (nonblank-str (:oauth.account/refresh-token account)))
   :autonomous_approved      (boolean (autonomous/oauth-account-autonomous-approved? account))
   :connected                (boolean (nonblank-str (:oauth.account/access-token account)))
   :expires_at               (instant->str (:oauth.account/expires-at account))
   :connected_at             (instant->str (:oauth.account/connected-at account))})

(defn- oauth-template->admin-body
  [template]
  {:id            (some-> (:id template) name)
   :name          (:name template)
   :description   (:description template)
   :authorize_url (:authorize-url template)
   :token_url     (:token-url template)
   :api_base_url  (:api-base-url template)
   :service_id    (:service-id template)
   :service_name  (:service-name template)
   :scopes        (:scopes template)
   :auth_params   (json/write-json-str (or (:auth-params template) {}))
   :token_params  (json/write-json-str (or (:token-params template) {}))
   :notes         (:notes template)})

(defn- site->admin-body
  [site]
  {:id                  (some-> (:site-cred/id site) name)
   :name                (:site-cred/name site)
   :login_url           (:site-cred/login-url site)
   :username_field      (:site-cred/username-field site)
   :password_field      (:site-cred/password-field site)
   :form_selector       (:site-cred/form-selector site)
   :extra_fields        (:site-cred/extra-fields site)
   :autonomous_approved (boolean (autonomous/site-autonomous-approved? site))
   :username_configured (boolean (nonblank-str (:site-cred/username site)))
   :password_configured (boolean (nonblank-str (:site-cred/password site)))})

(defn- tool->admin-body
  [tool]
  {:id          (some-> (:tool/id tool) name)
   :name        (:tool/name tool)
   :description (:tool/description tool)
   :approval    (some-> (:tool/approval tool) name)
   :enabled     (boolean (:tool/enabled? tool))})

(defn- skill->admin-body
  [skill]
  {:id          (some-> (:skill/id skill) name)
   :name        (:skill/name skill)
   :description (:skill/description skill)
   :version     (:skill/version skill)
   :enabled     (boolean (:skill/enabled? skill))})

(defn- session-scratch-pad
  [session-id pad-id]
  (let [pad (scratch/get-pad pad-id)]
    (when (and pad
               (= :session (:scope pad))
               (= session-id (:session-id pad)))
      pad)))

(defn- handle-create-session []
  (let [sid (db/create-session! :http)]
    (wm/ensure-wm! sid)
    (json-response 200 {:session_id (str sid)})))

(defn- handle-chat [req]
  (let [data       (read-body req)
        message    (get data "message")
        session-id (get data "session_id")]
    (if-not message
      (json-response 400 {:error "missing 'message' field"})
      (if (and session-id (not (session-exists? session-id)))
        (json-response 404 {:error "unknown session id"})
        (let [sid      (if session-id
                         (java.util.UUID/fromString session-id)
                         (db/create-session! :http))
              _wm      (wm/ensure-wm! sid)
              _status  (clear-session-status! sid)
              response (agent/process-message sid message :channel :http)]
          (json-response 200 {:session_id (str sid)
                              :role       "assistant"
                              :content    response}))))))

(defn- handle-get-status [session-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (json-response 200 {:session_id session-id
                        :status     (status->body (get @session-statuses session-id))})))

(defn- handle-get-approval [session-id]
  (if-not (parse-session-id session-id)
    (json-response 400 {:error "invalid session id"})
    (if-let [approval (get @pending-approvals session-id)]
      (json-response 200 {:pending true
                          :approval (approval->body approval)})
      (json-response 200 {:pending false}))))

(defn- handle-submit-approval [session-id req]
  (if-not (parse-session-id session-id)
    (json-response 400 {:error "invalid session id"})
    (let [data        (read-body req)
          approval-id (get data "approval_id")
          decision    (get data "decision")
          current     (get @pending-approvals session-id)
          decision*   (case decision
                        "allow" :allow
                        "deny"  :deny
                        nil)]
      (cond
        (nil? current)
        (json-response 404 {:error "no pending approval"})

        (not= approval-id (:approval-id current))
        (json-response 409 {:error "stale approval id"})

        (nil? decision*)
        (json-response 400 {:error "invalid decision"})

        :else
        (do
          (deliver (:decision current) decision*)
          (clear-pending-approval! session-id approval-id)
          (json-response 200 {:status "recorded"}))))))

(defn- handle-session-messages [session-id]
  (try
    (let [sid      (java.util.UUID/fromString session-id)
          messages (->> (db/session-messages sid)
                        (filter #(#{:user :assistant} (:role %)))
                        (mapv (fn [{:keys [role content created-at]}]
                                {:role       (name role)
                                 :content    content
                                 :created_at (some-> created-at .toInstant str)})))]
      (json-response 200 {:session_id session-id
                          :messages   messages}))
    (catch IllegalArgumentException _
      (json-response 400 {:error "invalid session id"}))))

(defn- handle-history-sessions []
  (json-response 200
                 {:sessions (->> (db/list-sessions)
                                 (filter #(contains? history-session-channels (:channel %)))
                                 (mapv history-session->body))}))

(defn- handle-history-schedules []
  (json-response 200
                 {:schedules (->> (schedule/list-schedules)
                                  (sort-by (fn [sched]
                                             (or (some-> (:last-run sched) .getTime)
                                                 (some-> (:next-run sched) .getTime)
                                                 Long/MIN_VALUE))
                                           >)
                                  (mapv history-schedule->body))}))

(defn- handle-history-schedule-runs
  [schedule-id]
  (try
    (let [sid   (parse-keyword-id schedule-id "schedule_id")
          sched (schedule/get-schedule sid)]
      (if-not sched
        (json-response 404 {:error "schedule not found"})
        (json-response 200
                       {:schedule (history-schedule->body sched)
                        :runs     (mapv history-run->body
                                        (schedule/schedule-history sid 20))})))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-list-scratch-pads [session-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (json-response 200
                   {:session_id session-id
                    :pads       (mapv scratch-metadata->body
                                      (scratch/list-pads {:scope :session
                                                          :session-id session-id}))})))

(defn- handle-create-scratch-pad [session-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (let [data (or (read-body req) {})
          pad  (scratch/create-pad! {:scope      :session
                                     :session-id session-id
                                     :title      (get data "title")
                                     :content    (get data "content")
                                     :mime       (get data "mime")})]
      (json-response 201 {:session_id session-id
                          :pad        (scratch-pad->body pad)}))))

(defn- handle-get-scratch-pad [session-id pad-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    :else
    (if-let [pad (session-scratch-pad session-id pad-id)]
      (json-response 200 {:session_id session-id
                          :pad        (scratch-pad->body pad)})
      (json-response 404 {:error "scratch pad not found"}))))

(defn- handle-save-scratch-pad [session-id pad-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response 404 {:error "scratch pad not found"})

    :else
    (let [data    (or (read-body req) {})
          updates (cond-> {}
                    (contains? data "title")            (assoc :title (get data "title"))
                    (contains? data "content")          (assoc :content (get data "content"))
                    (contains? data "mime")             (assoc :mime (get data "mime"))
                    (contains? data "expected_version") (assoc :expected-version
                                                               (get data "expected_version")))]
      (try
        (json-response 200
                       {:session_id session-id
                        :pad        (scratch-pad->body (scratch/save-pad! pad-id updates))})
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type]} (ex-data e)]
            (case type
              :scratch/version-conflict
              (json-response 409 {:error "scratch pad version conflict"
                                  :details (select-keys (ex-data e)
                                                        [:expected-version :actual-version])})
              :scratch/not-found
              (json-response 404 {:error "scratch pad not found"})
              (json-response 400 {:error (.getMessage e)}))))))))

(defn- handle-edit-scratch-pad [session-id pad-id req]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response 404 {:error "scratch pad not found"})

    :else
    (let [data      (or (read-body req) {})
          operation (if (map? (get data "operation"))
                      (get data "operation")
                      data)
          edit      (cond-> {:op (get operation "op")}
                      (contains? operation "text")          (assoc :text (get operation "text"))
                      (contains? operation "separator")     (assoc :separator (get operation "separator"))
                      (contains? operation "match")         (assoc :match (get operation "match"))
                      (contains? operation "replacement")   (assoc :replacement (get operation "replacement"))
                      (contains? operation "occurrence")    (assoc :occurrence (get operation "occurrence"))
                      (contains? operation "offset")        (assoc :offset (get operation "offset"))
                      (contains? operation "start_line")    (assoc :start-line (get operation "start_line"))
                      (contains? operation "end_line")      (assoc :end-line (get operation "end_line"))
                      (contains? data "expected_version")   (assoc :expected-version
                                                                   (get data "expected_version"))
                      (contains? operation "expected_version") (assoc :expected-version
                                                                     (get operation "expected_version")))]
      (try
        (json-response 200
                       {:session_id session-id
                        :pad        (scratch-pad->body (scratch/edit-pad! pad-id edit))})
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type]} (ex-data e)]
            (case type
              :scratch/version-conflict
              (json-response 409 {:error "scratch pad version conflict"
                                  :details (select-keys (ex-data e)
                                                        [:expected-version :actual-version])})
              :scratch/not-found
              (json-response 404 {:error "scratch pad not found"})
              (json-response 400 {:error (.getMessage e)}))))))))

(defn- handle-delete-scratch-pad [session-id pad-id]
  (cond
    (nil? (parse-session-id session-id))
    (json-response 400 {:error "invalid session id"})

    (not (session-exists? session-id))
    (json-response 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response 404 {:error "scratch pad not found"})

    :else
    (do
      (scratch/delete-pad! pad-id)
      (json-response 200 {:status "deleted"
                          :session_id session-id
                          :pad_id pad-id}))))

(defn- handle-admin-config [_req]
  (json-response
    200
    {:providers (->> (db/list-providers)
                     (map provider->admin-body)
                     sort-by-name)
     :llm_workloads (mapv (fn [{:keys [id label description]}]
                            {:id          (name id)
                             :label       label
                             :description description})
                          (llm/workload-routes))
     :oauth_provider_templates (->> (oauth-template/list-templates)
                                    (map oauth-template->admin-body)
                                    sort-by-name)
     :oauth_accounts (->> (db/list-oauth-accounts)
                          (map oauth-account->admin-body)
                          sort-by-name)
     :services  (->> (db/list-services)
                     (map service->admin-body)
                     sort-by-name)
     :sites     (->> (db/list-site-creds)
                     (map site->admin-body)
                     sort-by-name)
     :tools     (->> (db/list-tools)
                     (map tool->admin-body)
                     sort-by-name)
     :skills    (->> (db/list-skills)
                     (map skill->admin-body)
                     sort-by-name)}))

(defn- handle-save-provider [req]
  (try
    (let [data         (or (read-body req) {})
          provider-id  (parse-keyword-id (get data "id") "id")
          base-url     (nonblank-str (get data "base_url"))
          model        (nonblank-str (get data "model"))
          name         (or (nonblank-str (get data "name"))
                           (name provider-id))
          api-key      (nonblank-str (get data "api_key"))
          workloads    (when (contains? data "workloads")
                         (parse-provider-workloads (get data "workloads")))
          system-prompt-budget (parse-optional-positive-long (get data "system_prompt_budget")
                                                             "system_prompt_budget")
          history-budget (parse-optional-positive-long (get data "history_budget")
                                                       "history_budget")
          make-default (true? (get data "default"))
          has-default? (some? (db/get-default-provider))]
      (when-not base-url
        (throw (ex-info "missing 'base_url' field" {:field "base_url"})))
      (when-not model
        (throw (ex-info "missing 'model' field" {:field "model"})))
      (db/upsert-provider! (cond-> {:id       provider-id
                                    :name     name
                                    :base-url base-url
                                    :model    model
                                    :system-prompt-budget system-prompt-budget
                                    :history-budget history-budget}
                             (contains? data "workloads")
                             (assoc :workloads workloads)
                             api-key
                             (assoc :api-key api-key)))
      (when (or make-default (not has-default?))
        (db/set-default-provider! provider-id))
      (json-response 200 {:provider (provider->admin-body (db/get-provider provider-id))}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-save-service [req]
  (try
    (let [data              (or (read-body req) {})
          service-id        (parse-keyword-id (get data "id") "id")
          existing          (db/get-service service-id)
          base-url          (nonblank-str (get data "base_url"))
          name              (or (nonblank-str (get data "name"))
                                (name service-id))
          auth-type         (parse-auth-type (get data "auth_type"))
          entered-auth-key  (nonblank-str (get data "auth_key"))
          rate-limit-per-minute (parse-optional-positive-long (get data "rate_limit_per_minute")
                                                              "rate_limit_per_minute")
          autonomous-approved? (when (contains? data "autonomous_approved")
                                 (true? (get data "autonomous_approved")))
          enabled?          (if (contains? data "enabled")
                              (true? (get data "enabled"))
                              true)
          oauth-account-id  (when (= :oauth-account auth-type)
                              (let [value (or (nonblank-str (get data "oauth_account"))
                                              (some-> (:service/oauth-account existing) name))]
                                (when-not value
                                  (throw (ex-info "oauth_account is required for oauth-account auth_type"
                                                  {:field "oauth_account"})))
                                (let [account-id (keyword value)]
                                  (when-not (db/get-oauth-account account-id)
                                    (throw (ex-info "unknown oauth_account"
                                                    {:field "oauth_account"
                                                     :value value})))
                                  account-id)))
          entered-header    (nonblank-str (get data "auth_header"))
          auth-header       (when (#{:api-key-header :query-param} auth-type)
                              (or entered-header
                                  (:service/auth-header existing)))
          auth-key          (when-not (= :oauth-account auth-type)
                              (or entered-auth-key
                                  (:service/auth-key existing)
                                  ""))]
      (when-not base-url
        (throw (ex-info "missing 'base_url' field" {:field "base_url"})))
      (when (and (#{:api-key-header :query-param} auth-type)
                 (nil? auth-header))
        (throw (ex-info "auth_header is required for the selected auth_type"
                        {:field "auth_header"})))
      (db/save-service! {:id          service-id
                         :name        name
                         :base-url    base-url
                         :auth-type   auth-type
                         :auth-key    (or auth-key "")
                         :auth-header auth-header
                         :oauth-account oauth-account-id
                         :rate-limit-per-minute rate-limit-per-minute
                         :autonomous-approved? autonomous-approved?
                         :enabled?    enabled?})
      (json-response 200 {:service (service->admin-body (db/get-service service-id))}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-save-oauth-account [req]
  (try
    (let [data           (or (read-body req) {})
          account-id     (parse-keyword-id (get data "id") "id")
          existing       (db/get-oauth-account account-id)
          name           (or (nonblank-str (get data "name"))
                             (name account-id))
          authorize-url  (nonblank-str (get data "authorize_url"))
          token-url      (nonblank-str (get data "token_url"))
          client-id      (nonblank-str (get data "client_id"))
          client-secret  (or (nonblank-str (get data "client_secret"))
                             (:oauth.account/client-secret existing)
                             "")
          provider-template-id (if (contains? data "provider_template")
                                 (some-> (get data "provider_template") nonblank-str keyword)
                                 (:oauth.account/provider-template existing))
          scopes         (or (nonblank-str (get data "scopes")) "")
          redirect-uri   (nonblank-str (get data "redirect_uri"))
          auth-params    (parse-json-object-string (get data "auth_params") "auth_params")
          token-params   (parse-json-object-string (get data "token_params") "token_params")
          autonomous-approved? (when (contains? data "autonomous_approved")
                                 (true? (get data "autonomous_approved")))]
      (when-not authorize-url
        (throw (ex-info "missing 'authorize_url' field" {:field "authorize_url"})))
      (when-not token-url
        (throw (ex-info "missing 'token_url' field" {:field "token_url"})))
      (when-not client-id
        (throw (ex-info "missing 'client_id' field" {:field "client_id"})))
      (when (and provider-template-id
                 (nil? (oauth-template/get-template provider-template-id)))
        (throw (ex-info "unknown provider_template"
                        {:field "provider_template"
                         :value (name provider-template-id)})))
      (db/save-oauth-account! {:id            account-id
                               :name          name
                               :authorize-url authorize-url
                               :token-url     token-url
                               :client-id     client-id
                               :client-secret client-secret
                               :provider-template provider-template-id
                               :scopes        scopes
                               :redirect-uri  redirect-uri
                               :auth-params   auth-params
                               :token-params  token-params
                               :autonomous-approved? autonomous-approved?
                               :access-token  (:oauth.account/access-token existing)
                               :refresh-token (:oauth.account/refresh-token existing)
                               :token-type    (:oauth.account/token-type existing)
                               :expires-at    (:oauth.account/expires-at existing)
                               :connected-at  (:oauth.account/connected-at existing)})
      (json-response 200 {:oauth_account (oauth-account->admin-body
                                           (db/get-oauth-account account-id))}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-delete-oauth-account [account-id]
  (try
    (let [oauth-id (parse-keyword-id account-id "oauth_account_id")]
      (cond
        (nil? (db/get-oauth-account oauth-id))
        (json-response 404 {:error "oauth account not found"})

        (db/oauth-account-in-use? oauth-id)
        (json-response 409 {:error "oauth account is still referenced by a service"})

        :else
        (do
          (db/remove-oauth-account! oauth-id)
          (json-response 200 {:status "deleted"
                              :oauth_account_id (name oauth-id)}))))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-start-oauth-connect [account-id req]
  (try
    (let [oauth-id    (parse-keyword-id account-id "oauth_account_id")
          callback-url (str (or (request-base-url req)
                                (throw (ex-info "cannot determine callback base URL"
                                                {:field "host"})))
                            "/oauth/callback")
          started     (oauth/start-authorization! oauth-id callback-url)]
      (json-response 200 {:oauth_account_id (name oauth-id)
                          :authorization_url (:authorization-url started)
                          :redirect_uri (:redirect-uri started)}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-refresh-oauth-account [account-id]
  (try
    (let [oauth-id (parse-keyword-id account-id "oauth_account_id")
          account  (oauth/refresh-account! oauth-id)]
      (json-response 200 {:oauth_account (oauth-account->admin-body account)}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- oauth-callback-page
  [status title message account-id]
  (let [title*   (escape-html title)
        message* (escape-html message)
        account* (some-> account-id name escape-html)]
    (str "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">"
       "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
       "<title>Xia OAuth</title>"
       "<style>body{margin:0;font-family:\"Avenir Next\",\"Segoe UI\",sans-serif;background:#f5efe3;color:#172119;display:grid;place-items:center;min-height:100vh;padding:24px;}main{max-width:36rem;background:rgba(255,252,246,.96);border:1px solid rgba(23,33,25,.12);border-radius:24px;padding:28px;box-shadow:0 20px 50px rgba(23,33,25,.12);}h1{margin:0 0 12px;font-size:2rem;}p{line-height:1.6;margin:0 0 10px;}code{font-family:\"SFMono-Regular\",Consolas,monospace;background:rgba(23,33,25,.06);padding:2px 6px;border-radius:8px;}</style>"
       "</head><body><main><h1>" title* "</h1><p>" message* "</p>"
       (when account*
         (str "<p>OAuth account: <code>" account* "</code></p>"))
       "<p>You can close this window and return to Xia.</p>"
       "<script>"
       "try {"
       "  if (window.opener && window.opener !== window) {"
       "    window.opener.postMessage({type:'xia-oauth-complete', status:" (json/write-json-str (name status)) ", account_id:" (json/write-json-str (some-> account-id name)) "}, window.location.origin);"
       "  }"
       "} catch (_err) {}"
       "setTimeout(() => { try { window.close(); } catch (_err) {} }, 1200);"
       "</script></main></body></html>")))

(defn- handle-oauth-callback [req]
  (let [params            (parse-query-string (:query-string req))
        state             (get params "state")
        pending-account-id (some-> (and (seq state) (oauth/callback-account-id state)) name)
        code              (get params "code")
        error-code        (get params "error")
        error-description (or (get params "error_description") error-code)]
    (cond
      (not (seq state))
      (html-response (oauth-callback-page "error"
                                          "OAuth failed"
                                          "Missing authorization state."
                                          nil))

      (seq error-code)
      (html-response (oauth-callback-page "error"
                                          "OAuth was not completed"
                                          (str "Provider returned: " error-description)
                                          pending-account-id))

      (not (seq code))
      (html-response (oauth-callback-page "error"
                                          "OAuth failed"
                                          "Missing authorization code."
                                          pending-account-id))

      :else
      (try
        (let [account (oauth/complete-authorization! state code)]
          (html-response (oauth-callback-page "ok"
                                              "OAuth connected"
                                              "Xia stored the new access token and can now use this account for online work."
                                              (some-> (:oauth.account/id account) name))))
        (catch clojure.lang.ExceptionInfo e
          (html-response (oauth-callback-page "error"
                                              "OAuth failed"
                                              (.getMessage e)
                                              pending-account-id)))))))

(defn- handle-save-site [req]
  (try
    (let [data            (or (read-body req) {})
          site-id         (parse-keyword-id (get data "id") "id")
          existing        (db/get-site-cred site-id)
          login-url       (nonblank-str (get data "login_url"))
          name            (or (nonblank-str (get data "name"))
                              (name site-id))
          username-field  (or (nonblank-str (get data "username_field"))
                              "username")
          password-field  (or (nonblank-str (get data "password_field"))
                              "password")
          username        (or (nonblank-str (get data "username"))
                              (:site-cred/username existing)
                              "")
          password        (or (nonblank-str (get data "password"))
                              (:site-cred/password existing)
                              "")
          form-selector   (nonblank-str (get data "form_selector"))
          extra-fields    (parse-extra-fields (get data "extra_fields"))
          autonomous-approved? (when (contains? data "autonomous_approved")
                                 (true? (get data "autonomous_approved")))]
      (when-not login-url
        (throw (ex-info "missing 'login_url' field" {:field "login_url"})))
      (db/save-site-cred! {:id             site-id
                           :name           name
                           :login-url      login-url
                           :username-field username-field
                           :password-field password-field
                           :username       username
                           :password       password
                           :form-selector  form-selector
                           :extra-fields   extra-fields
                           :autonomous-approved? autonomous-approved?})
      (json-response 200 {:site (site->admin-body (db/get-site-cred site-id))}))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-delete-site [site-id]
  (try
    (let [site-key (parse-keyword-id site-id "site_id")]
      (if (db/get-site-cred site-key)
        (do
          (db/remove-site-cred! site-key)
          (json-response 200 {:status "deleted"
                              :site_id (name site-key)}))
        (json-response 404 {:error "site credential not found"})))
    (catch clojure.lang.ExceptionInfo e
      (json-response 400 {:error (.getMessage e)
                          :details (ex-data e)}))))

(defn- handle-skills [_req]
  (json-response 200 {:skills (mapv (fn [s]
                                      {:id          (name (:skill/id s))
                                       :name        (:skill/name s)
                                       :description (:skill/description s)
                                       :tags        (mapv name (or (:skill/tags s) []))
                                       :enabled     (:skill/enabled? s)})
                                    (db/list-skills))}))

(defn- handle-health [_req]
  (json-response 200 {:status "ok" :version "0.1.0"}))

(defn- handle-home [_req]
  (if-let [html (read-resource "index.html")]
    (assoc-in (html-response html)
              [:headers "Set-Cookie"]
              (session-cookie-header))
    {:status 404 :body "Not Found"}))

;; ---------------------------------------------------------------------------
;; Router
;; ---------------------------------------------------------------------------

(defn- router [req]
  (let [uri    (:uri req)
        method (:request-method req)
        session-match      (re-matches #"/sessions/([0-9a-fA-F-]+)/messages" uri)
        status-match       (re-matches #"/sessions/([0-9a-fA-F-]+)/status" uri)
        approval-match     (re-matches #"/sessions/([0-9a-fA-F-]+)/approval" uri)
        history-schedule-match (re-matches #"/history/schedules/([^/]+)/runs" uri)
        scratch-list-match (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads" uri)
        scratch-pad-match  (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads/([^/]+)" uri)
        scratch-edit-match (re-matches #"/sessions/([0-9a-fA-F-]+)/scratch-pads/([^/]+)/edit" uri)
        admin-site-match   (re-matches #"/admin/sites/([^/]+)" uri)
        admin-oauth-match  (re-matches #"/admin/oauth-accounts/([^/]+)" uri)
        admin-oauth-connect-match (re-matches #"/admin/oauth-accounts/([^/]+)/connect" uri)
        admin-oauth-refresh-match (re-matches #"/admin/oauth-accounts/([^/]+)/refresh" uri)]
    (cond
      (and (= method :get) (= uri "/"))
      (handle-home req)

      (and (= method :get) (= uri "/style.css"))
      (resource-response "style.css" "text/css")

      (and (= method :get) (= uri "/app.js"))
      (resource-response "app.js" "text/javascript")

      (and (= method :get) (= uri "/oauth/callback"))
      (handle-oauth-callback req)

      ;; WebSocket upgrade
      (and (= uri "/ws")
           (http/websocket-handshake-check req)
           (trusted-browser-request? req))
      (ws-handler req)

      (and (= uri "/ws") (http/websocket-handshake-check req))
      (if (trusted-local-origin? req)
        (unauthorized-response)
        (forbidden-response))

      ;; REST
      (and (= method :post) (= uri "/sessions"))
      (protected-route-response req handle-create-session)

      (and (= method :post) (= uri "/chat"))
      (protected-route-response req #(handle-chat req))

      (and (= method :get) status-match)
      (protected-route-response req #(handle-get-status (second status-match)))

      (and (= method :get) approval-match)
      (protected-route-response req #(handle-get-approval (second approval-match)))

      (and (= method :post) approval-match)
      (protected-route-response req #(handle-submit-approval (second approval-match) req))

      (and (= method :get) session-match)
      (protected-route-response req #(handle-session-messages (second session-match)))

      (and (= method :get) (= uri "/history/sessions"))
      (protected-route-response req handle-history-sessions)

      (and (= method :get) (= uri "/history/schedules"))
      (protected-route-response req handle-history-schedules)

      (and (= method :get) history-schedule-match)
      (protected-route-response req #(handle-history-schedule-runs (second history-schedule-match)))

      (and (= method :get) scratch-list-match)
      (protected-route-response req #(handle-list-scratch-pads (second scratch-list-match)))

      (and (= method :post) scratch-list-match)
      (protected-route-response req #(handle-create-scratch-pad (second scratch-list-match) req))

      (and (= method :get) scratch-pad-match)
      (protected-route-response req #(handle-get-scratch-pad (second scratch-pad-match)
                                                             (nth scratch-pad-match 2)))

      (and (= method :put) scratch-pad-match)
      (protected-route-response req #(handle-save-scratch-pad (second scratch-pad-match)
                                                              (nth scratch-pad-match 2)
                                                              req))

      (and (= method :delete) scratch-pad-match)
      (protected-route-response req #(handle-delete-scratch-pad (second scratch-pad-match)
                                                                (nth scratch-pad-match 2)))

      (and (= method :post) scratch-edit-match)
      (protected-route-response req #(handle-edit-scratch-pad (second scratch-edit-match)
                                                              (nth scratch-edit-match 2)
                                                              req))

      (and (= method :get) (= uri "/admin/config"))
      (protected-route-response req #(handle-admin-config req))

      (and (= method :post) (= uri "/admin/providers"))
      (protected-route-response req #(handle-save-provider req))

      (and (= method :post) (= uri "/admin/oauth-accounts"))
      (protected-route-response req #(handle-save-oauth-account req))

      (and (= method :post) admin-oauth-connect-match)
      (protected-route-response req #(handle-start-oauth-connect (second admin-oauth-connect-match) req))

      (and (= method :post) admin-oauth-refresh-match)
      (protected-route-response req #(handle-refresh-oauth-account (second admin-oauth-refresh-match)))

      (and (= method :delete) admin-oauth-match)
      (protected-route-response req #(handle-delete-oauth-account (second admin-oauth-match)))

      (and (= method :post) (= uri "/admin/services"))
      (protected-route-response req #(handle-save-service req))

      (and (= method :post) (= uri "/admin/sites"))
      (protected-route-response req #(handle-save-site req))

      (and (= method :delete) admin-site-match)
      (protected-route-response req #(handle-delete-site (second admin-site-match)))

      (and (= method :get) (= uri "/skills"))
      (protected-route-response req #(handle-skills req))

      (and (= method :get) (= uri "/health"))
      (handle-health req)

      :else
      (json-response 404 {:error "not found"}))))

;; ---------------------------------------------------------------------------
;; Server lifecycle
;; ---------------------------------------------------------------------------

(defn start!
  "Start the HTTP/WebSocket server.
   Defaults to loopback-only binding."
  ([port]
   (start! "127.0.0.1" port))
  ([bind-host port]
   (when @server-atom
     (log/warn "Server already running"))
   (prompt/register-approval! :http http-approval-handler)
   (prompt/register-approval! :websocket http-approval-handler)
   (prompt/register-status! :http http-status-handler)
   (prompt/register-status! :websocket http-status-handler)
   (let [s (http/run-server router {:ip bind-host :port port})]
     (reset! server-atom s)
     (log/info "HTTP/WebSocket server started on" bind-host ":" port)
     s)))

(defn stop! []
  (when-let [s @server-atom]
    (s) ; http-kit stop fn
    (prompt/register-approval! :http nil)
    (prompt/register-approval! :websocket nil)
    (prompt/register-status! :http nil)
    (prompt/register-status! :websocket nil)
    (reset! pending-approvals {})
    (reset! session-statuses {})
    (reset! server-atom nil)
    (log/info "Server stopped")))
