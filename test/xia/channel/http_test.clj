(ns xia.channel.http-test
  (:require [charred.api :as json]
            [clojure.test :refer :all]
            [clojure.string :as str]
            [xia.agent]
            [xia.channel.http :as http]
            [xia.db :as db]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.io ByteArrayInputStream]
           [java.nio.charset StandardCharsets]
           [java.util UUID]))

(use-fixtures :each with-test-db)

(defn- request-body [payload]
  (ByteArrayInputStream.
    (.getBytes (json/write-json-str payload) StandardCharsets/UTF_8)))

(defn- response-json [response]
  (json/read-json (:body response)))

(defn- local-session-cookie []
  (let [response (#'http/router {:uri "/" :request-method :get})]
    (first (str/split (get-in response [:headers "Set-Cookie"]) #";"))))

(defn- ui-headers []
  {"origin" "http://localhost:18790"
   "cookie" (local-session-cookie)})

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
    (is (re-find #"Notes" (:body response)))
    (is (re-find #"Settings" (:body response)))
    (is (re-find #"AI Models" (:body response)))
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
    (is (re-find #"href=\"style.css\"" (:body response)))))

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
      (is (re-find #"sessionStorage\.getItem" (:body response))))))

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
                                 :headers        {"origin" "http://localhost:18790"}
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

(deftest chat-route-validates-required-message
  (let [response (#'http/router {:uri            "/chat"
                                 :request-method :post
                                 :headers        (ui-headers)
                                 :body           (request-body {"session_id" (str (random-uuid))})})
        body     (response-json response)]
    (is (= 400 (:status response)))
    (is (= "missing 'message' field" (get body "error")))))

(deftest session-messages-route-returns-transcript
  (let [sid (db/create-session! :http)]
    (db/add-message! sid :user "hello")
    (db/add-message! sid :assistant "hi there")
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
      (is (= "assistant" (get (second messages) "role")))
      (is (= "hi there" (get (second messages) "content"))))))

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
                                 :headers        {"origin" "http://localhost:18790"}})
        body     (response-json response)]
    (is (= 401 (:status response)))
    (is (= "missing or invalid local session secret" (get body "error")))))

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
                         :rate-limit-per-minute 90})
  (db/register-oauth-account! {:id            :google
                               :name          "Google"
                               :authorize-url "https://accounts.google.com/o/oauth2/v2/auth"
                               :token-url     "https://oauth2.googleapis.com/token"
                               :client-id     "client-id"
                               :client-secret "client-secret"
                               :access-token  "access-123"
                               :refresh-token "refresh-123"})
  (db/register-site-cred! {:id             :github
                           :name           "GitHub login"
                           :login-url      "https://github.com/login"
                           :username-field "login"
                           :password-field "password"
                           :username       "hyang"
                           :password       "pw-secret"
                           :form-selector  "#login"
                           :extra-fields   "{\"remember_me\":\"1\"}"})
  (db/install-tool! {:id          :demo-tool
                     :name        "Demo tool"
                     :description "desc"
                     :parameters  "{}"
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
    (is (= "healthy" (get provider "health_status")))
    (is (= #{"assistant" "history-compaction" "topic-summary" "memory-summary" "memory-extraction"}
           (set (map #(get % "id") llm-workloads))))
    (is (= #{"github" "google" "microsoft"}
           (set (map #(get % "id") templates))))
    (is (= "{\"access_type\":\"offline\",\"prompt\":\"consent\"}"
           (get (first (filter #(= "google" (get % "id")) templates)) "auth_params")))
    (is (= "GitHub API"
           (get (first (filter #(= "github" (get % "id")) templates)) "service_name")))
    (is (= true (get oauth "client_secret_configured")))
    (is (= true (get oauth "access_token_configured")))
    (is (not (contains? oauth "client_secret")))
    (is (not (contains? oauth "access_token")))
    (is (= true (get service "auth_key_configured")))
    (is (not (contains? service "auth_key")))
    (is (= "api-key-header" (get service "auth_type")))
    (is (= 90 (get service "rate_limit_per_minute")))
    (is (= 90 (get service "effective_rate_limit_per_minute")))
    (is (= true (get site "username_configured")))
    (is (= true (get site "password_configured")))
    (is (not (contains? site "username")))
    (is (not (contains? site "password")))
    (is (= "session" (get tool "approval")))
    (is (= true (get tool "enabled")))
    (is (= true (get skill "enabled")))))

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
                                                                     "token_params" ""})})
        save-body     (response-json save-response)
        account       (db/get-oauth-account :google)]
    (is (= 200 (:status save-response)))
    (is (= "Google Workspace" (get-in save-body ["oauth_account" "name"])))
    (is (= "google" (get-in save-body ["oauth_account" "provider_template"])))
    (is (= "client-secret" (:oauth.account/client-secret account)))
    (is (= :google (:oauth.account/provider-template account)))
    (is (= "{\"access_type\":\"offline\"}" (:oauth.account/auth-params account))))
  (with-redefs [xia.oauth/start-authorization!
                (fn [account-id callback-url]
                  (is (= :google account-id))
                  (is (= "http://localhost:18790/oauth/callback" callback-url))
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
    (is (= ["assistant" "history-compaction"] (get-in body ["provider" "workloads"])))
    (is (= 16000 (get-in body ["provider" "system_prompt_budget"])))
    (is (= 32000 (get-in body ["provider" "history_budget"])))
    (is (= "openai-key" (:llm.provider/api-key (db/get-provider :openai))))
    (is (= #{:assistant :history-compaction}
           (set (:llm.provider/workloads (db/get-provider :openai)))))
    (is (= 16000 (:llm.provider/system-prompt-budget (db/get-provider :openai))))
    (is (= 32000 (:llm.provider/history-budget (db/get-provider :openai))))
    (is (= true (:llm.provider/default? (db/get-provider :openai))))
    (is (= false (:llm.provider/default? (db/get-provider :anthropic))))))

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
                                                                "enabled" false})})
        service  (db/get-service :github)]
    (is (= 200 (:status response)))
    (is (= :bearer (:service/auth-type service)))
    (is (= "gh-secret" (:service/auth-key service)))
    (is (nil? (:service/auth-header service)))
    (is (= 120 (:service/rate-limit-per-minute service)))
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
                                                                     "extra_fields" ""})})
        save-body     (response-json save-response)
        site          (db/get-site-cred :github)]
    (is (= 200 (:status save-response)))
    (is (= true (get-in save-body ["site" "username_configured"])))
    (is (= true (get-in save-body ["site" "password_configured"])))
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
