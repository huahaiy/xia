(ns xia.db-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.db :as db]
            [xia.runtime-state :as runtime-state]
            [xia.test-helpers :as th]))

(use-fixtures :each th/with-test-db)

(deftest default-datalevin-opts-use-xia-managed-nomic-provider
  (let [opts        (db/default-datalevin-opts)
        provider-id (get-in opts [:embedding-opts :provider])
        provider    (get-in opts [:embedding-providers provider-id])
        domains     (:embedding-domains opts)]
    (is (= :llama.cpp provider-id))
    (is (= :llama.cpp (:provider provider)))
    (is (= "nomic-embed-text-v2-moe-q8_0.gguf" (:model-filename provider)))
    (is (not (contains? provider :embedding-metadata)))
    (is (= #{db/episode-text-domain
             db/kg-node-domain
             db/kg-fact-domain
             db/local-doc-domain
             db/local-doc-chunk-domain
             db/artifact-domain}
           (set (keys domains))))
    (is (every? #(= :llama.cpp (:provider %))
                (vals domains)))
    (is (false? (:wal? opts)))
    (is (true? (:validate-data? opts)))
    (is (true? (:auto-entity-time? opts)))))

(deftest datalevin-type-coercion-and-auto-entity-time-are-enabled
  (let [session-id (str (random-uuid))]
    (db/transact! [{:session/id      session-id
                    :session/channel "terminal"
                    :session/active? true}])
    (let [session-eid (ffirst (db/q '[:find ?e :in $ ?sid :where [?e :session/id ?sid]]
                                    (java.util.UUID/fromString session-id)))
          entity      (db/entity session-eid)
          session     (first (db/list-sessions))]
      (is (uuid? (:session/id entity)))
      (is (= :terminal (:session/channel entity)))
      (is (integer? (:db/created-at entity)))
      (is (instance? java.util.Date (:created-at session))))))

(deftest set-session-active-overwrites-the-current-flag
  (let [sid            (db/create-session! :http)
        session-active (fn []
                         (:active? (some #(when (= sid (:id %)) %)
                                         (db/list-sessions))))]
    (is (true? (session-active)))
    (db/set-session-active! sid false)
    (is (false? (session-active)))
    (db/set-session-active! sid true)
    (is (true? (session-active)))))

(deftest sessions-persist-external-channel-metadata
  (let [external-key "slack:T1:C1:main"
        external-meta {:team-id "T1"
                       :channel-id "C1"}
        sid (db/create-session! :slack {:label "Slack C1"
                                        :external-key external-key
                                        :external-meta external-meta})
        session (db/find-session-by-external-key external-key)]
    (is (= sid (:id session)))
    (is (= :slack (:channel session)))
    (is (= "Slack C1" (:label session)))
    (is (= external-key (:external-key session)))
    (is (= external-meta (:external-meta session)))
    (is (= external-meta (db/session-external-meta sid)))
    (is (true? (db/save-session-external-meta! sid {:team-id "T1"
                                                    :channel-id "C1"
                                                    :thread-ts "1710.1"})))
      (is (= {:team-id "T1"
            :channel-id "C1"
            :thread-ts "1710.1"}
           (db/session-external-meta sid)))))

(deftest sessions-can-link-durable-user-profiles
  (let [profile-id (db/ensure-user-profile! {:key "telegram-user:55"
                                             :name "Alex"
                                             :summary "Prefers concise answers"
                                             :preferences {:tone :concise}})
        sid        (db/create-session! :telegram {:external-key "telegram:1001:main"
                                                  :user-profile-id profile-id
                                                  :external-meta {:chat-id 1001}})
        session    (db/find-session-by-external-key "telegram:1001:main")]
    (is (= profile-id (:id (db/get-user-profile profile-id))))
    (is (= profile-id (:id (:user-profile session))))
    (is (= "Alex" (get-in session [:user-profile :name])))
    (is (= {:tone :concise}
           (get-in session [:user-profile :preferences])))
    (is (= profile-id (:id (db/session-user-profile sid))))
    (let [updated-id (db/ensure-user-profile! {:key "telegram-user:55"
                                               :summary "Updated profile summary"})]
      (is (= profile-id updated-id))
      (is (= "Updated profile summary"
             (:summary (db/get-user-profile profile-id)))))))

(deftest tasks-persist-durable-contract-separately-from-runtime-meta
  (let [session-id (db/create-session! :terminal)
        contract   {:goal "Reply to the billing emails"
                    :constraints ["Use the approved tone" "Include invoice link"]
                    :deliverables ["Final reply draft"]}
        task-id     (db/create-task! {:session-id session-id
                                      :channel :terminal
                                      :type :interactive
                                      :state :running
                                      :title "Reply to the billing emails"
                                      :summary "Working the billing inbox"
                                      :contract contract
                                      :meta {:runtime {:phase :planning}}})
        task        (db/get-task task-id)]
    (is (= contract (:contract task)))
    (is (= {:runtime {:phase :planning}} (:meta task)))
    (db/update-task! task-id {:contract {:goal "Reply to the billing emails"
                                         :constraints ["Use the approved tone"]}})
    (is (= {:goal "Reply to the billing emails"
            :constraints ["Use the approved tone"]}
           (:contract (db/get-task task-id))))))

(deftest tasks-persist-turns-and-items
  (let [session-id (db/create-session! :terminal)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :terminal
                                     :type :interactive
                                     :state :running
                                     :title "Reply to the billing emails"
                                     :summary "Working the billing inbox"
                                     :autonomy-state {:stack [{:title "Reply to the billing emails"
                                                               :progress-status :in-progress}]}})
        turn-id    (db/start-task-turn! task-id
                                        {:operation :start
                                         :state :running
                                         :input "reply to the billing emails"
                                         :summary "Started working the inbox"})
        item-id    (db/add-task-item! turn-id
                                      {:type :user-message
                                       :role :user
                                       :summary "reply to the billing emails"
                                       :data {:text "reply to the billing emails"}})]
    (db/update-task-turn! turn-id {:state :completed
                                   :summary "Replied to the billing thread"})
    (db/update-task! task-id {:state :completed
                              :summary "Done"
                              :finished-at (java.util.Date.)})
    (let [task   (db/get-task task-id)
          tasks  (db/list-tasks {:session-id session-id})
          turns  (db/task-turns task-id)
          items  (db/turn-items turn-id)]
      (is (= task-id (:id task)))
      (is (= session-id (:session-id task)))
      (is (= :completed (:state task)))
      (is (= "Reply to the billing emails" (:title task)))
      (is (= 1 (count tasks)))
      (is (= [turn-id] (mapv :id turns)))
      (is (= [:completed] (mapv :state turns)))
      (is (= [:start] (mapv :operation turns)))
      (is (= [item-id] (mapv :id items)))
      (is (= [:user-message] (mapv :type items)))
      (is (= [{:text "reply to the billing emails"}]
             (mapv :data items))))))

(deftest tasks-can-span-multiple-sessions-while-keeping-a-current-session
  (let [sid-a   (db/create-session! :terminal)
        sid-b   (db/create-session! :http)
        task-id (db/create-task! {:session-id sid-a
                                  :channel :terminal
                                  :type :interactive
                                  :state :running
                                  :title "Cross-session task"})]
    (db/update-task! task-id {:session-id sid-b
                              :session-role :resumed
                              :channel :http})
    (let [task    (db/get-task task-id)
          links   (db/task-session-links task-id)
          tasks-a (db/list-tasks {:session-id sid-a})
          tasks-b (db/list-tasks {:session-id sid-b})]
      (is (= sid-b (:session-id task)))
      (is (= #{sid-a sid-b}
             (set (map :session-id links))))
      (is (= #{:origin :resumed}
             (set (map :role links))))
      (is (= [task-id] (mapv :id tasks-a)))
      (is (= [task-id] (mapv :id tasks-b)))
      (is (= task-id (:id (db/current-session-task sid-a))))
      (is (= task-id (:id (db/current-session-task sid-b)))))))

(deftest current-session-task-prefers-actionable-work-over-recent-completed-task
  (let [sid            (db/create-session! :terminal)
        paused-task-id (db/create-task! {:session-id sid
                                         :channel :terminal
                                         :type :interactive
                                         :state :paused
                                         :title "Paused task"})
        _completed-id  (db/create-task! {:session-id sid
                                         :channel :terminal
                                         :type :interactive
                                         :state :completed
                                         :title "Completed task"})]
    (is (= paused-task-id
           (:id (db/current-session-task sid))))))

(deftest current-session-task-prefers-live-runtime-state-over-stale-durable-state
  (let [sid              (db/create-session! :terminal)
        live-running-id  (db/create-task! {:session-id sid
                                           :channel :terminal
                                           :type :interactive
                                           :state :completed
                                           :title "Live task"
                                           :meta {:runtime {:state :running}}})
        _paused-task-id  (db/create-task! {:session-id sid
                                           :channel :terminal
                                           :type :interactive
                                           :state :paused
                                           :title "Paused task"})]
    (is (= live-running-id
           (:id (db/current-session-task sid))))))

(deftest close-records-debug-event-when-runtime-is-running
  (runtime-state/mark-running!)
  (try
    (db/close!)
    (let [event (db/last-close-event)]
      (is (= :running (:phase event)))
      (is (string? (:db-path event)))
      (is (seq (:callsite event))))
    (finally
      (runtime-state/mark-stopped!))))

(deftest connect-prefetches-managed-embedding-model-before-opening-db
  (let [path      (str (java.nio.file.Files/createTempDirectory
                         "xia-db-connect"
                         (make-array java.nio.file.attribute.FileAttribute 0)))
        calls     (atom [])
        output    (with-out-str
                    (with-redefs-fn {#'xia.db/download-file!
                                     (fn [url target-path]
                                       (swap! calls conj [:download url target-path])
                                       (io/make-parents target-path)
                                       (spit target-path "test-model")
                                       target-path)
                                     #'xia.paths/managed-embed-dir
                                     (fn [_db-path]
                                       (str path "/embed-cache"))
                                     #'datalevin.core/get-conn
                                     (fn [_db-path _schema _opts]
                                       (swap! calls conj :get-conn)
                                       ::conn)
                                     #'xia.crypto/configure! (fn [& _] nil)
                                     #'xia.db/init-embedding-provider! (fn [& _] nil)
                                     #'xia.db/init-llm-provider! (fn [& _] nil)
                                     #'datalevin.core/close (fn [_] nil)}
                      #(try
                         (db/connect! path {:local-llm-provider false
                                            :passphrase-provider (constantly "xia-test-passphrase")})
                         (finally
                           (db/close!)))))]
    (is (= [:download :get-conn]
           (mapv #(if (vector? %) (first %) %) @calls)))
    (is (.contains ^String output "Downloading Xia embedding model"))))

(deftest connect-does-not-run-secret-migration-on-startup
  (let [called? (atom false)]
    (with-redefs-fn {#'datalevin.core/get-conn (fn [_db-path _schema _opts] ::conn)
                     #'xia.db/download-file! (fn [& _] nil)
                     #'xia.crypto/configure! (fn [& _] nil)
                     #'xia.db/init-embedding-provider! (fn [& _] nil)
                     #'xia.db/init-llm-provider! (fn [& _] nil)
                     #'xia.db/migrate-secrets! (fn []
                                                (reset! called? true)
                                                0)
                     #'datalevin.core/close (fn [_] nil)}
      #(try
         (db/connect! "/tmp/xia-dev-connect"
                      {:local-llm-provider false
                       :passphrase-provider (constantly "xia-test-passphrase")})
         (finally
           (db/close!))))
    (is (false? @called?))))

(deftest connect-disables-datalevin-wal-by-default
  (let [conn-opts (atom nil)]
    (with-redefs-fn {#'datalevin.core/get-conn (fn [_db-path _schema opts]
                                                 (reset! conn-opts opts)
                                                 ::conn)
                     #'xia.db/download-file! (fn [& _] nil)
                     #'xia.crypto/configure! (fn [& _] nil)
                     #'xia.db/init-embedding-provider! (fn [& _] nil)
                     #'xia.db/init-llm-provider! (fn [& _] nil)
                     #'datalevin.core/close (fn [_] nil)}
      #(try
         (db/connect! "/tmp/xia-dev-connect"
                      {:local-llm-provider false
                       :passphrase-provider (constantly "xia-test-passphrase")})
         (finally
           (db/close!))))
    (is (false? (:wal? @conn-opts)))))

(deftest seed-initial-settings-from-db-copies-template-config
  (let [target-path (db/current-db-path)
        source-path (str (java.nio.file.Files/createTempDirectory
                           "xia-template-source"
                           (make-array java.nio.file.attribute.FileAttribute 0)))
        connect-opts (th/test-connect-options
                       {:passphrase-provider (constantly "xia-test-passphrase")})]
    (db/close!)
    (db/connect! source-path connect-opts)
    (try
      (db/set-identity! :name "Base Xia")
      (db/set-identity! :role "Ops assistant")
      (db/set-identity! :description "Handles shared operations setup.")
      (db/set-config! :user/name "Huahai")
      (db/set-config! :web/search-backend "brave")
      (db/set-config! :web/search-brave-api-key "brave-secret")
      (db/set-config! :local-doc/ocr-enabled? true)
      (db/register-oauth-account! {:id            :shared-oauth
                                   :name          "Shared OAuth"
                                   :authorize-url "https://example.com/oauth/authorize"
                                   :token-url     "https://example.com/oauth/token"
                                   :client-id     "client-id"
                                   :client-secret "client-secret"
                                   :access-token  "access-token"})
      (db/upsert-provider! {:id                :openai
                            :name              "OpenAI"
                            :base-url          "https://api.openai.com/v1"
                            :api-key           "openai-secret"
                            :model             "gpt-5"
                            :template          :openai
                            :credential-source :api-key
                            :workloads         #{:assistant :memory-summary}
                            :default?          true})
      (db/save-service! {:id       :github
                         :name     "GitHub"
                         :base-url "https://api.github.com"
                         :auth-type :bearer
                         :auth-key "github-secret"})
      (db/save-site-cred! {:id             :portal
                           :name           "Portal"
                           :login-url      "https://portal.example/login"
                           :username-field "email"
                           :password-field "password"
                           :username       "ops@example.com"
                           :password       "site-secret"})
      (finally
        (db/close!)))
    (db/connect! target-path connect-opts)
    (let [result (db/seed-initial-settings-from-db! {:source-db-path source-path
                                                     :crypto-opts connect-opts})
          provider (db/get-provider :openai)
          service  (db/get-service :github)
          site     (db/get-site-cred :portal)
          account  (db/get-oauth-account :shared-oauth)]
      (is (true? (:seeded? result)))
      (is (= 1 (:provider-count result)))
      (is (= 1 (:service-count result)))
      (is (= 1 (:site-count result)))
      (is (= 1 (:oauth-account-count result)))
      (is (= 0 (:skipped-secret-count result)))
      (is (= "Base Xia" (db/get-identity :name)))
      (is (= "Ops assistant" (db/get-identity :role)))
      (is (= "Huahai" (db/get-config :user/name)))
      (is (= "brave" (db/get-config :web/search-backend)))
      (is (= "brave-secret" (db/get-config :web/search-brave-api-key)))
      (is (= "true" (db/get-config :setup/complete)))
      (is (= "https://api.openai.com/v1" (:llm.provider/base-url provider)))
      (is (= "openai-secret" (:llm.provider/api-key provider)))
      (is (= "gpt-5" (:llm.provider/model provider)))
      (is (= :openai (:llm.provider/template provider)))
      (is (= :api-key (:llm.provider/credential-source provider)))
      (is (= #{:assistant :memory-summary} (:llm.provider/workloads provider)))
      (is (= :openai (:llm.provider/id (db/get-default-provider))))
      (is (= "https://api.github.com" (:service/base-url service)))
      (is (= "github-secret" (:service/auth-key service)))
      (is (= "https://portal.example/login" (:site-cred/login-url site)))
      (is (= "ops@example.com" (:site-cred/username site)))
      (is (= "site-secret" (:site-cred/password site)))
      (is (= "client-secret" (:oauth.account/client-secret account)))
      (is (= "access-token" (:oauth.account/access-token account))))))

(deftest providers-persist-vision-capability
  (db/upsert-provider! {:id       :openai
                        :name     "OpenAI"
                        :base-url "https://api.openai.com/v1"
                        :model    "gpt-4o"
                        :vision?  true})
  (is (true? (:llm.provider/vision? (db/get-provider :openai))))
  (db/upsert-provider! {:id       :openai
                        :name     "OpenAI"
                        :base-url "https://api.openai.com/v1"
                        :model    "gpt-4o-mini"
                        :vision?  false})
  (is (false? (:llm.provider/vision? (db/get-provider :openai)))))

(deftest providers-persist-rate-limit-and-can-clear-it
  (db/upsert-provider! {:id                    :openai
                        :name                  "OpenAI"
                        :base-url              "https://api.openai.com/v1"
                        :model                 "gpt-4o"
                        :rate-limit-per-minute 45})
  (is (= 45 (:llm.provider/rate-limit-per-minute (db/get-provider :openai))))
  (db/upsert-provider! {:id                    :openai
                        :name                  "OpenAI"
                        :base-url              "https://api.openai.com/v1"
                        :model                 "gpt-4o-mini"
                        :rate-limit-per-minute nil})
  (is (nil? (:llm.provider/rate-limit-per-minute (db/get-provider :openai)))))

(deftest providers-persist-template-and-auth-settings
  (db/register-oauth-account! {:id            :openai-login
                               :name          "OpenAI Login"
                               :authorize-url "https://example.com/oauth/authorize"
                               :token-url     "https://example.com/oauth/token"
                               :client-id     "client-id"
                               :client-secret "client-secret"
                               :access-token  "access-token"})
  (db/upsert-provider! {:id            :openai
                        :name          "OpenAI"
                        :template      :custom
                        :base-url      "https://api.openai.com/v1"
                        :model         "gpt-5"
                        :access-mode   :account
                        :credential-source :oauth-account
                        :oauth-account :openai-login})
  (let [provider (db/get-provider :openai)]
    (is (= :custom (:llm.provider/template provider)))
    (is (= :account (:llm.provider/access-mode provider)))
    (is (= :oauth-account (:llm.provider/credential-source provider)))
    (is (= :oauth-account (:llm.provider/auth-type provider)))
    (is (= :openai-login (:llm.provider/oauth-account provider)))))

(deftest providers-persist-browser-session-account-connector-settings
  (db/upsert-provider! {:id                :openai-account
                        :name              "OpenAI Account"
                        :template          :openai
                        :base-url          "https://api.openai.com/v1"
                        :model             "gpt-5"
                        :access-mode       :account
                        :credential-source :browser-session
                        :browser-session   "browser-session-1"})
  (let [provider (db/get-provider :openai-account)]
    (is (= :openai (:llm.provider/template provider)))
    (is (= :account (:llm.provider/access-mode provider)))
    (is (= :browser-session (:llm.provider/credential-source provider)))
    (is (= :browser-session (:llm.provider/auth-type provider)))
    (is (= "browser-session-1" (:llm.provider/browser-session provider)))))

(deftest worker-sessions-are-hidden-by-default
  (let [parent (db/create-session! :terminal)
        child  (db/create-session! :branch {:parent-session-id parent
                                            :worker? true
                                            :label "site-a"})]
    (is (= [parent]
           (mapv :id (db/list-sessions))))
    (is (= #{parent child}
           (set (map :id (db/list-sessions {:include-workers? true})))))
    (let [worker (some #(when (= child (:id %)) %) (db/list-sessions {:include-workers? true}))]
      (is (true? (:worker? worker)))
      (is (= parent (:parent-id worker)))
      (is (= "site-a" (:label worker))))))

(deftest session-message-window-queries-return-ordered-slices
  (let [sid         (db/create-session! :terminal)
        message-ids (vec (for [i (range 6)]
                           (db/add-message! sid
                                            (if (even? i) :user :assistant)
                                            (str "message " i))))]
    (is (= 6 (db/session-message-count sid)))
    (is (= (subvec message-ids 2 5)
           (mapv :id (db/session-message-metadata-range sid 2 5 6))))
    (is (= (subvec message-ids 4 6)
           (mapv :id (db/session-message-metadata-range sid 4 6 6))))
    (is (every? pos? (map :token-estimate
                          (db/session-message-metadata-range sid 0 6 6))))))
