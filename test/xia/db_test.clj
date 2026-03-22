(ns xia.db-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.db :as db]
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
    (is (= 768 (get-in provider [:embedding-metadata :embedding/output :dimensions])))
    (is (= #{db/episode-text-domain
             db/kg-node-domain
             db/kg-fact-domain
             db/local-doc-domain
             db/local-doc-chunk-domain
             db/artifact-domain}
           (set (keys domains))))
    (is (every? #(= :llama.cpp (:provider %))
                (vals domains)))
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
                                     #'datalevin.core/get-conn
                                     (fn [_db-path _schema _opts]
                                       (swap! calls conj :get-conn)
                                       ::conn)
                                     #'xia.crypto/configure! (fn [& _] nil)
                                     #'xia.db/init-embedding-provider! (fn [& _] nil)
                                     #'xia.db/init-llm-provider! (fn [& _] nil)
                                     #'xia.db/migrate-secrets! (fn [] nil)
                                     #'datalevin.core/close (fn [_] nil)}
                      #(try
                         (db/connect! path {:local-llm-provider false
                                            :passphrase-provider (constantly "xia-test-passphrase")})
                         (finally
                           (db/close!)))))]
    (is (= [:download :get-conn]
           (mapv #(if (vector? %) (first %) %) @calls)))
    (is (.contains ^String output "Downloading Xia embedding model"))))

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
