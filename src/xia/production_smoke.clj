(ns xia.production-smoke
  "Finite production checks executed by the shipped `xia smoke` command.

   These checks intentionally use Xia's production runtime and persistent DB.
   They are not exposed to SCI and should only be run against disposable state."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.embedding :as emb]
            [xia.browser.playwright :as playwright]
            [xia.db :as db]
            [xia.db-schema :as db-schema]
            [xia.sci-env :as sci-env]
            [xia.task-spec :as task-spec]
            [xia.tool :as tool]))

(def ^:private smoke-secret-key :credential/native-smoke)
(def ^:private smoke-handler-key :user/native-smoke-handler)
(def ^:private smoke-secret "xia-native-smoke-secret-value")
(def ^:private smoke-embedding-dimensions 32)

(def ^:private smoke-embedding-metadata
  {:embedding/provider {:kind :test
                        :id :xia-native-smoke
                        :model-id "xia-native-smoke-embedder"}
   :embedding/output   {:dimensions smoke-embedding-dimensions
                        :normalize? true}
   :embedding/artifact {:format :memory
                        :file "xia-native-smoke-embedder"}})

(defn- embedding-item-text
  [item]
  (cond
    (string? item) item
    (map? item)    (or (:text item) "")
    :else          (str item)))

(defn- embedding-tokens
  [item]
  (->> (str/split (str/lower-case (embedding-item-text item)) #"[^\p{Alnum}]+")
       (remove str/blank?)))

(defn- normalize-embedding
  [values]
  (let [norm (Math/sqrt
              (reduce (fn [sum value]
                        (+ (double sum)
                           (* (double value) (double value))))
                      0.0
                      values))]
    (if (pos? norm)
      (mapv #(float (/ (double %) norm)) values)
      values)))

(defn- embed-item
  [item]
  (normalize-embedding
   (reduce (fn [values token]
             (let [slot (Math/floorMod (int (hash token))
                                       (int smoke-embedding-dimensions))]
               (update values slot + 1.0)))
           (vec (repeat smoke-embedding-dimensions 0.0))
           (embedding-tokens item))))

(defn- truncate-embedding-item
  [item max-tokens]
  (let [text (->> (embedding-tokens item)
                  (take max-tokens)
                  (str/join " "))]
    (if (map? item)
      (assoc item :text text)
      text)))

(deftype NativeSmokeEmbeddingProvider []
  emb/IEmbeddingProvider
  (embedding [_ items _opts]
    (mapv embed-item items))
  (embedding-metadata [_]
    smoke-embedding-metadata)
  (embedding-dimensions [_]
    smoke-embedding-dimensions)
  (close-provider [_]
    nil)

  emb/ITokenCounter
  (token-count* [_ item _opts]
    (count (embedding-tokens item)))
  (truncate-item* [_ item max-tokens _opts]
    (truncate-embedding-item item max-tokens))

  java.lang.AutoCloseable
  (close [_]
    nil))

(defn with-native-smoke-embedding-provider
  "Install the deterministic, network-free provider used only by native smoke."
  [connect-options]
  (let [provider-id (get-in (db/default-datalevin-opts)
                            [:embedding-opts :provider])]
    (assoc-in (or connect-options {})
              [:datalevin-opts :embedding-providers provider-id]
              (NativeSmokeEmbeddingProvider.))))

(defn- fail!
  [check data]
  (throw (ex-info (str "Production smoke check failed: " (name check))
                  (assoc data :check check :type :production-smoke/failed))))

(defn- check!
  [check value data]
  (when-not value
    (fail! check data))
  true)

(defn- throws?
  [f]
  (try
    (f)
    false
    (catch Throwable _
      true)))

(defn- check-schema!
  []
  (check! :schema-version
          (= db-schema/current-version (db/schema-version))
          {:expected db-schema/current-version
           :actual (db/schema-version)})
  (check! :schema-resource
          (= (db-schema/schema-resource-path db-schema/current-version)
             (db/schema-resource-path))
          {:expected (db-schema/schema-resource-path db-schema/current-version)
           :actual (db/schema-resource-path)})
  (check! :frozen-schema-integrity
          (db-schema/ensure-frozen-schema-integrity!)
          {})
  (let [from-version (dec db-schema/current-version)
        before       (count (db/schema-migration-history))]
    (check! :migration-path
            (seq (db-schema/migration-path from-version))
            {:from-version from-version
             :to-version db-schema/current-version})
    (db-schema/apply-migrations! (db/conn) from-version)
    (let [history (db/schema-migration-history)
          latest  (peek history)]
      (check! :migration-recorded
              (and (= (inc before) (count history))
                   (= from-version (:from-version latest))
                   (= db-schema/current-version (:to-version latest)))
              {:before before :history history})))
  {:version (db/schema-version)
   :resource (db/schema-resource-path)})

(defn- check-embedding-provider!
  []
  (let [provider   (db/current-embedding-provider)
        metadata   (some-> provider emb/embedding-metadata)
        dimensions (some-> provider emb/embedding-dimensions)
        vector      (some-> provider
                            (emb/embedding ["native smoke embedding"] nil)
                            first)]
    (check! :embedding-provider
            (and (= :xia-native-smoke
                    (get-in metadata [:embedding/provider :id]))
                 (= smoke-embedding-dimensions dimensions)
                 (= smoke-embedding-dimensions (count vector)))
            {:metadata metadata
             :dimensions dimensions
             :vector-dimensions (some-> vector count)})
    {:status :ok
     :dimensions dimensions}))

(defn- raw-config-value
  [key]
  (ffirst (db/q '[:find ?v
                  :in $ ?k
                  :where
                  [?e :config/key ?k]
                  [?e :config/value ?v]]
                key)))

(defn- check-secret-encryption!
  []
  (db/set-config! smoke-secret-key smoke-secret)
  (let [raw (raw-config-value smoke-secret-key)]
    (check! :secret-round-trip
            (= smoke-secret (db/get-config smoke-secret-key))
            {})
    (check! :secret-ciphertext
            (and (string? raw)
                 (str/starts-with? raw "enc:v1:")
                 (not (str/includes? raw smoke-secret)))
            {:raw-prefix (some-> raw (subs 0 (min 7 (count raw))))})
    {:ciphertext-prefix (subs raw 0 (min 7 (count raw)))
     :plaintext-absent? (not (str/includes? raw smoke-secret))}))

(defn- check-sci!
  []
  (check! :sci-allowed
          (= 42 (sci-env/eval-string "(+ 20 22)"))
          {})
  (doseq [[check code]
          [[:sci-blocks-file-read "(slurp \"/etc/passwd\")"]
           [:sci-blocks-resolution "(resolve 'slurp)"]
           [:sci-blocks-secret-read
            "(xia.db/get-config :credential/native-smoke)"]
           [:sci-blocks-secret-write
            "(xia.db/set-config! :credential/native-smoke \"leak\")"]]]
    (check! check (throws? #(sci-env/eval-string code)) {:code code}))
  {:allowed-result 42
   :blocked-count 4})

(defn- check-permission-before-handler!
  []
  (let [bundled-result (tool/execute-tool :browser-runtime-status
                                          {}
                                          {:channel :terminal})
        session-id     (db/create-session! :http)]
    (check! :bundled-tool-handler
            (and (map? bundled-result)
                 (not (:error bundled-result))
                 (seq (:backends bundled-result)))
            {:result bundled-result})
    (tool/import-tool!
     {:id :browser-login-interactive
      :name "Native smoke blocked handler"
      :description "The HTTP channel policy must reject this before SCI runs."
      :approval :auto
      :handler (str "(fn [_] (xia.db/set-config! "
                    smoke-handler-key
                    " \"executed\"))")})
    (let [result (tool/execute-tool :browser-login-interactive
                                    {}
                                    {:channel :http
                                     :session-id session-id})]
      (check! :permission-denied
              (some-> (:error result)
                      (str/includes? "blocked"))
              {:result result})
      (check! :permission-before-handler
              (nil? (db/get-config smoke-handler-key))
              {:stored-value (db/get-config smoke-handler-key)})
      {:session-id session-id
       :blocked? true
       :handler-ran? false})))

(defn- check-task-lifecycle!
  [session-id]
  (let [task-id (task-spec/create-task!
                 {:goal "Production native task lifecycle"
                  :steps [{:id :prepare
                           :kind :value
                           :value "prepared"}
                          {:id :finish
                           :kind :value
                           :depends-on :prepare
                           :value "finished"}]}
                 :session-id session-id)
        task-before-run (db/get-task task-id)
        _        (db/update-task! task-id
                                  {:meta (assoc (:meta task-before-run)
                                                :branch-worker true)})
        paused  (task-spec/run-task! task-id :max-steps 1)
        task-at-pause (db/get-task task-id)
        resumed (task-spec/run-task! task-id)
        completed (db/get-task task-id)]
    (check! :task-paused
            (and (= :paused (:status paused))
                 (= :resumable (:state task-at-pause)))
            {:result paused :task task-at-pause})
    (check! :task-resumed-and-completed
            (and (= :completed (:status resumed))
                 (= :completed (:state completed))
                 (= "finished"
                    (get-in completed [:meta :task-spec :outputs :finish])))
            {:result resumed :task completed})
    {:task-id task-id
     :paused-status (:status paused)
     :completed-status (:status resumed)
     :turn-count (count (db/task-turns task-id))}))

(defn- write-result!
  [output {:keys [schema embedding secret sci permission task browser]}]
  (when output
    (let [file (io/file output)]
      (when-let [parent (.getParentFile file)]
        (.mkdirs parent))
      (spit file
            (str "task_id=" (get task :task-id) "\n"
                 "session_id=" (get permission :session-id) "\n"
                 "schema_version=" (get schema :version) "\n"
                 "embedding_status=" (name (get embedding :status)) "\n"
                 "ciphertext_prefix=" (get secret :ciphertext-prefix) "\n"
                 "sci_blocked=" (get sci :blocked-count) "\n"
                 "permission_handler_ran=" (get permission :handler-ran?) "\n"
                 "task_status=" (name (get task :completed-status)) "\n"
                 "browser_status=" (name (get browser :status)) "\n")))))

(defn run-smoke!
  "Run finite production checks against the current disposable Xia runtime."
  [& {:keys [output skip-browser?]}]
  (let [schema     (check-schema!)
        embedding  (check-embedding-provider!)
        secret     (check-secret-encryption!)
        sci        (check-sci!)
        permission (check-permission-before-handler!)
        task       (check-task-lifecycle! (:session-id permission))
        browser    (if skip-browser?
                     {:status :skipped}
                     (playwright/production-smoke!))
        result     {:status :ok
                    :schema schema
                    :embedding embedding
                    :secret secret
                    :sci sci
                    :permission permission
                    :task task
                    :browser browser}]
    (write-result! output result)
    result))
