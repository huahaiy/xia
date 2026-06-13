(ns xia.test-helpers
  "Shared test fixtures and helpers for xia tests."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.embedding :as emb]
            [datalevin.llm :as llm]
            [taoensso.timbre :as timbre]
            [xia.agent :as agent]
            [xia.agent.fact-review :as fact-review]
            [xia.async :as async]
            [xia.bridge :as bridge]
            [xia.browser.playwright :as playwright]
            [xia.channel.http :as http]
            [xia.channel.messaging :as messaging]
            [xia.checkpoint :as checkpoint]
            [xia.db :as db]
            [xia.hippocampus :as hippo]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.llm :as xia-llm]
            [xia.local-ocr :as local-ocr]
            [xia.oauth :as oauth]
            [xia.permission :as permission]
            [xia.prompt :as prompt]
            [xia.retrieval-state :as retrieval-state]
            [xia.runtime-context :as runtime-context]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.runtime-state :as runtime-state]
            [xia.sci-env :as sci-env]
            [xia.scheduler :as scheduler]
            [xia.service :as service]
            [xia.tool :as tool]
            [xia.web :as web]
            [xia.working-memory :as wm])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.nio.charset StandardCharsets]
           [java.util Base64]
           [java.util.concurrent.locks LockSupport]))

;; Keep routine test runs silent unless something truly unrecoverable happens.
(timbre/merge-config! {:min-level :fatal})

(defn- temp-db-path []
  (str (Files/createTempDirectory "xia-test"
         (into-array FileAttribute []))))

(defn- escape-pdf-text
  [text]
  (-> (str text)
      (str/replace "\\" "\\\\")
      (str/replace "(" "\\(")
      (str/replace ")" "\\)")))

(defn minimal-pdf-bytes
  [text]
  (let [stream  (str "BT\n/F1 12 Tf\n72 720 Td\n(" (escape-pdf-text text) ") Tj\nET\n")
        objects [(str "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")
                 (str "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n")
                 (str "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]"
                      " /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n")
                 (str "4 0 obj\n<< /Length " (count stream) " >>\nstream\n"
                      stream
                      "endstream\nendobj\n")
                 (str "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n")]
        header  "%PDF-1.4\n"
        offsets (loop [remaining objects
                       offset    (count header)
                       acc       []]
                  (if-let [object (first remaining)]
                    (recur (next remaining)
                           (+ offset (count object))
                           (conj acc offset))
                    acc))
        xref-start (+ (long (count header))
                      (long (reduce + 0 (map count objects))))
        xref       (str "xref\n0 6\n"
                        "0000000000 65535 f \n"
                        (apply str
                               (map (fn [offset]
                                      (format "%010d 00000 n \n" offset))
                                    offsets))
                        "trailer\n<< /Size 6 /Root 1 0 R >>\n"
                        "startxref\n"
                        xref-start
                        "\n%%EOF\n")]
    (.getBytes ^String (str header (apply str objects) xref)
               StandardCharsets/US_ASCII)))

(defn minimal-pdf-base64
  [text]
  (.encodeToString (Base64/getEncoder) ^bytes (minimal-pdf-bytes text)))

(defonce ^:private office-fixtures*
  (delay
    (or (some-> "fixtures/local-doc/office-fixtures.edn"
                io/resource
                slurp
                edn/read-string)
        (throw (ex-info "Office fixture data is missing"
                        {:resource "fixtures/local-doc/office-fixtures.edn"})))))

(defn office-fixture-base64
  [kind]
  (or (get @office-fixtures* kind)
      (throw (ex-info "Unknown office fixture kind"
                      {:kind kind
                       :known-kinds (sort (keys @office-fixtures*))}))))

(def ^:private test-embedding-dimensions
  32)

(def ^:private synonym->canonical
  {"auto" "vehicle"
   "automobile" "vehicle"
   "automobiles" "vehicle"
   "car" "vehicle"
   "cars" "vehicle"
   "vehicle" "vehicle"
   "vehicles" "vehicle"})

(def ^:private test-embedding-metadata
  {:embedding/provider {:kind :test
                        :id :default
                        :model-id "xia-test-embedder"}
   :embedding/output   {:dimensions test-embedding-dimensions
                        :normalize? true}
   :embedding/artifact {:format :memory
                        :file "xia-test-embedder"}})

(defn- provider-text
  [item]
  (cond
    (string? item) item
    (map? item)    (or (:text item) "")
    :else          (str item)))

(defn- canonical-token
  [token]
  (get synonym->canonical token token))

(defn- tokenize
  [text]
  (->> (str/split (str/lower-case (or text "")) #"[^\p{Alnum}]+")
       (remove str/blank?)
       (map canonical-token)))

(defn- token-slot
  [token]
  (Math/floorMod (int (hash token)) (int test-embedding-dimensions)))

(defn- normalize-vector
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

(defn- embed-text
  [text]
  (normalize-vector
    (reduce (fn [values token]
              (update values (token-slot token) + 1.0))
            (vec (repeat test-embedding-dimensions 0.0))
            (tokenize text))))

(defn- truncate-provider-item
  [item max-tokens]
  (let [text      (provider-text item)
        truncated (->> (tokenize text)
                       (take max-tokens)
                       (str/join " "))]
    (if (map? item)
      (assoc item :text truncated)
      truncated)))

(deftype TestEmbeddingProvider []
  emb/IEmbeddingProvider
  (embedding [_ items _opts]
    (mapv (comp embed-text provider-text) items))
  (embedding-metadata [_]
    test-embedding-metadata)
  (embedding-dimensions [_]
    test-embedding-dimensions)
  (close-provider [_]
    nil)

  emb/ITokenCounter
  (token-count* [_ item _opts]
    (count (tokenize (provider-text item))))
  (truncate-item* [_ item max-tokens _opts]
    (truncate-provider-item item max-tokens))

  java.lang.AutoCloseable
  (close [_]
    nil))

(defn test-embedding-provider
  []
  (TestEmbeddingProvider.))

(def ^:private test-llm-metadata
  {:llm/provider {:kind :test
                  :id :xia-test-llm}
   :llm/runtime  {:context-size 4096}
   :llm/artifact {:format :memory
                  :file "xia-test-llm"}})

(defn- summarize-provider-text
  [text]
  (let [summary (->> (tokenize text)
                     (take 14)
                     (str/join " ")
                     str/trim)]
    (if (seq summary)
      (str "model-summary: " summary)
      "model-summary: empty")))

(deftype TestLlmProvider []
  llm/ILLMProvider
  (generate-text* [_ prompt _max-tokens _opts]
    (summarize-provider-text prompt))
  (summarize-text* [_ text _max-tokens _opts]
    (summarize-provider-text text))
  (llm-metadata [_]
    test-llm-metadata)
  (llm-context-size [_]
    4096)
  (close-llm-provider [_]
    nil)

  java.lang.AutoCloseable
  (close [_]
    nil))

(defn test-llm-provider
  []
  (TestLlmProvider.))

(defn pause!
  [millis]
  (LockSupport/parkNanos (* 1000000 (long millis))))

(defn block-until-interrupted!
  "Yield in short intervals until the current thread is interrupted.
  Useful for timeout/cancellation tests without introducing long wall-clock sleeps."
  []
  (loop []
    (if (.isInterrupted (Thread/currentThread))
      (throw (InterruptedException.))
      (do
        (pause! 5)
        (recur)))))

(defn wait-until
  ([pred]
   (wait-until pred {}))
  ([pred {:keys [timeout-ms interval-ms]
          :or {timeout-ms 1000
               interval-ms 10}}]
   (let [deadline (+ (System/nanoTime) (* 1000000 (long timeout-ms)))]
     (loop []
       (let [result (pred)]
         (cond
           result result
           (>= (System/nanoTime) deadline) nil
           :else (do
                   (pause! interval-ms)
                   (recur))))))))

(defn test-connect-options
  ([]
   (test-connect-options nil))
  ([options]
   (let [provider-id (get-in (db/default-datalevin-opts) [:embedding-opts :provider])
         options     (merge {:local-llm-provider false}
                            options)]
     (-> (merge options
                {:datalevin-opts
                 {:embedding-providers {provider-id (test-embedding-provider)}
                  :flags #{:nosync}}})
         (update :datalevin-opts
                 #(merge (db/default-datalevin-opts) %))
         (update-in [:datalevin-opts :flags]
                    (fn [flags]
                      (conj (set (or flags #{})) :nosync)))))))

(defn with-test-db
  "Fixture: create a temp Datalevin DB for the duration of the test."
  [f]
  (let [path (temp-db-path)]
    (runtime-overlay/clear!)
    (instance-supervisor/clear-runtime!)
    (messaging/clear-runtime!)
    (tool/clear-runtime!)
    (checkpoint/clear-runtime!)
    (hippo/clear-runtime!)
    (local-ocr/clear-runtime!)
    (sci-env/clear-runtime!)
    (service/clear-runtime!)
    (web/clear-runtime!)
    (xia-llm/clear-runtime!)
    (scheduler/clear-runtime!)
    (playwright/clear-runtime!)
    (oauth/clear-runtime!)
    (retrieval-state/clear-runtime!)
    (runtime-state/clear-runtime!)
    (http/clear-runtime!)
    (bridge/clear-runtime!)
    (agent/clear-runtime!)
    (fact-review/clear-runtime!)
    (permission/clear-runtime!)
    (prompt/clear-runtime!)
    (async/clear-runtime!)
    (wm/clear-runtime!)
    (db/clear-runtime!)
    (let [runtime-state-runtime (runtime-state/make-runtime)
          retrieval-runtime     (retrieval-state/make-runtime)
          oauth-runtime         (oauth/make-runtime)
          browser-runtime       (playwright/make-runtime)
          async-runtime         (async/make-runtime)
          prompt-runtime        (prompt/make-runtime)
          fact-review-runtime   (fact-review/make-runtime)
          agent-runtime         (agent/make-runtime)
          bridge-runtime        (bridge/make-runtime)
          http-runtime          (http/make-runtime)
          instance-runtime      (instance-supervisor/make-runtime)
          sci-runtime           (sci-env/make-runtime)
          permission-runtime    (permission/make-runtime)
          tool-runtime          (tool/make-runtime)
          scheduler-runtime     (scheduler/make-runtime)
          wm-runtime            (wm/make-runtime)
          llm-runtime           (xia-llm/make-runtime)
          checkpoint-runtime    (checkpoint/make-runtime)
          hippocampus-runtime   (hippo/make-runtime)
          local-ocr-runtime     (local-ocr/make-runtime)
          service-runtime       (service/make-runtime)
          web-runtime           (web/make-runtime)
          messaging-runtime     (messaging/make-runtime)
          db-runtime            (db/make-runtime)
          context               (runtime-context/make
                                  {:xia/runtime-state-runtime {:runtime runtime-state-runtime}
                                   :xia/retrieval-runtime     {:runtime retrieval-runtime}
                                   :xia/oauth-runtime         {:runtime oauth-runtime}
                                   :xia/browser-runtime       {:runtime browser-runtime}
                                   :xia/async-runtime         {:runtime async-runtime}
                                   :xia/prompt-runtime        {:runtime prompt-runtime}
                                   :xia/fact-review-runtime   {:runtime fact-review-runtime}
                                   :xia/agent-runtime         {:runtime agent-runtime}
                                   :xia/bridge-runtime        {:runtime bridge-runtime}
                                   :xia/http-runtime          {:runtime http-runtime}
                                   :xia/instance-supervisor   {:runtime instance-runtime}
                                   :xia/sci-runtime           {:runtime sci-runtime}
                                   :xia/permission-runtime    {:runtime permission-runtime}
                                   :xia/tool-runtime          {:runtime tool-runtime}
                                   :xia/scheduler             {:runtime scheduler-runtime}
                                   :xia/working-memory-runtime {:runtime wm-runtime}
                                   :xia/llm-runtime           {:runtime llm-runtime}
                                   :xia/checkpoint-runtime    {:runtime checkpoint-runtime}
                                   :xia/hippocampus-runtime   {:runtime hippocampus-runtime}
                                   :xia/local-ocr-runtime     {:runtime local-ocr-runtime}
                                   :xia/service-runtime       {:runtime service-runtime}
                                   :xia/web-runtime           {:runtime web-runtime}
                                   :xia/messaging             {:runtime messaging-runtime}
                                   :xia/db                    {:runtime db-runtime}})]
      (runtime-context/with-runtime-context
        context
        #(do
           (sci-env/reset-runtime!)
           (wm/reset-runtime!)
           (db/connect! path (test-connect-options
                              {:passphrase-provider (constantly "xia-test-passphrase")}))))
      (try
        (runtime-context/with-runtime-context context f)
      (finally
        (runtime-context/with-runtime-context
          context
          #(do
             (runtime-overlay/clear!)
             (instance-supervisor/clear-runtime!)
             (messaging/clear-runtime!)
             (tool/clear-runtime!)
             (checkpoint/clear-runtime!)
             (hippo/clear-runtime!)
             (local-ocr/clear-runtime!)
             (sci-env/clear-runtime!)
             (service/clear-runtime!)
             (web/clear-runtime!)
             (xia-llm/clear-runtime!)
             (scheduler/clear-runtime!)
             (playwright/clear-runtime!)
             (oauth/clear-runtime!)
             (retrieval-state/clear-runtime!)
             (runtime-state/clear-runtime!)
             (http/clear-runtime!)
             (bridge/clear-runtime!)
             (agent/clear-runtime!)
             (fact-review/clear-runtime!)
             (permission/clear-runtime!)
             (prompt/clear-runtime!)
             (async/clear-runtime!)
             (wm/clear-runtime!)
             (db/clear-runtime!))))))))

(defn seed-node!
  "Helper: create a KG node and return its entity id."
  [name type]
  (let [id (java.util.UUID/randomUUID)]
    (db/transact! [{:kg.node/id         id
                    :kg.node/name       name
                    :kg.node/type       (keyword type)
                    :kg.node/created-at (java.util.Date.)
                    :kg.node/updated-at (java.util.Date.)}])
    (ffirst (db/q '[:find ?e :in $ ?id :where [?e :kg.node/id ?id]] id))))

(defn seed-episode!
  "Helper: create an episode and return its entity id."
  [summary & {:keys [context processed? timestamp session-id channel type importance]
              :or {processed? false
                   timestamp  (java.util.Date.)
                   type       :conversation}}]
  (let [id (java.util.UUID/randomUUID)]
    (db/transact! [(cond-> {:episode/id         id
                            :episode/type       type
                            :episode/summary    summary
                            :episode/timestamp  timestamp
                            :episode/processed? processed?}
                     context    (assoc :episode/context context)
                     importance (assoc :episode/importance (float importance))
                     session-id (assoc :episode/session-id (str session-id))
                     channel    (assoc :episode/channel (name channel)))])
    (ffirst (db/q '[:find ?e :in $ ?id :where [?e :episode/id ?id]] id))))

(defn seed-fact!
  "Helper: add a fact to a node and return the fact entity id."
  [node-eid content & {:keys [confidence utility] :or {confidence 1.0}}]
  (let [id (java.util.UUID/randomUUID)]
    (db/transact! [(cond-> {:kg.fact/id         id
                            :kg.fact/node       node-eid
                            :kg.fact/content    content
                            :kg.fact/confidence (float confidence)
                            :kg.fact/created-at (java.util.Date.)
                            :kg.fact/updated-at (java.util.Date.)}
                     (some? utility)
                     (assoc :kg.fact/utility (float utility)))])
    (ffirst (db/q '[:find ?e :in $ ?id :where [?e :kg.fact/id ?id]] id))))
