(ns xia.model-assets
  "Managed local model asset downloads."
  (:require [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [xia.http-client :as http-client]))

(def default-embedding-model-file
  "nomic-embed-text-v2-moe-q8_0.gguf")

(def default-embedding-model-url
  (str "https://huggingface.co/ggml-org/Nomic-Embed-Text-V2-GGUF/resolve/main/"
       default-embedding-model-file
       "?download=true"))

(def default-llm-model-file
  "gemma-3-4b-it.Q4_K_M.gguf")

(def default-llm-model-url
  (str "https://huggingface.co/MaziyarPanahi/gemma-3-4b-it-GGUF/resolve/main/"
       default-llm-model-file
       "?download=true"))

(def ^:private embedding-model-lock
  (Object.))

(def ^:private llm-model-lock
  (Object.))

(defn- announce-managed-download!
  [{:keys [artifact-kind artifact-label target-path]}]
  (let [message (str "Downloading Xia "
                     artifact-kind
                     " "
                     artifact-label
                     " to "
                     target-path
                     ". This may take a few minutes the first time.")]
    (log/info message)
    (println message)
    (flush)))

(defn ensure-managed-file!
  [{:keys [url target-path lock artifact-kind artifact-label request-label]
    :or   {artifact-kind  "managed asset"
           artifact-label "asset"}}]
  (when (and (seq target-path)
             (seq url)
             (not (.exists (io/file target-path))))
    (locking (or lock target-path)
      (when-not (.exists (io/file target-path))
        (announce-managed-download! {:artifact-kind artifact-kind
                                     :artifact-label artifact-label
                                     :target-path target-path})
        (http-client/download! {:url url
                                :target-path target-path
                                :timeout (* 30 60 1000)
                                :request-label (or request-label
                                                   (str artifact-kind " " artifact-label " download"))}))))
  target-path)

(defn ensure-managed-model!
  ([provider-spec]
   (ensure-managed-model! provider-spec llm-model-lock "LLM"))
  ([provider-spec lock artifact-label]
   (let [model-path (or (:model provider-spec) (:model-path provider-spec))]
     (cond
       (not (map? provider-spec))
       provider-spec

       (or (nil? model-path)
           (nil? (:model-url provider-spec))
           (.exists (io/file model-path)))
       provider-spec

       :else
       (locking lock
         (when-not (.exists (io/file model-path))
           (ensure-managed-file! {:url (:model-url provider-spec)
                                  :target-path model-path
                                  :lock lock
                                  :artifact-kind (str artifact-label " model")
                                  :artifact-label (or (:model-id provider-spec)
                                                      (:model-filename provider-spec)
                                                      "managed-model")
                                  :request-label (str "managed " artifact-label " model download")}))
         provider-spec)))))

(defn ensure-managed-embedding-model!
  [provider-spec]
  (ensure-managed-model! provider-spec embedding-model-lock "embedding"))
