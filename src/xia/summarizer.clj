(ns xia.summarizer
  "Local, in-process summarization backed by Datalevin's llama.cpp runtime."
  (:require [clojure.string :as str]
            [datalevin.core :as d]
            [taoensso.timbre :as log]
            [xia.config :as config]
            [xia.db :as db]))

(def ^:private default-enabled?
  false)

(def ^:private default-chunk-summary-max-tokens
  96)

(def ^:private default-doc-summary-max-tokens
  160)

(def ^:private chunk-summary-instructions
  (str "You are writing a retrieval summary for Xia, an assistant that helps users search uploaded local documents.\n"
       "This summary will be indexed and shown to another model before it reads the full text.\n"
       "Keep the facts someone would search for: names, headings, dates, numbers, owners, statuses, decisions, risks, and domain terms.\n"
       "Prefer exact entities and quantities from the source. Use different wording when possible, but do not drop critical facts.\n"
       "Do not invent facts, add advice, or mention that you are summarizing.\n"
       "Write 1 or 2 short sentences only.\n\n"
       "Source text:\n"))

(def ^:private document-summary-instructions
  (str "You are writing a document-level retrieval summary for Xia, an assistant that helps users search uploaded local documents.\n"
       "This summary will be indexed and shown to another model so it can decide whether to read the document in full.\n"
       "Preserve the document's subject plus the most retrieval-critical facts: names, headings, dates, numbers, owners, decisions, risks, and unique terms.\n"
       "Prefer exact entities and quantities from the source. Use different wording when possible, but do not drop critical facts.\n"
       "Do not invent facts, add advice, or mention that you are summarizing.\n"
       "Write 2 to 4 short sentences only.\n\n"))

(defn- compact-text
  [text]
  (some-> text
          str
          str/trim
          (str/replace #"\s+" " ")
          not-empty))

(defn- truncate-text
  [text max-chars]
  (when-let [compact (compact-text text)]
    (if (and max-chars (> (count compact) max-chars))
      (str (subs compact 0 (max 0 (dec max-chars))) "…")
      compact)))

(defn enabled?
  []
  (config/boolean-option :local-doc/model-summaries-enabled? default-enabled?))

(defn chunk-summary-max-tokens
  []
  (config/positive-long :local-doc/chunk-summary-max-tokens
                        default-chunk-summary-max-tokens))

(defn document-summary-max-tokens
  []
  (config/positive-long :local-doc/doc-summary-max-tokens
                        default-doc-summary-max-tokens))

(defn build-chunk-prompt
  [text]
  (str chunk-summary-instructions
       text
       "\n\nRetrieval summary:"))

(defn build-document-prompt
  [text source-kind]
  (str document-summary-instructions
       (if (= source-kind :chunk-summaries)
         "Evidence from document sections:\n"
         "Source document text:\n")
       text
       "\n\nDocument retrieval summary:"))

(defn- generate-summary*
  [prompt max-tokens max-chars kind]
  (when-let [provider (and (enabled?) (db/current-llm-provider))]
    (when-let [input (compact-text prompt)]
      (try
        (some-> (d/generate-text provider input (long max-tokens))
                (truncate-text max-chars))
        (catch Throwable t
          (log/warn t "Local" (name kind) "summarization failed; using extractive fallback")
          nil)))))

(defn summarize-chunk
  [text max-chars]
  (when-let [input (compact-text text)]
    (generate-summary* (build-chunk-prompt input)
                       (chunk-summary-max-tokens)
                       max-chars
                       :chunk)))

(defn summarize-document
  [chunks full-text max-chars]
  (let [chunk-input (->> chunks
                         (map :summary)
                         (remove str/blank?)
                         (map-indexed (fn [idx summary]
                                        (str "Chunk " (inc idx) ": " summary)))
                         (str/join "\n"))
        source-kind (if (seq chunk-input) :chunk-summaries :full-text)
        source-text (if (seq chunk-input) chunk-input full-text)]
    (when-let [input (compact-text source-text)]
      (generate-summary* (build-document-prompt input source-kind)
                         (document-summary-max-tokens)
                         max-chars
                         :document))))
