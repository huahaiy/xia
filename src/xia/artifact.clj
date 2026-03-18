(ns xia.artifact
  "Session-scoped output artifacts users can inspect and download."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [datalevin.core :as d]
            [xia.db :as db]
            [xia.memory :as memory]
            [xia.scratch :as scratch]
            [xia.working-memory :as wm])
  (:import [com.github.luben.zstd Zstd]
           [datalevin.db DB]
           [datalevin.storage Store]
           [java.math BigInteger]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util Base64 UUID]))

(def ^:private default-kind :txt)
(def ^:private default-source :generated)
(def ^:private default-name-stem "artifact")
(def ^:private preview-char-limit 1200)
(def ^:private read-char-limit 4000)
(def ^:private artifact-blob-dbi "xia/artifact-blobs")
(def ^:private default-blob-codec :zstd)
(def ^:private zstd-level 3)
(def ^:private byte-array-class (Class/forName "[B"))

(def ^:private kind-specs
  {:txt      {:kind :txt :media-type "text/plain"       :extension "txt"}
   :markdown {:kind :markdown :media-type "text/markdown"   :extension "md"}
   :csv      {:kind :csv :media-type "text/csv"         :extension "csv"}
   :json     {:kind :json :media-type "application/json" :extension "json"}
   :html     {:kind :html :media-type "text/html"       :extension "html"}
   :pdf      {:kind :pdf :media-type "application/pdf"  :extension "pdf"}
   :docx     {:kind :docx :media-type "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
              :extension "docx"}
   :xlsx     {:kind :xlsx :media-type "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
              :extension "xlsx"}
   :pptx     {:kind :pptx :media-type "application/vnd.openxmlformats-officedocument.presentationml.presentation"
              :extension "pptx"}
   :binary   {:kind :binary :media-type "application/octet-stream" :extension "bin"}})

(def ^:private kind-aliases
  {"csv" :csv
   "docx" :docx
   "htm" :html
   "html" :html
   "json" :json
   "markdown" :markdown
   "md" :markdown
   "pdf" :pdf
   "pptx" :pptx
   "text" :txt
   "txt" :txt
   "xlsx" :xlsx
   "bin" :binary
   "binary" :binary})

(def ^:private media-type->kind
  {"application/json" :json
   "application/octet-stream" :binary
   "application/pdf" :pdf
   "application/vnd.openxmlformats-officedocument.presentationml.presentation" :pptx
   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" :xlsx
   "application/vnd.openxmlformats-officedocument.wordprocessingml.document" :docx
   "text/csv" :csv
   "text/html" :html
   "text/markdown" :markdown
   "text/plain" :txt})

(def ^:private textual-kinds
  #{:txt :markdown :csv :json :html})

(defn- invalid-id-ex
  [message type-key field value]
  (ex-info message {:type type-key :field field :value value}))

(defn- normalize-uuid
  [value field type-key]
  (cond
    (instance? UUID value)
    value

    (string? value)
    (let [trimmed (str/trim value)]
      (when (str/blank? trimmed)
        (throw (invalid-id-ex (str field " is blank") type-key field value)))
      (try
        (UUID/fromString trimmed)
        (catch IllegalArgumentException _
          (throw (invalid-id-ex (str "invalid " field) type-key field value)))))

    :else
    (throw (invalid-id-ex (str "invalid " field) type-key field value))))

(defn- normalize-session-id
  [session-id]
  (normalize-uuid session-id "session_id" :artifact/invalid-session-id))

(defn- require-session-id
  []
  (or wm/*session-id*
      (throw (ex-info "session id is required"
                      {:type :artifact/session-required}))))

(defn- normalize-artifact-id
  [artifact-id]
  (normalize-uuid artifact-id "artifact_id" :artifact/invalid-artifact-id))

(defn- value-of
  [m key]
  (or (get m key)
      (get m (name key))
      (when (keyword? key)
        (get m (keyword (name key))))))

(defn- present?
  [m key]
  (or (contains? m key)
      (contains? m (name key))
      (when (keyword? key)
        (contains? m (keyword (name key))))))

(defn- lmdb []
  (let [db-value (d/db (db/conn))]
    (.-lmdb ^Store (.-store ^DB db-value))))

(defn- ensure-blob-dbi! []
  (d/open-dbi (lmdb) artifact-blob-dbi {:key-size 128}))

(defn- base-media-type
  [media-type]
  (some-> media-type
          str
          str/lower-case
          (str/split #";")
          first
          str/trim
          not-empty))

(defn- filename-extension
  [filename]
  (let [name (some-> filename str str/trim)
        idx  (when (seq name) (.lastIndexOf ^String name "."))]
    (when (and (some? idx) (pos? idx) (< idx (dec (count name))))
      (-> (.substring ^String name (inc idx))
          str/lower-case
          str/trim
          not-empty))))

(defn- strip-extension
  [filename]
  (let [name (some-> filename str str/trim)
        idx  (when (seq name) (.lastIndexOf ^String name "."))]
    (if (and (some? idx) (pos? idx))
      (.substring ^String name 0 idx)
      name)))

(defn- normalize-title
  [title]
  (let [value (some-> title str str/trim)]
    (when (seq value)
      value)))

(defn- filename-stem
  [value]
  (let [s (or (some-> value str str/trim) default-name-stem)
        slug (-> s
                 (str/replace #"[\r\n\t]+" " ")
                 (str/replace #"[\\/:*?\"<>|]+" "-")
                 (str/replace #"\s+" "-")
                 (str/replace #"-{2,}" "-")
                 (str/replace #"^-+" "")
                 (str/replace #"-+$" ""))]
    (if (seq slug) slug default-name-stem)))

(defn- ensure-extension
  [filename extension]
  (let [name (or (some-> filename str str/trim not-empty) (str default-name-stem "." extension))
        ext  (filename-extension name)]
    (if (= ext extension)
      name
      (str (strip-extension name) "." extension))))

(defn- normalize-kind
  [{:keys [kind name media-type]}]
  (or (cond
        (keyword? kind) (get kind-specs kind)
        (string? kind)  (some-> kind str/lower-case (get kind-aliases) kind-specs)
        :else           nil)
      (some-> media-type base-media-type media-type->kind kind-specs)
      (some-> name filename-extension kind-aliases kind-specs)
      (kind-specs default-kind)
      (throw (ex-info "unsupported artifact kind"
                      {:type :artifact/unsupported-kind
                       :kind kind
                       :media-type media-type
                       :name name}))))

(defn- normalize-name-and-title
  [{:keys [name title]} extension]
  (let [name*  (some-> name str str/trim not-empty)
        title* (or (normalize-title title)
                   (some-> name* strip-extension not-empty)
                   (str/capitalize default-name-stem))
        stem   (filename-stem (or name* title* default-name-stem))]
    {:name  (ensure-extension (or name* stem) extension)
     :title title*}))

(defn- normalize-string-content
  [content field]
  (let [value (cond
                (string? content) content
                (nil? content)    nil
                :else             (str content))]
    (when-not (seq value)
      (throw (ex-info (str "missing artifact " field)
                      {:type :artifact/missing-content
                       :field field})))
    value))

(defn- normalize-preview
  [preview]
  (let [value (some-> preview str str/trim)]
    (when (seq value)
      value)))

(defn- csv-escape
  [value]
  (let [s (cond
            (nil? value) ""
            (string? value) value
            :else (str value))]
    (if (re-find #"[\",\r\n]" s)
      (str "\"" (str/replace s "\"" "\"\"") "\"")
      s)))

(defn- render-csv-lines
  [rows]
  (let [rows* (vec rows)]
    (cond
      (empty? rows*)
      ""

      (every? map? rows*)
      (let [headers (reduce (fn [acc row]
                              (reduce (fn [acc* key]
                                        (if (some #(= % key) acc*)
                                          acc*
                                          (conj acc* key)))
                                      acc
                                      (keys row)))
                            []
                            rows*)
            header-text (str/join "," (map (comp csv-escape #(if (keyword? %) (name %) (str %))) headers))
            body-lines  (map (fn [row]
                               (str/join "," (map (comp csv-escape #(get row % "")) headers)))
                             rows*)]
        (str/join "\n" (cons header-text body-lines)))

      (every? sequential? rows*)
      (str/join "\n" (map (fn [row]
                            (str/join "," (map csv-escape row)))
                          rows*))

      :else
      (throw (ex-info "CSV artifacts require a string or rows of maps/vectors"
                      {:type :artifact/invalid-csv-rows})))))

(declare utf8-bytes
         decode-base64
         preview-text
         binary-preview)

(defn- bytes->utf8-string
  [^bytes data]
  (String. data StandardCharsets/UTF_8))

(defn- artifact-payload
  [spec]
  (let [{:keys [kind media-type extension]} (normalize-kind {:kind (value-of spec :kind)
                                                             :name (value-of spec :name)
                                                             :media-type (value-of spec :media-type)})
        content   (value-of spec :content)
        rows      (value-of spec :rows)
        data      (value-of spec :data)
        meta      (value-of spec :meta)
        bytes     (cond
                    (present? spec :bytes)
                    (let [value (value-of spec :bytes)]
                      (cond
                        (instance? byte-array-class value)
                        value

                        (string? value)
                        (utf8-bytes value)

                        :else
                        (throw (ex-info "artifact bytes must be a byte array or string"
                                        {:type :artifact/invalid-bytes}))))

                    (present? spec :bytes-base64)
                    (let [value (value-of spec :bytes-base64)]
                      (when-not (string? value)
                        (throw (ex-info "artifact bytes_base64 must be a base64 string"
                                        {:type :artifact/invalid-bytes-base64})))
                      (decode-base64 value))

                    :else
                    nil)
        rendered  (when (contains? textual-kinds kind)
                    (case kind
                      :json
                      (let [content* (or content
                                         (some-> bytes bytes->utf8-string))
                            value    (cond
                                       (present? spec :data) data
                                       (string? content*)    (json/read-json content*)
                                       (present? spec :content) content*
                                       :else                 (throw (ex-info "missing artifact data"
                                                                            {:type :artifact/missing-content
                                                                             :field "data"})))]
                        (json/write-json-str value {:indent-str "  "}))

                      :csv
                      (let [content* (or content
                                         (some-> bytes bytes->utf8-string))]
                        (cond
                          (string? content*)
                          content*

                          (sequential? rows)
                          (render-csv-lines rows)

                          :else
                          (throw (ex-info "CSV artifacts require 'content' or 'rows'"
                                          {:type :artifact/missing-content
                                           :field "rows"}))))

                      (normalize-string-content (or content
                                                    (some-> bytes bytes->utf8-string))
                                                "content")))
        {:keys [name title]} (normalize-name-and-title {:name (value-of spec :name)
                                                        :title (value-of spec :title)}
                                                       extension)]
    (when (and (not (contains? textual-kinds kind))
               (nil? bytes))
      (throw (ex-info "binary artifacts require 'bytes' or 'bytes_base64'"
                      {:type :artifact/missing-content
                       :field "bytes_base64"})))
    {:name       name
     :title      title
     :kind       kind
     :media-type (or (base-media-type media-type) media-type)
     :extension  extension
     :text       rendered
     :bytes      (when-not rendered bytes)
     :preview    (or (normalize-preview (value-of spec :preview))
                     (when rendered
                       (preview-text rendered))
                     (when bytes
                       (binary-preview {:kind kind
                                        :media-type (or (base-media-type media-type) media-type)
                                        :size-bytes (long (alength ^bytes bytes))})))
     :meta       meta}))

(defn- utf8-bytes
  [text]
  (.getBytes ^String text StandardCharsets/UTF_8))

(defn- decode-base64
  [text]
  (try
    (.decode (Base64/getDecoder) ^String text)
    (catch IllegalArgumentException e
      (throw (ex-info "artifact bytes_base64 must be valid base64"
                      {:type :artifact/invalid-bytes-base64}
                      e)))))

(defn- sha256-hex
  [^bytes data]
  (let [digest (doto (MessageDigest/getInstance "SHA-256")
                 (.update data))]
    (format "%064x" (BigInteger. 1 (.digest digest)))))

(defn- preview-text
  [text]
  (let [trimmed (str/trim (or text ""))]
    (if (> (count trimmed) preview-char-limit)
      (str (subs trimmed 0 (max 0 (dec preview-char-limit))) "…")
      trimmed)))

(defn- binary-preview
  [{:keys [kind media-type size-bytes]}]
  (str "Binary artifact"
       (when kind (str " (" (name kind) ")"))
       (when media-type (str "\nMedia type: " media-type))
       (when (some? size-bytes) (str "\nSize bytes: " size-bytes))))

(defn- zstd-bytes
  [^bytes data]
  (Zstd/compress data (int zstd-level)))

(defn- unzstd-bytes
  [^bytes data]
  (Zstd/decompress data))

(defn- store-blob!
  [blob-id codec ^bytes data]
  (ensure-blob-dbi!)
  (when-not (= codec :zstd)
    (throw (ex-info "unsupported artifact blob codec"
                    {:type :artifact/unsupported-blob-codec
                     :codec codec})))
  (let [encoded (zstd-bytes data)]
    (d/transact-kv (lmdb)
                   artifact-blob-dbi
                   [[:put (str blob-id) encoded]]
                   :string
                   :bytes)
    {:blob-id               blob-id
     :blob-codec            codec
     :compressed-size-bytes (long (alength encoded))}))

(defn- load-blob-bytes
  [blob-id codec]
  (when blob-id
    (ensure-blob-dbi!)
    (when-not (= codec :zstd)
      (throw (ex-info "unsupported artifact blob codec"
                      {:type :artifact/unsupported-blob-codec
                       :codec codec
                       :blob-id (str blob-id)})))
    (when-let [encoded ^bytes (d/get-value (lmdb) artifact-blob-dbi (str blob-id) :string :bytes)]
      (unzstd-bytes encoded))))

(defn- delete-blob!
  [blob-id]
  (when blob-id
    (ensure-blob-dbi!)
    (d/transact-kv (lmdb)
                   artifact-blob-dbi
                   [[:del (str blob-id)]]
                   :string)))

(defn- entity-created-at
  [entity-map]
  (some-> (:db/created-at entity-map) long java.util.Date.))

(defn- entity-updated-at
  [entity-map]
  (some-> (:db/updated-at entity-map) long java.util.Date.))

(defn- session-eid
  [session-id]
  (ffirst (db/q '[:find ?e :in $ ?sid :where [?e :session/id ?sid]]
                 session-id)))

(defn- session-channel
  [session-eid]
  (ffirst (db/q '[:find ?channel
                  :in $ ?session
                  :where [?session :session/channel ?channel]]
                session-eid)))

(defn- artifact-eid
  [artifact-id]
  (ffirst (db/q '[:find ?e :in $ ?id :where [?e :artifact/id ?id]]
                 artifact-id)))

(defn- artifact-session-id
  [eid]
  (ffirst (db/q '[:find ?sid
                  :in $ ?artifact
                  :where
                  [?artifact :artifact/session ?session]
                  [?session :session/id ?sid]]
                eid)))

(defn- session-not-found-ex
  [session-id]
  (ex-info "session not found"
           {:type :artifact/session-not-found
            :session-id (str session-id)}))

(defn- artifact-not-found-ex
  [artifact-id]
  (ex-info "artifact not found"
           {:type :artifact/not-found
            :artifact-id (str artifact-id)}))

(defn- event-episode
  [{:keys [session-id session-channel summary context]}]
  (cond-> {:episode/id         (random-uuid)
           :episode/type       :event
           :episode/summary    summary
           :episode/context    context
           :episode/timestamp  (java.util.Date.)
           :episode/processed? false
           :episode/session-id (str session-id)}
    session-channel
    (assoc :episode/channel (name session-channel))))

(defn- create-episode-context
  [{:keys [title kind media-type size-bytes sha256 preview artifact-id]}]
  (let [lines (cond-> [(str "Title: " title)
                       (str "Kind: " (name kind))
                       (str "Media type: " media-type)
                       (str "Size bytes: " size-bytes)
                       (str "SHA256: " sha256)
                       (str "Stored artifact id: " artifact-id)]
                (seq preview)
                (conj (str "Preview: " preview)))]
    (str/join "\n" lines)))

(defn- create-episode
  [{:keys [session-id session-channel name title kind media-type size-bytes sha256 preview artifact-id]}]
  (event-episode {:session-id session-id
                  :session-channel session-channel
                  :summary (str "Created artifact " name)
                  :context (create-episode-context {:title title
                                                    :kind kind
                                                    :media-type media-type
                                                    :size-bytes size-bytes
                                                    :sha256 sha256
                                                    :preview preview
                                                    :artifact-id artifact-id})}))

(defn- delete-episode-context
  [{:keys [title kind media-type size-bytes sha256 preview artifact-id]}]
  (let [lines (cond-> [(str "Title: " title)
                       (str "Kind: " (name kind))
                       (str "Media type: " media-type)
                       (str "Size bytes: " size-bytes)
                       (str "SHA256: " sha256)
                       (str "Deleted artifact id: " artifact-id)]
                (seq preview)
                (conj (str "Preview: " preview)))]
    (str/join "\n" lines)))

(defn- delete-episode
  [{:keys [session-id session-channel name title kind media-type size-bytes sha256 preview artifact-id]}]
  (event-episode {:session-id session-id
                  :session-channel session-channel
                  :summary (str "Deleted artifact " name)
                  :context (delete-episode-context {:title title
                                                    :kind kind
                                                    :media-type media-type
                                                    :size-bytes size-bytes
                                                    :sha256 sha256
                                                    :preview preview
                                                    :artifact-id artifact-id})}))

(defn- artifact-from-eid
  [eid]
  (when eid
    (let [entity (db/entity eid)]
      {:id         (:artifact/id entity)
       :session-id (some-> (artifact-session-id eid) str)
       :name       (:artifact/name entity)
       :title      (:artifact/title entity)
       :kind       (:artifact/kind entity)
       :media-type (:artifact/media-type entity)
       :extension  (:artifact/extension entity)
       :source     (:artifact/source entity)
       :status     (:artifact/status entity)
       :size-bytes (:artifact/size-bytes entity)
       :sha256     (:artifact/sha256 entity)
       :blob-id    (:artifact/blob-id entity)
       :blob-codec (:artifact/blob-codec entity)
       :compressed-size-bytes (:artifact/compressed-size-bytes entity)
       :error      (:artifact/error entity)
       :meta       (:artifact/meta entity)
       :text       (:artifact/text entity)
       :text-available? (some? (:artifact/text entity))
       :has-blob?  (some? (:artifact/blob-id entity))
       :preview    (:artifact/preview entity)
       :created-at (entity-created-at entity)
       :updated-at (entity-updated-at entity)})))

(defn list-artifacts
  ([] (list-artifacts (require-session-id)))
  ([session-id]
   (let [session-id* (normalize-session-id session-id)
         session-eid* (session-eid session-id*)]
     (when-not session-eid*
       (throw (session-not-found-ex session-id*)))
     (->> (db/q '[:find ?e :in $ ?session :where [?e :artifact/session ?session]]
                session-eid*)
          (map first)
          (map artifact-from-eid)
          (sort-by :updated-at #(compare %2 %1))
          vec))))

(defn get-artifact
  [artifact-id]
  (some-> artifact-id normalize-artifact-id artifact-eid artifact-from-eid))

(defn get-session-artifact
  [session-id artifact-id]
  (let [session-id*  (normalize-session-id session-id)
        artifact-id* (normalize-artifact-id artifact-id)
        artifact     (some-> artifact-id* artifact-eid artifact-from-eid)]
    (when (and artifact (= (str session-id*) (:session-id artifact)))
      artifact)))

(defn search-artifacts
  "Search session-scoped artifacts visible to the current session."
  [query & {:keys [top fts-query] :or {top 5}}]
  (memory/search-artifacts (require-session-id)
                           query
                           :top top
                           :fts-query fts-query))

(defn- read-session-artifact
  [session-id artifact-id & {:keys [offset max-chars]
                             :or {offset 0
                                  max-chars read-char-limit}}]
  (let [session-id*  (normalize-session-id session-id)
        artifact-id* (normalize-artifact-id artifact-id)
        offset*      (max 0 (int (or offset 0)))
        max-chars*   (max 1 (int (or max-chars read-char-limit)))]
    (when-let [artifact (get-session-artifact session-id* artifact-id*)]
      (let [text        (:text artifact)
            text-length (count (or text ""))
            start       (min offset* text-length)
            end         (min text-length (+ start max-chars*))]
        {:id          (:id artifact)
         :name        (:name artifact)
         :title       (:title artifact)
         :kind        (:kind artifact)
         :media-type  (:media-type artifact)
         :status      (:status artifact)
         :size-bytes  (:size-bytes artifact)
         :compressed-size-bytes (:compressed-size-bytes artifact)
         :has-blob?   (:has-blob? artifact)
         :text-available? (:text-available? artifact)
         :preview     (:preview artifact)
         :meta        (:meta artifact)
         :text        (when text
                        (subs text start end))
         :offset      start
         :end-offset  end
         :total-chars text-length
         :truncated?  (< end text-length)}))))

(defn read-artifact
  [artifact-id & {:keys [offset max-chars]
                  :or {offset 0
                       max-chars read-char-limit}}]
  (read-session-artifact (require-session-id)
                         artifact-id
                         :offset offset
                         :max-chars max-chars))

(defn artifact-download-data
  [session-id artifact-id]
  (let [session-id*  (normalize-session-id session-id)
        artifact-id* (normalize-artifact-id artifact-id)]
    (when-let [artifact (get-session-artifact session-id* artifact-id*)]
      (let [bytes (or (some-> (:text artifact) utf8-bytes)
                      (load-blob-bytes (:blob-id artifact)
                                       (:blob-codec artifact)))]
        (when (and (:has-blob? artifact) (nil? bytes))
          (throw (ex-info "artifact blob missing"
                          {:type :artifact/blob-missing
                           :artifact-id (str artifact-id*)
                           :blob-id (some-> (:blob-id artifact) str)})))
        {:id         (:id artifact)
         :name       (:name artifact)
         :media-type (:media-type artifact)
         :size-bytes (:size-bytes artifact)
         :bytes      bytes}))))

(defn- scratch-pad-episode-context
  [{:keys [name pad-id preview]}]
  (str/join "\n"
            (cond-> [(str "Artifact: " name)
                     (str "Scratch pad id: " pad-id)]
              (seq preview)
              (conj (str "Preview: " preview)))))

(defn- scratch-pad-episode
  [{:keys [session-id session-channel name pad-id preview]}]
  (event-episode {:session-id session-id
                  :session-channel session-channel
                  :summary (str "Created note from artifact " name)
                  :context (scratch-pad-episode-context {:name name
                                                         :pad-id pad-id
                                                         :preview preview})}))

(defn create-artifact!
  [spec]
  (let [session-id*  (normalize-session-id (or (value-of spec :session-id)
                                               (require-session-id)))
        session-eid* (session-eid session-id*)]
    (when-not session-eid*
      (throw (session-not-found-ex session-id*)))
    (let [{:keys [name title kind media-type extension text bytes preview meta]} (artifact-payload spec)
          artifact-id (random-uuid)
          source      (or (value-of spec :source) default-source)
          payload     (or bytes (utf8-bytes text))
          size-bytes  (long (alength payload))
          sha256      (sha256-hex payload)
          channel     (session-channel session-eid*)
          blob-info   (when bytes
                        (store-blob! (random-uuid) default-blob-codec bytes))]
      (try
        (db/transact!
          [(cond-> {:artifact/id         artifact-id
                    :artifact/session    session-eid*
                    :artifact/name       name
                    :artifact/title      title
                    :artifact/kind       kind
                    :artifact/media-type media-type
                    :artifact/extension  extension
                    :artifact/source     source
                    :artifact/status     :ready
                    :artifact/size-bytes size-bytes
                    :artifact/sha256     sha256
                    :artifact/preview    preview}
             text
             (assoc :artifact/text text)

             blob-info
             (assoc :artifact/blob-id (:blob-id blob-info)
                    :artifact/blob-codec (:blob-codec blob-info)
                    :artifact/compressed-size-bytes (:compressed-size-bytes blob-info))

             (some? meta)
             (assoc :artifact/meta meta))
           (create-episode {:session-id session-id*
                            :session-channel channel
                            :name name
                            :title title
                            :kind kind
                            :media-type media-type
                            :size-bytes size-bytes
                            :sha256 sha256
                            :preview preview
                            :artifact-id artifact-id})])
        (artifact-from-eid (artifact-eid artifact-id))
        (catch Exception e
          (when blob-info
            (delete-blob! (:blob-id blob-info)))
          (throw e))))))

(defn create-scratch-pad-from-artifact!
  ([artifact-id]
   (create-scratch-pad-from-artifact! (require-session-id) artifact-id))
  ([session-id artifact-id]
   (let [session-id*  (normalize-session-id session-id)
        session-eid* (session-eid session-id*)
        artifact-id* (normalize-artifact-id artifact-id)
        artifact     (get-session-artifact session-id* artifact-id*)
        channel      (session-channel session-eid*)]
     (when-not session-eid*
       (throw (session-not-found-ex session-id*)))
     (when-not artifact
       (throw (artifact-not-found-ex artifact-id*)))
     (let [content (or (:text artifact)
                       (:preview artifact)
                       "")
           pad     (scratch/create-pad! {:scope      :session
                                         :session-id (str session-id*)
                                         :title      (or (:title artifact)
                                                          (:name artifact))
                                         :content    content})]
       (try
         (db/transact! [(scratch-pad-episode {:session-id session-id*
                                              :session-channel channel
                                              :name (:name artifact)
                                              :pad-id (:id pad)
                                              :preview (:preview artifact)})])
         {:artifact artifact
          :pad      pad}
         (catch Throwable t
           (try
             (scratch/delete-pad! (:id pad))
             (catch Exception _))
           (throw t)))))))

(defn delete-artifact!
  ([artifact-id]
   (let [artifact-id* (normalize-artifact-id artifact-id)
         eid          (artifact-eid artifact-id*)
         artifact     (when eid (artifact-from-eid eid))]
     (when-not eid
       (throw (artifact-not-found-ex artifact-id*)))
     (db/transact! [[:db/retractEntity eid]])
     (delete-blob! (:blob-id artifact))
     {:status "deleted" :id (str artifact-id*)}))
  ([session-id artifact-id]
   (let [session-id*  (normalize-session-id session-id)
         session-eid* (session-eid session-id*)
         artifact-id* (normalize-artifact-id artifact-id)
         artifact     (get-session-artifact session-id* artifact-id*)
         eid          (artifact-eid artifact-id*)
         channel      (session-channel session-eid*)]
     (when-not session-eid*
       (throw (session-not-found-ex session-id*)))
     (when-not (and artifact eid)
       (throw (artifact-not-found-ex artifact-id*)))
     (db/transact! [(delete-episode {:session-id session-id*
                                     :session-channel channel
                                     :name (:name artifact)
                                     :title (:title artifact)
                                     :kind (:kind artifact)
                                     :media-type (:media-type artifact)
                                     :size-bytes (:size-bytes artifact)
                                     :sha256 (:sha256 artifact)
                                     :preview (:preview artifact)
                                     :artifact-id artifact-id*})
                    [:db/retractEntity eid]])
     (delete-blob! (:blob-id artifact))
     {:status "deleted" :id (str artifact-id*)})))
