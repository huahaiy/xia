(ns xia.workspace
  "Filesystem-backed shared workspace for cross-instance collaboration.

   Workspace items live outside any single Xia DB so multiple instances on the
   same machine can publish and consume shared artifacts, document exports, and
   notes without sharing Datalevin state."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.artifact :as artifact]
            [xia.db :as db]
            [xia.local-doc :as local-doc]
            [xia.paths :as paths]
            [xia.working-memory :as wm])
  (:import [java.io File]
           [java.math BigInteger]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files]
           [java.security MessageDigest]
           [java.util Date UUID]))

(def ^:private items-dir-name "items")
(def ^:private meta-file-name "meta.edn")
(def ^:private default-note-name "note.md")
(def ^:private preview-char-limit 1200)
(def ^:private read-char-limit 4000)
(def ^:private supported-source-types #{:artifact :local-doc :note})
(def ^:private textual-media-types
  #{"application/edn"
    "application/json"
    "application/xml"
    "application/yaml"
    "text/csv"
    "text/html"
    "text/javascript"
    "text/markdown"
    "text/plain"
    "text/tab-separated-values"
    "text/typescript"
    "text/x-c"
    "text/x-c++"
    "text/x-clojure"
    "text/x-java"
    "text/x-python"
    "text/x-ruby"
    "text/x-shellscript"
    "text/x-sql"})

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

(defn- normalize-item-id
  [item-id]
  (normalize-uuid item-id "item_id" :workspace/invalid-item-id))

(defn- require-session-id
  []
  (or wm/*session-id*
      (throw (ex-info "session id is required"
                      {:type :workspace/session-required}))))

(defn- nonblank-string
  [value]
  (let [text (some-> value str str/trim)]
    (when (seq text)
      text)))

(defn- normalize-workspace-id
  [workspace-id]
  (or (some-> workspace-id paths/normalize-instance-id)
      (when (nil? workspace-id)
        paths/default-workspace-id)
      (throw (ex-info "workspace_id must be a non-empty string"
                      {:type :workspace/invalid-workspace-id
                       :workspace-id workspace-id}))))

(defn- normalize-source-type
  [source-type]
  (when source-type
    (let [value (cond
                  (keyword? source-type) source-type
                  (string? source-type)  (some-> source-type
                                                 str/trim
                                                 str/lower-case
                                                 (str/replace "_" "-")
                                                 keyword)
                  :else nil)]
      (when-not (contains? supported-source-types value)
        (throw (ex-info "unsupported workspace source_type"
                        {:type :workspace/invalid-source-type
                         :source-type source-type})))
      value)))

(defn workspace-dir
  "Return the absolute directory for a named shared workspace."
  ([] (workspace-dir nil))
  ([workspace-id]
   (paths/path-str (paths/shared-workspace-root)
                   (normalize-workspace-id workspace-id))))

(defn- items-dir
  [workspace-id]
  (paths/path-str (workspace-dir workspace-id) items-dir-name))

(defn- item-dir
  [workspace-id item-id]
  (paths/path-str (items-dir workspace-id) (str item-id)))

(defn- meta-path
  [workspace-id item-id]
  (paths/path-str (item-dir workspace-id item-id) meta-file-name))

(defn- ensure-dir!
  [path]
  (.mkdirs (io/file path))
  path)

(defn- file-extension
  [filename]
  (let [name (nonblank-string filename)
        idx  (when name (.lastIndexOf ^String name "."))]
    (when (and (some? idx)
               (pos? (long idx))
               (< (long idx) (dec (long (count name)))))
      (-> (.substring ^String name (inc (long idx)))
          str/lower-case
          str/trim
          not-empty))))

(defn- strip-extension
  [filename]
  (let [name (or (nonblank-string filename) "")
        idx  (.lastIndexOf ^String name ".")]
    (if (and (pos? idx))
      (.substring ^String name 0 idx)
      name)))

(defn- safe-filename
  [filename]
  (let [name (or (nonblank-string filename) default-note-name)
        sanitized (-> name
                      (str/replace #"[\r\n\t]+" " ")
                      (str/replace #"[\\/:*?\"<>|]+" "-")
                      (str/replace #"\s+" " ")
                      str/trim)]
    (if (seq sanitized)
      sanitized
      default-note-name)))

(defn- note-filename
  [title]
  (let [stem (or (some-> title
                         nonblank-string
                         (str/replace #"[\r\n\t]+" " ")
                         (str/replace #"[\\/:*?\"<>|]+" "-")
                         (str/replace #"\s+" "-")
                         (str/replace #"-{2,}" "-")
                         (str/replace #"^-+" "")
                         (str/replace #"-+$" ""))
                 "note")]
    (str stem ".md")))

(defn- text-media-type?
  [media-type]
  (let [value (some-> media-type str str/lower-case (str/split #";") first str/trim)]
    (or (contains? textual-media-types value)
        (and value (str/starts-with? value "text/")))))

(defn- preview-text
  [text]
  (let [trimmed (str/trim (or text ""))]
    (if (> (count trimmed) preview-char-limit)
      (str (subs trimmed 0 (dec preview-char-limit)) "…")
      trimmed)))

(defn- utf8-bytes
  [text]
  (.getBytes ^String (str text) StandardCharsets/UTF_8))

(defn- bytes->utf8-string
  [^bytes data]
  (String. data StandardCharsets/UTF_8))

(defn- sha256-hex
  [^bytes data]
  (let [digest (doto (MessageDigest/getInstance "SHA-256")
                 (.update data))]
    (format "%064x" (BigInteger. 1 (.digest digest)))))

(defn- payload-file
  [workspace-id item-id filename]
  (io/file (item-dir workspace-id item-id) (safe-filename filename)))

(defn- write-bytes!
  [^File file ^bytes data]
  (with-open [out (io/output-stream file)]
    (.write out data)))

(defn- read-bytes
  [^File file]
  (Files/readAllBytes (.toPath file)))

(defn- read-meta-file
  [^File file]
  (try
    (edn/read-string (slurp file))
    (catch Exception e
      (log/warn e "Ignoring unreadable workspace metadata" (.getAbsolutePath file))
      nil)))

(defn- item-record
  [workspace-id metadata]
  (let [item-id    (str (:id metadata))
        payload*   (payload-file workspace-id item-id (:name metadata))]
    (assoc metadata
           :id item-id
           :workspace-id workspace-id
           :payload-path (paths/absolute-path (.getAbsolutePath payload*)))))

(defn- list-item-records
  [workspace-id]
  (let [dir (io/file (items-dir workspace-id))]
    (if-not (.isDirectory dir)
      []
      (->> (or (.listFiles dir) (make-array File 0))
           (filter #(.isDirectory ^File %))
           (map (fn [^File item-dir*]
                  (let [meta-file (io/file item-dir* meta-file-name)
                        metadata  (when (.isFile meta-file)
                                    (read-meta-file meta-file))]
                    (when (and (map? metadata)
                               (seq (str (:id metadata)))
                               (seq (str (:name metadata))))
                      (let [record (item-record workspace-id metadata)]
                        (if (.isFile (io/file ^String (:payload-path record)))
                          record
                          (do
                            (log/warn "Ignoring workspace item with missing payload" (:payload-path record))
                            nil)))))))
           (remove nil?)
           vec))))

(defn- item-sort-ms
  [item]
  (long (or (some-> (:created-at item) .getTime) 0)))

(defn list-items
  "List shared workspace items for a workspace. Defaults to the `default`
   workspace. Most recent items come first."
  [& {:keys [workspace-id top source-type]}]
  (let [workspace-id* (normalize-workspace-id workspace-id)
        source-type*  (normalize-source-type source-type)
        items         (cond->> (list-item-records workspace-id*)
                        source-type* (filter #(= source-type* (:source-type %)))
                        true (sort-by (fn [item]
                                        [(- (item-sort-ms item))
                                         (str (:id item))])))]
    (if top
      (into [] (take (max 0 (long top))) items)
      (vec items))))

(defn get-item
  "Return shared workspace item metadata by id."
  [item-id & {:keys [workspace-id]}]
  (let [workspace-id* (normalize-workspace-id workspace-id)
        item-id*      (str (normalize-item-id item-id))
        meta-file     (io/file (meta-path workspace-id* item-id*))]
    (when (.isFile meta-file)
      (when-let [metadata (read-meta-file meta-file)]
        (item-record workspace-id* metadata)))))

(defn- item-not-found-ex
  [workspace-id item-id]
  (ex-info "workspace item not found"
           {:type :workspace/not-found
            :workspace-id workspace-id
            :item-id (str item-id)}))

(defn- source-not-found-ex
  [field value]
  (ex-info "workspace source item not found"
           {:type :workspace/source-not-found
            :field field
            :value (str value)}))

(defn- read-item-bytes
  [workspace-id item-id]
  (let [workspace-id* (normalize-workspace-id workspace-id)
        item-id*      (str (normalize-item-id item-id))]
    (if-let [item (get-item item-id* :workspace-id workspace-id*)]
      (let [payload-file* (io/file ^String (:payload-path item))]
        (if (.isFile payload-file*)
          (assoc item :bytes (read-bytes payload-file*))
          (throw (ex-info "workspace payload missing"
                          {:type :workspace/payload-missing
                           :workspace-id workspace-id*
                           :item-id item-id*
                           :payload-path (:payload-path item)}))))
      (throw (item-not-found-ex workspace-id* item-id*)))))

(defn read-item
  "Read a shared workspace item. Text items return a sliced `:text` field.
   Binary items return metadata plus `:text-available? false`."
  [item-id & {:keys [workspace-id offset max-chars]
              :or {offset 0
                   max-chars read-char-limit}}]
  (let [{:keys [bytes] :as item} (read-item-bytes workspace-id item-id)
        offset*    (max 0 (int offset))
        max-chars* (max 1 (int max-chars))]
    (if (text-media-type? (:media-type item))
      (let [text        (bytes->utf8-string bytes)
            total-chars (count text)
            start       (min offset* total-chars)
            end         (min total-chars (+ start max-chars*))]
        (assoc item
               :bytes nil
               :text-available? true
               :text (subs text start end)
               :offset start
               :end-offset end
               :total-chars total-chars
               :truncated? (< end total-chars)))
      (assoc item
             :bytes nil
             :text-available? false
             :text nil
             :offset 0
             :end-offset 0
             :total-chars 0
             :truncated? false))))

(defn- current-instance-id
  []
  (or (db/current-instance-id)
      paths/default-instance-id))

(defn- current-session-id
  []
  (some-> wm/*session-id* str))

(defn- store-item!
  [workspace-id metadata ^bytes bytes]
  (let [workspace-id* (normalize-workspace-id workspace-id)
        item-id       (str (random-uuid))
        payload-name  (safe-filename (:name metadata))
        item-dir*     (item-dir workspace-id* item-id)
        payload-file* (payload-file workspace-id* item-id payload-name)
        meta-file*    (io/file (meta-path workspace-id* item-id))
        preview       (or (nonblank-string (:preview metadata))
                          (when (text-media-type? (:media-type metadata))
                            (preview-text (bytes->utf8-string bytes))))
        record        (-> metadata
                          (assoc :id item-id
                                 :workspace-id workspace-id*
                                 :name payload-name
                                 :size-bytes (long (alength bytes))
                                 :sha256 (sha256-hex bytes)
                                 :created-at (or (:created-at metadata) (Date.))
                                 :producer-instance-id (or (:producer-instance-id metadata)
                                                           (current-instance-id))
                                 :producer-session-id (or (:producer-session-id metadata)
                                                          (current-session-id))
                                 :preview preview)
                          (dissoc :payload-path))]
    (ensure-dir! item-dir*)
    (write-bytes! payload-file* bytes)
    (spit meta-file* (pr-str record))
    (item-record workspace-id* record)))

(defn- text-export-name
  [name]
  (let [base (or (nonblank-string (strip-extension name))
                 "document")]
    (safe-filename (str base ".txt"))))

(defn publish-artifact!
  "Publish an artifact into the shared workspace, preserving original bytes when
   available."
  [artifact-id & {:keys [workspace-id name]}]
  (let [artifact* (or (artifact/get-artifact artifact-id)
                      (throw (source-not-found-ex "artifact_id" artifact-id)))
        payload   (artifact/visible-artifact-download-data artifact-id)
        bytes     (:bytes payload)]
    (store-item! workspace-id
                 {:name              (or (nonblank-string name) (:name artifact*))
                  :title             (:title artifact*)
                  :source-type       :artifact
                  :source-id         (str (:id artifact*))
                  :source-session-id (:session-id artifact*)
                  :kind              (:kind artifact*)
                  :media-type        (or (:media-type artifact*)
                                          "application/octet-stream")
                  :meta              (:meta artifact*)
                  :preview           (:preview artifact*)
                  :text-derived?     false}
                 bytes)))

(defn publish-local-doc!
  "Publish a local document into the shared workspace.

  Xia only retains normalized text for local documents, so non-text originals
   are exported as derived `.txt` files with provenance metadata."
  [doc-id & {:keys [workspace-id name]}]
  (let [doc          (or (local-doc/visible-doc-export-data doc-id)
                         (throw (source-not-found-ex "doc_id" doc-id)))
        original-name (:name doc)
        media-type    (:media-type doc)
        text-derived? (not (text-media-type? media-type))
        export-name   (or (nonblank-string name)
                          (if text-derived?
                            (text-export-name original-name)
                            original-name))
        export-type   (if text-derived? "text/plain" (or media-type "text/plain"))
        text          (or (:text doc) "")
        metadata      {:name              export-name
                       :source-type       :local-doc
                       :source-id         (str (:id doc))
                       :source-session-id (:session-id doc)
                       :media-type        export-type
                       :summary           (:summary doc)
                       :preview           (or (:preview doc)
                                              (preview-text text))
                       :text-derived?     text-derived?
                       :original-name     (when text-derived? original-name)
                       :original-media-type (when text-derived? media-type)}]
    (store-item! workspace-id metadata (utf8-bytes text))))

(defn write-note!
  "Write a shared note directly into the shared workspace."
  [content & {:keys [workspace-id title name media-type]}]
  (let [content* (nonblank-string content)]
    (when-not content*
      (throw (ex-info "workspace note content is required"
                      {:type :workspace/missing-content
                       :field "content"})))
    (store-item! workspace-id
                 {:name          (or (nonblank-string name)
                                     (note-filename title))
                  :title         (nonblank-string title)
                  :source-type   :note
                  :media-type    (or (nonblank-string media-type)
                                     "text/markdown")
                  :preview       (preview-text content*)
                  :text-derived? false}
                 (utf8-bytes content*))))

(defn import-item-as-local-doc!
  "Import a shared workspace item into the current session as a local
   document."
  [item-id & {:keys [workspace-id session-id name ocr-mode]}]
  (let [{:keys [bytes media-type size-bytes]
         item-name :name} (read-item-bytes workspace-id item-id)
        session-id* (or session-id (require-session-id))]
    (local-doc/save-upload! {:session-id session-id*
                             :name       (or (nonblank-string name) item-name)
                             :media-type media-type
                             :size-bytes size-bytes
                             :source     :workspace
                             :bytes      bytes
                             :ocr-mode   ocr-mode})))

(defn import-item-as-artifact!
  "Import a shared workspace item into the current session as an artifact."
  [item-id & {:keys [workspace-id session-id name title]}]
  (let [{:keys [bytes media-type source-type source-id]
         item-name :name
         item-title :title
         item-workspace-id :workspace-id
         item-id* :id
         :as item} (read-item-bytes workspace-id item-id)
        session-id* (or session-id (require-session-id))]
    (artifact/create-artifact! {:session-id session-id*
                                :name       (or (nonblank-string name) item-name)
                                :title      (or (nonblank-string title) item-title)
                                :media-type media-type
                                :source     :workspace
                                :meta       {:workspace-item-id (str item-id*)
                                             :workspace-id      item-workspace-id
                                             :workspace-source-type source-type
                                             :workspace-source-id   source-id}
                                :bytes      bytes})))
