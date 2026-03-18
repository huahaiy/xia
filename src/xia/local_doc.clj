(ns xia.local-doc
  "Session-scoped local documents uploaded explicitly by the user."
  (:require [clojure.string :as str]
            [xia.db :as db]
            [xia.memory :as memory]
            [xia.working-memory :as wm]
            [xia.scratch :as scratch])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream IOException]
           [java.math BigInteger]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util Base64 Date UUID]
           [java.util.zip ZipEntry ZipInputStream]
           [javax.xml XMLConstants]
           [javax.xml.parsers DocumentBuilderFactory]
           [org.apache.pdfbox Loader]
           [org.apache.pdfbox.text PDFTextStripper]
           [org.w3c.dom Document Node]))

(def ^:private default-name "Untitled upload")
(def ^:private default-source :upload)
(def ^:private preview-char-limit 1200)
(def ^:private pdf-media-type "application/pdf")
(def ^:private docx-media-type "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
(def ^:private xlsx-media-type "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
(def ^:private pptx-media-type "application/vnd.openxmlformats-officedocument.presentationml.presentation")

(def ^:private extension->media-type
  {"c" "text/x-c"
   "cc" "text/x-c++"
   "clj" "text/x-clojure"
   "cljs" "text/x-clojure"
   "cljc" "text/x-clojure"
   "cpp" "text/x-c++"
   "csv" "text/csv"
   "docx" docx-media-type
   "edn" "application/edn"
   "htm" "text/html"
   "html" "text/html"
   "java" "text/x-java"
   "js" "text/javascript"
   "json" "application/json"
   "log" "text/plain"
   "md" "text/markdown"
   "markdown" "text/markdown"
   "pptx" pptx-media-type
   "py" "text/x-python"
   "rb" "text/x-ruby"
   "sh" "text/x-shellscript"
   "sql" "text/x-sql"
   "text" "text/plain"
   "ts" "text/typescript"
   "tsv" "text/tab-separated-values"
   "txt" "text/plain"
   "xlsx" xlsx-media-type
   "xml" "application/xml"
   "yaml" "application/yaml"
   "yml" "application/yaml"})

(def ^:private supported-media-types
  #{"application/edn"
    "application/json"
    docx-media-type
    pptx-media-type
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
    "text/x-sql"
    xlsx-media-type})

(def ^:private office-media-types
  #{docx-media-type
    xlsx-media-type
    pptx-media-type})

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
  (normalize-uuid session-id "session_id" :local-doc/invalid-session-id))

(defn- require-session-id
  []
  (or wm/*session-id*
      (throw (ex-info "session id is required"
                      {:type :local-doc/session-required}))))

(defn- normalize-doc-id
  [doc-id]
  (normalize-uuid doc-id "doc_id" :local-doc/invalid-doc-id))

(defn- normalize-name
  [name]
  (let [value (some-> name str str/trim)]
    (if (seq value) value default-name)))

(defn- file-extension
  [filename]
  (let [name (normalize-name filename)
        idx  (.lastIndexOf ^String name ".")]
    (when (and (pos? idx) (< idx (dec (count name))))
      (-> (.substring ^String name (inc idx))
          str/lower-case
          str/trim))))

(defn- base-media-type
  [media-type]
  (some-> media-type
          str
          str/lower-case
          (str/split #";")
          first
          str/trim
          not-empty))

(defn- pdf-bytes-required-ex
  [name media-type]
  (ex-info "PDF uploads require file bytes"
           {:type :local-doc/pdf-bytes-required
            :name name
            :media-type media-type}))

(defn- office-bytes-required-ex
  [name media-type]
  (ex-info "Office document uploads require file bytes"
           {:type :local-doc/office-bytes-required
            :name name
            :media-type media-type}))

(defn- unsupported-format-ex
  [name media-type]
  (ex-info "Unsupported local document format"
           {:type :local-doc/unsupported-format
            :name name
            :media-type media-type}))

(defn- normalize-media-type
  [name media-type]
  (let [filename     (normalize-name name)
        ext          (file-extension filename)
        media-type*  (base-media-type media-type)
        ext-media    (get extension->media-type ext)
        octet-stream? (= "application/octet-stream" media-type*)]
    (cond
      (or (= "application/pdf" media-type*)
          (= "pdf" ext))
      pdf-media-type

      (and media-type* (or (contains? supported-media-types media-type*)
                           (str/starts-with? media-type* "text/")))
      media-type*

      (and (or (nil? media-type*) octet-stream?) ext-media)
      ext-media

      ext-media
      ext-media

      :else
      (throw (unsupported-format-ex filename media-type)))))

(defn- normalize-text
  [text]
  (let [value (or text "")]
    (if (and (string? value)
             (str/starts-with? value "\uFEFF"))
      (subs value 1)
      (str value))))

(defn- office-media-type?
  [media-type]
  (contains? office-media-types media-type))

(defn- previewable-binary-media-type?
  [media-type]
  (and media-type
       (not= media-type pdf-media-type)
       (not (office-media-type? media-type))))

(defn- utf8-bytes
  [text]
  (.getBytes ^String text StandardCharsets/UTF_8))

(defn- decode-base64
  [text]
  (.decode (Base64/getDecoder) ^String text))

(defn- normalize-size-bytes
  [declared-size actual-size]
  (cond
    (integer? declared-size) (long declared-size)
    (string? declared-size)  (Long/parseLong declared-size)
    :else                    (long actual-size)))

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

(defn- extract-pdf-text
  [name media-type ^bytes pdf-bytes]
  (try
    (with-open [doc (Loader/loadPDF pdf-bytes)]
      (let [stripper (doto (PDFTextStripper.)
                       (.setSortByPosition true)
                       (.setAddMoreFormatting true))]
        (normalize-text (.getText stripper doc))))
    (catch IOException e
      (throw (ex-info (.getMessage e)
                      {:type :local-doc/pdf-extraction-failed
                       :name name
                       :media-type media-type}
                      e)))))

(defn- office-extraction-failed
  [type-key name media-type cause]
  (throw (ex-info (.getMessage ^Throwable cause)
                  {:type type-key
                   :name name
                   :media-type media-type}
                  cause)))

(defn- nonblank-lines
  [lines]
  (->> lines
       (map #(some-> % normalize-text str/trim))
       (remove str/blank?)))

(defn- millis->date
  [millis]
  (when (some? millis)
    (Date. (long millis))))

(defn- byte-array-length
  [data]
  (when data
    (alength ^bytes data)))

(defn- copy-input-stream-bytes
  [in]
  (let [^ByteArrayOutputStream out (ByteArrayOutputStream.)
        buffer (byte-array 8192)]
    (loop [read-count (.read ^java.io.InputStream in buffer)]
      (when (pos? read-count)
        (.write out buffer 0 read-count)
        (recur (.read ^java.io.InputStream in buffer))))
    (.toByteArray out)))

(defn- zip-entry-bytes-map
  [^bytes archive-bytes]
  (with-open [in (ByteArrayInputStream. archive-bytes)
              ^ZipInputStream zip (ZipInputStream. in)]
    (loop [^ZipEntry entry (.getNextEntry zip)
           acc {}]
      (if-not entry
        acc
        (let [entry-name (.getName entry)
              next-acc (if (.isDirectory entry)
                         acc
                         (assoc acc entry-name (copy-input-stream-bytes zip)))]
          (.closeEntry zip)
          (recur (.getNextEntry zip) next-acc))))))

(defn- secure-document-builder-factory
  []
  (doto (DocumentBuilderFactory/newInstance)
    (.setNamespaceAware true)
    (.setExpandEntityReferences false)
    (.setXIncludeAware false)
    (.setFeature XMLConstants/FEATURE_SECURE_PROCESSING true)
    (.setFeature "http://apache.org/xml/features/disallow-doctype-decl" true)))

(defn- parse-xml-bytes
  [^bytes xml-bytes]
  (with-open [in (ByteArrayInputStream. xml-bytes)]
    (let [^DocumentBuilderFactory factory (secure-document-builder-factory)
          ^Document document (.parse (.newDocumentBuilder factory) in)]
      document)))

(defn- child-nodes
  [node]
  (let [children (.getChildNodes ^org.w3c.dom.Node node)
        total    (.getLength children)]
    (loop [idx 0
           acc []]
      (if (>= idx total)
        acc
        (recur (inc idx) (conj acc (.item children idx)))))))

(defn- element-node?
  [node]
  (= org.w3c.dom.Node/ELEMENT_NODE
     (.getNodeType ^org.w3c.dom.Node node)))

(defn- node-local-name
  [node]
  (let [^org.w3c.dom.Node node node
        local-name (.getLocalName node)]
    (or local-name (.getNodeName node))))

(defn- descendant-elements-by-local-name
  [node local-name]
  (->> (tree-seq #(seq (child-nodes %)) child-nodes node)
       rest
       (filter #(and (element-node? %)
                     (= local-name (node-local-name %))))))

(defn- direct-child-elements-by-local-name
  [node local-name]
  (->> (child-nodes node)
       (filter #(and (element-node? %)
                     (= local-name (node-local-name %))))))

(defn- node-text
  [node]
  (when node
    (let [text (.getTextContent ^Node node)]
      (some-> text
              normalize-text
              str/trim
              not-empty))))

(defn- element-attr
  [element attr-name]
  (let [value (.getAttribute ^org.w3c.dom.Element element ^String attr-name)]
    (not-empty (str/trim value))))

(def ^:private office-rel-ns
  "http://schemas.openxmlformats.org/officeDocument/2006/relationships")

(defn- element-attr-ns
  [element namespace-uri local-name fallback-attr]
  (or (let [value (.getAttributeNS ^org.w3c.dom.Element element
                                   ^String namespace-uri
                                   ^String local-name)]
        (not-empty (str/trim value)))
      (element-attr element fallback-attr)))

(defn- path-sort-key
  [path]
  (let [idx (some-> (re-find #"\d+" (str path))
                    Integer/parseInt)]
    [(or idx Long/MAX_VALUE) (str path)]))

(defn- docx-paragraph-text
  [paragraph]
  (->> (descendant-elements-by-local-name paragraph "t")
       (keep node-text)
       (str/join "")))

(defn- office-relationships
  [entries rels-path prefix]
  (if-let [rels-bytes (get entries rels-path)]
    (let [^Document rels-doc (parse-xml-bytes rels-bytes)
          doc (.getDocumentElement rels-doc)]
      (into {}
            (keep (fn [relationship]
                    (let [id     (element-attr relationship "Id")
                          target (some-> (element-attr relationship "Target")
                                         (str/replace #"^\./" "")
                                         (#(if (str/starts-with? % "/")
                                             (subs % 1)
                                             %))
                                         (#(if (str/starts-with? % prefix)
                                             %
                                             (str prefix %))))]
                      (when (and id target)
                        [id target]))))
            (descendant-elements-by-local-name doc "Relationship")))
    {}))

(defn- shared-string-item-text
  [si]
  (->> (descendant-elements-by-local-name si "t")
       (keep node-text)
       (str/join "")))

(defn- xlsx-shared-strings
  [entries]
  (if-let [shared-strings-bytes (get entries "xl/sharedStrings.xml")]
    (let [^Document shared-doc (parse-xml-bytes shared-strings-bytes)
          doc (.getDocumentElement shared-doc)]
      (mapv shared-string-item-text
            (descendant-elements-by-local-name doc "si")))
    []))

(defn- xlsx-cell-text
  [cell shared-strings]
  (let [cell-type     (element-attr cell "t")
        value-node    (first (descendant-elements-by-local-name cell "v"))
        inline-node   (first (descendant-elements-by-local-name cell "is"))
        inline-text   (some-> inline-node shared-string-item-text)
        value-text    (node-text value-node)
        resolved-text (case cell-type
                        "s" (when value-text
                              (nth shared-strings (Integer/parseInt value-text) ""))
                        "inlineStr" inline-text
                        "str" value-text
                        (or value-text inline-text ""))]
    (normalize-text resolved-text)))

(defn- xlsx-sheet-descriptors
  [entries]
  (let [worksheet-paths (->> (keys entries)
                             (filter #(re-matches #"xl/worksheets/sheet\d+\.xml" %))
                             (sort-by path-sort-key)
                             vec)
        rel-map         (office-relationships entries "xl/_rels/workbook.xml.rels" "xl/")
        workbook-bytes  (get entries "xl/workbook.xml")]
    (if workbook-bytes
      (let [^Document workbook-doc (parse-xml-bytes workbook-bytes)
            doc (.getDocumentElement workbook-doc)
            by-workbook
            (->> (descendant-elements-by-local-name doc "sheet")
                 (map-indexed
                   (fn [idx sheet]
                     (let [entry (or (some-> (element-attr-ns sheet office-rel-ns "id" "r:id")
                                             rel-map)
                                     (nth worksheet-paths idx nil))]
                       (when entry
                         {:name  (or (element-attr sheet "name")
                                     (str "Sheet " (inc idx)))
                          :entry entry}))))
                 (remove nil?)
                 vec)]
        (if (seq by-workbook)
          by-workbook
          (mapv (fn [idx path]
                  {:name (str "Sheet " (inc idx))
                   :entry path})
                (range (count worksheet-paths))
                worksheet-paths)))
      (mapv (fn [idx path]
              {:name (str "Sheet " (inc idx))
               :entry path})
            (range (count worksheet-paths))
            worksheet-paths))))

(defn- slide-entry-paths
  [entries]
  (->> (keys entries)
       (filter #(re-matches #"ppt/slides/slide\d+\.xml" %))
       (sort-by path-sort-key)
       vec))

(defn- extract-docx-text
  [name media-type ^bytes docx-bytes]
  (try
    (let [entries        (zip-entry-bytes-map docx-bytes)
          document-bytes (or (get entries "word/document.xml")
                             (throw (ex-info "Missing word/document.xml" {})))
          ^Document document-doc (parse-xml-bytes document-bytes)
          document-root  (.getDocumentElement document-doc)]
      (->> (descendant-elements-by-local-name document-root "p")
           (keep docx-paragraph-text)
           (remove str/blank?)
           (str/join "\n")
           normalize-text))
    (catch Exception e
      (office-extraction-failed :local-doc/docx-extraction-failed name media-type e))))

(defn- extract-xlsx-text
  [name media-type ^bytes xlsx-bytes]
  (try
    (let [entries         (zip-entry-bytes-map xlsx-bytes)
          shared-strings  (xlsx-shared-strings entries)
          sheet-sections  (for [{:keys [name entry]} (xlsx-sheet-descriptors entries)
                                :let [sheet-bytes (get entries entry)]
                                :when sheet-bytes]
                            (let [^Document sheet-doc (parse-xml-bytes sheet-bytes)
                                  sheet-root (.getDocumentElement sheet-doc)
                                  rows       (->> (descendant-elements-by-local-name sheet-root "row")
                                                  (map (fn [row]
                                                         (->> (direct-child-elements-by-local-name row "c")
                                                              (map #(xlsx-cell-text % shared-strings))
                                                              (remove str/blank?)
                                                              (str/join "\t"))))
                                                  (remove str/blank?))]
                              (str/join "\n" (cons (str "## Sheet: " name) rows))))]
      (normalize-text (str/join "\n\n" (remove str/blank? sheet-sections))))
    (catch Exception e
      (office-extraction-failed :local-doc/xlsx-extraction-failed name media-type e))))

(defn- extract-pptx-text
  [name media-type ^bytes pptx-bytes]
  (try
    (let [entries        (zip-entry-bytes-map pptx-bytes)
          slide-sections (map-indexed
                           (fn [idx entry-path]
                             (let [^Document slide-doc (parse-xml-bytes (get entries entry-path))
                                   slide-root (.getDocumentElement slide-doc)
                                   text-lines (->> (descendant-elements-by-local-name slide-root "t")
                                                   (keep node-text))]
                               (str/join "\n"
                                         (cons (str "## Slide " (inc idx))
                                               (nonblank-lines text-lines)))))
                           (slide-entry-paths entries))]
      (normalize-text (str/join "\n\n" (remove str/blank? slide-sections))))
    (catch Exception e
      (office-extraction-failed :local-doc/pptx-extraction-failed name media-type e))))

(defn- extract-office-text
  [name media-type ^bytes office-bytes]
  (case media-type
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    (extract-docx-text name media-type office-bytes)

    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    (extract-xlsx-text name media-type office-bytes)

    "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    (extract-pptx-text name media-type office-bytes)))

(defn- upload-source
  [{:keys [name media-type text bytes bytes-base64]}]
  (let [name*       (normalize-name name)
        media-type* (normalize-media-type name* media-type)]
    (cond
      (= pdf-media-type media-type*)
      (if-let [source-bytes (or bytes
                                (some-> bytes-base64 str str/trim not-empty decode-base64))]
        (let [source-bytes ^bytes source-bytes]
          {:media-type   media-type*
           :source-bytes source-bytes
           :text         (extract-pdf-text name* media-type* source-bytes)})
        (throw (pdf-bytes-required-ex name* media-type*)))

      (office-media-type? media-type*)
      (if-let [source-bytes (or bytes
                                (some-> bytes-base64 str str/trim not-empty decode-base64))]
        (let [source-bytes ^bytes source-bytes]
          {:media-type   media-type*
           :source-bytes source-bytes
           :text         (extract-office-text name* media-type* source-bytes)})
        (throw (office-bytes-required-ex name* media-type*)))

      (some? bytes)
      (let [source-bytes ^bytes bytes]
        {:media-type   media-type*
         :source-bytes source-bytes
         :text         (normalize-text (String. source-bytes StandardCharsets/UTF_8))})

      (some? bytes-base64)
      (let [source-bytes (decode-base64 (str bytes-base64))]
        {:media-type   media-type*
         :source-bytes source-bytes
         :text         (normalize-text (String. ^bytes source-bytes StandardCharsets/UTF_8))})

      :else
      (let [text* (normalize-text text)]
        {:media-type   media-type*
         :source-bytes (utf8-bytes text*)
         :text         text*}))))

(defn- raw-upload-bytes
  [{:keys [text bytes bytes-base64]}]
  (cond
    (some? bytes)
    ^bytes bytes

    (some? bytes-base64)
    (decode-base64 (str bytes-base64))

    (some? text)
    (utf8-bytes (normalize-text text))

    :else
    nil))

(defn- best-effort-media-type
  [name media-type]
  (let [name*      (normalize-name name)
        ext        (file-extension name*)
        media-type* (base-media-type media-type)]
    (or (when (or (= media-type* pdf-media-type)
                  (= ext "pdf"))
          pdf-media-type)
        media-type*
        (get extension->media-type ext))))

(defn- text-preview-for-failure
  [{:keys [text bytes bytes-base64 name media-type]}]
  (let [media-type* (best-effort-media-type name media-type)]
    (cond
      (seq text)
      (preview-text (normalize-text text))

      (and (previewable-binary-media-type? media-type*)
           (some? bytes))
      (preview-text (normalize-text (String. ^bytes bytes StandardCharsets/UTF_8)))

      (and (previewable-binary-media-type? media-type*)
           (some? bytes-base64))
      (preview-text (normalize-text (String. ^bytes (decode-base64 (str bytes-base64))
                                                   StandardCharsets/UTF_8)))

      :else
      nil)))

(defn- entity-created-at
  [entity-map]
  (some-> (:db/created-at entity-map) long millis->date))

(defn- entity-updated-at
  [entity-map]
  (some-> (:db/updated-at entity-map) long millis->date))

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

(defn- existing-doc-eid
  [session-eid sha256]
  (ffirst (db/q '[:find ?e
                  :in $ ?session ?sha256
                  :where
                  [?e :local.doc/session ?session]
                  [?e :local.doc/sha256 ?sha256]]
                session-eid
                sha256)))

(defn- doc-eid
  [doc-id]
  (ffirst (db/q '[:find ?e :in $ ?id :where [?e :local.doc/id ?id]]
                 doc-id)))

(defn- doc-session-id
  [eid]
  (ffirst (db/q '[:find ?sid
                  :in $ ?doc
                  :where
                  [?doc :local.doc/session ?session]
                  [?session :session/id ?sid]]
                eid)))

(defn- session-not-found-ex
  [session-id]
  (ex-info "session not found"
           {:type :local-doc/session-not-found
            :session-id (str session-id)}))

(defn- doc-not-found-ex
  [doc-id]
  (ex-info "local document not found"
           {:type :local-doc/not-found
            :doc-id (str doc-id)}))

(defn- upload-episode-context
  [{:keys [media-type size-bytes sha256 preview existing? doc-id]}]
  (let [lines (cond-> [(str "Media type: " media-type)
                       (str "Size bytes: " size-bytes)
                       (str "SHA256: " sha256)
                       (str "Stored document id: " doc-id)
                       (str "Reused existing stored document: " (if existing? "yes" "no"))]
                (seq preview)
                (conj (str "Preview: " preview)))]
    (str/join "\n" lines)))

(defn- event-episode
  [{:keys [session-id session-channel summary context]}]
  (cond-> {:episode/id        (random-uuid)
           :episode/type      :event
           :episode/summary   summary
           :episode/context   context
           :episode/timestamp (java.util.Date.)
           :episode/processed? false
           :episode/session-id (str session-id)}
    session-channel
    (assoc :episode/channel (clojure.core/name session-channel))))

(defn- upload-episode
  [{:keys [session-id session-channel name media-type size-bytes sha256 preview existing? doc-id]}]
  (event-episode {:session-id session-id
                  :session-channel session-channel
                  :summary (str "Uploaded local document " name)
                  :context (upload-episode-context {:media-type media-type
                                                    :size-bytes size-bytes
                                                    :sha256 sha256
                                                    :preview preview
                                                    :existing? existing?
                                                    :doc-id doc-id})}))

(defn- failed-upload-episode-context
  [{:keys [media-type size-bytes sha256 preview doc-id error-message]}]
  (let [lines (cond-> []
                (seq media-type)
                (conj (str "Media type: " media-type))
                (some? size-bytes)
                (conj (str "Size bytes: " size-bytes))
                (seq sha256)
                (conj (str "SHA256: " sha256))
                doc-id
                (conj (str "Stored failed document id: " doc-id))
                (seq error-message)
                (conj (str "Error: " error-message))
                (seq preview)
                (conj (str "Preview: " preview)))]
    (str/join "\n" lines)))

(defn- failed-upload-episode
  [{:keys [session-id session-channel name media-type size-bytes sha256 preview doc-id error-message]}]
  (event-episode {:session-id session-id
                  :session-channel session-channel
                  :summary (str "Failed to upload local document " name)
                  :context (failed-upload-episode-context {:media-type media-type
                                                           :size-bytes size-bytes
                                                           :sha256 sha256
                                                           :preview preview
                                                           :doc-id doc-id
                                                           :error-message error-message})}))

(defn- delete-episode-context
  [{:keys [media-type size-bytes sha256 preview doc-id]}]
  (let [lines (cond-> [(str "Media type: " media-type)
                       (str "Size bytes: " size-bytes)
                       (str "SHA256: " sha256)
                       (str "Deleted document id: " doc-id)]
                (seq preview)
                (conj (str "Preview: " preview)))]
    (str/join "\n" lines)))

(defn- delete-episode
  [{:keys [session-id session-channel name media-type size-bytes sha256 preview doc-id]}]
  (event-episode {:session-id session-id
                  :session-channel session-channel
                  :summary (str "Deleted local document " name)
                  :context (delete-episode-context {:media-type media-type
                                                    :size-bytes size-bytes
                                                    :sha256 sha256
                                                    :preview preview
                                                    :doc-id doc-id})}))

(defn- scratch-pad-episode-context
  [{:keys [name pad-id preview]}]
  (let [lines (cond-> [(str "Source document: " name)
                       (str "Created scratch pad id: " pad-id)]
                (seq preview)
                (conj (str "Preview: " preview)))]
    (str/join "\n" lines)))

(defn- scratch-pad-episode
  [{:keys [session-id session-channel name pad-id preview]}]
  (event-episode {:session-id session-id
                  :session-channel session-channel
                  :summary (str "Created note from local document " name)
                  :context (scratch-pad-episode-context {:name name
                                                         :pad-id pad-id
                                                         :preview preview})}))

(defn- document-from-eid
  [eid]
  (when eid
    (let [entity (db/entity eid)]
      {:id         (:local.doc/id entity)
       :session-id (some-> (doc-session-id eid) str)
       :name       (:local.doc/name entity)
       :media-type (:local.doc/media-type entity)
       :source     (:local.doc/source entity)
       :size-bytes (:local.doc/size-bytes entity)
       :sha256     (:local.doc/sha256 entity)
       :status     (:local.doc/status entity)
       :error      (:local.doc/error entity)
       :text       (:local.doc/text entity)
       :preview    (:local.doc/preview entity)
       :created-at (entity-created-at entity)
       :updated-at (entity-updated-at entity)})))

(defn list-docs
  ([] (list-docs (require-session-id)))
  ([session-id]
   (let [session-id* (normalize-session-id session-id)
        session-eid* (session-eid session-id*)]
     (when-not session-eid*
       (throw (session-not-found-ex session-id*)))
     (->> (db/q '[:find ?e :in $ ?session :where [?e :local.doc/session ?session]]
                session-eid*)
          (map first)
          (map document-from-eid)
          (sort-by :updated-at #(compare %2 %1))
          vec))))

(defn get-doc
  [doc-id]
  (some-> doc-id normalize-doc-id doc-eid document-from-eid))

(defn get-session-doc
  [session-id doc-id]
  (let [session-id* (normalize-session-id session-id)
        doc-id*     (normalize-doc-id doc-id)
        doc         (some-> doc-id* doc-eid document-from-eid)]
    (when (and doc (= (str session-id*) (:session-id doc)))
      doc)))

(defn- read-session-doc
  [session-id doc-id & {:keys [offset max-chars]
                        :or {offset 0
                             max-chars 4000}}]
  (let [session-id* (normalize-session-id session-id)
        doc-id*     (normalize-doc-id doc-id)
        offset*     (max 0 (int (or offset 0)))
        max-chars*  (max 1 (int (or max-chars 4000)))]
    (when-let [doc (get-session-doc session-id* doc-id*)]
      (let [text        (or (:text doc) "")
            text-length (count text)
            start       (min offset* text-length)
            end         (min text-length (+ start max-chars*))]
        {:id          (:id doc)
         :name        (:name doc)
         :media-type  (:media-type doc)
         :status      (:status doc)
         :size-bytes  (:size-bytes doc)
         :preview     (:preview doc)
         :text        (subs text start end)
         :offset      start
         :end-offset  end
         :total-chars text-length
         :truncated?  (< end text-length)}))))

(defn search-docs
  "Search local documents visible to the current session."
  [query & {:keys [top fts-query] :or {top 5}}]
  (memory/search-local-docs (require-session-id)
                            query
                            :top top
                            :fts-query fts-query))

(defn read-doc
  "Read a local document visible to the current session, optionally slicing the text."
  [doc-id & {:keys [offset max-chars]
             :or {offset 0
                  max-chars 4000}}]
  (read-session-doc (require-session-id)
                    doc-id
                    :offset offset
                    :max-chars max-chars))

(defn save-upload!
  [{:keys [session-id name media-type size-bytes source text bytes bytes-base64]}]
  (let [session-id*  (normalize-session-id session-id)
        session-eid* (session-eid session-id*)]
    (when-not session-eid*
      (throw (session-not-found-ex session-id*)))
    (let [name*       (normalize-name name)
          {:keys [media-type source-bytes text]}
          (upload-source {:name name*
                          :media-type media-type
                          :text text
                          :bytes bytes
                          :bytes-base64 bytes-base64})
          text*       text
          sha256      (sha256-hex source-bytes)
          size-bytes* (normalize-size-bytes size-bytes (byte-array-length source-bytes))
          existing    (existing-doc-eid session-eid* sha256)
          doc-id      (or (some-> existing db/entity :local.doc/id) (random-uuid))
          preview     (preview-text text*)
          channel     (session-channel session-eid*)]
      (db/transact!
        [{:local.doc/id         doc-id
          :local.doc/session    session-eid*
          :local.doc/name       name*
          :local.doc/media-type media-type
          :local.doc/source     (or source default-source)
          :local.doc/size-bytes size-bytes*
          :local.doc/sha256     sha256
          :local.doc/status     :ready
          :local.doc/text       text*
          :local.doc/preview    preview}
         (upload-episode {:session-id session-id*
                         :session-channel channel
                         :name name*
                         :media-type media-type
                         :size-bytes size-bytes*
                         :sha256 sha256
                         :preview preview
                          :existing? (boolean existing)
                          :doc-id doc-id})])
      (document-from-eid (doc-eid doc-id)))))

(defn save-failed-upload!
  [{:keys [session-id name media-type size-bytes source text bytes bytes-base64]} error]
  (let [session-id*  (normalize-session-id session-id)
        session-eid* (session-eid session-id*)]
    (when-not session-eid*
      (throw (session-not-found-ex session-id*)))
    (let [name*          (normalize-name name)
          source-bytes   (try
                           (raw-upload-bytes {:text text
                                              :bytes bytes
                                              :bytes-base64 bytes-base64})
                           (catch Exception _
                             nil))
          media-type*    (best-effort-media-type name* media-type)
          size-bytes*    (cond
                           (some? size-bytes) (normalize-size-bytes size-bytes (or (byte-array-length source-bytes) 0))
                           source-bytes       (long (byte-array-length source-bytes))
                           :else              nil)
          sha256         (some-> source-bytes sha256-hex)
          preview        (text-preview-for-failure {:name name*
                                                    :media-type media-type*
                                                    :text text
                                                    :bytes bytes
                                                    :bytes-base64 bytes-base64})
          channel        (session-channel session-eid*)
          doc-id         (random-uuid)
          error-message  (.getMessage ^Throwable error)]
      (db/transact!
        [(cond-> {:local.doc/id      doc-id
                  :local.doc/session session-eid*
                  :local.doc/name    name*
                  :local.doc/source  (or source default-source)
                  :local.doc/status  :failed
                  :local.doc/error   error-message}
           media-type* (assoc :local.doc/media-type media-type*)
           (some? size-bytes*) (assoc :local.doc/size-bytes size-bytes*)
           sha256 (assoc :local.doc/sha256 sha256)
           preview (assoc :local.doc/preview preview))
         (failed-upload-episode {:session-id session-id*
                                 :session-channel channel
                                 :name name*
                                 :media-type media-type*
                                 :size-bytes size-bytes*
                                 :sha256 sha256
                                 :preview preview
                                 :doc-id doc-id
                                 :error-message error-message})])
      (document-from-eid (doc-eid doc-id)))))

(defn delete-doc!
  ([doc-id]
   (let [doc-id* (normalize-doc-id doc-id)
         eid     (doc-eid doc-id*)]
     (when-not eid
       (throw (doc-not-found-ex doc-id*)))
     (db/transact! [[:db/retractEntity eid]])
     {:status "deleted" :id (str doc-id*)}))
  ([session-id doc-id]
   (let [session-id*  (normalize-session-id session-id)
         session-eid* (session-eid session-id*)
         doc-id*      (normalize-doc-id doc-id)
         doc          (get-session-doc session-id* doc-id*)
         eid          (doc-eid doc-id*)
         channel      (session-channel session-eid*)]
     (when-not session-eid*
       (throw (session-not-found-ex session-id*)))
     (when-not (and doc eid)
       (throw (doc-not-found-ex doc-id*)))
     (db/transact! [(delete-episode {:session-id session-id*
                                     :session-channel channel
                                     :name (:name doc)
                                     :media-type (:media-type doc)
                                     :size-bytes (:size-bytes doc)
                                     :sha256 (:sha256 doc)
                                     :preview (:preview doc)
                                     :doc-id doc-id*})
                    [:db/retractEntity eid]])
     {:status "deleted" :id (str doc-id*)})))

(defn create-scratch-pad-from-doc!
  [session-id doc-id]
  (let [session-id*  (normalize-session-id session-id)
        session-eid* (session-eid session-id*)
        doc-id*      (normalize-doc-id doc-id)
        doc          (get-session-doc session-id* doc-id*)
        channel      (session-channel session-eid*)]
    (when-not session-eid*
      (throw (session-not-found-ex session-id*)))
    (when-not doc
      (throw (doc-not-found-ex doc-id*)))
    (let [pad (scratch/create-pad! {:scope      :session
                                    :session-id (str session-id*)
                                    :title      (:name doc)
                                    :content    (:text doc)})]
      (try
        (db/transact! [(scratch-pad-episode {:session-id session-id*
                                             :session-channel channel
                                             :name (:name doc)
                                             :pad-id (:id pad)
                                             :preview (:preview doc)})])
        {:document doc
         :pad      pad}
        (catch Throwable t
          (try
            (scratch/delete-pad! (:id pad))
            (catch Exception _))
          (throw t))))))
