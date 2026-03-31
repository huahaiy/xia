(ns xia.channel.http.workspace
  "Scratch pad, local document, artifact, and workspace-item HTTP handlers."
  (:require [clojure.java.io :as io]
            [xia.artifact :as artifact]
            [xia.local-doc :as local-doc]
            [xia.paths :as paths]
            [xia.scratch :as scratch]
            [xia.workspace :as workspace])
  (:import [java.nio.file Files]))

(defn- json-response*
  [deps status body]
  ((:json-response deps) status body))

(defn- exception-response*
  [deps throwable]
  ((:exception-response deps) throwable))

(defn- instant->str*
  [deps value]
  ((:instant->str deps) value))

(defn- touch-rest-session!*
  [deps session-id]
  ((:touch-rest-session! deps) session-id))

(defn- parse-session-id*
  [deps session-id]
  ((:parse-session-id deps) session-id))

(defn- session-exists?*
  [deps session-id]
  ((:session-exists? deps) session-id))

(defn- read-body*
  [deps req]
  ((:read-body deps) req))

(defn- read-body-bytes*
  [deps body]
  ((:read-body-bytes deps) body))

(defn- multipart-form-request?*
  [deps req]
  ((:multipart-form-request? deps) req))

(defn- download-response*
  [deps name media-type bytes]
  ((:download-response deps) name media-type bytes))

(defn- throwable-message*
  [deps throwable]
  ((:throwable-message deps) throwable))

(defn- nonblank-str*
  [deps value]
  ((:nonblank-str deps) value))

(defn- parse-optional-positive-long*
  [deps value field-name]
  ((:parse-optional-positive-long deps) value field-name))

(defn- parse-query-string*
  [deps query-string]
  ((:parse-query-string deps) query-string))

(defn- scratch-pad->body
  [deps pad]
  {:id         (:id pad)
   :scope      (name (:scope pad))
   :session_id (:session-id pad)
   :title      (:title pad)
   :content    (:content pad)
   :mime       (:mime pad)
   :version    (:version pad)
   :created_at (instant->str* deps (:created-at pad))
   :updated_at (instant->str* deps (:updated-at pad))})

(defn- scratch-metadata->body
  [deps pad]
  (dissoc (scratch-pad->body deps pad) :content))

(defn- local-doc->body
  [deps doc]
  {:id         (some-> (:id doc) str)
   :session_id (:session-id doc)
   :name       (:name doc)
   :media_type (:media-type doc)
   :source     (some-> (:source doc) name)
   :size_bytes (:size-bytes doc)
   :sha256     (:sha256 doc)
   :status     (some-> (:status doc) name)
   :error      (:error doc)
   :summary    (:summary doc)
   :text       (:text doc)
   :preview    (:preview doc)
   :chunk_count (:chunk-count doc)
   :created_at (instant->str* deps (:created-at doc))
   :updated_at (instant->str* deps (:updated-at doc))})

(defn- local-doc-metadata->body
  [deps doc]
  (dissoc (local-doc->body deps doc) :text :sha256))

(defn- artifact->body
  [deps artifact]
  {:id         (some-> (:id artifact) str)
   :session_id (:session-id artifact)
   :name       (:name artifact)
   :title      (:title artifact)
   :kind       (some-> (:kind artifact) name)
   :media_type (:media-type artifact)
   :extension  (:extension artifact)
   :source     (some-> (:source artifact) name)
   :status     (some-> (:status artifact) name)
   :size_bytes (:size-bytes artifact)
   :compressed_size_bytes (:compressed-size-bytes artifact)
   :has_blob   (boolean (:has-blob? artifact))
   :text_available (boolean (:text-available? artifact))
   :sha256     (:sha256 artifact)
   :error      (:error artifact)
   :meta       (:meta artifact)
   :text       (:text artifact)
   :preview    (:preview artifact)
   :created_at (instant->str* deps (:created-at artifact))
   :updated_at (instant->str* deps (:updated-at artifact))})

(defn- artifact-metadata->body
  [deps artifact]
  (dissoc (artifact->body deps artifact) :text :sha256 :meta))

(defn- workspace-item->body
  [deps item]
  (let [item-id (some-> (:id item) str)]
    {:id           item-id
     :workspace_id (:workspace-id item)
     :name         (:name item)
     :title        (:title item)
     :source_type  (some-> (:source-type item) name)
     :source_id    (:source-id item)
     :media_type   (:media-type item)
     :size_bytes   (:size-bytes item)
     :created_at   (instant->str* deps (:created-at item))
     :download_url (when item-id
                     (str "/workspace/items/" item-id "/download"))}))

(defn- session-scratch-pad
  [session-id pad-id]
  (let [pad (scratch/get-pad pad-id)]
    (when (and pad
               (= :session (:scope pad))
               (= session-id (:session-id pad)))
      pad)))

(defn- session-local-doc
  [session-id doc-id]
  (local-doc/get-session-doc session-id doc-id))

(defn- session-artifact
  [session-id artifact-id]
  (artifact/get-session-artifact session-id artifact-id))

(defn handle-list-scratch-pads
  [deps session-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    :else
    (do
      (touch-rest-session!* deps session-id)
      (json-response* deps 200
                      {:session_id session-id
                       :pads       (into [] (map #(scratch-metadata->body deps %))
                                          (scratch/list-pads {:scope :session
                                                              :session-id session-id}))}))))

(defn handle-create-scratch-pad
  [deps session-id req]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    :else
    (let [data (or (read-body* deps req) {})
          pad  (scratch/create-pad! {:scope      :session
                                     :session-id session-id
                                     :title      (get data "title")
                                     :content    (get data "content")
                                     :mime       (get data "mime")})]
      (touch-rest-session!* deps session-id)
      (json-response* deps 201 {:session_id session-id
                                :pad        (scratch-pad->body deps pad)}))))

(defn handle-get-scratch-pad
  [deps session-id pad-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    :else
    (if-let [pad (session-scratch-pad session-id pad-id)]
      (do
        (touch-rest-session!* deps session-id)
        (json-response* deps 200 {:session_id session-id
                                  :pad        (scratch-pad->body deps pad)}))
      (json-response* deps 404 {:error "scratch pad not found"}))))

(defn handle-save-scratch-pad
  [deps session-id pad-id req]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response* deps 404 {:error "scratch pad not found"})

    :else
    (let [data    (or (read-body* deps req) {})
          updates (cond-> {}
                    (contains? data "title")            (assoc :title (get data "title"))
                    (contains? data "content")          (assoc :content (get data "content"))
                    (contains? data "mime")             (assoc :mime (get data "mime"))
                    (contains? data "expected_version") (assoc :expected-version
                                                               (get data "expected_version")))]
      (try
        (touch-rest-session!* deps session-id)
        (json-response* deps 200
                        {:session_id session-id
                         :pad        (scratch-pad->body deps (scratch/save-pad! pad-id updates))})
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type]} (ex-data e)]
            (case type
              :scratch/version-conflict
              (json-response* deps 409 {:error "scratch pad version conflict"
                                        :details (select-keys (ex-data e)
                                                              [:expected-version :actual-version])})
              :scratch/not-found
              (json-response* deps 404 {:error "scratch pad not found"})
              (json-response* deps 400 {:error (.getMessage e)}))))))))

(defn handle-edit-scratch-pad
  [deps session-id pad-id req]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response* deps 404 {:error "scratch pad not found"})

    :else
    (let [data      (or (read-body* deps req) {})
          operation (if (map? (get data "operation"))
                      (get data "operation")
                      data)
          edit      (cond-> {:op (get operation "op")}
                      (contains? operation "text")          (assoc :text (get operation "text"))
                      (contains? operation "separator")     (assoc :separator (get operation "separator"))
                      (contains? operation "match")         (assoc :match (get operation "match"))
                      (contains? operation "replacement")   (assoc :replacement (get operation "replacement"))
                      (contains? operation "occurrence")    (assoc :occurrence (get operation "occurrence"))
                      (contains? operation "offset")        (assoc :offset (get operation "offset"))
                      (contains? operation "start_line")    (assoc :start-line (get operation "start_line"))
                      (contains? operation "end_line")      (assoc :end-line (get operation "end_line"))
                      (contains? data "expected_version")   (assoc :expected-version
                                                                   (get data "expected_version"))
                      (contains? operation "expected_version") (assoc :expected-version
                                                                     (get operation "expected_version")))]
      (try
        (touch-rest-session!* deps session-id)
        (json-response* deps 200
                        {:session_id session-id
                         :pad        (scratch-pad->body deps (scratch/edit-pad! pad-id edit))})
        (catch clojure.lang.ExceptionInfo e
          (let [{:keys [type]} (ex-data e)]
            (case type
              :scratch/version-conflict
              (json-response* deps 409 {:error "scratch pad version conflict"
                                        :details (select-keys (ex-data e)
                                                              [:expected-version :actual-version])})
              :scratch/not-found
              (json-response* deps 404 {:error "scratch pad not found"})
              (json-response* deps 400 {:error (.getMessage e)}))))))))

(defn handle-delete-scratch-pad
  [deps session-id pad-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    (nil? (session-scratch-pad session-id pad-id))
    (json-response* deps 404 {:error "scratch pad not found"})

    :else
    (do
      (scratch/delete-pad! pad-id)
      (touch-rest-session!* deps session-id)
      (json-response* deps 200 {:status "deleted"
                                :session_id session-id
                                :pad_id pad-id}))))

(defn- parse-local-doc-upload
  [entry]
  {:name         (get entry "name")
   :media-type   (get entry "media_type")
   :size-bytes   (get entry "size_bytes")
   :source       (get entry "source")
   :bytes-base64 (get entry "bytes_base64")
   :ocr-mode     (get entry "ocr_mode")
   :text         (get entry "text")})

(defn- json-local-doc-upload-entries
  [deps data]
  (let [entries (cond
                  (sequential? (get data "documents"))
                  (get data "documents")

                  (map? data)
                  [data]

                  :else
                  [])]
    (->> entries
         (into [] (comp (filter map?)
                        (map parse-local-doc-upload))))))

(defn- multipart-local-doc-upload?
  [value]
  (and (map? value)
       (or (contains? value :tempfile)
           (contains? value "tempfile")
           (contains? value :filename)
           (contains? value "filename"))))

(defn- local-doc-part-bytes
  [deps part]
  (let [tempfile (or (:tempfile part) (get part "tempfile"))
        body     (or tempfile
                     (:bytes part)
                     (get part "bytes")
                     (:stream part)
                     (get part "stream"))]
    (when-not body
      (throw (ex-info "missing uploaded file bytes"
                      {:type :local-doc/missing-file-bytes
                       :name (or (:filename part) (get part "filename"))})))
    (try
      (read-body-bytes* deps body)
      (finally
        (when tempfile
          (.delete ^java.io.File tempfile))))))

(defn- multipart-local-doc-upload-entry
  [deps part]
  {:name       (or (:filename part) (get part "filename"))
   :media-type (or (:content-type part) (get part "content-type"))
   :size-bytes (or (:size part) (get part "size"))
   :source     (get part "source")
   :bytes      (local-doc-part-bytes deps part)})

(defn- multipart-local-doc-upload-entries
  [deps req]
  (let [params  (or (:multipart-params req) (:params req))
        uploads (or (get params "documents")
                    (get params :documents)
                    (get params "documents[]")
                    (get params :documents[]))]
    (->> (cond
           (sequential? uploads) uploads
           (some? uploads)       [uploads]
           :else                 [])
         (into [] (comp (filter multipart-local-doc-upload?)
                        (map #(multipart-local-doc-upload-entry deps %)))))))

(defn- local-doc-upload-entries
  [deps req]
  (if (multipart-form-request?* deps req)
    (multipart-local-doc-upload-entries deps req)
    (json-local-doc-upload-entries deps (or (read-body* deps req) {}))))

(defn- local-doc-error->body
  [deps upload throwable]
  {:name  (get upload :name)
   :error (throwable-message* deps throwable)
   :code  (some-> (ex-data throwable) :type name)})

(defn handle-list-local-docs
  [deps session-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    :else
    (do
      (touch-rest-session!* deps session-id)
      (json-response* deps 200
                      {:session_id session-id
                       :documents  (into [] (map #(local-doc-metadata->body deps %))
                                         (local-doc/list-docs session-id))}))))

(defn handle-create-local-docs
  [deps session-id req]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    :else
    (let [uploads (local-doc-upload-entries deps req)]
      (if-not (seq uploads)
        (json-response* deps 400 {:error "missing local documents"})
        (let [{:keys [documents errors]}
              (reduce (fn [acc upload]
                        (try
                          (update acc :documents conj
                                  (local-doc-metadata->body
                                   deps
                                   (local-doc/save-upload!
                                    {:session-id   session-id
                                     :name         (:name upload)
                                     :media-type   (:media-type upload)
                                     :size-bytes   (:size-bytes upload)
                                     :source       (:source upload)
                                     :bytes        (:bytes upload)
                                     :bytes-base64 (:bytes-base64 upload)
                                     :ocr-mode     (:ocr-mode upload)
                                     :text         (:text upload)})))
                          (catch clojure.lang.ExceptionInfo e
                            (let [failed-doc (try
                                               (local-doc/save-failed-upload!
                                                {:session-id   session-id
                                                 :name         (:name upload)
                                                 :media-type   (:media-type upload)
                                                 :size-bytes   (:size-bytes upload)
                                                 :source       (:source upload)
                                                 :bytes        (:bytes upload)
                                                 :bytes-base64 (:bytes-base64 upload)
                                                 :ocr-mode     (:ocr-mode upload)
                                                 :text         (:text upload)}
                                                e)
                                               (catch Exception _
                                                 nil))]
                              (cond-> (update acc :errors conj (local-doc-error->body deps upload e))
                                failed-doc
                                (update :documents conj (local-doc-metadata->body deps failed-doc)))))))
                      {:documents [] :errors []}
                      uploads)
              status (if (seq documents) 201 400)]
          (touch-rest-session!* deps session-id)
          (json-response* deps status
                          {:session_id session-id
                           :documents  documents
                           :errors     errors}))))))

(defn handle-get-local-doc
  [deps session-id doc-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    :else
    (if-let [doc (session-local-doc session-id doc-id)]
      (do
        (touch-rest-session!* deps session-id)
        (json-response* deps 200 {:session_id session-id
                                  :document   (local-doc->body deps doc)}))
      (json-response* deps 404 {:error "local document not found"}))))

(defn handle-delete-local-doc
  [deps session-id doc-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    (nil? (session-local-doc session-id doc-id))
    (json-response* deps 404 {:error "local document not found"})

    :else
    (do
      (local-doc/delete-doc! session-id doc-id)
      (touch-rest-session!* deps session-id)
      (json-response* deps 200 {:status "deleted"
                                :session_id session-id
                                :doc_id doc-id}))))

(defn handle-create-local-doc-scratch-pad
  [deps session-id doc-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    (nil? (session-local-doc session-id doc-id))
    (json-response* deps 404 {:error "local document not found"})

    :else
    (let [{:keys [pad]} (local-doc/create-scratch-pad-from-doc! session-id doc-id)]
      (touch-rest-session!* deps session-id)
      (json-response* deps 201 {:session_id session-id
                                :pad        (scratch-pad->body deps pad)}))))

(defn handle-create-artifact-scratch-pad
  [deps session-id artifact-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    (nil? (session-artifact session-id artifact-id))
    (json-response* deps 404 {:error "artifact not found"})

    :else
    (let [{:keys [pad]} (artifact/create-scratch-pad-from-artifact! session-id artifact-id)]
      (touch-rest-session!* deps session-id)
      (json-response* deps 201 {:session_id session-id
                                :pad        (scratch-pad->body deps pad)}))))

(defn- artifact-create-spec
  [deps data]
  (let [payload (if (map? (get data "artifact"))
                  (get data "artifact")
                  data)]
    (cond-> {:source (or (some-> (get payload "source") (nonblank-str* deps) keyword)
                         :manual)}
      (contains? payload "name")
      (assoc :name (get payload "name"))

      (contains? payload "title")
      (assoc :title (get payload "title"))

      (or (contains? payload "kind")
          (contains? payload "format"))
      (assoc :kind (or (get payload "kind")
                       (get payload "format")))

      (contains? payload "media_type")
      (assoc :media-type (get payload "media_type"))

      (contains? payload "content")
      (assoc :content (get payload "content"))

      (contains? payload "bytes_base64")
      (assoc :bytes-base64 (get payload "bytes_base64"))

      (contains? payload "preview")
      (assoc :preview (get payload "preview"))

      (contains? payload "rows")
      (assoc :rows (get payload "rows"))

      (contains? payload "data")
      (assoc :data (get payload "data"))

      (contains? payload "meta")
      (assoc :meta (get payload "meta")))))

(defn handle-list-artifacts
  [deps session-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    :else
    (do
      (touch-rest-session!* deps session-id)
      (json-response* deps 200
                      {:session_id session-id
                       :artifacts  (into [] (map #(artifact-metadata->body deps %))
                                         (artifact/list-artifacts session-id))}))))

(defn handle-create-artifact
  [deps session-id req]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    :else
    (try
      (let [data    (or (read-body* deps req) {})
            created (artifact/create-artifact! (assoc (artifact-create-spec deps data)
                                                      :session-id session-id))]
        (touch-rest-session!* deps session-id)
        (json-response* deps 201 {:session_id session-id
                                  :artifact   (artifact->body deps created)}))
      (catch clojure.lang.ExceptionInfo e
        (json-response* deps (or (:status (ex-data e)) 400)
                        {:error (.getMessage e)})))))

(defn handle-get-artifact
  [deps session-id artifact-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    :else
    (if-let [artifact (session-artifact session-id artifact-id)]
      (do
        (touch-rest-session!* deps session-id)
        (json-response* deps 200 {:session_id session-id
                                  :artifact   (artifact->body deps artifact)}))
      (json-response* deps 404 {:error "artifact not found"}))))

(defn handle-download-artifact
  [deps session-id artifact-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    :else
    (if-let [{:keys [name media-type bytes]} (artifact/artifact-download-data session-id artifact-id)]
      (do
        (touch-rest-session!* deps session-id)
        (download-response* deps name media-type (or bytes (byte-array 0))))
      (json-response* deps 404 {:error "artifact not found"}))))

(defn handle-delete-artifact
  [deps session-id artifact-id]
  (cond
    (nil? (parse-session-id* deps session-id))
    (json-response* deps 400 {:error "invalid session id"})

    (not (session-exists?* deps session-id))
    (json-response* deps 404 {:error "session not found"})

    (nil? (session-artifact session-id artifact-id))
    (json-response* deps 404 {:error "artifact not found"})

    :else
    (do
      (artifact/delete-artifact! session-id artifact-id)
      (touch-rest-session!* deps session-id)
      (json-response* deps 200 {:status "deleted"
                                :session_id session-id
                                :artifact_id artifact-id}))))

(defn handle-list-workspace-items
  [deps req]
  (try
    (let [params       (parse-query-string* deps (:query-string req))
          workspace-id (nonblank-str* deps (get params "workspace_id"))
          top          (parse-optional-positive-long* deps (get params "top") "top")
          items        (workspace/list-items :workspace-id workspace-id :top top)]
      (json-response* deps 200
                      {:workspace_id (or workspace-id paths/default-workspace-id)
                       :items        (into [] (map #(workspace-item->body deps %)) items)}))
    (catch clojure.lang.ExceptionInfo e
      (exception-response* deps e))))

(defn handle-download-workspace-item
  [deps item-id req]
  (try
    (let [params       (parse-query-string* deps (:query-string req))
          workspace-id (nonblank-str* deps (get params "workspace_id"))]
      (if-let [item (workspace/get-item item-id :workspace-id workspace-id)]
        (let [payload-file (some-> (:payload-path item) io/file)
              bytes        (when (and payload-file (.isFile payload-file))
                             (Files/readAllBytes (.toPath payload-file)))]
          (if bytes
            (download-response* deps (:name item) (:media-type item) bytes)
            (json-response* deps 404 {:error "shared workspace item not found"})))
        (json-response* deps 404 {:error "shared workspace item not found"})))
    (catch clojure.lang.ExceptionInfo e
      (exception-response* deps e))))
