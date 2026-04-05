(ns xia.local-ocr
  "OCR helpers for image uploads, using either Xia-managed local PaddleOCR assets
   or an external vision-capable model provider."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.config :as cfg]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.paths :as paths]
            [xia.task-policy :as task-policy])
  (:import [datalevin.dtlvnative DTLV DTLV$dtlv_llama_vision_generator]
           [java.io File InputStream]
           [java.net URI]
           [java.net.http HttpClient HttpClient$Redirect HttpRequest HttpResponse$BodyHandlers]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files Path Paths StandardCopyOption]
           [java.nio.file.attribute FileAttribute]
           [java.time Duration]
           [java.util Base64]))

(def ^:private default-model-backend :local)
(def ^:private default-timeout-ms 120000)
(def ^:private default-max-tokens 2048)
(def ^:private max-native-output-resize-attempts 8)
(def ^:private spotting-image-max-pixels 1605632)
(def ^:private default-model-file "PaddleOCR-VL-1.5.gguf")
(def ^:private default-mmproj-file "PaddleOCR-VL-1.5-mmproj.gguf")
(def ^:private default-model-url
  (str "https://huggingface.co/PaddlePaddle/PaddleOCR-VL-1.5-GGUF/resolve/main/"
       default-model-file
       "?download=true"))
(def ^:private default-mmproj-url
  (str "https://huggingface.co/PaddlePaddle/PaddleOCR-VL-1.5-GGUF/resolve/main/"
       default-mmproj-file
       "?download=true"))
(def ^:private managed-asset-lock (Object.))
(def ^:private supported-backends #{:local :external})

(def ^:private ocr-mode-definitions
  {:ocr {:id :ocr
         :label "Text Recognition"
         :prompt "OCR:"}
   :formula {:id :formula
             :label "Formula Recognition"
             :prompt "Formula Recognition:"}
   :table {:id :table
           :label "Table Recognition"
           :prompt "Table Recognition:"}
   :chart {:id :chart
           :label "Chart Recognition"
           :prompt "Chart Recognition:"}
   :seal {:id :seal
          :label "Seal Recognition"
          :prompt "Seal Recognition:"}
   :spotting {:id :spotting
              :label "Spotting"
              :prompt "Spotting:"
              :image-max-pixels spotting-image-max-pixels}})

(defn supported-modes
  []
  (mapv (fn [[mode {:keys [label prompt image-max-pixels]}]]
          {:id mode
           :label label
           :prompt prompt
           :image-max-pixels image-max-pixels})
        ocr-mode-definitions))

(defn normalize-ocr-mode
  [mode]
  (let [normalized (cond
                     (nil? mode) :ocr
                     (keyword? mode) mode
                     (string? mode) (some-> mode str/trim not-empty str/lower-case keyword)
                     :else (throw (ex-info "Invalid OCR mode"
                                           {:type :local-doc/invalid-ocr-mode
                                            :ocr-mode mode
                                            :supported-modes (mapv name (keys ocr-mode-definitions))})))]
    (or (get ocr-mode-definitions normalized)
        (throw (ex-info "Unsupported OCR mode"
                        {:type :local-doc/invalid-ocr-mode
                         :ocr-mode mode
                         :supported-modes (mapv name (keys ocr-mode-definitions))})))))

(defn enabled?
  []
  (cfg/boolean-option :local-doc/ocr-enabled? false))

(defn model-backend
  []
  (cfg/keyword-option :local-doc/ocr-backend
                      default-model-backend
                      supported-backends))

(defn external-provider-id
  []
  (some-> (cfg/string-option :local-doc/ocr-provider-id nil)
          keyword))

(defn config-resolutions
  []
  {:enabled
   (cfg/boolean-option-resolution :local-doc/ocr-enabled? false)
   :backend
   (cfg/keyword-option-resolution :local-doc/ocr-backend
                                  default-model-backend
                                  supported-backends)
   :provider-id
   (cfg/string-option-resolution :local-doc/ocr-provider-id nil)
   :timeout-ms
   (cfg/positive-long-resolution :local-doc/ocr-timeout-ms
                                 default-timeout-ms)
   :max-tokens
   (cfg/positive-long-resolution :local-doc/ocr-max-tokens
                                 default-max-tokens)})

(defn- timeout-ms
  []
  (task-policy/local-doc-ocr-timeout-ms))

(defn- max-tokens
  []
  (task-policy/local-doc-ocr-max-tokens))

(defn- resolved-external-provider
  []
  (let [provider-id (external-provider-id)]
    (cond
      provider-id
      {:provider-id provider-id
       :provider    (db/get-provider provider-id)
       :default?    false}

      :else
      (when-let [provider (db/get-default-provider)]
        {:provider-id (:llm.provider/id provider)
         :provider    provider
         :default?    true}))))

(defn- external-provider-ready?
  []
  (let [{:keys [provider]} (resolved-external-provider)]
    (boolean (and provider
                  (llm/vision-capable? provider)))))

(defn- current-db-path
  []
  (db/current-db-path))

(defn- managed-ocr-dir
  []
  (when-let [db-path (current-db-path)]
    (paths/managed-ocr-dir db-path)))

(defn- managed-model-path
  []
  (some-> (managed-ocr-dir)
          (str File/separator default-model-file)))

(defn- managed-mmproj-path
  []
  (some-> (managed-ocr-dir)
          (str File/separator default-mmproj-file)))

(defn- resolved-model-path
  []
  (managed-model-path))

(defn- resolved-mmproj-path
  []
  (managed-mmproj-path))

(defn settings
  []
  (let [resolved-model-path*  (resolved-model-path)
        resolved-mmproj-path* (resolved-mmproj-path)
        model-backend*        (model-backend)
        {:keys [provider-id provider]} (resolved-external-provider)]
    {:enabled               (enabled?)
     :model-backend         model-backend*
     :external-provider-id  (external-provider-id)
     :resolved-external-provider-id provider-id
     :external-provider-vision? (boolean (and provider
                                             (llm/vision-capable? provider)))
     :configured            (boolean (case model-backend*
                                       :external (external-provider-ready?)
                                       (and (seq resolved-model-path*)
                                            (seq resolved-mmproj-path*))))
     :timeout-ms            (timeout-ms)
     :max-tokens            (max-tokens)
     :default-mode          :ocr
     :spotting-image-max-pixels spotting-image-max-pixels
     :supported-modes       (supported-modes)}))

(defn admin-body
  []
  (let [{:keys [enabled configured model-backend external-provider-id resolved-external-provider-id
                external-provider-vision?
                timeout-ms max-tokens default-mode spotting-image-max-pixels supported-modes]}
        (settings)]
    {:enabled                     (boolean enabled)
     :model_backend               (name model-backend)
     :external_provider_id        (some-> external-provider-id name)
     :resolved_external_provider_id (some-> resolved-external-provider-id name)
     :external_provider_vision    (boolean external-provider-vision?)
     :configured                  (boolean configured)
     :timeout_ms                  timeout-ms
     :max_tokens                  max-tokens
     :default_mode                (name default-mode)
     :spotting_image_max_pixels   spotting-image-max-pixels
     :supported_modes             (mapv (fn [mode]
                                          (cond-> {:id (name (:id mode))
                                                   :label (:label mode)
                                                   :prompt (:prompt mode)}
                                            (:image-max-pixels mode)
                                            (assoc :image_max_pixels (:image-max-pixels mode))))
                                        supported-modes)}))

(defn- missing-config-ex
  []
  (let [missing  (cond-> []
                   (not (seq (current-db-path)))
                   (conj :db-path)
                   (not (seq (resolved-model-path)))
                   (conj :model-path)
                   (not (seq (resolved-mmproj-path)))
                   (conj :mmproj-path))]
    (ex-info "Local OCR is not configured. Xia needs an open database so it can place its managed PaddleOCR assets."
             {:type :local-doc/ocr-not-configured
              :missing-config missing})))

(defn- external-provider-required-ex
  []
  (ex-info "External OCR requires a vision-capable provider or default provider."
           {:type :local-doc/ocr-provider-required}))

(defn- external-provider-not-vision-ex
  [provider-id]
  (ex-info "Selected OCR provider is not vision-capable."
           {:type :local-doc/ocr-provider-not-vision
            :provider-id provider-id}))

(defn- ensure-ready!
  [mode]
  (when-not (enabled?)
    (throw (ex-info "Local OCR is disabled."
                    {:type :local-doc/ocr-disabled})))
  (case (model-backend)
    :external
    (let [{:keys [provider-id provider]} (resolved-external-provider)]
      (cond
        (nil? provider)
        (throw (external-provider-required-ex))

        (not (llm/vision-capable? provider))
        (throw (external-provider-not-vision-ex provider-id))

        :else nil))

    (when-not (and (seq (resolved-model-path))
                   (seq (resolved-mmproj-path)))
      (throw (missing-config-ex)))))

(defn- create-http-client
  []
  (-> (HttpClient/newBuilder)
      (.connectTimeout (Duration/ofSeconds 20))
      (.followRedirects HttpClient$Redirect/NORMAL)
      (.build)))

(defn- move-file!
  [^Path source ^Path target]
  (try
    (Files/move source target
                (into-array java.nio.file.CopyOption
                            [StandardCopyOption/ATOMIC_MOVE
                             StandardCopyOption/REPLACE_EXISTING]))
    (catch Exception _
      (Files/move source target
                  (into-array java.nio.file.CopyOption
                              [StandardCopyOption/REPLACE_EXISTING])))))

(defn- download-file!
  [url target-path]
  (let [^Path target  (Paths/get target-path (make-array String 0))
        ^Path parent  (.getParent target)
        tmp-dir       (or parent (Paths/get "." (make-array String 0)))
        _             (when parent
                        (Files/createDirectories parent (make-array FileAttribute 0)))
        prefix        (str (.getFileName target) ".part-")
        suffix        ".tmp"
        tmp           (Files/createTempFile tmp-dir prefix suffix
                                            (make-array FileAttribute 0))
        ^HttpClient client (create-http-client)
        ^HttpRequest req   (-> (HttpRequest/newBuilder (URI/create url))
                               (.header "User-Agent" "xia")
                               (.header "Accept" "application/octet-stream")
                               (.timeout (Duration/ofMinutes 30))
                               (.GET)
                               (.build))
        ^"[Ljava.nio.file.CopyOption;" copy-opts
        (into-array java.nio.file.CopyOption
                    [StandardCopyOption/REPLACE_EXISTING])]
    (try
      (let [resp   (.send client req (HttpResponse$BodyHandlers/ofInputStream))
            status (.statusCode resp)]
        (when-not (= 200 status)
          (throw (ex-info "Failed to download managed OCR asset"
                          {:url url :status status :target target-path})))
        (with-open [^InputStream in (.body resp)]
          (Files/copy in ^Path tmp copy-opts))
        (move-file! tmp target)
        target-path)
      (finally
        (when (Files/exists tmp (make-array java.nio.file.LinkOption 0))
          (try
            (Files/deleteIfExists tmp)
            (catch Exception _)))))))

(defn- announce-managed-download!
  [label target-path]
  (let [message (str "Downloading Xia managed OCR "
                     label
                     " to "
                     target-path
                     ". This may take a few minutes the first time.")]
    (log/info message)
    (println message)
    (flush)))

(defn- ensure-managed-asset!
  [target-path url label]
  (when (and (seq target-path)
             (seq url)
             (not (.exists (io/file target-path))))
    (locking managed-asset-lock
      (when-not (.exists (io/file target-path))
        (announce-managed-download! label target-path)
        (download-file! url target-path))))
  target-path)

(defn- ensure-managed-runtime-assets!
  []
  (ensure-managed-asset! (managed-model-path) default-model-url "model")
  (ensure-managed-asset! (managed-mmproj-path) default-mmproj-url "mmproj"))

(defn- image-suffix
  [{:keys [name media-type]}]
  (let [name (some-> name str/trim)
        idx  (when name (.lastIndexOf ^String name "."))
        ext  (when (and idx (pos? idx) (< idx (dec (count name))))
               (subs name idx))]
    (or (when (seq ext) ext)
        (case media-type
          "image/jpeg" ".jpg"
          "image/png" ".png"
          "image/webp" ".webp"
          ".img"))))

(defn- create-temp-image-file!
  [image-bytes opts]
  (let [suffix (image-suffix opts)
        ^File tmp (File/createTempFile "xia-local-ocr-" suffix)]
    (with-open [out (io/output-stream tmp)]
      (.write out ^bytes image-bytes))
    tmp))

(defn- next-buffer-size
  [current-size required-size]
  (when (neg? required-size)
    (throw (IllegalArgumentException. "required-size must be non-negative")))
  (let [doubled (if (> current-size (quot Integer/MAX_VALUE 2))
                  Integer/MAX_VALUE
                  (* 2 current-size))]
    (max required-size doubled)))

(defn- initial-output-buffer-size
  [max-tokens]
  (let [estimated-size (max 256 (* 16 (max 1 (long max-tokens))))]
    (if (>= estimated-size Integer/MAX_VALUE)
      Integer/MAX_VALUE
      (int estimated-size))))

(defn- destroy-vision-generator!
  [^DTLV$dtlv_llama_vision_generator generator]
  (when generator
    (DTLV/dtlv_llama_vision_generator_destroy generator)))

(defn- create-vision-generator!
  [mode]
  (let [generator        (DTLV$dtlv_llama_vision_generator.)
        image-max-pixels (long (or (:image-max-pixels mode) 0))
        rc               (DTLV/dtlv_llama_vision_generator_create generator
                                                                  (resolved-model-path)
                                                                  (resolved-mmproj-path)
                                                                  0
                                                                  0
                                                                  0
                                                                  0
                                                                  (int image-max-pixels))]
    (if (zero? (long rc))
      generator
      (do
        (destroy-vision-generator! generator)
        (throw (ex-info "Local OCR failed to initialize Datalevin vision runtime."
                        {:type :local-doc/ocr-failed
                         :action :initialize-generator
                         :rc (long rc)
                         :ocr-mode (:id mode)
                         :image-max-pixels image-max-pixels}))))))

(defn- run-vision-operation!
  [^DTLV$dtlv_llama_vision_generator generator {:keys [prompt image-path max-tokens mode]}]
  (loop [buffer-size (initial-output-buffer-size max-tokens)
         attempt     0]
    (when (>= attempt max-native-output-resize-attempts)
      (throw (ex-info "Local OCR failed after resizing the output buffer."
                      {:type :local-doc/ocr-failed
                       :action :generate
                       :ocr-mode (:id mode)
                       :max-tokens max-tokens})))
    (let [buffer      (byte-array buffer-size)
          actual-size (if (= (:id mode) :ocr)
                        (DTLV/dtlv_llama_ocr generator image-path (int max-tokens) buffer (long buffer-size))
                        (DTLV/dtlv_llama_vision_generate generator prompt image-path (int max-tokens) buffer (long buffer-size)))]
      (cond
        (and (<= 0 actual-size)
             (<= actual-size buffer-size))
        (String. buffer 0 actual-size StandardCharsets/UTF_8)

        (> (Math/abs (long actual-size)) buffer-size)
        (recur (next-buffer-size buffer-size (Math/abs (long actual-size)))
               (inc attempt))

        :else
        (throw (ex-info "Local OCR generation failed."
                        {:type :local-doc/ocr-failed
                         :action :generate
                         :ocr-mode (:id mode)
                         :prompt prompt
                         :max-tokens max-tokens
                         :rc (long actual-size)}))))))

(defn- invoke-local-model!
  [image-path mode]
  (let [generator (create-vision-generator! mode)]
    (try
      (run-vision-operation! generator {:prompt (:prompt mode)
                                        :image-path image-path
                                        :max-tokens (max-tokens)
                                        :mode mode})
      (finally
        (destroy-vision-generator! generator)))))

(def ^:private timeout-sentinel ::timeout)

(defn- run-with-timeout!
  [timeout-ms thunk]
  ;; Native llama.cpp work is not interruptible here, so timeout is best-effort
  ;; at the caller boundary.
  (let [task   (future (thunk))
        result (deref task timeout-ms timeout-sentinel)]
    (if (= timeout-sentinel result)
      (do
        (future-cancel task)
        (throw (ex-info "Local OCR timed out."
                        {:type :local-doc/ocr-timeout
                         :timeout-ms timeout-ms})))
      result)))

(defn- normalize-output
  [text prompt]
  (let [value (some-> text
                      (#(str/replace % "\u0000" ""))
                      str/trim)]
    (cond
      (str/blank? value)
      ""

      (str/starts-with? value prompt)
      (some-> value
              (subs (count prompt))
              str/trim)

      :else
      value)))

(defn- bytes->data-url
  [mime-type ^bytes data]
  (str "data:" mime-type ";base64,"
       (.encodeToString (Base64/getEncoder) data)))

(defn- run-external-ocr!
  [image-bytes {:keys [media-type mode]}]
  (let [{:keys [provider-id default?]} (resolved-external-provider)
        messages [{"role" "user"
                   "content" [{"type" "text"
                               "text" (:prompt mode)}
                              {"type" "image_url"
                               "image_url" {"url" (bytes->data-url media-type image-bytes)
                                            "detail" "high"}}]}]
        opts (cond-> [:max-tokens (long (max-tokens))
                      :temperature 0]
               (and provider-id (not default?))
               (into [:provider-id provider-id]))]
    (apply llm/chat-simple messages opts)))

(defn- run-local-ocr!
  [image-bytes {:keys [name media-type mode]}]
  (ensure-managed-runtime-assets!)
  (let [prompt     (:prompt mode)
        tmp        (create-temp-image-file! image-bytes {:name name :media-type media-type})
        image-path (.getAbsolutePath tmp)]
    (try
      (-> (run-with-timeout! (timeout-ms)
            #(invoke-local-model! image-path mode))
          (normalize-output prompt))
      (finally
        (when (.exists tmp)
          (when-not (.delete tmp)
            (log/debug "Failed to delete temp OCR image file"
                       {:path image-path})))))))

(defn ocr-image-bytes
  [image-bytes {:keys [name media-type ocr-mode] :as opts}]
  (let [mode (normalize-ocr-mode ocr-mode)]
    (ensure-ready! mode)
    (try
      (case (model-backend)
        :external
        (-> (run-external-ocr! image-bytes {:media-type media-type
                                            :mode mode})
            (normalize-output (:prompt mode)))

        (run-local-ocr! image-bytes {:name name
                                     :media-type media-type
                                     :mode mode}))
      (catch Exception e
        (throw (if (= :local-doc/ocr-failed (some-> e ex-data :type))
                 e
                 (ex-info "Local OCR failed."
                          (merge {:type :local-doc/ocr-failed
                                  :ocr-mode (:id mode)
                                  :model-backend (model-backend)}
                                 (select-keys (ex-data e)
                                              [:timeout-ms :image-max-pixels :provider-id]))
                          e)))))))
