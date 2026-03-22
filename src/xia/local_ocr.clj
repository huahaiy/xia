(ns xia.local-ocr
  "Local OCR helpers for image uploads backed by a local llama.cpp CLI runtime."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.config :as cfg])
  (:import [java.io File]
           [java.nio.charset StandardCharsets]
           [java.util.concurrent TimeUnit]))

(def ^:private default-command "llama-cli")
(def ^:private default-timeout-ms 120000)
(def ^:private default-max-tokens 2048)
(def ^:private spotting-image-max-pixels 1605632)

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
              :requires-spotting-mmproj? true
              :image-max-pixels spotting-image-max-pixels}})

(def ^:private supported-ocr-mode-set
  (set (keys ocr-mode-definitions)))

(defn supported-modes
  []
  (mapv (fn [[mode {:keys [label prompt requires-spotting-mmproj? image-max-pixels]}]]
          {:id mode
           :label label
           :prompt prompt
           :requires-spotting-mmproj? (boolean requires-spotting-mmproj?)
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

(defn- command-path
  []
  (cfg/string-option :local-doc/ocr-command default-command))

(defn- model-path
  []
  (cfg/string-option :local-doc/ocr-model-path nil))

(defn- mmproj-path
  []
  (cfg/string-option :local-doc/ocr-mmproj-path nil))

(defn- spotting-mmproj-path
  []
  (cfg/string-option :local-doc/ocr-spotting-mmproj-path nil))

(defn- timeout-ms
  []
  (cfg/positive-long :local-doc/ocr-timeout-ms default-timeout-ms))

(defn- max-tokens
  []
  (cfg/positive-long :local-doc/ocr-max-tokens default-max-tokens))

(defn- configured?
  []
  (and (seq (command-path))
       (seq (model-path))
       (seq (mmproj-path))))

(defn settings
  []
  {:enabled               (enabled?)
   :backend               :llama.cpp-cli
   :configured            (boolean (configured?))
   :command               (command-path)
   :model-path            (model-path)
   :mmproj-path           (mmproj-path)
   :spotting-mmproj-path  (spotting-mmproj-path)
   :timeout-ms            (timeout-ms)
   :max-tokens            (max-tokens)
   :default-mode          :ocr
   :spotting-image-max-pixels spotting-image-max-pixels
   :supported-modes       (supported-modes)})

(defn admin-body
  []
  (let [{:keys [enabled configured command model-path mmproj-path spotting-mmproj-path
                timeout-ms max-tokens default-mode spotting-image-max-pixels supported-modes]}
        (settings)]
    {:enabled                     (boolean enabled)
     :backend                     "llama.cpp-cli"
     :configured                  (boolean configured)
     :command                     command
     :model_path                  model-path
     :mmproj_path                 mmproj-path
     :spotting_mmproj_path        spotting-mmproj-path
     :timeout_ms                  timeout-ms
     :max_tokens                  max-tokens
     :default_mode                (name default-mode)
     :spotting_image_max_pixels   spotting-image-max-pixels
     :supported_modes             (mapv (fn [mode]
                                          (cond-> {:id (name (:id mode))
                                                   :label (:label mode)
                                                   :prompt (:prompt mode)}
                                            (:requires-spotting-mmproj? mode)
                                            (assoc :requires_spotting_mmproj true)
                                            (:image-max-pixels mode)
                                            (assoc :image_max_pixels (:image-max-pixels mode))))
                                        supported-modes)}))

(defn- missing-config-ex
  []
  (let [settings (settings)
        missing  (cond-> []
                   (not (seq (:command settings)))
                   (conj :command)
                   (not (seq (:model-path settings)))
                   (conj :model-path)
                   (not (seq (:mmproj-path settings)))
                   (conj :mmproj-path))]
    (ex-info "Local OCR is not configured. Set the llama.cpp command, model path, and mmproj path first."
             {:type :local-doc/ocr-not-configured
              :missing-config missing})))

(defn- spotting-mmproj-required-ex
  []
  (ex-info (str "Spotting OCR requires a mmproj file patched with "
                "clip.vision.image_max_pixels="
                spotting-image-max-pixels
                ". Configure local-doc/ocr-spotting-mmproj-path.")
           {:type :local-doc/ocr-spotting-mmproj-required
            :image-max-pixels spotting-image-max-pixels}))

(defn- ensure-ready!
  [mode]
  (when-not (enabled?)
    (throw (ex-info "Local OCR is disabled."
                    {:type :local-doc/ocr-disabled})))
  (when-not (configured?)
    (throw (missing-config-ex)))
  (when (and (= (:id mode) :spotting)
             (not (seq (spotting-mmproj-path))))
    (throw (spotting-mmproj-required-ex))))

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
    (spit tmp image-bytes :binary true)
    tmp))

(defn- read-stream!
  [stream]
  (with-open [reader (io/reader stream :encoding "UTF-8")]
    (slurp reader)))

(defn- process-result
  [^Process process timeout-ms]
  (let [stdout* (future (read-stream! (.getInputStream process)))
        stderr* (future (read-stream! (.getErrorStream process)))]
    (if (.waitFor process (long timeout-ms) TimeUnit/MILLISECONDS)
      {:exit   (.exitValue process)
       :stdout @stdout*
       :stderr @stderr*}
      (do
        (.destroyForcibly process)
        (throw (ex-info "Local OCR timed out."
                        {:type :local-doc/ocr-timeout
                         :timeout-ms timeout-ms})))))) 

(defn- run-llama-cli!
  [{:keys [prompt image-path mode]}]
  (let [mmproj (if (= (:id mode) :spotting)
                 (spotting-mmproj-path)
                 (mmproj-path))
        command [(command-path)
                 "-m" (model-path)
                 "--mmproj" mmproj
                 "--temp" "0"
                 "-n" (str (max-tokens))
                 "-p" prompt
                 "--image" image-path]
        process (.start (ProcessBuilder. ^java.util.List command))
        {:keys [exit stdout stderr]} (process-result process (timeout-ms))]
    (when-not (zero? (long exit))
      (throw (ex-info "Local OCR command failed."
                      {:type :local-doc/ocr-failed
                       :exit exit
                       :command command
                       :stderr (some-> stderr str/trim not-empty)
                       :stdout-preview (some-> stdout str/trim not-empty
                                               (#(if (> (count %) 300)
                                                   (str (subs % 0 297) "...")
                                                   %)))})))
    stdout))

(defn- normalize-output
  [text prompt]
  (let [value (some-> text str/replace "\u0000" "" str/trim)]
    (cond
      (str/blank? value)
      ""

      (str/starts-with? value prompt)
      (some-> value
              (subs (count prompt))
              str/trim)

      :else
      value)))

(defn ocr-image-bytes
  [image-bytes {:keys [name media-type ocr-mode] :as opts}]
  (let [mode (normalize-ocr-mode ocr-mode)]
    (ensure-ready! mode)
    (let [prompt (:prompt mode)
          tmp    (create-temp-image-file! image-bytes {:name name :media-type media-type})]
      (try
        (-> (run-llama-cli! {:prompt prompt
                             :image-path (.getAbsolutePath tmp)
                             :mode mode})
            (normalize-output prompt))
        (catch Exception e
          (throw (if (= :local-doc/ocr-failed (some-> e ex-data :type))
                   e
                   (ex-info "Local OCR failed."
                            (merge {:type :local-doc/ocr-failed
                                    :ocr-mode (:id mode)}
                                   (select-keys (ex-data e)
                                                [:timeout-ms :image-max-pixels]))
                            e))))
        (finally
          (when (.exists tmp)
            (when-not (.delete tmp)
              (log/debug "Failed to delete temp OCR image file"
                         {:path (.getAbsolutePath tmp)}))))))))
