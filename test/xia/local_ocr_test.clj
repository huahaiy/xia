(ns xia.local-ocr-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.local-ocr :as local-ocr]
            [xia.paths :as paths]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.io File]
           [java.nio.charset StandardCharsets]))

(use-fixtures :each with-test-db)

(deftest settings-default-to-managed-paddleocr-paths
  (let [settings  (local-ocr/settings)
        ocr-dir   (paths/managed-ocr-dir (db/current-db-path))]
    (is (= true (:managed-install settings)))
    (is (= :local (:model-backend settings)))
    (is (= "llama-cli" (:command settings)))
    (is (nil? (:model-path settings)))
    (is (nil? (:mmproj-path settings)))
    (is (= (str ocr-dir File/separator "PaddleOCR-VL-1.5.gguf")
           (:resolved-model-path settings)))
    (is (= (str ocr-dir File/separator "PaddleOCR-VL-1.5-mmproj.gguf")
           (:resolved-mmproj-path settings)))
    (is (= true (:configured settings)))))

(deftest spotting-metadata-file-uses-required-max-pixels
  (let [^File file (#'local-ocr/create-spotting-metadata-file!)]
    (try
      (is (.exists file))
      (is (= "{\"clip.vision.image_max_pixels\":1605632}"
             (slurp file)))
      (finally
        (.delete file)))))

(deftest external-backend-uses-vision-provider
  (db/upsert-provider! {:id :vision
                        :name "Vision"
                        :base-url "https://api.example.com/v1"
                        :model "gpt-4.1-mini"
                        :vision? true})
  (db/set-default-provider! :vision)
  (db/set-config! :local-doc/ocr-enabled? true)
  (db/set-config! :local-doc/ocr-backend "external")
  (let [calls (atom [])]
    (with-redefs [llm/chat-simple (fn [messages & opts]
                                    (swap! calls conj {:messages messages
                                                       :opts (apply hash-map opts)})
                                    "Formula Recognition:\nE = mc^2")]
      (let [text (local-ocr/ocr-image-bytes (.getBytes "fake-image" StandardCharsets/UTF_8)
                                            {:name "formula.png"
                                             :media-type "image/png"
                                             :ocr-mode :formula})
            {:keys [messages opts]} (first @calls)
            content (get-in messages [0 "content"])]
        (is (= "E = mc^2" text))
        (is (= 2048 (:max-tokens opts)))
        (is (= 0 (:temperature opts)))
        (is (nil? (:provider-id opts)))
        (is (= "Formula Recognition:" (get-in content [0 "text"])))
        (is (str/starts-with? (get-in content [1 "image_url" "url"])
                              "data:image/png;base64,"))))))
