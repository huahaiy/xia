(ns xia.local-ocr-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.db :as db]
            [xia.llm :as llm]
            [xia.local-ocr :as local-ocr]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.nio.charset StandardCharsets]))

(use-fixtures :each with-test-db)

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
