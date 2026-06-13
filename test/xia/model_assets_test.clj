(ns xia.model-assets-test
  (:require [clojure.test :refer [deftest is]]
            [xia.http-client :as http-client]
            [xia.model-assets :as model-assets])
  (:import [java.nio.file Files]
           [java.nio.charset StandardCharsets]))

(defn- temp-target
  []
  (let [dir (Files/createTempDirectory "xia-model-assets-test"
                                       (make-array java.nio.file.attribute.FileAttribute 0))]
    (str (.resolve dir "asset.bin"))))

(deftest missing-managed-file-downloads-through-http-client
  (let [target (temp-target)
        calls  (atom [])]
    (with-redefs [http-client/download! (fn [opts]
                                          (swap! calls conj opts)
                                          (spit (:target-path opts) "asset")
                                          {:status 200
                                           :target-path (:target-path opts)})]
      (binding [*out* (java.io.StringWriter.)]
        (is (= target
               (model-assets/ensure-managed-file! {:url "https://example.test/model.gguf"
                                                   :target-path target
                                                   :lock (Object.)
                                                   :artifact-kind "test model"
                                                   :artifact-label "model.gguf"})))))
    (is (= [{:url "https://example.test/model.gguf"
             :target-path target
             :timeout (* 30 60 1000)
             :request-label "test model model.gguf download"}]
           @calls))))

(deftest existing-managed-file-does-not-download
  (let [target (temp-target)
        calls  (atom [])]
    (Files/write (java.nio.file.Paths/get target (make-array String 0))
                 (.getBytes "asset" StandardCharsets/UTF_8)
                 (make-array java.nio.file.OpenOption 0))
    (with-redefs [http-client/download! (fn [opts]
                                          (swap! calls conj opts)
                                          (throw (ex-info "should not download" {})))]
      (is (= target
             (model-assets/ensure-managed-file! {:url "https://example.test/model.gguf"
                                                 :target-path target
                                                 :lock (Object.)
                                                 :artifact-kind "test model"
                                                 :artifact-label "model.gguf"}))))
    (is (= [] @calls))))
