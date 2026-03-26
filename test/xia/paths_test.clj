(ns xia.paths-test
  (:require [clojure.test :refer [deftest is]]
            [xia.paths :as paths]))

(deftest managed-model-dirs-use-shared-machine-cache
  (let [shared-root (paths/path-str (System/getProperty "user.home")
                                    ".xia"
                                    "models")]
    (is (= (paths/path-str shared-root "embed")
           (paths/managed-embed-dir)))
    (is (= (paths/path-str shared-root "embed")
           (paths/managed-embed-dir "/tmp/xia-a")))
    (is (= (paths/path-str shared-root "llm")
           (paths/managed-llm-dir "/tmp/xia-b")))
    (is (= (paths/path-str shared-root "ocr")
           (paths/managed-ocr-dir "/tmp/xia-c")))))

(deftest shared-workspace-root-uses-machine-shared-cache
  (is (= (paths/path-str (System/getProperty "user.home")
                         ".xia"
                         "workspaces")
         (paths/shared-workspace-root))))

(deftest storage-layout-keeps-db-local-but-model-cache-shared
  (let [layout (paths/storage-layout "/tmp/xia-db")
        shared-root (paths/absolute-path
                      (paths/path-str (System/getProperty "user.home")
                                      ".xia"
                                      "models"))
        workspace-root (paths/absolute-path
                         (paths/path-str (System/getProperty "user.home")
                                         ".xia"
                                         "workspaces"))]
    (is (= "/tmp/xia-db" (:db-path layout)))
    (is (= "/tmp/xia-db/.xia" (:support-dir layout)))
    (is (= workspace-root (:workspace-root layout)))
    (is (= (paths/path-str shared-root "embed") (:embed-dir layout)))
    (is (= (paths/path-str shared-root "llm") (:llm-dir layout)))
    (is (= (paths/path-str shared-root "ocr") (:ocr-dir layout)))))
