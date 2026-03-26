(ns xia.paths
  "Helpers for Xia storage paths.

   DB state is instance-scoped.
   Managed model caches are machine-shared by default."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log])
  (:import [java.nio.file Paths]))

(def support-dir-name ".xia")
(def default-instance-id "default")
(def default-workspace-id "default")

(defn- env-value
  [k]
  (System/getenv k))

(defn path-str
  [base & more]
  (str (Paths/get base (into-array String more))))

(defn absolute-path
  [path]
  (when (some? path)
    (.getAbsolutePath (io/file path))))

(defn normalize-instance-id
  [value]
  (let [text (some-> value str str/trim str/lower-case)
        slug (some-> text
                     (str/replace #"[^a-z0-9._-]+" "-")
                     (str/replace #"^[._-]+|[._-]+$" ""))]
    (when (seq slug)
      slug)))

(defn resolve-instance-id
  ([] (or (normalize-instance-id (env-value "XIA_INSTANCE"))
          default-instance-id))
  ([value]
   (or (normalize-instance-id value)
       (resolve-instance-id))))

(defn warn-if-instance-id-normalized!
  [raw-value resolved-value]
  (let [raw-text (some-> raw-value str str/trim)]
    (when (and (seq raw-text)
               (not= raw-text resolved-value))
      (log/info "Normalized Xia instance id" (pr-str raw-text) "->" (pr-str resolved-value)))))

(defn default-instance-root
  ([] (default-instance-root nil))
  ([instance-id]
   (path-str (System/getProperty "user.home")
             ".xia"
             "instances"
             (resolve-instance-id instance-id))))

(defn default-db-path
  ([] (default-db-path nil))
  ([instance-id]
   (path-str (default-instance-root instance-id) "db")))

(defn support-dir-path
  [db-path]
  (when db-path
    (path-str db-path support-dir-name)))

(defn shared-model-root
  []
  (path-str (System/getProperty "user.home")
            ".xia"
            "models"))

(defn shared-workspace-root
  []
  (or (some-> (env-value "XIA_WORKSPACE_ROOT")
              str/trim
              not-empty
              absolute-path)
      (path-str (System/getProperty "user.home")
                ".xia"
                "workspaces")))

(defn managed-embed-dir
  ([] (path-str (shared-model-root) "embed"))
  ([_db-path]
   (managed-embed-dir)))

(defn managed-llm-dir
  ([] (path-str (shared-model-root) "llm"))
  ([_db-path]
   (managed-llm-dir)))

(defn managed-ocr-dir
  ([] (path-str (shared-model-root) "ocr"))
  ([_db-path]
   (managed-ocr-dir)))

(defn storage-layout
  [db-path]
  (let [db-path* (absolute-path db-path)]
    {:db-path     db-path*
     :support-dir (some-> db-path* support-dir-path absolute-path)
     :workspace-root (some-> (shared-workspace-root) absolute-path)
     :embed-dir   (some-> (managed-embed-dir) absolute-path)
     :llm-dir     (some-> (managed-llm-dir) absolute-path)
     :ocr-dir     (some-> (managed-ocr-dir) absolute-path)}))
