(ns xia.channel.http.assets
  "Local web UI asset serving."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log])
  (:import [java.io ByteArrayOutputStream]
           [java.nio.file FileVisitOption Files LinkOption Path Paths]))

(def ^:private read-bundled-resource
  (memoize
    (fn [path]
      (some-> (str "web/" path)
              io/resource
              slurp))))

(def ^:private read-bundled-resource-bytes
  (memoize
    (fn [path]
      (when-let [resource (some-> (str "web/" path) io/resource)]
        (with-open [in (io/input-stream resource)
                    out (ByteArrayOutputStream.)]
          (io/copy in out)
          (.toByteArray out))))))

(def ^:private web-dev-poll-interval-ms 1000)

(def ^:private web-dev-no-cache-headers
  {"Cache-Control" "no-store, no-cache, must-revalidate, max-age=0"
   "Pragma" "no-cache"
   "Expires" "0"})

(def ^:private static-assets
  {"/style.css"                     {:path "style.css" :content-type "text/css"}
   "/app.js"                        {:path "app.js" :content-type "text/javascript"}
   "/favicon.ico"                   {:path "favicon/favicon.ico" :content-type "image/x-icon" :binary? true}
   "/favicon/favicon.svg"           {:path "favicon/favicon.svg" :content-type "image/svg+xml"}
   "/favicon/favicon-96x96.png"     {:path "favicon/favicon-96x96.png" :content-type "image/png" :binary? true}
   "/favicon/apple-touch-icon.png"  {:path "favicon/apple-touch-icon.png" :content-type "image/png" :binary? true}
   "/favicon/web-app-manifest-192x192.png" {:path "favicon/web-app-manifest-192x192.png" :content-type "image/png" :binary? true}
   "/favicon/web-app-manifest-512x512.png" {:path "favicon/web-app-manifest-512x512.png" :content-type "image/png" :binary? true}
   "/favicon/site.webmanifest"      {:path "favicon/site.webmanifest"
                                     :content-type "application/manifest+json; charset=utf-8"}})

(defn- web-dev-state-atom
  [deps]
  (:web-dev-state-atom deps))

(defn- web-dev-enabled?
  [deps]
  (true? (:enabled? @(web-dev-state-atom deps))))

(defn- resolve-web-dev-root
  []
  (try
    (when-let [resource (io/resource "web/index.html")]
      (when (= "file" (.getProtocol resource))
        (.getParent (Paths/get (.toURI resource)))))
    (catch Exception _
      nil)))

(defn configure-web-dev!
  [deps enabled?]
  (if-not enabled?
    (reset! (web-dev-state-atom deps) {:enabled? false
                                       :root nil})
    (if-let [root (resolve-web-dev-root)]
      (do
        (reset! (web-dev-state-atom deps) {:enabled? true
                                           :root root})
        (log/info "Web dev mode enabled; serving live web assets from" (str root)))
      (do
        (reset! (web-dev-state-atom deps) {:enabled? false
                                           :root nil})
        (log/warn "Web dev mode requested, but web resources are not file-backed; falling back to bundled assets")))))

(defn- web-dev-root
  [deps]
  (:root @(web-dev-state-atom deps)))

(defn- web-dev-path
  ^Path [deps path]
  (when-let [^Path root (web-dev-root deps)]
    (.normalize (.resolve root ^String path))))

(defn- read-web-dev-resource
  [deps path]
  (when-let [^Path p (web-dev-path deps path)]
    (when (Files/isRegularFile p (make-array LinkOption 0))
      (slurp (.toFile p)))))

(defn- read-web-dev-resource-bytes
  [deps path]
  (when-let [^Path p (web-dev-path deps path)]
    (when (Files/isRegularFile p (make-array LinkOption 0))
      (Files/readAllBytes p))))

(defn- read-resource
  [deps path]
  (if (web-dev-enabled? deps)
    (or (read-web-dev-resource deps path)
        (read-bundled-resource path))
    (read-bundled-resource path)))

(defn- read-resource-bytes
  [deps path]
  (if (web-dev-enabled? deps)
    (or (read-web-dev-resource-bytes deps path)
        (read-bundled-resource-bytes path))
    (read-bundled-resource-bytes path)))

(defn- with-web-dev-headers
  [deps response]
  (if (web-dev-enabled? deps)
    (update response :headers #(merge web-dev-no-cache-headers (or % {})))
    response))

(defn- web-dev-version
  [deps]
  (when-let [^Path root (web-dev-root deps)]
    (try
      (with-open [stream (Files/walk root (make-array FileVisitOption 0))]
        (let [{:keys [file-count max-modified total-size]}
              (reduce (fn [{:keys [file-count max-modified total-size]} ^Path path]
                        (if (Files/isRegularFile path (make-array LinkOption 0))
                          {:file-count   (unchecked-inc-int (int file-count))
                           :max-modified (max (long max-modified)
                                              (.toMillis (Files/getLastModifiedTime path
                                                                                    (make-array LinkOption 0))))
                           :total-size   (+ (long total-size) (Files/size path))}
                          {:file-count file-count
                           :max-modified max-modified
                           :total-size total-size}))
                      {:file-count 0
                       :max-modified 0
                       :total-size 0}
                      (iterator-seq (.iterator stream)))]
          (str file-count ":" max-modified ":" total-size)))
      (catch Exception e
        (log/debug e "Failed to compute web dev version")
        nil))))

(defn- inject-web-dev-client
  [deps html]
  (if-not (web-dev-enabled? deps)
    html
    (let [version (or (web-dev-version deps) "0")
          script  (str "<script>"
                       "(function(){"
                       "var currentVersion=" (pr-str version) ";"
                       "async function poll(){"
                       "try{"
                       "var response=await fetch('/__dev/web-reload',{cache:'no-store'});"
                       "if(!response.ok){return;}"
                       "var payload=await response.json();"
                       "if(payload && payload.version && payload.version!==currentVersion){"
                       "window.location.reload();"
                       "return;}"
                       "if(payload && payload.version){currentVersion=payload.version;}"
                       "}catch(_err){}"
                       "}"
                       "window.setInterval(function(){"
                       "if(document.visibilityState!=='hidden'){poll();}"
                       "},"
                       web-dev-poll-interval-ms
                       ");"
                       "})();"
                       "</script>")]
      (if (str/includes? html "</body>")
        (str/replace html "</body>" (str script "</body>"))
        (str html script)))))

(defn- resource-response
  [deps path content-type]
  (if-let [content (read-resource deps path)]
    (with-web-dev-headers
      deps
      {:status  200
       :headers {"Content-Type" content-type}
       :body    content})
    {:status 404 :body "Not Found"}))

(defn- binary-resource-response
  [deps path content-type]
  (if-let [content (read-resource-bytes deps path)]
    (with-web-dev-headers
      deps
      {:status  200
       :headers {"Content-Type" content-type}
       :body    content})
    {:status 404 :body "Not Found"}))

(defn static-asset-uri?
  [uri]
  (contains? static-assets uri))

(defn static-asset-response
  [deps uri]
  (when-let [{:keys [path content-type binary?]} (get static-assets uri)]
    (if binary?
      (binary-resource-response deps path content-type)
      (resource-response deps path content-type))))

(defn handle-web-dev-reload
  [deps]
  (if (web-dev-enabled? deps)
    (with-web-dev-headers
      deps
      ((:json-response deps) 200 {:enabled true
                                  :version (or (web-dev-version deps) "0")}))
    ((:json-response deps) 404 {:error "not found"})))

(defn- html-response
  [body]
  {:status  200
   :headers {"Content-Type" "text/html; charset=utf-8"}
   :body    body})

(defn handle-home
  [deps]
  (if-let [html (read-resource deps "index.html")]
    (with-web-dev-headers
      deps
      (assoc-in (html-response (inject-web-dev-client deps html))
                [:headers "Set-Cookie"]
                ((:session-cookie-header deps))))
    {:status 404 :body "Not Found"}))
