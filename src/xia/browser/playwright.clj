(ns xia.browser.playwright
  "Playwright browser backend."
  (:require [charred.api :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.browser.backend :as backend]
            [xia.browser.query :as browser.query]
            [xia.config :as cfg])
  (:import [com.microsoft.playwright Playwright Browser BrowserContext Locator Page Route
            Browser$NewContextOptions BrowserType$LaunchOptions Playwright$CreateOptions]
           [com.microsoft.playwright.impl.driver Driver]
           [com.microsoft.playwright.impl.driver.jar DriverJar]
           [java.net URI]
           [java.nio.file FileSystem FileSystemAlreadyExistsException FileSystems Files LinkOption Paths]
           [java.util Base64 Collections Map]
           [java.util.concurrent ConcurrentHashMap]
           [org.jsoup Jsoup]
           [org.jsoup.nodes Document Element]))

(def ^:private backend-id :playwright)
(def ^:private browser-name "chromium")
(def ^:private max-content-chars 8000)
(def ^:private live-session-ttl-ms (* 60 60 1000))
(def ^:private action-settle-ms 1200)
(def ^:private session-snapshot-lock-count 256)

(defonce ^:private runtime (atom {:playwright nil
                                  :browser nil
                                  :bootstrapped? false
                                  :browser-installed? false
                                  :browser-executable nil
                                  :browser-channel nil
                                  :driver-resource-fs nil
                                  :driver-resource-fs-owned? false}))
(defonce ^:private runtime-lock (Object.))
(defonce ^ConcurrentHashMap ^:private sessions (ConcurrentHashMap.))
(defonce ^:private session-snapshot-locks
  (vec (repeatedly session-snapshot-lock-count #(Object.))))

(defn- now-ms
  ^long []
  (System/currentTimeMillis))

(defn- enabled? []
  (cfg/boolean-option :browser/playwright-enabled? true))

(defn- headless? []
  (cfg/boolean-option :browser/playwright-headless? true))

(defn- timeout-ms []
  (cfg/positive-long :browser/playwright-timeout-ms 15000))

(defn- auto-install? []
  (cfg/boolean-option :browser/playwright-auto-install? true))

(defn- browsers-path []
  (cfg/string-option :browser/playwright-browsers-path nil))

(defn- normalize-channel
  [value]
  (some-> value str str/trim not-empty))

(defn- configured-channel []
  (normalize-channel (cfg/string-option :browser/playwright-channel nil)))

(defn- playwright-env
  []
  (cond-> {}
    (seq (browsers-path))
    (assoc "PLAYWRIGHT_BROWSERS_PATH" (browsers-path))))

(defn- linux-platform?
  []
  (some-> (System/getProperty "os.name")
          str/lower-case
          (str/includes? "linux")))

(defn- platform-summary
  []
  {:os-name (System/getProperty "os.name")
   :arch (System/getProperty "os.arch")})

(defn- session-snapshot-lock
  [session-id]
  (when session-id
    (let [session-hash (int (hash session-id))]
      (nth session-snapshot-locks
           (mod (bit-and Integer/MAX_VALUE session-hash)
                session-snapshot-lock-count)))))

(defn- with-session-snapshot-lock
  [session-id f]
  (if-let [lock (session-snapshot-lock session-id)]
    (locking lock
      (f))
    (f)))

(defn- runtime-running?
  [{:keys [browser]}]
  (try
    (boolean (and browser (.isConnected ^Browser browser)))
    (catch Exception _
      false)))

(defn- driver-resource-uri
  []
  (try
    (DriverJar/getDriverResourceURI)
    (catch Exception e
      (log/debug e "Unable to resolve Playwright driver resource URI")
      nil)))

(defn- open-driver-resource-fs
  [^URI resource-uri]
  (let [^Map fs-env (Collections/emptyMap)]
    (FileSystems/newFileSystem resource-uri fs-env)))

(defn- existing-driver-resource-fs
  [^URI resource-uri]
  (FileSystems/getFileSystem resource-uri))

(defn- close-driver-resource-fs!
  [driver-fs]
  (when (instance? FileSystem driver-fs)
    (.close ^FileSystem driver-fs)))

(defn- ensure-driver-resource-fs!
  []
  (locking runtime-lock
    (when-not (:driver-resource-fs @runtime)
      (when-let [^URI resource-uri (driver-resource-uri)]
        (when-not (= "jar" (.getScheme resource-uri))
          (try
            (let [^FileSystem fs (open-driver-resource-fs resource-uri)]
              (swap! runtime assoc
                     :driver-resource-fs fs
                     :driver-resource-fs-owned? true))
            (catch FileSystemAlreadyExistsException _
              (try
                (let [^FileSystem fs (existing-driver-resource-fs resource-uri)]
                  (swap! runtime assoc
                         :driver-resource-fs fs
                         :driver-resource-fs-owned? false))
                (catch Exception e
                  (log/debug e "Failed to recover existing Playwright driver resource filesystem"))))
            (catch UnsupportedOperationException e
              (log/debug e "Playwright driver resource URI does not support explicit filesystem initialization"))
            (catch Exception e
              (log/debug e "Failed to initialize Playwright driver resource filesystem"))))))))

(defn- create-playwright
  []
  (let [env (playwright-env)]
    (ensure-driver-resource-fs!)
    (if (seq env)
      (Playwright/create (doto (Playwright$CreateOptions.)
                           (.setEnv env)))
      (Playwright/create))))

(defn- path-exists?
  [path]
  (and (string? path)
       (not (str/blank? path))
       (Files/exists (Paths/get path (make-array String 0))
                     (make-array LinkOption 0))))

(defn- browser-installation-info
  ([]
   (try
     (with-open [^Playwright playwright (create-playwright)]
       (browser-installation-info playwright))
     (catch Exception e
       {:browser browser-name
        :available? false
        :installed? false
        :auto-install? (auto-install?)
        :message (or (.getMessage e)
                     "Playwright runtime is unavailable.")
        :error (.getMessage e)})))
  ([^Playwright playwright]
   (let [executable (.executablePath (.chromium playwright))
         installed? (path-exists? executable)]
     {:browser browser-name
      :available? true
      :installed? installed?
      :auto-install? (auto-install?)
      :executable executable
      :message (if installed?
                 "Playwright browser binaries are installed."
                 "Playwright browser binaries are missing.")})))

(defn- read-process-output
  [^Process process]
  (with-open [reader (io/reader (.getInputStream process) :encoding "UTF-8")]
    (slurp reader)))

(defn- run-playwright-cli!
  ([args]
   (run-playwright-cli! args {}))
  ([args {:keys [inherit-io?] :or {inherit-io? false}}]
  (let [driver (Driver/ensureDriverInstalled (playwright-env) Boolean/FALSE)
        pb (.createProcessBuilder driver)]
    (.addAll (.command pb) args)
    (if inherit-io?
      (.inheritIO pb)
      (.redirectErrorStream pb true))
    (let [process (.start pb)
          output  (when-not inherit-io?
                    (read-process-output process))
          exit    (.waitFor process)]
      {:args (vec args)
       :exit exit
       :output output
       :interactive? (boolean inherit-io?)}))))

(defn- install-browser!
  []
  (let [{:keys [exit] :as result} (run-playwright-cli! ["install" browser-name])]
    (when-not (zero? (long exit))
      (throw (ex-info "Playwright browser installation failed."
                      (assoc result
                             :backend backend-id
                             :browser browser-name
                             :status :install-failed))))
    result))

(defn- install-browser-deps!
  [{:keys [dry-run]
    :or {dry-run true}}]
  (let [platform (platform-summary)]
    (if-not (linux-platform?)
      {:backend backend-id
       :browser browser-name
       :supported? false
       :dry-run (boolean dry-run)
       :status :unsupported-platform
       :message "Playwright system dependency installation is only supported on Linux."
       :platform platform}
	      (let [interactive? (and (not dry-run)
	                              (some? (System/console)))
	            args (cond-> ["install-deps" browser-name]
	                   dry-run (conj "--dry-run"))
	            {:keys [exit] :as result} (run-playwright-cli! args {:inherit-io? interactive?})]
	        (when-not (zero? (long exit))
	          (throw (ex-info "Playwright system dependency installation failed."
	                          (merge result
	                                 {:backend backend-id
                                  :browser browser-name
                                  :supported? true
                                  :dry-run (boolean dry-run)
                                  :status :install-deps-failed
                                  :platform platform}))))
        (merge result
               {:backend backend-id
                :browser browser-name
                :supported? true
                :dry-run (boolean dry-run)
                :status (if dry-run :dry-run :installed)
                :message (if dry-run
                           "Previewed Playwright system dependency installation commands."
                           "Installed Playwright system dependencies.")
                :platform platform})))))

(defn- ensure-browser-installed!
  [^Playwright playwright]
  (let [{:keys [installed? executable] :as info} (browser-installation-info playwright)]
    (cond
      installed?
      info

      (not (auto-install?))
      (throw (ex-info "Playwright browser binaries are missing and auto-install is disabled."
                      (assoc info
                             :backend backend-id
                             :status :missing-browser)))

      :else
      (do
        (log/info "Installing Playwright browser binaries for first use"
                  {:backend backend-id
                   :browser browser-name
                   :path executable})
        (let [{:keys [output]} (install-browser!)
              rechecked (browser-installation-info playwright)]
          (if (:installed? rechecked)
            (assoc rechecked
                   :installed-by-xia? true
                   :install-output output)
            (throw (ex-info "Playwright browser installation completed but the browser executable is still missing."
                            (merge rechecked
                                   {:backend backend-id
                                    :browser browser-name
                                    :status :missing-browser-after-install
                                    :install-output output})))))))))

(defn- launch-browser
  ([^Playwright playwright]
   (launch-browser playwright (headless?) (configured-channel)))
  ([^Playwright playwright headless-override]
   (launch-browser playwright headless-override (configured-channel)))
  ([^Playwright playwright headless-override channel]
   (let [launch-opts (doto (BrowserType$LaunchOptions.)
                       (.setHeadless (boolean headless-override))
                       (.setTimeout (double (timeout-ms))))
         channel*    (normalize-channel channel)]
     (when channel*
       (.setChannel launch-opts channel*))
     (.launch (.chromium playwright) launch-opts))))

(defn- stop-runtime!
  []
  (locking runtime-lock
    (let [{:keys [browser playwright driver-resource-fs driver-resource-fs-owned?]} @runtime]
      (try
        (when browser
          (.close ^Browser browser))
        (catch Exception _))
      (try
        (when playwright
          (.close ^Playwright playwright))
        (catch Exception _))
      (try
        (when driver-resource-fs-owned?
          (close-driver-resource-fs! driver-resource-fs))
        (catch Exception _))
      (reset! runtime {:playwright nil
                       :browser nil
                       :bootstrapped? false
                       :browser-installed? false
                       :browser-executable nil
                       :browser-channel nil
                       :driver-resource-fs nil
                       :driver-resource-fs-owned? false}))))

(defn runtime-status
  []
  (let [{:keys [bootstrapped?
                browser-installed?
                browser-executable
                browser-channel]} @runtime
        running? (runtime-running? @runtime)]
    (cond
      (not (enabled?))
      {:backend backend-id
       :available? false
       :ready? false
       :running? false
       :bootstrapped? false
       :headless? (headless?)
       :browser browser-name
       :browser-channel browser-channel
       :browser-installed? false
       :browser-executable nil
       :status :disabled
       :message "Playwright backend is disabled by config."}

      running?
      {:backend backend-id
       :available? true
       :ready? true
       :running? true
       :bootstrapped? true
       :headless? (headless?)
       :browser browser-name
       :browser-channel browser-channel
       :browser-installed? (boolean browser-installed?)
       :browser-executable browser-executable
       :status :running
       :message "Playwright backend is running."}

      bootstrapped?
      {:backend backend-id
       :available? true
       :ready? true
       :running? false
       :bootstrapped? true
       :headless? (headless?)
       :browser browser-name
       :browser-channel browser-channel
       :browser-installed? (boolean browser-installed?)
       :browser-executable browser-executable
       :status :ready
       :message "Playwright backend is bootstrapped."}

      :else
      (let [{:keys [available? installed? executable message] :as info}
            (browser-installation-info)]
        (cond
          (not available?)
          {:backend backend-id
           :available? false
           :ready? false
           :running? false
           :bootstrapped? false
           :headless? (headless?)
           :browser browser-name
           :browser-channel browser-channel
           :browser-installed? false
           :browser-executable nil
           :status :unavailable
           :message message
           :error (:error info)}

          installed?
          {:backend backend-id
           :available? true
           :ready? false
           :running? false
           :bootstrapped? false
           :headless? (headless?)
           :browser browser-name
           :browser-channel browser-channel
           :browser-installed? true
           :browser-executable executable
           :status :available
           :message "Playwright backend is available and browser binaries are installed."}

          :else
          {:backend backend-id
           :available? true
           :ready? false
           :running? false
           :bootstrapped? false
           :headless? (headless?)
           :browser browser-name
           :browser-channel browser-channel
           :browser-installed? false
           :browser-executable executable
           :status :missing-browser
           :auto-install? (auto-install?)
           :message "Playwright browser binaries are missing. Xia can install them on first use."})))))

(defn- not-ready-ex
  []
  (let [{:keys [status message] :as runtime-status*} (runtime-status)]
    (ex-info message
             {:backend backend-id
              :status status
              :runtime runtime-status*})))

(defn- ensure-runtime!
  []
  (when-not (enabled?)
    (throw (not-ready-ex)))
  (locking runtime-lock
    (if (runtime-running? @runtime)
      @runtime
      (let [playwright (create-playwright)]
        (try
          (let [{:keys [installed? executable]} (ensure-browser-installed! playwright)
                launch-channel (configured-channel)
                browser (launch-browser playwright (headless?) launch-channel)
                state {:playwright playwright
                       :browser browser
                       :bootstrapped? true
                       :browser-installed? (boolean installed?)
                       :browser-executable executable
                       :browser-channel launch-channel}]
            (reset! runtime state)
            state)
          (catch Exception e
            (try (.close ^Playwright playwright) (catch Exception _))
            (throw e)))))))

(defn- page-result
  [session-id result]
  (assoc result :session-id session-id
                :backend backend-id))

(defn- bytes->data-url
  [mime-type ^bytes data]
  (str "data:" mime-type ";base64,"
       (.encodeToString (Base64/getEncoder) data)))

(defn- snapshot-backend-id
  [snapshot]
  (some-> (get snapshot "backend") keyword))

(defn- backend-snapshots
  [ops]
  (->> ((:all-snapshots ops))
       (filter (fn [[_ snapshot]]
                 (= backend-id (snapshot-backend-id snapshot))))))

(defn- session-expired?
  [{:keys [last-access]}]
  (let [last-access-ms (long last-access)
        live-session-ttl-ms* (long live-session-ttl-ms)]
    (> (- (now-ms) last-access-ms) live-session-ttl-ms*)))

(defn- page-text
  [^Document doc]
  (some-> doc
          .body
          .text
          str))

(defn- truncate-content
  [text]
  (let [^String text* (or text "")
        max-content-chars* (long max-content-chars)]
    (if (> (count text*) max-content-chars*)
      [(str (subs text* 0 max-content-chars*) "\n\n[content truncated]") true]
      [text* false])))

(defn- field-type
  [^Element el]
  (let [tag (.tagName el)
        type-attr (.attr el "type")]
    (cond
      (= "textarea" tag) "textarea"
      (= "select" tag) "select"
      (seq type-attr) type-attr
      :else tag)))

(defn- element-value
  [^Element el]
  (try
    (.val el)
    (catch Exception _
      (.attr el "value"))))

(defn- document->map
  [url title html]
  (let [^String html* (or html "")
        ^String url* (or url "about:blank")
        ^Document doc (Jsoup/parse html* url*)
        text (page-text doc)
        [content truncated?] (truncate-content text)
        forms (.select doc "form")
        links (->> (.select doc "a[href]")
                   (keep (fn [^Element a]
                           (let [text* (str/trim (.text a))
                                 href (.attr a "href")]
                             (when (and (seq text*)
                                        (seq href)
                                        (not (str/starts-with? href "javascript:")))
                               {:text text*
                                :url href}))))
                   vec)
        link-preview-limit (long browser.query/default-link-preview-limit)]
    {:url url
     :title (or title (.title doc))
     :content content
     :form_count (count forms)
     :forms (mapv (fn [^Element f]
                    {:id (not-empty (.id f))
                     :name (not-empty (.attr f "name"))
                     :action (.attr f "action")
                     :method (.attr f "method")
                     :fields (mapv (fn [^Element el]
                                     {:name (.attr el "name")
                                      :type (field-type el)
                                      :value (element-value el)})
                                   (.select f "input[type=text], input[type=email], input[type=password], input[type=search], input[type=number], input[type=tel], input[type=url], textarea, select"))})
                  forms)
     :link_count (count links)
     :links_truncated? (> (count links) link-preview-limit)
     :links (vec (take link-preview-limit links))
     :truncated? truncated?}))

(def ^:private query-elements-script
  (str
   "payloadJson => {"
   " const payload = JSON.parse(payloadJson);"
   " const selector = payload.selector || '*';"
   " const offset = Math.max(0, payload.offset || 0);"
   " const limit = Math.max(1, Math.min(payload.limit || 25, 200));"
   " const visibleOnly = !!payload.visibleOnly;"
   " const textContains = payload.textContains ? String(payload.textContains).toLowerCase() : null;"
   " const escapeAttr = (value) => {"
   "   const raw = String(value ?? '');"
   "   if (globalThis.CSS && typeof CSS.escape === 'function') {"
   "     return CSS.escape(raw);"
   "   }"
   "   return raw.replace(/\\\\/g, '\\\\\\\\').replace(/\"/g, '\\\\\"');"
   " };"
   " const selectorFor = (el) => {"
   "   if (el.id) {"
   "     return `[id=\"${escapeAttr(el.id)}\"]`;"
   "   }"
   "   const parts = [];"
   "   let node = el;"
   "   while (node && node.nodeType === 1) {"
   "     if (node.id) {"
   "       parts.unshift(`[id=\"${escapeAttr(node.id)}\"]`);"
   "       break;"
   "     }"
   "     const tag = node.tagName.toLowerCase();"
   "     let index = 1;"
   "     let prev = node.previousElementSibling;"
   "     while (prev) {"
   "       if (prev.tagName === node.tagName) {"
   "         index += 1;"
   "       }"
   "       prev = prev.previousElementSibling;"
   "     }"
   "     parts.unshift(`${tag}:nth-of-type(${index})`);"
   "     node = node.parentElement;"
   "   }"
   "   return parts.join(' > ') || '*';"
   " };"
   " const visible = (el) => {"
   "   if (el.matches('input[type=\"hidden\"]')) {"
   "     return false;"
   "   }"
   "   const style = window.getComputedStyle ? window.getComputedStyle(el) : null;"
   "   const rect = typeof el.getBoundingClientRect === 'function' ? el.getBoundingClientRect() : { width: 1, height: 1 };"
   "   return !(el.hidden"
   "            || el.getAttribute('aria-hidden') === 'true'"
   "            || (style && (style.display === 'none'"
   "                         || style.visibility === 'hidden'"
   "                         || style.visibility === 'collapse'))"
   "            || (rect.width === 0 && rect.height === 0));"
   " };"
   " const textFor = (el) => {"
   "   const text = (el.innerText || el.textContent || '').trim();"
   "   if (text) return text;"
   "   const value = (el.value || '').trim();"
   "   if (value) return value;"
   "   return (el.getAttribute('aria-label')"
   "           || el.getAttribute('placeholder')"
   "           || el.getAttribute('title')"
   "           || el.getAttribute('alt')"
   "           || '').trim();"
   " };"
   " const fieldType = (el) => {"
   "   const tag = el.tagName.toLowerCase();"
   "   const typeAttr = (el.getAttribute('type') || '').trim();"
   "   if (tag === 'textarea') return 'textarea';"
   "   if (tag === 'select') return 'select';"
   "   if (typeAttr) return typeAttr;"
   "   return tag;"
   " };"
   " const closestFormSelector = (el) => {"
   "   const form = el.closest('form');"
   "   return form ? selectorFor(form) : null;"
   " };"
   " const summarize = (el, index) => {"
   "   const tag = el.tagName.toLowerCase();"
   "   return {"
   "     index,"
   "     tag,"
   "     selector: selectorFor(el),"
   "     visible: visible(el),"
   "     disabled: !!el.disabled,"
   "     text: textFor(el),"
   "     id: el.id || null,"
   "     name: el.getAttribute('name'),"
   "     type: fieldType(el),"
   "     role: el.getAttribute('role'),"
   "     href: tag === 'a' ? el.getAttribute('href') : null,"
   "     value: tag === 'input' || tag === 'textarea' || tag === 'select' ? (el.value || null) : null,"
   "     placeholder: el.getAttribute('placeholder'),"
   "     'aria-label': el.getAttribute('aria-label'),"
   "     title: el.getAttribute('title'),"
   "     action: tag === 'form' ? el.getAttribute('action') : null,"
   "     method: tag === 'form' ? el.getAttribute('method') : null,"
   "     field_count: tag === 'form' ? el.querySelectorAll('input, textarea, select, button').length : null,"
   "     form_selector: closestFormSelector(el),"
   "     src: tag === 'img' ? el.getAttribute('src') : null,"
   "     alt: tag === 'img' ? el.getAttribute('alt') : null"
   "   };"
   " };"
   " const matchesText = (item) => {"
   "   if (!textContains) return true;"
   "   return [item.text, item.id, item.name, item.role, item.href, item.value,"
   "           item.placeholder, item['aria-label'], item.title, item.action,"
   "           item.method, item.selector]"
   "     .filter((value) => value !== null && value !== undefined && String(value).trim() !== '')"
   "     .some((value) => String(value).toLowerCase().includes(textContains));"
   " };"
   " const all = Array.from(document.querySelectorAll(selector));"
   " const filtered = all"
   "   .map((el, index) => summarize(el, index))"
   "   .filter((item) => (!visibleOnly || item.visible) && matchesText(item));"
   " const pageItems = filtered.slice(offset, offset + limit);"
   " return JSON.stringify({"
   "   total_count: filtered.length,"
   "   returned_count: pageItems.length,"
   "   truncated: offset + pageItems.length < filtered.length,"
   "   elements: pageItems"
   " });"
   "}"))

(defn- compact-map
  [m]
  (into {}
        (keep (fn [[k v]]
                (when-not (or (nil? v)
                              (and (string? v) (str/blank? v)))
                  [(if (keyword? k) k (keyword k)) v])))
        m))

(defn- query-elements-result
  [session-id ^Page page opts raw-json]
  (let [payload (json/read-json raw-json)
        elements (->> (get payload "elements")
                      (mapv compact-map))]
    (page-result
     session-id
     {:url (.url page)
      :title (.title page)
      :kind (:kind opts)
      :selector (:selector opts)
      :text_contains (:text-contains opts)
      :visible_only (:visible-only opts)
      :offset (:offset opts)
      :limit (:limit opts)
      :total_count (long (or (get payload "total_count") 0))
      :returned_count (long (or (get payload "returned_count") 0))
      :truncated? (boolean (get payload "truncated"))
      :elements elements})))

(defn- current-page*
  [session-atom]
  (let [{:keys [page context]} @session-atom
        page* (cond
                (and page (not (.isClosed ^Page page))) page
                context (last (remove #(.isClosed ^Page %)
                                      (.pages ^BrowserContext context)))
                :else nil)]
    (when page*
      (swap! session-atom assoc :page page*))
    page*))

(defn- live-session->snapshot
  [session-id session-atom]
  (let [{:keys [context created-at-ms last-access js-enabled]} @session-atom
        ^Page page (current-page* session-atom)]
    {"session_id" session-id
     "backend" (name backend-id)
     "current_url" (when page (.url page))
     "created_at_ms" (long (or created-at-ms (now-ms)))
     "updated_at_ms" (long (now-ms))
     "last_access_ms" (long (or last-access (now-ms)))
     "js_enabled" (if (nil? js-enabled)
                    true
                    (boolean js-enabled))
     "browser_state" (when context
                       (.storageState ^BrowserContext context))}))

(defn- persist-session!
  [ops session-id]
  (with-session-snapshot-lock
    session-id
    (fn []
      (when-let [sess (.get sessions session-id)]
        ((:write-snapshot! ops)
         session-id
         (live-session->snapshot session-id sess))))))

(defn- close-session-value!
  [sess]
  (when sess
    (try
      (.close ^BrowserContext (:context @sess))
      (catch Exception _))
    (when (:owned-browser? @sess)
      (try
        (.close ^Browser (:browser @sess))
        (catch Exception _)))
    true))

(defn- close-live-session!
  [session-id]
  (when-let [sess (.remove sessions session-id)]
    (close-session-value! sess)
    true))

(defn- discard-live-session!
  [session-id sess]
  (when sess
    (.remove sessions session-id sess)
    (close-session-value! sess)
    true))

(defn- evict-expired!
  [ops]
  (doseq [[session-id snapshot] (backend-snapshots ops)]
    (when ((:snapshot-expired? ops) snapshot)
      ((:delete-snapshot! ops) session-id)))
  (doseq [[session-id sess] sessions]
    (when (session-expired? @sess)
      (persist-session! ops session-id)
      (close-live-session! session-id)))
  (when (zero? (.size sessions))
    (stop-runtime!)))

(defn- install-request-guard!
  [ops ^BrowserContext context]
  (.route context "**/*"
          (reify java.util.function.Consumer
            (accept [_ route]
              (let [route ^Route route
                    url (.url (.request route))]
                (try
                  ((:resolve-url! ops) url)
                  (.resume route)
                  (catch Exception e
                    (log/warn e "Blocked Playwright request" url)
                    (.abort route "blockedbyclient")))))))
  context)

(defn- settle-page!
  [^Page page]
  (let [settle-timeout (double action-settle-ms)
        wait-opts (doto (com.microsoft.playwright.Page$WaitForLoadStateOptions.)
                    (.setTimeout settle-timeout))]
    (try
      (.waitForLoadState page com.microsoft.playwright.options.LoadState/DOMCONTENTLOADED wait-opts)
      (catch Exception e
        (log/debug e "Playwright DOMContentLoaded wait failed")))
    (try
      (.waitForLoadState page com.microsoft.playwright.options.LoadState/NETWORKIDLE wait-opts)
      (catch Exception e
        (log/debug e "Playwright network idle wait failed")))
    (try
      (.waitForTimeout page settle-timeout)
      (catch Exception e
        (log/debug e "Playwright page settle timeout wait failed"))))
  page)

(defn- new-context
  [ops {:keys [browser]} js-enabled storage-state]
  (let [context (.newContext ^Browser browser
                             (cond-> (doto (Browser$NewContextOptions.)
                                       (.setJavaScriptEnabled (boolean js-enabled))
                                       (.setViewportSize 1280 800))
                               (seq storage-state) (.setStorageState storage-state)))]
    (.setDefaultTimeout context (double (timeout-ms)))
    (.setDefaultNavigationTimeout context (double (timeout-ms)))
    (install-request-guard! ops context)))

(defn- session-browser
  [runtime* headless-override channel-override]
  (let [configured-headless (headless?)
        configured-channel  (normalize-channel (:browser-channel runtime*))
        effective-headless  (if (nil? headless-override)
                              configured-headless
                              (boolean headless-override))
        effective-channel   (or (normalize-channel channel-override)
                                configured-channel)]
    (if (and (= effective-headless configured-headless)
             (= effective-channel configured-channel))
      [(:browser runtime*) false]
      [(launch-browser ^Playwright (:playwright runtime*) effective-headless effective-channel) true])))

(defn- create-session!
  [ops session-id url js-enabled storage-state created-at-ms headless-override channel-override]
  (evict-expired! ops)
  (when (>= (.size sessions) (long (or (:max-sessions ops) 5)))
    (throw (ex-info (str "Too many browser sessions (max " (or (:max-sessions ops) 5)
                         "). Close one first.")
                    {:active (.size sessions)})))
  ((:validate-url! ops) url)
  (let [runtime* (ensure-runtime!)
        [browser owned-browser?] (session-browser runtime* headless-override channel-override)
        context (new-context ops {:browser browser} js-enabled storage-state)
        page (.newPage ^BrowserContext context)
        sess (atom {:context context
                    :browser browser
                    :owned-browser? owned-browser?
                    :page page
                    :last-access (now-ms)
                    :created-at-ms (long (or created-at-ms (now-ms)))
                    :js-enabled js-enabled})]
    (.put sessions session-id sess)
    (try
      (.navigate ^Page page url)
      (settle-page! page)
      (persist-session! ops session-id)
      @sess
      (catch Exception e
        (discard-live-session! session-id sess)
        (throw e)))))

(defn- restore-session!
  [ops session-id]
  (when-let [snapshot ((:read-snapshot ops) session-id)]
    (cond
      ((:snapshot-expired? ops) snapshot)
      (do
        ((:delete-snapshot! ops) session-id)
        nil)

      (str/blank? (get snapshot "current_url"))
      nil

      (not= backend-id (snapshot-backend-id snapshot))
      nil

      :else
      (let [sess (create-session! ops
                                  session-id
                                  (get snapshot "current_url")
                                  (if (contains? snapshot "js_enabled")
                                    (boolean (get snapshot "js_enabled"))
                                    true)
                                  (get snapshot "browser_state")
                                  (or (get snapshot "created_at_ms")
                                      (now-ms))
                                  nil
                                  nil)]
        (persist-session! ops session-id)
        sess))))

(defn- get-session
  [ops session-id]
  (let [sess (or (.get sessions session-id)
                 (with-session-snapshot-lock
                   session-id
                   (fn []
                     (or (.get sessions session-id)
                         (do
                           (restore-session! ops session-id)
                           (.get sessions session-id))))))]
    (when-not sess
      (throw (ex-info (str "No browser session: " session-id
                           ". Call browser-open first.")
                      {:session-id session-id})))
    (let [sess (if (session-expired? @sess)
                 (with-session-snapshot-lock
                   session-id
                   (fn []
                     (let [current (.get sessions session-id)]
                       (cond
                         (and current (not (session-expired? @current)))
                         current

                         current
                         (do
                           (persist-session! ops session-id)
                           (close-live-session! session-id)
                           (if-let [restored (restore-session! ops session-id)]
                             restored
                             (throw (ex-info "Browser session expired"
                                             {:session-id session-id}))))

                         :else
                         (if-let [restored (restore-session! ops session-id)]
                           restored
                           (throw (ex-info "Browser session expired"
                                           {:session-id session-id})))))))
                 sess)]
      (swap! sess assoc :last-access (now-ms))
      @sess)))

(defn- session-page-result
  [session-id ^Page page]
  (page-result session-id
               (document->map (.url page)
                              (.title page)
                              (.content page))))

(defn- screenshot-result
  [session-id ^Page page {:keys [full-page detail]}]
  (let [opts (doto (com.microsoft.playwright.Page$ScreenshotOptions.)
               (.setFullPage (boolean full-page)))
        data (.screenshot page opts)
        mime-type "image/png"
        title (.title page)
        url (.url page)]
    (page-result
     session-id
     {:url url
      :title title
      :mime_type mime-type
      :byte_count (alength ^bytes data)
      :full_page (boolean full-page)
      :detail (or detail "auto")
      :summary (str "Captured browser screenshot"
                    (when (seq title)
                      (str " of \"" title "\""))
                    (when (seq url)
                      (str " at " url))
                    ".")
      :image_data_url (bytes->data-url mime-type data)})))

(defn- current-page-or-throw
  [session-id session-atom]
  (or (current-page* session-atom)
      (throw (ex-info "No page loaded in session" {:session-id session-id}))))

(defn- css-attr-escape
  [s]
  (-> (str s)
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")))

(defn- field-selector
  [field-name]
  (let [value (css-attr-escape field-name)]
    (str "[name=\"" value "\"], [id=\"" value "\"]")))

(defn- form-locator
  [^Page page form-selector]
  (let [locator (if form-selector
                  (.locator page form-selector)
                  (.locator page "form"))]
    (when (zero? (.count ^Locator locator))
      (throw (ex-info "No form found on page"
                      {:form-selector form-selector})))
    (.first ^Locator locator)))

(defn- field-locator
  [^Locator form field-name]
  (let [locator (.locator form ^String (field-selector field-name))]
    (when (pos? (.count ^Locator locator))
      (.first ^Locator locator))))

(defn- fill-locator!
  [^Locator locator value]
  (let [tag-name (some-> (.evaluate locator "el => el.tagName.toLowerCase()") str/lower-case)
        type-name (some-> (.getAttribute locator "type") str/lower-case)]
    (cond
      (= "select" tag-name)
      (.selectOption locator (str value))

      (contains? #{"checkbox" "radio"} type-name)
      (.setChecked locator (boolean value))

      :else
      (.fill locator (str value)))))

(defn- submit-form!
  [^Locator form]
  (let [submit-loc (.locator form "input[type=submit], button[type=submit], button:not([type])")]
    (if (pos? (.count ^Locator submit-loc))
      (.click (.first ^Locator submit-loc))
      (.evaluate form "form => { if (typeof form.requestSubmit === 'function') { form.requestSubmit(); } else { form.submit(); } }"))))

(defn- wait-condition-met?
  [^Page page {:keys [selector text url-contains]}]
  (cond
    selector
    (try
      (boolean (.isVisible page selector))
      (catch Exception _
        false))

    text
    (try
      (str/includes? (or (.innerText page "body") "") text)
      (catch Exception _
        false))

    url-contains
    (str/includes? (.url page) url-contains)

    :else
    true))

(defrecord PlaywrightBackend [ops]
  backend/BrowserBackend
  (backend-id [_]
    backend-id)
  (runtime-status* [_]
    (runtime-status))
  (bootstrap-runtime!* [_ _opts]
    (if-not (enabled?)
      (runtime-status)
      (do
        (ensure-runtime!)
        (runtime-status))))
  (install-browser-deps!* [_ opts]
    (install-browser-deps! opts))
  (open-session* [_ url {:keys [js storage-state headless channel]}]
    (let [session-id (str (random-uuid))
          _sess (create-session! ops session-id url (if (nil? js) true js) storage-state nil headless channel)
          session-atom (.get sessions session-id)]
      (session-page-result session-id (current-page-or-throw session-id session-atom))))
  (navigate* [_ session-id url]
    ((:validate-url! ops) url)
    (let [_sess (get-session ops session-id)
          session-atom (.get sessions session-id)
          page (current-page-or-throw session-id session-atom)]
      (.navigate ^Page page url)
      (settle-page! page)
      (persist-session! ops session-id)
      (session-page-result session-id page)))
  (click* [_ session-id selector]
    (let [_sess (get-session ops session-id)
          session-atom (.get sessions session-id)
          page (current-page-or-throw session-id session-atom)
          locator (.locator ^Page page selector)]
      (when (zero? (.count ^Locator locator))
        (throw (ex-info (str "No element matches selector: " selector)
                        {:selector selector})))
      (.click (.first ^Locator locator))
      (let [page* (settle-page! (current-page-or-throw session-id session-atom))]
        (swap! session-atom assoc :page page*)
        (persist-session! ops session-id)
        (session-page-result session-id page*))))
  (fill-selector* [_ session-id selector value _opts]
    (let [_sess (get-session ops session-id)
          session-atom (.get sessions session-id)
          page (current-page-or-throw session-id session-atom)
          locator (.locator ^Page page selector)]
      (when (zero? (.count ^Locator locator))
        (throw (ex-info (str "No element matches selector: " selector)
                        {:selector selector})))
      (fill-locator! (.first ^Locator locator) value)
      (persist-session! ops session-id)
      (session-page-result session-id page)))
  (fill-form* [_ session-id fields {:keys [form-selector submit]}]
    (let [_sess (get-session ops session-id)
          session-atom (.get sessions session-id)
          page (current-page-or-throw session-id session-atom)
          form (form-locator page form-selector)]
      (doseq [[field-name value] fields]
        (if-let [locator (field-locator form field-name)]
          (fill-locator! locator value)
          (log/warn "Form field not found:" field-name)))
      (if submit
        (do
          (submit-form! form)
          (let [page* (settle-page! (current-page-or-throw session-id session-atom))]
            (swap! session-atom assoc :page page*)
            (persist-session! ops session-id)
            (session-page-result session-id page*)))
        (do
          (persist-session! ops session-id)
          (session-page-result session-id page)))))
  (read-page* [_ session-id]
    (let [_sess (get-session ops session-id)
          session-atom (.get sessions session-id)
          page (current-page-or-throw session-id session-atom)]
      (persist-session! ops session-id)
      (session-page-result session-id page)))
  (query-elements* [_ session-id opts]
    (let [_sess (get-session ops session-id)
          session-atom (.get sessions session-id)
          ^Page page (current-page-or-throw session-id session-atom)
          opts* (browser.query/normalize-opts opts)
          payload-json (json/write-json-str {:selector (:selector opts*)
                                             :textContains (:text-contains opts*)
                                             :visibleOnly (:visible-only opts*)
                                             :offset (:offset opts*)
                                             :limit (:limit opts*)})
          raw-json (try
                     (.evaluate page ^String query-elements-script ^String payload-json)
                     (catch Exception e
                       (throw (ex-info "Invalid browser query selector"
                                       {:selector (:selector opts*)}
                                       e))))]
      (persist-session! ops session-id)
      (query-elements-result session-id page opts* raw-json)))
  (screenshot* [_ session-id {:keys [full-page detail]
                              :or {full-page false
                                   detail "auto"}}]
    (let [_sess (get-session ops session-id)
          session-atom (.get sessions session-id)
          page (current-page-or-throw session-id session-atom)]
      (persist-session! ops session-id)
      (screenshot-result session-id page {:full-page full-page
                                          :detail detail})))
	  (wait-for-page* [_ session-id {:keys [timeout-ms interval-ms selector text url-contains]
	                                 :or {timeout-ms 10000
	                                      interval-ms 500}}]
	    (let [_sess (get-session ops session-id)
	          session-atom (.get sessions session-id)
	          ^Page page (current-page-or-throw session-id session-atom)
	          timeout-ms* (long (clojure.core/max 0 (long timeout-ms)))
	          interval-ms* (long (clojure.core/max 50 (long interval-ms)))
	          condition? (or selector text url-contains)
	          deadline (+ (now-ms) timeout-ms*)]
      (if-not condition?
        (do
          (.waitForTimeout page (double timeout-ms*))
          (persist-session! ops session-id)
          (assoc (session-page-result session-id page)
                 :matched true
                 :timed_out false))
        (loop []
          (.waitForTimeout page (double interval-ms*))
          (if (wait-condition-met? page {:selector selector
                                         :text text
                                         :url-contains url-contains})
            (do
              (persist-session! ops session-id)
              (assoc (session-page-result session-id page)
                     :matched true
                     :timed_out false))
            (if (>= (now-ms) deadline)
              (do
                (persist-session! ops session-id)
                (assoc (session-page-result session-id page)
                       :matched false
                       :timed_out true))
              (recur)))))))
  (close-session* [_ session-id]
    (close-live-session! session-id)
    ((:delete-snapshot! ops) session-id)
    (when (zero? (.size sessions))
      (stop-runtime!))
    {:status "closed" :session-id session-id})
  (close-all-sessions!* [_]
    (doseq [[session-id sess] sessions]
      (close-session-value! sess)
      (.remove sessions session-id))
    (doseq [[session-id _snapshot] (backend-snapshots ops)]
      ((:delete-snapshot! ops) session-id))
    (stop-runtime!)
    {:backend backend-id
     :status "closed"})
	  (list-sessions* [_]
	    (evict-expired! ops)
	    (let [snapshot-map (into {}
	                             (keep (fn [[session-id snapshot]]
	                                     (when-not ((:snapshot-expired? ops) snapshot)
	                                       (let [last-access-ms (long (or (get snapshot "last_access_ms")
	                                                                      (get snapshot "updated_at_ms")
	                                                                      0))]
	                                       [session-id {:session-id session-id
	                                                    :backend backend-id
	                                                    :url (get snapshot "current_url")
	                                                    :age-seconds (quot (- (now-ms) last-access-ms)
	                                                                       1000)
	                                                    :live? false
	                                                    :resumable? true}])))
	                                   (backend-snapshots ops)))]
	      (->> sessions
	           (reduce (fn [acc [session-id sess]]
	                     (let [^Page page (current-page* sess)
	                           state @sess
	                           last-access-ms (long (:last-access state))]
	                       (assoc acc session-id
	                              {:session-id session-id
	                               :backend backend-id
	                               :url (when page (.url page))
	                               :age-seconds (quot (- (now-ms)
	                                                     last-access-ms)
	                                                  1000)
	                               :live? true
                               :resumable? true})))
                   snapshot-map)
           vals
           (sort-by :session-id)
           vec))))

(defn create-backend
  [ops]
  (->PlaywrightBackend ops))
