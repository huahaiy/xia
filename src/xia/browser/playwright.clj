(ns xia.browser.playwright
  "Playwright browser backend."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.browser.backend :as backend]
            [xia.config :as cfg])
  (:import [com.microsoft.playwright Playwright Browser BrowserContext Locator Page Route
            Browser$NewContextOptions BrowserType$LaunchOptions]
           [java.util.concurrent ConcurrentHashMap]
           [org.jsoup Jsoup]
           [org.jsoup.nodes Element]))

(def ^:private backend-id :playwright)
(def ^:private max-content-chars 8000)
(def ^:private live-session-ttl-ms (* 60 60 1000))
(def ^:private action-settle-ms 1200)
(def ^:private session-restore-lock-count 256)

(defonce ^:private runtime (atom {:playwright nil
                                  :browser nil
                                  :bootstrapped? false}))
(defonce ^:private runtime-lock (Object.))
(defonce ^:private sessions (ConcurrentHashMap.))
(defonce ^:private session-restore-locks
  (vec (repeatedly session-restore-lock-count #(Object.))))

(defn- now-ms []
  (System/currentTimeMillis))

(defn- enabled? []
  (cfg/boolean-option :browser/playwright-enabled? true))

(defn- headless? []
  (cfg/boolean-option :browser/playwright-headless? true))

(defn- timeout-ms []
  (cfg/positive-long :browser/playwright-timeout-ms 15000))

(defn- session-restore-lock
  [session-id]
  (when session-id
    (nth session-restore-locks
         (mod (bit-and Integer/MAX_VALUE (hash session-id))
              session-restore-lock-count))))

(defn- with-session-restore-lock
  [session-id f]
  (if-let [lock (session-restore-lock session-id)]
    (locking lock
      (f))
    (f)))

(defn- runtime-running?
  [{:keys [browser]}]
  (try
    (boolean (and browser (.isConnected ^Browser browser)))
    (catch Exception _
      false)))

(defn- stop-runtime!
  []
  (locking runtime-lock
    (let [{:keys [browser playwright]} @runtime]
      (try
        (when browser
          (.close ^Browser browser))
        (catch Exception _))
      (try
        (when playwright
          (.close ^Playwright playwright))
        (catch Exception _))
      (reset! runtime {:playwright nil
                       :browser nil
                       :bootstrapped? false}))))

(defn runtime-status
  []
  (let [{:keys [bootstrapped?]} @runtime
        running? (runtime-running? @runtime)]
    (cond
      (not (enabled?))
      {:backend backend-id
       :available? false
       :ready? false
       :running? false
       :bootstrapped? false
       :status :disabled
       :message "Playwright backend is disabled by config."}

      running?
      {:backend backend-id
       :available? true
       :ready? true
       :running? true
       :bootstrapped? true
       :status :running
       :message "Playwright backend is running."}

      bootstrapped?
      {:backend backend-id
       :available? true
       :ready? true
       :running? false
       :bootstrapped? true
       :status :ready
       :message "Playwright backend is bootstrapped."}

      :else
      {:backend backend-id
       :available? true
       :ready? false
       :running? false
       :bootstrapped? false
       :status :available
       :message "Playwright backend is available but not bootstrapped."})))

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
      (let [playwright (Playwright/create)]
        (try
          (let [browser (.launch (.chromium playwright)
                                 (doto (BrowserType$LaunchOptions.)
                                   (.setHeadless (headless?))
                                   (.setTimeout (double (timeout-ms)))))
                state {:playwright playwright
                       :browser browser
                       :bootstrapped? true}]
            (reset! runtime state)
            state)
          (catch Exception e
            (try (.close playwright) (catch Exception _))
            (throw e)))))))

(defn- page-result
  [session-id result]
  (assoc result :session-id session-id
                :backend backend-id))

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
  (> (- (now-ms) last-access) live-session-ttl-ms))

(defn- page-text
  [doc]
  (some-> doc
          .body
          .text
          str))

(defn- truncate-content
  [text]
  (let [text* (or text "")]
    (if (> (count text*) max-content-chars)
      [(str (subs text* 0 max-content-chars) "\n\n[content truncated]") true]
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
  (let [doc (Jsoup/parse (or html "") (or url "about:blank"))
        text (page-text doc)
        [content truncated?] (truncate-content text)
        forms (.select doc "form")
        anchors (.select doc "a[href]")]
    {:url url
     :title (or title (.title doc))
     :content content
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
     :links (vec
             (take 20
                   (keep (fn [^Element a]
                           (let [text* (str/trim (.text a))
                                 href (.attr a "href")]
                             (when (and (seq text*)
                                        (seq href)
                                        (not (str/starts-with? href "javascript:")))
                               {:text text* :url href})))
                         anchors)))
     :truncated? truncated?}))

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
        page (current-page* session-atom)]
    {"session_id" session-id
     "backend" (name backend-id)
     "current_url" (some-> page .url)
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
  (when-let [sess (.get sessions session-id)]
    ((:write-snapshot! ops)
     session-id
     (live-session->snapshot session-id sess))))

(defn- close-session-value!
  [sess]
  (when sess
    (try
      (.close ^BrowserContext (:context @sess))
      (catch Exception _))
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

(defn- create-session!
  [ops session-id url js-enabled storage-state created-at-ms]
  (evict-expired! ops)
  (when (>= (.size sessions) (long (or (:max-sessions ops) 5)))
    (throw (ex-info (str "Too many browser sessions (max " (or (:max-sessions ops) 5)
                         "). Close one first.")
                    {:active (.size sessions)})))
  ((:validate-url! ops) url)
  (let [runtime* (ensure-runtime!)
        context (new-context ops runtime* js-enabled storage-state)
        page (.newPage ^BrowserContext context)
        sess (atom {:context context
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
                                      (now-ms)))]
        (persist-session! ops session-id)
        sess))))

(defn- get-session
  [ops session-id]
  (let [sess (or (.get sessions session-id)
                 (with-session-restore-lock
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
                 (with-session-restore-lock
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
  (let [locator (.locator form (field-selector field-name))]
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
  (open-session* [_ url {:keys [js]}]
    (let [session-id (str (random-uuid))
          _sess (create-session! ops session-id url (if (nil? js) true js) nil nil)
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
  (wait-for-page* [_ session-id {:keys [timeout-ms interval-ms selector text url-contains]
                                 :or {timeout-ms 10000
                                      interval-ms 500}}]
    (let [_sess (get-session ops session-id)
          session-atom (.get sessions session-id)
          page (current-page-or-throw session-id session-atom)
          timeout-ms* (long (max 0 timeout-ms))
          interval-ms* (long (max 50 interval-ms))
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
      (try
        (.close ^BrowserContext (:context @sess))
        (catch Exception _))
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
                                       [session-id {:session-id session-id
                                                    :backend backend-id
                                                    :url (get snapshot "current_url")
                                                    :age-seconds (quot (- (now-ms)
                                                                          (long (or (get snapshot "last_access_ms")
                                                                                    (get snapshot "updated_at_ms")
                                                                                    0)))
                                                                       1000)
                                                    :live? false
                                                    :resumable? true}]))
                                   (backend-snapshots ops)))]
      (->> sessions
           (reduce (fn [acc [session-id sess]]
                     (let [page (current-page* sess)
                           state @sess]
                       (assoc acc session-id
                              {:session-id session-id
                               :backend backend-id
                               :url (some-> page .url)
                               :age-seconds (quot (- (now-ms)
                                                     (:last-access state))
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
