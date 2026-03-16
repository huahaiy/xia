(ns xia.browser
  "Headless browser for interactive web browsing.

   Provides stateful browser sessions behind a pluggable backend seam.
   Today Xia registers HtmlUnit as the default backend. Tools can open a
   session, navigate pages, click elements, fill forms, and submit — all
   within the SCI sandbox. Each action returns the resulting page content
   so the LLM minimizes round-trips.

   Live sessions auto-close after an idle timeout, but their cookies and
   current URL are snapshotted into Xia's DB so multi-step browser tasks
   can resume later."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [charred.api :as json]
            [datalevin.core :as d]
            [xia.browser.backend :as browser.backend]
            [xia.browser.htmlunit :as htmlunit]
            [xia.browser.playwright :as playwright]
            [xia.autonomous :as autonomous]
            [xia.config :as cfg]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.ssrf :as ssrf])
  (:import [org.htmlunit WebClient BrowserVersion WebRequest HttpWebConnection]
           [org.htmlunit.html HtmlPage HtmlElement HtmlForm
            HtmlInput HtmlTextArea HtmlSelect
            HtmlOption HtmlAnchor
            DomElement DomNode]
           [org.htmlunit.util Cookie WebConnectionWrapper]
           [datalevin.db DB]
           [datalevin.storage Store]
           [java.util Date]
           [java.util.concurrent ConcurrentHashMap]))

;; ---------------------------------------------------------------------------
;; Session management
;; ---------------------------------------------------------------------------

(def ^:private max-sessions 5)
(def ^:private live-session-ttl-ms (* 60 60 1000)) ; 60 minutes
(def ^:private resumable-session-ttl-ms (* 7 24 60 60 1000)) ; 7 days
(def ^:private browser-session-dbi "xia/browser-sessions")
(def ^:private action-settle-ms 1200)
(def ^:private session-restore-lock-count 256)
(def ^:private htmlunit-backend-id :htmlunit)
(def ^:private auto-backend-id :auto)

(defonce ^:private sessions (ConcurrentHashMap.))
(defonce ^:private session-restore-locks
  (vec (repeatedly session-restore-lock-count #(Object.))))

(declare resolve-url!)
(declare send-browser-request!)
(declare validate-url!)
(declare current-page)
(declare evict-expired!)

(defn- now-ms []
  (System/currentTimeMillis))

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

(defn- lmdb []
  (try
    (let [db-value (d/db (db/conn))]
      (.-lmdb ^Store (.-store ^DB db-value)))
    (catch Exception _
      nil)))

(defn- ensure-session-dbi! []
  (when-let [store (lmdb)]
    (d/open-dbi store browser-session-dbi {:key-size 128})
    store))

(defn- snapshot-aad
  [session-id]
  (str "browser-session:" session-id))

(defn- date->millis
  [value]
  (some-> ^Date value .getTime))

(defn- millis->date
  [value]
  (when (some? value)
    (Date. (long value))))

(defn- cookie->body
  [^Cookie cookie]
  {:name (.getName cookie)
   :value (.getValue cookie)
   :domain (.getDomain cookie)
   :path (.getPath cookie)
   :expires_at (date->millis (.getExpires cookie))
   :secure (.isSecure cookie)
   :http_only (.isHttpOnly cookie)
   :same_site (.getSameSite cookie)})

(defn- body->cookie
  [body]
  (let [domain (get body "domain")
        name (get body "name")
        value (get body "value")
        path (or (get body "path") "/")
        expires (millis->date (get body "expires_at"))
        secure? (boolean (get body "secure"))
        http-only (boolean (get body "http_only"))
        same-site (some-> (get body "same_site") str str/trim)]
    (if (seq same-site)
      (Cookie. domain name value path expires secure? http-only same-site)
      (Cookie. domain name value path expires secure? http-only))))

(defn- read-session-snapshot
  [session-id]
  (when-let [store (ensure-session-dbi!)]
    (when-let [ciphertext (d/get-value store browser-session-dbi session-id :string :string)]
      (some-> ciphertext
              (crypto/decrypt (snapshot-aad session-id))
              json/read-json))))

(defn- write-session-snapshot!
  [session-id snapshot]
  (when-let [store (ensure-session-dbi!)]
    (d/transact-kv store
                   browser-session-dbi
                   [[:put session-id
                     (crypto/encrypt (json/write-json-str snapshot)
                                     (snapshot-aad session-id))]]
                   :string
                   :string)))

(defn- delete-session-snapshot!
  [session-id]
  (when-let [store (ensure-session-dbi!)]
    (d/transact-kv store
                   browser-session-dbi
                   [[:del session-id]]
                   :string)))

(defn- all-session-snapshots
  []
  (if-let [store (ensure-session-dbi!)]
    (->> (d/get-range store browser-session-dbi [:all] :string :string false)
         (keep (fn [[session-id ciphertext]]
                 (try
                   [session-id (json/read-json (crypto/decrypt ciphertext
                                                               (snapshot-aad session-id)))]
                   (catch Exception e
                     (log/warn e "Failed to read browser session snapshot" session-id)
                     nil)))))
    []))

(defn- live-session->snapshot
  [session-id {:keys [backend client created-at-ms last-access js-enabled]}]
  (let [page (current-page client)
        cookies (.getCookies (.getCookieManager ^WebClient client))]
    {"session_id" session-id
     "backend" (name (or backend htmlunit-backend-id))
     "current_url" (some-> page .getUrl str)
     "created_at_ms" (long (or created-at-ms (now-ms)))
     "updated_at_ms" (long (now-ms))
     "last_access_ms" (long (or last-access (now-ms)))
     "js_enabled" (if (nil? js-enabled)
                    true
                    (boolean js-enabled))
     "cookies" (mapv cookie->body cookies)}))

(defn- snapshot-expired?
  [snapshot]
  (> (- (now-ms)
        (long (or (get snapshot "last_access_ms")
                  (get snapshot "updated_at_ms")
                  0)))
     resumable-session-ttl-ms))

(defn- persist-session!
  [session-id]
  (when-let [sess (.get sessions session-id)]
    (write-session-snapshot! session-id (live-session->snapshot session-id @sess))))

(defn- snapshot-backend-id
  [snapshot]
  (some-> (or (get snapshot "backend")
              (name htmlunit-backend-id))
          keyword))

(defn- close-session-value!
  [sess]
  (when sess
    (try (.close ^WebClient (:client @sess)) (catch Exception _))
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

(defn- install-url-guard!
  [^WebClient client]
  (.setWebConnection
   client
   (proxy [WebConnectionWrapper] [client]
     (getResponse [^WebRequest request]
       (let [resolution (resolve-url! (str (.getUrl request)))]
         (send-browser-request! client request resolution)))))
  client)

(defn- make-client
  "Create a configured HtmlUnit WebClient."
  ^WebClient []
  (let [client (WebClient. BrowserVersion/CHROME)]
    (doto client
      (-> .getOptions (.setCssEnabled false))
      (-> .getOptions (.setJavaScriptEnabled true))
      (-> .getOptions (.setThrowExceptionOnScriptError false))
      (-> .getOptions (.setThrowExceptionOnFailingStatusCode false))
      (-> .getOptions (.setPrintContentOnFailingStatusCode false))
      (-> .getOptions (.setTimeout 15000))
      (-> .getOptions (.setDownloadImages false))
      install-url-guard!)))

(defn- session-expired?
  [{:keys [last-access]}]
  (> (- (now-ms) last-access) live-session-ttl-ms))

(defn- wait-for-js!
  [^WebClient client wait-ms]
  (let [wait-ms* (long (max 0 (or wait-ms 0)))]
    (when (pos? wait-ms*)
      (try
        (.waitForBackgroundJavaScriptStartingBefore client wait-ms*)
        (.waitForBackgroundJavaScript client wait-ms*)
        (catch Exception e
          (log/debug e "Browser JS wait failed"))))
    client))

(defn- restore-session!
  [session-id]
  (when-let [snapshot (read-session-snapshot session-id)]
    (cond
      (snapshot-expired? snapshot)
      (do
        (delete-session-snapshot! session-id)
        nil)

      (str/blank? (get snapshot "current_url"))
      nil

      :else
      (do
        (when-not (= htmlunit-backend-id (snapshot-backend-id snapshot))
          (throw (ex-info "Unsupported browser backend in snapshot"
                          {:session-id session-id
                           :backend (get snapshot "backend")})))
        (evict-expired!)
        (when (>= (.size sessions) max-sessions)
          (throw (ex-info (str "Too many browser sessions (max " max-sessions
                               "). Close one first.")
                          {:active (count sessions)})))
        (let [client (make-client)
              js? (if (contains? snapshot "js_enabled")
                    (boolean (get snapshot "js_enabled"))
                    true)
              sid session-id]
          (when-not js?
            (-> client .getOptions (.setJavaScriptEnabled false)))
          (doseq [cookie-body (get snapshot "cookies")]
            (try
              (.addCookie (.getCookieManager client) (body->cookie cookie-body))
              (catch Exception e
                (log/debug e "Skipping invalid restored cookie" session-id))))
          (let [sess (atom {:client client
                            :backend htmlunit-backend-id
                            :last-access (now-ms)
                            :created-at-ms (long (or (get snapshot "created_at_ms")
                                                     (now-ms)))
                            :js-enabled js?})]
            (.put sessions sid sess)
            (try
              (.getPage client ^String (get snapshot "current_url"))
              (wait-for-js! client action-settle-ms)
              (persist-session! sid)
              @sess
              (catch Exception e
                (discard-live-session! sid sess)
                (throw (ex-info "Failed to restore browser session"
                                {:session-id session-id
                                 :url (get snapshot "current_url")}
                                e))))))))))

(defn- evict-expired!
  "Remove expired live sessions and expired resumable snapshots."
  []
  (doseq [[session-id snapshot] (all-session-snapshots)]
    (when (snapshot-expired? snapshot)
      (delete-session-snapshot! session-id)))
  (doseq [[id sess] sessions]
    (when (session-expired? @sess)
      (persist-session! id)
      (close-live-session! id))))

(defn- get-session
  "Get a live session or restore it from the persisted snapshot."
  [session-id]
  (let [sess (or (.get sessions session-id)
                 (with-session-restore-lock
                   session-id
                   (fn []
                     (or (.get sessions session-id)
                         (do
                           (restore-session! session-id)
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
                           (persist-session! session-id)
                           (close-live-session! session-id)
                           (if-let [restored (restore-session! session-id)]
                             restored
                             (throw (ex-info "Browser session expired"
                                             {:session-id session-id}))))

                         :else
                         (if-let [restored (restore-session! session-id)]
                           restored
                           (throw (ex-info "Browser session expired"
                                           {:session-id session-id})))))))
                 sess)]
      (swap! sess assoc :last-access (now-ms))
      @sess)))

;; ---------------------------------------------------------------------------
;; Page → readable content
;; ---------------------------------------------------------------------------

(def ^:private max-content-chars 8000) ; ~2000 tokens

(defn- page->map
  "Convert an HtmlPage to a tool-friendly result map."
  [^HtmlPage page]
  (let [text    (.asNormalizedText page)
        content (if (> (count text) max-content-chars)
                  (str (subs text 0 max-content-chars) "\n\n[content truncated]")
                  text)
        forms   (.getForms page)
        anchors (.getAnchors page)]
    {:url        (.toString (.getUrl page))
     :title      (.getTitleText page)
     :content    content
     :forms      (mapv (fn [^HtmlForm f]
                         {:id     (let [id (.getId f)] (when (seq id) id))
                          :name   (let [n (.getNameAttribute f)] (when (seq n) n))
                          :action (.getActionAttribute f)
                          :method (.getMethodAttribute f)
                          :fields (vec
                                    (for [^HtmlElement el (concat
                                                            (.getInputsByType f "text")
                                                            (.getInputsByType f "email")
                                                            (.getInputsByType f "password")
                                                            (.getInputsByType f "search")
                                                            (.getInputsByType f "number")
                                                            (.getInputsByType f "tel")
                                                            (.getInputsByType f "url"))]
                                      {:name  (.getAttribute el "name")
                                       :type  (.getAttribute el "type")
                                       :value (.getAttribute el "value")}))})
                       forms)
     :links      (vec
                   (take 20
                     (for [^HtmlAnchor a anchors
                           :let [text (str/trim (.asNormalizedText a))
                                 href (.getHrefAttribute a)]
                           :when (and (seq text)
                                      (seq href)
                                      (not (str/starts-with? href "javascript:")))]
                       {:text text :url href})))
     :truncated? (> (count text) max-content-chars)}))

(defn- current-page
  "Get the current page from a WebClient, or nil."
  [^WebClient client]
  (try
    (let [w (.getCurrentWindow client)]
      (when w
        (let [page (.getEnclosedPage w)]
          (when (instance? HtmlPage page)
            page))))
    (catch Exception _ nil)))

(defn- page-result
  [session-id result]
  (assoc result :session-id session-id
                :backend htmlunit-backend-id))

(defn- selector-match?
  [^HtmlPage page selector]
  (boolean (.querySelector page selector)))

(defn- wait-condition-met?
  [^HtmlPage page {:keys [selector text url-contains]}]
  (cond
    selector
    (selector-match? page selector)

    text
    (str/includes? (.asNormalizedText page) text)

    url-contains
    (str/includes? (str (.getUrl page)) url-contains)

    :else
    true))

;; ---------------------------------------------------------------------------
;; SSRF protection (reuse pattern from xia.web)
;; ---------------------------------------------------------------------------

(defn- resolve-host-addresses
  [host]
  (ssrf/resolve-host-addresses host))

(defn- resolve-url!
  [url]
  (ssrf/resolve-public-url! resolve-host-addresses url))

(defn- validate-url!
  [url]
  (ssrf/validate-url! resolve-host-addresses url))

(defn- new-pinned-web-connection
  [^WebClient client resolution]
  ;; HttpWebConnection caches its Apache connection manager, so create a fresh
  ;; instance per request to ensure the pinned DNS resolver is applied.
  (proxy [HttpWebConnection] [client]
    (createHttpClientBuilder []
      (doto (proxy-super createHttpClientBuilder)
        (.setDnsResolver (ssrf/pinned-dns-resolver resolution))))))

(defn- send-browser-request!
  [^WebClient client ^WebRequest request resolution]
  (let [connection (new-pinned-web-connection client resolution)]
    (try
      (.getResponse connection request)
      (finally
        (.close connection)))))

;; ---------------------------------------------------------------------------
;; HtmlUnit backend implementation
;; ---------------------------------------------------------------------------

(defn- htmlunit-open-session
  "Open a new browser session and navigate to a URL.

   Returns the page content along with a session-id for subsequent calls.

   Options:
     :js — enable JavaScript (default true)"
  [url & {:keys [js] :or {js true}}]
  (evict-expired!)
  (when (>= (.size sessions) max-sessions)
    (throw (ex-info (str "Too many browser sessions (max " max-sessions
                         "). Close one first.")
                    {:active (count sessions)})))
  (validate-url! url)
  (let [sid (str (random-uuid))
        client (make-client)]
    (when-not js
      (-> client .getOptions (.setJavaScriptEnabled false)))
    (.put sessions sid (atom {:client client
                              :backend htmlunit-backend-id
                              :last-access (now-ms)
                              :created-at-ms (now-ms)
                              :js-enabled js}))
    (try
      (let [page (.getPage client url)]
        (wait-for-js! client action-settle-ms)
        (persist-session! sid)
        (if (instance? HtmlPage page)
          (page-result sid (page->map page))
          (page-result sid {:url url
                            :content (str page)
                            :forms []
                            :links []})))
      (catch Exception e
        ;; Clean up on failure
        (close-live-session! sid)
        (delete-session-snapshot! sid)
        (throw e)))))

(defn- htmlunit-navigate
  "Navigate to a new URL in an existing session.
   Returns the page content."
  [session-id url]
  (validate-url! url)
  (let [{:keys [client]} (get-session session-id)
        page (.getPage ^WebClient client ^String url)]
    (wait-for-js! client action-settle-ms)
    (persist-session! session-id)
    (if (instance? HtmlPage page)
      (page-result session-id (page->map page))
      (page-result session-id {:url url :content (str page) :forms [] :links []}))))

(defn- htmlunit-click
  "Click an element matching a CSS selector.
   Returns the resulting page content."
  [session-id selector]
  (let [{:keys [client]} (get-session session-id)
        page (current-page client)]
    (when-not page
      (throw (ex-info "No page loaded in session" {:session-id session-id})))
    (let [el (.querySelector page selector)]
      (when-not el
        (throw (ex-info (str "No element matches selector: " selector)
                        {:selector selector})))
      (let [result (.click ^HtmlElement el)]
        (wait-for-js! client action-settle-ms)
        (persist-session! session-id)
        (if (instance? HtmlPage result)
          (page-result session-id (page->map result))
          ;; Click might return a different page type or same page
          (if-let [p (current-page client)]
            (page-result session-id (page->map p))
            (page-result session-id
                         {:content "Click performed but no HTML page resulted."
                          :forms [] :links []})))))))

(defn- find-form-field-element
  [^HtmlForm form field-name]
  (or
    ;; Prefer HtmlUnit's exact-name lookups for standard controls.
    (try (.getInputByName form ^String field-name)
         (catch Exception _ nil))
    (try (first (.getTextAreasByName form ^String field-name))
         (catch Exception _ nil))
    (try (first (.getSelectsByName form ^String field-name))
         (catch Exception _ nil))
    ;; Fall back to exact attribute matching for id-based addressing without
    ;; interpreting the field name as a CSS selector.
    (some (fn [^HtmlElement el]
            (when (or (= field-name (.getAttribute el "name"))
                      (= field-name (.getAttribute el "id")))
              el))
          (.getFormElements form))))

(defn- htmlunit-fill-form
  "Fill form fields and optionally submit.

   Arguments:
     session-id — browser session
     fields     — map of field name/id to value
     opts:
       :form-selector — CSS selector for the form (default: first form)
       :submit        — true to submit after filling (default false)

   Returns the page content (after submit if :submit true)."
  [session-id fields & {:keys [form-selector submit]
                        :or {submit false}}]
  (let [{:keys [client]} (get-session session-id)
        page (current-page client)]
    (when-not page
      (throw (ex-info "No page loaded in session" {:session-id session-id})))
    (let [form (if form-selector
                 (.querySelector page form-selector)
                 (first (.getForms page)))]
      (when-not form
        (throw (ex-info "No form found on page"
                        {:form-selector form-selector})))
      ;; Fill each field
      (doseq [[field-name value] fields]
        (let [el (find-form-field-element form field-name)]
          (when-not el
            (log/warn "Form field not found:" field-name))
          (when el
            (cond
              (instance? HtmlInput el)
              (.setValue ^HtmlInput el ^String (str value))

              (instance? HtmlTextArea el)
              (.setText ^HtmlTextArea el ^String (str value))

              (instance? HtmlSelect el)
              (.setSelectedAttribute ^HtmlSelect el ^String (str value) true)

              :else
              (.setAttribute ^DomElement el "value" (str value))))))
      ;; Submit if requested
      (if submit
        (let [submit-btn (or
                           ;; Find submit button
                          (first (.getInputsByType ^HtmlForm form "submit"))
                           ;; Find button[type=submit]
                          (.querySelector ^HtmlForm form "button[type=submit], button:not([type])"))]
          (let [result (if submit-btn
                         (.click ^HtmlElement submit-btn)
                         ;; Fallback: submit the form directly
                         ;; HtmlForm doesn't have a direct submit(), so we click the first button
                         ;; or use JS to submit
                         (do (.getPage ^WebClient client
                                       (.toString (.getUrl page)))
                             (current-page client)))]
            (wait-for-js! client action-settle-ms)
            (persist-session! session-id)
            (if (instance? HtmlPage result)
              (page-result session-id (page->map result))
              (if-let [p (current-page client)]
                (page-result session-id (page->map p))
                (page-result session-id
                             {:content "Form submitted but no HTML page resulted."
                              :forms [] :links []})))))
        ;; No submit — return current page state
        (do
          (persist-session! session-id)
          (page-result session-id (page->map page)))))))

(defn- htmlunit-read-page
  "Read the current page content in a session.
   Useful after waiting for JS to render."
  [session-id]
  (let [{:keys [client]} (get-session session-id)]
    (persist-session! session-id)
    (if-let [page (current-page client)]
      (page-result session-id (page->map page))
      (page-result session-id {:content "No page loaded." :forms [] :links []}))))

(defn- htmlunit-wait-for-page
  "Wait for JS/rendering in a browser session, optionally until a selector,
   text snippet, or URL substring appears. Returns the current page along with
   :matched and :timed_out flags."
  [session-id & {:keys [timeout-ms interval-ms selector text url-contains]
                 :or {timeout-ms 10000
                      interval-ms 500}}]
  (let [{:keys [client]} (get-session session-id)
        timeout-ms* (long (max 0 timeout-ms))
        interval-ms* (long (max 50 interval-ms))
        condition? (or selector text url-contains)
        deadline (+ (now-ms) timeout-ms*)]
    (if-not condition?
      (do
        (wait-for-js! client timeout-ms*)
        (persist-session! session-id)
        (if-let [page (current-page client)]
          (assoc (page-result session-id (page->map page))
                 :matched true
                 :timed_out false)
          (page-result session-id
                       {:content "No page loaded."
                        :forms []
                        :links []
                        :matched false
                        :timed_out true})))
      (loop []
        (wait-for-js! client interval-ms*)
        (if-let [page (current-page client)]
          (if (wait-condition-met? page {:selector selector
                                         :text text
                                         :url-contains url-contains})
            (do
              (persist-session! session-id)
              (assoc (page-result session-id (page->map page))
                     :matched true
                     :timed_out false))
            (if (>= (now-ms) deadline)
              (do
                (persist-session! session-id)
                (assoc (page-result session-id (page->map page))
                       :matched false
                       :timed_out true))
              (recur)))
          (if (>= (now-ms) deadline)
            (page-result session-id
                         {:content "No page loaded."
                          :forms []
                          :links []
                          :matched false
                          :timed_out true})
            (recur)))))))

(defn- htmlunit-close-session
  "Close a browser session and free resources."
  [session-id]
  (close-live-session! session-id)
  (delete-session-snapshot! session-id)
  {:status "closed" :session-id session-id})

(defn- htmlunit-close-all-sessions!
  "Close all browser sessions and remove any saved resume snapshots.
   Useful for test cleanup."
  []
  (doseq [[id sess] sessions]
    (try (.close ^WebClient (:client @sess)) (catch Exception _))
    (.remove sessions id))
  (doseq [[session-id _snapshot] (all-session-snapshots)]
    (delete-session-snapshot! session-id)))

(defn- htmlunit-list-sessions
  "List browser sessions, including resumable sessions restored from snapshots."
  []
  (evict-expired!)
  (let [snapshot-map (into {}
                           (keep (fn [[session-id snapshot]]
                                   (when-not (snapshot-expired? snapshot)
                                     [session-id {:session-id session-id
                                                  :backend (snapshot-backend-id snapshot)
                                                  :url (get snapshot "current_url")
                                                  :age-seconds (quot (- (now-ms)
                                                                        (long (or (get snapshot "last_access_ms")
                                                                                  (get snapshot "updated_at_ms")
                                                                                  0)))
                                                                     1000)
                                                  :live? false
                                                  :resumable? true}]))
                                 (all-session-snapshots)))]
    (->> sessions
         (reduce (fn [acc [id sess]]
                   (let [s @sess]
                     (assoc acc id
                            {:session-id id
                             :backend (or (:backend s) htmlunit-backend-id)
                             :url (try (some-> (:client s) current-page
                                               (.getUrl) str)
                                       (catch Exception _ nil))
                             :age-seconds (quot (- (now-ms)
                                                   (:last-access s))
                                                1000)
                             :live? true
                             :resumable? true})))
                 snapshot-map)
         vals
         (sort-by :session-id)
         vec)))

(defn- htmlunit-runtime-status
  []
  {:backend htmlunit-backend-id
   :available? true
   :ready? true
   :status :ready
   :message "HtmlUnit backend is ready."})

(defn- htmlunit-bootstrap-runtime!
  [_opts]
  (assoc (htmlunit-runtime-status) :bootstrapped? true))

(defn- htmlunit-install-browser-deps!
  [_opts]
  {:backend htmlunit-backend-id
   :supported? false
   :status :unsupported-backend
   :message "HtmlUnit does not require Playwright system dependencies."})

(defonce ^:private registered-backends (atom {}))

(defn- register-backend!
  [backend]
  (swap! registered-backends assoc (browser.backend/backend-id backend) backend)
  backend)

(defn- backend-by-id
  [backend-id]
  (or (get @registered-backends backend-id)
      (throw (ex-info (str "Unsupported browser backend: " backend-id)
                      {:backend backend-id}))))

(defn- normalize-backend-id
  [backend]
  (cond
    (nil? backend) auto-backend-id
    (keyword? backend) backend
    (string? backend) (keyword backend)
    :else (throw (ex-info "Browser backend must be a keyword or string"
                          {:backend backend}))))

(defn- configured-default-backend-id
  []
  (cfg/keyword-option :browser/backend-default
                      auto-backend-id
                      #{auto-backend-id htmlunit-backend-id :playwright}))

(defn- auto-backend-id*
  []
  (or (first (filter (fn [backend-id]
                       (when-let [backend (get @registered-backends backend-id)]
                         (true? (:available? (browser.backend/runtime-status* backend)))))
                     [:playwright htmlunit-backend-id]))
      htmlunit-backend-id))

(defn- resolve-open-backend-id
  [requested-backend]
  (let [backend-id (if (nil? requested-backend)
                     (configured-default-backend-id)
                     (normalize-backend-id requested-backend))]
    (if (= auto-backend-id backend-id)
      (auto-backend-id*)
      backend-id)))

(defn- session-backend-id
  [session-id]
  (or (some-> (.get sessions session-id) deref :backend)
      (some-> (read-session-snapshot session-id) snapshot-backend-id)
      htmlunit-backend-id))

(def ^:private htmlunit-backend
  (register-backend!
   (htmlunit/create-backend
    {:id htmlunit-backend-id
     :runtime-status htmlunit-runtime-status
     :bootstrap-runtime htmlunit-bootstrap-runtime!
     :install-browser-deps htmlunit-install-browser-deps!
     :open-session (fn [url opts]
                     (htmlunit-open-session url
                                            :js (if (contains? opts :js)
                                                  (:js opts)
                                                  true)))
     :navigate htmlunit-navigate
     :click htmlunit-click
     :fill-form (fn [session-id fields opts]
                  (htmlunit-fill-form session-id
                                      fields
                                      :form-selector (:form-selector opts)
                                      :submit (:submit opts)))
     :read-page htmlunit-read-page
     :wait-for-page (fn [session-id opts]
                      (htmlunit-wait-for-page session-id
                                              :timeout-ms (or (:timeout-ms opts) 10000)
                                              :interval-ms (or (:interval-ms opts) 500)
                                              :selector (:selector opts)
                                              :text (:text opts)
                                              :url-contains (:url-contains opts)))
     :close-session htmlunit-close-session
     :close-all-sessions htmlunit-close-all-sessions!
     :list-sessions htmlunit-list-sessions})))

(def ^:private playwright-backend
  (register-backend!
   (playwright/create-backend
    {:max-sessions max-sessions
     :now-ms now-ms
     :read-snapshot read-session-snapshot
     :write-snapshot! write-session-snapshot!
     :delete-snapshot! delete-session-snapshot!
     :all-snapshots all-session-snapshots
     :snapshot-expired? snapshot-expired?
     :validate-url! validate-url!
     :resolve-url! resolve-url!})))

;; ---------------------------------------------------------------------------
;; Public API — exposed to SCI sandbox
;; ---------------------------------------------------------------------------

(defn open-session
  "Open a new browser session and navigate to a URL.

   Returns the page content along with a session-id for subsequent calls.

   Options:
     :js — enable JavaScript (default true)
     :backend — browser backend keyword/string (default :auto)"
  [url & {:keys [js backend] :or {js true}}]
  (let [requested-backend (normalize-backend-id backend)
        selected-backend (resolve-open-backend-id backend)
        open-with (fn [backend-id]
                    (browser.backend/open-session* (backend-by-id backend-id)
                                                   url
                                                   {:js js}))]
    (if (and (= auto-backend-id requested-backend)
             (not= htmlunit-backend-id selected-backend))
      (try
        (open-with selected-backend)
        (catch Exception e
          (log/warn e "Browser backend failed during auto-open, falling back to HtmlUnit"
                    {:requested selected-backend
                     :fallback htmlunit-backend-id
                     :url url})
          (open-with htmlunit-backend-id)))
      (open-with selected-backend))))

(defn navigate
  "Navigate to a new URL in an existing session.
   Returns the page content."
  [session-id url]
  (browser.backend/navigate* (backend-by-id (session-backend-id session-id))
                             session-id
                             url))

(defn click
  "Click an element matching a CSS selector.
   Returns the resulting page content."
  [session-id selector]
  (browser.backend/click* (backend-by-id (session-backend-id session-id))
                          session-id
                          selector))

(defn fill-form
  "Fill form fields and optionally submit.

   Arguments:
     session-id — browser session
     fields     — map of field name/id to value
     opts:
       :form-selector — CSS selector for the form (default: first form)
       :submit        — true to submit after filling (default false)

   Returns the page content (after submit if :submit true)."
  [session-id fields & {:keys [form-selector submit]
                        :or {submit false}}]
  (browser.backend/fill-form* (backend-by-id (session-backend-id session-id))
                              session-id
                              fields
                              {:form-selector form-selector
                               :submit submit}))

(defn read-page
  "Read the current page content in a session.
   Useful after waiting for JS to render."
  [session-id]
  (browser.backend/read-page* (backend-by-id (session-backend-id session-id))
                              session-id))

(defn wait-for-page
  "Wait for JS/rendering in a browser session, optionally until a selector,
   text snippet, or URL substring appears. Returns the current page along with
   :matched and :timed_out flags."
  [session-id & {:keys [timeout-ms interval-ms selector text url-contains]
                 :or {timeout-ms 10000
                      interval-ms 500}}]
  (browser.backend/wait-for-page* (backend-by-id (session-backend-id session-id))
                                  session-id
                                  {:timeout-ms timeout-ms
                                   :interval-ms interval-ms
                                   :selector selector
                                   :text text
                                   :url-contains url-contains}))

(defn close-session
  "Close a browser session and free resources."
  [session-id]
  (browser.backend/close-session* (backend-by-id (session-backend-id session-id))
                                  session-id))

(defn close-all-sessions!
  "Close all browser sessions and remove any saved resume snapshots.
   Useful for test cleanup."
  []
  (doseq [backend (vals @registered-backends)]
    (browser.backend/close-all-sessions!* backend)))

(defn list-sessions
  "List browser sessions, including resumable sessions restored from snapshots."
  []
  (vec (mapcat browser.backend/list-sessions* (vals @registered-backends))))

(defn browser-runtime-status
  "Return browser backend readiness information."
  []
  {:configured-default-backend (configured-default-backend-id)
   :selected-auto-backend (auto-backend-id*)
   :backends (->> (vals @registered-backends)
                  (map browser.backend/runtime-status*)
                  (sort-by :backend)
                  vec)})

(defn bootstrap-browser-runtime!
  "Prepare one backend runtime, or all backends when backend is :auto."
  [& {:keys [backend] :or {backend auto-backend-id}}]
  (let [backend-id (normalize-backend-id backend)]
    (if (= auto-backend-id backend-id)
      {:requested-backend auto-backend-id
       :results (->> (vals @registered-backends)
                     (map #(browser.backend/bootstrap-runtime!* % {}))
                     (sort-by :backend)
                     vec)}
      (browser.backend/bootstrap-runtime!* (backend-by-id backend-id) {}))))

(defn install-browser-deps!
  "Preview or install browser system dependencies for a backend.

   This currently matters only for the Playwright backend on Linux.
   Defaults to :playwright and uses a dry-run preview unless :dry-run false."
  [& {:keys [backend dry-run]
      :or {backend :playwright
           dry-run true}}]
  (browser.backend/install-browser-deps!* (backend-by-id (normalize-backend-id backend))
                                          {:dry-run dry-run}))

;; ---------------------------------------------------------------------------
;; Login helpers — credential injection without LLM exposure
;; ---------------------------------------------------------------------------

(defn login
  "Log into a site using stored credentials.

   The LLM calls this with a site keyword — it never sees the actual
   username or password. Credentials are loaded from the DB (system-level
   access, bypassing SCI secret filters).

  Arguments:
     site-id — keyword id of a registered site credential

  Returns:
     The logged-in page content with session-id for further browsing."
  [site-id & {:keys [backend]}]
  (let [cred (db/get-site-cred site-id)]
    (when-not cred
      (throw (ex-info (str "No site credentials registered for: " (name site-id)
                           ". Register with xia.db/register-site-cred!")
                      {:site-id site-id})))
    (when (autonomous/autonomous-run?)
      (cond
        (not (autonomous/trusted?))
        (do
          (autonomous/audit! {:type    "site-login"
                              :site-id (name site-id)
                              :status  "blocked"
                              :error   "trusted autonomous execution is required for site login"})
          (throw (ex-info "site login requires trusted autonomous execution"
                          {:site-id site-id})))

        (not (autonomous/site-autonomous-approved? cred))
        (do
          (autonomous/audit! {:type    "site-login"
                              :site-id (name site-id)
                              :status  "blocked"
                              :error   "site account is not approved for autonomous execution"})
          (throw (ex-info (str "Site account " (name site-id)
                               " is not approved for autonomous execution")
                          {:site-id site-id})))))
    (let [login-url (:site-cred/login-url cred)
          username-field (:site-cred/username-field cred)
          password-field (:site-cred/password-field cred)
          username (:site-cred/username cred)
          password (:site-cred/password cred)
          form-selector (:site-cred/form-selector cred)
          extra-fields (when-let [ef (:site-cred/extra-fields cred)]
                         (try (json/read-json ef) (catch Exception _ {})))
          ;; Open a session and navigate to login page
          result (open-session login-url :backend backend)
          session-id (:session-id result)
          field-values (cond-> {username-field username
                                password-field password}
                         (map? extra-fields) (merge extra-fields))]
      (try
        (let [login-result (fill-form session-id
                                      field-values
                                      :form-selector form-selector
                                      :submit true)]
          (autonomous/audit! {:type       "site-login"
                              :site-id    (name site-id)
                              :session-id session-id
                              :status     "success"})
          login-result)
        (catch Exception e
          (autonomous/audit! {:type       "site-login"
                              :site-id    (name site-id)
                              :session-id session-id
                              :status     "error"
                              :error      (.getMessage e)})
          (throw e))))))

(defn login-interactive
  "Log into a site by prompting the user for credentials.

   Credentials are entered directly in the terminal (password masked),
   never stored in DB, never in LLM context or message history.

   Arguments:
     url    — the login page URL
     fields — vector of field descriptors, each a map:
                {:name \"email\" :label \"Email\" :mask? false}
                {:name \"password\" :label \"Password\" :mask? true}

  Returns:
     The logged-in page content with session-id."
  [url fields & {:keys [backend]}]
  (when (autonomous/autonomous-run?)
    (autonomous/audit! {:type   "site-login-interactive"
                        :url    url
                        :status "blocked"
                        :error  "interactive login is unavailable during autonomous execution"})
    (throw (ex-info "interactive login is unavailable during autonomous execution" {:url url})))
  (when-not (prompt/prompt-available?)
    (throw (ex-info "Interactive login requires a terminal session" {})))
  (println)
  (println (str "  \uD83D\uDD12 Site login: " url))
  ;; Collect credentials from user
  (let [field-values (reduce
                      (fn [m {:strs [name label mask?] :as field}]
                        (let [lbl (or label name)
                              mask (boolean mask?)
                              value (prompt/prompt! lbl :mask? mask)]
                          (assoc m name value)))
                      {}
                      fields)
        ;; Open browser and navigate
        result (open-session url :backend backend)
        session-id (:session-id result)
        page-result (fill-form session-id field-values :submit true)]
    (println "  login submitted.")
    (println)
    page-result))

(defn list-sites
  "List registered site credentials. Returns names and URLs — never credentials."
  []
  (->> (db/list-site-creds)
       (filter (fn [cred]
                 (or (not (autonomous/autonomous-run?))
                     (autonomous/site-autonomous-approved? cred))))
       (mapv (fn [cred]
               {:id (:site-cred/id cred)
                :name (:site-cred/name cred)
                :login-url (:site-cred/login-url cred)
                :autonomous-approved? (autonomous/site-autonomous-approved? cred)}))))
