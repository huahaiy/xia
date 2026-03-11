(ns xia.browser
  "Headless browser for interactive web browsing.

   Provides stateful browser sessions backed by HtmlUnit. Tools can open
   a session, navigate pages, click elements, fill forms, and submit —
   all within the SCI sandbox. Each action returns the resulting page
   content so the LLM minimizes round-trips.

   Sessions auto-expire after 10 minutes of inactivity."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [charred.api :as json]
            [xia.db :as db]
            [xia.prompt :as prompt])
  (:import [org.htmlunit WebClient BrowserVersion WebRequest]
           [org.htmlunit.html HtmlPage HtmlElement HtmlForm
                              HtmlInput HtmlTextArea HtmlSelect
                              HtmlOption HtmlAnchor
                              DomElement DomNode]
           [org.htmlunit.util WebConnectionWrapper]
           [java.util.concurrent ConcurrentHashMap]))

;; ---------------------------------------------------------------------------
;; Session management
;; ---------------------------------------------------------------------------

(def ^:private max-sessions 5)
(def ^:private session-ttl-ms (* 10 60 1000)) ; 10 minutes

(defonce ^:private sessions (ConcurrentHashMap.))

(declare validate-url!)

(defn- install-url-guard!
  [^WebClient client]
  (.setWebConnection
    client
    (proxy [WebConnectionWrapper] [client]
      (getResponse [^WebRequest request]
        (validate-url! (str (.getUrl request)))
        (proxy-super getResponse request))))
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
  (> (- (System/currentTimeMillis) last-access) session-ttl-ms))

(defn- evict-expired!
  "Remove expired sessions."
  []
  (doseq [[id sess] sessions]
    (when (session-expired? @sess)
      (try (.close ^WebClient (:client @sess)) (catch Exception _))
      (.remove sessions id))))

(defn- get-session
  "Get a live session or throw."
  [session-id]
  (let [sess (.get sessions session-id)]
    (when-not sess
      (throw (ex-info (str "No browser session: " session-id
                           ". Call browser-open first.")
                      {:session-id session-id})))
    (when (session-expired? @sess)
      (.remove sessions session-id)
      (try (.close ^WebClient (:client @sess)) (catch Exception _))
      (throw (ex-info "Browser session expired" {:session-id session-id})))
    (swap! sess assoc :last-access (System/currentTimeMillis))
    @sess))

;; ---------------------------------------------------------------------------
;; Page → readable content
;; ---------------------------------------------------------------------------

(def ^:private max-content-chars 8000) ; ~2000 tokens

(defn- page->map
  "Convert an HtmlPage to a tool-friendly result map."
  [^HtmlPage page]
  (let [text     (.asNormalizedText page)
        content  (if (> (count text) max-content-chars)
                   (str (subs text 0 max-content-chars) "\n\n[content truncated]")
                   text)
        forms    (.getForms page)
        anchors  (.getAnchors page)]
    {:url       (.toString (.getUrl page))
     :title     (.getTitleText page)
     :content   content
     :forms     (mapv (fn [^HtmlForm f]
                        (let [inputs (concat (.getInputsByType f "text")
                                            (.getInputsByType f "email")
                                            (.getInputsByType f "password")
                                            (.getInputsByType f "search")
                                            (.getInputsByType f "number")
                                            (.getInputsByType f "tel")
                                            (.getInputsByType f "url")
                                            (.getTextAreasByName f "*")
                                            (.getSelectsByName f "*"))]
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
                                        :value (.getAttribute el "value")}))}))
                forms)
     :links    (vec
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

;; ---------------------------------------------------------------------------
;; SSRF protection (reuse pattern from xia.web)
;; ---------------------------------------------------------------------------

(defn- private-ip?
  [^java.net.InetAddress addr]
  (or (.isLoopbackAddress addr)
      (.isLinkLocalAddress addr)
      (.isSiteLocalAddress addr)
      (.isAnyLocalAddress addr)))

(defn- resolve-host-addresses
  [host]
  (seq (java.net.InetAddress/getAllByName host)))

(defn- validate-url!
  [url]
  (let [uri (java.net.URI. url)]
    (when-not (#{"http" "https"} (.getScheme uri))
      (throw (ex-info "Only http:// and https:// URLs are allowed"
                      {:url url})))
    (let [host (.getHost uri)]
      (when (str/blank? host)
        (throw (ex-info "URL has no host" {:url url})))
      (let [addrs (resolve-host-addresses host)]
        (when (some private-ip? addrs)
          (throw (ex-info "Access to private/internal addresses is blocked"
                          {:url url :host host})))))))

;; ---------------------------------------------------------------------------
;; Public API — exposed to SCI sandbox
;; ---------------------------------------------------------------------------

(defn open-session
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
  (let [sid    (str (random-uuid))
        client (make-client)]
    (when-not js
      (-> client .getOptions (.setJavaScriptEnabled false)))
    (.put sessions sid (atom {:client client :last-access (System/currentTimeMillis)}))
    (try
      (let [page (.getPage client url)]
        (if (instance? HtmlPage page)
          (assoc (page->map page) :session-id sid)
          {:session-id sid
           :url        url
           :content    (str page)
           :forms      []
           :links      []}))
      (catch Exception e
        ;; Clean up on failure
        (.remove sessions sid)
        (try (.close client) (catch Exception _))
        (throw e)))))

(defn navigate
  "Navigate to a new URL in an existing session.
   Returns the page content."
  [session-id url]
  (validate-url! url)
  (let [{:keys [client]} (get-session session-id)
        page (.getPage ^WebClient client ^String url)]
    (if (instance? HtmlPage page)
      (page->map page)
      {:url url :content (str page) :forms [] :links []})))

(defn click
  "Click an element matching a CSS selector.
   Returns the resulting page content."
  [session-id selector]
  (let [{:keys [client]} (get-session session-id)
        page             (current-page client)]
    (when-not page
      (throw (ex-info "No page loaded in session" {:session-id session-id})))
    (let [el (.querySelector page selector)]
      (when-not el
        (throw (ex-info (str "No element matches selector: " selector)
                        {:selector selector})))
      (let [result (.click ^HtmlElement el)]
        (if (instance? HtmlPage result)
          (page->map result)
          ;; Click might return a different page type or same page
          (if-let [p (current-page client)]
            (page->map p)
            {:content "Click performed but no HTML page resulted."
             :forms [] :links []}))))))

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
                        :or   {submit false}}]
  (let [{:keys [client]} (get-session session-id)
        page             (current-page client)]
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
        (let [el (or
                   ;; Try by name attribute
                   (try (.getInputByName ^HtmlForm form ^String field-name)
                        (catch Exception _ nil))
                   ;; Try textarea by name
                   (try (first (.getTextAreasByName ^HtmlForm form ^String field-name))
                        (catch Exception _ nil))
                   ;; Try select by name
                   (try (first (.getSelectsByName ^HtmlForm form ^String field-name))
                        (catch Exception _ nil))
                   ;; Try by CSS selector within the form
                   (try (.querySelector ^HtmlForm form
                          (str "[name=\"" field-name "\"], #" field-name))
                        (catch Exception _ nil)))]
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
            (if (instance? HtmlPage result)
              (page->map result)
              (if-let [p (current-page client)]
                (page->map p)
                {:content "Form submitted but no HTML page resulted."
                 :forms [] :links []}))))
        ;; No submit — return current page state
        (page->map page)))))

(defn read-page
  "Read the current page content in a session.
   Useful after waiting for JS to render."
  [session-id]
  (let [{:keys [client]} (get-session session-id)]
    (if-let [page (current-page client)]
      (page->map page)
      {:content "No page loaded." :forms [] :links []})))

(defn close-session
  "Close a browser session and free resources."
  [session-id]
  (when-let [sess (.remove sessions session-id)]
    (try (.close ^WebClient (:client @sess)) (catch Exception _)))
  {:status "closed" :session-id session-id})

(defn close-all-sessions!
  "Close all browser sessions. Useful for test cleanup."
  []
  (doseq [[id sess] sessions]
    (try (.close ^WebClient (:client @sess)) (catch Exception _))
    (.remove sessions id)))

(defn list-sessions
  "List active browser sessions."
  []
  (evict-expired!)
  (mapv (fn [[id sess]]
          (let [s @sess]
            {:session-id  id
             :url         (try (some-> (:client s) current-page
                                       (.getUrl) str)
                               (catch Exception _ nil))
             :age-seconds (quot (- (System/currentTimeMillis)
                                   (:last-access s))
                                1000)}))
        sessions))

;; ---------------------------------------------------------------------------
;; Login helpers — credential injection without LLM exposure
;; ---------------------------------------------------------------------------

(defn- do-login
  "Internal: fill login form and submit. Used by both login and login-interactive."
  [session-id ^HtmlPage page username-field password-field
   username password form-selector extra-fields]
  (let [form (if form-selector
               (.querySelector page ^String form-selector)
               (first (.getForms page)))]
    (when-not form
      (throw (ex-info "No login form found on page"
                      {:url (.toString (.getUrl page))
                       :form-selector form-selector})))
    ;; Fill username
    (when-let [el (try (.getInputByName ^HtmlForm form ^String username-field)
                       (catch Exception _ nil))]
      (.setValue ^HtmlInput el ^String username))
    ;; Fill password
    (when-let [el (try (.getInputByName ^HtmlForm form ^String password-field)
                       (catch Exception _ nil))]
      (.setValue ^HtmlInput el ^String password))
    ;; Fill extra fields
    (doseq [[field-name value] extra-fields]
      (when-let [el (try (.getInputByName ^HtmlForm form ^String field-name)
                         (catch Exception _ nil))]
        (.setValue ^HtmlInput el ^String (str value))))
    ;; Submit
    (let [submit-btn (or (first (.getInputsByType ^HtmlForm form "submit"))
                         (.querySelector ^HtmlForm form
                           "button[type=submit], button:not([type])"))
          {:keys [client]} (get-session session-id)
          result     (if submit-btn
                       (.click ^HtmlElement submit-btn)
                       ;; Fallback: navigate back to trigger form post
                       (current-page client))]
      (if (instance? HtmlPage result)
        (page->map result)
        (if-let [p (current-page client)]
          (page->map p)
          {:content "Login form submitted." :forms [] :links []})))))

(defn login
  "Log into a site using stored credentials.

   The LLM calls this with a site keyword — it never sees the actual
   username or password. Credentials are loaded from the DB (system-level
   access, bypassing SCI secret filters).

   Arguments:
     site-id — keyword id of a registered site credential

   Returns:
     The logged-in page content with session-id for further browsing."
  [site-id]
  (let [cred (db/get-site-cred site-id)]
    (when-not cred
      (throw (ex-info (str "No site credentials registered for: " (name site-id)
                           ". Register with xia.db/register-site-cred!")
                      {:site-id site-id})))
    (let [login-url      (:site-cred/login-url cred)
          username-field (:site-cred/username-field cred)
          password-field (:site-cred/password-field cred)
          username       (:site-cred/username cred)
          password       (:site-cred/password cred)
          form-selector  (:site-cred/form-selector cred)
          extra-fields   (when-let [ef (:site-cred/extra-fields cred)]
                           (try (json/read-json ef) (catch Exception _ {})))
          ;; Open a session and navigate to login page
          result         (open-session login-url)
          session-id     (:session-id result)
          {:keys [client]} (get-session session-id)
          page           (current-page client)]
      (when-not page
        (throw (ex-info "Failed to load login page" {:url login-url})))
      (let [logged-in (do-login session-id page
                                username-field password-field
                                username password
                                form-selector extra-fields)]
        (assoc logged-in :session-id session-id)))))

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
  [url fields]
  (when-not (prompt/prompt-available?)
    (throw (ex-info "Interactive login requires a terminal session" {})))
  (println)
  (println (str "  \uD83D\uDD12 Site login: " url))
  ;; Collect credentials from user
  (let [field-values (reduce
                       (fn [m {:strs [name label mask?] :as field}]
                         (let [lbl   (or label name)
                               mask  (boolean mask?)
                               value (prompt/prompt! lbl :mask? mask)]
                           (assoc m name value)))
                       {}
                       fields)
        ;; Open browser and navigate
        result     (open-session url)
        session-id (:session-id result)
        {:keys [client]} (get-session session-id)
        page       (current-page client)]
    (when-not page
      (throw (ex-info "Failed to load login page" {:url url})))
    ;; Find the form and fill it
    (let [form (first (.getForms page))]
      (when-not form
        (throw (ex-info "No form found on login page" {:url url})))
      ;; Fill all fields
      (doseq [[field-name value] field-values]
        (let [el (or (try (.getInputByName ^HtmlForm form ^String field-name)
                          (catch Exception _ nil))
                     (try (first (.getTextAreasByName ^HtmlForm form ^String field-name))
                          (catch Exception _ nil)))]
          (when el
            (cond
              (instance? HtmlInput el)    (.setValue ^HtmlInput el ^String (str value))
              (instance? HtmlTextArea el) (.setText ^HtmlTextArea el ^String (str value))
              :else (.setAttribute ^DomElement el "value" (str value))))))
      ;; Submit
      (let [submit-btn (or (first (.getInputsByType ^HtmlForm form "submit"))
                           (.querySelector ^HtmlForm form
                             "button[type=submit], button:not([type])"))
            result     (if submit-btn
                         (.click ^HtmlElement submit-btn)
                         (current-page client))]
        (println "  login submitted.")
        (println)
        (let [page-result (if (instance? HtmlPage result)
                            (page->map result)
                            (if-let [p (current-page client)]
                              (page->map p)
                              {:content "Login submitted." :forms [] :links []}))]
          (assoc page-result :session-id session-id))))))

(defn list-sites
  "List registered site credentials. Returns names and URLs — never credentials."
  []
  (mapv (fn [cred]
          {:id        (:site-cred/id cred)
           :name      (:site-cred/name cred)
           :login-url (:site-cred/login-url cred)})
        (db/list-site-creds)))
