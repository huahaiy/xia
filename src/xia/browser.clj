(ns xia.browser
  "Headless browser for interactive web browsing.

   Provides stateful browser sessions behind a pluggable backend seam.
   Xia currently supports Playwright for interactive browser automation. Tools can open a
   session, navigate pages, click elements, fill forms, and submit — all
   within the SCI sandbox. Each action returns the resulting page content
   so the LLM minimizes round-trips.

   Live sessions auto-close after an idle timeout, but their cookies and
   current URL are snapshotted into Xia's DB so multi-step browser tasks
   can resume later."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [datalevin.core :as d]
            [taoensso.timbre :as log]
            [xia.autonomous :as autonomous]
            [xia.browser.backend :as browser.backend]
            [xia.browser.playwright :as playwright]
            [xia.config :as cfg]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.ssrf :as ssrf])
  (:import [datalevin.db DB]
           [datalevin.storage Store]))

;; ---------------------------------------------------------------------------
;; Session management
;; ---------------------------------------------------------------------------

(def ^:private max-sessions 5)
(def ^:private resumable-session-ttl-ms (* 7 24 60 60 1000)) ; 7 days
(def ^:private browser-session-dbi "xia/browser-sessions")
(def ^:private legacy-htmlunit-backend-id :htmlunit)
(def ^:private auto-backend-id :auto)
(def ^:private supported-backend-ids [:playwright])

(defn- now-ms []
  (System/currentTimeMillis))

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

(defn- snapshot-expired?
  [snapshot]
  (> (- (long (now-ms))
        (long (or (get snapshot "last_access_ms")
                  (get snapshot "updated_at_ms")
                  0)))
     (long resumable-session-ttl-ms)))

(defn- snapshot-backend-id
  [snapshot]
  (some-> (or (get snapshot "backend")
              (name legacy-htmlunit-backend-id))
          keyword))

;; ---------------------------------------------------------------------------
;; SSRF protection
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

;; ---------------------------------------------------------------------------
;; Backend registry
;; ---------------------------------------------------------------------------

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
                      (conj (set supported-backend-ids) auto-backend-id)))

(defn- auto-backend-id*
  []
  :playwright)

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
  (let [backend-id (some-> (read-session-snapshot session-id) snapshot-backend-id)]
    (cond
      (= legacy-htmlunit-backend-id backend-id)
      (throw (ex-info "Browser session belongs to the removed HtmlUnit backend. Close it and open a new Playwright session."
                      {:session-id session-id
                       :backend backend-id}))

      (or (nil? backend-id)
          (contains? (set supported-backend-ids) backend-id))
      (or backend-id :playwright)

      :else
      (throw (ex-info "Unsupported browser backend in session snapshot"
                      {:session-id session-id
                       :backend backend-id})))))

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
     :backend — browser backend keyword/string (default :auto)
     :storage-state — optional backend-specific serialized storage state
     :headless — optional backend-specific headless override
     :channel — optional backend-specific browser channel (for Playwright,
                values like \"chrome\" or \"msedge\")"
  [url & {:keys [js backend storage-state headless channel] :or {js true}}]
  (browser.backend/open-session* (backend-by-id (resolve-open-backend-id backend))
                                 url
                                 {:js js
                                  :storage-state storage-state
                                  :headless headless
                                  :channel channel}))

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

(defn fill-selector
  "Fill a visible field-like element matching a CSS selector."
  [session-id selector value]
  (browser.backend/fill-selector* (backend-by-id (session-backend-id session-id))
                                  session-id
                                  selector
                                  value
                                  {}))

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

(defn query-elements
  "Inspect the current page in a browser session and return paginated elements.

   Options:
     :kind          — query preset such as :interactive, :links, :buttons, :forms, or :fields
     :selector      — optional CSS selector override
     :text-contains — optional case-insensitive text/attribute substring filter
     :visible-only  — when true, only return visible elements
     :offset        — zero-based pagination offset (default 0)
     :limit         — page size (default 25, max 200)"
  [session-id & {:keys [kind selector text-contains visible-only offset limit]}]
  (browser.backend/query-elements* (backend-by-id (session-backend-id session-id))
                                   session-id
                                   {:kind kind
                                    :selector selector
                                    :text-contains text-contains
                                    :visible-only visible-only
                                    :offset offset
                                    :limit limit}))

(defn screenshot
  "Capture a screenshot in a browser session.

   Options:
     :full-page — capture the full scrollable page instead of the viewport
     :detail    — optional image detail hint for downstream vision models

   Returns screenshot metadata and a data URL for downstream multimodal use."
  [session-id & {:keys [full-page detail]
                 :or {full-page false
                      detail "auto"}}]
  (browser.backend/screenshot* (backend-by-id (session-backend-id session-id))
                               session-id
                               {:full-page full-page
                                :detail detail}))

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
  (let [snapshot-backend (some-> (read-session-snapshot session-id) snapshot-backend-id)]
    (if (= legacy-htmlunit-backend-id snapshot-backend)
      (do
        (delete-session-snapshot! session-id)
        {:status "closed" :session-id session-id})
      (browser.backend/close-session* (backend-by-id (session-backend-id session-id))
                                      session-id))))

(defn close-all-sessions!
  "Close all browser sessions and remove any saved resume snapshots.
   Useful for test cleanup."
  []
  (doseq [backend (vals @registered-backends)]
    (browser.backend/close-all-sessions!* backend))
  (doseq [[session-id snapshot] (all-session-snapshots)]
    (when (= legacy-htmlunit-backend-id (snapshot-backend-id snapshot))
      (delete-session-snapshot! session-id))))

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

(defn clone-session
  "Open a new browser session using the serialized storage state from an
   existing resumable session. Useful for provider-backed account connectors
   that need a fresh browser context without re-running login."
  [session-id & {:keys [url js]}]
  (let [snapshot (or (read-session-snapshot session-id)
                     (throw (ex-info "No resumable browser session snapshot found"
                                     {:session-id session-id})))
        target-url (or url
                       (get snapshot "current_url")
                       (throw (ex-info "Browser session snapshot has no current URL"
                                       {:session-id session-id})))
        js-enabled (if (nil? js)
                     (if (contains? snapshot "js_enabled")
                       (boolean (get snapshot "js_enabled"))
                       true)
                     js)]
    (browser.backend/open-session* (backend-by-id (session-backend-id session-id))
                                   target-url
                                   {:js js-enabled
                                    :storage-state (get snapshot "browser_state")})))

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
  (let [field-values (reduce
                      (fn [m {:strs [name label mask?]}]
                        (let [lbl (or label name)
                              value (prompt/prompt! lbl :mask? (boolean mask?))]
                          (assoc m name value)))
                      {}
                      fields)
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
