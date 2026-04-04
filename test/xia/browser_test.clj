(ns xia.browser-test
  (:require [charred.api :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [org.httpkit.server :as http-server]
            [taoensso.timbre :as timbre]
            [xia.browser :as browser]
            [xia.browser.backend :as browser.backend]
            [xia.browser.playwright]
            [xia.browser.remote]
            [xia.db :as db]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.net InetAddress ServerSocket URI]
           [java.nio.file FileSystemAlreadyExistsException FileSystems]
           [java.util.concurrent ConcurrentHashMap]))

(use-fixtures :each
  with-test-db
  (fn [f]
    (browser/close-all-sessions!)
    (db/set-config! :browser/playwright-enabled? "true")
    (db/set-config! :browser/backend-default "auto")
    (f)
    (browser/close-all-sessions!)))

(defn- close-playwright-live-session!
  [session-id]
  (let [close-live-session! (var-get (ns-resolve 'xia.browser.playwright
                                                 'close-live-session!))]
    (close-live-session! session-id)))

(defn- playwright-var
  [sym]
  (ns-resolve 'xia.browser.playwright sym))

(defn- browser-var
  [sym]
  (ns-resolve 'xia.browser sym))

(defn- call-playwright-private
  [sym & args]
  (apply (var-get (playwright-var sym)) args))

(defn- with-playwright-runtime-state
  [state f]
  (let [runtime* (var-get (playwright-var 'runtime))
        original @runtime*]
    (reset! runtime* state)
    (try
      (f)
      (finally
        (reset! runtime* original)))))

(defn- empty-playwright-runtime-state
  []
  {:playwright nil
   :browser nil
   :bootstrapped? false
   :browser-installed? false
   :browser-executable nil
   :driver-resource-fs nil
   :driver-resource-fs-owned? false})

(defn- ephemeral-port
  []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

(defn- spa-page-html
  []
  (str "<!doctype html>"
       "<html><head><meta charset='utf-8'><title>SPA Fixture</title></head>"
       "<body>"
       "<main>"
       "<h1>SPA Fixture</h1>"
       "<div id='status'>Booting</div>"
       "<div id='summary'>Loading items...</div>"
       "<ul id='items'></ul>"
       "<button id='add-item' type='button'>Add Item</button>"
       "<form id='search-form'>"
       "<label for='query'>Query</label>"
       "<input id='query' name='query' type='text' value='' />"
       "<button id='submit-search' type='submit'>Search</button>"
       "</form>"
       "<div id='result'>No search yet</div>"
       "</main>"
       "<script>"
       "const state = { items: [], query: '' };"
       "function render() {"
       "  document.getElementById('summary').textContent = `Items: ${state.items.length}`;"
       "  document.getElementById('items').innerHTML = state.items.map((item) => `<li>${item.name}</li>`).join('');"
       "  document.getElementById('result').textContent = state.query ? `Result for ${state.query}` : 'No search yet';"
       "}"
       "async function boot() {"
       "  const response = await fetch('/api/items');"
       "  state.items = await response.json();"
       "  document.getElementById('status').textContent = 'Ready';"
       "  render();"
       "}"
       "document.getElementById('add-item').addEventListener('click', () => {"
       "  window.setTimeout(() => {"
       "    state.items = [...state.items, { name: `Extra ${state.items.length + 1}` }];"
       "    render();"
       "  }, 50);"
       "});"
       "document.getElementById('search-form').addEventListener('submit', (event) => {"
       "  event.preventDefault();"
       "  const value = document.getElementById('query').value;"
       "  window.setTimeout(() => {"
       "    state.query = value;"
       "    render();"
       "  }, 50);"
       "});"
       "boot();"
       "</script>"
       "</body></html>"))

(defn- dense-dashboard-html
  []
  (str "<!doctype html>"
       "<html><head><meta charset='utf-8'><title>Dense Dashboard</title></head>"
       "<body>"
       "<main>"
       "<h1>Dense Dashboard</h1>"
       "<nav>"
       (apply str
              (for [i (range 1 41)]
                (str "<a class='dash-link' href='/item/" i "'>Item " i "</a>")))
       "</nav>"
       "<button id='run-report' type='button'>Run report</button>"
       "<button id='hidden-action' type='button' style='display:none'>Hidden action</button>"
       "<section>"
       "<form id='filter-form'>"
       "<label for='q'>Filter</label>"
       "<input id='q' name='q' type='search' value='' />"
       "</form>"
       "</section>"
       "</main>"
       "</body></html>"))

(defn- id-shadow-login-html
  []
  (str "<!doctype html>"
       "<html><head><meta charset='utf-8'><title>Shadowed Login</title></head>"
       "<body>"
       "<main>"
       "<form method='post' action='/login'>"
       "<input type='hidden' name='sectok' value='abc123' />"
       "<input type='hidden' name='id' value='start' />"
       "<input type='hidden' name='do' value='login' />"
       "<label>Username <input name='u' type='text' /></label>"
       "<label>Password <input name='p' type='password' /></label>"
       "<button type='submit'>Log In</button>"
       "</form>"
       "</main>"
       "</body></html>"))

(defn- with-spa-server
  [f]
  (let [port (ephemeral-port)
        server (http-server/run-server
                 (fn [{:keys [uri]}]
                   (case uri
                     "/"
                     {:status 200
                      :headers {"content-type" "text/html; charset=utf-8"}
                      :body (spa-page-html)}

                     "/api/items"
                     {:status 200
                      :headers {"content-type" "application/json"}
                      :body "[{\"name\":\"Alpha\"},{\"name\":\"Beta\"}]"}

                     "/favicon.ico"
                     {:status 404
                      :headers {"content-type" "text/plain"}
                      :body ""}

                     {:status 404
                      :headers {"content-type" "text/plain"}
                      :body "not found"}))
                 {:ip "127.0.0.1"
                  :port port})
        base-url (str "http://127.0.0.1:" port)]
    (try
      (f base-url)
      (finally
        (server)))))

(defn- with-dense-dashboard-server
  [f]
  (let [port (ephemeral-port)
        server (http-server/run-server
                 (fn [{:keys [uri]}]
                   (cond
                     (= "/" uri)
                     {:status 200
                      :headers {"content-type" "text/html; charset=utf-8"}
                      :body (dense-dashboard-html)}

                     (re-matches #"/item/\d+" uri)
                     {:status 200
                      :headers {"content-type" "text/html; charset=utf-8"}
                      :body (str "<html><body><h1>" uri "</h1></body></html>")}

                     (= "/favicon.ico" uri)
                     {:status 404
                      :headers {"content-type" "text/plain"}
                      :body ""}

                     :else
                     {:status 404
                      :headers {"content-type" "text/plain"}
                      :body "not found"}))
                 {:ip "127.0.0.1"
                  :port port})
        base-url (str "http://127.0.0.1:" port)]
    (try
      (f base-url)
      (finally
        (server)))))

(defn- with-id-shadow-login-server
  [f]
  (let [port (ephemeral-port)
        server (http-server/run-server
                 (fn [{:keys [uri]}]
                   (case uri
                     "/"
                     {:status 200
                      :headers {"content-type" "text/html; charset=utf-8"}
                      :body (id-shadow-login-html)}

                     "/favicon.ico"
                     {:status 404
                      :headers {"content-type" "text/plain"}
                      :body ""}

                     {:status 200
                      :headers {"content-type" "text/html; charset=utf-8"}
                      :body (id-shadow-login-html)}))
                 {:ip "127.0.0.1"
                  :port port})
        base-url (str "http://127.0.0.1:" port)]
    (try
      (f base-url)
      (finally
        (server)))))

(defn- test-playwright-backend
  []
  (let [snapshots (atom {})]
    {:backend
     ((ns-resolve 'xia.browser.playwright 'create-backend)
      {:max-sessions 5
       :now-ms #(System/currentTimeMillis)
       :read-snapshot #(get @snapshots %)
       :write-snapshot! (fn [session-id snapshot]
                          (swap! snapshots assoc session-id snapshot))
       :delete-snapshot! (fn [session-id]
                           (swap! snapshots dissoc session-id))
       :all-snapshots #(seq @snapshots)
       :snapshot-expired? (constantly false)
       :validate-url! (fn [_] nil)
       :resolve-url! (fn [_] nil)})
     :snapshots snapshots}))

(defn- test-remote-backend
  []
  (let [snapshots (atom {})]
    {:backend
     ((ns-resolve 'xia.browser.remote 'create-backend)
      {:read-snapshot #(get @snapshots %)
       :write-snapshot! (fn [session-id snapshot]
                          (swap! snapshots assoc session-id snapshot))
       :delete-snapshot! (fn [session-id]
                           (swap! snapshots dissoc session-id))
       :all-snapshots #(seq @snapshots)
       :snapshot-expired? (constantly false)
       :snapshot-usable? (constantly {:ok? true})
       :validate-url! (fn [_] nil)
       :resolve-url! (fn [_] nil)})
     :snapshots snapshots}))

;; ---------------------------------------------------------------------------
;; SSRF protection
;; ---------------------------------------------------------------------------

(deftest validate-url-blocks-private
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"private/internal"
        (#'browser/validate-url! "http://127.0.0.1/")))
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"private/internal"
        (#'browser/validate-url! "http://localhost/"))))

(deftest validate-url-blocks-mixed-public-and-private-resolution
  (with-redefs-fn {#'browser/resolve-host-addresses
                   (fn [_]
                     [(InetAddress/getByAddress "public.example" (byte-array [(byte 93) (byte -72) (byte 34) (byte 20)]))
                      (InetAddress/getByAddress "private.example" (byte-array [(byte 127) (byte 0) (byte 0) (byte 1)]))])}
    #(is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"private/internal"
           (#'browser/validate-url! "https://mixed.example")))))

(deftest validate-url-blocks-bad-schemes
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"Only http"
        (#'browser/validate-url! "file:///etc/passwd")))
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"Only http"
        (#'browser/validate-url! "ftp://example.com"))))

(deftest validate-url-allows-public
  (is (nil? (#'browser/validate-url! "https://example.com")))
  (is (nil? (#'browser/validate-url! "http://example.com"))))

;; ---------------------------------------------------------------------------
;; Session lifecycle
;; ---------------------------------------------------------------------------

(deftest ^:integration open-and-close-session
  (let [result (browser/open-session "https://example.com")]
    (is (string? (:session-id result)))
    (is (= :playwright (:backend result)))
    (is (= "Example Domain" (:title result)))
    (is (string? (:content result)))
    (is (vector? (:forms result)))
    (is (vector? (:links result)))
    ;; Close
    (let [closed (browser/close-session (:session-id result))]
      (is (= "closed" (:status closed))))))

(deftest close-nonexistent-session-is-safe
  (let [result (browser/close-session "nonexistent")]
    (is (= "closed" (:status result)))))

(deftest get-session-throws-for-unknown
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"No browser session"
        (browser/navigate "bogus-id" "https://example.com"))))

;; ---------------------------------------------------------------------------
;; Navigation
;; ---------------------------------------------------------------------------

(deftest ^:integration navigate-to-new-url
  (let [result (browser/open-session "https://example.com")
        sid    (:session-id result)
        page2  (browser/navigate sid "https://example.com")]
    (is (= "Example Domain" (:title page2)))
    (browser/close-session sid)))

;; ---------------------------------------------------------------------------
;; Click
;; ---------------------------------------------------------------------------

(deftest ^:integration click-a-link
  (let [result (browser/open-session "https://example.com")
        sid    (:session-id result)]
    ;; example.com has one link: "More information..."
    (is (pos? (count (:links result))))
    (let [clicked (browser/click sid "a")]
      (is (string? (:content clicked))))
    (browser/close-session sid)))

(deftest ^:integration click-nonexistent-throws
  (let [result (browser/open-session "https://example.com")
        sid    (:session-id result)]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"No element matches"
          (browser/click sid "#nonexistent-button")))
    (browser/close-session sid)))

;; ---------------------------------------------------------------------------
;; Read page
;; ---------------------------------------------------------------------------

(deftest ^:integration read-page-returns-content
  (let [result (browser/open-session "https://example.com")
        sid    (:session-id result)
        page   (browser/read-page sid)]
    (is (= "Example Domain" (:title page)))
    (is (string? (:content page)))
    (browser/close-session sid)))

(deftest ^:integration wait-for-page-matches-text
  (let [result  (browser/open-session "https://example.com")
        sid     (:session-id result)
        waited  (browser/wait-for-page sid
                                       :text "Example Domain"
                                       :timeout-ms 2000
                                       :interval-ms 100)]
    (is (= sid (:session-id waited)))
    (is (= true (:matched waited)))
    (is (= false (:timed_out waited)))
    (browser/close-session sid)))

;; ---------------------------------------------------------------------------
;; List sessions
;; ---------------------------------------------------------------------------

(deftest ^:integration list-sessions-test
  (let [result (browser/open-session "https://example.com")
        sid    (:session-id result)
        listing (browser/list-sessions)]
    (is (some #(and (= sid (:session-id %))
                    (= :playwright (:backend %)))
              listing))
    (browser/close-session sid)))

(deftest ^:integration list-sessions-includes-resumable-snapshots
  (let [result  (browser/open-session "https://example.com")
        sid     (:session-id result)
        _       (close-playwright-live-session! sid)
        listing (browser/list-sessions)
        entry   (some #(when (= sid (:session-id %)) %) listing)]
    (is (some? entry))
    (is (= false (:live? entry)))
    (is (= true (:resumable? entry)))
    (browser/close-session sid)))

(deftest playwright-snapshot-persistence-serializes-per-session
  (let [persist-session! (var-get (playwright-var 'persist-session!))
        ^ConcurrentHashMap sessions* (var-get (playwright-var 'sessions))
        session-id       (str (random-uuid))
        session-value    (atom {:last-access 1
                                :created-at-ms 1
                                :js-enabled true})
        snapshots        (atom {})
        active           (atom 0)
        max-active       (atom 0)
        ops              {:write-snapshot! (fn [sid snapshot]
                                             (swap! snapshots assoc sid snapshot))}]
    (.put sessions* session-id session-value)
    (try
      (with-redefs-fn {(playwright-var 'live-session->snapshot)
                       (fn [sid _sess]
                         (let [n (swap! active inc)]
                           (swap! max-active max n)
                           (Thread/sleep 40)
                           (swap! active dec)
                           {"session_id" sid
                            "backend" "playwright"
                            "current_url" (str "https://example.com/" n)}))}
        #(let [start   (promise)
               futures (mapv (fn [_]
                               (future
                                 @start
                                 (persist-session! ops session-id)))
                             (range 8))]
           (deliver start true)
           (doseq [fut futures]
             (is (not= ::timeout (deref fut 5000 ::timeout))))
           (is (= 1 @max-active))
           (is (= session-id (-> @snapshots (get session-id) (get "session_id"))))))
      (finally
        (.remove sessions* session-id)))))

(deftest clone-session-rejects-changed-auth-url-without-matching-stored-state
  (let [deleted (atom nil)
        opened  (atom false)
        snapshot {"backend" "playwright"
                  "current_url" "https://auth.old.example/login"
                  "browser_state"
                  (json/write-json-str
                   {"cookies" [{"name" "sid"
                                "domain" "auth.old.example"
                                "path" "/"
                                "expires" (+ (quot (System/currentTimeMillis) 1000) 3600)}]})}
        backend
        (reify browser.backend/BrowserBackend
          (backend-id [_] :playwright)
          (runtime-status* [_] nil)
          (bootstrap-runtime!* [_ _opts] nil)
          (install-browser-deps!* [_ _opts] nil)
          (open-session* [_ _url _opts]
            (reset! opened true)
            {:session-id "unexpected"})
          (navigate* [_ _session-id _url] nil)
          (click* [_ _session-id _selector] nil)
          (fill-selector* [_ _session-id _selector _value _opts] nil)
          (fill-form* [_ _session-id _fields _opts] nil)
          (read-page* [_ _session-id] nil)
          (query-elements* [_ _session-id _opts] nil)
          (screenshot* [_ _session-id _opts] nil)
          (wait-for-page* [_ _session-id _opts] nil)
          (release-session* [_ _session-id] nil)
          (close-session* [_ _session-id] nil)
          (release-all-sessions!* [_] nil)
          (close-all-sessions!* [_] nil)
          (list-sessions* [_] []))]
    (with-redefs [xia.browser/read-session-snapshot (fn [_] snapshot)
                  xia.browser/delete-session-snapshot! (fn [session-id]
                                                         (reset! deleted session-id))
                  xia.browser/session-backend-id (constantly :playwright)
                  xia.browser/backend-by-id (constantly backend)]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"does not contain state for the requested URL"
            (browser/clone-session "browser-session-1"
                                   :url "https://auth.new.example/login")))
      (is (= "browser-session-1" @deleted))
      (is (= false @opened)))))

(deftest playwright-driver-resource-fs-recovers-existing-filesystem
  (let [existing-fs (FileSystems/getDefault)
        resource-uri (URI. "resource:/playwright-driver")]
    (with-playwright-runtime-state (empty-playwright-runtime-state)
      #(with-redefs-fn {(playwright-var 'driver-resource-uri) (constantly resource-uri)
                        (playwright-var 'open-driver-resource-fs)
                        (fn [_]
                          (throw (FileSystemAlreadyExistsException. "exists")))
                        (playwright-var 'existing-driver-resource-fs)
                        (constantly existing-fs)}
         (fn []
           (call-playwright-private 'ensure-driver-resource-fs!)
           (let [runtime* (var-get (playwright-var 'runtime))]
             (is (identical? existing-fs (:driver-resource-fs @runtime*)))
             (is (= false (:driver-resource-fs-owned? @runtime*)))))))))

(deftest playwright-stop-runtime-closes-only-owned-driver-filesystems
  (let [closed (atom [])]
    (with-redefs-fn {(playwright-var 'close-driver-resource-fs!)
                     (fn [driver-fs]
                       (swap! closed conj driver-fs))}
      #(do
         (with-playwright-runtime-state
           (assoc (empty-playwright-runtime-state)
                  :driver-resource-fs ::owned
                  :driver-resource-fs-owned? true)
           (fn []
             (call-playwright-private 'stop-runtime!)))
         (is (= [::owned] @closed))
         (reset! closed [])
         (with-playwright-runtime-state
           (assoc (empty-playwright-runtime-state)
                  :driver-resource-fs ::borrowed
                  :driver-resource-fs-owned? false)
           (fn []
             (call-playwright-private 'stop-runtime!)))
         (is (empty? @closed))))))

(deftest playwright-restore-session-drops-stale-cookie-snapshot
  (let [deleted      (atom nil)
        created?     (atom false)
        validate!    (var-get (browser-var 'storage-state-validation))
        stale-cookie {"name" "sid"
                      "domain" "example.com"
                      "path" "/"
                      "expires" 1}
        snapshot     {"backend" "playwright"
                      "current_url" "https://example.com/account"
                      "browser_state" (json/write-json-str {"cookies" [stale-cookie]})}
        ops          {:read-snapshot (fn [_] snapshot)
                      :delete-snapshot! (fn [session-id]
                                          (reset! deleted session-id))
                      :snapshot-expired? (constantly false)
                      :snapshot-usable? validate!}]
    (with-redefs-fn {(playwright-var 'create-session!)
                     (fn [& _]
                       (reset! created? true)
                       (throw (ex-info "should not create session" {})))}
      #(do
         (is (nil? (call-playwright-private 'restore-session! ops "browser-session-2")))
         (is (= "browser-session-2" @deleted))
         (is (= false @created?))))))

(deftest ^:integration read-page-restores-from-snapshot
  (let [result (browser/open-session "https://example.com")
        sid    (:session-id result)
        _      (close-playwright-live-session! sid)
        page   (browser/read-page sid)]
    (is (= sid (:session-id page)))
    (is (= "Example Domain" (:title page)))
    (browser/close-session sid)))

;; ---------------------------------------------------------------------------
;; Session limit
;; ---------------------------------------------------------------------------

(deftest ^:integration session-limit-evicts-least-recently-used-live-session
  (let [sids         (atom [])
        opened-extra (atom nil)]
    (try
      (dotimes [_ 5]
        (let [r (browser/open-session "https://example.com")]
          (swap! sids conj (:session-id r))))
      (reset! opened-extra (browser/open-session "https://example.com"))
      (is (string? (:session-id @opened-extra)))
      (let [restored (browser/read-page (first @sids))]
        (is (= (first @sids) (:session-id restored)))
        (is (= "Example Domain" (:title restored))))
      (finally
        (doseq [sid (distinct (cond-> @sids
                                @opened-extra
                                (conj (:session-id @opened-extra))))]
          (browser/close-session sid))))))

;; ---------------------------------------------------------------------------
;; Page content structure
;; ---------------------------------------------------------------------------

(deftest ^:integration page-map-structure
  (let [result (browser/open-session "https://example.com")
        sid    (:session-id result)]
    (is (contains? result :backend))
    (is (contains? result :url))
    (is (contains? result :title))
    (is (contains? result :content))
    (is (contains? result :forms))
    (is (contains? result :links))
    (is (contains? result :truncated?))
    (is (contains? result :session-id))
    (browser/close-session sid)))

(deftest playwright-document->map-prefers-main-content-over-nav-sitemap
  (let [nav-links (apply str
                         (for [i (range 1 250)]
                           (str "<a href='/namespace/" i "'>Namespace " i "</a> ")))
        html (str "<!doctype html>"
                  "<html><head><title>Docs Home</title></head><body>"
                  "<nav>" nav-links "</nav>"
                  "<main><article>"
                  "<h1>Cluster Restore Guide</h1>"
                  "<p>Use this guide to restore a cluster from backup.</p>"
                  "<a href='/guides/restore/next'>Next step</a>"
                  "</article></main>"
                  "</body></html>")
        result (call-playwright-private 'document->map
                                        "https://docs.example.com/guides/restore"
                                        nil
                                        html)]
    (is (= "Docs Home" (:title result)))
    (is (re-find #"Cluster Restore Guide" (:content result)))
    (is (re-find #"restore a cluster from backup" (:content result)))
    (is (not (re-find #"Namespace 200" (:content result))))
    (is (= 1 (:link_count result)))
    (is (= [{:text "Next step"
             :url "https://docs.example.com/guides/restore/next"}]
           (:links result)))))

(deftest open-session-uses-configured-default-backend-when-auto
  (db/set-config! :browser/backend-default "auto")
  (let [result (browser/open-session "https://example.com")]
    (try
      (is (= :playwright (:backend result)))
      (finally
        (browser/close-session (:session-id result))))))

(deftest open-session-normalizes-colon-prefixed-quoted-url
  (let [seen (atom nil)
        backend
        (reify browser.backend/BrowserBackend
          (backend-id [_] :playwright)
          (runtime-status* [_] nil)
          (bootstrap-runtime!* [_ _opts] nil)
          (install-browser-deps!* [_ _opts] nil)
          (open-session* [_ url _opts]
            (reset! seen url)
            {:session-id "sess-1"})
          (navigate* [_ _session-id _url] nil)
          (click* [_ _session-id _selector] nil)
          (fill-selector* [_ _session-id _selector _value _opts] nil)
          (fill-form* [_ _session-id _fields _opts] nil)
          (read-page* [_ _session-id] nil)
          (query-elements* [_ _session-id _opts] nil)
          (screenshot* [_ _session-id _opts] nil)
          (wait-for-page* [_ _session-id _opts] nil)
          (release-session* [_ _session-id] nil)
          (close-session* [_ _session-id] nil)
          (release-all-sessions!* [_] nil)
          (close-all-sessions!* [_] nil)
          (list-sessions* [_] []))]
    (with-redefs-fn {(ns-resolve 'xia.browser 'resolve-open-backend-id) (constantly :playwright)
                     (ns-resolve 'xia.browser 'backend-by-id) (fn [_] backend)}
      #(do
         (browser/open-session ": \"https://wiki.juji-inc.com/start\"")
         (is (= "https://wiki.juji-inc.com/start" @seen))))))

(deftest navigate-normalizes-colon-prefixed-quoted-url
  (let [seen (atom nil)
        backend
        (reify browser.backend/BrowserBackend
          (backend-id [_] :playwright)
          (runtime-status* [_] nil)
          (bootstrap-runtime!* [_ _opts] nil)
          (install-browser-deps!* [_ _opts] nil)
          (open-session* [_ _url _opts] nil)
          (navigate* [_ session-id url]
            (is (= "sess-1" session-id))
            (reset! seen url)
            {:session-id session-id})
          (click* [_ _session-id _selector] nil)
          (fill-selector* [_ _session-id _selector _value _opts] nil)
          (fill-form* [_ _session-id _fields _opts] nil)
          (read-page* [_ _session-id] nil)
          (query-elements* [_ _session-id _opts] nil)
          (screenshot* [_ _session-id _opts] nil)
          (wait-for-page* [_ _session-id _opts] nil)
          (release-session* [_ _session-id] nil)
          (close-session* [_ _session-id] nil)
          (release-all-sessions!* [_] nil)
          (close-all-sessions!* [_] nil)
          (list-sessions* [_] []))]
    (with-redefs-fn {(ns-resolve 'xia.browser 'backend-by-id) (fn [_] backend)
                     (ns-resolve 'xia.browser 'session-backend-id) (constantly :playwright)}
      #(do
         (browser/navigate "sess-1" ": \"https://wiki.juji-inc.com/start\"")
         (is (= "https://wiki.juji-inc.com/start" @seen))))))

(deftest open-session-auto-surfaces-playwright-failure
  (let [calls (atom [])
        playwright-backend
        (reify browser.backend/BrowserBackend
          (backend-id [_] :playwright)
          (runtime-status* [_] {:backend :playwright
                                :available? true
                                :ready? false
                                :status :missing-browser})
          (bootstrap-runtime!* [_ _opts] nil)
          (install-browser-deps!* [_ _opts] nil)
          (open-session* [_ _url _opts]
            (swap! calls conj :playwright)
            (throw (ex-info "Playwright launch failed." {:backend :playwright})))
          (navigate* [_ _session-id _url] nil)
          (click* [_ _session-id _selector] nil)
          (fill-selector* [_ _session-id _selector _value _opts] nil)
          (fill-form* [_ _session-id _fields _opts] nil)
          (read-page* [_ _session-id] nil)
          (query-elements* [_ _session-id _opts] nil)
          (wait-for-page* [_ _session-id _opts] nil)
          (release-session* [_ _session-id] nil)
          (close-session* [_ _session-id] nil)
          (release-all-sessions!* [_] nil)
          (close-all-sessions!* [_] nil)
          (list-sessions* [_] []))]
    (with-redefs-fn {(ns-resolve 'xia.browser 'resolve-open-backend-id) (constantly :playwright)
                     (ns-resolve 'xia.browser 'backend-by-id) (fn [backend-id]
                                                                (case backend-id
                                                                  :playwright playwright-backend
                                                                  (throw (ex-info "unexpected backend"
                                                                                  {:backend backend-id}))))}
      #(timbre/with-min-level :fatal
         (is (thrown-with-msg?
               clojure.lang.ExceptionInfo #"Playwright launch failed"
               (browser/open-session "https://example.com" :backend :auto)))
         (is (= [:playwright] @calls))))))

(deftest ^:integration open-session-explicit-playwright-works
  (db/set-config! :browser/playwright-enabled? "true")
  (let [result (browser/open-session "https://example.com" :backend :playwright)]
    (try
      (is (= :playwright (:backend result)))
      (is (= "Example Domain" (:title result)))
      (is (string? (:content result)))
      (finally
        (browser/close-session (:session-id result))))))

(deftest ^:integration playwright-list-sessions-includes-resumable-snapshots
  (db/set-config! :browser/playwright-enabled? "true")
  (let [result  (browser/open-session "https://example.com" :backend :playwright)
        sid     (:session-id result)
        _       (close-playwright-live-session! sid)
        listing (browser/list-sessions)
        entry   (some #(when (= sid (:session-id %)) %) listing)]
    (is (some? entry))
    (is (= :playwright (:backend entry)))
    (is (= false (:live? entry)))
    (is (= true (:resumable? entry)))
    (browser/close-session sid)))

(deftest ^:integration playwright-read-page-restores-from-snapshot
  (db/set-config! :browser/playwright-enabled? "true")
  (let [result (browser/open-session "https://example.com" :backend :playwright)
        sid    (:session-id result)
        _      (close-playwright-live-session! sid)
        page   (browser/read-page sid)]
    (is (= sid (:session-id page)))
    (is (= :playwright (:backend page)))
    (is (= "Example Domain" (:title page)))
    (browser/close-session sid)))

(deftest remote-open-session-persists-local-browser-snapshot
  (db/set-config! :browser/remote-enabled? "true")
  (db/set-config! :browser/remote-base-url "http://browser-pool.internal/v1")
  (let [{:keys [backend snapshots]} (test-remote-backend)
        requests (atom [])]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (swap! requests conj (select-keys req [:method :url :headers :body]))
                    {:status 200
                     :headers {"content-type" "application/json"}
                     :body (json/write-json-str
                            {"session" {"session_id" "browser-session-1"
                                        "current_url" "https://example.com"
                                        "browser_state" "{\"cookies\":[]}"
                                        "created_at_ms" 100
                                        "updated_at_ms" 120
                                        "last_access_ms" 120
                                        "js_enabled" true}
                             "result" {"url" "https://example.com"
                                       "title" "Remote Example"
                                       "content" "Example content"
                                       "forms" []
                                       "links" []
                                       "truncated?" false}})})]
      (let [result (browser.backend/open-session* backend "https://example.com" {:js true})
            session-id (:session-id result)
            snapshot (get @snapshots session-id)]
        (is (= :remote (:backend result)))
        (is (= "Remote Example" (:title result)))
        (is (= "https://example.com" (:url result)))
        (is (= "http://browser-pool.internal/v1/sessions"
               (:url (first @requests))))
        (is (= :post (:method (first @requests))))
        (is (= "remote" (get snapshot "backend")))
        (is (= "https://example.com" (get snapshot "current_url")))
        (is (= "{\"cookies\":[]}" (get snapshot "browser_state")))
        (is (= true (get snapshot "js_enabled")))))))

(deftest remote-read-page-restores-session-from-local-snapshot-when-worker-loses-it
  (db/set-config! :browser/remote-enabled? "true")
  (db/set-config! :browser/remote-base-url "http://browser-pool.internal/v1")
  (let [{:keys [backend snapshots]} (test-remote-backend)
        session-id "browser-session-restore"
        calls (atom [])]
    (swap! snapshots assoc session-id
           {"session_id" session-id
            "backend" "remote"
            "current_url" "https://example.com/account"
            "browser_state" "{\"cookies\":[{\"name\":\"sid\"}]}"
            "created_at_ms" 10
            "updated_at_ms" 20
            "last_access_ms" 20
            "js_enabled" true})
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (swap! calls conj [(:method req) (:url req)])
                    (let [request-key [(:method req) (:url req)]
                          page-key [:get "http://browser-pool.internal/v1/sessions/browser-session-restore/page"]
                          restored-body (json/write-json-str
                                         {"session" {"session_id" session-id
                                                     "current_url" "https://example.com/account"
                                                     "browser_state" "{\"cookies\":[{\"name\":\"sid2\"}]}"
                                                     "created_at_ms" 10
                                                     "updated_at_ms" 40
                                                     "last_access_ms" 40
                                                     "js_enabled" true}
                                          "result" {"url" "https://example.com/account"
                                                    "title" "Recovered"
                                                    "content" "Recovered page"
                                                    "forms" []
                                                    "links" []
                                                    "truncated?" false}})
                          create-body (json/write-json-str
                                       {"session" {"session_id" session-id
                                                   "current_url" "https://example.com/account"
                                                   "browser_state" "{\"cookies\":[{\"name\":\"sid2\"}]}"
                                                   "created_at_ms" 10
                                                   "updated_at_ms" 30
                                                   "last_access_ms" 30
                                                   "js_enabled" true}
                                        "result" {"url" "https://example.com/account"
                                                  "title" "Recovered"
                                                  "content" "Recovered page"
                                                  "forms" []
                                                  "links" []
                                                  "truncated?" false}})]
                      (cond
                        (= request-key page-key)
                        (if (= 1 (count (filter #(= page-key %) @calls)))
                          {:status 404
                           :headers {"content-type" "application/json"}
                           :body (json/write-json-str {"error" "No browser session"})}
                          {:status 200
                           :headers {"content-type" "application/json"}
                           :body restored-body})

                        (= request-key [:post "http://browser-pool.internal/v1/sessions"])
                        {:status 200
                         :headers {"content-type" "application/json"}
                         :body create-body}

                        :else
                        (throw (ex-info "Unexpected remote browser request"
                                        {:request req})))))]
      (let [result (browser.backend/read-page* backend session-id)
            snapshot (get @snapshots session-id)]
        (is (= :remote (:backend result)))
        (is (= "Recovered" (:title result)))
        (is (= [[:get "http://browser-pool.internal/v1/sessions/browser-session-restore/page"]
                [:post "http://browser-pool.internal/v1/sessions"]
                [:get "http://browser-pool.internal/v1/sessions/browser-session-restore/page"]]
               @calls))
        (is (= "{\"cookies\":[{\"name\":\"sid2\"}]}"
               (get snapshot "browser_state")))
        (is (= "https://example.com/account"
               (get snapshot "current_url")))))))

(deftest ^:integration playwright-screenshot-returns-data-url
  (db/set-config! :browser/playwright-enabled? "true")
  (let [result (browser/open-session "https://example.com" :backend :playwright)
        sid    (:session-id result)
        shot   (browser/screenshot sid :full-page true :detail "high")]
    (try
      (is (= sid (:session-id shot)))
      (is (= :playwright (:backend shot)))
      (is (= "image/png" (:mime_type shot)))
      (is (= true (:full_page shot)))
      (is (= "high" (:detail shot)))
      (is (pos? (:byte_count shot)))
      (is (str/starts-with? (:image_data_url shot) "data:image/png;base64,"))
      (finally
        (browser/close-session sid)))))

(deftest browser-runtime-status-reports-playwright
  (let [status (browser/browser-runtime-status)
        by-backend (into {} (map (juxt :backend identity) (:backends status)))]
    (is (= :auto (:configured-default-backend status)))
    (is (= :playwright (:selected-auto-backend status)))
    (is (= #{:playwright :remote} (set (keys by-backend))))
    (is (= true (:available? (get by-backend :playwright))))
    (is (contains? #{:available :missing-browser}
                   (:status (get by-backend :playwright))))
    (is (= false (:ready? (get by-backend :playwright))))
    (is (= :disabled (:status (get by-backend :remote))))
    (is (= false (:available? (get by-backend :remote))))))

(deftest browser-runtime-status-prefers-configured-remote-backend
  (db/set-config! :browser/remote-enabled? "true")
  (db/set-config! :browser/remote-base-url "http://browser-pool.internal/v1")
  (with-redefs [xia.http-client/request
                (fn [req]
                  (is (= :get (:method req)))
                  (is (= "http://browser-pool.internal/v1/runtime/status" (:url req)))
                  {:status 200
                   :headers {"content-type" "application/json"}
                   :body (json/write-json-str {"status" "ready"
                                               "ready" true
                                               "running" true
                                               "leases" 2})})]
    (let [status (browser/browser-runtime-status)
          by-backend (into {} (map (juxt :backend identity) (:backends status)))]
      (is (= :remote (:selected-auto-backend status)))
      (is (= :ready (:status (get by-backend :remote))))
      (is (= true (:ready? (get by-backend :remote))))
      (is (= 2 (get-in by-backend [:remote :service :leases]))))))

(deftest open-session-auto-uses-remote-when-configured
  (db/set-config! :browser/remote-enabled? "true")
  (db/set-config! :browser/remote-base-url "http://browser-pool.internal/v1")
  (let [requests (atom [])]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (swap! requests conj (select-keys req [:method :url :body]))
                    {:status 200
                     :headers {"content-type" "application/json"}
                     :body (json/write-json-str
                            {"session" {"session_id" "browser-session-auto"
                                        "current_url" "https://example.com"
                                        "browser_state" "{\"cookies\":[]}"
                                        "created_at_ms" 100
                                        "updated_at_ms" 120
                                        "last_access_ms" 120
                                        "js_enabled" true}
                             "result" {"url" "https://example.com"
                                       "title" "Remote Auto"
                                       "content" "Example content"
                                       "forms" []
                                       "links" []
                                       "truncated?" false}})})]
      (let [result (browser/open-session "https://example.com" :backend :auto)]
        (try
          (is (= :remote (:backend result)))
          (is (= "Remote Auto" (:title result)))
          (is (= "http://browser-pool.internal/v1/sessions"
                 (:url (first @requests))))
          (finally
            (browser/close-session (:session-id result))))))))

(deftest remote-actions-update-snapshot-metadata-without-inline-browser-state
  (db/set-config! :browser/remote-enabled? "true")
  (db/set-config! :browser/remote-base-url "http://browser-pool.internal/v1")
  (let [{:keys [backend snapshots]} (test-remote-backend)
        requests (atom [])
        session-id* (atom nil)]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (swap! requests conj [(:method req) (:url req)])
                    (let [url (:url req)
                          current-session-id @session-id*]
                      (cond
                        (= [(:method req) url]
                           [:post "http://browser-pool.internal/v1/sessions"])
                        (let [payload (json/read-json (:body req))
                              session-id (get payload "session_id")]
                          (reset! session-id* session-id)
                          {:status 200
                           :headers {"content-type" "application/json"}
                           :body (json/write-json-str
                                  {"session" {"session_id" session-id
                                              "current_url" "https://example.com"
                                              "browser_state" "{\"cookies\":[{\"name\":\"sid\"}]}"
                                              "created_at_ms" 100
                                              "updated_at_ms" 120
                                              "last_access_ms" 120
                                              "js_enabled" true}
                                   "result" {"url" "https://example.com"
                                             "title" "Opened"
                                             "content" "Open page"
                                             "forms" []
                                             "links" []
                                             "truncated?" false}})})

                        (= [(:method req) url]
                           [:get (str "http://browser-pool.internal/v1/sessions/" current-session-id "/page")])
                        {:status 200
                         :headers {"content-type" "application/json"}
                         :body (json/write-json-str
                                {"url" "https://example.com/next"
                                 "title" "Read page"
                                 "content" "Read page"
                                 "forms" []
                                 "links" []
                                 "truncated?" false})}

                        (= [(:method req) url]
                           [:delete (str "http://browser-pool.internal/v1/sessions/" current-session-id)])
                        {:status 204
                         :headers {}
                         :body ""}

                        :else
                        (throw (ex-info "Unexpected remote browser request"
                                        {:request req})))))]
      (let [opened (browser.backend/open-session* backend "https://example.com" {:js true})
            session-id (:session-id opened)
            original-snapshot (get @snapshots session-id)]
        (reset! session-id* session-id)
        (let [page (browser.backend/read-page* backend session-id)]
          (is (= :remote (:backend page)))
          (is (= "Read page" (:title page)))
          (is (= (get original-snapshot "browser_state")
                 (get-in @snapshots [session-id "browser_state"]))
              "Lightweight action responses should preserve the last exported browser state")
          (is (= "https://example.com/next"
                 (get-in @snapshots [session-id "current_url"]))
              "Lightweight action responses should still advance resumable URL metadata")
          (is (= [[:post "http://browser-pool.internal/v1/sessions"]
                  [:get (str "http://browser-pool.internal/v1/sessions/" session-id "/page")]]
                 @requests))))
      (when-let [session-id @session-id*]
        (browser/close-session session-id)))))

(deftest remote-open-session-closes-lease-when-local-snapshot-persist-fails
  (db/set-config! :browser/remote-enabled? "true")
  (db/set-config! :browser/remote-base-url "http://browser-pool.internal/v1")
  (let [requests    (atom [])
        session-id* (atom nil)
        backend     ((ns-resolve 'xia.browser.remote 'create-backend)
                     {:read-snapshot (constantly nil)
                      :write-snapshot! (fn [_ _]
                                         (throw (ex-info "snapshot write failed" {})))
                      :delete-snapshot! (fn [_] nil)
                      :all-snapshots (constantly nil)
                      :snapshot-expired? (constantly false)
                      :snapshot-usable? (constantly {:ok? true})
                      :validate-url! (fn [_] nil)
                      :resolve-url! (fn [_] nil)})]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (swap! requests conj [(:method req) (:url req)])
                    (let [url (:url req)]
                      (cond
                        (= [(:method req) url]
                           [:post "http://browser-pool.internal/v1/sessions"])
                        (let [payload (json/read-json (:body req))
                              session-id (get payload "session_id")]
                          (reset! session-id* session-id)
                          {:status 200
                           :headers {"content-type" "application/json"}
                           :body (json/write-json-str
                                  {"session" {"session_id" session-id
                                              "current_url" "https://example.com"
                                              "browser_state" "{\"cookies\":[]}"
                                              "created_at_ms" 100
                                              "updated_at_ms" 120
                                              "last_access_ms" 120
                                              "js_enabled" true}
                                   "result" {"url" "https://example.com"
                                             "title" "Opened"
                                             "content" "Open page"
                                             "forms" []
                                             "links" []
                                             "truncated?" false}})})

                        (= [(:method req) url]
                           [:delete (str "http://browser-pool.internal/v1/sessions/" @session-id*)])
                        {:status 204
                         :headers {}
                         :body ""}

                        :else
                        (throw (ex-info "Unexpected remote browser request"
                                        {:request req})))))]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"snapshot write failed"
            (browser.backend/open-session* backend "https://example.com" {:js true})))
      (is (= [[:post "http://browser-pool.internal/v1/sessions"]
              [:delete (str "http://browser-pool.internal/v1/sessions/" @session-id*)]]
             @requests)
          "Remote lease should be cleaned up if Xia cannot persist the session locally"))))

(deftest remote-release-session-exports-state-once-then-drops-lease
  (db/set-config! :browser/remote-enabled? "true")
  (db/set-config! :browser/remote-base-url "http://browser-pool.internal/v1")
  (let [{:keys [backend snapshots]} (test-remote-backend)
        session-id "browser-session-release"
        requests (atom [])]
    (swap! snapshots assoc session-id
           {"session_id" session-id
            "backend" "remote"
            "current_url" "https://example.com/start"
            "browser_state" "{\"cookies\":[{\"name\":\"old\"}]}"
            "created_at_ms" 10
            "updated_at_ms" 20
            "last_access_ms" 20
            "js_enabled" true})
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (swap! requests conj [(:method req) (:url req)])
                    (case [(:method req) (:url req)]
                      [:post "http://browser-pool.internal/v1/sessions/browser-session-release/export"]
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body (json/write-json-str
                              {"session" {"session_id" session-id
                                          "current_url" "https://example.com/final"
                                          "browser_state" "{\"cookies\":[{\"name\":\"new\"}]}"
                                          "created_at_ms" 10
                                          "updated_at_ms" 40
                                          "last_access_ms" 40
                                          "js_enabled" true}})}

                      [:delete "http://browser-pool.internal/v1/sessions/browser-session-release"]
                      {:status 204
                       :headers {}
                       :body ""}

                      (throw (ex-info "Unexpected remote browser request"
                                      {:request req}))))]
      (is (= {:status "released"
              :session-id session-id
              :resumable? true}
             (browser.backend/release-session* backend session-id)))
      (is (= [[:post "http://browser-pool.internal/v1/sessions/browser-session-release/export"]
              [:delete "http://browser-pool.internal/v1/sessions/browser-session-release"]]
             @requests))
      (is (= "https://example.com/final"
             (get-in @snapshots [session-id "current_url"])))
      (is (= "{\"cookies\":[{\"name\":\"new\"}]}"
             (get-in @snapshots [session-id "browser_state"]))))))      

(deftest playwright-runtime-status-reports-missing-browser-when-installable
  (with-playwright-runtime-state {:playwright nil
                                  :browser nil
                                  :bootstrapped? false
                                  :browser-installed? false
                                  :browser-executable nil}
    (fn []
      (with-redefs-fn {(playwright-var 'enabled?) (constantly true)
                       (playwright-var 'auto-install?) (constantly true)
                       (playwright-var 'browser-installation-info)
                       (fn
                         ([] {:browser "chromium"
                              :available? true
                              :installed? false
                              :executable "/tmp/ms-playwright/chromium"
                              :message "missing"})
                         ([_] {:browser "chromium"
                               :available? true
                               :installed? false
                               :executable "/tmp/ms-playwright/chromium"
                               :message "missing"}))}
        #(let [status (call-playwright-private 'runtime-status)]
           (is (= :missing-browser (:status status)))
           (is (= true (:available? status)))
           (is (= false (:ready? status)))
           (is (= true (:auto-install? status))))))))

(deftest playwright-ensure-browser-installed-installs-on-first-use
  (let [state (atom :missing)
        installs (atom 0)]
    (with-redefs-fn {(playwright-var 'auto-install?) (constantly true)
                     (playwright-var 'browser-installation-info)
                     (fn
                       ([] (case @state
                             :missing {:browser "chromium"
                                       :available? true
                                       :installed? false
                                       :executable "/tmp/ms-playwright/chromium"
                                       :message "missing"}
                             :installed {:browser "chromium"
                                         :available? true
                                         :installed? true
                                         :executable "/tmp/ms-playwright/chromium"
                                         :message "installed"}))
                       ([_] (case @state
                              :missing {:browser "chromium"
                                        :available? true
                                        :installed? false
                                        :executable "/tmp/ms-playwright/chromium"
                                        :message "missing"}
                              :installed {:browser "chromium"
                                          :available? true
                                          :installed? true
                                          :executable "/tmp/ms-playwright/chromium"
                                          :message "installed"})))
                     (playwright-var 'install-browser!)
                     (fn []
                       (swap! installs inc)
                       (reset! state :installed)
                       {:exit 0
                        :output "installed"})}
      #(timbre/with-min-level :fatal
         (let [result (call-playwright-private 'ensure-browser-installed! nil)]
           (is (= 1 @installs))
           (is (= true (:installed? result)))
           (is (= true (:installed-by-xia? result)))
           (is (= "/tmp/ms-playwright/chromium" (:executable result))))))))

(deftest playwright-ensure-browser-installed-throws-when-auto-install-is-disabled
  (with-redefs-fn {(playwright-var 'auto-install?) (constantly false)
                   (playwright-var 'browser-installation-info)
                   (fn
                     ([] {:browser "chromium"
                          :available? true
                          :installed? false
                          :executable "/tmp/ms-playwright/chromium"
                          :message "missing"})
                     ([_] {:browser "chromium"
                           :available? true
                           :installed? false
                           :executable "/tmp/ms-playwright/chromium"
                           :message "missing"}))}
    #(let [result (try
                    (call-playwright-private 'ensure-browser-installed! nil)
                    (catch clojure.lang.ExceptionInfo e
                      e))]
       (is (instance? clojure.lang.ExceptionInfo result))
       (is (= :missing-browser (:status (ex-data result)))))))

(deftest playwright-install-browser-deps-reports-unsupported-platform
  (with-redefs-fn {(playwright-var 'linux-platform?) (constantly false)}
    #(let [result (call-playwright-private 'install-browser-deps! {:dry-run true})]
       (is (= :playwright (:backend result)))
       (is (= false (:supported? result)))
       (is (= :unsupported-platform (:status result))))))

(deftest playwright-install-browser-deps-dry-run-on-linux
  (with-redefs-fn {(playwright-var 'linux-platform?) (constantly true)
                   (playwright-var 'run-playwright-cli!)
                   (fn [args opts]
                     {:args (vec args)
                      :exit 0
                      :output "sudo apt-get install ..."
                      :interactive? (:inherit-io? opts)})}
    #(let [result (call-playwright-private 'install-browser-deps! {:dry-run true})]
       (is (= :playwright (:backend result)))
       (is (= true (:supported? result)))
       (is (= :dry-run (:status result)))
       (is (= ["install-deps" "chromium" "--dry-run"] (:args result)))
       (is (= "sudo apt-get install ..." (:output result))))))

(deftest browser-runtime-status-reports-disabled-playwright
  (db/set-config! :browser/playwright-enabled? "false")
  (let [status (browser/browser-runtime-status)
        by-backend (into {} (map (juxt :backend identity) (:backends status)))]
    (is (= :playwright (:selected-auto-backend status)))
    (is (= :disabled (:status (get by-backend :playwright))))
    (is (= false (:available? (get by-backend :playwright))))
    (is (= false (:ready? (get by-backend :playwright))))
    (is (= :disabled (:status (get by-backend :remote))))))

(deftest open-session-explicit-playwright-throws-when-disabled
  (db/set-config! :browser/playwright-enabled? "false")
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"disabled"
        (browser/open-session "https://example.com" :backend :playwright))))

(deftest bootstrap-browser-runtime-status-is-reported
  (db/set-config! :browser/playwright-enabled? "true")
  (let [auto-result (browser/bootstrap-browser-runtime!)
        playwright-result (browser/bootstrap-browser-runtime! :backend :playwright)
        after (browser/browser-runtime-status)]
    (is (= :auto (:requested-backend auto-result)))
    (is (= #{:playwright :remote}
           (set (map :backend (:results auto-result)))))
    (is (= :playwright (:backend playwright-result)))
    (is (= :running (:status playwright-result)))
    (is (= true (:ready? playwright-result)))
    (is (= :playwright (:selected-auto-backend after)))))

(deftest ^:integration playwright-handles-local-spa-fixture
  (db/set-config! :browser/playwright-enabled? "true")
  (with-spa-server
    (fn [base-url]
      (let [{:keys [backend]} (test-playwright-backend)]
        (try
          (let [opened (browser.backend/open-session* backend (str base-url "/") {:js true})
                sid    (:session-id opened)]
            (is (= :playwright (:backend opened)))
            (is (= "SPA Fixture" (:title opened)))

            (let [ready (browser.backend/wait-for-page* backend sid
                                                        {:text "Ready"
                                                         :timeout-ms 5000
                                                         :interval-ms 100})]
              (is (= true (:matched ready)))
              (is (re-find #"Alpha" (:content ready)))
              (is (re-find #"Beta" (:content ready)))
              (is (re-find #"Items: 2" (:content ready))))

            (let [clicked (browser.backend/click* backend sid "#add-item")]
              (is (= :playwright (:backend clicked))))

            (let [after-click (browser.backend/wait-for-page* backend sid
                                                              {:text "Extra 3"
                                                               :timeout-ms 5000
                                                               :interval-ms 100})]
              (is (= true (:matched after-click)))
              (is (re-find #"Items: 3" (:content after-click)))
              (is (re-find #"Extra 3" (:content after-click))))

            (let [submitted (browser.backend/fill-form* backend sid
                                                        {"query" "widgets"}
                                                        {:form-selector "#search-form"
                                                         :submit true})]
              (is (= :playwright (:backend submitted))))

            (let [after-submit (browser.backend/wait-for-page* backend sid
                                                               {:text "Result for widgets"
                                                                :timeout-ms 5000
                                                                :interval-ms 100})]
              (is (= true (:matched after-submit)))
              (is (re-find #"Result for widgets" (:content after-submit)))))
          (finally
            (browser.backend/close-all-sessions!* backend)))))))

(deftest ^:integration playwright-fill-form-can-require-all-fields
  (db/set-config! :browser/playwright-enabled? "true")
  (with-spa-server
    (fn [base-url]
      (let [{:keys [backend]} (test-playwright-backend)]
        (try
          (let [opened (browser.backend/open-session* backend (str base-url "/") {:js true})
                sid    (:session-id opened)]
            (try
              (browser.backend/fill-form* backend sid
                                          {"username" "hyang"
                                           "password" "pw"}
                                          {:form-selector "#search-form"
                                           :require-all-fields? true
                                           :submit true})
              (is false "Expected strict form fill to fail when fields are missing")
              (catch clojure.lang.ExceptionInfo e
                (is (= "Configured form fields not found on page" (.getMessage e)))
                (is (= "#search-form" (:form-selector (ex-data e))))
                (is (= sid (:session-id (ex-data e))))
                (is (= #{"username" "password"}
                       (set (:missing-fields (ex-data e))))))))
          (finally
            (browser.backend/close-all-sessions!* backend)))))))

(deftest ^:integration playwright-query-elements-form-selector-ignores-shadowed-form-id-property
  (db/set-config! :browser/playwright-enabled? "true")
  (with-id-shadow-login-server
    (fn [base-url]
      (let [{:keys [backend]} (test-playwright-backend)]
        (try
          (let [opened (browser.backend/open-session* backend (str base-url "/") {:js true})
                sid    (:session-id opened)
                fields (browser.backend/query-elements* backend sid
                                                       {:kind :fields
                                                        :visible-only false
                                                        :limit 50})
                password-field (first (filter #(= "p" (:name %)) (:elements fields)))
                form-selector  (:form_selector password-field)]
            (is (string? form-selector))
            (is (not (str/includes? form-selector "[object HTMLInputElement]")))
            (let [filled (browser.backend/fill-form* backend sid
                                                     {"u" "hyang"
                                                      "p" "pw"}
                                                     {:form-selector form-selector
                                                      :require-all-fields? true
                                                      :submit false})]
              (is (= :playwright (:backend filled)))))
          (finally
            (browser.backend/close-all-sessions!* backend)))))))

(deftest ^:integration playwright-query-elements-supports-kinds-and-visibility
  (db/set-config! :browser/playwright-enabled? "true")
  (with-dense-dashboard-server
    (fn [base-url]
      (let [{:keys [backend]} (test-playwright-backend)]
        (try
          (let [opened (browser.backend/open-session* backend (str base-url "/") {:js true})
                sid    (:session-id opened)
                visible-buttons (browser.backend/query-elements* backend sid
                                                                {:kind :buttons
                                                                 :visible-only true})
                all-buttons (browser.backend/query-elements* backend sid
                                                             {:kind :buttons
                                                              :visible-only false})
                filtered-links (browser.backend/query-elements* backend sid
                                                                {:kind :links
                                                                 :text-contains "Item 3"
                                                                 :limit 5})]
            (is (= :playwright (:backend visible-buttons)))
            (is (= 1 (:total_count visible-buttons)))
            (is (= "Run report" (get-in visible-buttons [:elements 0 :text])))
            (is (= 2 (:total_count all-buttons)))
            (is (= "Hidden action" (get-in all-buttons [:elements 1 :text])))
            (is (= 5 (:returned_count filtered-links)))
            (is (= 11 (:total_count filtered-links)))
            (is (= "Item 3" (get-in filtered-links [:elements 0 :text]))))
          (finally
            (browser.backend/close-all-sessions!* backend)))))))

(deftest ^:integration open-session-auto-prefers-playwright-for-local-spa-when-ready
  (db/set-config! :browser/playwright-enabled? "true")
  (browser/bootstrap-browser-runtime! :backend :playwright)
  (with-spa-server
    (fn [base-url]
      (with-redefs [browser/resolve-host-addresses
                    (fn [_host]
                      [(InetAddress/getByAddress
                         "public.test"
                         (byte-array [(byte 93) (byte -72) (byte 34) (byte 20)]))])]
        (let [opened (browser/open-session (str base-url "/") :backend :auto)
              sid    (:session-id opened)]
          (try
            (is (= :playwright (:backend opened)))
            (let [ready (browser/wait-for-page sid
                                               :text "Ready"
                                               :timeout-ms 5000
                                               :interval-ms 100)]
              (is (= true (:matched ready)))
              (is (re-find #"Items: 2" (:content ready)))
              (is (re-find #"Alpha" (:content ready))))
            (finally
              (browser/close-session sid))))))))
