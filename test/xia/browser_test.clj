(ns xia.browser-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [org.httpkit.server :as http-server]
            [taoensso.timbre :as timbre]
            [xia.browser :as browser]
            [xia.browser.backend :as browser.backend]
            [xia.browser.playwright]
            [xia.db :as db]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.net InetAddress ServerSocket URL]
           [org.htmlunit MockWebConnection WebClient WebRequest]
           [org.htmlunit.html HtmlInput]))

(use-fixtures :each
  with-test-db
  (fn [f]
    (browser/close-all-sessions!)
    (db/set-config! :browser/playwright-enabled? "false")
    (db/set-config! :browser/backend-default "auto")
    (f)
    (browser/close-all-sessions!)))

(defn- mock-html-page
  [html]
  (let [client (WebClient.)
        mock   (MockWebConnection.)
        url    (URL. "https://example.com/form")]
    (.setResponse mock url html)
    (.setWebConnection client mock)
    {:client client
     :page   (.getPage client (.toString url))}))

(defn- close-playwright-live-session!
  [session-id]
  (let [close-live-session! (var-get (ns-resolve 'xia.browser.playwright
                                                 'close-live-session!))]
    (close-live-session! session-id)))

(defn- playwright-var
  [sym]
  (ns-resolve 'xia.browser.playwright sym))

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

(deftest web-client-validates-every-request
  (let [client (#'browser/make-client)]
    (with-redefs-fn {#'browser/resolve-url!
                     (fn [_]
                       (throw (ex-info "blocked redirect target" {})))}
      #(is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"blocked redirect target"
             (.getResponse (.getWebConnection client)
                           (WebRequest. (URL. "https://example.com"))))))
    (.close client)))

(deftest web-client-pins-each-request-resolution
  (let [client (#'browser/make-client)
        calls  (atom [])]
    (with-redefs-fn {#'browser/resolve-host-addresses
                     (fn [host]
                       (case host
                         "public.example"
                         [(InetAddress/getByAddress "public.example" (byte-array [(byte 93) (byte -72) (byte 34) (byte 20)]))]
                         "next.example"
                         [(InetAddress/getByAddress "next.example" (byte-array [(byte 93) (byte -72) (byte 34) (byte 21)]))]))
                     #'browser/send-browser-request!
                     (fn [_client request resolution]
                       (swap! calls conj {:url (str (.getUrl ^WebRequest request))
                                          :host (:host resolution)
                                          :addresses (mapv #(.getHostAddress ^InetAddress %)
                                                           (:addresses resolution))})
                       nil)}
      #(do
         (.getResponse (.getWebConnection client)
                       (WebRequest. (URL. "https://public.example/start")))
         (.getResponse (.getWebConnection client)
                       (WebRequest. (URL. "https://next.example/next")))))
    (is (= [{:url "https://public.example/start"
             :host "public.example"
             :addresses ["93.184.34.20"]}
            {:url "https://next.example/next"
             :host "next.example"
             :addresses ["93.184.34.21"]}]
           @calls))
    (.close client)))

;; ---------------------------------------------------------------------------
;; Session lifecycle
;; ---------------------------------------------------------------------------

(deftest ^:integration open-and-close-session
  (let [result (browser/open-session "https://example.com")]
    (is (string? (:session-id result)))
    (is (= :htmlunit (:backend result)))
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

(deftest find-form-field-element-matches-exact-id-with-css-metacharacters
  (let [field-id "victim\"], #attacker"
        {:keys [client page]} (mock-html-page
                                (str "<html><body><form>"
                                     "<input id='" field-id "' value='original'/>"
                                     "<input id='attacker' value='sentinel'/>"
                                     "</form></body></html>"))
        form (first (.getForms page))]
    (try
      (let [el (#'browser/find-form-field-element form field-id)]
        (is (some? el))
        (is (= field-id (.getAttribute ^HtmlInput el "id")))
        (is (not= "attacker" (.getAttribute ^HtmlInput el "id"))))
      (finally
        (.close client)))))

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
                    (= :htmlunit (:backend %)))
              listing))
    (browser/close-session sid)))

(deftest ^:integration list-sessions-includes-resumable-snapshots
  (let [result  (browser/open-session "https://example.com")
        sid     (:session-id result)
        _       ((var-get #'browser/close-live-session!) sid)
        listing (browser/list-sessions)
        entry   (some #(when (= sid (:session-id %)) %) listing)]
    (is (some? entry))
    (is (= false (:live? entry)))
    (is (= true (:resumable? entry)))
    (browser/close-session sid)))

(deftest ^:integration read-page-restores-from-snapshot
  (let [result (browser/open-session "https://example.com")
        sid    (:session-id result)
        _      ((var-get #'browser/close-live-session!) sid)
        page   (browser/read-page sid)]
    (is (= sid (:session-id page)))
    (is (= "Example Domain" (:title page)))
    (browser/close-session sid)))

(deftest concurrent-get-session-restores-once-per-session-id
  (let [sid "restore-race"
        url "https://example.com/restored"
        snapshot {"session_id" sid
                  "current_url" url
                  "created_at_ms" (System/currentTimeMillis)
                  "updated_at_ms" (System/currentTimeMillis)
                  "last_access_ms" (System/currentTimeMillis)
                  "js_enabled" true
                  "cookies" []}
        make-client-calls (atom 0)
        first-restore-started (promise)
        allow-restore (promise)]
    ((var-get #'browser/write-session-snapshot!) sid snapshot)
    (with-redefs [browser/make-client
                  (fn []
                    (swap! make-client-calls inc)
                    (deliver first-restore-started true)
                    @allow-restore
                    (let [client (WebClient.)
                          mock (MockWebConnection.)]
                      (.setResponse mock (URL. url)
                                    "<html><head><title>Restored</title></head><body>restored</body></html>")
                      (.setWebConnection client mock)
                      client))
                  browser/wait-for-js! (fn [client _] client)
                  browser/persist-session! (fn [_] nil)]
      (let [f1 (future ((var-get #'browser/get-session) sid))
            _ @first-restore-started
            f2 (future ((var-get #'browser/get-session) sid))]
        (Thread/sleep 100)
        (is (= 1 @make-client-calls))
        (deliver allow-restore true)
        (let [sess1 @f1
              sess2 @f2]
          (is (= 1 @make-client-calls))
          (is (identical? (:client sess1) (:client sess2)))
          (browser/close-session sid))))))

;; ---------------------------------------------------------------------------
;; Session limit
;; ---------------------------------------------------------------------------

(deftest ^:integration session-limit-enforced
  (let [sids (atom [])]
    (try
      ;; Open max sessions
      (dotimes [_ 5]
        (let [r (browser/open-session "https://example.com")]
          (swap! sids conj (:session-id r))))
      ;; One more should fail
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Too many browser sessions"
            (browser/open-session "https://example.com")))
      (finally
        (doseq [sid @sids]
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

(deftest open-session-uses-configured-default-backend-when-auto
  (db/set-config! :browser/backend-default "auto")
  (db/set-config! :browser/playwright-enabled? "false")
  (let [result (browser/open-session "https://example.com")]
    (try
      (is (= :htmlunit (:backend result)))
      (finally
        (browser/close-session (:session-id result))))))

(deftest open-session-auto-falls-back-when-playwright-open-fails
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
          (fill-form* [_ _session-id _fields _opts] nil)
          (read-page* [_ _session-id] nil)
          (wait-for-page* [_ _session-id _opts] nil)
          (close-session* [_ _session-id] nil)
          (close-all-sessions!* [_] nil)
          (list-sessions* [_] []))
        htmlunit-backend
        (reify browser.backend/BrowserBackend
          (backend-id [_] :htmlunit)
          (runtime-status* [_] {:backend :htmlunit
                                :available? true
                                :ready? true
                                :status :ready})
          (bootstrap-runtime!* [_ _opts] nil)
          (install-browser-deps!* [_ _opts] nil)
          (open-session* [_ url _opts]
            (swap! calls conj :htmlunit)
            {:session-id "fallback-htmlunit"
             :backend :htmlunit
             :url url
             :title "Fallback"
             :content ""
             :forms []
             :links []
             :truncated? false})
          (navigate* [_ _session-id _url] nil)
          (click* [_ _session-id _selector] nil)
          (fill-form* [_ _session-id _fields _opts] nil)
          (read-page* [_ _session-id] nil)
          (wait-for-page* [_ _session-id _opts] nil)
          (close-session* [_ _session-id] nil)
          (close-all-sessions!* [_] nil)
          (list-sessions* [_] []))]
    (with-redefs-fn {(ns-resolve 'xia.browser 'resolve-open-backend-id) (constantly :playwright)
                     (ns-resolve 'xia.browser 'backend-by-id) (fn [backend-id]
                                                                (case backend-id
                                                                  :playwright playwright-backend
                                                                  :htmlunit htmlunit-backend
                                                                  (throw (ex-info "unexpected backend"
                                                                                  {:backend backend-id}))))}
      #(timbre/with-min-level :fatal
         (let [result (browser/open-session "https://example.com" :backend :auto)]
           (is (= :htmlunit (:backend result)))
           (is (= [:playwright :htmlunit] @calls)))))))

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

(deftest htmlunit-screenshot-is-explicitly-unsupported
  (let [result (browser/open-session "https://example.com")
        sid    (:session-id result)]
    (try
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"does not support screenshots"
            (browser/screenshot sid)))
      (finally
        (browser/close-session sid)))))

(deftest browser-runtime-status-reports-htmlunit-and-playwright
  (db/set-config! :browser/playwright-enabled? "true")
  (let [status (browser/browser-runtime-status)
        by-backend (into {} (map (juxt :backend identity) (:backends status)))]
    (is (= :auto (:configured-default-backend status)))
    (is (= :playwright (:selected-auto-backend status)))
    (is (= :ready (:status (get by-backend :htmlunit))))
    (is (= true (:ready? (get by-backend :htmlunit))))
    (is (= true (:available? (get by-backend :playwright))))
    (is (contains? #{:available :missing-browser}
                   (:status (get by-backend :playwright))))
    (is (= false (:ready? (get by-backend :playwright))))))

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
    (is (= :htmlunit (:selected-auto-backend status)))
    (is (= :disabled (:status (get by-backend :playwright))))
    (is (= false (:available? (get by-backend :playwright))))
    (is (= false (:ready? (get by-backend :playwright))))))

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
    (is (= #{:htmlunit :playwright}
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
