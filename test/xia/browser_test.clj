(ns xia.browser-test
  (:require [clojure.test :refer :all]
            [xia.browser :as browser]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.net InetAddress URL]
           [org.htmlunit MockWebConnection WebClient WebRequest]
           [org.htmlunit.html HtmlInput]))

(use-fixtures :each
  with-test-db
  (fn [f]
    (browser/close-all-sessions!)
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
    (is (some #(= sid (:session-id %)) listing))
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
    (is (contains? result :url))
    (is (contains? result :title))
    (is (contains? result :content))
    (is (contains? result :forms))
    (is (contains? result :links))
    (is (contains? result :truncated?))
    (is (contains? result :session-id))
    (browser/close-session sid)))
