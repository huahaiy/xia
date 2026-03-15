(ns xia.web-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [charred.api :as json]
            [xia.ssrf :as ssrf]
            [xia.web :as web])
  (:import [java.net InetAddress]
           [java.util.concurrent ConcurrentHashMap]
           [org.jsoup Jsoup]
           [org.jsoup.nodes Document Element]))

;; ---------------------------------------------------------------------------
;; SSRF protection
;; ---------------------------------------------------------------------------

(deftest validate-url-blocks-private-ips
  (testing "blocks localhost"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"private/internal"
          (ssrf/validate-url! "http://127.0.0.1/secret"))))
  (testing "blocks localhost hostname"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"private/internal"
          (ssrf/validate-url! "http://localhost/secret")))))

(deftest validate-url-blocks-mixed-public-and-private-resolution
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"private/internal"
        (ssrf/validate-url! (fn [_]
                              [(InetAddress/getByAddress "public.example" (byte-array [(byte 93) (byte -72) (byte 34) (byte 20)]))
                               (InetAddress/getByAddress "private.example" (byte-array [(byte 127) (byte 0) (byte 0) (byte 1)]))])
                            "https://mixed.example/path"))))

(deftest validate-url-blocks-bad-schemes
  (testing "blocks file://"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Only http"
          (ssrf/validate-url! "file:///etc/passwd"))))
  (testing "blocks ftp://"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Only http"
          (ssrf/validate-url! "ftp://example.com/file"))))
  (testing "blocks javascript:"
    (is (thrown-with-msg?
          Exception #"."
          (ssrf/validate-url! "javascript:alert(1)")))))

(deftest validate-url-allows-public-urls
  (testing "allows https"
    (is (nil? (ssrf/validate-url! "https://example.com"))))
  (testing "allows http"
    (is (nil? (ssrf/validate-url! "http://example.com")))))

(deftest rate-limit-overflow-does-not-mutate-state
  (let [host  "example.com"
        now   (System/currentTimeMillis)
        hit-ts (- now 1000)
        state (atom {:timestamps (vec (repeat (var-get #'web/rate-limit-max) hit-ts))
                     :cleaned    now})
        limits (doto (ConcurrentHashMap.)
                 (.put host state))]
    (with-redefs [xia.web/rate-limits limits]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Rate limit exceeded for example.com"
            (#'web/check-rate-limit! "https://example.com/path")))
      (is (= {:timestamps (vec (repeat (var-get #'web/rate-limit-max) hit-ts))
              :cleaned    now}
             @state)))))

;; ---------------------------------------------------------------------------
;; HTML → readable text conversion
;; ---------------------------------------------------------------------------

(defn- html->markdown [html & {:keys [include-links] :or {include-links true}}]
  (let [doc (Jsoup/parse ^String html)
        body (.body doc)]
    (#'web/element->markdown body include-links)))

(deftest converts-headings
  (is (re-find #"^# Title" (html->markdown "<h1>Title</h1>")))
  (is (re-find #"## Sub" (html->markdown "<h2>Sub</h2>")))
  (is (re-find #"### Third" (html->markdown "<h3>Third</h3>"))))

(deftest converts-paragraphs
  (let [result (html->markdown "<p>First paragraph</p><p>Second paragraph</p>")]
    (is (re-find #"First paragraph" result))
    (is (re-find #"Second paragraph" result))))

(deftest converts-links
  (testing "with include-links true"
    (let [result (html->markdown "<a href=\"https://example.com\">click here</a>")]
      (is (re-find #"\[click here\]\(https://example.com\)" result))))
  (testing "with include-links false"
    (let [result (html->markdown "<a href=\"https://example.com\">click here</a>"
                                 :include-links false)]
      (is (re-find #"click here" result))
      (is (not (re-find #"https://example.com" result))))))

(deftest converts-lists
  (testing "unordered list"
    (let [result (html->markdown "<ul><li>Apple</li><li>Banana</li></ul>")]
      (is (re-find #"- Apple" result))
      (is (re-find #"- Banana" result))))
  (testing "ordered list"
    (let [result (html->markdown "<ol><li>First</li><li>Second</li></ol>")]
      (is (re-find #"1\. First" result))
      (is (re-find #"2\. Second" result)))))

(deftest converts-code-blocks
  (let [result (html->markdown "<pre><code>def foo(): pass</code></pre>")]
    (is (re-find #"```" result))
    (is (re-find #"def foo" result))))

(deftest converts-blockquotes
  (let [result (html->markdown "<blockquote>A wise quote</blockquote>")]
    (is (re-find #"> A wise quote" result))))

(deftest converts-tables
  (let [result (html->markdown
                 "<table><tr><th>Name</th><th>Value</th></tr>
                  <tr><td>A</td><td>1</td></tr></table>")]
    (is (re-find #"\| Name \| Value \|" result))
    (is (re-find #"\| A \| 1 \|" result))))

(deftest converts-images
  (let [result (html->markdown "<img alt=\"A cute cat\" src=\"cat.jpg\">")]
    (is (re-find #"\[image: A cute cat\]" result))))

(deftest strips-noise-elements
  (let [doc (Jsoup/parse "<body><nav>Menu</nav><article>Content</article><footer>Foot</footer></body>")]
    (doseq [^Element el (.select doc ^String @#'web/noise-selectors)]
      (.remove el))
    (let [text (.text (.body doc))]
      (is (re-find #"Content" text))
      (is (not (re-find #"Menu" text)))
      (is (not (re-find #"Foot" text))))))

;; ---------------------------------------------------------------------------
;; Main content extraction
;; ---------------------------------------------------------------------------

(deftest finds-article-as-main-content
  (let [doc (Jsoup/parse "<body><div>Noise</div><article>Main content here</article></body>")
        main (#'web/find-main-content doc)]
    (is (= "article" (.tagName main)))
    (is (re-find #"Main content" (.text main)))))

(deftest finds-main-element
  (let [doc (Jsoup/parse "<body><div>Noise</div><main>The real content</main></body>")
        main (#'web/find-main-content doc)]
    (is (= "main" (.tagName main)))))

(deftest falls-back-to-body
  (let [doc (Jsoup/parse "<body><div>Just divs</div></body>")
        main (#'web/find-main-content doc)]
    (is (= "body" (.tagName main)))))

;; ---------------------------------------------------------------------------
;; Truncation
;; ---------------------------------------------------------------------------

(deftest truncate-short-text-unchanged
  (is (= "hello" (#'web/truncate-to-tokens "hello" 100))))

(deftest truncate-long-text
  (let [long-text (apply str (repeat 500 "word "))
        result (#'web/truncate-to-tokens long-text 100)]
    ;; 100 tokens * 4 chars = 400 chars max
    (is (<= (count result) 500))
    (is (re-find #"\[content truncated\]" result))))

;; ---------------------------------------------------------------------------
;; Link extraction
;; ---------------------------------------------------------------------------

(deftest extracts-links
  (let [doc   (Jsoup/parse "<body><a href=\"https://a.com\">A</a><a href=\"https://b.com\">B</a></body>"
                           "https://example.com")
        links (#'web/extract-links (.body doc))]
    (is (= 2 (count links)))
    (is (= "A" (:text (first links))))
    (is (= "https://a.com" (:url (first links))))))

(deftest skips-javascript-links
  (let [doc   (Jsoup/parse "<body><a href=\"javascript:void(0)\">Bad</a><a href=\"https://ok.com\">OK</a></body>"
                           "https://example.com")
        links (#'web/extract-links (.body doc))]
    (is (= 1 (count links)))
    (is (= "OK" (:text (first links))))))

;; ---------------------------------------------------------------------------
;; DDG URL extraction
;; ---------------------------------------------------------------------------

(deftest extract-ddg-url-test
  (testing "extracts url from uddg parameter"
    (is (= "https://clojure.org/"
           (#'web/extract-ddg-url "//duckduckgo.com/l/?uddg=https%3A%2F%2Fclojure.org%2F&rut=abc"))))
  (testing "handles protocol-relative URLs"
    (is (= "https://example.com"
           (#'web/extract-ddg-url "//example.com"))))
  (testing "returns nil for nil input"
    (is (nil? (#'web/extract-ddg-url nil))))
  (testing "returns plain URL as-is"
    (is (= "https://example.com/page"
           (#'web/extract-ddg-url "https://example.com/page")))))

;; ---------------------------------------------------------------------------
;; Web search
;; ---------------------------------------------------------------------------

(deftest search-web-parses-duckduckgo-html-results
  (with-redefs-fn {#'web/fetch-raw
                   (fn
                     ([url]
                      {:status 200
                       :headers {"content-type" "text/html"}
                       :body "<html><body>
                              <div class='result'>
                                <a class='result__a' href='//duckduckgo.com/l/?uddg=https%3A%2F%2Fclojure.org%2F&rut=abc'>Clojure</a>
                                <a class='result__snippet'>Functional Lisp for the JVM</a>
                              </div>
                              </body></html>"
                       :final-url url})
                     ([url _headers]
                      (is (= "https://html.duckduckgo.com/html/?q=clojure" url))
                      {:status 200
                       :headers {"content-type" "text/html"}
                       :body "<html><body>
                              <div class='result'>
                                <a class='result__a' href='//duckduckgo.com/l/?uddg=https%3A%2F%2Fclojure.org%2F&rut=abc'>Clojure</a>
                                <a class='result__snippet'>Functional Lisp for the JVM</a>
                              </div>
                              </body></html>"
                       :final-url url}))}
    #(let [result (web/search-web "clojure" :backend :duckduckgo-html)]
       (is (true? (:success? result)))
       (is (= "duckduckgo-html" (:backend result)))
       (is (= [{:title "Clojure"
                :url "https://clojure.org/"
                :snippet "Functional Lisp for the JVM"}]
              (:results result)))
       (is (nil? (:error result))))))

(deftest search-web-ddg-health-check-errors-on-parser-mismatch
  (with-redefs-fn {#'web/fetch-raw
                   (fn
                     ([url]
                      {:status 200
                       :headers {"content-type" "text/html"}
                       :body "<html><body><div class='unexpected'>markup changed</div></body></html>"
                       :final-url url})
                     ([url _headers]
                      {:status 200
                       :headers {"content-type" "text/html"}
                       :body "<html><body><div class='unexpected'>markup changed</div></body></html>"
                       :final-url url}))}
    #(let [result (web/search-web "clojure" :backend :duckduckgo-html)]
       (is (false? (:success? result)))
       (is (nil? (:results result)))
       (is (= "duckduckgo-html" (:backend result)))
       (is (= [{:backend "duckduckgo-html"
                :error "DuckDuckGo HTML markup changed; search results could not be parsed"}]
              (:failures result)))
       (is (re-find #"markup changed" (:error result))))))

(deftest search-web-ddg-allows-empty-no-results-pages
  (with-redefs-fn {#'web/fetch-raw
                   (fn
                     ([url]
                      {:status 200
                       :headers {"content-type" "text/html"}
                       :body "<html><body><div>No results found for test query</div></body></html>"
                       :final-url url})
                     ([url _headers]
                      {:status 200
                       :headers {"content-type" "text/html"}
                       :body "<html><body><div>No results found for test query</div></body></html>"
                       :final-url url}))}
    #(let [result (web/search-web "test query" :backend :duckduckgo-html)]
       (is (true? (:success? result)))
       (is (= [] (:results result)))
       (is (= "duckduckgo-html" (:backend result)))
       (is (nil? (:error result))))))

(deftest search-web-falls-back-to-searxng-json
  (let [calls (atom [])
        response-for-url
        (fn [url]
          (swap! calls conj url)
          (cond
            (str/includes? url "html.duckduckgo.com")
            {:status 200
             :headers {"content-type" "text/html"}
             :body "<html><body><div class='unexpected'>markup changed</div></body></html>"
             :final-url url}

            (str/includes? url "search.example/search")
            {:status 200
             :headers {"content-type" "application/json"}
             :body (json/write-json-str
                    {"results" [{"title" "Clojure"
                                 "url" "https://clojure.org/"
                                 "content" "A dynamic Lisp."}]})
             :final-url url}))]
    (with-redefs-fn {#'web/configured-search-backend-raw (constantly nil)
                     #'web/configured-searxng-url (constantly "https://search.example/search")
                     #'web/fetch-raw
                     (fn
                       ([url]
                        (response-for-url url))
                       ([url _headers]
                        (response-for-url url)))}
      #(let [result (web/search-web "clojure")]
         (is (true? (:success? result)))
         (is (= ["https://html.duckduckgo.com/html/?q=clojure"
                 "https://search.example/search?q=clojure&format=json"]
                @calls))
         (is (= "searxng-json" (:backend result)))
         (is (= [{:title "Clojure"
                  :url "https://clojure.org/"
                  :snippet "A dynamic Lisp."}]
                (:results result)))
         (is (= [{:backend "duckduckgo-html"
                  :error "DuckDuckGo HTML markup changed; search results could not be parsed"}]
                (:fallbacks result)))))))

;; ---------------------------------------------------------------------------
;; Integration: fetch-page (live HTTP)
;; ---------------------------------------------------------------------------

(deftest fetch-page-sets-success-flag-on-success
  (with-redefs-fn {#'web/fetch-raw
                   (fn [_url]
                     {:status 200
                      :headers {"content-type" "text/html"}
                      :body "<html><head><title>Example</title></head><body><main><p>Hello</p></main></body></html>"
                      :final-url "https://example.com"})}
    #(let [result (web/fetch-page "https://example.com")]
       (is (true? (:success? result)))
       (is (= "Example" (:title result)))
       (is (re-find #"Hello" (:content result)))
       (is (nil? (:error result))))))

(deftest ^:integration fetch-page-example-com
  (let [result (web/fetch-page "https://example.com")]
    (is (true? (:success? result)))
    (is (nil? (:error result)))
    (is (string? (:title result)))
    (is (string? (:content result)))
    (is (vector? (:links result)))))

(deftest fetch-page-returns-error-for-bad-url
  (let [result (web/fetch-page "http://localhost:1/nope")]
    (is (false? (:success? result)))
    (is (some? (:error result)))))

(deftest fetch-page-blocks-file-scheme
  (let [result (web/fetch-page "file:///etc/passwd")]
    (is (false? (:success? result)))
    (is (some? (:error result)))
    (is (re-find #"Only http" (:error result)))))

(deftest fetch-raw-follows-redirects-hop-by-hop
  (let [calls (atom [])]
    (with-redefs-fn {#'web/resolve-host-addresses
                     (fn [host]
                       (case host
                         "example.com"
                         [(InetAddress/getByAddress "example.com" (byte-array [(byte 93) (byte -72) (byte 34) (byte 20)]))]
                         "next.example"
                         [(InetAddress/getByAddress "next.example" (byte-array [(byte 93) (byte -72) (byte 34) (byte 21)]))]))
                     #'web/fetch-url!
                     (fn [url _headers resolution]
                       (swap! calls conj {:url url
                                          :host (:host resolution)
                                          :addresses (mapv #(.getHostAddress ^InetAddress %)
                                                           (:addresses resolution))})
                       (if (= url "https://example.com/start")
                         {:status 302
                          :headers {"location" "https://next.example/next"}
                          :body ""}
                         {:status 200
                          :headers {"content-type" "text/plain"}
                          :body "ok"}))}
      #(let [result (#'web/fetch-raw "https://example.com/start")]
         (is (= [{:url "https://example.com/start"
                  :host "example.com"
                  :addresses ["93.184.34.20"]}
                 {:url "https://next.example/next"
                  :host "next.example"
                  :addresses ["93.184.34.21"]}]
                @calls))
         (is (= "https://next.example/next" (:final-url result)))
         (is (= 200 (:status result)))
         (is (= "ok" (:body result)))))))

(deftest fetch-raw-blocks-private-redirect-target
  (with-redefs-fn {#'web/resolve-host-addresses
                   (fn [host]
                     (case host
                       "public.example"
                       [(InetAddress/getByAddress "public.example" (byte-array [(byte 93) (byte -72) (byte 34) (byte 20)]))]
                       "127.0.0.1"
                       [(InetAddress/getByAddress "127.0.0.1" (byte-array [(byte 127) (byte 0) (byte 0) (byte 1)]))]))
                   #'web/fetch-url!
                   (fn [url _headers _resolution]
                     (if (= url "https://public.example/start")
                       {:status 302
                        :headers {"location" "http://127.0.0.1/secret"}
                        :body ""}
                       (throw (ex-info "unexpected follow-up request" {:url url}))))}
    #(is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"private/internal"
           (#'web/fetch-raw "https://public.example/start")))))

;; ---------------------------------------------------------------------------
;; Integration: extract-data (live HTTP)
;; ---------------------------------------------------------------------------

(deftest ^:integration extract-data-example-com
  (let [result (web/extract-data "https://example.com"
                 {"headings" "h1"
                  "links"    "a[href]"})]
    (is (true? (:success? result)))
    (is (nil? (:error result)))
    (is (map? (:data result)))
    (is (vector? (get-in result [:data "headings"])))))
