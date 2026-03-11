(ns xia.web-test
  (:require [clojure.test :refer :all]
            [xia.web :as web])
  (:import [java.net InetAddress]
           [org.jsoup Jsoup]
           [org.jsoup.nodes Document Element]))

;; ---------------------------------------------------------------------------
;; SSRF protection
;; ---------------------------------------------------------------------------

(deftest validate-url-blocks-private-ips
  (testing "blocks localhost"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"private/internal"
          (#'web/validate-url! "http://127.0.0.1/secret"))))
  (testing "blocks localhost hostname"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"private/internal"
          (#'web/validate-url! "http://localhost/secret")))))

(deftest validate-url-blocks-mixed-public-and-private-resolution
  (with-redefs-fn {#'web/resolve-host-addresses
                   (fn [_]
                     [(InetAddress/getByAddress "public.example" (byte-array [(byte 93) (byte -72) (byte 34) (byte 20)]))
                      (InetAddress/getByAddress "private.example" (byte-array [(byte 127) (byte 0) (byte 0) (byte 1)]))])}
    #(is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"private/internal"
           (#'web/validate-url! "https://mixed.example/path")))))

(deftest validate-url-blocks-bad-schemes
  (testing "blocks file://"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Only http"
          (#'web/validate-url! "file:///etc/passwd"))))
  (testing "blocks ftp://"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Only http"
          (#'web/validate-url! "ftp://example.com/file"))))
  (testing "blocks javascript:"
    (is (thrown-with-msg?
          Exception #"."
          (#'web/validate-url! "javascript:alert(1)")))))

(deftest validate-url-allows-public-urls
  (testing "allows https"
    (is (nil? (#'web/validate-url! "https://example.com"))))
  (testing "allows http"
    (is (nil? (#'web/validate-url! "http://example.com")))))

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
;; Integration: fetch-page (live HTTP)
;; ---------------------------------------------------------------------------

(deftest ^:integration fetch-page-example-com
  (let [result (web/fetch-page "https://example.com")]
    (is (nil? (:error result)))
    (is (string? (:title result)))
    (is (string? (:content result)))
    (is (vector? (:links result)))))

(deftest fetch-page-returns-error-for-bad-url
  (let [result (web/fetch-page "http://localhost:1/nope")]
    (is (some? (:error result)))))

(deftest fetch-page-blocks-file-scheme
  (let [result (web/fetch-page "file:///etc/passwd")]
    (is (some? (:error result)))
    (is (re-find #"Only http" (:error result)))))

(deftest fetch-raw-follows-redirects-hop-by-hop
  (let [calls (atom [])]
    (with-redefs-fn {#'web/resolve-host-addresses
                     (fn [_]
                       [(InetAddress/getByAddress "example.com" (byte-array [(byte 93) (byte -72) (byte 34) (byte 20)]))])
                     #'hato.client/request
                     (fn [{:keys [url]}]
                       (swap! calls conj url)
                       (if (= url "https://example.com/start")
                         {:status 302
                          :headers {"location" "/next"}
                          :body ""}
                         {:status 200
                          :headers {"content-type" "text/plain"}
                          :body "ok"}))}
      #(let [result (#'web/fetch-raw "https://example.com/start")]
         (is (= ["https://example.com/start" "https://example.com/next"] @calls))
         (is (= "https://example.com/next" (:final-url result)))
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
                   #'hato.client/request
                   (fn [{:keys [url]}]
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
    (is (nil? (:error result)))
    (is (map? (:data result)))
    (is (vector? (get-in result [:data "headings"])))))
