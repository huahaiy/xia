(ns xia.web
  "Web browsing capabilities for tool handlers.

   Provides three functions exposed to the SCI sandbox:
     fetch-page   — fetch a URL, return readable text content
     search-web   — web search, return structured results
     extract-data — fetch a URL, extract elements by CSS selectors

   All HTTP requests go through this module (not raw hato) so we can
   enforce SSRF protection, rate limiting, and content size limits."
  (:require [clojure.string :as str]
            [charred.api :as json]
            [xia.db :as db]
            [xia.rate-limit :as rate-limit]
            [xia.ssrf :as ssrf])
  (:import [org.jsoup Jsoup]
           [org.jsoup.nodes Document Element TextNode]
           [org.jsoup.select Elements]
           [java.io BufferedInputStream ByteArrayOutputStream IOException OutputStreamWriter]
           [java.net InetAddress InetSocketAddress Socket URI URLEncoder URLDecoder]
           [java.nio.charset Charset StandardCharsets]
           [java.util.concurrent ConcurrentHashMap]
           [javax.net.ssl SNIHostName SSLParameters SSLSocket SSLSocketFactory]))

(def ^:private user-agent "Xia/0.1 (personal AI assistant)")
(def ^:private max-body-bytes (* 1 1024 1024)) ; 1 MB
(def ^:private max-redirects 5)
(def ^:private connect-timeout-ms 10000)
(def ^:private request-timeout-ms 120000)
(def ^:private default-search-backend :duckduckgo-html)
(def ^:private supported-search-backends
  {:ddg              :duckduckgo-html
   :duckduckgo       :duckduckgo-html
   :duckduckgo-html  :duckduckgo-html
   :searx            :searxng-json
   :searxng          :searxng-json
   :searxng-json     :searxng-json})
(def ^:private ddg-no-results-pattern
  #"(?i)\bno results\b|\bno more results\b")
(def ^:private ddg-blocked-pattern
  #"(?i)automated traffic|unusual traffic|captcha|verify you are human")

;; ---------------------------------------------------------------------------
;; SSRF protection
;; ---------------------------------------------------------------------------

(defn- resolve-host-addresses
  [host]
  (ssrf/resolve-host-addresses host))

(defn- resolve-url!
  [url]
  (ssrf/resolve-public-url! resolve-host-addresses url))

;; ---------------------------------------------------------------------------
;; Rate limiting — simple per-domain token bucket
;; ---------------------------------------------------------------------------

(defonce ^ConcurrentHashMap ^:private rate-limits (ConcurrentHashMap.))

(def ^:private rate-limit-max 10)         ; max requests
(def ^:private rate-limit-window-ms 60000) ; per minute

(defn- check-rate-limit!
  "Enforce per-domain rate limiting. Throws if limit exceeded."
  [url]
  (let [host (.getHost (URI. url))
        now  (System/currentTimeMillis)
        state (.computeIfAbsent rate-limits host
                (reify java.util.function.Function
                  (apply [_ _] (atom {:timestamps [] :cleaned now}))))]
    (rate-limit/consume-slot!
      state
      now
      rate-limit-window-ms
      rate-limit-max
      (fn []
        (ex-info (str "Rate limit exceeded for " host
                      " (max " rate-limit-max " requests/minute)")
                 {:host host})))))

;; ---------------------------------------------------------------------------
;; HTTP fetch
;; ---------------------------------------------------------------------------

(defn- response-charset
  [headers]
  (let [content-type (get headers "content-type")]
    (try
      (if-let [[_ charset] (some->> content-type
                                    (re-find #"(?i)(?:^|;)\s*charset=([^;]+)"))]
        (Charset/forName (str/trim charset))
        StandardCharsets/UTF_8)
      (catch Exception _
        StandardCharsets/UTF_8))))

(defn- decode-body
  [body-bytes headers]
  (String. ^bytes body-bytes ^Charset (response-charset headers)))

(defn- read-body-bytes!
  [body-bytes url]
  (let [body-bytes (or body-bytes (byte-array 0))
        size       (alength ^bytes body-bytes)]
    (when (> size max-body-bytes)
      (throw (ex-info "Response too large"
                      {:url   url
                       :size  size
                       :limit max-body-bytes})))
    body-bytes))

(defn- default-port
  [^URI uri]
  (case (.getScheme uri)
    "https" 443
    80))

(defn- effective-port
  [^URI uri]
  (let [port (.getPort uri)]
    (if (neg? port)
      (default-port uri)
      port)))

(defn- request-target
  [^URI uri]
  (let [path  (or (.getRawPath uri) "")
        query (.getRawQuery uri)]
    (str (if (seq path) path "/")
         (when (seq query)
           (str "?" query)))))

(defn- host-header
  [^URI uri host]
  (let [port          (effective-port uri)
        default-port? (= port (default-port uri))]
    (if default-port?
      host
      (str host ":" port))))

(defn- read-line!
  [^BufferedInputStream in]
  (let [out (ByteArrayOutputStream.)]
    (loop [prev -1]
      (let [b (.read in)]
        (cond
          (= b -1)
          (when (or (pos? (.size out)) (not= prev -1))
            (.toString out "ISO-8859-1"))

          (and (= prev 13) (= b 10))
          (let [line-bytes (.toByteArray out)
                line-size  (alength ^bytes line-bytes)]
            (String. ^bytes line-bytes 0 (max 0 (dec line-size)) StandardCharsets/ISO_8859_1))

          :else
          (do
            (.write out b)
            (recur b)))))))

(defn- header-body-allowed?
  [status]
  (not (or (<= 100 status 199)
           (= 204 status)
           (= 304 status))))

(defn- read-exactly!
  [^BufferedInputStream in size url]
  (let [limit (long size)]
    (when (> limit max-body-bytes)
      (throw (ex-info "Response too large"
                      {:url   url
                       :size  limit
                       :limit max-body-bytes})))
    (let [buffer (byte-array (int limit))]
      (loop [offset 0]
        (if (= offset limit)
          buffer
          (let [read-count (.read in buffer offset (int (- limit offset)))]
            (when (neg? read-count)
              (throw (ex-info "Unexpected end of response body"
                              {:url url
                               :expected-bytes limit
                               :received-bytes offset})))
            (recur (+ offset read-count))))))))

(defn- read-until-eof!
  [^BufferedInputStream in url]
  (let [buffer (byte-array 8192)
        out    (ByteArrayOutputStream.)]
    (loop [total 0]
      (let [read-count (.read in buffer)]
        (if (neg? read-count)
          (.toByteArray out)
          (let [next-total (+ total read-count)]
            (when (> next-total max-body-bytes)
              (throw (ex-info "Response too large"
                              {:url   url
                               :size  next-total
                               :limit max-body-bytes})))
            (.write out buffer 0 read-count)
            (recur next-total)))))))

(defn- read-chunked-body!
  [^BufferedInputStream in url]
  (let [buffer (byte-array 8192)
        out    (ByteArrayOutputStream.)]
    (loop [total 0]
      (let [line (some-> (read-line! in) str/trim)]
        (when-not (some? line)
          (throw (ex-info "Unexpected end of chunked response"
                          {:url url})))
        (let [chunk-size (Long/parseLong (first (str/split line #";")) 16)]
          (if (zero? chunk-size)
            (do
              (loop []
                (let [trailer (read-line! in)]
                  (when (and trailer (not (str/blank? trailer)))
                    (recur))))
              (.toByteArray out))
            (let [next-total (+ total chunk-size)]
              (when (> next-total max-body-bytes)
                (throw (ex-info "Response too large"
                                {:url   url
                                 :size  next-total
                                 :limit max-body-bytes})))
              (loop [remaining chunk-size]
                (when (pos? remaining)
                  (let [read-count (.read in buffer 0 (int (min remaining (alength buffer))))]
                    (when (neg? read-count)
                      (throw (ex-info "Unexpected end of chunked response body"
                                      {:url url
                                       :remaining-bytes remaining})))
                    (.write out buffer 0 read-count)
                    (recur (- remaining read-count)))))
              (let [chunk-end (read-line! in)]
                (when-not (string? chunk-end)
                  (throw (ex-info "Malformed chunked response terminator"
                                  {:url url}))))
              (recur next-total))))))))

(defn- parse-status-line
  [status-line url]
  (let [[_ status reason] (re-matches #"HTTP/\d+(?:\.\d+)?\s+(\d{3})(?:\s+(.*))?" (or status-line ""))]
    (when-not status
      (throw (ex-info "Malformed HTTP response status line"
                      {:url url
                       :status-line status-line})))
    {:status (Long/parseLong status)
     :reason (or reason "")}))

(defn- read-response!
  [^BufferedInputStream in url]
  (let [status-line        (read-line! in)
        {:keys [status]}   (parse-status-line status-line url)
        headers            (loop [acc {}]
                             (let [line (read-line! in)]
                               (cond
                                 (nil? line) acc
                                 (str/blank? line) acc
                                 :else
                                 (let [[name value] (str/split line #":\s*" 2)
                                       header-name  (str/lower-case name)
                                       header-value (or value "")]
                                   (recur (update acc header-name
                                                  (fn [existing]
                                                    (if (seq existing)
                                                      (str existing "," header-value)
                                                      header-value))))))))
        transfer-encoding  (some-> (get headers "transfer-encoding") str/lower-case)
        content-length     (some-> (get headers "content-length")
                                   str/trim
                                   Long/parseLong)
        body-bytes         (cond
                             (not (header-body-allowed? status))
                             (byte-array 0)

                             (and transfer-encoding
                                  (str/includes? transfer-encoding "chunked"))
                             (read-chunked-body! in url)

                             (some? content-length)
                             (read-exactly! in content-length url)

                             :else
                             (read-until-eof! in url))]
    {:status  status
     :headers headers
     :body    (decode-body (read-body-bytes! body-bytes url) headers)}))

(defn- open-plain-socket!
  [^InetAddress address port]
  (doto (Socket.)
    (.connect (InetSocketAddress. address (int port)) (int connect-timeout-ms))
    (.setSoTimeout (int request-timeout-ms))))

(defn- open-ssl-socket!
  [^Socket plain-socket ^String host port]
  (let [factory ^SSLSocketFactory (SSLSocketFactory/getDefault)
        ^SSLSocket socket         (.createSocket factory plain-socket host (int port) true)
        ^SSLParameters params     (.getSSLParameters socket)]
    (.setEndpointIdentificationAlgorithm params "HTTPS")
    (.setServerNames params (java.util.Collections/singletonList (SNIHostName. host)))
    (.setSSLParameters socket params)
    (.setSoTimeout socket (int request-timeout-ms))
    (.startHandshake socket)
    socket))

(defn- request-socket!
  [resolution ^InetAddress address]
  (let [^URI uri     (:uri resolution)
        host         (:host resolution)
        port         (effective-port uri)
        plain-socket (open-plain-socket! address port)]
    (if (= "https" (.getScheme uri))
      (open-ssl-socket! plain-socket host port)
      plain-socket)))

(defn- write-request!
  [^Socket socket resolution headers]
  (let [^URI uri (:uri resolution)
        request-headers (merge {"Host"            (host-header uri (:host resolution))
                                "Connection"      "close"
                                "Accept-Encoding" "identity"}
                               headers)
        writer (OutputStreamWriter. (.getOutputStream socket) StandardCharsets/ISO_8859_1)]
    ;; Do not close the writer here; it would close the underlying socket
    ;; before we can read the response body.
    (.write writer (str "GET " (request-target uri) " HTTP/1.1\r\n"))
    (doseq [[name value] request-headers]
      (.write writer (str name ": " value "\r\n")))
    (.write writer "\r\n")
    (.flush writer)))

(defn- fetch-address!
  [url headers resolution ^InetAddress address]
  (with-open [^Socket socket (request-socket! resolution address)
              in     (BufferedInputStream. (.getInputStream socket))]
    (write-request! socket resolution headers)
    (read-response! in url)))

(defn- fetch-url!
  [url headers resolution]
  (let [addresses (:addresses resolution)]
    (loop [[^InetAddress address & more] addresses
           last-error nil]
      (if-not address
        (throw (or last-error
                   (ex-info "Host did not resolve to any addresses"
                            {:url  url
                             :host (:host resolution)})))
        (let [{:keys [response error]}
              (try
                {:response (fetch-address! url headers resolution address)}
                (catch IOException e
                  {:error e}))]
          (if response
            response
            (if (seq more)
              (recur more error)
              (throw error))))))))

(defn- fetch-raw
  "Fetch a URL, return {:status :headers :body :final-url}."
  ([url]
   (fetch-raw url {}))
  ([url extra-headers]
   (loop [current-url url
          redirects   0]
     (let [resolution (resolve-url! current-url)]
       (check-rate-limit! current-url)
       (let [resp (fetch-url! current-url
                              (merge {"User-Agent" user-agent
                                      "Accept"     "text/html,application/xhtml+xml,*/*"}
                                     extra-headers)
                              resolution)
             status (:status resp)
             location (get-in resp [:headers "location"])]
         (if (and (#{301 302 303 307 308} status) (seq location))
           (do
             (when (>= redirects max-redirects)
               (throw (ex-info "Too many redirects"
                               {:url current-url :redirects redirects})))
             (let [next-url (str (.resolve (URI. ^String current-url) ^String location))]
               (recur next-url (inc redirects))))
           {:status    status
            :headers   (:headers resp)
            :body      (str (:body resp))
            :final-url current-url}))))))

;; ---------------------------------------------------------------------------
;; HTML → readable text conversion
;; ---------------------------------------------------------------------------

(def ^:private noise-selectors
  "CSS selectors for elements to remove before extraction."
  (str/join ", "
    ["script" "style" "noscript" "iframe"
     "nav" "footer" ".nav" ".footer" ".sidebar" ".menu"
     ".ad" ".ads" ".advert" ".advertisement" ".banner"
     ".cookie-banner" ".popup" ".modal"
     "[role=navigation]" "[role=banner]" "[aria-hidden=true]"]))

(def ^:private main-content-selectors
  "CSS selectors to try for main content, in priority order."
  ["article" "main" "[role=main]"
   "#content" "#main-content" "#article"
   ".post-content" ".article-content" ".article-body"
   ".entry-content" ".post-body" ".content"])

(defn- find-main-content
  "Find the main content element using readability heuristics."
  ^Element [^Document doc]
  (or
    ;; Try known content selectors
    (some (fn [sel]
            (let [^Elements els (.select doc ^String sel)]
              (when (pos? (.size els))
                (.first els))))
          main-content-selectors)
    ;; Fallback: body
    (.body doc)))

(defn- element->markdown
  "Convert a Jsoup Element to compact markdown-like text."
  [^Element elem include-links?]
  (let [^StringBuilder sb (StringBuilder.)]
    (letfn [(walk [^Element el depth]
              (let [tag (str/lower-case (.tagName el))]
                (case tag
                  ;; Headings
                  ("h1" "h2" "h3" "h4" "h5" "h6")
                  (let [level (- (int (second tag)) (int \0))]
                    (when (pos? (.length sb)) (.append sb "\n\n"))
                    (.append sb (apply str (repeat level "#")))
                    (.append sb " ")
                    (.append sb (str/trim (.text el)))
                    (.append sb "\n"))

                  ;; Paragraphs
                  "p"
                  (let [text (str/trim (.text el))]
                    (when (seq text)
                      (when (pos? (.length sb)) (.append sb "\n\n"))
                      (walk-children el depth)))

                  ;; Lists
                  ("ul" "ol")
                  (do (when (pos? (.length sb)) (.append sb "\n"))
                      (let [^Elements items (.select el "> li")]
                        (dotimes [i (.size items)]
                          (let [^Element li (.get items i)
                                prefix (if (= tag "ol")
                                         (str (inc i) ". ")
                                         "- ")]
                            (.append sb "\n")
                            (.append sb prefix)
                            (.append sb (str/trim (.text li)))))))

                  ;; List items handled by parent ul/ol
                  "li" nil

                  ;; Links
                  "a"
                  (let [text (str/trim (.text el))
                        href (.absUrl el "href")]
                    (when (seq text)
                      (if (and include-links? (seq href)
                               (not (str/starts-with? href "javascript:")))
                        (do (.append sb "[") (.append sb text) (.append sb "](") (.append sb href) (.append sb ")"))
                        (.append sb text))))

                  ;; Code blocks
                  "pre"
                  (do (when (pos? (.length sb)) (.append sb "\n\n"))
                      (.append sb "```\n")
                      (.append sb (str/trim (.text el)))
                      (.append sb "\n```"))

                  ;; Inline code
                  "code"
                  (when-not (= "pre" (some-> (.parent el) .tagName str/lower-case))
                    (.append sb "`")
                    (.append sb (.text el))
                    (.append sb "`"))

                  ;; Blockquote
                  "blockquote"
                  (do (when (pos? (.length sb)) (.append sb "\n\n"))
                      (doseq [line (str/split-lines (str/trim (.text el)))]
                        (.append sb "> ")
                        (.append sb line)
                        (.append sb "\n")))

                  ;; Images
                  "img"
                  (let [alt (.attr el "alt")]
                    (when (seq alt)
                      (.append sb (str "[image: " alt "]"))))

                  ;; Tables
                  "table"
                  (do (when (pos? (.length sb)) (.append sb "\n\n"))
                      (let [^Elements rows (.select el "tr")]
                        (dotimes [r (.size rows)]
                          (let [^Element row   (.get rows r)
                                ^Elements cells (.select row "td, th")]
                            (when (pos? (.size cells))
                              (.append sb "| ")
                              (dotimes [c (.size cells)]
                                (when (pos? c) (.append sb " | "))
                                (.append sb (str/trim (.text ^Element (.get cells c)))))
                              (.append sb " |\n")
                              ;; Header separator after first row
                              (when (and (zero? r) (pos? (.size (.select row "th"))))
                                (.append sb "|")
                                (dotimes [_ (.size cells)]
                                  (.append sb " --- |"))
                                (.append sb "\n")))))))

                  ;; Line break
                  "br"
                  (.append sb "\n")

                  ;; Horizontal rule
                  "hr"
                  (do (when (pos? (.length sb)) (.append sb "\n\n"))
                      (.append sb "---\n"))

                  ;; Div, section, article — recurse
                  ("div" "section" "article" "span" "em" "strong"
                   "b" "i" "mark" "small" "sub" "sup" "body"
                   "header" "figure" "figcaption" "details" "summary"
                   "dl" "dt" "dd" "time" "abbr")
                  (walk-children el depth)

                  ;; Everything else — just get the text
                  (let [text (.ownText el)]
                    (when (seq text)
                      (.append sb text))
                    (walk-children el depth)))))

            (walk-children [^Element el depth]
              (doseq [child (.childNodes el)]
                (cond
                  (instance? Element child)
                  (walk child (inc depth))

                  (instance? TextNode child)
                  (let [text (.getWholeText ^TextNode child)]
                    (when-not (str/blank? text)
                      (.append sb (str/trim text))
                      (.append sb " "))))))]

      (walk elem 0))
    ;; Clean up
    (-> (str sb)
        (str/replace #"\n{3,}" "\n\n")
        (str/replace #" {2,}" " ")
        str/trim)))

(defn- extract-links
  "Extract links from the main content element."
  [^Element elem]
  (->> (.select elem "a[href]")
       (map (fn [^Element a]
              (let [text (str/trim (.text a))
                    href (.absUrl a "href")]
                (when (and (seq text) (seq href)
                           (not (str/starts-with? href "javascript:")))
                  {:text text :url href}))))
       (filter some?)
       (distinct)
       vec))

(defn- truncate-to-tokens
  "Truncate text to approximately max-tokens (4 chars/token estimate)."
  [^String text max-tokens]
  (let [max-chars (* max-tokens 4)]
    (if (<= (count text) max-chars)
      text
      (let [truncated (subs text 0 max-chars)
            ;; Cut at last paragraph or sentence boundary
            last-para (str/last-index-of truncated "\n\n")
            last-sent (str/last-index-of truncated ". ")
            cut-point (cond
                        (and last-para (> last-para (* max-chars 0.5))) last-para
                        (and last-sent (> last-sent (* max-chars 0.5))) (+ last-sent 1)
                        :else max-chars)]
        (str (subs truncated 0 cut-point) "\n\n[content truncated]")))))

;; ---------------------------------------------------------------------------
;; Public API — exposed to SCI sandbox
;; ---------------------------------------------------------------------------

(defn fetch-page
  "Fetch a web page and return readable content.

   Arguments:
     url — the URL to fetch

   Options:
     :max-tokens    — max content size in estimated tokens (default 2000)
     :include-links — include link URLs in output (default true)

   Returns:
     {:success?   true
      :url        \"https://...\"
      :title      \"Page Title\"
      :content    \"Readable markdown-like text\"
      :links      [{:text \"Link\" :url \"https://...\"}]
      :truncated? false}"
  [url & {:keys [max-tokens include-links]
          :or   {max-tokens 2000 include-links true}}]
  (try
    (let [{:keys [status body final-url headers]} (fetch-raw url)]
      (when-not (<= 200 status 299)
        (throw (ex-info (str "HTTP " status) {:url url :status status})))
      (let [content-type (or (get headers "content-type")
                            (get headers "Content-Type")
                            "")]
        (if (or (str/includes? content-type "text/html")
                (str/includes? content-type "application/xhtml"))
          ;; HTML response — parse and extract
          (let [^String base-url (or final-url url)
                ^Document doc    (Jsoup/parse ^String body ^String base-url)
                _      (doseq [^Element el (.select doc ^String noise-selectors)]
                         (.remove el))
                ^Element main   (find-main-content doc)
                title  (.title doc)
                raw    (element->markdown main include-links)
                content (truncate-to-tokens raw max-tokens)
                links  (when include-links (extract-links main))]
            {:success?   true
             :url        (or final-url url)
             :title      title
             :content    content
             :links      (or links [])
             :truncated? (not= raw content)})
          ;; Non-HTML — return raw text truncated
          (let [content (truncate-to-tokens (or body "") max-tokens)]
            {:success?   true
             :url        (or final-url url)
             :title      nil
             :content    content
             :links      []
             :truncated? (not= body content)}))))
    (catch Exception e
      {:success? false
       :url      url
       :error    (.getMessage e)})))

(defn- extract-ddg-url
  "Extract the actual target URL from a DuckDuckGo redirect href.
   DDG links look like //duckduckgo.com/l/?uddg=<url-encoded>&rut=..."
  [^String href]
  (when (seq href)
    (if (str/includes? href "uddg=")
      (let [start (+ (str/index-of href "uddg=") 5)
            end   (let [amp (str/index-of href "&" start)]
                    (if amp amp (count href)))]
        (URLDecoder/decode ^String (subs href start end) "UTF-8"))
      ;; Not a redirect — return as-is, normalizing protocol-relative URLs
      (if (str/starts-with? href "//")
        (str "https:" href)
        href))))

(defn- safe-get-config
  [config-key]
  (try
    (db/get-config config-key)
    (catch Exception _
      nil)))

(defn- normalize-search-backend
  [backend]
  (when backend
    (let [value (cond
                  (keyword? backend) backend
                  (string? backend)  (-> backend
                                         str/trim
                                         str/lower-case
                                         (str/replace #"[_\s]+" "-")
                                         keyword)
                  :else nil)]
      (get supported-search-backends value))))

(defn- configured-search-backend-raw []
  (safe-get-config :web/search-backend))

(defn- configured-searxng-url []
  (let [value (some-> (safe-get-config :web/search-searxng-url)
                      str
                      str/trim)]
    (when (seq value)
      value)))

(defn- resolve-search-backend!
  [backend]
  (or (normalize-search-backend backend)
      (throw (ex-info (str "Unsupported search backend: " backend)
                      {:backend backend
                       :supported-backends (mapv name (distinct (vals supported-search-backends)))}))))

(defn- append-query-params
  [base-url params]
  (let [query-string (->> params
                          (map (fn [[k v]]
                                 (str (URLEncoder/encode ^String (name k) "UTF-8")
                                      "="
                                      (URLEncoder/encode ^String (str v) "UTF-8"))))
                          (str/join "&"))
        separator    (cond
                       (str/ends-with? base-url "?") ""
                       (str/ends-with? base-url "&") ""
                       (str/includes? base-url "?") "&"
                       :else "?")]
    (str base-url separator query-string)))

(defn- select-first-any
  [^Element root selectors]
  (some (fn [selector]
          (.selectFirst root ^String selector))
        selectors))

(defn- fetch-search-body!
  [url headers]
  (let [{:keys [status body]} (fetch-raw url headers)]
    (when-not (<= 200 status 299)
      (throw (ex-info (str "Search backend HTTP " status)
                      {:type :search/backend-http
                       :url url
                       :status status})))
    (or body "")))

(defn- parse-ddg-html-result
  [^Element result-el]
  (let [^Element title-el (select-first-any result-el
                                   ["a.result__a"
                                    ".result__title a"
                                    "h2 a"
                                    "a[href]"])
        ^Element snip-el  (select-first-any result-el
                                   [".result__snippet"
                                    ".result__body"
                                    ".result__extras__snippet"
                                    ".result__content"])
        title    (some-> title-el .text str/trim)
        raw-href (some-> title-el (.attr "href"))
        url      (some-> raw-href extract-ddg-url str/trim)
        snippet  (some-> snip-el .text str/trim)]
    (when (and (seq title) (seq url))
      {:title title
       :url url
       :snippet (or snippet "")})))

(defn- parse-ddg-html-results
  [body max-results]
  (let [^Document doc          (Jsoup/parse ^String body)
        ^Elements result-nodes (.select doc ".result, .results_links, .web-result")
        parsed       (->> result-nodes
                          (map parse-ddg-html-result)
                          (filter some?)
                          distinct
                          (take max-results)
                          vec)
        page-text    (some-> doc .body .text)]
    (cond
      (seq parsed)
      parsed

      (and page-text (re-find ddg-no-results-pattern page-text))
      []

      (and page-text (re-find ddg-blocked-pattern page-text))
      (throw (ex-info "DuckDuckGo blocked or challenged the search request"
                      {:type :search/backend-blocked
                       :backend :duckduckgo-html}))

      :else
      (throw (ex-info "DuckDuckGo HTML markup changed; search results could not be parsed"
                      {:type :search/backend-unhealthy
                       :backend :duckduckgo-html})))))

(defn- parse-searxng-results
  [body max-results]
  (let [parsed  (json/read-json body)
        results (get parsed "results")]
    (when-not (sequential? results)
      (throw (ex-info "SearxNG JSON response did not include a results array"
                      {:type :search/backend-unhealthy
                       :backend :searxng-json})))
    (->> results
         (map (fn [result]
                {:title   (or (get result "title") "")
                 :url     (or (get result "url") "")
                 :snippet (or (get result "content")
                              (get result "snippet")
                              "")}))
         (filter (fn [{:keys [title url]}]
                   (or (seq title) (seq url))))
         (take max-results)
         vec)))

(defn- search-via-backend!
  [backend query max-results]
  (case backend
    :duckduckgo-html
    (let [url  (str "https://html.duckduckgo.com/html/?q="
                    (URLEncoder/encode ^String (str query) "UTF-8"))
          body (fetch-search-body! url {"Accept" "text/html,application/xhtml+xml,*/*"})]
      (parse-ddg-html-results body max-results))

    :searxng-json
    (let [base-url (or (configured-searxng-url)
                       (throw (ex-info "SearxNG search backend is not configured"
                                       {:type :search/backend-unconfigured
                                        :backend :searxng-json
                                        :config-key :web/search-searxng-url})))
          url      (append-query-params base-url {:q query :format "json"})
          body     (fetch-search-body! url {"Accept" "application/json,text/json,*/*"})]
      (parse-searxng-results body max-results))

    (throw (ex-info (str "Unsupported search backend: " backend)
                    {:backend backend}))))

(defn- search-backend-order
  [backend]
  (if backend
    [(resolve-search-backend! backend)]
    (if-let [configured (configured-search-backend-raw)]
      [(resolve-search-backend! configured)]
      (cond-> [default-search-backend]
        (configured-searxng-url) (conj :searxng-json)))))

(defn search-web
  "Search the web. Returns structured results.

   Uses DuckDuckGo HTML by default, with parser health checks so markup changes
   fail loudly instead of silently returning empty results. The backend can be
   selected explicitly or configured via :web/search-backend. A configured
   public SearxNG JSON endpoint can be used via :web/search-searxng-url.

   Arguments:
     query — the search query

   Options:
     :max-results — max number of results (default 5)
     :backend     — one of :duckduckgo-html or :searxng-json

   Returns:
     {:success? true
      :query    \"search terms\"
      :backend  \"duckduckgo-html\"
      :results  [{:title \"...\" :url \"...\" :snippet \"...\"}]}

     On failure:
     {:success? false
      :query    \"search terms\"
      :error    \"...\"}"
  [query & {:keys [max-results backend]
            :or   {max-results 5}}]
  (try
    (loop [[backend-id & more] (search-backend-order backend)
           failures            []]
      (when-not backend-id
        (throw (ex-info "No search backend is available"
                        {:type :search/backend-unavailable
                         :failures failures})))
      (let [attempt (try
                      {:results (search-via-backend! backend-id query max-results)}
                      (catch Exception e
                        {:exception e}))]
        (if-let [results (:results attempt)]
          {:success?  true
           :query     query
           :backend   (name backend-id)
           :results   results
           :fallbacks (when (seq failures) failures)}
          (let [e       (:exception attempt)
                failure {:backend (name backend-id)
                         :error   (.getMessage ^Throwable e)}]
            (if (seq more)
              (recur more (conj failures failure))
              (throw (ex-info (.getMessage ^Throwable e)
                              {:type     (or (:type (ex-data e))
                                             :search/backend-failed)
                               :backend  (name backend-id)
                               :failures (conj failures failure)}
                              e)))))))
    (catch Exception e
      (cond-> {:success? false
               :query    query
               :error    (.getMessage ^Throwable e)}
        (:backend (ex-data e)) (assoc :backend (:backend (ex-data e)))
        (:failures (ex-data e)) (assoc :failures (:failures (ex-data e)))))))

(defn extract-data
  "Fetch a page and extract structured data using CSS selectors.

   Arguments:
     url       — the page URL
     selectors — map of output-key to CSS selector string

   Example:
     (extract-data \"https://example.com\"
       {\"headings\" \"h1, h2, h3\"
        \"links\"    \"a[href]\"
        \"prices\"   \".price\"})

   Returns:
     {:success? true
      :url      \"https://...\"
      :data     {\"headings\" [\"Title\" \"Subtitle\"]
                 \"links\"    [{:text \"...\" :href \"...\"}]
                 \"prices\"   [\"$19.99\"]}}"
  [url selectors]
  (try
    (let [{:keys [status body final-url]} (fetch-raw url)]
      (when-not (<= 200 status 299)
        (throw (ex-info (str "HTTP " status) {:url url :status status})))
      (let [^String base-url (or final-url url)
            ^Document doc  (Jsoup/parse ^String body ^String base-url)
            data (reduce-kv
                   (fn [m k sel]
                     (let [^Elements els (.select doc ^String sel)]
                       (assoc m k
                         (mapv (fn [^Element el]
                                 (if (= "a" (str/lower-case (.tagName el)))
                                   {:text (.text el) :href (.absUrl el "href")}
                                   (.text el)))
                               els))))
                   {}
                   selectors)]
        {:success? true
         :url      (or final-url url)
         :data     data}))
    (catch Exception e
      {:success? false
       :url      url
       :error    (.getMessage e)})))
