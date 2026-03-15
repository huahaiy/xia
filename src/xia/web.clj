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
            [xia.ssrf :as ssrf])
  (:import [org.jsoup Jsoup]
           [org.jsoup.nodes Document Element TextNode]
           [org.apache.http Header]
           [org.apache.http.client.config RequestConfig]
           [org.apache.http.client.methods RequestBuilder]
           [org.apache.http.impl.client HttpClientBuilder]
           [java.io ByteArrayOutputStream InputStream]
           [java.net URI]
           [java.nio.charset Charset StandardCharsets]
           [java.util.concurrent ConcurrentHashMap]))

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

(defonce ^:private rate-limits (ConcurrentHashMap.))

(def ^:private rate-limit-max 10)         ; max requests
(def ^:private rate-limit-window-ms 60000) ; per minute

(defn- consume-rate-limit-slot!
  [state now limit error-fn]
  (loop []
    (let [{:keys [timestamps] :as current} @state
          cutoff (- now rate-limit-window-ms)
          recent (filterv #(> % cutoff) timestamps)]
      (when (>= (count recent) limit)
        (throw (error-fn)))
      (let [updated {:timestamps (conj recent now)
                     :cleaned    now}]
        (when-not (compare-and-set! state current updated)
          (recur))))))

(defn- check-rate-limit!
  "Enforce per-domain rate limiting. Throws if limit exceeded."
  [url]
  (let [host (.getHost (URI. url))
        now  (System/currentTimeMillis)
        state (.computeIfAbsent rate-limits host
                (reify java.util.function.Function
                  (apply [_ _] (atom {:timestamps [] :cleaned now}))))]
    (consume-rate-limit-slot!
      state
      now
      rate-limit-max
      (fn []
        (ex-info (str "Rate limit exceeded for " host
                      " (max " rate-limit-max " requests/minute)")
                 {:host host})))))

;; ---------------------------------------------------------------------------
;; HTTP fetch
;; ---------------------------------------------------------------------------

(defn- normalize-headers
  [headers]
  (reduce (fn [m ^Header header]
            (let [header-name  (str/lower-case (.getName header))
                  header-value (.getValue header)]
              (update m header-name
                      (fn [existing]
                        (if existing
                          (str existing "," header-value)
                          header-value)))))
          {}
          headers))

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

(defn- read-entity-bytes!
  [entity url]
  (if-not entity
    (byte-array 0)
    (let [declared-size (.getContentLength entity)]
      (when (> declared-size max-body-bytes)
        (throw (ex-info "Response too large"
                        {:url   url
                         :size  declared-size
                         :limit max-body-bytes})))
      (with-open [^InputStream in (.getContent entity)
                  out            (ByteArrayOutputStream.)]
        (let [buffer (byte-array 8192)]
          (loop [total 0]
            (let [read-count (.read in buffer)]
              (if (neg? read-count)
                (.toByteArray out)
                (let [new-total (+ total read-count)]
                  (when (> new-total max-body-bytes)
                    (throw (ex-info "Response too large"
                                    {:url   url
                                     :size  new-total
                                     :limit max-body-bytes})))
                  (.write out buffer 0 read-count)
                  (recur new-total))))))))))

(defn- add-header!
  [^RequestBuilder builder header value]
  (if (sequential? value)
    (doseq [item value]
      (.addHeader builder (str header) (str item)))
    (.addHeader builder (str header) (str value)))
  builder)

(defn- fetch-url!
  [url headers resolution]
  (let [request-config (-> (RequestConfig/custom)
                           (.setConnectTimeout (int connect-timeout-ms))
                           (.setConnectionRequestTimeout (int request-timeout-ms))
                           (.setSocketTimeout (int request-timeout-ms))
                           .build)
        request-builder (doto (RequestBuilder/get url)
                          (.setConfig request-config))
        _               (doseq [[header value] headers]
                          (add-header! request-builder header value))
        dns-resolver    (ssrf/pinned-dns-resolver resolution)]
    (with-open [client   (-> (HttpClientBuilder/create)
                             (.disableAutomaticRetries)
                             (.disableRedirectHandling)
                             (.setDefaultRequestConfig request-config)
                             (.setDnsResolver dns-resolver)
                             .build)
                response (.execute client (.build request-builder))]
      (let [headers    (normalize-headers (.getAllHeaders response))
            body-bytes (if-let [entity (.getEntity response)]
                         (read-entity-bytes! entity url)
                         (byte-array 0))]
        {:status  (.getStatusCode (.getStatusLine response))
         :headers headers
         :body    (decode-body body-bytes headers)}))))

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
             (let [next-url (str (.resolve (URI. current-url) location))]
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
            (let [els (.select doc sel)]
              (when (pos? (.size els))
                (.first els))))
          main-content-selectors)
    ;; Fallback: body
    (.body doc)))

(defn- element->markdown
  "Convert a Jsoup Element to compact markdown-like text."
  [^Element elem include-links?]
  (let [sb (StringBuilder.)]
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
                      (let [items (.select el "> li")]
                        (dotimes [i (.size items)]
                          (let [li     (.get items i)
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
                      (let [rows (.select el "tr")]
                        (dotimes [r (.size rows)]
                          (let [row   (.get rows r)
                                cells (.select row "td, th")]
                            (when (pos? (.size cells))
                              (.append sb "| ")
                              (dotimes [c (.size cells)]
                                (when (pos? c) (.append sb " | "))
                                (.append sb (str/trim (.text (.get cells c)))))
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
          (let [doc    (Jsoup/parse ^String body ^String (or final-url url))
                _      (doseq [^Element el (.select doc noise-selectors)]
                         (.remove el))
                main   (find-main-content doc)
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
        (java.net.URLDecoder/decode (subs href start end) "UTF-8"))
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
                                 (str (java.net.URLEncoder/encode (name k) "UTF-8")
                                      "="
                                      (java.net.URLEncoder/encode (str v) "UTF-8"))))
                          (str/join "&"))
        separator    (cond
                       (str/ends-with? base-url "?") ""
                       (str/ends-with? base-url "&") ""
                       (str/includes? base-url "?") "&"
                       :else "?")]
    (str base-url separator query-string)))

(defn- select-first-any
  [root selectors]
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
  [result-el]
  (let [title-el (select-first-any result-el
                                   ["a.result__a"
                                    ".result__title a"
                                    "h2 a"
                                    "a[href]"])
        snip-el  (select-first-any result-el
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
  (let [doc          (Jsoup/parse ^String body)
        result-nodes (.select doc ".result, .results_links, .web-result")
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
                    (java.net.URLEncoder/encode query "UTF-8"))
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
                         :error   (.getMessage e)}]
            (if (seq more)
              (recur more (conj failures failure))
              (throw (ex-info (.getMessage e)
                              {:type     (or (:type (ex-data e))
                                             :search/backend-failed)
                               :backend  (name backend-id)
                               :failures (conj failures failure)}
                              e)))))))
    (catch Exception e
      (cond-> {:success? false
               :query    query
               :error    (.getMessage e)}
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
      (let [doc  (Jsoup/parse ^String body ^String (or final-url url))
            data (reduce-kv
                   (fn [m k sel]
                     (let [els (.select doc ^String sel)]
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
