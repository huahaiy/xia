(ns xia.web
  "Web browsing capabilities for tool handlers.

   Provides three functions exposed to the SCI sandbox:
     fetch-page   — fetch a URL, return readable text content
     search-web   — web search, return structured results
     extract-data — fetch a URL, extract elements by CSS selectors

   All HTTP requests go through this module (not raw hato) so we can
   enforce SSRF protection, rate limiting, and content size limits."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [charred.api :as json]
            [xia.http-client :as http])
  (:import [org.jsoup Jsoup]
           [org.jsoup.nodes Document Element TextNode]
           [org.jsoup.select Elements]
           [java.net InetAddress URI]
           [java.util.concurrent ConcurrentHashMap]))

(def ^:private user-agent "Xia/0.1 (personal AI assistant)")
(def ^:private max-body-bytes (* 1 1024 1024)) ; 1 MB
(def ^:private max-redirects 5)

;; ---------------------------------------------------------------------------
;; SSRF protection
;; ---------------------------------------------------------------------------

(defn- private-ip?
  "True if the address is private, loopback, or link-local."
  [^InetAddress addr]
  (or (.isLoopbackAddress addr)
      (.isLinkLocalAddress addr)
      (.isSiteLocalAddress addr)
      (.isAnyLocalAddress addr)))

(defn- resolve-host-addresses
  [host]
  (seq (InetAddress/getAllByName host)))

(defn- validate-url!
  "Validate a URL for safety. Throws on disallowed schemes or private IPs."
  [url]
  (let [uri (URI. url)]
    (when-not (#{"http" "https"} (.getScheme uri))
      (throw (ex-info "Only http:// and https:// URLs are allowed"
                      {:url url :scheme (.getScheme uri)})))
    (let [host (.getHost uri)]
      (when (str/blank? host)
        (throw (ex-info "URL has no host" {:url url})))
      (let [addrs (resolve-host-addresses host)]
        (when (some private-ip? addrs)
          (throw (ex-info "Access to private/internal network addresses is blocked"
                          {:url url :host host})))))))

;; ---------------------------------------------------------------------------
;; Rate limiting — simple per-domain token bucket
;; ---------------------------------------------------------------------------

(defonce ^:private rate-limits (ConcurrentHashMap.))

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
    (swap! state
      (fn [{:keys [timestamps]}]
        (let [cutoff (- now rate-limit-window-ms)
              recent (filterv #(> % cutoff) timestamps)]
          (when (>= (count recent) rate-limit-max)
            (throw (ex-info (str "Rate limit exceeded for " host
                                 " (max " rate-limit-max " requests/minute)")
                            {:host host})))
          {:timestamps (conj recent now)
           :cleaned    now})))))

;; ---------------------------------------------------------------------------
;; HTTP fetch
;; ---------------------------------------------------------------------------

(defn- fetch-raw
  "Fetch a URL, return {:status :headers :body :final-url}."
  [url]
  (loop [current-url url
         redirects   0]
    (validate-url! current-url)
    (check-rate-limit! current-url)
    (let [resp (http/request {:url             current-url
                              :method          :get
                              :headers         {"User-Agent" user-agent
                                                "Accept"     "text/html,application/xhtml+xml,*/*"}
                              :connect-timeout 10000
                              :request-label   "Web fetch"})
          status (:status resp)
          location (get-in resp [:headers "location"])]
      (if (and (#{301 302 303 307 308} status) (seq location))
        (do
          (when (>= redirects max-redirects)
            (throw (ex-info "Too many redirects"
                            {:url current-url :redirects redirects})))
          (let [next-url (str (.resolve (URI. current-url) location))]
            (recur next-url (inc redirects))))
        (let [body (str (:body resp))]
          (when (> (count body) max-body-bytes)
            (throw (ex-info "Response too large" {:url current-url :size (count body)})))
          {:status    status
           :headers   (:headers resp)
           :body      body
           :final-url current-url})))))

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
                          (let [li (.get items i)
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
                          (let [row (.get rows r)
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

(defn- heading-level [^String tag]
  (when (and tag (= 1 (count tag)) (Character/isDigit (.charAt tag 0)))
    (- (int (.charAt tag 0)) (int \0))))

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
     {:url        \"https://...\"
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
            {:url        (or final-url url)
             :title      title
             :content    content
             :links      (or links [])
             :truncated? (not= raw content)})
          ;; Non-HTML — return raw text truncated
          (let [content (truncate-to-tokens (or body "") max-tokens)]
            {:url        (or final-url url)
             :title      nil
             :content    content
             :links      []
             :truncated? (not= body content)}))))
    (catch Exception e
      {:url   url
       :error (.getMessage e)})))

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

(defn search-web
  "Search the web. Returns structured results.

   Uses DuckDuckGo HTML search as the default backend (no API key needed).

   Arguments:
     query — the search query

   Options:
     :max-results — max number of results (default 5)

   Returns:
     {:query   \"search terms\"
      :results [{:title \"...\" :url \"...\" :snippet \"...\"}]}"
  [query & {:keys [max-results]
            :or   {max-results 5}}]
  (try
    (let [enc-query (java.net.URLEncoder/encode query "UTF-8")
          url       (str "https://html.duckduckgo.com/html/?q=" enc-query)
          resp      (http/request {:url             url
                                   :method          :get
                                   :headers         {"User-Agent" user-agent}
                                   :connect-timeout 10000
                                   :request-label   "Web search"})
          doc       (Jsoup/parse ^String (str (:body resp)))
          results   (.select doc ".result")]
      {:query   query
       :results (->> results
                     (take max-results)
                     (mapv (fn [^Element r]
                             (let [title-el (.selectFirst r ".result__a")
                                   snip-el  (.selectFirst r ".result__snippet")
                                   raw-href (when title-el (.attr title-el "href"))]
                               {:title   (if title-el (.text title-el) "")
                                :url     (or (extract-ddg-url raw-href) "")
                                :snippet (if snip-el (.text snip-el) "")}))))})
    (catch Exception e
      {:query query
       :error (.getMessage e)})))

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
     {:url  \"https://...\"
      :data {\"headings\" [\"Title\" \"Subtitle\"]
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
        {:url  (or final-url url)
         :data data}))
    (catch Exception e
      {:url   url
       :error (.getMessage e)})))
