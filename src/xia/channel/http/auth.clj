(ns xia.channel.http.auth
  "HTTP auth, managed-proxy auth, command auth, and ingress rate limiting."
  (:require [clojure.string :as str]
            [xia.config :as cfg]
            [xia.rate-limit :as rate-limit]
            [xia.runtime-overlay :as runtime-overlay]
            [xia.util :as util])
  (:import [java.net InetAddress]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util Base64]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicLong]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]))

(def local-session-cookie-name "xia-local-session")

(def ^:private local-hosts #{"localhost" "127.0.0.1" "::1" "[::1]"})
(def ^:private wildcard-binds #{"0.0.0.0" "0:0:0:0:0:0:0:0" "::"})

(defn- ipv4-octets
  [value]
  (let [parts (str/split (or value "") #"\." -1)]
    (when (and (= 4 (count parts))
               (every? #(boolean (re-matches #"[0-9]{1,3}" %)) parts))
      (let [octets (mapv #(Long/parseLong %) parts)]
        (when (every? #(<= 0 % 255) octets)
          octets)))))

(defn- normalize-ip-brackets
  [value]
  (if (and (str/starts-with? value "[")
           (str/ends-with? value "]")
           (str/includes? value ":"))
    (subs value 1 (dec (count value)))
    value))

(defn- loopback-ip-literal?
  "True only for numeric IPv4 or IPv6 loopback literals. This deliberately
   does not resolve peer-supplied hostnames."
  [value]
  (let [address (some-> value str str/trim not-empty normalize-ip-brackets)]
    (boolean
     (when address
       (if-let [octets (ipv4-octets address)]
         (= 127 (first octets))
         (when (str/includes? address ":")
           (try
             (.isLoopbackAddress (InetAddress/getByName address))
             (catch Exception _ false))))))))

(defn- loopback-remote-addr?
  "True when :remote-addr is a numeric loopback address. Only the socket peer
   is trusted; X-Forwarded-For / X-Real-IP are deliberately ignored."
  [req]
  (loopback-ip-literal? (:remote-addr req)))

(defn- invalid-bind-host!
  [bind-host cause]
  (throw (ex-info "HTTP bind host must be a resolvable IP address or hostname."
                  {:bind-host bind-host
                   :type :http/invalid-bind-host}
                  cause)))

(defn- bind-host-analysis
  [bind-host]
  (let [requested (some-> bind-host str str/trim not-empty)]
    (when-not requested
      (invalid-bind-host! bind-host nil))
    (let [normalized (normalize-ip-brackets requested)
          lower      (str/lower-case normalized)]
      (if (contains? wildcard-binds lower)
        {:requested-host requested
         :effective-host normalized
         :non-loopback? true}
        (do
          ;; Do not let malformed numeric-looking values fall through to DNS.
          (when (and (re-matches #"[0-9.]+" normalized)
                     (nil? (ipv4-octets normalized)))
            (invalid-bind-host! bind-host nil))
          (let [addresses (try
                            (vec (InetAddress/getAllByName normalized))
                            (catch Exception e
                              (invalid-bind-host! bind-host e)))]
            (when-not (seq addresses)
              (invalid-bind-host! bind-host nil))
            {:requested-host requested
             ;; Pin hostname resolution so the server binds the address that
             ;; was validated instead of resolving the hostname a second time.
             :effective-host (if (or (ipv4-octets normalized)
                                     (str/includes? normalized ":"))
                               normalized
                               (.getHostAddress ^InetAddress (first addresses)))
             :non-loopback? (not-every? #(.isLoopbackAddress ^InetAddress %)
                                        addresses)}))))))

(defn non-loopback-bind?
  "True when bind-host would expose the server beyond loopback."
  [bind-host]
  (try
    (:non-loopback? (bind-host-analysis bind-host))
    (catch Exception _
      true)))

(def ^:private command-channel-token-config-key :secret/command-channel-token)
(def ^:private command-channel-next-token-config-key :secret/command-channel-token-next)
(def ^:private managed-proxy-enabled-config-key :http/managed-proxy-enabled?)
(def ^:private managed-proxy-secret-file-config-key :http/managed-proxy-secret-file)
(def ^:private managed-tenant-origin-config-key :http/managed-tenant-origin)
(def ^:private ingress-rate-limit-window-ms 60000)
(def ^:private command-signature-max-skew-ms (* 5 60 1000))
(def ^:private command-auth-nonce-cleanup-interval-ms (* 60 1000))
(def ^:private command-auth-nonce-max-length 200)
(def ^:private managed-proxy-signature-max-skew-ms (* 5 60 1000))
(def ^:private managed-proxy-nonce-cleanup-interval-ms (* 60 1000))
(def ^:private managed-proxy-request-id-max-length 200)

(defn default-ingress-rate-limit-per-minute
  []
  1200)

(defn chat-ingress-rate-limit-per-minute
  []
  60)

(defn session-create-ingress-rate-limit-per-minute
  []
  120)

(defn- nonblank-str
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn request-header
  [req header-name]
  (let [target (str/lower-case header-name)]
    (or (get-in req [:headers header-name])
        (get-in req [:headers target])
        (some (fn [[k v]]
                (when (= target (str/lower-case (str k)))
                  v))
              (:headers req)))))

(defn- first-forwarded
  [value]
  (some-> value str (str/split #",") first str/trim nonblank-str))

(defn request-base-url
  [req]
  (or (when-let [origin (nonblank-str (request-header req "origin"))]
        (let [uri (java.net.URI. origin)]
          (str (.getScheme uri) "://" (.getAuthority uri))))
      (let [scheme (or (first-forwarded (request-header req "x-forwarded-proto"))
                       (some-> (:scheme req) name)
                       "http")
            host   (or (first-forwarded (request-header req "x-forwarded-host"))
                       (nonblank-str (request-header req "host")))]
        (when host
          (str scheme "://" host)))))

(defn- env-value
  [k]
  (System/getenv k))

(defn session-secret
  [deps]
  @((:local-session-secret-delay deps)))

(defn- session-cookie-value
  [deps]
  (str local-session-cookie-name "=" (session-secret deps)))

(defn- session-cookie-header
  [deps]
  (str (session-cookie-value deps) "; Path=/; HttpOnly; SameSite=Strict"))

(defn- bearer-token
  [value]
  (when-let [header (some-> value str str/trim not-empty)]
    (let [[scheme token & extra] (str/split header #"\s+")]
      (when (and (= "bearer" (some-> scheme str/lower-case))
                 (seq token)
                 (empty? extra))
        token))))

(defn- request-bearer-token
  [req]
  (bearer-token (request-header req "authorization")))

(defn- command-channel-token
  []
  (or (some-> (env-value "XIA_COMMAND_TOKEN") nonblank-str)
      (some-> (cfg/string-option command-channel-token-config-key nil) nonblank-str)))

(defn- command-channel-next-token
  []
  (or (some-> (env-value "XIA_COMMAND_TOKEN_NEXT") nonblank-str)
      (some-> (cfg/string-option command-channel-next-token-config-key nil) nonblank-str)))

(defn- command-channel-tokens
  []
  (->> [(command-channel-token)
        (command-channel-next-token)]
       (keep #(some-> % str str/trim not-empty))
       distinct
       vec))

(defn- managed-proxy-enabled?
  []
  (cfg/boolean-option managed-proxy-enabled-config-key false))

(defn- local-ui-auth-enabled?
  []
  (not (managed-proxy-enabled?)))

(defn- trim-trailing-newline
  [value]
  (some-> value (str/replace #"(?:\r?\n)\z" "")))

(defn- managed-proxy-secret
  []
  (when (managed-proxy-enabled?)
    (let [path (some-> (cfg/string-option managed-proxy-secret-file-config-key nil)
                       str/trim
                       not-empty)]
      (when path
        (try
          (some-> (slurp path) trim-trailing-newline not-empty)
          (catch java.io.FileNotFoundException _
            (throw (ex-info "Managed proxy secret file does not exist."
                            {:config-key managed-proxy-secret-file-config-key
                             :path path}))))))))

(defn- validate-managed-proxy-config!
  []
  (when (managed-proxy-enabled?)
    (when-not (managed-proxy-secret)
      (throw (ex-info "Managed proxy authentication is enabled but its secret file is missing or empty."
                      {:config-key managed-proxy-secret-file-config-key
                       :type :http/invalid-managed-proxy-config})))
    :managed-proxy))

(defn validate-bind-host!
  "Validate and pin the HTTP bind host. Non-loopback binds require either a
   usable managed-proxy configuration or a command-channel token."
  [bind-host]
  (let [{:keys [effective-host non-loopback?]} (bind-host-analysis bind-host)
        managed-auth-mode                    (validate-managed-proxy-config!)
        command-auth-mode                    (when (and non-loopback?
                                                        (seq (command-channel-tokens)))
                                               :command-channel)
        remote-auth-mode                     (or managed-auth-mode command-auth-mode)]
    (when (and non-loopback? (nil? remote-auth-mode))
      (throw (ex-info "Refusing to bind to a non-loopback address without remote authentication. Configure managed-proxy authentication, set XIA_COMMAND_TOKEN, or use --bind 127.0.0.1."
                      {:bind-host bind-host
                       :effective-bind-host effective-host
                       :type :http/remote-auth-required})))
    effective-host))

(defn- normalize-base-url
  [value]
  (some-> value str str/trim not-empty (str/replace #"/+$" "")))

(defn- managed-tenant-origin
  []
  (normalize-base-url (cfg/string-option managed-tenant-origin-config-key nil)))

(defn- local-command-client?
  [req]
  (let [remote-addr (some-> (:remote-addr req) str str/trim not-empty)]
    (or (nil? remote-addr)
        (contains? local-hosts remote-addr)
        (= "0:0:0:0:0:0:0:1" remote-addr))))

(defn constant-time-string=
  [a b]
  (and (string? a)
       (string? b)
       (MessageDigest/isEqual
        (.getBytes ^String a StandardCharsets/UTF_8)
        (.getBytes ^String b StandardCharsets/UTF_8))))

(defn- base64url
  [^bytes bytes]
  (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bytes))

(defn- sha256-base64url
  [^bytes bytes]
  (base64url (.digest (doto (MessageDigest/getInstance "SHA-256")
                        (.update bytes)))))

(defn hmac-sha256-base64url
  [secret message]
  (let [mac (Mac/getInstance "HmacSHA256")]
    (.init mac (SecretKeySpec.
                (.getBytes ^String secret StandardCharsets/UTF_8)
                "HmacSHA256"))
    (base64url (.doFinal mac (.getBytes ^String message StandardCharsets/UTF_8)))))

(defn- cookie-map
  [req]
  (let [cookie-header (request-header req "cookie")]
    (into {}
          (keep (fn [part]
                  (let [[k v] (str/split (str/trim part) #"=" 2)]
                    (when (and (seq k) (some? v))
                      [k v]))))
          (str/split (or cookie-header "") #";"))))

(defn- local-origin?
  [origin]
  (try
    (let [host (.getHost (java.net.URI. origin))]
      (contains? local-hosts host))
    (catch Exception _
      false)))

(defn- valid-session-secret?
  [deps req]
  (= (get (cookie-map req) local-session-cookie-name)
     (session-secret deps)))

(defn trusted-local-origin?
  "Allow loopback browser origins and direct local clients with no origin
   headers. Origin checks prevent cross-site requests from using the cookie."
  [req]
  (let [origin  (request-header req "origin")
        referer (request-header req "referer")]
    (cond
      (seq origin)  (local-origin? origin)
      (seq referer) (local-origin? referer)
      :else         true)))

(defn- ingress-route-class
  [req]
  (let [uri    (:uri req)
        method (:request-method req)]
    (cond
      (and (= method :post)
           (#{
              "/chat"
              "/command/chat"} uri))
      :chat

      (and (= method :post)
           (#{
              "/sessions"
              "/command/sessions"} uri))
      :session-create

      :else
      :default)))

(defn- ingress-rate-limit-per-minute
  [route-class]
  (case route-class
    :chat (chat-ingress-rate-limit-per-minute)
    :session-create (session-create-ingress-rate-limit-per-minute)
    (default-ingress-rate-limit-per-minute)))

(defn- request-client-id
  [req]
  (or (some-> (:remote-addr req) str str/trim not-empty)
      (some-> (request-header req "x-real-ip") str str/trim not-empty)
      (first-forwarded (request-header req "x-forwarded-for"))
      "unknown"))

(defn- ingress-rate-limit-key
  [req scope route-class]
  [scope route-class (request-client-id req)])

(defn- new-ingress-rate-limit-state
  []
  (atom {:timestamps []
         :cleaned 0}))

(defn- ingress-rate-limit-state
  [deps bucket-key now]
  (let [^ConcurrentHashMap states ((:ingress-rate-limits deps))]
    (rate-limit/maybe-prune-states! states
                                    ((:ingress-rate-limit-cleanup deps))
                                    now
                                    ingress-rate-limit-window-ms)
    (or (.get states bucket-key)
        (let [state (new-ingress-rate-limit-state)]
          (or (.putIfAbsent states bucket-key state)
              state)))))

(defn- ingress-retry-after-ms
  [state now]
  (let [now*   (long now)
        cutoff (- now* (long ingress-rate-limit-window-ms))
        oldest (reduce (fn [acc timestamp]
                         (let [timestamp* (long timestamp)]
                           (if (> timestamp* cutoff)
                             (if (or (nil? acc) (< timestamp* acc))
                               timestamp*
                               acc)
                             acc)))
                       nil
                       (:timestamps @state))]
    (-> (if oldest
          (- (long ingress-rate-limit-window-ms)
             (- now* oldest))
          (long ingress-rate-limit-window-ms))
        (util/long-max 0))))

(defn- ingress-rate-limited-response
  [deps scope route-class limit state now]
  (let [retry-after-ms      (ingress-retry-after-ms state now)
        retry-after-seconds (long (max 1 (Math/ceil (/ retry-after-ms 1000.0))))]
    (-> ((:json-response deps)
         429
         {:error "too many requests"
          :channel (name scope)
          :route_kind (name route-class)
          :limit_per_minute (long limit)
          :retry_after_seconds retry-after-seconds})
        (assoc-in [:headers "Retry-After"] (str retry-after-seconds)))))

(defn- check-ingress-rate-limit
  [deps req scope]
  (let [route-class (ingress-route-class req)
        limit       (ingress-rate-limit-per-minute route-class)
        now         (System/currentTimeMillis)
        state       (ingress-rate-limit-state deps
                                              (ingress-rate-limit-key req scope route-class)
                                              now)]
    (try
      (rate-limit/consume-slot! state
                                now
                                ingress-rate-limit-window-ms
                                limit
                                #(ex-info "too many requests"
                                          {:type :http/ingress-rate-limit}))
      nil
      (catch clojure.lang.ExceptionInfo e
        (if (= :http/ingress-rate-limit (some-> e ex-data :type))
          (ingress-rate-limited-response deps scope route-class limit state now)
          (throw e))))))

(defn reset-runtime-ingress-rate-limits!
  [runtime]
  (when runtime
    (.clear ^ConcurrentHashMap (:ingress-rate-limits runtime))
    (.set ^AtomicLong (:ingress-rate-limit-cleanup runtime) 0))
  nil)

(defn reset-runtime-command-auth!
  [runtime]
  (when runtime
    (.clear ^ConcurrentHashMap (:command-auth-nonces runtime))
    (.set ^AtomicLong (:command-auth-nonce-cleanup runtime) 0))
  nil)

(defn reset-runtime-managed-proxy-auth!
  [runtime]
  (when runtime
    (.clear ^ConcurrentHashMap (:managed-proxy-nonces runtime))
    (.set ^AtomicLong (:managed-proxy-nonce-cleanup runtime) 0))
  nil)

(defn- forbidden-response
  [deps]
  ((:json-response deps) 403 {:error "forbidden origin"}))

(defn- unauthorized-response
  [deps]
  ((:json-response deps) 401 {:error "missing or invalid local session secret"}))

(defn handle-local-session-bootstrap
  [deps req]
  (if-not (and (local-ui-auth-enabled?)
               (loopback-remote-addr? req)
               (trusted-local-origin? req))
    (forbidden-response deps)
    (-> ((:json-response deps) 200 {:ok true})
        (assoc-in [:headers "Set-Cookie"] (session-cookie-header deps)))))

(defn- command-channel-unavailable-response
  [deps]
  ((:json-response deps) 503 {:error "command channel is not configured"}))

(defn- command-unauthorized-response
  [deps]
  ((:json-response deps) 401 {:error "missing or invalid command token"}))

(defn- parse-command-timestamp-ms
  [value]
  (when-let [text (some-> value str str/trim not-empty)]
    (try
      (let [parsed (Long/parseLong text)]
        (when (pos? parsed)
          (if (< parsed 1000000000000)
            (* parsed 1000)
            parsed)))
      (catch NumberFormatException _
        nil))))

(defn- command-signature-attempt?
  [req]
  (or (some? (request-header req "x-xia-command-timestamp"))
      (some? (request-header req "x-xia-command-nonce"))
      (some? (request-header req "x-xia-command-signature"))))

(defn- command-request-body-bytes
  [deps req]
  (if (contains? req :body)
    (let [body (:body req)]
      (if (nil? body)
        [(byte-array 0) req]
        (let [body-bytes (or ((:read-body-bytes deps) body)
                             (byte-array 0))]
          [body-bytes (assoc req :body body-bytes)])))
    [(byte-array 0) req]))

(defn- command-signing-payload
  [req timestamp nonce body-bytes]
  (str (str/upper-case (name (:request-method req)))
       "\n"
       (:uri req)
       "\n"
       (or (:query-string req) "")
       "\n"
       timestamp
       "\n"
       nonce
       "\n"
       (sha256-base64url body-bytes)))

(defn- prune-command-auth-nonces!
  [deps now]
  (let [now* (long now)
        ^AtomicLong cleanup ((:command-auth-nonce-cleanup deps))]
    (loop []
      (let [last-cleanup (.get cleanup)]
        (cond
          (< (- now* last-cleanup) command-auth-nonce-cleanup-interval-ms)
          nil

          (not (.compareAndSet cleanup last-cleanup now*))
          (recur)

          :else
          (let [cutoff (- now* command-signature-max-skew-ms)]
            (doseq [entry (iterator-seq (.iterator (.entrySet ((:command-auth-nonces deps)))))]
              (let [nonce (.getKey entry)
                    timestamp (long (.getValue entry))]
                (when (< timestamp cutoff)
                  (.remove ^ConcurrentHashMap ((:command-auth-nonces deps)) nonce timestamp))))))))))

(defn- reserve-command-auth-nonce!
  [deps nonce timestamp now]
  (prune-command-auth-nonces! deps now)
  (nil? (.putIfAbsent ^ConcurrentHashMap ((:command-auth-nonces deps)) nonce (long timestamp))))

(defn- signed-command-auth
  [deps req token]
  (let [timestamp-text (some-> (request-header req "x-xia-command-timestamp")
                               str str/trim not-empty)
        nonce          (some-> (request-header req "x-xia-command-nonce")
                               str str/trim not-empty)
        signature      (some-> (request-header req "x-xia-command-signature")
                               str str/trim not-empty)
        timestamp      (parse-command-timestamp-ms timestamp-text)
        now            (System/currentTimeMillis)]
    (when (and timestamp-text nonce signature)
      (when (and timestamp
                 (<= (Math/abs (- (long now) (long timestamp)))
                     command-signature-max-skew-ms)
                 (<= (count nonce) command-auth-nonce-max-length))
        (let [[body-bytes req*] (command-request-body-bytes deps req)
              expected          (hmac-sha256-base64url
                                 token
                                 (command-signing-payload req timestamp-text nonce body-bytes))]
          (when (and (constant-time-string= expected signature)
                     (reserve-command-auth-nonce! deps nonce timestamp now))
            req*))))))

(defn- authenticated-command-req
  [deps req token]
  (cond
    (command-signature-attempt? req)
    (signed-command-auth deps req token)

    (and (local-command-client? req)
         (constant-time-string= (request-bearer-token req) token))
    req

    :else
    nil))

(defn- managed-proxy-signature-attempt?
  [req]
  (or (some? (request-header req "x-xia-proxy-mode"))
      (some? (request-header req "x-xia-tenant-id"))
      (some? (request-header req "x-xia-runtime-id"))
      (some? (request-header req "x-xia-user-id"))
      (some? (request-header req "x-xia-request-id"))
      (some? (request-header req "x-xia-proxy-timestamp"))
      (some? (request-header req "x-xia-proxy-signature"))))

(defn- runtime-overlay-tenant-id
  []
  (some-> (runtime-overlay/current-overlay) :tenant/id str str/trim not-empty))

(defn- runtime-overlay-runtime-id
  []
  (some-> (runtime-overlay/current-overlay) :runtime/id str str/trim not-empty))

(defn- managed-proxy-origin-valid?
  [req]
  (if-let [expected (managed-tenant-origin)]
    (= expected (normalize-base-url (request-base-url req)))
    true))

(defn- managed-proxy-signature-value
  [value]
  (let [text (some-> value str str/trim not-empty)]
    (cond
      (and text (str/starts-with? text "v1:"))
      (let [parts (str/split text #":")]
        (case (count parts)
          2 (second parts)
          3 (nth parts 2)
          nil))

      :else
      nil)))

(defn managed-proxy-signing-payload
  [req timestamp request-id tenant-id runtime-id user-id]
  (str (str/upper-case (name (:request-method req)))
       "\n"
       (:uri req)
       "\n"
       (or (:query-string req) "")
       "\n"
       timestamp
       "\n"
       request-id
       "\n"
       tenant-id
       "\n"
       runtime-id
       "\n"
       user-id))

(defn- prune-managed-proxy-nonces!
  [deps now]
  (let [now* (long now)
        ^AtomicLong cleanup ((:managed-proxy-nonce-cleanup deps))]
    (loop []
      (let [last-cleanup (.get cleanup)]
        (cond
          (< (- now* last-cleanup) managed-proxy-nonce-cleanup-interval-ms)
          nil

          (not (.compareAndSet cleanup last-cleanup now*))
          (recur)

          :else
          (let [cutoff (- now* managed-proxy-signature-max-skew-ms)]
            (doseq [entry (iterator-seq (.iterator (.entrySet ((:managed-proxy-nonces deps)))))]
              (let [request-id (.getKey entry)
                    timestamp (long (.getValue entry))]
                (when (< timestamp cutoff)
                  (.remove ^ConcurrentHashMap ((:managed-proxy-nonces deps)) request-id timestamp))))))))))

(defn- reserve-managed-proxy-request-id!
  [deps request-id timestamp now]
  (prune-managed-proxy-nonces! deps now)
  (nil? (.putIfAbsent ^ConcurrentHashMap ((:managed-proxy-nonces deps)) request-id (long timestamp))))

(defn- authenticated-managed-proxy-req
  [deps req]
  (let [secret         (managed-proxy-secret)
        overlay-tenant (runtime-overlay-tenant-id)
        overlay-runtime (runtime-overlay-runtime-id)
        mode           (some-> (request-header req "x-xia-proxy-mode") str str/trim not-empty)
        tenant-id      (some-> (request-header req "x-xia-tenant-id") str str/trim not-empty)
        runtime-id     (some-> (request-header req "x-xia-runtime-id") str str/trim not-empty)
        user-id        (some-> (request-header req "x-xia-user-id") str str/trim not-empty)
        request-id     (some-> (request-header req "x-xia-request-id") str str/trim not-empty)
        timestamp-text (some-> (request-header req "x-xia-proxy-timestamp") str str/trim not-empty)
        signature      (managed-proxy-signature-value
                        (request-header req "x-xia-proxy-signature"))
        timestamp      (parse-command-timestamp-ms timestamp-text)
        now            (System/currentTimeMillis)]
    (when (and (managed-proxy-enabled?)
               secret
               overlay-tenant
               overlay-runtime
               (= "tenant" mode)
               (= overlay-tenant tenant-id)
               (= overlay-runtime runtime-id)
               user-id
               request-id
               timestamp-text
               timestamp
               signature
               (managed-proxy-origin-valid? req)
               (<= (count request-id) managed-proxy-request-id-max-length)
               (<= (Math/abs (- (long now) (long timestamp)))
                   managed-proxy-signature-max-skew-ms))
      (let [expected (hmac-sha256-base64url
                      secret
                      (managed-proxy-signing-payload req
                                                     timestamp-text
                                                     request-id
                                                     tenant-id
                                                     runtime-id
                                                     user-id))]
        (when (and (constant-time-string= expected signature)
                   (reserve-managed-proxy-request-id! deps request-id timestamp now))
          (assoc req
                 :xia/managed-proxy {:tenant-id tenant-id
                                     :runtime-id runtime-id
                                     :user-id user-id
                                     :request-id request-id}))))))

(defn- valid-local-ui-request?
  [deps req]
  (and (local-ui-auth-enabled?)
       (loopback-remote-addr? req)
       (trusted-local-origin? req)
       (valid-session-secret? deps req)))

(defn- valid-managed-proxy-request?
  [deps req]
  (and (managed-proxy-signature-attempt? req)
       (boolean (authenticated-managed-proxy-req deps req))))

(defn protected-route-response
  [deps req allowed-fn]
  (if-let [response (check-ingress-rate-limit deps req :http)]
    response
    (let [authorized? (or (valid-local-ui-request? deps req)
                          (valid-managed-proxy-request? deps req))]
      (cond
        authorized?
        (if ((:runtime-available? deps))
          (allowed-fn)
          ((:runtime-unavailable-response deps)))

        (not (trusted-local-origin? req))
        (forbidden-response deps)

        :else
        (unauthorized-response deps)))))

(defn command-route-response
  [deps req allowed-fn]
  (if-let [response (check-ingress-rate-limit deps req :command)]
    response
    (cond
      (not ((:runtime-available? deps)))
      ((:runtime-unavailable-response deps))

      :else
      (if-let [tokens (seq (command-channel-tokens))]
        (if-let [req* (some #(authenticated-command-req deps req %) tokens)]
          (allowed-fn req*)
          (command-unauthorized-response deps))
        (command-channel-unavailable-response deps)))))
