(ns xia.oauth
  "OAuth 2 authorization-code + PKCE support for registered service accounts."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xia.db :as db]
            [xia.http-client :as http])
  (:import [java.net URI URLDecoder URLEncoder]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest SecureRandom]
           [java.time Instant]
           [java.util Base64 Date]))
(defonce ^:private pending-authorizations (atom {}))

(def ^:private auth-state-ttl-ms (* 15 60 1000))
(def ^:private refresh-skew-ms 60000)

(defn- now-ms []
  (System/currentTimeMillis))

(defn- nonblank-str [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn- base64url
  [^bytes data]
  (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) data))

(defn- random-token
  [n]
  (let [bytes (byte-array n)]
    (.nextBytes (SecureRandom.) bytes)
    (base64url bytes)))

(defn- pkce-verifier []
  (random-token 48))

(defn- pkce-challenge
  [verifier]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (.update digest (.getBytes ^String verifier StandardCharsets/US_ASCII))
    (base64url (.digest digest))))

(defn- cleanup-pending!
  []
  (let [cutoff (- (now-ms) auth-state-ttl-ms)]
    (swap! pending-authorizations
           (fn [pending]
             (into {}
                   (filter (fn [[_ {:keys [created-at-ms]}]]
                             (> (long created-at-ms) cutoff)))
                   pending)))))

(defn- parse-json-map
  [value]
  (when-let [text (nonblank-str value)]
    (let [parsed (json/read-json text)]
      (if (map? parsed)
        parsed
        (throw (ex-info "OAuth params must be a JSON object" {:value value}))))))

(defn- encode-www-form
  [params]
  (->> params
       (keep (fn [[k v]]
               (when (some? v)
                 (str (URLEncoder/encode (name k) "UTF-8")
                      "="
                      (URLEncoder/encode (str v) "UTF-8")))))
       (str/join "&")))

(defn- append-query-params
  [url params]
  (let [query (encode-www-form params)
        sep   (if (str/includes? url "?") "&" "?")]
    (str url sep query)))

(defn- parse-form-response
  [body]
  (into {}
        (keep (fn [part]
                (let [[k v] (str/split part #"=" 2)]
                  (when (seq k)
                    [(URLDecoder/decode k "UTF-8")
                     (some-> v (URLDecoder/decode "UTF-8"))]))))
        (str/split (or body "") #"&")))

(defn- parse-token-response
  [body]
  (cond
    (map? body)
    body

    (string? body)
    (try
      (json/read-json body)
      (catch Exception _
        (parse-form-response body)))

    :else
    {}))

(defn- token-request!
  [url params]
  (let [resp   (http/request {:url           url
                              :method        :post
                              :headers       {"Accept" "application/json"
                                              "Content-Type" "application/x-www-form-urlencoded"}
                              :body          (encode-www-form params)
                              :request-label "OAuth token request"})
        body   (parse-token-response (:body resp))
        status (:status resp)]
    (when (or (< status 200) (>= status 300))
      (throw (ex-info "OAuth token request failed"
                      {:status status
                       :body body})))
    body))

(defn- response-expires-at
  [token-response]
  (when-let [expires-in (some-> (get token-response "expires_in") str Long/parseLong)]
    (Date/from (.plusSeconds (Instant/now) expires-in))))

(defn- normalize-token-type
  [value]
  (when-let [token-type (nonblank-str value)]
    (str/capitalize token-type)))

(defn- update-account-tokens!
  [account token-response]
  (let [access-token  (nonblank-str (get token-response "access_token"))
        refresh-token (or (nonblank-str (get token-response "refresh_token"))
                          (:oauth.account/refresh-token account))
        token-type    (or (normalize-token-type (get token-response "token_type"))
                          (:oauth.account/token-type account)
                          "Bearer")
        expires-at    (or (response-expires-at token-response)
                          (:oauth.account/expires-at account))
        connected-at  (Date.)]
    (when-not access-token
      (throw (ex-info "OAuth token response did not include access_token"
                      {:body token-response})))
    (db/save-oauth-account! {:id            (:oauth.account/id account)
                             :name          (:oauth.account/name account)
                             :authorize-url (:oauth.account/authorize-url account)
                             :token-url     (:oauth.account/token-url account)
                             :client-id     (:oauth.account/client-id account)
                             :client-secret (:oauth.account/client-secret account)
                             :provider-template (:oauth.account/provider-template account)
                             :scopes        (:oauth.account/scopes account)
                             :redirect-uri  (:oauth.account/redirect-uri account)
                             :auth-params   (:oauth.account/auth-params account)
                             :token-params  (:oauth.account/token-params account)
                             :access-token  access-token
                             :refresh-token refresh-token
                             :token-type    token-type
                             :expires-at    expires-at
                             :connected-at  connected-at})
    (db/get-oauth-account (:oauth.account/id account))))

(defn oauth-header
  [account]
  (let [token-type (or (normalize-token-type (:oauth.account/token-type account)) "Bearer")
        access-token (:oauth.account/access-token account)]
    (when-not (seq (or access-token ""))
      (throw (ex-info "OAuth account is not connected" {:account-id (:oauth.account/id account)})))
    (str token-type " " access-token)))

(defn account-expiring?
  [account]
  (if-let [expires-at (:oauth.account/expires-at account)]
    (<= (.getTime ^Date expires-at)
        (+ (now-ms) refresh-skew-ms))
    false))

(defn get-account
  [account-id]
  (or (db/get-oauth-account account-id)
      (throw (ex-info (str "Unknown OAuth account: " (name account-id))
                      {:account-id account-id}))))

(defn list-accounts
  []
  (db/list-oauth-accounts))

(defn start-authorization!
  [account-id callback-url]
  (cleanup-pending!)
  (let [account       (get-account account-id)
        verifier      (pkce-verifier)
        state         (random-token 24)
        redirect-uri  (or (nonblank-str (:oauth.account/redirect-uri account))
                          (nonblank-str callback-url))
        auth-params   (or (parse-json-map (:oauth.account/auth-params account)) {})
        auth-url      (append-query-params
                        (:oauth.account/authorize-url account)
                        (merge {"response_type" "code"
                                "client_id" (:oauth.account/client-id account)
                                "redirect_uri" redirect-uri
                                "state" state
                                "code_challenge" (pkce-challenge verifier)
                                "code_challenge_method" "S256"}
                               (when-let [scopes (nonblank-str (:oauth.account/scopes account))]
                                 {"scope" scopes})
                               (into {} (map (fn [[k v]] [(name k) v]) auth-params))))]
    (when-not redirect-uri
      (throw (ex-info "OAuth redirect URI is required"
                      {:account-id account-id})))
    (swap! pending-authorizations assoc state {:account-id    account-id
                                               :redirect-uri  redirect-uri
                                               :code-verifier verifier
                                               :created-at-ms (now-ms)})
    {:account-id    account-id
     :authorization-url auth-url
     :redirect-uri  redirect-uri
     :state         state}))

(defn complete-authorization!
  [state code]
  (cleanup-pending!)
  (let [{:keys [account-id redirect-uri code-verifier] :as pending}
        (get @pending-authorizations state)]
    (when-not pending
      (throw (ex-info "OAuth authorization state is invalid or expired" {:state state})))
    (swap! pending-authorizations dissoc state)
    (let [account      (get-account account-id)
          token-params (or (parse-json-map (:oauth.account/token-params account)) {})
          params       (merge {"grant_type" "authorization_code"
                               "code" code
                               "redirect_uri" redirect-uri
                               "client_id" (:oauth.account/client-id account)
                               "code_verifier" code-verifier}
                              (when-let [secret (nonblank-str (:oauth.account/client-secret account))]
                                {"client_secret" secret})
                              (into {} (map (fn [[k v]] [(name k) v]) token-params)))
          response     (token-request! (:oauth.account/token-url account) params)]
      (log/info "OAuth account connected" (name account-id))
      (update-account-tokens! account response))))

(defn refresh-account!
  [account-id]
  (let [account        (get-account account-id)
        refresh-token  (nonblank-str (:oauth.account/refresh-token account))
        token-params   (or (parse-json-map (:oauth.account/token-params account)) {})]
    (when-not refresh-token
      (throw (ex-info "OAuth account has no refresh token" {:account-id account-id})))
    (let [params   (merge {"grant_type" "refresh_token"
                           "refresh_token" refresh-token
                           "client_id" (:oauth.account/client-id account)}
                          (when-let [secret (nonblank-str (:oauth.account/client-secret account))]
                            {"client_secret" secret})
                          (into {} (map (fn [[k v]] [(name k) v]) token-params)))
          response (token-request! (:oauth.account/token-url account) params)]
      (log/info "OAuth account refreshed" (name account-id))
      (update-account-tokens! account response))))

(defn ensure-account-ready!
  [account-id]
  (let [account (get-account account-id)]
    (cond
      (and (nil? (nonblank-str (:oauth.account/access-token account)))
           (nonblank-str (:oauth.account/refresh-token account)))
      (refresh-account! account-id)

      (nil? (nonblank-str (:oauth.account/access-token account)))
      (throw (ex-info "OAuth account is not connected" {:account-id account-id}))

      (account-expiring? account)
      (refresh-account! account-id)

      :else
      account)))

(defn callback-account-id
  [state]
  (some-> (get @pending-authorizations state) :account-id))
