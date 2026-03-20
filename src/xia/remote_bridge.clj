(ns xia.remote-bridge
  "Local-side notification/status bridge foundation.

   This namespace manages:
   - persistent bridge identity and config
   - paired device records
   - compact operational event records
   - a computed remote status snapshot

   Relay transport and mobile delivery are intentionally not implemented here
   yet; this is the Xia-side substrate that those pieces will build on."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.db :as db])
  (:import [java.nio.charset StandardCharsets]
           [java.security KeyFactory KeyPairGenerator]
           [java.security.spec NamedParameterSpec PKCS8EncodedKeySpec X509EncodedKeySpec]
           [java.util Base64 Base64$Decoder Base64$Encoder Date UUID]))

(def ^:private bridge-id :primary)
(def ^:private bridge-private-key-config-key :secret/remote-bridge-private-key)
(def ^:private bridge-version "0.1.0")
(def ^:private keep-event-count 200)
(def ^:private attention-expiry-warning-ms (* 24 60 60 1000))

(def supported-topics
  #{:schedule.failed
    :schedule.backoff
    :schedule.recovered
    :oauth.attention
    :service.attention
    :instance.online
    :instance.offline
    :instance.starting})

(def ^:private default-device-topics
  supported-topics)

(defn- now []
  (Date.))

(defn- nonblank-str
  [value]
  (when-let [s (some-> value str str/trim)]
    (when (seq s) s)))

(defn- url-encoder ^Base64$Encoder []
  (.withoutPadding (Base64/getUrlEncoder)))

(defn- url-decoder ^Base64$Decoder []
  (Base64/getUrlDecoder))

(defn- encode-bytes
  [^bytes value]
  (.encodeToString ^Base64$Encoder (url-encoder) value))

(defn- decode-bytes
  [value]
  (.decode ^Base64$Decoder (url-decoder) ^String value))

(defn- generate-keypair []
  (let [generator (KeyPairGenerator/getInstance "X25519")]
    (.initialize generator (NamedParameterSpec. "X25519"))
    (let [keypair (.generateKeyPair generator)]
      {:public-key  (encode-bytes (.getEncoded (.getPublic keypair)))
       :private-key (encode-bytes (.getEncoded (.getPrivate keypair)))})))

(defn- validate-public-key!
  [encoded]
  (try
    (let [factory (KeyFactory/getInstance "X25519")]
      (.generatePublic factory (X509EncodedKeySpec. (decode-bytes encoded))))
    encoded
    (catch Exception e
      (throw (ex-info "invalid remote bridge public key"
                      {:type :remote-bridge/invalid-public-key}
                      e)))))

(defn- validate-private-key!
  [encoded]
  (try
    (let [factory (KeyFactory/getInstance "X25519")]
      (.generatePrivate factory (PKCS8EncodedKeySpec. (decode-bytes encoded))))
    encoded
    (catch Exception e
      (throw (ex-info "invalid remote bridge private key"
                      {:type :remote-bridge/invalid-private-key}
                      e)))))

(defn- bridge-eid []
  (ffirst (db/q '[:find ?e :in $ ?id :where [?e :remote.bridge/id ?id]]
                bridge-id)))

(defn- bridge-entity []
  (when-let [eid (bridge-eid)]
    (db/entity eid)))

(defn- bridge-connection-state
  [bridge]
  (cond
    (not (:remote.bridge/enabled? bridge))
    :disabled

    (:remote.bridge/connected-at bridge)
    :connected

    :else
    :disconnected))

(defn- bridge-instance-label
  [bridge]
  (or (nonblank-str (:remote.bridge/instance-label bridge))
      (nonblank-str (db/get-config :user/name))
      "Xia"))

(defn ensure-bridge-identity!
  []
  (let [bridge      (bridge-entity)
        instance-id (or (:remote.bridge/instance-id bridge)
                        (str (random-uuid)))
        public-key  (nonblank-str (:remote.bridge/public-key bridge))
        private-key (some-> (db/get-config bridge-private-key-config-key)
                            nonblank-str)
        generated   (when (or (nil? public-key) (nil? private-key))
                      (generate-keypair))
        public-key* (or public-key (:public-key generated))
        private-key* (or private-key (:private-key generated))]
    (validate-public-key! public-key*)
    (validate-private-key! private-key*)
    (db/transact!
      [{:remote.bridge/id bridge-id
        :remote.bridge/enabled? (boolean (:remote.bridge/enabled? bridge))
        :remote.bridge/instance-id instance-id
        :remote.bridge/instance-label (bridge-instance-label bridge)
        :remote.bridge/public-key public-key*}])
    (when generated
      (db/set-config! bridge-private-key-config-key private-key*))
    (bridge-entity)))

(defn bridge-config []
  (let [bridge (or (bridge-entity) (ensure-bridge-identity!))
        private-key (some-> (db/get-config bridge-private-key-config-key)
                            nonblank-str)]
    {:id bridge-id
     :enabled? (boolean (:remote.bridge/enabled? bridge))
     :instance-id (:remote.bridge/instance-id bridge)
     :instance-label (bridge-instance-label bridge)
     :relay-url (:remote.bridge/relay-url bridge)
     :public-key (:remote.bridge/public-key bridge)
     :keypair-ready? (boolean (and (nonblank-str (:remote.bridge/public-key bridge))
                                   private-key))
     :connected-at (:remote.bridge/connected-at bridge)
     :last-seen-at (:remote.bridge/last-seen-at bridge)
     :connection-state (bridge-connection-state bridge)}))

(defn bridge-enabled? []
  (:enabled? (bridge-config)))

(defn save-bridge-config!
  [{:keys [enabled? instance-label relay-url]}]
  (let [bridge       (ensure-bridge-identity!)
        bridge-eid*  (bridge-eid)
        relay-url*   (nonblank-str relay-url)
        label*       (or (nonblank-str instance-label)
                         (bridge-instance-label bridge))
        tx-data      (cond-> [{:remote.bridge/id bridge-id
                               :remote.bridge/enabled? (boolean enabled?)
                               :remote.bridge/instance-id (:remote.bridge/instance-id bridge)
                               :remote.bridge/instance-label label*
                               :remote.bridge/public-key (:remote.bridge/public-key bridge)}]
                       relay-url*
                       (update 0 assoc :remote.bridge/relay-url relay-url*)

                       (and bridge-eid*
                            (nil? relay-url*)
                            (contains? bridge :remote.bridge/relay-url))
                       (conj [:db/retract bridge-eid*
                              :remote.bridge/relay-url
                              (:remote.bridge/relay-url bridge)]))]
    (db/transact! tx-data)
    (bridge-config)))

(defn mark-bridge-connected!
  []
  (let [timestamp (now)]
    (db/transact! [{:remote.bridge/id bridge-id
                    :remote.bridge/connected-at timestamp
                    :remote.bridge/last-seen-at timestamp}])
    (bridge-config)))

(defn mark-bridge-seen!
  []
  (let [timestamp (now)]
    (db/transact! [{:remote.bridge/id bridge-id
                    :remote.bridge/last-seen-at timestamp}])
    (bridge-config)))

(defn mark-bridge-disconnected!
  []
  (when-let [eid (bridge-eid)]
    (when-let [connected-at (:remote.bridge/connected-at (bridge-entity))]
      (db/transact! [[:db/retract eid :remote.bridge/connected-at connected-at]])))
  (bridge-config))

(defn- parse-token-json
  [token]
  (let [trimmed (nonblank-str token)]
    (when-not trimmed
      (throw (ex-info "missing pairing token"
                      {:type :remote-bridge/missing-pairing-token})))
    (try
      (if (str/starts-with? trimmed "{")
        (json/read-json trimmed)
        (let [decoded ^bytes (decode-bytes trimmed)]
          (json/read-json (String. decoded StandardCharsets/UTF_8))))
      (catch Exception e
        (throw (ex-info "invalid pairing token"
                        {:type :remote-bridge/invalid-pairing-token}
                        e))))))

(defn- parse-device-id
  [value]
  (when-let [text (nonblank-str value)]
    (try
      (UUID/fromString text)
      (catch IllegalArgumentException e
        (throw (ex-info "pairing token has invalid device id"
                        {:type :remote-bridge/invalid-device-id
                         :value text}
                        e))))))

(defn- parse-topics
  [value]
  (let [topics (cond
                 (nil? value) nil
                 (sequential? value) value
                 :else [value])]
    (when topics
      (let [parsed (->> topics
                        (map #(some-> % str keyword))
                        (filter supported-topics)
                        set)]
        (when (seq parsed)
          parsed)))))

(defn- device-eid
  [device-id]
  (ffirst (db/q '[:find ?e :in $ ?id :where [?e :remote.device/id ?id]]
                device-id)))

(defn- device->body
  [entity]
  {:id (:remote.device/id entity)
   :name (:remote.device/name entity)
   :public-key (:remote.device/public-key entity)
   :platform (:remote.device/platform entity)
   :status (:remote.device/status entity)
   :topics (->> (or (:remote.device/topics entity) [])
                sort
                vec)
   :muted? (boolean (:remote.device/muted? entity))
   :created-at (:remote.device/created-at entity)
   :last-seen-at (:remote.device/last-seen-at entity)})

(defn list-devices []
  (->> (db/q '[:find ?e :where [?e :remote.device/id _]])
       (map first)
       (map db/entity)
       (sort-by (fn [entity]
                  (let [created-at (or (:remote.device/created-at entity) (Date. 0))
                        created-ms (.getTime ^Date created-at)]
                    [(- (long created-ms))
                     (str (:remote.device/name entity))])))
       (mapv device->body)))

(defn pair-device!
  [pairing-token]
  (ensure-bridge-identity!)
  (let [payload        (parse-token-json pairing-token)
        device-id      (or (parse-device-id (get payload "device_id"))
                           (random-uuid))
        device-name    (or (nonblank-str (get payload "device_name"))
                           (nonblank-str (get payload "name"))
                           "Paired device")
        public-key     (or (nonblank-str (get payload "public_key"))
                           (throw (ex-info "pairing token missing public key"
                                           {:type :remote-bridge/missing-public-key})))
        platform       (some-> (or (get payload "platform") "unknown") str keyword)
        topics         (or (parse-topics (get payload "topics"))
                           default-device-topics)
        push-token     (nonblank-str (get payload "push_token"))
        muted?         (boolean (get payload "muted"))
        current-eid    (device-eid device-id)
        current        (when current-eid (db/entity current-eid))
        timestamp      (now)
        tx-data        (cond-> [{:remote.device/id device-id
                                 :remote.device/name device-name
                                 :remote.device/public-key (validate-public-key! public-key)
                                 :remote.device/platform platform
                                 :remote.device/status :paired
                                 :remote.device/topics topics
                                 :remote.device/muted? muted?
                                 :remote.device/created-at (or (:remote.device/created-at current) timestamp)
                                 :remote.device/last-seen-at timestamp}]
                          push-token
                          (update 0 assoc :remote.device/push-token push-token)

                          (and current-eid
                               (nil? push-token)
                               (contains? current :remote.device/push-token))
                          (conj [:db/retract current-eid
                                 :remote.device/push-token
                                 (:remote.device/push-token current)]))]
    (db/transact! tx-data)
    (log/info "Paired remote bridge device:" device-name)
    (some-> device-id device-eid db/entity device->body)))

(defn revoke-device!
  [device-id]
  (let [device-id* (if (uuid? device-id) device-id (UUID/fromString (str device-id)))]
    (when-not (device-eid device-id*)
      (throw (ex-info "remote bridge device not found"
                      {:type :remote-bridge/device-not-found
                       :device-id device-id*})))
    (db/transact! [{:remote.device/id device-id*
                    :remote.device/status :revoked
                    :remote.device/last-seen-at (now)}])
    (some-> device-id* device-eid db/entity device->body)))

(defn- trim-events!
  []
  (let [events (->> (db/q '[:find ?e ?created
                            :where
                            [?e :remote.event/id _]
                            [?e :remote.event/created-at ?created]])
                    (sort-by second #(compare %2 %1))
                    (drop keep-event-count)
                    (map first)
                    vec)]
    (when (seq events)
      (db/transact! (mapv (fn [eid] [:db/retractEntity eid]) events)))))

(defn- event->body
  [entity]
  {:id (:remote.event/id entity)
   :type (:remote.event/type entity)
   :topic (:remote.event/topic entity)
   :severity (:remote.event/severity entity)
   :title (:remote.event/title entity)
   :detail (:remote.event/detail entity)
   :metadata (:remote.event/metadata entity)
   :status (:remote.event/status entity)
   :device-id (:remote.event/device-id entity)
   :created-at (:remote.event/created-at entity)
   :delivered-at (:remote.event/delivered-at entity)})

(defn list-events
  ([] (list-events 20))
  ([limit]
   (->> (db/q '[:find ?e ?created
                :where
                [?e :remote.event/id _]
                [?e :remote.event/created-at ?created]])
        (sort-by second #(compare %2 %1))
        (take limit)
        (map first)
        (map db/entity)
        (mapv event->body))))

(defn record-event!
  [{:keys [type topic severity title detail metadata status device-id created-at]}]
  (when (bridge-enabled?)
    (let [event-id (random-uuid)]
      (db/transact!
        [(cond-> {:remote.event/id event-id
                  :remote.event/type type
                  :remote.event/topic (or topic type)
                  :remote.event/severity (or severity :info)
                  :remote.event/title (or title (name (or type topic)))
                  :remote.event/detail (or detail "")
                  :remote.event/status (or status :queued)
                  :remote.event/created-at (or created-at (now))}
           metadata (assoc :remote.event/metadata metadata)
           device-id (assoc :remote.event/device-id device-id))])
      (trim-events!)
      event-id)))

(defn- schedule-name
  [schedule-id]
  (or (ffirst (db/q '[:find ?name :in $ ?id
                      :where
                      [?e :schedule/id ?id]
                      [?e :schedule/name ?name]]
                    schedule-id))
      (some-> schedule-id name)
      "schedule"))

(defn notify-schedule-failure!
  [schedule-id {:keys [paused? consecutive-failures backoff-until error-message]}]
  (let [schedule-name* (schedule-name schedule-id)
        event-type     (if (<= (long (or consecutive-failures 0)) 1)
                         :schedule.failed
                         :schedule.backoff)
        title          (cond
                         paused? (str "Schedule paused: " schedule-name*)
                         (= event-type :schedule.failed) (str "Schedule failed: " schedule-name*)
                         :else (str "Schedule backing off: " schedule-name*))
        detail         (cond
                         paused?
                         (str "Disabled after " consecutive-failures " repeated failures."
                              (when-let [message (nonblank-str error-message)]
                                (str " " message)))

                         (= event-type :schedule.failed)
                         (or (nonblank-str error-message)
                             "Scheduled run failed.")

                         :else
                         (str "Failure " consecutive-failures
                              (when backoff-until
                                (str "; next retry after " backoff-until))
                              (when-let [message (nonblank-str error-message)]
                                (str ". " message))))]
    (record-event! {:type event-type
                    :topic event-type
                    :severity (if paused? :error :warn)
                    :title title
                    :detail detail
                    :metadata {:schedule-id schedule-id
                               :schedule-name schedule-name*
                               :consecutive-failures consecutive-failures
                               :paused? (boolean paused?)
                               :backoff-until backoff-until}})))

(defn notify-schedule-recovered!
  [schedule-id {:keys [previous-failures result-summary]}]
  (let [schedule-name* (schedule-name schedule-id)]
    (record-event! {:type :schedule.recovered
                    :topic :schedule.recovered
                    :severity :info
                    :title (str "Schedule recovered: " schedule-name*)
                    :detail (or (nonblank-str result-summary)
                                (str "Recovered after " previous-failures " failed attempt"
                                     (when (not= 1 previous-failures) "s")
                                     "."))
                    :metadata {:schedule-id schedule-id
                               :schedule-name schedule-name*
                               :previous-failures previous-failures}})))

(defn- running-schedules []
  (let [eids (db/q '[:find ?e
                     :where
                     [?e :schedule.state/status :running]])]
    (->> eids
         (map first)
         (map db/entity)
         (mapv (fn [entity]
                 {:schedule-id (:schedule.state/schedule-id entity)
                  :schedule-name (schedule-name (:schedule.state/schedule-id entity))
                  :phase (:schedule.state/phase entity)
                  :checkpoint-at (:schedule.state/checkpoint-at entity)})))))

(defn- recent-runs-by-status
  [status limit]
  (let [runs (db/q '[:find ?e ?started
                     :in $ ?status
                     :where
                     [?e :schedule-run/status ?status]
                     [?e :schedule-run/started-at ?started]]
                   status)]
    (->> runs
         (sort-by second #(compare %2 %1))
         (take limit)
         (map first)
         (map db/entity)
         (mapv (fn [entity]
                 {:id (:schedule-run/id entity)
                  :schedule-id (:schedule-run/schedule-id entity)
                  :schedule-name (schedule-name (:schedule-run/schedule-id entity))
                  :status (:schedule-run/status entity)
                  :started-at (:schedule-run/started-at entity)
                  :finished-at (:schedule-run/finished-at entity)
                  :detail (or (nonblank-str (:schedule-run/error entity))
                              (when-let [result (nonblank-str (:schedule-run/result entity))]
                                (if (> (count result) 160)
                                  (str (subs result 0 159) "…")
                                  result)))})))))

(defn- oauth-attention-items []
  (let [timestamp (.getTime ^Date (now))
        warning-at (+ timestamp attention-expiry-warning-ms)]
    (reduce
      (fn [items account]
        (let [id        (:oauth.account/id account)
              name      (or (:oauth.account/name account)
                            (some-> id name)
                            "OAuth account")
              connected? (boolean (nonblank-str (:oauth.account/access-token account)))
              expires-at (:oauth.account/expires-at account)
              expires-ms (when expires-at (.getTime ^Date expires-at))]
          (cond
            (not connected?)
            (conj items {:type :oauth.attention
                         :severity :warn
                         :title (str "OAuth account needs attention: " name)
                         :detail "Account is not connected."
                         :oauth-account-id id})

            (and expires-ms (<= expires-ms timestamp))
            (conj items {:type :oauth.attention
                         :severity :warn
                         :title (str "OAuth token expired: " name)
                         :detail "Stored access token has expired."
                         :oauth-account-id id})

            (and expires-ms (<= expires-ms warning-at))
            (conj items {:type :oauth.attention
                         :severity :info
                         :title (str "OAuth token expires soon: " name)
                         :detail (str "Token expires at " expires-at)
                         :oauth-account-id id})

            :else
            items)))
      []
      (db/list-oauth-accounts))))

(defn- service-attention-items []
  (let [accounts (into {}
                       (map (fn [account]
                              [(:oauth.account/id account) account]))
                       (db/list-oauth-accounts))]
    (reduce
      (fn [items service]
        (let [id         (:service/id service)
              name       (or (:service/name service)
                             (some-> id name)
                             "Service")
              auth-type  (:service/auth-type service)
              enabled?   (not= false (:service/enabled? service))
              oauth-id   (:service/oauth-account service)
              oauth-acct (get accounts oauth-id)
              auth-key   (nonblank-str (:service/auth-key service))]
          (cond
            (not enabled?)
            items

            (= auth-type :oauth-account)
            (cond
              (nil? oauth-id)
              (conj items {:type :service.attention
                           :severity :warn
                           :title (str "Service needs attention: " name)
                           :detail "OAuth-backed service has no linked account."
                           :service-id id})

              (nil? oauth-acct)
              (conj items {:type :service.attention
                           :severity :warn
                           :title (str "Service needs attention: " name)
                           :detail "Linked OAuth account is missing."
                           :service-id id
                           :oauth-account-id oauth-id})

              (not (nonblank-str (:oauth.account/access-token oauth-acct)))
              (conj items {:type :service.attention
                           :severity :warn
                           :title (str "Service needs attention: " name)
                           :detail "Linked OAuth account is not connected."
                           :service-id id
                           :oauth-account-id oauth-id})

              :else
              items)

            (and (not= auth-type :oauth-account)
                 (not auth-key))
            (conj items {:type :service.attention
                         :severity :warn
                         :title (str "Service needs attention: " name)
                         :detail "Service is enabled but has no configured secret."
                         :service-id id})

            :else
            items)))
      []
      (db/list-services))))

(defn status-snapshot []
  (let [bridge (bridge-config)]
    {:instance {:id (:instance-id bridge)
                :label (:instance-label bridge)
                :version bridge-version}
     :connectivity {:enabled? (:enabled? bridge)
                    :relay-url (:relay-url bridge)
                    :connection-state (:connection-state bridge)
                    :connected-at (:connected-at bridge)
                    :last-seen-at (:last-seen-at bridge)}
     :running (running-schedules)
     :recent-failures (recent-runs-by-status :error 5)
     :recent-successes (recent-runs-by-status :success 5)
     :attention (vec (concat (oauth-attention-items)
                             (service-attention-items)))}))
