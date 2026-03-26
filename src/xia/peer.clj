(ns xia.peer
  "Helpers for one Xia instance to communicate with another through the
   command channel, using stored service definitions so tokens never reach
   tool arguments."
  (:require [clojure.string :as str]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.service :as service])
  (:import [java.net URI]))

(def ^:private loopback-hosts #{"localhost" "127.0.0.1" "::1" "[::1]"})

(defn- normalize-service-id
  [service-id]
  (cond
    (keyword? service-id) service-id
    (string? service-id)  (keyword service-id)
    :else (throw (ex-info "peer service_id must be a string or keyword"
                          {:service_id service-id}))))

(defn- nonblank-string
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s) s)))

(defn- loopback-base-url?
  [base-url]
  (try
    (contains? loopback-hosts
               (some-> base-url URI. .getHost str/lower-case))
    (catch Exception _
      false)))

(defn- bearer-peer-service?
  [service]
  (and (not (false? (:service/enabled? service)))
       (= :bearer (:service/auth-type service))
       (not (str/blank? (or (:service/base-url service) "")))))

(defn- visible-peer-service?
  [service]
  (and (bearer-peer-service? service)
       (or (not (autonomous/autonomous-run?))
           (autonomous/service-approved? (:service/id service)))))

(defn list-peers
  "List configured bearer-auth services that can be used as Xia peers."
  []
  (->> (db/list-services)
       (filter visible-peer-service?)
       (sort-by (fn [service]
                  [(str/lower-case (or (:service/name service)
                                       (name (:service/id service))))
                   (name (:service/id service))]))
       (mapv (fn [service]
               {:service_id            (name (:service/id service))
                :name                  (:service/name service)
                :base_url              (:service/base-url service)
                :allow_private_network (boolean (:service/allow-private-network? service))
                :local                 (loopback-base-url? (:service/base-url service))
                :autonomous_approved   (boolean (autonomous/service-autonomous-approved? service))}))))

(defn- response-error-message
  [body]
  (cond
    (map? body)    (or (nonblank-string (get body "error"))
                       (nonblank-string (get body :error))
                       (pr-str body))
    (string? body) (nonblank-string body)
    :else          nil))

(defn chat
  "Send a chat message to another Xia instance through a configured service.

   Options:
     :session-id  continue an existing peer command session
     :timeout-ms  override the service-request timeout"
  [service-id message & {:keys [session-id timeout-ms]}]
  (let [service-id* (normalize-service-id service-id)
        message*    (nonblank-string message)]
    (when-not message*
      (throw (ex-info "peer message must be a non-empty string"
                      {:message message})))
    (let [body     (cond-> {"message" message*}
                     session-id (assoc "session_id" (str session-id)))
          response (service/request service-id*
                                    :post
                                    "/command/chat"
                                    :body body
                                    :as :json
                                    :timeout timeout-ms)
          status   (:status response)
          body     (:body response)]
      (if (= 200 status)
        {:service_id (name service-id*)
         :session_id (or (get body "session_id")
                         (get body :session_id))
         :role       (or (get body "role")
                         (get body :role)
                         "assistant")
         :content    (or (get body "content")
                         (get body :content))}
        (throw (ex-info (str "peer request failed"
                             (when-let [msg (response-error-message body)]
                               (str ": " msg)))
                        {:service_id service-id*
                         :status     status
                         :body       body}))))))
