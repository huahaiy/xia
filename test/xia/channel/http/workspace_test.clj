(ns xia.channel.http.workspace-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [xia.channel.http.workspace :as http-workspace]
            [xia.db :as db]
            [xia.scratch :as scratch]
            [xia.test-helpers :as th])
  (:import [java.util UUID]))

(use-fixtures :each th/with-test-db)

(defn- parse-session-id
  [value]
  (try
    (UUID/fromString (str value))
    (catch IllegalArgumentException _
      nil)))

(defn- nonblank-str
  [value]
  (let [s (some-> value str str/trim)]
    (when (seq s)
      s)))

(defn- handler-deps
  [{:keys [body session-exists? touches]
    :or {session-exists? (constantly true)
         touches (atom [])}}]
  {:download-response (fn [name media-type bytes]
                        {:status 200
                         :body {:name name
                                :media_type media-type
                                :bytes bytes}})
   :exception-response (fn [^Throwable throwable]
                         {:status (or (:status (ex-data throwable)) 400)
                          :body {:error (.getMessage throwable)}})
   :instant->str (fn [value]
                   (some-> value str))
   :json-response (fn [status body*]
                    {:status status
                     :body body*})
   :multipart-form-request? (constantly false)
   :nonblank-str nonblank-str
   :parse-optional-positive-long (fn [value _field-name]
                                   (some-> (nonblank-str value) Long/parseLong))
   :parse-query-string (constantly {})
   :parse-session-id parse-session-id
   :read-body (constantly body)
   :read-body-bytes identity
   :session-exists? session-exists?
   :throwable-message (fn [^Throwable throwable]
                        (.getMessage throwable))
   :touch-rest-session! (fn [session-id]
                          (swap! touches conj session-id))})

(deftest workspace-session-guard-rejects-invalid-and-missing-sessions
  (let [touches (atom [])
        deps*   (handler-deps {:touches touches
                               :session-exists? (constantly false)})]
    (is (= {:status 400
            :body {:error "invalid session id"}}
           (http-workspace/handle-list-scratch-pads deps* "not-a-uuid")))
    (is (= [] @touches))
    (let [session-id (str (random-uuid))]
      (is (= {:status 404
              :body {:error "session not found"}}
             (http-workspace/handle-list-scratch-pads deps* session-id)))
      (is (= [] @touches)))))

(deftest workspace-scratch-pad-listing-touches-valid-session
  (let [session-id (str (db/create-session! :http))
        touches    (atom [])
        deps*      (handler-deps {:touches touches})
        pad        (scratch/create-pad! {:scope :session
                                         :session-id session-id
                                         :title "Run notes"
                                         :content "Private content"})
        response   (http-workspace/handle-list-scratch-pads deps* session-id)
        pads       (get-in response [:body :pads])]
    (is (= 200 (:status response)))
    (is (= session-id (get-in response [:body :session_id])))
    (is (= [session-id] @touches))
    (is (= [(:id pad)] (mapv :id pads)))
    (is (= ["Run notes"] (mapv :title pads)))
    (is (= [nil] (mapv :content pads)))))

(deftest workspace-scratch-pad-resource-miss-does-not-touch-session
  (let [session-id (str (db/create-session! :http))
        touches    (atom [])
        deps*      (handler-deps {:touches touches})
        response   (http-workspace/handle-get-scratch-pad deps* session-id "missing-pad")]
    (is (= {:status 404
            :body {:error "scratch pad not found"}}
           response))
    (is (= [] @touches))))
