(ns xia.email-test
  (:require [charred.api :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [xia.db :as db]
            [xia.email :as email]
            [xia.test-helpers :refer [with-test-db]])
  (:import [java.nio.charset StandardCharsets]
           [java.util Base64]))

(use-fixtures :each with-test-db)

(defn- encode-base64url
  [text]
  (.encodeToString (.withoutPadding (Base64/getUrlEncoder))
                   (.getBytes ^String text StandardCharsets/UTF_8)))

(defn- decode-base64url
  [text]
  (let [remainder (int (mod (count text) 4))
        padded    (str text
                       (case remainder
                         0 ""
                         2 "=="
                         3 "="
                         1 "==="
                         ""))]
    (String. (.decode (Base64/getUrlDecoder) ^String padded)
             StandardCharsets/UTF_8)))

(deftest list-messages-detects-gmail-service-and-summarizes-results
  (db/register-service! {:id       :work-mail
                         :name     "Work Mail"
                         :base-url "https://gmail.googleapis.com/"
                         :auth-type :bearer
                         :auth-key "tok"})
  (let [requests (atom [])]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (swap! requests conj req)
                    (cond
                      (and (= :get (:method req))
                           (= "https://gmail.googleapis.com/gmail/v1/users/me/messages" (:url req)))
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body "{\"messages\":[{\"id\":\"m1\"}],\"resultSizeEstimate\":99}"}

                      (and (= :get (:method req))
                           (= "https://gmail.googleapis.com/gmail/v1/users/me/messages/m1" (:url req)))
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body "{\"id\":\"m1\",\"threadId\":\"t1\",\"snippet\":\"Need a reply\",\"labelIds\":[\"INBOX\",\"UNREAD\"],\"internalDate\":\"1710000000000\",\"payload\":{\"headers\":[{\"name\":\"Subject\",\"value\":\"Budget update\"},{\"name\":\"From\",\"value\":\"boss@example.com\"},{\"name\":\"To\",\"value\":\"me@example.com\"},{\"name\":\"Date\",\"value\":\"Tue, 09 Jan 2024 10:00:00 +0000\"}]}}"}

                      :else
                      (throw (ex-info "Unexpected Gmail request" {:request req}))))]
      (let [result (email/list-messages :query "from:boss@example.com"
                                        :max-results 5
                                        :unread-only? true)]
        (is (= "work-mail" (:service-id result)))
        (is (= "in:inbox is:unread from:boss@example.com" (:query result)))
        (is (= 1 (:returned-count result)))
        (is (= 99 (:result-size-estimate result)))
        (is (= {:id             "m1"
                :thread-id      "t1"
                :subject        "Budget update"
                :from           "boss@example.com"
                :to             "me@example.com"
                :cc             nil
                :date           "Tue, 09 Jan 2024 10:00:00 +0000"
                :message-id     nil
                :snippet        "Need a reply"
                :labels         ["INBOX" "UNREAD"]
                :unread?        true
                :received-at-ms 1710000000000}
               (first (:messages result))))
        (is (= {"maxResults" 5
                "q" "in:inbox is:unread from:boss@example.com"}
               (:query-params (first @requests))))
        (is (= "Bearer tok" (get-in (first @requests) [:headers "Authorization"])))))))

(deftest list-messages-auto-creates-gmail-service-from-gmail-oauth-account
  (db/register-oauth-account! {:id                :gmail-login
                               :name              "Gmail Login"
                               :provider-template :gmail
                               :access-token      "oauth-token"
                               :token-type        "Bearer"})
  (let [requests (atom [])]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (swap! requests conj req)
                    (cond
                      (and (= :get (:method req))
                           (= "https://gmail.googleapis.com/gmail/v1/users/me/messages" (:url req)))
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body "{\"messages\":[],\"resultSizeEstimate\":0}"}

                      :else
                      (throw (ex-info "Unexpected Gmail request" {:request req}))))]
      (let [result  (email/list-messages :max-results 3)
            service (db/get-service :gmail)]
        (is (= "gmail" (:service-id result)))
        (is (= :oauth-account (:service/auth-type service)))
        (is (= :gmail-login (:service/oauth-account service)))
        (is (= "https://gmail.googleapis.com" (:service/base-url service)))
        (is (= "Bearer oauth-token" (get-in (first @requests) [:headers "Authorization"])))
        (is (= {"maxResults" 3
                "q" "in:inbox"}
               (:query-params (first @requests))))))))

(deftest list-messages-treats-invalid-service-id-text-as-query
  (db/register-service! {:id        :gmail
                         :name      "Gmail"
                         :base-url  "https://gmail.googleapis.com"
                         :auth-type :bearer
                         :auth-key  "tok"})
  (let [requests (atom [])]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (swap! requests conj req)
                    (cond
                      (and (= :get (:method req))
                           (= "https://gmail.googleapis.com/gmail/v1/users/me/messages" (:url req)))
                      {:status 200
                       :headers {"content-type" "application/json"}
                       :body "{\"messages\":[],\"resultSizeEstimate\":0}"}

                      :else
                      (throw (ex-info "Unexpected Gmail request" {:request req}))))]
      (let [result (email/list-messages :service-id "hyang@juji-inc.com"
                                        :max-results 2)]
        (is (= "gmail" (:service-id result)))
        (is (= "in:inbox hyang@juji-inc.com" (:query result)))
        (is (= {"maxResults" 2
                "q" "in:inbox hyang@juji-inc.com"}
               (:query-params (first @requests))))))))

(deftest read-message-prefers-plain-text-and-preserves-newlines
  (db/register-service! {:id       :gmail
                         :name     "Gmail"
                         :base-url "https://gmail.googleapis.com"
                         :auth-type :bearer
                         :auth-key "tok"})
  (with-redefs [xia.http-client/request
                (fn [_]
                  {:status 200
                   :headers {"content-type" "application/json"}
                   :body (json/write-json-str
                           {"id" "m1"
                            "threadId" "t1"
                            "snippet" "Hello World"
                            "labelIds" ["INBOX"]
                            "payload" {"headers" [{"name" "Subject"
                                                   "value" "Status update"}
                                                  {"name" "From"
                                                   "value" "boss@example.com"}]
                                       "parts" [{"mimeType" "text/plain"
                                                 "body" {"data" (encode-base64url "Hello,\nWorld")}}
                                                {"mimeType" "text/html"
                                                 "body" {"data" (encode-base64url "<p>Hello,<br>World</p>")}}]}})})]
    (let [result (email/read-message "m1")]
      (is (= "gmail" (:service-id result)))
      (is (= "Status update" (:subject result)))
      (is (= "boss@example.com" (:from result)))
      (is (= "Hello,\nWorld" (:body result)))
      (is (= :plain (:body-kind result))))))

(deftest send-message-encodes-rfc822-payload
  (db/register-service! {:id       :gmail
                         :name     "Gmail"
                         :base-url "https://gmail.googleapis.com"
                         :auth-type :bearer
                         :auth-key "tok"})
  (let [captured (atom nil)]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (reset! captured req)
                    {:status 200
                     :headers {"content-type" "application/json"}
                     :body "{\"id\":\"sent-1\",\"threadId\":\"thread-7\",\"labelIds\":[\"SENT\"]}"})]
      (let [result (email/send-message "alice@example.com"
                                       "Quarterly update"
                                       "All done."
                                       :cc "ops@example.com"
                                       :reply-to "me@example.com"
                                       :in-reply-to "<prior@example.com>"
                                       :references ["<older-1@example.com>" "<older-2@example.com>"]
                                       :thread-id "thread-7")
            payload (json/read-json (:body @captured))
            raw     (decode-base64url (get payload "raw"))]
        (is (= {:service-id "gmail"
                :status     "sent"
                :id         "sent-1"
                :thread-id  "thread-7"
                :labels     ["SENT"]}
               result))
        (is (= :post (:method @captured)))
        (is (= "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
               (:url @captured)))
        (is (= "application/json" (get-in @captured [:headers "Content-Type"])))
        (is (= "thread-7" (get payload "threadId")))
        (is (re-find #"(?m)^To: alice@example.com$" raw))
        (is (re-find #"(?m)^Cc: ops@example.com$" raw))
        (is (re-find #"(?m)^Reply-To: me@example.com$" raw))
        (is (re-find #"(?m)^In-Reply-To: <prior@example.com>$" raw))
        (is (re-find #"(?m)^References: <older-1@example.com> <older-2@example.com>$" raw))
        (is (re-find #"(?m)^Subject: Quarterly update$" raw))
        (is (str/includes? raw "\r\n\r\nAll done."))))))

(deftest delete-message-moves-message-to-trash-by-default
  (db/register-service! {:id        :gmail
                         :name      "Gmail"
                         :base-url  "https://gmail.googleapis.com"
                         :auth-type :bearer
                         :auth-key  "tok"})
  (let [captured (atom nil)]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (reset! captured req)
                    {:status 200
                     :headers {"content-type" "application/json"}
                     :body "{\"id\":\"m1\",\"threadId\":\"t1\",\"labelIds\":[\"TRASH\"]}"})]
      (let [result (email/delete-message "m1")]
        (is (= {:service-id "gmail"
                :status "trashed"
                :id "m1"
                :thread-id "t1"
                :labels ["TRASH"]}
               result))
        (is (= :post (:method @captured)))
        (is (= "https://gmail.googleapis.com/gmail/v1/users/me/messages/m1/trash"
               (:url @captured)))))))

(deftest delete-message-permanently-deletes-when-requested
  (db/register-service! {:id        :gmail
                         :name      "Gmail"
                         :base-url  "https://gmail.googleapis.com"
                         :auth-type :bearer
                         :auth-key  "tok"})
  (let [captured (atom nil)]
    (with-redefs [xia.http-client/request
                  (fn [req]
                    (reset! captured req)
                    {:status 204
                     :headers {}
                     :body ""})]
      (let [result (email/delete-message "m1" :permanent? true)]
        (is (= {:service-id "gmail"
                :status "deleted"
                :id "m1"
                :thread-id nil
                :labels []}
               result))
        (is (= :delete (:method @captured)))
        (is (= "https://gmail.googleapis.com/gmail/v1/users/me/messages/m1"
               (:url @captured)))))))
