(ns xia.channel.http.session-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [org.httpkit.server :as http]
            [xia.bridge :as bridge]
            [xia.channel.http.interaction :as http-interaction]
            [xia.channel.http.session :as http-session]
            [xia.db :as db]
            [xia.test-helpers :as th])
  (:import [java.util Date UUID]))

(use-fixtures :each th/with-test-db)

(defn- parse-session-id
  [value]
  (try
    (UUID/fromString (str value))
    (catch IllegalArgumentException _
      nil)))

(defn- deps
  [{:keys [touches resumes active? accessible?]
    :or {active? true
         accessible? true}}]
  {:approval->body http-interaction/approval->body
   :json-response (fn [status body]
                    {:status status
                     :body body})
   :maybe-resume-http-session! (fn [session-id expected-channel]
                                 (swap! resumes conj
                                        [session-id expected-channel]))
   :parse-session-id parse-session-id
   :prompt->body http-interaction/prompt->body
   :read-body :body
   :session-accessible? (fn [_session-id _expected-channel]
                          accessible?)
   :session-active? (fn [_session-id]
                      active?)
   :touch-rest-session! (fn [session-id]
                          (swap! touches conj session-id))})

(deftest session-prompt-handlers-use-shared-interaction-flow
  (let [session-id (db/create-session! :http)
        response   (promise)
        touches    (atom [])
        resumes    (atom [])
        deps*      (deps {:touches touches
                          :resumes resumes})
        prompt-id  "prompt-1"]
    (bridge/register-interaction!
     {:interaction-id prompt-id
      :kind :prompt
      :channel :http
      :session-id (str session-id)
      :prompt-id prompt-id
      :label "Enter code"
      :created-at (Date.)
      :response response})
    (let [get-response (http-session/handle-get-prompt deps*
                                                       (str session-id)
                                                       :http)]
      (is (= 200 (:status get-response)))
      (is (= true (get-in get-response [:body :pending])))
      (is (= prompt-id
             (get-in get-response [:body :prompt :prompt_id])))
      (is (= [[(str session-id) :http]] @resumes))
      (is (= [(str session-id)] @touches)))
    (let [submit-response (http-session/handle-submit-prompt
                           deps*
                           (str session-id)
                           {:body {"prompt_id" prompt-id
                                   "value" "123456"}}
                           :http)]
      (is (= 200 (:status submit-response)))
      (is (= {:status "recorded"} (:body submit-response)))
      (is (= "123456" (deref response 0 ::missing)))
      (is (= [(str session-id) (str session-id)] @touches)))))

(deftest task-approval-handlers-use-shared-interaction-flow
  (let [session-id  (db/create-session! :http)
        task-id     (db/create-task! {:session-id session-id
                                      :channel :http
                                      :type :task
                                      :state :waiting_approval
                                      :title "Approve tool"})
        response    (promise)
        touches     (atom [])
        resumes     (atom [])
        deps*       (deps {:touches touches
                           :resumes resumes})
        approval-id "approval-1"]
    (bridge/register-interaction!
     {:interaction-id approval-id
      :kind :approval
      :channel :http
      :session-id (str session-id)
      :task-id task-id
      :approval-id approval-id
      :tool-id :dangerous-tool
      :tool-name "dangerous-tool"
      :description "Run a dangerous tool"
      :policy :manual
      :created-at (Date.)
      :response response})
    (let [get-response (http-session/handle-get-task-approval deps*
                                                              (str task-id))]
      (is (= 200 (:status get-response)))
      (is (= true (get-in get-response [:body :pending])))
      (is (= approval-id
             (get-in get-response [:body :approval :approval_id]))))
    (let [submit-response (http-session/handle-submit-task-approval
                           deps*
                           (str task-id)
                           {:body {"approval_id" approval-id
                                   "decision" "deny"}})]
      (is (= 200 (:status submit-response)))
      (is (= {:status "recorded"} (:body submit-response)))
      (is (= :deny (deref response 0 ::missing)))
      (is (= [] @touches))
      (is (= [] @resumes)))))

(deftest interaction-handlers-preserve-validation-errors
  (let [session-id (db/create-session! :http)
        touches    (atom [])
        resumes    (atom [])
        deps*      (deps {:touches touches
                          :resumes resumes})]
    (is (= {:status 400
            :body {:error "missing value"}}
           (http-session/handle-submit-prompt deps*
                                              (str session-id)
                                              {:body {"prompt_id" "p1"}}
                                              :http)))
    (is (= {:status 400
            :body {:error "invalid task id"}}
           (http-session/handle-get-task-approval deps* "not-a-uuid")))))

(deftest task-event-stream-opens-with-a-nonempty-sse-comment
  (let [session-id (db/create-session! :http)
        task-id    (db/create-task! {:session-id session-id
                                     :channel :http
                                     :type :task
                                     :state :completed
                                     :title "Completed task"})
        sends      (atom [])
        deps*      {:json-response (fn [status body]
                                     {:status status :body body})
                    :parse-query-string (constantly {})
                    :register-task-runtime-stream-subscriber!
                    (fn [_task-id _subscriber-id _callback])
                    :task-runtime-events-after
                    (fn [_task-id after]
                      {:next-index after :events []})
                    :unregister-task-runtime-stream-subscriber!
                    (fn [_task-id _subscriber-id])}]
    (with-redefs [http/as-channel (fn [_req callbacks]
                                    ((:on-open callbacks) ::channel))
                  http/send! (fn [channel payload close?]
                               (swap! sends conj [channel payload close?]))]
      (http-session/handle-get-task-event-stream
       deps*
       (str task-id)
       {:query-string "" :headers {}}))
    (let [[channel response close?] (first @sends)]
      (is (= ::channel channel))
      (is (= 200 (:status response)))
      (is (= "text/event-stream; charset=utf-8"
             (get-in response [:headers "content-type"])))
      (is (= ": connected\n\n" (:body response)))
      (is (false? close?)))))
