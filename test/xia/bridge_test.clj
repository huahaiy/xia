(ns xia.bridge-test
  (:require [clojure.test :refer :all]
            [xia.agent :as agent]
            [xia.bridge :as bridge]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.session-lifecycle :as session-life]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(deftest bridge-creates-sessions-and-forwards-messages
  (let [{:keys [session-id channel]} (bridge/create-session! :http)
        calls (atom [])]
    (is (= :http channel))
    (is (uuid? session-id))
    (with-redefs [agent/process-message
                  (fn [sid text & {:as opts}]
                    (swap! calls conj {:session-id sid
                                       :text text
                                       :opts opts})
                    "assistant response")]
      (is (= "assistant response"
             (bridge/send-message! session-id
                                   "hello"
                                   :channel :http
                                   :local-doc-ids ["doc-1"]
                                   :artifact-ids ["artifact-1"]))))
    (is (= [{:session-id session-id
             :text "hello"
             :opts {:channel :http
                    :local-doc-ids ["doc-1"]
                    :artifact-ids ["artifact-1"]}}]
           @calls))))

(deftest bridge-submits-pending-prompt-and-approval-replies
  (let [{:keys [session-id]} (bridge/create-session! :http)
        prompt*  (prompt/register-interaction! {:interaction-id "prompt-1"
                                                :prompt-id "public-prompt-1"
                                                :kind :prompt
                                                :channel :http
                                                :session-id session-id
                                                :label "OTP"
                                                :response (promise)})]
    (try
      (is (= :stale
             (:status (bridge/submit-interaction! {:session-id session-id
                                                   :kind :prompt}
                                                  "wrong-id"
                                                  "123456"))))
      (is (= :delivered
             (:status (bridge/submit-interaction! {:session-id session-id
                                                   :kind :prompt}
                                                  "public-prompt-1"
                                                  "123456"))))
      (is (= "123456" (deref (:response prompt*) 0 nil)))
      (finally
        (prompt/clear-pending-interaction! {:interaction-id (:interaction-id prompt*)})))
    (let [approval (prompt/register-interaction! {:interaction-id "approval-1"
                                                  :approval-id "public-approval-1"
                                                  :kind :approval
                                                  :channel :http
                                                  :session-id session-id
                                                  :tool-id :browser-open
                                                  :response (promise)})]
      (try
        (is (= "public-approval-1"
               (:approval-id (bridge/pending-interaction {:session-id session-id
                                                          :kind :approval}))))
        (is (= :invalid
               (:status (bridge/submit-freeform-reply! {:session-id session-id
                                                        :kind :approval}
                                                       "maybe"))))
        (is (= "Still waiting for approval. Reply YES, NO, or CANCEL."
               (bridge/interaction-retry-text approval)))
        (is (= :delivered
               (:status (bridge/submit-freeform-reply! {:session-id session-id
                                                        :kind :approval}
                                                       "yes"))))
        (is (= :allow (deref (:response approval) 0 nil)))
        (finally
          (prompt/clear-pending-interaction! {:interaction-id (:interaction-id approval)}))))))

(deftest bridge-applies-task-control-messages
  (let [{:keys [session-id]} (bridge/create-session! :slack)
        task-id (random-uuid)
        calls (atom [])]
    (with-redefs [db/current-session-task
                  (fn [sid]
                    (is (= session-id sid))
                    {:id task-id
                     :session-id session-id})
                  agent/pause-task!
                  (fn [id]
                    (swap! calls conj [:pause id])
                    {:status :pausing
                     :task-id id
                     :session-id session-id})]
      (let [result (bridge/apply-control-message! session-id :slack "pause")]
        (is (= :pause (:intent result)))
        (is (= :task (:scope result)))
        (is (= "Pausing the current task." (:text result)))
        (is (= {:status :pausing
                :task-id task-id
                :session-id session-id}
               (:result result)))))
    (is (= [[:pause task-id]] @calls))))

(deftest bridge-applies-session-control-when-no-task-is-active
  (let [{:keys [session-id]} (bridge/create-session! :telegram)
        calls (atom [])]
    (with-redefs [db/current-session-task (constantly nil)
                  agent/cancel-session!
                  (fn [sid reason]
                    (swap! calls conj [sid reason])
                    true)]
      (let [result (bridge/apply-control-message! session-id :telegram "cancel")]
        (is (= :interrupt (:intent result)))
        (is (= :session (:scope result)))
        (is (= {:status :cancelling
                :session-id session-id}
               (:result result)))
        (is (= "Cancelling the current session." (:text result)))))
    (is (= [[session-id "session cancel requested"]] @calls))))

(deftest bridge-closes-sessions-through-shared-lifecycle
  (let [{:keys [session-id]} (bridge/create-session! :http)]
    (is (true? (session-life/active? session-id)))
    (is (= {:status :closed
            :session-id session-id}
           (select-keys (bridge/control-session! session-id
                                                 :close
                                                 :reason "session close requested"
                                                 :context {:session-id session-id
                                                           :channel :http})
                        [:status :session-id])))
    (is (false? (session-life/active? session-id)))
    (is (= :already-closed
           (:status (bridge/control-session! session-id
                                             :close
                                             :reason "session close requested"
                                             :context {:session-id session-id
                                                       :channel :http}))))))
