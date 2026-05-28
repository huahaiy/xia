(ns xia.bridge-test
  (:require [clojure.test :refer :all]
            [xia.agent :as agent]
            [xia.agent.task-runtime :as task-runtime]
            [xia.bridge :as bridge]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.session-lifecycle :as session-life]
            [xia.task-inspection :as task-inspection]
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

(deftest bridge-finalizes-channel-sessions-through-shared-lifecycle
  (let [session-id (random-uuid)
        call (atom nil)]
    (with-redefs [session-life/finalize!
                  (fn [sid & opts]
                    (reset! call {:session-id sid
                                  :opts (apply hash-map opts)})
                    true)]
      (is (true? (bridge/finalize-channel-session! session-id
                                                   :websocket
                                                   :reason :websocket-close
                                                   :consolidation-mode :sync)))
      (is (= session-id (:session-id @call)))
      (is (= {:reason :websocket-close
              :default-channel :websocket
              :mark-inactive? true
              :consolidation-mode :sync}
             (select-keys (:opts @call)
                          [:reason
                           :default-channel
                           :mark-inactive?
                           :consolidation-mode])))
      (is (= session-life/clear-session-state!
             (:clear-state! (:opts @call)))))))

(deftest bridge-finalizes-active-channel-sessions
  (let [http-session-id (random-uuid)
        command-session-id (random-uuid)
        websocket-session-id (random-uuid)
        finalized (atom [])]
    (with-redefs [db/list-sessions
                  (fn [opts]
                    (is (= {:include-workers? true} opts))
                    [{:id http-session-id
                      :channel :http
                      :active? true}
                     {:id command-session-id
                      :channel :command
                      :active? true}
                     {:id websocket-session-id
                      :channel :websocket
                      :active? true}
                     {:id (random-uuid)
                      :channel :http
                      :active? false}])]
      (is (= 2
             (bridge/finalize-active-channel-sessions!
              #{:http :command}
              (fn [session-id reason]
                (swap! finalized conj [session-id reason]))
              :reason :server-stop)))
      (is (= [[http-session-id :server-stop]
              [command-session-id :server-stop]]
             @finalized)))))

(deftest bridge-records-and-streams-task-runtime-events
  (let [events-atom (atom {})
        subscribers-atom (atom {})
        store (bridge/runtime-event-store events-atom subscribers-atom)
        task-id (random-uuid)
        delivered (atom [])]
    (bridge/register-task-runtime-event-subscriber!
     store
     task-id
     "subscriber-1"
     #(swap! delivered conj %))
    (is (nil? (bridge/handle-task-runtime-event!
               store
               {:type :task.status
                :summary "missing task"})))
    (let [event (bridge/handle-task-runtime-event!
                 store
                 {:type :task.status
                  :task-id task-id
                  :summary "working"
                  :data {:state :running}})]
      (is (= 1 (:stream-index event)))
      (is (some? (:received-at event)))
      (is (= [event] @delivered))
      (is (= event (bridge/latest-task-runtime-status-event store task-id)))
      (is (= {:next-index 1
              :events [event]}
             (bridge/task-runtime-events-after store task-id 0)))
      (is (= {:next-index 1
              :events []}
             (bridge/task-runtime-events-after store task-id 1))))
    (bridge/unregister-task-runtime-event-subscriber! store task-id "subscriber-1")
    (is (empty? @subscribers-atom))))

(deftest bridge-reports-missing-task-for-non-interrupt-control
  (let [{:keys [session-id]} (bridge/create-session! :imessage)]
    (with-redefs [db/current-session-task (constantly nil)]
      (let [result (bridge/apply-control-message! session-id :imessage "pause")]
        (is (= :pause (:intent result)))
        (is (= :task (:scope result)))
        (is (= {:status :missing} (:result result)))
        (is (= "No current task to control." (:text result)))))))

(deftest bridge-builds-channel-task-view
  (let [session-id (random-uuid)
        other-session-id (random-uuid)
        task-id (random-uuid)
        task {:id task-id
              :session-id session-id
              :state :running
              :meta {:runtime {:state :waiting_input}}
              :session-links [{:session-id session-id
                               :role :execution}
                              {:session-id other-session-id
                               :role :observer}]}
        autonomy-state {:stack [{:title "Root goal"}
                                {:title "Current step"
                                 :progress-status :in-progress}]}]
    (with-redefs [task-runtime/task-recovery (constantly {:mode :resume})
                  task-runtime/task-boundary-summary (constantly {:status :inside})
                  task-runtime/task-checkpoint (constantly {:turn 2})
                  task-runtime/task-checkpoint-at (constantly ::checkpoint-at)
                  task-runtime/task-resume-hint (constantly "resume here")
                  task-runtime/task-recovery-brief (constantly "brief")
                  task-inspection/task-inspection
                  (fn [& args]
                    {:arg-count (count args)
                     :compact? (nth args 3 nil)
                     :history-data (nth args 4 nil)})]
      (let [view (bridge/task-view {}
                                   task
                                   {:autonomy-state autonomy-state
                                    :compact? true
                                    :history-data {:turns []}})]
        (is (= :waiting_input (:state view)))
        (is (= :execution (:execution-session-role view)))
        (is (= {:recovery {:mode :resume}
                :boundary-summary {:status :inside}
                :checkpoint {:turn 2}
                :checkpoint-at ::checkpoint-at
                :resume-hint "resume here"
                :recovery-brief "brief"}
               (:runtime-view view)))
        (is (= {:arg-count 5
                :compact? true
                :history-data {:turns []}}
               (:inspection view)))
        (is (= [{:session-id session-id
                 :role :execution
                 :current? true
                 :execution-current? true}
                {:session-id other-session-id
                 :role :observer
                 :current? false
                 :execution-current? false}]
               (:session-links view)))
        (is (= {:depth 2
                :current-focus "Current step"
                :root-goal "Root goal"}
               (select-keys (:stack view) [:depth :current-focus :root-goal])))))))

(deftest bridge-resolves-current-session-task-id
  (let [session-id (random-uuid)
        task-id (random-uuid)
        calls (atom [])]
    (with-redefs [db/current-session-task
                  (fn [sid]
                    (swap! calls conj sid)
                    {:id task-id
                     :session-id sid})]
      (is (= task-id (bridge/current-session-task-id (str session-id))))
      (is (nil? (bridge/current-session-task-id "not-a-uuid"))))
    (is (= [session-id] @calls))))
