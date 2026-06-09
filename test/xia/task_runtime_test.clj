(ns xia.task-runtime-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.agent.branch :as branch]
            [xia.agent.task-runtime :as task-runtime]
            [xia.async :as async]
            [xia.db :as db]
            [xia.test-helpers :as th]
            [xia.working-memory :as wm]))

(use-fixtures :each th/with-test-db)

(defn- session-by-id
  [session-id]
  (some #(when (= session-id (:id %)) %)
        (db/list-sessions {:include-workers? true})))

(deftest branch-task-uses-shared-worker-lifecycle
  (let [parent-session-id (db/create-session! :terminal)
        parent-task-id    (db/create-task! {:session-id parent-session-id
                                            :channel :terminal
                                            :type :task
                                            :state :running
                                            :title "Parent task"
                                            :summary "Parent task"
                                            :contract {:kind :task
                                                       :version 1
                                                       :goal "Parent task"}})
        registered        (atom [])
        unregistered      (atom [])
        runs              (atom [])
        deps              {:throw-if-runtime-stopping! (fn [_session-id] nil)
                           :throw-if-cancelled! (fn [_session-id] nil)
                           :trace-context (fn [_context]
                                            {:request-id "parent-request"})
                           :new-request-id (fn [] "branch-request")
                           :register-child-session! (fn [parent child]
                                                      (swap! registered conj
                                                             [parent child]))
                           :unregister-child-session! (fn [parent child]
                                                        (swap! unregistered conj
                                                               [parent child]))
                           :throwable-detail (fn [t] {:message (.getMessage t)})
                           :run-task-spec! (fn [child-task-id & {:as opts}]
                                             (swap! runs conj
                                                    (assoc opts
                                                           :task-id child-task-id))
                                             {:status :completed
                                              :state {:outputs
                                                      {:work-on-branch
                                                       "branch done"}}})}
        result            (branch/run-branch-task*
                           deps
                           parent-session-id
                           {:task "Collect sources"
                            :prompt "Find the current source list"}
                           {:parent-task-id parent-task-id
                            :objective "Write a report"
                            :resource-session-id :resource-session
                            :tool-context {:source :test}})
        child-task-id     (:task-id result)
        child-session-id  (:session-id result)
        child-task        (db/get-task child-task-id)
        run               (first @runs)]
    (is (= "completed" (:status result)))
    (is (= "branch done" (:result result)))
    (is (= parent-task-id (:parent-id child-task)))
    (is (= "Find the current source list"
           (get-in child-task [:contract :prompt])))
    (is (= "Write a report"
           (get-in child-task [:contract :objective])))
    (is (= parent-task-id
           (get-in child-task [:contract :parent-task-id])))
    (is (re-find #"Parent objective:\nWrite a report"
                 (get-in child-task [:contract :spec :steps 0 :prompt])))
    (is (true? (get-in child-task [:meta :branch-worker])))
    (is (= parent-session-id
           (get-in child-task [:meta :parent-session-id])))
    (is (= :resource-session
           (get-in child-task [:meta :resource-session-id])))
    (is (= [[parent-session-id child-session-id]] @registered))
    (is (= @registered @unregistered))
    (is (= :start (:runtime-op run)))
    (is (= :branch-spawn (:operation run)))
    (is (= :resource-session (:resource-session-id run)))
    (is (= {:branch-worker? true
            :parent-session-id parent-session-id
            :resource-session-id :resource-session
            :request-id "branch-request"
            :channel :branch
            :correlation-id "parent-request"
            :parent-request-id "parent-request"
            :source :test}
           (:tool-context run)))
    (is (false? (:active? (session-by-id child-session-id))))
    (is (nil? (wm/get-wm child-session-id)))))

(deftest fork-task-creates-attached-child-and-cleans-up-worker-session
  (let [parent-session-id (db/create-session! :terminal)
        parent-task-id    (db/create-task! {:session-id parent-session-id
                                            :channel :terminal
                                            :type :task
                                            :state :running
                                            :title "Parent task"
                                            :summary "Parent task"
                                            :contract {:kind :task
                                                       :version 1
                                                       :goal "Parent task"}})
        registered        (atom [])
        unregistered      (atom [])
        runs              (atom [])
        deps              {:truncate-summary (fn [text _limit] text)
                           :register-child-session! (fn [parent child]
                                                      (swap! registered conj
                                                             [parent child]))
                           :unregister-child-session! (fn [parent child]
                                                        (swap! unregistered conj
                                                               [parent child]))
                           :run-task-spec! (fn [child-task-id & {:as opts}]
                                             (wm/create-wm! (:session-id
                                                            (db/get-task child-task-id)))
                                             (swap! runs conj
                                                    (assoc opts
                                                           :task-id child-task-id))
                                             {:status :completed})}]
    (with-redefs [async/submit-background! (fn [_label f]
                                             (f)
                                             ::future)]
      (let [result          (task-runtime/fork-task! deps
                                                     parent-task-id
                                                     "Research pricing")
            child-task-id   (:task-id result)
            child-session-id (:session-id result)
            child-task      (db/get-task child-task-id)
            parent-task     (db/get-task parent-task-id)]
        (is (= :forking (:status result)))
        (is (= parent-task-id (:parent-id child-task)))
        (is (= :branch (:channel child-task)))
        (is (= :branch (get-in child-task [:meta :trigger :kind])))
        (is (= :agent (get-in child-task [:meta :execution :mode])))
        (is (= "Research pricing"
               (get-in child-task [:contract :spec :steps 0 :prompt])))
        (is (= [[parent-session-id child-session-id]] @registered))
        (is (= @registered @unregistered))
        (is (= [{:message "Research pricing"
                 :channel :branch
                 :runtime-op :fork
                 :operation :branch-spawn
                 :resource-session-id parent-session-id
                 :tool-context {:branch-worker? true
                                :parent-session-id parent-session-id
                                :resource-session-id parent-session-id}
                 :task-id child-task-id}]
               @runs))
        (is (= "Delegated child task: Research pricing"
               (:summary parent-task)))
        (is (false? (:active? (session-by-id child-session-id))))
        (is (nil? (wm/get-wm child-session-id)))))))
