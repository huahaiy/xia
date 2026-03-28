(ns xia.instance-supervisor-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.instance-supervisor :as instance-supervisor]
            [xia.paths :as paths]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(defn- fake-process
  []
  (let [alive?      (atom true)
        exit-code   (atom 0)
        destroy-log (atom [])
        self        (atom nil)
        process     (proxy [Process] []
                      (getOutputStream [] nil)
                      (getInputStream [] nil)
                      (getErrorStream [] nil)
                      (waitFor [] (reset! alive? false) @exit-code)
                      (exitValue []
                        (if @alive?
                          (throw (IllegalThreadStateException. "process still running"))
                          @exit-code))
                      (destroy []
                        (swap! destroy-log conj :soft)
                        (reset! alive? false))
                      (destroyForcibly []
                        (swap! destroy-log conj :force)
                        (reset! alive? false)
                        @self)
                      (isAlive [] @alive?)
                      (pid [] 4242))]
    (reset! self process)
    {:process process
     :alive? alive?
     :exit-code exit-code
     :destroy-log destroy-log}))

(defn- with-reset-supervisor
  [f]
  (instance-supervisor/shutdown!)
  (instance-supervisor/configure! {:enabled? false})
  (try
    (f)
    (finally
      (instance-supervisor/shutdown!)
      (instance-supervisor/configure! {:enabled? false}))))

(deftest capability-disabled-blocks-instance-start
  (with-reset-supervisor
    #(do
       (instance-supervisor/set-instance-management-enabled! true)
       (let [error (try
                     (instance-supervisor/start-instance! "ops-child")
                     nil
                     (catch clojure.lang.ExceptionInfo e
                       (ex-data e)))]
         (is (= :instance-supervisor/capability-disabled
                (:type error)))
         (is (= :host (:scope error)))))))

(deftest default-instance-starts-with-controller-mode-enabled
  (with-reset-supervisor
    #(do
       (instance-supervisor/configure! {:enabled? true})
       (with-redefs [xia.db/current-instance-id (constantly paths/default-instance-id)]
         (is (= true (instance-supervisor/instance-management-configured?)))
         (is (= true (instance-supervisor/instance-management-enabled?)))))))

(deftest non-default-instance-starts-with-controller-mode-disabled
  (with-reset-supervisor
    #(do
       (instance-supervisor/configure! {:enabled? true})
       (with-redefs [xia.db/current-instance-id (constantly "ops-child")]
         (is (= false (instance-supervisor/instance-management-configured?)))
         (is (= false (instance-supervisor/instance-management-enabled?)))))))

(deftest default-instance-can-be-explicitly-disabled
  (with-reset-supervisor
    #(do
       (instance-supervisor/configure! {:enabled? true})
       (with-redefs [xia.db/current-instance-id (constantly paths/default-instance-id)]
         (instance-supervisor/set-instance-management-enabled! false)
         (is (= false (instance-supervisor/instance-management-configured?)))
         (is (= false (instance-supervisor/instance-management-enabled?)))))))

(deftest start-and-stop-managed-instance-registers-service
  (with-reset-supervisor
    #(let [{:keys [process destroy-log]} (fake-process)
           spawn-call (atom nil)]
       (instance-supervisor/configure! {:enabled? true
                                        :command "/opt/xia/bin/xia"})
       (instance-supervisor/set-instance-management-enabled! true)
       (with-redefs [xia.instance-supervisor/spawn-process!
                     (fn [command args env log-path]
                       (reset! spawn-call {:command command
                                           :args args
                                           :env env
                                           :log-path log-path})
                       process)
                     xia.instance-supervisor/wait-until-ready!
                     (fn [_base-url _process _wait-for-ready-ms _log-path]
                       true)
                     xia.instance-supervisor/start-exit-watcher!
                     (fn [_instance-id _process]
                       nil)
                     xia.instance-supervisor/wait-for-exit
                     (fn [_process _timeout-ms]
                       true)]
         (let [started (instance-supervisor/start-instance! "Ops Child"
                                                            :template-instance "Base Config"
                                                            :port 4115
                                                            :service-name "Ops Child")
               service (db/get-service :xia-managed-instance-ops-child)
               managed (db/get-managed-child :ops-child)
               listed  (instance-supervisor/list-managed-instances)
               status  (instance-supervisor/instance-status "ops-child")
               stopped (instance-supervisor/stop-instance! "ops-child")]
           (is (= "/opt/xia/bin/xia" (:command @spawn-call)))
           (is (= ["--instance" "ops-child"
                   "--mode" "server"
                   "--bind" "127.0.0.1"
                   "--port" "4115"
                   ]
                  (subvec (:args @spawn-call) 0 8)))
           (is (= "--log-file" (get-in @spawn-call [:args 8])))
           (is (.endsWith ^String (get-in @spawn-call [:args 9])
                          "/.xia/instances/ops-child/xia.log"))
           (is (= ["--template-instance" "base-config"]
                  (subvec (:args @spawn-call) 10 12)))
           (is (string? (get-in @spawn-call [:env "XIA_COMMAND_TOKEN"])))
           (is (= "false" (get-in @spawn-call [:env "XIA_ALLOW_INSTANCE_MANAGEMENT"])))
           (is (= "ops-child" (:instance_id started)))
           (is (= "running" (:state started)))
           (is (= "base-config" (:template_instance started)))
           (is (= "http://127.0.0.1:4115" (:base_url started)))
           (is (= 4242 (:pid started)))
           (is (= "Ops Child" (:service/name service)))
           (is (= "Ops Child" (:managed.child/name managed)))
           (is (= :running (:managed.child/state managed)))
           (is (= :xia-managed-instance-ops-child (:managed.child/service-id managed)))
           (is (= :bearer (:service/auth-type service)))
           (is (= "http://127.0.0.1:4115" (:service/base-url service)))
           (is (= true (:service/allow-private-network? service)))
           (is (= false (:service/autonomous-approved? service)))
           (is (= true (:service/enabled? service)))
            (is (= 1 (count listed)))
           (is (= (:instance_id started) (:instance_id status)))
           (is (= "ops-child" (:instance_id stopped)))
           (is (= "exited" (:state stopped)))
           (is (= [:soft] @destroy-log))
           (is (= :exited (:managed.child/state (db/get-managed-child :ops-child))))
           (is (= false (:service/enabled? (db/get-service :xia-managed-instance-ops-child)))))))))

(deftest stop-instance-reports-missing-instance
  (with-reset-supervisor
    #(do
       (instance-supervisor/configure! {:enabled? true})
       (instance-supervisor/set-instance-management-enabled! true)
       (let [error (try
                     (instance-supervisor/stop-instance! "missing-child")
                     nil
                     (catch clojure.lang.ExceptionInfo e
                       (ex-data e)))]
         (is (= :instance-supervisor/not-found
                (:type error)))))))

(deftest stop-instance-can-use-persisted-child-link-after-controller-restart
  (with-reset-supervisor
    #(do
       (instance-supervisor/configure! {:enabled? true})
       (instance-supervisor/set-instance-management-enabled! true)
       (db/save-managed-child! {:id                :ops-child
                                :name              "Ops Child"
                                :service-id        :xia-managed-instance-ops-child
                                :service-name      "Ops Child"
                                :base-url          "http://127.0.0.1:4115"
                                :template-instance "base-config"
                                :state             :running
                                :started-at        (java.util.Date.)})
       (db/save-service! {:id                     :xia-managed-instance-ops-child
                          :name                   "Ops Child"
                          :base-url               "http://127.0.0.1:4115"
                          :auth-type              :bearer
                          :auth-key               "child-token"
                          :allow-private-network? true
                          :enabled?               true})
       (let [requests (atom [])
             alive?   (atom true)]
         (with-redefs [xia.http-client/request
                       (fn [req]
                         (swap! requests conj req)
                         (reset! alive? false)
                         {:status 202 :body "{\"status\":\"stopping\"}"})
                       xia.instance-supervisor/child-ready?
                       (fn [_]
                         @alive?)]
           (let [stopped (instance-supervisor/stop-instance! "ops-child")]
             (is (= "ops-child" (:instance_id stopped)))
             (is (= "exited" (:state stopped)))
             (is (= false (:attached stopped)))
             (is (= false (:service/enabled? (db/get-service :xia-managed-instance-ops-child))))
             (is (= :exited (:managed.child/state (db/get-managed-child :ops-child))))
             (is (= "http://127.0.0.1:4115/command/shutdown"
                    (:url (first @requests))))))))))

(deftest record-parent-link-from-env-persists-parent-instance-id
  (with-reset-supervisor
    #(with-redefs [xia.instance-supervisor/env-value
                   (fn [key]
                     (case key
                       "XIA_PARENT_INSTANCE_ID" "Controller Xia"
                       nil))]
       (let [result (instance-supervisor/record-parent-link-from-env!)]
         (is (= "controller-xia" (:parent_instance_id result)))
         (is (= "controller-xia" (db/get-config :instance/parent-instance-id)))
         (is (= "controller-xia" (instance-supervisor/parent-instance-id)))))))
