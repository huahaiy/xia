(ns xia.plugin-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.permission :as permission]
            [xia.policy :as policy]
            [xia.plugin :as plugin]
            [xia.test-helpers :as th]
            [xia.tool :as tool]))

(use-fixtures :each th/with-test-db)

(def authorization-order (atom []))

(defn authorization-order-handler
  [_arguments]
  (swap! authorization-order conj :handler)
  {"status" "ok"})

(defn- install-stub-tool!
  [tool-id]
  (db/install-tool! {:id tool-id
                     :name (name tool-id)
                     :description "Stub tool"
                     :approval :auto
                     :handler "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! tool-id))

(defn- install-authorization-order-fixture!
  []
  (db/install-tool! {:id :authorization-order-tool
                     :name "Authorization order tool"
                     :description "Proves authorization precedes execution"
                     :approval :always
                     :handler-var 'xia.plugin-test/authorization-order-handler})
  (tool/load-tool! :authorization-order-tool)
  (plugin/install-plugin!
   {:id :authorization-order-plugin
    :name "Authorization order plugin"
    :capabilities #{:hook/pre-tool :hook/post-tool}
    :hooks [{:id :before-handler
             :event :pre-tool
             :handler "(fn [_] {:observed :pre-tool})"}
            {:id :after-handler
             :event :post-tool
             :handler "(fn [_] {:observed :post-tool})"}]})
  (plugin/enable-plugin! :authorization-order-plugin true))

(defn- execution-markers
  [events]
  (into []
        (keep (fn [event]
                (cond
                  (keyword? event) event
                  (:hook-event event) (keyword (:hook-event event)))))
        events))

(deftest plugin-manifest-requires-explicit-hook-capability
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"explicit hook capabilities"
       (plugin/install-plugin!
        {:id :missing-cap
         :name "Missing capability"
         :capabilities #{}
         :hooks [{:id :guard
                  :event :pre-tool
                  :handler "(fn [_] nil)"}]}))))

(deftest denied-authorization-runs-neither-tool-hooks-nor-handler
  (install-authorization-order-fixture!)
  (reset! authorization-order [])
  (let [result (tool/execute-tool
                :authorization-order-tool
                {}
                {:channel :terminal
                 :audit-log authorization-order
                 :permission/approval-callback
                 (fn [_request]
                   (swap! authorization-order conj :authorization-denied)
                   false)})]
    (is (re-find #"user denied approval" (:error result)))
    (is (= [:authorization-denied]
           (execution-markers @authorization-order)))
    (is (not-any? #(contains? % :hook-event)
                  (filter map? @authorization-order)))))

(deftest authorization-precedes-pre-hook-handler-and-post-hook
  (install-authorization-order-fixture!)
  (reset! authorization-order [])
  (let [result (tool/execute-tool
                :authorization-order-tool
                {}
                {:channel :terminal
                 :audit-log authorization-order
                 :permission/approval-callback
                 (fn [_request]
                   (swap! authorization-order conj :authorization-allowed)
                   true)})]
    (is (= {"status" "ok"} result))
    (is (= [:authorization-allowed :pre-tool :handler :post-tool]
           (execution-markers @authorization-order)))))

(deftest tool-hooks-reject-missing-authorization-proof
  (install-authorization-order-fixture!)
  (doseq [event [:pre-tool :post-tool]]
    (let [audit-log (atom [])
          error (try
                  (plugin/run-hooks! event
                                     {:tool-id :authorization-order-tool
                                      :audit-log audit-log})
                  nil
                  (catch clojure.lang.ExceptionInfo e
                    e))]
      (is (= :permission/authorization-required
             (:type (ex-data error))))
      (is (empty? @audit-log)))))

(deftest forged-allowed-decision-runs-neither-tool-hooks-nor-handler
  (install-authorization-order-fixture!)
  (reset! authorization-order [])
  (let [result (with-redefs [permission/authorize-tool!
                             (fn [_tool _arguments _context]
                               {:allowed? true
                                :policy :always
                                :mode :forged
                                :tool-id :authorization-order-tool})]
                 (tool/execute-tool
                  :authorization-order-tool
                  {}
                  {:channel :terminal
                   :audit-log authorization-order}))]
    (is (re-find #"Verified tool authorization is required" (:error result)))
    (is (empty? (execution-markers @authorization-order)))
    (is (not-any? #(contains? % :hook-event)
                  (filter map? @authorization-order)))))

(deftest enabled-pre-tool-hook-can-block-tool-execution
  (install-stub-tool! :blocked-tool)
  (plugin/install-plugin!
   {:id :tool-guard
    :name "Tool guard"
    :capabilities #{:hook/pre-tool}
    :hooks [{:id :block-tool
             :event :pre-tool
             :handler "(fn [event]
                         (when (= (:tool-id event) :blocked-tool)
                           {:allow? false
                            :reason \"blocked by test plugin\"}))"}]})
  (plugin/enable-plugin! :tool-guard true)
  (let [session-id (db/create-session! :terminal)
        result     (tool/execute-tool :blocked-tool
                                      {}
                                      {:channel :terminal
                                       :session-id session-id})
        audit      (db/session-audit-events session-id)]
    (is (= "Tool blocked-tool blocked: blocked by test plugin"
           (:error result)))
    (is (some #(and (= :plugin-hook (:type %))
                    (= "tool-guard" (get-in % [:data "plugin-id"]))
                    (= "pre-tool" (get-in % [:data "hook-event"]))
                    (= "success" (get-in % [:data "status"])))
              audit))))

(deftest disabled-plugin-hooks-do-not-run
  (install-stub-tool! :blocked-tool)
  (plugin/install-plugin!
   {:id :disabled-guard
    :name "Disabled guard"
    :capabilities #{:hook/pre-tool}
    :hooks [{:id :block-tool
             :event :pre-tool
             :handler "(fn [_] {:allow? false :reason \"should not run\"})"}]})
  (is (= {"status" "ok"}
         (tool/execute-tool :blocked-tool {} {:channel :terminal}))))

(deftest hook-handlers-run-in-restricted-sci
  (plugin/install-plugin!
   {:id :sandbox-check
    :name "Sandbox check"
    :capabilities #{:hook/post-llm}
    :hooks [{:id :try-io
             :event :post-llm
             :handler "(fn [_] (slurp \"/etc/passwd\"))"}]})
  (plugin/enable-plugin! :sandbox-check true)
  (let [results (plugin/run-hooks! :post-llm {:session-id (db/create-session! :terminal)
                                              :channel :terminal})]
    (is (= :error (:status (first results))))
    (is (re-find #"slurp|not available|Could not resolve"
                 (:error (first results))))))

(deftest post-tool-hook-runs-through-normal-tool-execution
  (install-stub-tool! :safe-tool)
  (plugin/install-plugin!
   {:id :post-tool-observer
    :name "Post tool observer"
    :capabilities #{:hook/post-tool}
    :hooks [{:id :observe
             :event :post-tool
             :handler "(fn [event]
                         {:tool (:tool-id event)
                          :status (:status event)
                          :result-status (get (:result event) \"status\")})"}]})
  (plugin/enable-plugin! :post-tool-observer true)
  (let [session-id (db/create-session! :terminal)
        result     (tool/execute-tool :safe-tool
                                      {}
                                      {:channel :terminal
                                       :session-id session-id})
        audit      (db/session-audit-events session-id)]
    (is (= {"status" "ok"} result))
    (is (some #(and (= :plugin-hook (:type %))
                    (= "post-tool-observer" (get-in % [:data "plugin-id"]))
                    (= "post-tool" (get-in % [:data "hook-event"]))
                    (= "success" (get-in % [:data "status"])))
              audit))))

(deftest installed-plugin-defaults-to-disabled
  (let [saved (plugin/install-plugin!
               {:id :default-disabled
                :name "Default disabled"
                :capabilities #{:hook/post-llm}
                :hooks [{:id :observe
                         :event :post-llm
                         :handler "(fn [_] {:ran true})"}]})]
    (is (false? (:plugin/enabled? saved)))
    (is (empty? (plugin/run-hooks! :post-llm {:channel :terminal})))))

(deftest hook-timeout-stops-tight-loops
  (plugin/install-plugin!
   {:id :looping-hook
    :name "Looping hook"
    :capabilities #{:hook/post-llm}
    :hooks [{:id :loop
             :event :post-llm
             :handler "(fn [_] (while true nil))"}]})
  (plugin/enable-plugin! :looping-hook true)
  (with-redefs [policy/plugin-hook-timeout-ms (constantly 10)]
    (let [results (plugin/run-hooks! :post-llm {:session-id (db/create-session! :terminal)
                                                :channel :terminal})]
      (is (= :error (:status (first results))))
      (is (re-find #"timed out" (:error (first results)))))))
