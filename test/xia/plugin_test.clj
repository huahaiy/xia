(ns xia.plugin-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.policy :as policy]
            [xia.plugin :as plugin]
            [xia.test-helpers :as th]
            [xia.tool :as tool]))

(use-fixtures :each th/with-test-db)

(defn- install-stub-tool!
  [tool-id]
  (db/install-tool! {:id tool-id
                     :name (name tool-id)
                     :description "Stub tool"
                     :approval :auto
                     :handler "(fn [_] {\"status\" \"ok\"})"})
  (tool/load-tool! tool-id))

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
