(ns xia.mcp-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xia.db :as db]
            [xia.mcp :as mcp]
            [xia.test-helpers :as th]
            [xia.tool :as tool]))

(use-fixtures :each th/with-test-db)

(deftest mcp-initialize-advertises-tools-capability
  (let [response (mcp/handle-json-rpc
                  {"jsonrpc" "2.0"
                   "id" 1
                   "method" "initialize"
                   "params" {"protocolVersion" "2025-11-25"}})]
    (is (= "2.0" (get response "jsonrpc")))
    (is (= "2025-11-25"
           (get-in response ["result" "protocolVersion"])))
    (is (= false
           (get-in response ["result" "capabilities" "tools" "listChanged"])))))

(deftest mcp-lists-only-allowlisted-tools
  (db/set-config! :mcp/tool-allowlist "[:mcp-echo]")
  (db/install-tool! {:id :mcp-echo
                     :name "mcp-echo"
                     :description "Echo test"
                     :parameters {"type" "object"
                                  "properties" {"value" {"type" "string"}}}
                     :approval :auto
                     :handler "(fn [args] {:echo (get args \"value\")})"})
  (db/install-tool! {:id :not-exposed
                     :name "not-exposed"
                     :description "Hidden test"
                     :approval :auto
                     :handler "(fn [_] {:hidden true})"})
  (let [response (mcp/handle-json-rpc {"id" 2
                                       "method" "tools/list"})
        names    (->> (get-in response ["result" "tools"])
                      (map #(get % "name"))
                      set)]
    (is (= #{"mcp-echo"} names))))

(deftest mcp-calls-allowlisted-tools-through-normal-tool-runtime
  (db/set-config! :mcp/tool-allowlist "[:mcp-echo]")
  (db/install-tool! {:id :mcp-echo
                     :name "mcp-echo"
                     :description "Echo test"
                     :approval :auto
                     :handler "(fn [args] {:echo (get args \"value\")})"})
  (tool/load-tool! :mcp-echo)
  (let [response (mcp/handle-json-rpc
                  {"id" 3
                   "method" "tools/call"
                   "params" {"name" "mcp-echo"
                             "arguments" {"value" "hello"}}}
                  {:request-id "test-request"})]
    (is (= "hello"
           (get-in response ["result" "structuredContent" :echo])))
    (is (= false
           (get-in response ["result" "isError"])))))

(deftest mcp-rejects-tools-outside-allowlist
  (db/set-config! :mcp/tool-allowlist "[:mcp-echo]")
  (db/install-tool! {:id :not-exposed
                     :name "not-exposed"
                     :description "Hidden test"
                     :approval :auto
                     :handler "(fn [_] {:hidden true})"})
  (tool/load-tool! :not-exposed)
  (let [response (mcp/handle-json-rpc
                  {"id" 4
                   "method" "tools/call"
                   "params" {"name" "not-exposed"
                             "arguments" {}}})]
    (is (= -32602 (get-in response ["error" "code"])))
    (is (= :mcp/tool-not-allowed
           (get-in response ["error" "data" :type])))))
