(ns xia.sci-env-test
  (:require [clojure.test :refer :all]
            [xia.agent :as agent]
            [xia.artifact :as artifact]
            [xia.browser :as browser]
            [xia.db]
            [xia.local-doc :as local-doc]
            [xia.memory :as memory]
            [xia.schedule :as schedule]
            [xia.sci-env :as sci-env]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(defn- sci-blocked?
  [code]
  (try
    (sci-env/eval-string code)
    false
    (catch Throwable _
      true)))

(deftest environment-and-reflection-access-are-blocked-in-sci
  (doseq [code ["(System/getenv \"PATH\")"
                "(System/getProperty \"user.home\")"
                "(java.lang.System/getenv \"PATH\")"
                "(java.lang.System/getProperty \"user.home\")"
                "(Class/forName \"java.lang.System\")"
                "(java.lang.Class/forName \"java.lang.System\")"]]
    (is (sci-blocked? code) code)))

(deftest dynamic-var-and-namespace-resolution-are-blocked-in-sci
  (doseq [code ["(resolve 'slurp)"
                "((resolve 'slurp) \"/etc/hosts\")"
                "(ns-resolve 'clojure.core 'slurp)"
                "((ns-resolve 'clojure.core 'slurp) \"/etc/hosts\")"
                "(find-var 'clojure.core/slurp)"
                "((find-var 'clojure.core/slurp) \"/etc/hosts\")"
                "(var clojure.core/slurp)"
                "(#'clojure.core/slurp \"/etc/hosts\")"
                "(requiring-resolve 'xia.crypto/env)"
                "((requiring-resolve 'xia.crypto/env) \"PATH\")"
                "(find-ns 'clojure.core)"
                "(the-ns 'clojure.core)"
                "(all-ns)"
                "(ns-publics 'clojure.core)"
                "(ns-interns 'clojure.core)"
                "(ns-map 'clojure.core)"
                "(ns-refers 'clojure.core)"
                "(ns-aliases 'clojure.core)"]]
    (is (sci-blocked? code) code)))

(deftest sci-eval-times-out
  (xia.db/set-config! :tool/sci-eval-timeout-ms 100)
  (let [ex (try
             (sci-env/eval-string "(deref (promise))")
             nil
             (catch clojure.lang.ExceptionInfo e
               e))]
    (is (some? ex))
    (is (re-find #"SCI eval timed out" (.getMessage ^Throwable ex)))
    (is (= {:stage :eval
            :timeout-ms 100}
           (select-keys (ex-data ex) [:stage :timeout-ms])))))

(deftest sci-handler-call-times-out
  (xia.db/set-config! :tool/sci-handler-timeout-ms 100)
  (let [handler (sci-env/eval-string "(fn [_] (deref (promise)))")
        ex      (try
                  (sci-env/call-fn handler {})
                  nil
                  (catch clojure.lang.ExceptionInfo e
                    e))]
    (is (some? ex))
    (is (re-find #"SCI handler timed out" (.getMessage ^Throwable ex)))
    (is (= {:stage :handler
            :timeout-ms 100}
           (select-keys (ex-data ex) [:stage :timeout-ms])))))

(deftest file-shell-and-source-access-are-blocked-in-sci
  (doseq [code ["(slurp \"/etc/hosts\")"
                "(require 'clojure.java.io) (clojure.java.io/reader \"/etc/hosts\")"
                "(require 'clojure.java.shell) (clojure.java.shell/sh \"echo\" \"hi\")"
                "(require 'clojure.repl) (clojure.repl/source-fn 'clojure.set/union)"]]
    (is (thrown? Exception
                 (sci-env/eval-string code))
        code)))

(deftest safe-utility-namespaces-remain-available-in-sci
  (is (= "a,b"
         (sci-env/eval-string
           "(require '[clojure.string :as str]) (str/join \",\" [\"a\" \"b\"])")))
  (is (= #{1 2}
         (sci-env/eval-string
           "(require '[clojure.set :as set]) (set/union #{1} #{2})"))))

(deftest memory-access-is-read-only-in-sci
  (memory/record-episode! {:summary "trusted episode"})
  (is (= 1
         (sci-env/eval-string "(count (xia.memory/recent-episodes 10))")))
  (doseq [code ["(xia.memory/record-episode! {:summary \"forged episode\"})"
                "(xia.memory/add-node! {:name \"Injected\" :type :concept})"
                "(xia.memory/add-edge! {:from-eid nil :to-eid nil :type :related-to})"
                "(xia.memory/add-fact! {:node-eid nil :content \"forged fact\"})"
                "(xia.memory/set-node-property! nil [:role] \"hijacked\")"
                "(xia.memory/remove-node-property! nil [:role])"]]
    (is (thrown-with-msg?
          Exception
          #"not available in Xia's SCI sandbox"
          (sci-env/eval-string code))
        code))
  (is (= 1 (count (memory/recent-episodes 10)))))

(deftest schedule-history-is-redacted-in-sci
  (schedule/create-schedule!
    {:id :sandbox-history
     :spec {:minute #{0} :hour #{9}}
     :type :tool
     :tool-id :x})
  (schedule/record-run! :sandbox-history
    {:started-at  (java.util.Date.)
     :finished-at (java.util.Date.)
     :status      :error
     :actions     [{:tool-id "service-call" :status "blocked"}]
     :result      "{\"secret\":true}"
     :error       "sensitive failure"})
  (let [history (sci-env/eval-string "(xia.schedule/schedule-history :sandbox-history 1)")
        run     (first history)]
    (is (= 1 (count history)))
    (is (= :error (:status run)))
    (is (not (contains? run :actions)))
    (is (not (contains? run :result)))
    (is (not (contains? run :error)))))

(deftest scratch-pads-are-exposed-through-sci
  (let [sid    (str (xia.db/create-session! :http))
        result (sci-env/eval-string
                 (str "(let [pad (xia.scratch/create-pad! {:scope :session"
                      "                                     :session-id \"" sid "\""
                      "                                     :title \"Draft\""
                      "                                     :content \"alpha\"})]"
                      "   (xia.scratch/edit-pad! (:id pad) {:op :append :text \" beta\"})"
                      "   (xia.scratch/get-pad (:id pad)))"))]
    (is (= "Draft" (:title result)))
    (is (= "alpha beta" (:content result)))))

(deftest browser-runtime-functions-are-exposed-through-sci
  (let [status (sci-env/eval-string "(xia.browser/runtime-status)")
        bootstrap (sci-env/eval-string "(xia.browser/bootstrap-runtime! :backend :playwright)")
        deps (sci-env/eval-string "(xia.browser/install-browser-deps! :backend :playwright)")]
    (is (= :playwright (:selected-auto-backend status)))
    (is (= #{:playwright}
           (set (map :backend (:backends status)))))
    (is (= :playwright (:backend bootstrap)))
    (is (= :running (:status bootstrap)))
    (is (= :playwright (:backend deps)))
    (is (contains? #{:unsupported-platform :dry-run} (:status deps)))))

(deftest browser-query-elements-is-exposed-through-sci
  (let [opened (browser/open-session "https://example.com" :backend :playwright)
        sid    (:session-id opened)]
    (try
      (let [result (sci-env/eval-string
                     (str "(xia.browser/query-elements \"" sid "\" :kind :links :limit 1)"))]
        (is (= sid (:session-id result)))
        (is (= :playwright (:backend result)))
        (is (= :links (:kind result)))
        (is (= 1 (:returned_count result)))
        (is (pos? (:total_count result))))
      (finally
        (browser/close-session sid)))))

(deftest local-doc-functions-are-exposed-through-sci
  (let [sid   (xia.db/create-session! :http)
        saved (local-doc/save-upload! {:session-id sid
                                       :name "paper.txt"
                                       :media-type "text/plain"
                                       :text "The automobile paper lives here."})]
    (binding [xia.working-memory/*session-id* sid]
      (let [results (sci-env/eval-string "(xia.local-doc/search-docs \"car\")")
            doc     (sci-env/eval-string
                      (str "(xia.local-doc/read-doc \"" (:id saved) "\" :max-chars 9)"))]
        (is (= "paper.txt" (:name (first results))))
        (is (= (:id saved) (:id doc)))
        (is (= "The autom" (:text doc)))
        (is (true? (:truncated? doc)))))))

(deftest artifact-functions-are-exposed-through-sci
  (let [sid     (xia.db/create-session! :http)
        created (artifact/create-artifact! {:session-id sid
                                            :name "summary.json"
                                            :kind :json
                                            :data {"topic" "cars"}})]
    (binding [xia.working-memory/*session-id* sid]
      (let [listed (sci-env/eval-string "(xia.artifact/list-artifacts)")
            found  (sci-env/eval-string "(xia.artifact/search-artifacts \"cars\")")
            read   (sci-env/eval-string
                     (str "(xia.artifact/read-artifact \"" (:id created) "\" :max-chars 12)"))
            note   (sci-env/eval-string
                     (str "(xia.artifact/create-scratch-pad-from-artifact! \"" (:id created) "\")"))]
        (is (= "summary.json" (:name (first listed))))
        (is (= "summary.json" (:name (first found))))
        (is (= (:id created) (:id read)))
        (is (string? (:text read)))
        (is (true? (:truncated? read)))
        (is (= "summary.json" (get-in note [:artifact :name])))
        (is (= "summary" (get-in note [:pad :title])))))))

(deftest branch-task-runner-is-exposed-through-sci
  (with-redefs [agent/run-branch-tasks (fn [tasks & opts]
                                         {:tasks tasks :opts opts})]
    (is (= {:tasks [{"task" "a"}]
            :opts  [:objective "test"]}
           (sci-env/eval-string
             "(xia.agent/run-branch-tasks [{\"task\" \"a\"}] :objective \"test\")")))))
