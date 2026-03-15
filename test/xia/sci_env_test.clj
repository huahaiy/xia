(ns xia.sci-env-test
  (:require [clojure.test :refer :all]
            [xia.db]
            [xia.memory :as memory]
            [xia.schedule :as schedule]
            [xia.sci-env :as sci-env]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(deftest system-class-is-not-exposed-to-sci
  (is (thrown? Exception
               (sci-env/eval-string "(System/getenv \"PATH\")"))))

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
