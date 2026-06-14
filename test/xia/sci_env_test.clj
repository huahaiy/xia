(ns xia.sci-env-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [xia.db :as db]
            [xia.sci-env :as sci-env]
            [xia.test-helpers :refer [with-test-db]]))

(use-fixtures :each with-test-db)

(defn- thrown-by
  [f]
  (try
    (f)
    nil
    (catch Throwable t
      t)))

(deftest sci-sandbox-blocks-dangerous-host-access
  (doseq [[label code] [["file read" "(slurp \"/etc/passwd\")"]
                        ["file object" "(clojure.java.io/file \"/tmp/x\")"]
                        ["shell execution" "(clojure.java.shell/sh \"echo\" \"x\")"]
                        ["var resolution" "(resolve 'slurp)"]
                        ["namespace introspection" "(ns-publics 'clojure.core)"]
                        ["source introspection" "(clojure.repl/source +)"]]]
    (testing label
      (is (some? (thrown-by #(sci-env/eval-string code)))))))

(deftest sci-exposed-db-namespace-uses-secret-safe-wrappers
  (db/set-config! :user/theme "dark")
  (db/set-config! :secret/sci-token "hidden")

  (is (= "dark"
         (sci-env/eval-string "(xia.db/get-config :user/theme)")))
  (is (some? (thrown-by
              #(sci-env/eval-string "(xia.db/get-config :secret/sci-token)"))))
  (is (some? (thrown-by
              #(sci-env/eval-string "(xia.db/set-config! :secret/sci-token \"leak\")")))))
