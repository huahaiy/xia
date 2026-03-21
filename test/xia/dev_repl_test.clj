(ns xia.dev-repl-test
  (:require [clojure.test :refer :all]
            [user :as user]
            [xia.core :as core]))

(defn- reset-user-options!
  []
  (reset! (var-get #'user/runtime-options*)
          (merge core/default-run-options
                 {:mode "server"
                  :web-dev true})))

(use-fixtures :each
  (fn [f]
    (reset-user-options!)
    (try
      (f)
      (finally
        (reset-user-options!)))))

(deftest repl-go-starts-server-runtime-with-current-options
  (let [stopped    (atom nil)
        configured (atom nil)
        started    (atom nil)]
    (user/set-options! {:port 3011
                        :db "/tmp/xia-dev-user"})
    (with-redefs [xia.core/stop-runtime! (fn [options]
                                           (reset! stopped options)
                                           :stopped)
                  xia.logging/configure! (fn [options]
                                           (reset! configured options))
                  xia.core/start-server-runtime! (fn [options]
                                                   (reset! started options)
                                                   options)]
      (let [result (user/go)]
        (is (= "server" (:mode result)))
        (is (= true (:web-dev result)))
        (is (= 3011 (:port result)))
        (is (= "/tmp/xia-dev-user" (:db result)))
        (is (= result @stopped))
        (is (= result @configured))
        (is (= result @started))))))

(deftest repl-reset-stops-and-refreshes
  (let [stopped   (atom nil)
        refreshed (atom nil)]
    (with-redefs [xia.core/stop-runtime! (fn [options]
                                           (reset! stopped options)
                                           :stopped)
                  clojure.tools.namespace.repl/refresh (fn [& args]
                                                         (reset! refreshed args)
                                                         :refreshed)]
      (is (= :refreshed (user/reset))))
    (is (= (user/options) @stopped))
    (is (= '(:after user/go) @refreshed))))
