(ns xia.dev-repl-test
  (:require [clojure.test :refer :all]
            [user :as user]
            [xia.core :as core]))

(defn- reset-user-options!
  []
  (reset! (var-get #'user/runtime-options*)
          (merge core/default-run-options
                 {:mode "server"
                  :web-dev true
                  :dev-nrepl? true
                  :dev-nrepl-bind "127.0.0.1"
                  :dev-nrepl-port 0
                  :dev-nrepl-port-file "/tmp/xia-dev-repl-port"})))

(use-fixtures :each
  (fn [f]
    (reset-user-options!)
    (try
      (f)
      (finally
        (reset-user-options!)))))

(deftest repl-go-starts-server-runtime-with-current-options
  (let [nrepl      (atom nil)
        stopped    (atom nil)
        configured (atom nil)
        started    (atom nil)]
    (user/set-options! {:port 3011
                        :db "/tmp/xia-dev-user"})
    (with-redefs [xia.dev-nrepl/start! (fn [opts]
                                         (reset! nrepl opts)
                                         {:bind "127.0.0.1"
                                          :port 45678
                                          :port-file "/tmp/xia-dev-repl-port"})
                  xia.core/stop-runtime! (fn [options]
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
        (is (= {:enabled? true
                :bind "127.0.0.1"
                :port 0
                :port-file "/tmp/xia-dev-repl-port"}
               @nrepl))
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

(deftest repl-stop-does-not-stop-dev-nrepl
  (let [stopped (atom nil)
        nrepl-stop-calls (atom 0)]
    (with-redefs [xia.core/stop-runtime! (fn [options]
                                           (reset! stopped options)
                                           :stopped)
                  xia.dev-nrepl/stop! (fn []
                                        (swap! nrepl-stop-calls inc)
                                        nil)]
      (is (= :stopped (user/stop))))
    (is (= (user/options) @stopped))
    (is (zero? @nrepl-stop-calls))))
