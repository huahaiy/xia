(ns xia.core-test
  (:require [clojure.test :refer :all]
    [xia.core :as core]
    [xia.channel.http]
    [xia.crypto :as crypto]
    [xia.db :as db]
    [xia.instance-supervisor :as instance-supervisor]
    [xia.oauth :as oauth]
    [xia.browser.playwright :as playwright]
    [xia.paths :as paths]
    [xia.pack :as pack]
    [xia.retrieval-state :as retrieval-state]
    [xia.runtime-overlay :as runtime-overlay]
    [xia.runtime-state :as runtime-state]
    [xia.scheduler :as scheduler]
    [xia.test-helpers :as th])
  (:import [java.nio.file Files LinkOption Paths]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.util Base64]))

(defn- temp-dir []
  (str (Files/createTempDirectory "xia-core-test"
         (into-array FileAttribute []))))

(defn- reset-core-runtime!
  []
  (reset! (var-get #'xia.core/runtime-system-atom) nil)
  (xia.channel.http/clear-runtime!)
  (xia.db/clear-runtime!)
  (scheduler/clear-runtime!)
  (playwright/clear-runtime!)
  (oauth/clear-runtime!)
  (retrieval-state/clear-runtime!)
  (runtime-state/clear-runtime!)
  (xia.channel.http/clear-command-shutdown-handler!)
  (runtime-overlay/clear!)
  (runtime-state/install-runtime!)
  (runtime-state/mark-stopped!))

(use-fixtures :each
  (fn [f]
    (reset-core-runtime!)
    (try
      (f)
      (finally
        (reset-core-runtime!)))))

(defn- encode-key [byte-value]
  (.encodeToString (Base64/getEncoder)
                   (byte-array (repeat 32 (byte byte-value)))))

(defn- path-of
  [path]
  (Paths/get path (make-array String 0)))

(defn- maybe-set-owner-only-perms!
  [path]
  (try
    (Files/getPosixFilePermissions (path-of path) (make-array LinkOption 0))
    (Files/setPosixFilePermissions (path-of path)
                                   (PosixFilePermissions/fromString "rw-------"))
    (catch UnsupportedOperationException _)
    (catch Exception _)))

(deftest main-opens-archive-argument-directly
  (let [dir          (temp-dir)
        db-path      (str dir "/db")
        archive      (str dir "/backup.xia")
        key-file     (str dir "/master.key")
        key-file-env {"XIA_MASTER_KEY_FILE" key-file}
        started      (atom nil)]
    (spit key-file (encode-key 11))
    (maybe-set-owner-only-perms! key-file)
    (with-redefs-fn {#'xia.crypto/env-value (fn [k] (get key-file-env k))}
      #(do
         (db/connect! db-path (th/test-connect-options))
         (try
           (db/set-config! :user/name "CLI Archive")
           (finally
             (db/close!)))))
    (with-redefs-fn {#'xia.core/start! (fn [options] (reset! started options))
                     #'xia.core/register-shutdown-hook! (fn [_] ::hook)
                     #'xia.core/remove-shutdown-hook! (fn [_] nil)
                     #'xia.core/make-cleanup (fn [_] (fn [] nil))}
      (fn []
         (with-redefs-fn {#'xia.pack/env-value (fn [k] (get key-file-env k))}
           (fn []
             (pack/pack! db-path archive)))
         (core/-main archive)))
    (is (string? (:db @started)))
    (is (= archive (get-in @started [:archive-context :archive-path])))
    (is (.endsWith ^String (:db @started) "/db"))
    (is (.endsWith ^String (:key-file (:crypto-opts @started))
                   "/db/.xia/master.key"))
    (is (true? (:allow-insecure-key-file? (:crypto-opts @started))))))

(deftest main-forwards-web-dev-option
  (let [started (atom nil)]
    (with-redefs-fn {#'xia.core/start! (fn [options] (reset! started options))
                     #'xia.logging/configure! (fn [_] nil)
                     #'xia.core/register-shutdown-hook! (fn [_] ::hook)
                     #'xia.core/remove-shutdown-hook! (fn [_] nil)
                     #'xia.core/make-cleanup (fn [_] (fn [] nil))}
      #(core/-main "--mode" "server" "--web-dev"))
    (is (= "server" (:mode @started)))
    (is (= true (:web-dev @started)))))

(deftest main-forwards-runtime-overlay-option
  (let [started (atom nil)]
    (with-redefs-fn {#'xia.core/start! (fn [options] (reset! started options))
                     #'xia.logging/configure! (fn [_] nil)
                     #'xia.core/register-shutdown-hook! (fn [_] ::hook)
                     #'xia.core/remove-shutdown-hook! (fn [_] nil)
                     #'xia.core/make-cleanup (fn [_] (fn [] nil))}
      #(core/-main "--mode" "server" "--runtime-overlay" "/run/xia/overlay.edn"))
    (is (= "server" (:mode @started)))
    (is (= "/run/xia/overlay.edn" (:runtime-overlay @started)))))

(deftest main-falls-back-to-runtime-overlay-env
  (let [started (atom nil)]
    (with-redefs-fn {#'xia.core/start! (fn [options] (reset! started options))
                     #'xia.core/env-value (fn [k]
                                            (case k
                                              "XIA_RUNTIME_OVERLAY" "/run/xia/env-overlay.edn"
                                              nil))
                     #'xia.logging/configure! (fn [_] nil)
                     #'xia.core/register-shutdown-hook! (fn [_] ::hook)
                     #'xia.core/remove-shutdown-hook! (fn [_] nil)
                     #'xia.core/make-cleanup (fn [_] (fn [] nil))}
      #(core/-main "--mode" "server"))
    (is (= "/run/xia/env-overlay.edn" (:runtime-overlay @started)))))

(deftest main-defaults-to-both-mode
  (let [started (atom nil)]
    (with-redefs-fn {#'xia.core/start! (fn [options] (reset! started options))
                     #'xia.logging/configure! (fn [_] nil)
                     #'xia.core/register-shutdown-hook! (fn [_] ::hook)
                     #'xia.core/remove-shutdown-hook! (fn [_] nil)
                     #'xia.core/make-cleanup (fn [_] (fn [] nil))}
      #(core/-main))
    (is (= "both" (:mode @started)))))

(deftest start-server-runtime-initializes-http-runtime
  (let [calls   (atom [])
        started (atom nil)
        options {:db "/tmp/xia-dev-repl"
                 :bind "0.0.0.0"
                 :port 4011
                 :web-dev true}]
    (with-redefs-fn {#'xia.system/ensure-db-dir! (fn [db-path]
                                                 (swap! calls conj [:ensure-db-dir db-path]))
                     #'xia.db/connect! (fn [db-path crypto-opts]
                                         (swap! calls conj [:db/connect db-path
                                                            (contains? crypto-opts :passphrase-provider)]))
                     #'xia.crypto/current-key-source (fn [] :passphrase)
                     #'xia.setup/needs-setup? (constantly false)
                     #'xia.setup/run-setup! (fn [] (swap! calls conj :setup/run))
                     #'xia.identity/init-identity! (fn [] (swap! calls conj :identity/init))
                     #'xia.sci-env/reset-runtime! (fn [] (swap! calls conj :sci/reset))
                     #'xia.instance-supervisor/configure! (fn [opts]
                                                            (swap! calls conj [:instance-supervisor/configure
                                                                               (:enabled? opts)
                                                                               (:command opts)]))
                     #'xia.tool/ensure-bundled-tools! (fn [] 0)
                     #'xia.tool/reset-runtime! (fn [] (swap! calls conj :tool/reset))
                     #'xia.tool/load-all-tools! (fn [] (swap! calls conj :tool/load))
                     #'xia.tool/registered-tools (fn [] [:tool-a :tool-b])
                     #'xia.skill/all-enabled-skills (fn [] [:skill-a])
                     #'xia.scheduler/start! (fn [] (swap! calls conj :scheduler/start))
                     #'xia.channel.http/start! (fn [bind port opts]
                                                (swap! calls conj [:http/start bind port opts]))
                     #'xia.core/local-ui-url (fn [bind port]
                                              (str "http://"
                                                   (if (= bind "0.0.0.0") "localhost" bind)
                                                   ":"
                                                   port
                                                   "/"))}
      #(let [output (with-out-str
                      (reset! started (core/start-server-runtime! options)))]
         (is (.contains ^String output "Xia server running on 0.0.0.0:4011"))
         (is (.contains ^String output "open http://localhost:4011/"))))
    (is (= "server" (:mode @started)))
    (is (= true (:web-dev @started)))
    (is (= [[:ensure-db-dir "/tmp/xia-dev-repl"]
            [:db/connect "/tmp/xia-dev-repl" true]
            [:instance-supervisor/configure true nil]
            :identity/init
            :sci/reset
            :tool/reset
            :tool/load
            :scheduler/start
            [:http/start "0.0.0.0" 4011 {:web-dev? true}]]
           @calls))))

(deftest start-server-runtime-disables-host-instance-management-when-env-is-false
  (let [calls   (atom [])
        options {:db "/tmp/xia-dev-repl"
                 :bind "127.0.0.1"
                 :port 4011}]
    (with-redefs-fn {#'xia.core/env-value (fn [k]
                                            (case k
                                              "XIA_ALLOW_INSTANCE_MANAGEMENT" "false"
                                              nil))
                     #'xia.system/ensure-db-dir! (fn [_] nil)
                     #'xia.db/connect! (fn [_ _] nil)
                     #'xia.crypto/current-key-source (fn [] :passphrase)
                     #'xia.setup/needs-setup? (constantly false)
                     #'xia.setup/run-setup! (fn [] nil)
                     #'xia.identity/init-identity! (fn [] nil)
                     #'xia.instance-supervisor/configure! (fn [opts]
                                                            (swap! calls conj [:instance-supervisor/configure
                                                                               (:enabled? opts)
                                                                               (:command opts)]))
                     #'xia.tool/ensure-bundled-tools! (fn [] 0)
                     #'xia.tool/reset-runtime! (fn [] nil)
                     #'xia.tool/load-all-tools! (fn [] nil)
                     #'xia.tool/registered-tools (fn [] [])
                     #'xia.skill/all-enabled-skills (fn [] [])
                     #'xia.scheduler/start! (fn [] nil)
                     #'xia.channel.http/start! (fn [_ _ _] nil)
                     #'xia.core/local-ui-url (fn [_ _] "http://localhost:4011/")}
      #(core/start-server-runtime! options))
    (is (= [[:instance-supervisor/configure false nil]]
           @calls))))

(deftest start-server-runtime-reports-actual-bound-port
  (let [started (atom nil)
        options {:db "/tmp/xia-dev-repl"
                 :bind "0.0.0.0"
                 :port 4011}]
    (with-redefs-fn {#'xia.system/ensure-db-dir! (fn [_] nil)
                     #'xia.db/connect! (fn [_ _] nil)
                     #'xia.crypto/current-key-source (fn [] :passphrase)
                     #'xia.setup/needs-setup? (constantly false)
                     #'xia.setup/run-setup! (fn [] nil)
                     #'xia.identity/init-identity! (fn [] nil)
                     #'xia.instance-supervisor/configure! (fn [_] nil)
                     #'xia.tool/ensure-bundled-tools! (fn [] 0)
                     #'xia.tool/reset-runtime! (fn [] nil)
                     #'xia.tool/load-all-tools! (fn [] nil)
                     #'xia.tool/registered-tools (fn [] [])
                     #'xia.skill/all-enabled-skills (fn [] [])
                     #'xia.scheduler/start! (fn [] nil)
                     #'xia.channel.http/start! (fn [_ _ _] nil)
                     #'xia.channel.http/current-port (fn [] 4012)
                     #'xia.core/local-ui-url (fn [bind port]
                                              (str "http://"
                                                   (if (= bind "0.0.0.0") "localhost" bind)
                                                   ":"
                                                   port
                                                   "/"))}
      #(let [output (with-out-str
                      (reset! started (core/start-server-runtime! options)))]
         (is (.contains ^String output "Xia server running on 0.0.0.0:4012"))
         (is (.contains ^String output "open http://localhost:4012/"))))
    (is (= 4012 (:port @started)))))

(deftest start-server-runtime-recovers-interrupted-tasks-on-startup
  (let [calls   (atom [])
        options {:db "/tmp/xia-dev-repl"
                 :bind "127.0.0.1"
                 :port 4011}]
    (with-redefs-fn {#'xia.system/ensure-db-dir! (fn [_] nil)
                     #'xia.db/connect! (fn [_ _] nil)
                     #'xia.crypto/current-key-source (fn [] :passphrase)
                     #'xia.setup/needs-setup? (constantly false)
                     #'xia.setup/run-setup! (fn [] nil)
                     #'xia.identity/init-identity! (fn [] nil)
                     #'xia.instance-supervisor/configure! (fn [_] nil)
                     #'xia.tool/ensure-bundled-tools! (fn [] 0)
                     #'xia.tool/reset-runtime! (fn [] nil)
                     #'xia.tool/load-all-tools! (fn [] nil)
                     #'xia.tool/registered-tools (fn [] [])
                     #'xia.skill/all-enabled-skills (fn [] [])
                     #'xia.agent/recover-runtime-tasks! (fn []
                                                         (swap! calls conj :agent/recover-runtime-tasks)
                                                         [])
                     #'xia.scheduler/start! (fn [] nil)
                     #'xia.channel.http/start! (fn [_ _ _] nil)
                     #'xia.core/local-ui-url (fn [_ _] "http://localhost:4011/")}
      #(core/start-server-runtime! options))
    (is (= [:agent/recover-runtime-tasks] @calls))))

(deftest start-server-runtime-skips-interactive-setup-on-first-run
  (let [calls   (atom [])
        options {:db "/tmp/xia-dev-repl"
                 :bind "127.0.0.1"
                 :port 4011}]
    (with-redefs-fn {#'xia.system/ensure-db-dir! (fn [_] nil)
                     #'xia.db/connect! (fn [_ _] nil)
                     #'xia.crypto/current-key-source (fn [] :passphrase)
                     #'xia.setup/needs-setup? (constantly true)
                     #'xia.setup/run-setup! (fn [] (swap! calls conj :setup/run))
                     #'xia.identity/init-identity! (fn [] nil)
                     #'xia.instance-supervisor/configure! (fn [_] nil)
                     #'xia.tool/ensure-bundled-tools! (fn [] 0)
                     #'xia.tool/reset-runtime! (fn [] nil)
                     #'xia.tool/load-all-tools! (fn [] nil)
                     #'xia.tool/registered-tools (fn [] [])
                     #'xia.skill/all-enabled-skills (fn [] [])
                     #'xia.scheduler/start! (fn [] nil)
                     #'xia.channel.http/start! (fn [_ _ _] nil)
                     #'xia.core/local-ui-url (fn [_ _] "http://localhost:4011/")}
      #(core/start-server-runtime! options))
    (is (empty? @calls))))

(deftest stop-runtime-stops-process-components
  (let [calls   (atom [])
        options {:db "/tmp/xia-dev-repl"
                 :bind "127.0.0.1"
                 :port 4011}]
    (with-redefs-fn {#'integrant.core/init (fn [_config _root-keys]
                                             {:xia/scheduler :running})
                     #'integrant.core/halt! (fn [running-system]
                                              (swap! calls conj [:ig/halt running-system]))}
      #(do
         (core/start-server-runtime! options)
         (core/stop-runtime! options)
         ;; A fresh start after stop verifies cleanup released the prior runtime.
         (core/start-server-runtime! options)
         (core/stop-runtime! options)))
    (is (= [[:ig/halt {:xia/scheduler :running}]
            [:ig/halt {:xia/scheduler :running}]]
           @calls))
    (let [event (core/last-stop-event)]
      (is (= "/tmp/xia-dev-repl" (:db-path event)))
      (is (seq (:callsite event))))))
