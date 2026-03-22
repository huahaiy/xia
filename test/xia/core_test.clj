(ns xia.core-test
  (:require [clojure.test :refer :all]
            [xia.core :as core]
            [xia.crypto :as crypto]
            [xia.db :as db]
            [xia.pack :as pack]
            [xia.test-helpers :as th])
  (:import [java.nio.file Files LinkOption Paths]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.util Base64]))

(defn- temp-dir []
  (str (Files/createTempDirectory "xia-core-test"
         (into-array FileAttribute []))))

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

(deftest startup-passphrase-provider-retries-after-mismatch
  (let [responses (atom ["alpha" "beta" "gamma" "gamma"])
        prompts    (atom [])
        provider   (#'xia.core/startup-passphrase-provider "terminal")
        output     (with-out-str
                     (with-redefs [xia.core/read-startup-secret
                                   (fn [label _mode]
                                     (swap! prompts conj label)
                                     (let [value (first @responses)]
                                       (swap! responses subvec 1)
                                       value))]
                       (is (= "gamma" (provider {:new? true})))))]
    (is (= ["Master passphrase"
            "Confirm master passphrase"
            "Master passphrase"
            "Confirm master passphrase"]
           @prompts))
    (is (.contains ^String output "Master passphrases did not match. Please try again."))))

(deftest startup-passphrase-provider-skips-confirmation-for-existing-db
  (let [prompts   (atom [])
        provider  (#'xia.core/startup-passphrase-provider "terminal")]
    (with-redefs [xia.core/read-startup-secret
                  (fn [label _mode]
                    (swap! prompts conj label)
                    "unlock-passphrase")]
      (is (= "unlock-passphrase" (provider {:new? false}))))
    (is (= ["Master passphrase"] @prompts))))

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

(deftest start-server-runtime-initializes-http-runtime
  (let [calls   (atom [])
        started (atom nil)
        options {:db "/tmp/xia-dev-repl"
                 :bind "0.0.0.0"
                 :port 4011
                 :web-dev true}]
    (with-redefs-fn {#'xia.core/ensure-db-dir! (fn [db-path]
                                                 (swap! calls conj [:ensure-db-dir db-path]))
                     #'xia.db/connect! (fn [db-path crypto-opts]
                                         (swap! calls conj [:db/connect db-path
                                                            (contains? crypto-opts :passphrase-provider)]))
                     #'xia.crypto/current-key-source (fn [] :passphrase)
                     #'xia.setup/needs-setup? (constantly false)
                     #'xia.setup/run-setup! (fn [] (swap! calls conj :setup/run))
                     #'xia.identity/init-identity! (fn [] (swap! calls conj :identity/init))
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
            :identity/init
            :tool/reset
            :tool/load
            :scheduler/start
            [:http/start "0.0.0.0" 4011 {:web-dev? true}]]
           @calls))))

(deftest start-server-runtime-skips-interactive-setup-on-first-run
  (let [calls   (atom [])
        options {:db "/tmp/xia-dev-repl"
                 :bind "127.0.0.1"
                 :port 4011}]
    (with-redefs-fn {#'xia.core/ensure-db-dir! (fn [_] nil)
                     #'xia.db/connect! (fn [_ _] nil)
                     #'xia.crypto/current-key-source (fn [] :passphrase)
                     #'xia.setup/needs-setup? (constantly true)
                     #'xia.setup/run-setup! (fn [] (swap! calls conj :setup/run))
                     #'xia.identity/init-identity! (fn [] nil)
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

(deftest connect-passes-built-in-embedding-provider-ids-to-datalevin
  (let [captured (atom nil)]
    (with-redefs-fn {#'datalevin.core/get-conn
                     (fn [_db-path _schema opts]
                       (reset! captured opts)
                       ::conn)
                     #'xia.db/download-file! (fn [& _] nil)
                     #'xia.crypto/configure! (fn [& _] nil)
                     #'xia.db/init-embedding-provider! (fn [_ _] ::embedding-provider)
                     #'xia.db/init-llm-provider! (fn [_ _] nil)
                     #'xia.db/migrate-secrets! (fn [] nil)
                     #'datalevin.core/close (fn [_] nil)}
      #(try
         (db/connect! "/tmp/xia-dev-connect"
                      {:local-llm-provider false
                       :passphrase-provider (constantly "xia-test-passphrase")})
         (finally
           (db/close!))))
    (is (= :llama.cpp
           (get-in @captured [:embedding-opts :provider])))
    (is (= #{db/episode-text-domain
             db/kg-node-domain
             db/kg-fact-domain
             db/local-doc-domain
             db/local-doc-chunk-domain
             db/artifact-domain}
           (set (keys (:embedding-domains @captured)))))
    (is (every? #(= :llama.cpp (:provider %))
                (vals (:embedding-domains @captured))))))

(deftest stop-runtime-stops-process-components
  (let [calls (atom [])]
    (with-redefs-fn {#'xia.channel.http/stop! (fn [] (swap! calls conj :http/stop))
                     #'xia.scheduler/stop! (fn [] (swap! calls conj :scheduler/stop))
                     #'xia.db/close! (fn [] (swap! calls conj :db/close))
                     #'xia.core/save-archive! (fn [options]
                                                (swap! calls conj [:save-archive (:db options)]))}
      #(core/stop-runtime! {:db "/tmp/xia-dev-repl"}))
    (is (= [:http/stop
            :scheduler/stop
            :db/close
            [:save-archive "/tmp/xia-dev-repl"]]
           @calls))))
