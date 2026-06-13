(ns xia.system-test
  (:require [clojure.test :refer [deftest is]]
            [integrant.core :as ig]
            [xia.agent]
            [xia.agent.fact-review]
            [xia.async]
            [xia.bridge]
            [xia.browser.playwright]
            [xia.channel.http]
            [xia.channel.messaging]
            [xia.checkpoint]
            [xia.db]
            [xia.hippocampus]
            [xia.instance-supervisor]
            [xia.llm]
            [xia.local-ocr]
            [xia.oauth]
            [xia.permission]
            [xia.prompt]
            [xia.retrieval-state]
            [xia.runtime-context :as runtime-context]
            [xia.runtime-state]
            [xia.scheduler]
            [xia.sci-env]
            [xia.service]
            [xia.skill]
            [xia.system]
            [xia.tool]
            [xia.web]
            [xia.working-memory])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(deftest runtime-support-halt-does-not-own-subruntime-shutdown
  (let [calls (atom [])]
    (with-redefs [xia.agent/cancel-all-sessions! (fn [reason]
                                                   (swap! calls conj [:cancel-all reason])
                                                   0)
                  xia.browser/release-all-sessions! (fn []
                                                     (swap! calls conj :browser-release-all)
                                                     nil)
                  xia.async/clear-runtime! (fn []
                                             (swap! calls conj :async-clear))
                  xia.hippocampus/clear-runtime! (fn []
                                                   (swap! calls conj :hippo-clear))]
      (ig/halt-key! :xia/runtime-support nil))
    (is (= [] @calls))))

(deftest runtime-support-exposes-explicit-runtime-context
  (let [db-runtime    {:runtime-name :db}
        async-runtime {:runtime-name :async}
        support       (ig/init-key
                        :xia/runtime-support
                        {:db {:runtime db-runtime
                              :db-path "test.db"}
                         :overlay {:snapshot-id 1}
                         :async-runtime {:runtime async-runtime}})]
    (is (= db-runtime
           (runtime-context/runtime support :xia/db)))
    (is (= async-runtime
           (runtime-context/runtime support :xia/async-runtime)))
    (is (= {:runtime async-runtime}
           (runtime-context/component support :xia/async-runtime)))))

(deftest runtime-context-merge-preserves-outer-keys-and-prefers-later-contexts
  (let [outer-db      {:runtime-name :outer-db}
        inner-db      {:runtime-name :inner-db}
        inner-http    {:runtime-name :inner-http}
        outer-context (runtime-context/make
                        {:xia/db {:runtime outer-db}})
        inner-context (runtime-context/make
                        {:xia/db {:runtime inner-db}
                         :xia/http-runtime {:runtime inner-http}})
        merged        (runtime-context/merge-contexts outer-context inner-context)]
    (is (= inner-db
           (runtime-context/runtime merged :xia/db)))
    (is (= inner-http
           (runtime-context/runtime merged :xia/http-runtime)))
    (is (nil? (runtime-context/merge-contexts nil nil)))))

(deftest async-runtime-uses-bound-runtime-context
  (xia.async/clear-runtime!)
  (let [scoped-runtime    (xia.async/make-runtime)
        context           (runtime-context/make
                            {:xia/async-runtime {:runtime scoped-runtime}})]
    (try
      (runtime-context/with-runtime-context
        context
        #(let [future (xia.async/submit-background! "scoped-runtime-test" (fn [] :scoped))]
           (is (= :scoped (deref future 1000 ::timeout)))
           (is (contains? @(:executors-atom scoped-runtime) :background))))
      (finally
        (runtime-context/with-runtime-context context #(xia.async/clear-runtime!))))))

(deftest agent-init-binds-direct-runtime-dependencies-for-recovery
  (let [agent-runtime {:runtime-name :agent}
        db {:runtime {:runtime-name :db}}
        async-runtime {:runtime {:runtime-name :async}}
        runtime-state-runtime {:runtime {:runtime-name :runtime-state}}
        prompt-runtime {:runtime {:runtime-name :prompt}}
        working-memory-runtime {:runtime {:runtime-name :working-memory}}
        llm-runtime {:runtime {:runtime-name :llm}}
        fact-review-runtime {:runtime {:runtime-name :fact-review}}
        observed (atom nil)]
    (with-redefs [xia.agent/make-runtime (fn [] agent-runtime)
                  xia.agent/recover-runtime-tasks! (fn []
                                                     (reset! observed
                                                             {:agent (runtime-context/runtime :xia/agent-runtime)
                                                              :fact-review (runtime-context/runtime :xia/fact-review-runtime)
                                                              :db (runtime-context/runtime :xia/db)
                                                              :async (runtime-context/runtime :xia/async-runtime)
                                                              :runtime-state (runtime-context/runtime :xia/runtime-state-runtime)
                                                              :prompt (runtime-context/runtime :xia/prompt-runtime)
                                                              :working-memory (runtime-context/runtime :xia/working-memory-runtime)
                                                              :llm (runtime-context/runtime :xia/llm-runtime)})
                                                     [])]
      (let [component (ig/init-key :xia/agent-runtime
                                   {:db db
                                    :async-runtime async-runtime
                                    :runtime-state-runtime runtime-state-runtime
                                    :prompt-runtime prompt-runtime
                                    :working-memory-runtime working-memory-runtime
                                    :llm-runtime llm-runtime
                                    :fact-review-runtime fact-review-runtime})]
        (is (= agent-runtime
               (dissoc (:runtime component) :runtime-context)))
        (is (= {:agent agent-runtime
                :fact-review (:runtime fact-review-runtime)
                :db (:runtime db)
                :async (:runtime async-runtime)
                :runtime-state (:runtime runtime-state-runtime)
                :prompt (:runtime prompt-runtime)
                :working-memory (:runtime working-memory-runtime)
                :llm (:runtime llm-runtime)}
               @observed))))))

(deftest scheduler-init-binds-runtime-context-while-starting
  (let [tool-runtime {:runtime {:runtime-name :tool}}
        instance-supervisor {:runtime {:runtime-name :instance-supervisor}}
        bridge-runtime {:runtime {:runtime-name :bridge}}
        oauth-runtime {:runtime {:runtime-name :oauth}}
        runtime-state-runtime {:runtime {:runtime-name :runtime-state}}
        support      (runtime-context/make {:xia/tool-runtime tool-runtime})
        observed     (atom nil)]
    (with-redefs [xia.scheduler/make-runtime (fn [] {:runtime-name :scheduler})
                  xia.scheduler/start! (fn []
                                         (reset! observed
                                                 {:scheduler-runtime (runtime-context/runtime :xia/scheduler)
                                                  :tool-runtime      (runtime-context/runtime :xia/tool-runtime)
                                                  :instance-supervisor (runtime-context/runtime :xia/instance-supervisor)
                                                  :bridge-runtime (runtime-context/runtime :xia/bridge-runtime)
                                                  :oauth-runtime (runtime-context/runtime :xia/oauth-runtime)
                                                  :runtime-state-runtime (runtime-context/runtime :xia/runtime-state-runtime)}))]
      (let [component (ig/init-key :xia/scheduler {:tool-runtime tool-runtime
                                                   :instance-supervisor instance-supervisor
                                                   :bridge-runtime bridge-runtime
                                                   :oauth-runtime oauth-runtime
                                                   :runtime-state-runtime runtime-state-runtime
                                                   :runtime-support support})]
        (is (= :scheduler
               (:runtime-name (:runtime component))))
        (is (= :scheduler
               (:runtime-name (:scheduler-runtime @observed))))
        (is (= (:runtime tool-runtime)
               (:tool-runtime @observed)))
        (is (= (:runtime instance-supervisor)
               (:instance-supervisor @observed)))
        (is (= (:runtime bridge-runtime)
               (:bridge-runtime @observed)))
        (is (= (:runtime oauth-runtime)
               (:oauth-runtime @observed)))
        (is (= (:runtime runtime-state-runtime)
               (:runtime-state-runtime @observed)))
        (is (some? (:runtime-context (:runtime component))))))))

(deftest messaging-init-binds-instance-supervisor-context
  (let [messaging-runtime {:runtime-name :messaging}
        instance-supervisor {:runtime {:runtime-name :instance-supervisor}}
        db {:runtime {:runtime-name :db}}
        bridge-runtime {:runtime {:runtime-name :bridge}}
        runtime-state-runtime {:runtime {:runtime-name :runtime-state}}
        async-runtime {:runtime {:runtime-name :async}}
        support (runtime-context/make {})
        observed (atom nil)]
    (with-redefs [xia.channel.messaging/make-runtime (fn [] messaging-runtime)
                  xia.channel.messaging/start! (fn []
                                                 (reset! observed
                                                         {:messaging (runtime-context/runtime :xia/messaging)
                                                          :instance-supervisor (runtime-context/runtime :xia/instance-supervisor)
                                                          :db (runtime-context/runtime :xia/db)
                                                          :bridge-runtime (runtime-context/runtime :xia/bridge-runtime)
                                                          :runtime-state-runtime (runtime-context/runtime :xia/runtime-state-runtime)
                                                          :async-runtime (runtime-context/runtime :xia/async-runtime)}))]
      (let [component (ig/init-key :xia/messaging {:runtime-support support
                                                   :instance-supervisor instance-supervisor
                                                   :db db
                                                   :bridge-runtime bridge-runtime
                                                   :runtime-state-runtime runtime-state-runtime
                                                   :async-runtime async-runtime})]
        (is (= messaging-runtime (dissoc (:runtime component) :runtime-context)))
        (is (= messaging-runtime (:messaging @observed)))
        (is (= (:runtime instance-supervisor)
               (:instance-supervisor @observed)))
        (is (= (:runtime db)
               (:db @observed)))
        (is (= (:runtime bridge-runtime)
               (:bridge-runtime @observed)))
        (is (= (:runtime runtime-state-runtime)
               (:runtime-state-runtime @observed)))
        (is (= (:runtime async-runtime)
               (:async-runtime @observed)))))))

(deftest tool-init-binds-direct-runtime-dependencies
  (let [tool-runtime {:registry (atom {})}
        db {:runtime {:runtime-name :db}}
        sci-runtime {:runtime {:runtime-name :sci}}
        instance-supervisor {:runtime {:runtime-name :instance-supervisor}}
        llm-runtime {:runtime {:runtime-name :llm}}
        prompt-runtime {:runtime {:runtime-name :prompt}}
        working-memory-runtime {:runtime {:runtime-name :working-memory}}
        permission-runtime {:runtime {:runtime-name :permission}}
        support (runtime-context/make {})
        observed (atom [])]
    (with-redefs [xia.tool/make-runtime (fn [] tool-runtime)
                  xia.tool/ensure-bundled-tools! (fn []
                                                   (swap! observed conj
                                                          [:ensure
                                                           {:db (runtime-context/runtime :xia/db)
                                                            :sci (runtime-context/runtime :xia/sci-runtime)
                                                            :instance-supervisor (runtime-context/runtime :xia/instance-supervisor)
                                                            :llm (runtime-context/runtime :xia/llm-runtime)
                                                            :prompt (runtime-context/runtime :xia/prompt-runtime)
                                                            :working-memory (runtime-context/runtime :xia/working-memory-runtime)
                                                            :permission (runtime-context/runtime :xia/permission-runtime)}])
                                                   0)
                  xia.tool/load-all-tools! (fn [] (swap! observed conj :load))
                  xia.tool/registered-tools (fn [] [])
                  xia.skill/all-enabled-skills (fn [] [])]
      (let [component (ig/init-key :xia/tool-runtime
                                   {:identity {}
                                    :sci-runtime sci-runtime
                                    :runtime-support support
                                    :instance-supervisor instance-supervisor
                                    :db db
                                    :llm-runtime llm-runtime
                                    :prompt-runtime prompt-runtime
                                    :working-memory-runtime working-memory-runtime
                                    :permission-runtime permission-runtime})]
        (is (= tool-runtime
               (dissoc (:runtime component) :runtime-context)))
        (is (= [[:ensure {:db (:runtime db)
                          :sci (:runtime sci-runtime)
                          :instance-supervisor (:runtime instance-supervisor)
                          :llm (:runtime llm-runtime)
                          :prompt (:runtime prompt-runtime)
                          :working-memory (:runtime working-memory-runtime)
                          :permission (:runtime permission-runtime)}]
                :load]
               @observed))
        (is (= (:runtime db)
               (runtime-context/runtime (:runtime-context component) :xia/db)))
        (is (= (:runtime llm-runtime)
               (runtime-context/runtime (:runtime-context component) :xia/llm-runtime)))
        (is (= (:runtime prompt-runtime)
               (runtime-context/runtime (:runtime-context component) :xia/prompt-runtime)))
        (is (= (:runtime working-memory-runtime)
               (runtime-context/runtime (:runtime-context component) :xia/working-memory-runtime)))))))

(deftest scheduler-halt-binds-component-runtime-context
  (let [runtime  {:runtime-name :scheduler}
        context  (runtime-context/make {:xia/scheduler {:runtime runtime}})
        observed (atom [])]
    (with-redefs [xia.scheduler/stop! (fn []
                                        (swap! observed conj
                                               [:stop (runtime-context/runtime :xia/scheduler)]))
                  xia.scheduler/clear-runtime! (fn []
                                                 (swap! observed conj
                                                        [:clear (runtime-context/runtime :xia/scheduler)]))]
      (ig/halt-key! :xia/scheduler {:runtime runtime
                                     :runtime-context context}))
    (is (= [[:stop runtime]
            [:clear runtime]]
           @observed))))

(deftest standalone-runtime-components-are-context-owned
  (letfn [(clear-globals! []
            (runtime-context/without-runtime-context
              #(do
                 (xia.db/clear-runtime!)
                 (xia.runtime-state/clear-runtime!)
                 (xia.retrieval-state/clear-runtime!)
                 (xia.oauth/clear-runtime!)
                 (xia.browser.playwright/clear-runtime!)
                 (xia.async/clear-runtime!)
                 (xia.agent/clear-runtime!)
                 (xia.agent.fact-review/clear-runtime!)
                 (xia.working-memory/clear-runtime!)
                 (xia.prompt/clear-runtime!)
                 (xia.bridge/clear-runtime!)
                 (xia.sci-env/clear-runtime!)
                 (xia.tool/clear-runtime!)
                 (xia.permission/clear-runtime!)
                 (xia.instance-supervisor/clear-runtime!)
                 (xia.scheduler/clear-runtime!)
                 (xia.channel.messaging/clear-runtime!)
                 (xia.channel.http/clear-runtime!)
                 (xia.hippocampus/clear-runtime!)
                 (xia.checkpoint/clear-runtime!)
                 (xia.llm/clear-runtime!)
                 (xia.local-ocr/clear-runtime!)
                 (xia.service/clear-runtime!)
                 (xia.web/clear-runtime!))))
          (component-specs []
            (let [db-component            {:runtime {:runtime-name :db}}
                  async-component         {:runtime {:runtime-name :async}}
                  runtime-state-component {:runtime {:runtime-name :runtime-state}}
                  fact-review-component   {:runtime {:runtime-name :fact-review}}
                  bridge-component        {:runtime {:runtime-name :bridge}}
                  oauth-component         {:runtime {:runtime-name :oauth}}
                  llm-component           {:runtime {:runtime-name :llm}}
                  prompt-component        {:runtime {:runtime-name :prompt}}
                  wm-component            {:runtime {:runtime-name :working-memory}}
                  sci-component           {:runtime {:runtime-name :sci}}
                  instance-component      {:runtime {:runtime-name :instance-supervisor}}
                  permission-component    {:runtime {:runtime-name :permission}}
                  tool-component          {:runtime {:runtime-name :tool}}
                  runtime-support  (runtime-context/make
                                      {:xia/db db-component
                                       :xia/async-runtime async-component
                                       :xia/fact-review-runtime fact-review-component
                                       :xia/permission-runtime permission-component
                                       :xia/tool-runtime tool-component})]
              [[:xia/db {:db-path "/tmp/xia-system-test.db"
                         :connect-options {}}]
               [:xia/runtime-state-runtime nil]
               [:xia/retrieval-runtime nil]
               [:xia/oauth-runtime nil]
               [:xia/browser-runtime {:db db-component}]
               [:xia/async-runtime {:db db-component}]
               [:xia/agent-runtime {:db db-component
                                     :async-runtime async-component
                                     :runtime-state-runtime runtime-state-component
                                     :prompt-runtime prompt-component
                                     :working-memory-runtime wm-component
                                     :llm-runtime llm-component
                                     :fact-review-runtime fact-review-component}]
               [:xia/fact-review-runtime nil]
               [:xia/working-memory-runtime {:async-runtime async-component}]
               [:xia/prompt-runtime {:async-runtime async-component}]
               [:xia/bridge-runtime nil]
               [:xia/sci-runtime {:db db-component
                                   :runtime-support runtime-support}]
               [:xia/tool-runtime {:identity {}
                                    :sci-runtime sci-component
                                    :runtime-support runtime-support
                                    :instance-supervisor instance-component
                                    :db db-component
                                    :llm-runtime llm-component
                                    :prompt-runtime prompt-component
                                    :working-memory-runtime wm-component
                                    :permission-runtime permission-component}]
               [:xia/permission-runtime nil]
               [:xia/instance-supervisor {:db db-component
                                           :runtime-support runtime-support
                                           :enabled? false}]
               [:xia/scheduler {:tool-runtime tool-component
                                 :runtime-support runtime-support
                                 :instance-supervisor instance-component
                                 :bridge-runtime bridge-component
                                 :oauth-runtime oauth-component
                                 :runtime-state-runtime runtime-state-component}]
               [:xia/messaging {:runtime-support runtime-support
                                 :instance-supervisor instance-component
                                 :db db-component
                                 :bridge-runtime bridge-component
                                 :runtime-state-runtime runtime-state-component
                                 :async-runtime async-component}]
               [:xia/http-runtime {:runtime-support runtime-support}]
               [:xia/llm-runtime {:db db-component}]
               [:xia/hippocampus-runtime {:db db-component
                                           :llm-runtime llm-component}]
               [:xia/checkpoint-runtime {:db db-component}]
               [:xia/local-ocr-runtime nil]
               [:xia/service-runtime nil]
               [:xia/web-runtime nil]]))]
    (clear-globals!)
    (try
      (with-redefs [xia.system/ensure-db-dir! (fn [_] nil)
                    xia.db/connect! (fn [_ _] nil)
                    xia.agent/recover-runtime-tasks! (fn [] [])
                    xia.working-memory/reset-runtime! (fn [] nil)
                    xia.tool/ensure-bundled-tools! (fn [] 0)
                    xia.tool/load-all-tools! (fn [] nil)
                    xia.tool/registered-tools (fn [] [])
                    xia.skill/all-enabled-skills (fn [] [])
                    xia.scheduler/start! (fn [] nil)
                    xia.channel.messaging/start! (fn [] nil)]
        (doseq [[component-key init-args] (component-specs)]
          (let [component (ig/init-key component-key init-args)]
            (try
              (let [context-runtime (runtime-context/runtime (:runtime-context component)
                                                             component-key)]
                (is (some? context-runtime))
                (is (= (dissoc (:runtime component) :runtime-context)
                       (dissoc context-runtime :runtime-context))))
              (finally
                (ig/halt-key! component-key component))))))
      (finally
        (clear-globals!)))))

(deftest integrant-retrieval-runtime-requires-bound-context
  (runtime-context/without-runtime-context xia.retrieval-state/clear-runtime!)
  (let [component (ig/init-key :xia/retrieval-runtime nil)]
    (try
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Retrieval runtime is not installed"
                            (xia.retrieval-state/version)))
      (runtime-context/with-runtime-context
        (:runtime-context component)
        #(do
           (xia.retrieval-state/bump-knowledge!)
           (is (= {:knowledge-epoch 1
                   :local-doc-session nil
                   :local-doc-epoch 0}
                  (xia.retrieval-state/version)))))
      (finally
        (ig/halt-key! :xia/retrieval-runtime component)
        (runtime-context/without-runtime-context xia.retrieval-state/clear-runtime!)))))

(deftest http-init-binds-route-runtime-dependencies
  (let [http-base-context (runtime-context/make {})
        http-runtime     {:runtime-context http-base-context}
        scheduler-runtime {:runtime-name :scheduler}
        messaging-runtime {:runtime-name :messaging}
        instance-runtime {:runtime-name :instance-supervisor}
        db-runtime {:runtime-name :db}
        bridge-runtime {:runtime-name :bridge}
        runtime-state-runtime {:runtime-name :runtime-state}
        scheduler-component {:runtime scheduler-runtime}
        messaging-component {:runtime messaging-runtime}
        instance-component {:runtime instance-runtime}
        db-component {:runtime db-runtime}
        bridge-component {:runtime bridge-runtime}
        runtime-state-component {:runtime runtime-state-runtime}
        observed (atom nil)]
    (with-redefs [xia.channel.http/start! (fn [_bind-host _port _opts]
                                            (reset! observed
                                                    {:scheduler (runtime-context/runtime :xia/scheduler)
                                                     :messaging (runtime-context/runtime :xia/messaging)
                                                     :instance-supervisor (runtime-context/runtime :xia/instance-supervisor)
                                                     :db (runtime-context/runtime :xia/db)
                                                     :bridge-runtime (runtime-context/runtime :xia/bridge-runtime)
                                                     :runtime-state-runtime (runtime-context/runtime :xia/runtime-state-runtime)}))
                  xia.channel.http/current-port (fn [] 3009)]
      (let [component (ig/init-key :xia/http
                                   {:http-runtime {:runtime http-runtime
                                                   :runtime-context http-base-context}
                                    :scheduler scheduler-component
                                    :messaging messaging-component
                                    :instance-supervisor instance-component
                                    :db db-component
                                    :bridge-runtime bridge-component
                                    :runtime-state-runtime runtime-state-component
                                    :bind-host "127.0.0.1"
                                    :port 3008})]
        (is (= {:scheduler scheduler-runtime
                :messaging messaging-runtime
                :instance-supervisor instance-runtime
                :db db-runtime
                :bridge-runtime bridge-runtime
                :runtime-state-runtime runtime-state-runtime}
               @observed))
        (is (= scheduler-runtime
               (runtime-context/runtime (:runtime-context component) :xia/scheduler)))
        (is (= messaging-runtime
               (runtime-context/runtime (:runtime-context component) :xia/messaging)))
        (is (= instance-runtime
               (runtime-context/runtime (:runtime-context component) :xia/instance-supervisor)))
        (is (= db-runtime
               (runtime-context/runtime (:runtime-context component) :xia/db)))
        (is (= bridge-runtime
               (runtime-context/runtime (:runtime-context component) :xia/bridge-runtime)))
        (is (= runtime-state-runtime
               (runtime-context/runtime (:runtime-context component) :xia/runtime-state-runtime)))
        (is (= 3009 (:port component)))))))

(deftest http-with-runtime-preserves-bound-runtime-context
  (let [tool-runtime {:runtime-name :tool}
        http-runtime {:runtime-context
                      (runtime-context/make
                        {:xia/http-runtime {:runtime {:runtime-name :http}}})}
        observed     (atom nil)]
    (runtime-context/with-runtime-context
      (runtime-context/make {:xia/tool-runtime {:runtime tool-runtime}})
      #(xia.channel.http/with-runtime
         http-runtime
         (fn []
           (reset! observed
                   {:tool (runtime-context/runtime :xia/tool-runtime)
                    :http (runtime-context/runtime :xia/http-runtime)}))))
    (is (= {:tool tool-runtime
            :http {:runtime-name :http}}
           @observed))))

(deftest runtime-components-own-shutdown-work
  (let [calls (atom [])]
    (with-redefs [xia.agent/cancel-all-sessions! (fn [reason]
                                                   (swap! calls conj [:cancel-all reason])
                                                   0)
                  xia.agent/clear-runtime! (fn []
                                             (swap! calls conj :agent-clear))
                  xia.agent.fact-review/clear-runtime! (fn []
                                                         (swap! calls conj :fact-review-clear))
                  xia.browser/release-all-sessions! (fn []
                                                     (swap! calls conj :browser-release-all)
                                                     nil)
                  xia.browser.playwright/clear-runtime! (fn []
                                                          (swap! calls conj :playwright-clear))
                  xia.async/prepare-shutdown! (fn []
                                                (swap! calls conj :async-prepare)
                                                0)
                  xia.async/await-background-tasks! (fn []
                                                      (swap! calls conj :async-await)
                                                      true)
                  xia.async/clear-runtime! (fn []
                                             (swap! calls conj :async-clear))
                  xia.hippocampus/prepare-shutdown! (fn []
                                                     (swap! calls conj :hippo-prepare)
                                                     0)
                  xia.hippocampus/await-background-tasks! (fn []
                                                            (swap! calls conj :hippo-await))
                  xia.hippocampus/clear-runtime! (fn []
                                                   (swap! calls conj :hippo-clear))
                  xia.checkpoint/prepare-shutdown! (fn []
                                                     (swap! calls conj :checkpoint-prepare)
                                                     0)
                  xia.checkpoint/await-background-tasks! (fn []
                                                           (swap! calls conj :checkpoint-await))
                  xia.checkpoint/clear-runtime! (fn []
                                                  (swap! calls conj :checkpoint-clear))
                  xia.llm/clear-runtime! (fn []
                                           (swap! calls conj :llm-clear))
                  xia.local-ocr/clear-runtime! (fn []
                                                 (swap! calls conj :local-ocr-clear))
                  xia.service/clear-runtime! (fn []
                                               (swap! calls conj :service-clear))
                  xia.web/clear-runtime! (fn []
                                           (swap! calls conj :web-clear))
                  xia.sci-env/clear-runtime! (fn []
                                                (swap! calls conj :sci-clear))
                  xia.instance-supervisor/clear-runtime! (fn []
                                                           (swap! calls conj :instance-supervisor-clear))
                  xia.tool/clear-runtime! (fn []
                                            (swap! calls conj :tool-clear))
                  xia.permission/clear-runtime! (fn []
                                                  (swap! calls conj :permission-clear))]
      (ig/halt-key! :xia/agent-runtime nil)
      (ig/halt-key! :xia/fact-review-runtime nil)
      (ig/halt-key! :xia/browser-runtime nil)
      (ig/halt-key! :xia/async-runtime nil)
      (ig/halt-key! :xia/hippocampus-runtime nil)
      (ig/halt-key! :xia/checkpoint-runtime nil)
      (ig/halt-key! :xia/llm-runtime nil)
      (ig/halt-key! :xia/local-ocr-runtime nil)
      (ig/halt-key! :xia/service-runtime nil)
      (ig/halt-key! :xia/web-runtime nil)
      (ig/halt-key! :xia/sci-runtime nil)
      (ig/halt-key! :xia/instance-supervisor nil)
      (ig/halt-key! :xia/tool-runtime nil)
      (ig/halt-key! :xia/permission-runtime nil))
    (is (= [[:cancel-all "runtime stopping"]
            :agent-clear
            :fact-review-clear
            :browser-release-all
            :playwright-clear
            :async-prepare
            :async-await
            :async-clear
            :hippo-prepare
            :hippo-await
            :hippo-clear
            :checkpoint-prepare
            :checkpoint-await
            :checkpoint-clear
            :llm-clear
            :local-ocr-clear
            :service-clear
            :web-clear
            :sci-clear
            :instance-supervisor-clear
            :tool-clear
            :permission-clear]
           @calls))))

(deftest async-runtime-shutdown-drains-accepted-work-and-rejects-new-work
  (xia.async/clear-runtime!)
  (let [runtime (xia.async/make-runtime)
        context (runtime-context/make
                  {:xia/async-runtime {:runtime runtime}})
        started  (CountDownLatch. 1)
        release  (CountDownLatch. 1)
        finished (promise)]
    (runtime-context/with-runtime-context
      context
      #(let [submitted (xia.async/submit-background!
                        "drain-test"
                        (fn []
                          (.countDown started)
                          (.await release 1 TimeUnit/SECONDS)
                          (deliver finished :done)))]
         (try
           (is (some? submitted))
           (is (.await started 1000 TimeUnit/MILLISECONDS))
           (is (= 1 (xia.async/prepare-shutdown!)))
           (is (nil? (xia.async/submit-background! "late-work" (fn [] (deliver finished :late)))))
           (.countDown release)
           (is (true? (xia.async/await-background-tasks! 1000)))
           (is (= :done (deref finished 1000 ::timeout)))
           (finally
             (.countDown release)
             (xia.async/clear-runtime!)))))))

(deftest working-memory-runtime-halt-snapshots-and-clears-runtime
  (let [calls (atom [])]
    (with-redefs [xia.working-memory/prepare-shutdown! (fn []
                                                         (swap! calls conj :prepare))
                  xia.working-memory/snapshot-all! (fn []
                                                     (swap! calls conj :snapshot-all)
                                                     2)
                  xia.working-memory/clear-runtime! (fn []
                                                      (swap! calls conj :clear))]
      (ig/halt-key! :xia/working-memory-runtime nil))
    (is (= [:prepare :snapshot-all :clear]
           @calls))))
