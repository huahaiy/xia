(ns xia.system-test
  (:require [clojure.test :refer [deftest is]]
            [integrant.core :as ig]
            [xia.runtime-context :as runtime-context]
            [xia.system]
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

(deftest async-runtime-prefers-bound-runtime-context
  (xia.async/clear-runtime!)
  (let [installed-runtime (xia.async/install-runtime! (xia.async/make-runtime))
        scoped-runtime    (xia.async/make-runtime)
        context           (runtime-context/make
                            {:xia/async-runtime {:runtime scoped-runtime}})]
    (try
      (runtime-context/with-runtime-context
        context
        #(let [future (xia.async/submit-background! "scoped-runtime-test" (fn [] :scoped))]
           (is (= :scoped (deref future 1000 ::timeout)))
           (is (contains? @(:executors-atom scoped-runtime) :background))
           (is (empty? @(:executors-atom installed-runtime)))))
      (finally
        (runtime-context/with-runtime-context context #(xia.async/clear-runtime!))
        (xia.async/clear-runtime!)))))

(deftest scheduler-init-binds-runtime-context-while-starting
  (let [tool-runtime {:runtime {:runtime-name :tool}}
        support      (runtime-context/make {:xia/tool-runtime tool-runtime})
        observed     (atom nil)]
    (with-redefs [xia.scheduler/make-runtime (fn [] {:runtime-name :scheduler})
                  xia.scheduler/install-runtime! identity
                  xia.scheduler/start! (fn []
                                         (reset! observed
                                                 {:scheduler-runtime (runtime-context/runtime :xia/scheduler)
                                                  :tool-runtime      (runtime-context/runtime :xia/tool-runtime)}))]
      (let [component (ig/init-key :xia/scheduler {:tool-runtime tool-runtime
                                                   :runtime-support support})]
        (is (= :scheduler
               (:runtime-name (:runtime component))))
        (is (= :scheduler
               (:runtime-name (:scheduler-runtime @observed))))
        (is (= (:runtime tool-runtime)
               (:tool-runtime @observed)))
        (is (some? (:runtime-context (:runtime component))))))))

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

(deftest runtime-components-own-shutdown-work
  (let [calls (atom [])]
    (with-redefs [xia.agent/cancel-all-sessions! (fn [reason]
                                                   (swap! calls conj [:cancel-all reason])
                                                   0)
                  xia.agent/clear-runtime! (fn []
                                             (swap! calls conj :agent-clear))
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
                                            (swap! calls conj :tool-clear))]
      (ig/halt-key! :xia/agent-runtime nil)
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
      (ig/halt-key! :xia/tool-runtime nil))
    (is (= [[:cancel-all "runtime stopping"]
            :agent-clear
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
            :tool-clear]
           @calls))))

(deftest async-runtime-shutdown-drains-accepted-work-and-rejects-new-work
  (xia.async/clear-runtime!)
  (xia.async/install-runtime! (xia.async/make-runtime))
  (let [started  (CountDownLatch. 1)
        release  (CountDownLatch. 1)
        finished (promise)
        submitted (xia.async/submit-background!
                   "drain-test"
                   #(do
                      (.countDown started)
                      (.await release 1 TimeUnit/SECONDS)
                      (deliver finished :done)))]
    (try
      (is (some? submitted))
      (is (.await started 1000 TimeUnit/MILLISECONDS))
      (is (= 1 (xia.async/prepare-shutdown!)))
      (is (nil? (xia.async/submit-background! "late-work" #(deliver finished :late))))
      (.countDown release)
      (is (true? (xia.async/await-background-tasks! 1000)))
      (is (= :done (deref finished 1000 ::timeout)))
      (finally
        (.countDown release)
        (xia.async/clear-runtime!)))))

(deftest working-memory-runtime-halt-snapshots-and-clears-installed-runtime
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
