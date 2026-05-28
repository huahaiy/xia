(ns xia.system-test
  (:require [clojure.test :refer [deftest is]]
            [integrant.core :as ig]
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
