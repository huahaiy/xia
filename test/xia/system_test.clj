(ns xia.system-test
  (:require [clojure.test :refer [deftest is]]
            [integrant.core :as ig]
            [xia.system]
            [xia.working-memory])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(deftest runtime-support-halt-shuts-down-other-runtime-work
  (let [calls (atom [])]
    (with-redefs [xia.agent/cancel-all-sessions! (fn [reason]
                                                   (swap! calls conj [:cancel-all reason])
                                                   0)
                  xia.browser/release-all-sessions! (fn []
                                                     (swap! calls conj :browser-release-all)
                                                     nil)
                  xia.async/prepare-shutdown! (fn []
                                                (swap! calls conj :async-prepare)
                                                0)
                  xia.hippocampus/prepare-shutdown! (fn []
                                                     (swap! calls conj :hippo-prepare)
                                                     0)
                  xia.checkpoint/prepare-shutdown! (fn []
                                                     (swap! calls conj :checkpoint-prepare)
                                                     0)
                  xia.llm/prepare-shutdown! (fn []
                                              (swap! calls conj :llm-prepare)
                                                  0)
                  xia.hippocampus/await-background-tasks! (fn []
                                                            (swap! calls conj :hippo-await))
                  xia.checkpoint/await-background-tasks! (fn []
                                                           (swap! calls conj :checkpoint-await))
                  xia.checkpoint/reset-runtime! (fn []
                                                  (swap! calls conj :checkpoint-reset))
                  xia.llm/await-background-tasks! (fn []
                                                   (swap! calls conj :llm-await))
                  xia.async/await-background-tasks! (fn []
                                                      (swap! calls conj :async-await)
                                                      true)
                  xia.async/clear-runtime! (fn []
                                             (swap! calls conj :async-clear))]
      (ig/halt-key! :xia/runtime-support nil))
    (is (= [[:cancel-all "runtime stopping"]
            :browser-release-all
            :async-prepare
            :hippo-prepare
            :checkpoint-prepare
            :llm-prepare
            :hippo-await
            :checkpoint-await
            :checkpoint-reset
            :llm-await
            :async-await
            :async-clear]
           @calls))))

(deftest async-runtime-shutdown-drains-accepted-work-and-rejects-new-work
  (xia.async/clear-runtime!)
  (xia.async/install-runtime!)
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
