(ns xia.system-test
  (:require [clojure.test :refer [deftest is]]
            [integrant.core :as ig]
            [xia.system]
            [xia.working-memory]))

(deftest runtime-support-halt-shuts-down-other-runtime-work
  (let [calls (atom [])]
    (with-redefs [xia.agent/cancel-all-sessions! (fn [reason]
                                                   (swap! calls conj [:cancel-all reason])
                                                   0)
                  xia.browser/release-all-sessions! (fn []
                                                     (swap! calls conj :browser-release-all)
                                                     nil)
                  xia.hippocampus/prepare-shutdown! (fn []
                                                     (swap! calls conj :hippo-prepare)
                                                     0)
                  xia.llm/prepare-shutdown! (fn []
                                              (swap! calls conj :llm-prepare)
                                                  0)
                  xia.hippocampus/await-background-tasks! (fn []
                                                            (swap! calls conj :hippo-await))
                  xia.llm/await-background-tasks! (fn []
                                                   (swap! calls conj :llm-await))]
      (ig/halt-key! :xia/runtime-support nil))
    (is (= [[:cancel-all "runtime stopping"]
            :browser-release-all
            :hippo-prepare
            :llm-prepare
            :hippo-await
            :llm-await]
           @calls))))

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
