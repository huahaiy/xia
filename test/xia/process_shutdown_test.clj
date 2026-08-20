(ns xia.process-shutdown-test
  (:require [clojure.test :refer :all]
            [xia.process-shutdown :as process-shutdown]))

(deftest termination-runs-cleanup-before-preserving-signal-exit-status
  (let [handling? (atom false)
        calls     (atom [])
        cleanup   #(swap! calls conj :cleanup)
        exit!     #(swap! calls conj [:exit %])]
    (process-shutdown/run-termination! handling? cleanup exit! "TERM" 15)
    (process-shutdown/run-termination! handling? cleanup exit! "TERM" 15)
    (is (= [:cleanup [:exit 143]] @calls))))
