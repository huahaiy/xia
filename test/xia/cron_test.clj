(ns xia.cron-test
  (:require [clojure.test :refer :all]
            [xia.cron :as cron])
  (:import [java.time DayOfWeek]
           [java.util Calendar Date]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- make-date
  "Create a java.util.Date from year, month (1-based), day, hour, minute."
  [year month day hour minute]
  (let [cal (Calendar/getInstance)]
    (.set cal year (dec (long month)) day hour minute 0)
    (.set cal Calendar/MILLISECOND 0)
    (.getTime cal)))

(defn- date->parts [^Date d]
  (let [cal (Calendar/getInstance)]
    (.setTime cal d)
    {:month  (inc (.get cal Calendar/MONTH))
     :day    (.get cal Calendar/DAY_OF_MONTH)
     :hour   (.get cal Calendar/HOUR_OF_DAY)
     :minute (.get cal Calendar/MINUTE)
     :dow    (.get cal Calendar/DAY_OF_WEEK)}))

;; ---------------------------------------------------------------------------
;; Validation
;; ---------------------------------------------------------------------------

(deftest validate-interval
  (is (nil? (cron/validate! {:interval-minutes 30})))
  (is (thrown? Exception (cron/validate! {:interval-minutes -1})))
  (is (thrown? Exception (cron/validate! {:interval-minutes 0}))))

(deftest validate-calendar-fields
  (is (nil? (cron/validate! {:minute #{0} :hour #{9}})))
  (is (thrown? Exception (cron/validate! {:minute #{60}})))
  (is (thrown? Exception (cron/validate! {:hour #{25}})))
  (is (thrown? Exception (cron/validate! {:dow #{7}})))
  (is (thrown? Exception (cron/validate! {:minute #{}}))))

;; ---------------------------------------------------------------------------
;; Normalization
;; ---------------------------------------------------------------------------

(deftest normalize-fills-defaults
  (let [n (cron/normalize {:minute #{0} :hour #{9}})]
    (is (= #{0} (:minute n)))
    (is (= #{9} (:hour n)))
    (is (= 31 (count (:dom n))))
    (is (= 12 (count (:month n))))
    (is (= 7 (count (:dow n))))))

(deftest normalize-coerces-vectors
  (let [n (cron/normalize {:minute [0 30] :hour [9]})]
    (is (= #{0 30} (:minute n)))
    (is (= #{9} (:hour n)))))

(deftest normalize-tracks-restrictions
  (let [n (cron/normalize {:minute #{0} :dow #{1}})]
    (is (not (:dom-restricted? n)))
    (is (:dow-restricted? n)))
  (let [n (cron/normalize {:minute #{0} :dom #{15}})]
    (is (:dom-restricted? n))
    (is (not (:dow-restricted? n)))))

(deftest normalize-interval-passthrough
  (is (= {:interval-minutes 30}
         (cron/normalize {:interval-minutes 30}))))

(deftest java-dow-to-cron-uses-explicit-day-mapping
  (is (= 0 (#'xia.cron/java-dow->cron DayOfWeek/SUNDAY)))
  (is (= 1 (#'xia.cron/java-dow->cron DayOfWeek/MONDAY)))
  (is (= 2 (#'xia.cron/java-dow->cron DayOfWeek/TUESDAY)))
  (is (= 3 (#'xia.cron/java-dow->cron DayOfWeek/WEDNESDAY)))
  (is (= 4 (#'xia.cron/java-dow->cron DayOfWeek/THURSDAY)))
  (is (= 5 (#'xia.cron/java-dow->cron DayOfWeek/FRIDAY)))
  (is (= 6 (#'xia.cron/java-dow->cron DayOfWeek/SATURDAY))))

;; ---------------------------------------------------------------------------
;; Next-run: interval mode
;; ---------------------------------------------------------------------------

(deftest next-run-interval
  (let [after (make-date 2026 3 10 14 30)
        result (cron/next-run {:interval-minutes 30} after)]
    (let [parts (date->parts result)]
      (is (= 0 (:minute parts)))
      (is (= 15 (:hour parts))))))

(deftest next-run-interval-with-last-run
  (let [after    (make-date 2026 3 10 14 30)
        last-run (make-date 2026 3 10 14 15)
        result   (cron/next-run {:interval-minutes 30} after :last-run last-run)]
    (let [parts (date->parts result)]
      ;; 14:15 + 30min = 14:45
      (is (= 45 (:minute parts)))
      (is (= 14 (:hour parts))))))

;; ---------------------------------------------------------------------------
;; Next-run: calendar mode
;; ---------------------------------------------------------------------------

(deftest next-run-every-minute
  (let [result (cron/next-run {} (make-date 2026 3 10 14 30))]
    (is (some? result))
    (let [parts (date->parts result)]
      (is (= 31 (:minute parts)))
      (is (= 14 (:hour parts))))))

(deftest next-run-specific-time
  ;; {:minute #{0} :hour #{9}} = every day at 9:00
  (let [spec {:minute #{0} :hour #{9}}]
    ;; After 8:00 → 9:00 same day
    (let [parts (date->parts (cron/next-run spec (make-date 2026 3 10 8 0)))]
      (is (= 0 (:minute parts)))
      (is (= 9 (:hour parts)))
      (is (= 10 (:day parts))))
    ;; After 10:00 → 9:00 next day
    (let [parts (date->parts (cron/next-run spec (make-date 2026 3 10 10 0)))]
      (is (= 0 (:minute parts)))
      (is (= 9 (:hour parts)))
      (is (= 11 (:day parts))))))

(deftest next-run-every-30-minutes
  (let [spec   {:minute #{0 15 30 45}}
        result (cron/next-run spec (make-date 2026 3 10 14 31))]
    (let [parts (date->parts result)]
      (is (= 45 (:minute parts)))
      (is (= 14 (:hour parts))))))

(deftest next-run-specific-dow
  ;; {:minute #{0} :hour #{9} :dow #{1}} = Mondays at 9:00
  ;; 2026-03-10 is Tuesday
  (let [spec   {:minute #{0} :hour #{9} :dow #{1}}
        result (cron/next-run spec (make-date 2026 3 10 10 0))]
    (is (some? result))
    (let [parts (date->parts result)]
      (is (= 0 (:minute parts)))
      (is (= 9 (:hour parts)))
      (is (= 16 (:day parts)))
      (is (= Calendar/MONDAY (:dow parts))))))

(deftest next-run-specific-month
  ;; {:minute #{0} :hour #{0} :dom #{1} :month #{12}} = Dec 1st midnight
  (let [spec   {:minute #{0} :hour #{0} :dom #{1} :month #{12}}
        result (cron/next-run spec (make-date 2026 3 10 0 0))]
    (is (some? result))
    (let [parts (date->parts result)]
      (is (= 0 (:minute parts)))
      (is (= 0 (:hour parts)))
      (is (= 1 (:day parts)))
      (is (= 12 (:month parts))))))

(deftest next-run-dom-dow-or-semantics
  ;; {:minute #{0} :hour #{9} :dom #{15} :dow #{1}} = 15th OR Mondays at 9am
  ;; After Tue March 10 10am:
  ;; Mar 11 (Wed) — neither 15th nor Monday
  ;; Mar 15 (Sun) — DOM=15 matches → yes!
  (let [spec   {:minute #{0} :hour #{9} :dom #{15} :dow #{1}}
        result (cron/next-run spec (make-date 2026 3 10 10 0))]
    (is (some? result))
    (let [parts (date->parts result)]
      (is (= 9 (:hour parts)))
      (is (= 15 (:day parts))))))

(deftest next-run-impossible-returns-nil
  ;; Feb 31st — never happens
  (let [spec {:minute #{0} :hour #{0} :dom #{31} :month #{2}}]
    (is (nil? (cron/next-run spec (make-date 2026 1 1 0 0))))))

;; ---------------------------------------------------------------------------
;; Describe
;; ---------------------------------------------------------------------------

(deftest describe-interval
  (is (= "every 30 minutes" (cron/describe {:interval-minutes 30})))
  (is (= "every 2 hours" (cron/describe {:interval-minutes 120}))))

(deftest describe-calendar
  (let [desc (cron/describe {:minute #{0} :hour #{9}})]
    (is (string? desc))
    (is (pos? (count desc)))))
