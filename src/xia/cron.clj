(ns xia.cron
  "Schedule time specification and next-run calculation.

   Specs are plain data maps, not cron strings.

   Two modes:
   - Calendar: {:minute #{0} :hour #{9} :dow #{1 3 5}}
     Omitted fields mean 'every'. Matches like cron but is already data.
   - Interval: {:interval-minutes 30}
     Simple recurring from last run.

   Day matching follows standard cron semantics: if both :dom and :dow
   are restricted, match on EITHER (OR)."
  (:import [java.time LocalDateTime ZoneId]))

;; ---------------------------------------------------------------------------
;; Defaults
;; ---------------------------------------------------------------------------

(def ^:private field-defaults
  {:minute (set (range 0 60))
   :hour   (set (range 0 24))
   :dom    (set (range 1 32))
   :month  (set (range 1 13))
   :dow    (set (range 0 7))})

(def ^:private field-ranges
  {:minute [0 59]
   :hour   [0 23]
   :dom    [1 31]
   :month  [1 12]
   :dow    [0 6]})

;; ---------------------------------------------------------------------------
;; Validation
;; ---------------------------------------------------------------------------

(defn- validate-field! [field-name values]
  (let [[lo hi] (get field-ranges field-name)]
    (when-not (and (set? values) (seq values))
      (throw (ex-info (str "Schedule field " field-name " must be a non-empty set")
                      {:field field-name :value values})))
    (doseq [v values]
      (when-not (and (integer? v) (<= lo v hi))
        (throw (ex-info (str "Schedule field " field-name " value out of range [" lo "-" hi "]")
                        {:field field-name :value v}))))))

(defn validate!
  "Validate a schedule spec. Throws on invalid."
  [spec]
  (cond
    ;; Interval mode
    (:interval-minutes spec)
    (let [m (:interval-minutes spec)]
      (when-not (and (integer? m) (pos? (long m)))
        (throw (ex-info "interval-minutes must be a positive integer" {:value m}))))

    ;; Calendar mode — validate any specified fields
    :else
    (doseq [field [:minute :hour :dom :month :dow]]
      (when (contains? spec field)
        (validate-field! field (get spec field))))))

;; ---------------------------------------------------------------------------
;; Normalization
;; ---------------------------------------------------------------------------

(defn normalize
  "Normalize a spec: coerce vectors to sets, fill in defaults for missing fields.
   Returns a map with :minute :hour :dom :month :dow as sets,
   plus :dom-restricted? and :dow-restricted? for day-matching semantics."
  [spec]
  (if (:interval-minutes spec)
    spec
    (let [coerce (fn [v] (if (sequential? v) (set v) v))
          spec   (reduce (fn [m k] (if (contains? m k) (update m k coerce) m))
                         spec
                         [:minute :hour :dom :month :dow])]
      (-> (merge field-defaults spec)
          (assoc :dom-restricted? (contains? spec :dom)
                 :dow-restricted? (contains? spec :dow))))))

;; ---------------------------------------------------------------------------
;; Next-run calculation
;; ---------------------------------------------------------------------------

(defn- java-dow->cron
  "Convert java.time DayOfWeek (1=Mon..7=Sun) to cron-style (0=Sun..6=Sat)."
  [java-dow]
  (mod java-dow 7))

(defn- day-matches?
  "Standard cron day semantics: if both DOM and DOW are restricted, match EITHER."
  [{:keys [dom dow dom-restricted? dow-restricted?]} ^LocalDateTime t]
  (let [dom-match? (contains? dom (.getDayOfMonth t))
        dow-match? (contains? dow (java-dow->cron (.getValue (.getDayOfWeek t))))]
    (cond
      (and (not dom-restricted?) (not dow-restricted?)) true
      (not dom-restricted?) dow-match?
      (not dow-restricted?) dom-match?
      :else                 (or dom-match? dow-match?))))

(defn next-run
  "Calculate the next run time after `after` (java.util.Date).
   Returns a java.util.Date, or nil if no match within 1 year.
   For interval specs, requires `last-run` (java.util.Date or nil)."
  [spec ^java.util.Date after & {:keys [last-run]}]
  (if-let [interval (:interval-minutes spec)]
    ;; Interval mode: last-run + interval, or after + interval if no last-run
    (let [^java.util.Date base (or last-run after)]
      (java.util.Date. (long (+ (.getTime base) (* (long interval) 60 1000)))))
    ;; Calendar mode
    (let [norm  (normalize spec)
          zone  (ZoneId/systemDefault)
          start (-> after .toInstant (.atZone zone) .toLocalDateTime
                     (.plusMinutes 1) (.withSecond 0) (.withNano 0))
          limit (.plusYears start 1)
          {:keys [minute hour month]} norm]
      (loop [t start]
        (when (.isBefore t limit)
          (cond
            (not (contains? month (.getMonthValue t)))
            (recur (-> t (.withDayOfMonth 1) (.withHour 0) (.withMinute 0) (.plusMonths 1)))

            (not (day-matches? norm t))
            (recur (-> t (.withHour 0) (.withMinute 0) (.plusDays 1)))

            (not (contains? hour (.getHour t)))
            (recur (-> t (.withMinute 0) (.plusHours 1)))

            (not (contains? minute (.getMinute t)))
            (recur (.plusMinutes t 1))

            :else
            (java.util.Date/from (.toInstant (.atZone t zone)))))))))

;; ---------------------------------------------------------------------------
;; Human-readable description
;; ---------------------------------------------------------------------------

(defn describe
  "Return a human-readable description of a schedule spec."
  [spec]
  (if-let [m (:interval-minutes spec)]
    (cond
      (< (long m) 60)          (str "every " m " minutes")
      (zero? (long (mod (long m) 60))) (str "every " (/ (long m) 60) " hours")
      :else             (str "every " m " minutes"))
    (let [norm (normalize spec)]
      (str "at minute " (sort (:minute norm))
           " hour " (sort (:hour norm))
           (when (:dow-restricted? norm)
             (str " on days " (sort (:dow norm))))
           (when (:dom-restricted? norm)
             (str " on dates " (sort (:dom norm))))))))
