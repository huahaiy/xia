(ns xia.extractive-summary
  (:require [clojure.set :as set]
            [clojure.string :as str]))

(def ^:private month-or-time-pattern
  #"(?i)\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec|q[1-4]|utc)\b")

(defn- compact-text
  [text]
  (some-> text
          str
          str/trim
          (str/replace #"\s+" " ")
          not-empty))

(defn- truncate-text
  [text max-chars]
  (when-let [compact (compact-text text)]
    (if (and max-chars (> (count compact) max-chars))
      (str (subs compact 0 (max 0 (dec max-chars))) "…")
      compact)))

(defn- tokenize
  [text]
  (->> (str/split (str/lower-case (or text "")) #"[^\p{Alnum}]+")
       (remove #(or (str/blank? %)
                    (< (count %) 2)))
       vec))

(defn- token-set
  [text]
  (set (tokenize text)))

(defn- overlap-ratio
  [left right]
  (let [left*  (token-set left)
        right* (token-set right)
        denom  (max 1 (min (count left*) (count right*)))]
    (/ (double (count (set/intersection left* right*)))
       denom)))

(defn- paragraph-blocks
  [text]
  (let [normalized (str/replace (or text "") #"\r\n?" "\n")]
    (->> (str/split normalized #"\n\s*\n+")
         (map str/trim)
         (remove str/blank?)
         vec)))

(defn- heading-block?
  [block]
  (let [lines   (->> (str/split-lines (or block ""))
                     (map str/trim)
                     (remove str/blank?)
                     vec)
        compact (compact-text block)]
    (and compact
         (<= (count lines) 3)
         (every? #(<= (count %) 80) lines)
         (not-any? #(re-find #"[.!?]\)?$" %) lines))))

(defn- extract-heading
  [text]
  (let [blocks (paragraph-blocks text)
        first-block (first blocks)]
    (when (heading-block? first-block)
      (compact-text first-block))))

(defn- content-text
  [text]
  (let [blocks (paragraph-blocks text)]
    (if (and (seq blocks) (heading-block? (first blocks)))
      (str/join "\n\n" (rest blocks))
      text)))

(defn- sentence-fragments
  [text]
  (->> (str/split (or (compact-text text) "") #"(?<=[.!?])\s+")
       (map str/trim)
       (remove str/blank?)
       vec))

(defn- salient-score
  [fragment idx heading-tokens]
  (let [compact         (compact-text fragment)
        text            (or compact "")
        char-count      (count text)
        fragment-tokens (token-set text)
        heading-overlap (count (set/intersection fragment-tokens heading-tokens))]
    (+ (max 0 (- 6 idx))
       (cond
         (<= 40 char-count 180) 2.5
         (<= 25 char-count 260) 1.0
         :else 0.0)
       (if (re-find #"\d" text) 3.0 0.0)
       (if (re-find #"[/$%]" text) 2.0 0.0)
       (if (re-find month-or-time-pattern text) 2.0 0.0)
       (if (re-find #"\b[A-Z]{2,}\b" fragment) 1.0 0.0)
       (* 1.5 heading-overlap))))

(defn- select-fragments
  [fragments max-chars {:keys [heading]}]
  (let [heading*        (compact-text heading)
        heading-tokens  (token-set heading*)
        indexed         (map-indexed vector fragments)
        anchor          (first indexed)
        candidates      (sort-by (fn [[idx fragment]]
                                   [(- (salient-score fragment idx heading-tokens))
                                    idx])
                                 (rest indexed))
        chosen          (loop [selected (cond-> [] anchor (conj anchor))
                               remaining candidates]
                          (if-let [[idx fragment] (first remaining)]
                            (if (some #(>= (overlap-ratio fragment (second %)) 0.8)
                                      selected)
                              (recur selected (rest remaining))
                              (recur (conj selected [idx fragment]) (rest remaining)))
                            selected))
        ordered         (->> chosen
                             (sort-by first)
                             (map second)
                             (remove str/blank?))
        kept-fragments  (reduce (fn [acc fragment]
                                  (let [candidate (str/join " " (conj acc fragment))]
                                    (if (and max-chars
                                             (> (count candidate) max-chars))
                                      acc
                                      (conj acc fragment))))
                                []
                                ordered)
        body            (or (some->> kept-fragments seq (str/join " "))
                            (some-> (first ordered) (truncate-text max-chars)))
        prefixed-body   (if (and heading*
                                 (seq body)
                                 (not (str/includes? (str/lower-case body)
                                                     (str/lower-case heading*))))
                          (str heading* ": " body)
                          body)]
    (truncate-text prefixed-body max-chars)))

(defn summarize-text
  [text max-chars]
  (let [heading   (extract-heading text)
        content   (content-text text)
        sentences (sentence-fragments content)]
    (or (when (seq sentences)
          (select-fragments sentences max-chars {:heading heading}))
        (truncate-text text max-chars))))

(defn summarize-document
  [chunk-summaries full-text max-chars]
  (let [summaries (->> chunk-summaries
                       (map compact-text)
                       (remove str/blank?)
                       vec)]
    (or (when (seq summaries)
          (select-fragments summaries max-chars {}))
        (summarize-text full-text max-chars))))
