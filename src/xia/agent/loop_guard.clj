(ns xia.agent.loop-guard
  "Autonomous-loop stall detection helpers."
  (:require [clojure.string :as str]
            [datalevin.embedding :as emb]
            [taoensso.timbre :as log]
            [xia.autonomous :as autonomous]
            [xia.db :as db]
            [xia.task-policy :as task-policy]))

(def ^:private loop-signature-stopwords
  #{"a" "an" "and" "are" "for" "from" "into" "its" "more" "now" "of" "on" "or"
    "remaining" "remains" "step" "steps" "still" "the" "to" "with" "work" "working"})

(def ^:private loop-signature-token-aliases
  {"again" "retry"
   "attempt" "retry"
   "attempting" "retry"
   "attempts" "retry"
   "check" "inspect"
   "checked" "inspect"
   "checking" "inspect"
   "compose" "draft"
   "composed" "draft"
   "composing" "draft"
   "drafted" "draft"
   "drafting" "draft"
   "examine" "inspect"
   "examined" "inspect"
   "examining" "inspect"
   "fetch" "search"
   "fetched" "search"
   "fetching" "search"
   "find" "search"
   "finding" "search"
   "found" "search"
   "inspect" "inspect"
   "inspected" "inspect"
   "inspecting" "inspect"
   "look" "inspect"
   "looked" "inspect"
   "looking" "inspect"
   "lookup" "search"
   "open" "inspect"
   "opened" "inspect"
   "opening" "inspect"
   "post" "send"
   "posted" "send"
   "posting" "send"
   "query" "search"
   "queried" "search"
   "querying" "search"
   "read" "inspect"
   "reading" "inspect"
   "reads" "inspect"
   "repeat" "retry"
   "repeated" "retry"
   "repeating" "retry"
   "reply" "send"
   "replied" "send"
   "replying" "send"
   "respond" "send"
   "responded" "send"
   "responding" "send"
   "review" "inspect"
   "reviewed" "inspect"
   "reviewing" "inspect"
   "scan" "inspect"
   "scanned" "inspect"
   "scanning" "inspect"
   "search" "search"
   "searched" "search"
   "searching" "search"
   "send" "send"
   "sending" "send"
   "sent" "send"
   "submit" "send"
   "submitted" "send"
   "submitting" "send"
   "view" "inspect"
   "viewed" "inspect"
   "viewing" "inspect"
   "write" "draft"
   "writing" "draft"
   "written" "draft"})

(def ^:private progress-status-scores
  {:not-started 0
   :pending 0
   :paused 0
   :resumable 0
   :diverged 0
   :blocked 0
   :in-progress 1
   :complete 5})

(defn- loop-text-signature
  [text]
  (some->> text
           str
           str/lower-case
           (re-seq #"[a-z0-9]+")
           (map #(get loop-signature-token-aliases % %))
           (remove #(or (< (count ^String %) 3)
                        (contains? loop-signature-stopwords %)))
           distinct
           sort
           vec
           not-empty))

(defn wm-query-signature
  [message]
  (loop-text-signature message))

(defn- loop-agenda-signature
  [agenda]
  (->> agenda
       (keep (fn [{:keys [item status]}]
               (when (or item status)
                 {:item-terms (loop-text-signature item)
                  :status status})))
       vec
       not-empty))

(defn- loop-stack-signature
  [stack]
  (->> stack
       (keep (fn [{:keys [title progress-status]}]
               (when (or title progress-status)
                 {:title-terms (loop-text-signature title)
                  :progress-status progress-status})))
       vec
       not-empty))

(defn- semantic-loop-fallback-signature
  [autonomy-state control]
  (let [tip (autonomous/current-frame autonomy-state)]
    {:root-goal-terms (loop-text-signature (autonomous/root-goal autonomy-state))
     :title-terms (loop-text-signature (:title tip))
     :next-step-terms (loop-text-signature (:next-step control))
     :agenda (loop-agenda-signature (:agenda tip))
     :stack (loop-stack-signature (:stack autonomy-state))}))

(defn- semantic-loop-text
  [autonomy-state control]
  (let [tip (autonomous/current-frame autonomy-state)
        goal (some-> (autonomous/root-goal autonomy-state) str not-empty)
        focus (some-> (:title tip) str not-empty)
        next-step (some-> (:next-step control) str not-empty)
        stack (->> (:stack autonomy-state)
                   (keep (fn [{:keys [title progress-status]}]
                           (when title
                             (str "- "
                                  "[" (some-> progress-status name) "] "
                                  title)))))
        agenda (->> (:agenda tip)
                    (keep (fn [{:keys [item status]}]
                            (when item
                              (str "- "
                                   "[" (some-> status name) "] "
                                   item)))))]
    (some->> [(when goal
                (str "Goal: " goal))
              (when focus
                (str "Focus: " focus))
              (when next-step
                (str "Next step: " next-step))
              (when (seq agenda)
                (str "Agenda:\n" (str/join "\n" agenda)))
              (when (seq stack)
                (str "Stack:\n" (str/join "\n" stack)))]
             (remove str/blank?)
             seq
             (str/join "\n"))))

(defn- cosine-similarity
  [left right]
  (when (and (sequential? left)
             (sequential? right)
             (= (count left) (count right))
             (pos? (count left)))
    (let [[dot norm-left norm-right]
          (reduce (fn [[dot* norm-left* norm-right*] [left-value right-value]]
                    (let [left* (double left-value)
                          right* (double right-value)]
                      [(+ dot* (* left* right*))
                       (+ norm-left* (* left* left*))
                       (+ norm-right* (* right* right*))]))
                  [0.0 0.0 0.0]
                  (map vector left right))]
      (when (and (pos? norm-left) (pos? norm-right))
        (/ dot
           (* (Math/sqrt norm-left)
              (Math/sqrt norm-right)))))))

(defn- embed-loop-text
  [embedding-cache text]
  (let [text* (some-> text str str/trim not-empty)]
    (cond
      (nil? text*)
      [embedding-cache nil]

      (contains? embedding-cache text*)
      [embedding-cache (get embedding-cache text*)]

      :else
      (let [embedding (try
                        (when-let [provider (db/current-embedding-provider)]
                          (some-> (emb/embedding provider [text*] nil)
                                  first
                                  vec))
                        (catch Throwable t
                          (log/debug t "Failed to embed autonomy loop state")
                          nil))]
        [(assoc embedding-cache text* embedding) embedding]))))

(defn- semantic-loop-equivalent?
  [embedding-cache previous-signature next-signature]
  (let [previous-fallback (:semantic-fallback previous-signature)
        next-fallback (:semantic-fallback next-signature)]
    (if (and previous-fallback
             next-fallback
             (= previous-fallback next-fallback))
      {:embedding-cache embedding-cache
       :same-semantic? true
       :semantic-similarity 1.0
       :semantic-match-source :fallback}
      (let [[embedding-cache* previous-embedding]
            (embed-loop-text embedding-cache (:semantic-text previous-signature))
            [embedding-cache** next-embedding]
            (embed-loop-text embedding-cache* (:semantic-text next-signature))
            similarity (cosine-similarity previous-embedding next-embedding)]
        {:embedding-cache embedding-cache**
         :same-semantic? (boolean (and similarity
                                       (>= similarity
                                           (task-policy/supervisor-semantic-loop-threshold))))
         :semantic-similarity similarity
         :semantic-match-source (when similarity :embedding)}))))

(defn- iteration-progress-marker
  [autonomy-state control]
  (let [tip (autonomous/current-frame autonomy-state)
        stack (vec (:stack autonomy-state))
        agenda (vec (:agenda tip))
        stack-statuses (frequencies (keep :progress-status stack))
        agenda-statuses (frequencies (keep :status agenda))]
    {:goal-complete? (true? (:goal-complete? control))
     :stack-depth (count stack)
     :stack-complete (long (get stack-statuses :complete 0))
     :tip-status (:progress-status tip)
     :agenda-total (count agenda)
     :agenda-complete (long (+ (get agenda-statuses :completed 0)
                               (get agenda-statuses :skipped 0)))
     :agenda-active (long (get agenda-statuses :in-progress 0))
     :agenda-statuses agenda-statuses
     :stack-statuses stack-statuses}))

(defn- iteration-progress-score
  [{:keys [goal-complete? stack-complete tip-status agenda-complete agenda-active]}]
  (+ (if goal-complete? 1000 0)
     (* 100 (long (or stack-complete 0)))
     (* 10 (long (or agenda-complete 0)))
     (* 2 (long (or agenda-active 0)))
     (long (get progress-status-scores tip-status 0))))

(defn- iteration-tool-marker
  [tool-activity]
  (let [results (->> tool-activity
                     (mapcat :results)
                     vec)
        statuses (frequencies (keep :status results))
        failure-count (long (get statuses "error" 0))
        success-count (long (get statuses "success" 0))
        total-count (+ failure-count success-count)
        error-terms (->> results
                         (keep (fn [{:keys [error summary status]}]
                                 (when (= status "error")
                                   (or (loop-text-signature error)
                                       (loop-text-signature summary)))))
                         vec
                         not-empty)]
    {:round-count (count tool-activity)
     :total-count total-count
     :failure-count failure-count
     :success-count success-count
     :only-failures? (and (pos? total-count)
                          (zero? success-count))
     :error-signature error-terms}))

(defn- repeated-tool-failure-loop?
  [previous-signature next-signature]
  (let [previous-tool-marker (:tool-marker previous-signature)
        next-tool-marker (:tool-marker next-signature)]
    (and (:only-failures? previous-tool-marker)
         (:only-failures? next-tool-marker))))

(defn- iteration-stall-key
  [signature]
  (select-keys signature
               [:progress-status
                :stack-action
                :progress-marker]))

(defn iteration-signature
  [autonomy-state control tool-activity]
  (let [tip (autonomous/current-frame autonomy-state)
        progress-marker (iteration-progress-marker autonomy-state control)]
    {:progress-status (:progress-status tip)
     :stack-action (:stack-action control)
     :tool-activity tool-activity
     :tool-marker (iteration-tool-marker tool-activity)
     :progress-marker progress-marker
     :semantic-text (semantic-loop-text autonomy-state control)
     :semantic-fallback (semantic-loop-fallback-signature autonomy-state control)}))

(defn update-iteration-loop-state
  [{:keys [signature stall-key progress-score count embedding-cache]} next-signature]
  (let [next-stall-key (iteration-stall-key next-signature)
        next-progress-score (iteration-progress-score (:progress-marker next-signature))
        same-stall-state? (= stall-key next-stall-key)
        progressed? (> next-progress-score
                       (long (or progress-score Long/MIN_VALUE)))
        repeated-tool-failure? (and signature
                                    same-stall-state?
                                    (not progressed?)
                                    (repeated-tool-failure-loop? signature
                                                                 next-signature))
        {:keys [embedding-cache same-semantic? semantic-similarity semantic-match-source]}
        (if (and signature
                 same-stall-state?
                 (not progressed?))
          (semantic-loop-equivalent? (or embedding-cache {}) signature next-signature)
          {:embedding-cache (or embedding-cache {})
           :same-semantic? false
           :semantic-similarity nil
           :semantic-match-source nil})
        semantic-match-source (cond
                                (and repeated-tool-failure? same-semantic?)
                                :tool-failure

                                :else
                                semantic-match-source)]
    (if (and same-stall-state? same-semantic? (not progressed?))
      {:signature next-signature
       :stall-key next-stall-key
       :progress-score next-progress-score
       :embedding-cache embedding-cache
       :semantic-similarity semantic-similarity
       :semantic-match-source semantic-match-source
       :count (inc (long (or count 0)))}
      {:signature next-signature
       :stall-key next-stall-key
       :progress-score next-progress-score
       :embedding-cache embedding-cache
       :semantic-similarity semantic-similarity
       :semantic-match-source semantic-match-source
       :count 1})))

(defn throw-if-identical-iteration-loop!
  [session-id channel iteration max-iterations loop-state autonomy-state control]
  (let [count* (long (or (:count loop-state) 0))
        limit (long (task-policy/supervisor-max-identical-iterations))]
    (when (>= count* limit)
      (let [tip (autonomous/current-frame autonomy-state)]
        (throw (ex-info (str "Autonomous loop made no progress after "
                             limit
                             " identical iterations")
                        {:type :autonomous-loop-stalled
                         :session-id session-id
                         :channel channel
                         :iteration iteration
                         :max-iterations max-iterations
                         :current-focus (:title tip)
                         :progress-status (:progress-status tip)
                         :next-step (:next-step control)
                         :semantic-similarity (:semantic-similarity loop-state)
                         :semantic-match-source (:semantic-match-source loop-state)
                         :agenda (:agenda tip)
                         :stack (:stack autonomy-state)}))))))
