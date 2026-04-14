(ns xia.agent.fact-review
  "Background fact-utility review queue and scheduler."
  (:require [taoensso.timbre :as log]
            [xia.async :as async]
            [xia.working-memory :as wm]))

(def ^:private default-fact-utility-review-debounce-ms 2000)
(def ^:private fact-utility-review-batch-size 20)
(def ^:private fact-utility-review-max-pending 120)
(defonce fact-utility-review-state (atom {}))

(defn reset-runtime!
  []
  (reset! fact-utility-review-state {})
  nil)

(defn- fact-utility-observations
  [fact-eids user-message assistant-response explicit-fact-eids]
  (let [explicit-facts (set explicit-fact-eids)]
    (into []
          (map (fn [fact-eid]
                 {:fact-eid fact-eid
                  :user-message user-message
                  :assistant-response assistant-response
                  :explicitly-used? (contains? explicit-facts fact-eid)}))
          (distinct fact-eids))))

(declare run-fact-utility-review-worker!)

(defn- fact-utility-review-retry-backoff!
  [attempt]
  (let [attempt* (long (max 1 (long attempt)))]
    (if (<= attempt* 3)
      (Thread/yield)
      (Thread/sleep (long (min 5 (bit-shift-left 1 (min 2 (- attempt* 4)))))))))

(defn- update-fact-utility-review-state!
  ([f]
   (update-fact-utility-review-state! fact-utility-review-state compare-and-set! f))
  ([state-atom cas-fn f]
   (loop [attempt 1]
     (let [before @state-atom
           [after result] (f before)]
       (if (cas-fn state-atom before after)
         result
         (do
           (fact-utility-review-retry-backoff! attempt)
           (recur (inc attempt))))))))

(defn- enqueue-fact-utility-review-state
  [state session-id observations]
  (let [{:keys [pending] :as session-state}
        (get state session-id)
        pending* (->> (concat pending observations)
                      (take-last fact-utility-review-max-pending)
                      vec)]
    (if-let [existing-token (:worker-token session-state)]
      [(assoc state session-id (assoc session-state :pending pending*)) nil]
      (let [token (random-uuid)]
        [(assoc state session-id {:pending pending*
                                  :worker-token token})
         token]))))

(defn- claim-fact-utility-review-batch-state
  [state session-id worker-token]
  (let [{:keys [pending] :as session-state}
        (get state session-id)]
    (if (= worker-token (:worker-token session-state))
      (let [batch*     (vec (take fact-utility-review-batch-size pending))
            remaining* (vec (drop fact-utility-review-batch-size pending))]
        [(assoc state session-id (assoc session-state :pending remaining*))
         batch*])
      [state nil])))

(defn- finish-fact-utility-review-worker-state
  [state session-id worker-token]
  (let [{:keys [pending] :as session-state} (get state session-id)]
    (cond
      (not= worker-token (:worker-token session-state))
      [state nil]

      (seq pending)
      (let [next-token (random-uuid)]
        [(assoc state session-id {:pending pending
                                  :worker-token next-token})
         next-token])

      :else
      [(dissoc state session-id) nil])))

(defn- abandon-fact-utility-review-worker-state
  [state session-id worker-token]
  (let [{current-token :worker-token :as session-state} (get state session-id)]
    (if (= worker-token current-token)
      [(assoc state session-id (dissoc session-state :worker-token)) true]
      [state false])))

(defn- enqueue-fact-utility-review!
  [session-id fact-eids user-message assistant-response explicit-fact-eids debounce-ms]
  (let [observations (fact-utility-observations fact-eids
                                                user-message
                                                assistant-response
                                                explicit-fact-eids)]
    (when (seq observations)
      (when-let [token (update-fact-utility-review-state!
                        #(enqueue-fact-utility-review-state % session-id observations))]
        (when-not (async/submit-background!
                   "fact-utility-review"
                   #(run-fact-utility-review-worker! session-id token debounce-ms))
          (update-fact-utility-review-state!
           #(abandon-fact-utility-review-worker-state % session-id token)))))
    (count observations)))

(defn- claim-fact-utility-review-batch!
  [session-id worker-token]
  (update-fact-utility-review-state!
   #(claim-fact-utility-review-batch-state % session-id worker-token)))

(defn- finish-fact-utility-review-worker!
  [session-id worker-token debounce-ms]
  (when-let [token
             (update-fact-utility-review-state!
              #(finish-fact-utility-review-worker-state % session-id worker-token))]
    (when-not (async/submit-background!
               "fact-utility-review"
               #(run-fact-utility-review-worker! session-id token debounce-ms))
      (update-fact-utility-review-state!
       #(abandon-fact-utility-review-worker-state % session-id token)))))

(defn- run-fact-utility-review-worker!
  [session-id worker-token debounce-ms]
  (try
    (Thread/sleep (long debounce-ms))
    (loop []
      (when-let [batch (seq (claim-fact-utility-review-batch! session-id worker-token))]
        (try
          (wm/review-fact-utility-observations! batch)
          (catch Exception e
            (log/warn e "Failed to review fact utility batch"
                      {:session-id session-id
                       :batch-size (count batch)})))
        (recur)))
    (catch InterruptedException _
      (.interrupt (Thread/currentThread)))
    (finally
      (finish-fact-utility-review-worker! session-id worker-token debounce-ms))))

(defn schedule-fact-utility-review!
  [session-id fact-eids user-message assistant-response & {:keys [debounce-ms]
                                                           explicit-fact-eids :explicit-fact-eids
                                                           :or {debounce-ms default-fact-utility-review-debounce-ms}}]
  (let [fact-eids* (->> (concat fact-eids explicit-fact-eids)
                        distinct
                        vec)]
    (when (and (seq fact-eids*)
             (seq assistant-response))
      (wm/apply-explicit-fact-utility! explicit-fact-eids)
      (wm/apply-fact-utility-heuristic! fact-eids* assistant-response)
      (enqueue-fact-utility-review! session-id
                                    fact-eids*
                                    user-message
                                    assistant-response
                                    explicit-fact-eids
                                    debounce-ms))))
