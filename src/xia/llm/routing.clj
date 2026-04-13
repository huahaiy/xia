(ns xia.llm.routing
  "Provider-routing and runtime helpers for the LLM subsystem."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [xia.util :as util])
  (:import [java.net URI]))

(defn workload-routes
  [workload-options]
  workload-options)

(defn reset-runtime!
  [async-log-lock async-log-state]
  (locking async-log-lock
    (reset! async-log-state
            {:accepting? true
             :tasks #{}})))

(defn prepare-shutdown!
  [async-log-lock async-log-state]
  (locking async-log-lock
    (let [{:keys [tasks]} (swap! async-log-state assoc :accepting? false)
          pending        (count tasks)]
      (when (pos? pending)
        (log/info "Waiting for" pending "LLM log write(s) before database close"))
      pending)))

(defn await-background-tasks!
  [async-log-lock async-log-state]
  (loop []
    (when-let [task (locking async-log-lock
                      (first (:tasks @async-log-state)))]
      (try
        @task
        (catch Exception _
          nil))
      (recur))))

(defn submit-log-write!
  [{:keys [async-log-lock async-log-state submit-background! log-llm-call!]} log-entry]
  (locking async-log-lock
    (when (:accepting? @async-log-state)
      (let [self (promise)
            task (submit-background!
                  "llm-log-write"
                  #(let [me @self]
                     (try
                       (log-llm-call! log-entry)
                       (catch Exception e
                         (log/debug e "Failed to write LLM call log"))
                       (finally
                         (locking async-log-lock
                           (swap! async-log-state update :tasks disj me))))))]
        (when task
          (swap! async-log-state update :tasks conj task)
          (deliver self task))
        task))))

(defn- current-workload-id-set
  [workload-options]
  (set (map :id workload-options)))

(defn known-workload?
  [workload-options workload]
  (contains? (current-workload-id-set workload-options) workload))

(defn normalize-workload
  [workload-options workload]
  (let [normalized (cond
                     (nil? workload) nil
                     (keyword? workload) workload
                     (string? workload) (keyword workload)
                     :else (throw (ex-info "LLM workload must be a keyword or string"
                                           {:workload workload})))]
    (when (and normalized
               (not (known-workload? workload-options normalized)))
      (throw (ex-info (str "Unknown LLM workload: " normalized)
                      {:workload normalized
                       :known-workloads (mapv :id workload-options)})))
    normalized))

(defn- provider-key
  [provider]
  (or (:llm.provider/id provider) (:id provider)))

(defn- provider-workloads
  [provider]
  (set (or (:llm.provider/workloads provider)
           (:workloads provider))))

(defn effective-rate-limit-per-minute
  [default-rate-limit-per-minute provider]
  (long (or (:llm.provider/rate-limit-per-minute provider)
            (:rate-limit-per-minute provider)
            default-rate-limit-per-minute)))

(defn loopback-base-url?
  [loopback-hosts base-url]
  (try
    (contains? loopback-hosts
               (some-> base-url URI. .getHost str/lower-case))
    (catch Exception _
      false)))

(defn provider-allows-private-network?
  [loopback-hosts provider]
  (or (:llm.provider/allow-private-network? provider)
      (:allow-private-network? provider)
      (loopback-base-url? loopback-hosts
                          (or (:llm.provider/base-url provider)
                              (:base-url provider)))))

(defn normalize-base-url
  [base-url]
  (str/replace (str (or base-url "")) #"/+$" ""))

(defn base-url-host
  [base-url]
  (try
    (some-> base-url normalize-base-url URI. .getHost str/lower-case)
    (catch Exception _
      nil)))

(defn provider-family-from-base-url
  [base-url]
  (if (= "api.anthropic.com" (base-url-host base-url))
    :anthropic
    :openai))

(defn vision-capable?
  [get-provider-fn provider-or-id]
  (let [provider (cond
                   (nil? provider-or-id) nil
                   (map? provider-or-id) provider-or-id
                   :else (get-provider-fn provider-or-id))]
    (boolean (:llm.provider/vision? provider))))

(defn- provider-cooldown-ms
  ^long
  [base-cooldown-ms max-cooldown-ms consecutive-failures]
  (loop [cooldown (long base-cooldown-ms)
         remaining (util/long-max 0 (dec (long consecutive-failures)))]
    (if (zero? remaining)
      cooldown
      (recur (util/long-min (long max-cooldown-ms)
                       (* 2 cooldown))
             (dec remaining)))))

(defn- provider-health-entry
  [provider-health provider-id]
  (get @provider-health provider-id))

(defn- default-provider-id
  [get-default-provider-fn]
  (some-> (get-default-provider-fn) provider-key))

(defn provider-health-summary
  [{:keys [provider-health get-default-provider now-ms]} provider-id]
  (let [provider-id           (or provider-id (default-provider-id get-default-provider))
        {:keys [consecutive-failures cooldown-until-ms last-success-ms last-failure-ms last-error]}
        (provider-health-entry provider-health provider-id)
        current-ms            (long (now-ms))
        cooldown-remaining-ms (when cooldown-until-ms
                                (util/long-max 0 (- (long cooldown-until-ms) current-ms)))
        status                (cond
                                (pos? (long (or cooldown-remaining-ms 0))) :cooling-down
                                (pos? (long (or consecutive-failures 0)))   :degraded
                                :else                                       :healthy)]
    {:provider-id           provider-id
     :status                status
     :healthy?              (= :healthy status)
     :available?            (not= :cooling-down status)
     :consecutive-failures  (long (or consecutive-failures 0))
     :cooldown-until-ms     cooldown-until-ms
     :cooldown-remaining-ms cooldown-remaining-ms
     :last-success-ms       last-success-ms
     :last-failure-ms       last-failure-ms
     :last-error            last-error}))

(defn record-provider-success!
  [provider-health now-ms provider-id]
  (let [timestamp (long (now-ms))]
    (swap! provider-health assoc provider-id
           {:consecutive-failures 0
            :cooldown-until-ms   nil
            :last-success-ms     timestamp
            :last-failure-ms     nil
            :last-error          nil})))

(defn record-provider-failure!
  [{:keys [provider-health now-ms base-cooldown-ms max-cooldown-ms]}
   provider-id error-message & {:keys [cooldown-ms]}]
  (let [timestamp (long (now-ms))]
    (swap! provider-health
           (fn [state]
             (let [previous             (get state provider-id)
                   consecutive-failures (inc (long (or (:consecutive-failures previous) 0)))
                   cooldown-ms          (util/long-max (long (or cooldown-ms 0))
                                                  (provider-cooldown-ms base-cooldown-ms
                                                                        max-cooldown-ms
                                                                        consecutive-failures))]
               (assoc state provider-id
                      {:consecutive-failures consecutive-failures
                       :cooldown-until-ms   (+ timestamp cooldown-ms)
                       :last-success-ms     (:last-success-ms previous)
                       :last-failure-ms     timestamp
                       :last-error          error-message}))))
    (provider-health-summary {:provider-health provider-health
                              :get-default-provider (constantly nil)
                              :now-ms now-ms}
                             provider-id)))

(defn attempts-cooldown-delay-ms
  [provider-health-summary-fn attempts]
  (let [health (mapv #(provider-health-summary-fn (:provider-id %)) attempts)]
    (when (and (seq health)
               (every? (comp not :available?) health))
      (some->> health
               (keep :cooldown-remaining-ms)
               (filter pos?)
               seq
               (apply min)))))

(defn- provider-group-key
  [{:keys [available? consecutive-failures cooldown-until-ms]}]
  (if available?
    [0 consecutive-failures]
    [1 (or cooldown-until-ms Long/MAX_VALUE) consecutive-failures]))

(defn- provider-sort-key
  [{:keys [available? consecutive-failures cooldown-until-ms provider-id]}]
  (conj (provider-group-key {:available? available?
                             :consecutive-failures consecutive-failures
                             :cooldown-until-ms cooldown-until-ms})
        (name provider-id)))

(defn- rotate-vector
  [items offset]
  (let [items  (vec items)
        length (count items)]
    (cond
      (<= length 1) items
      :else
      (let [offset (mod offset length)]
        (vec (concat (subvec items offset)
                     (subvec items 0 offset)))))))

(defn- default-provider-selection
  [get-default-provider-fn]
  (let [provider (or (get-default-provider-fn)
                     (throw (ex-info "No default LLM provider configured. Run first-time setup." {})))]
    {:provider    provider
     :provider-id (provider-key provider)}))

(defn- workload-providers
  [list-providers-fn workload]
  (->> (list-providers-fn)
       (filter #(contains? (provider-workloads %) workload))
       (sort-by #(some-> % provider-key name))
       vec))

(defn- round-robin-provider
  [workload-counters workload-options workload providers]
  (let [active-workloads (current-workload-id-set workload-options)
        state (swap! workload-counters
                     (fn [counters]
                       (let [counters* (select-keys counters active-workloads)]
                         (update counters* workload (fnil inc -1)))))
        index (mod (get state workload 0) (count providers))]
    (nth providers index)))

(defn- ordered-workload-providers
  [provider-health-summary-fn workload-counters workload-options workload providers]
  (let [annotated    (->> providers
                          (mapv (fn [provider]
                                  (let [provider-id (provider-key provider)
                                        health      (provider-health-summary-fn provider-id)]
                                    (assoc health
                                           :provider provider
                                           :provider-id provider-id
                                           :group-key (provider-group-key health)
                                           :sort-key (provider-sort-key
                                                      (assoc health :provider-id provider-id))))))
                          (sort-by :sort-key))
        grouped      (mapv vec (partition-by :group-key annotated))
        first-group  (first grouped)
        rotated-head (if (seq first-group)
                       (let [seed-provider (round-robin-provider workload-counters
                                                                 workload-options
                                                                 workload
                                                                 (mapv :provider first-group))
                             seed-id       (provider-key seed-provider)
                             start-index   (or (first (keep-indexed (fn [idx entry]
                                                                      (when (= seed-id (:provider-id entry))
                                                                        idx))
                                                                    first-group))
                                               0)]
                         (rotate-vector first-group start-index))
                       [])
        rest-groups  (mapcat identity (rest grouped))]
    (mapv :provider (concat rotated-head rest-groups))))

(defn provider-attempts
  [{:keys [workload-options get-provider get-default-provider list-providers workload-counters provider-health-summary-fn]}
   {:keys [provider-id workload]}]
  (let [workload (normalize-workload workload-options workload)]
    (cond
      provider-id
      [(let [provider (or (get-provider provider-id)
                          (throw (ex-info (str "Unknown provider: " provider-id)
                                          {:provider provider-id})))]
         {:provider    provider
          :provider-id (provider-key provider)})]

      workload
      (if-let [providers (seq (workload-providers list-providers workload))]
        (mapv (fn [provider]
                {:provider    provider
                 :provider-id (provider-key provider)
                 :workload    workload})
              (ordered-workload-providers provider-health-summary-fn
                                          workload-counters
                                          workload-options
                                          workload
                                          (vec providers)))
        [(assoc (default-provider-selection get-default-provider) :workload workload)])

      :else
      [(default-provider-selection get-default-provider)])))

(defn resolve-provider-selection
  [deps opts]
  (first (provider-attempts deps opts)))
