(ns xia.schedule-test
  (:require [clojure.test :refer :all]
            [xia.db :as db]
            [xia.prompt :as prompt]
            [xia.schedule :as schedule]
            [xia.test-helpers :refer [with-test-db]]
            [xia.working-memory :as wm]))

(use-fixtures :each with-test-db)

;; ---------------------------------------------------------------------------
;; Create
;; ---------------------------------------------------------------------------

(deftest create-tool-schedule
  (let [result (schedule/create-schedule!
                 {:id      :test-tool
                  :name    "Test Tool Schedule"
                  :spec    {:minute #{0} :hour #{9}}
                  :type    :tool
                  :tool-id :web-fetch})]
    (is (= :test-tool (:id result)))
    (is (some? (:next-run result)))))

(deftest create-prompt-schedule
  (let [result (schedule/create-schedule!
                 {:id     :test-prompt
                  :name   "Morning Check"
                  :spec   {:minute #{0} :hour #{8}}
                  :type   :prompt
                  :prompt "Check my email and summarize"})]
    (is (= :test-prompt (:id result)))
    (is (some? (:next-run result)))))

(deftest create-interval-schedule
  (let [result (schedule/create-schedule!
                 {:id      :every-30
                  :spec    {:interval-minutes 30}
                  :type    :tool
                  :tool-id :web-fetch})]
    (is (= :every-30 (:id result)))
    (is (some? (:next-run result)))))

(deftest create-requires-id
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"must have an :id"
        (schedule/create-schedule! {:spec {:minute #{0}} :type :tool :tool-id :x}))))

(deftest create-requires-spec
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"must have a :spec"
        (schedule/create-schedule! {:id :x :type :tool :tool-id :x}))))

(deftest create-requires-valid-type
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"must be :tool or :prompt"
        (schedule/create-schedule! {:id :x :spec {:minute #{0}} :type :bad}))))

(deftest create-tool-requires-tool-id
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"must specify :tool-id"
        (schedule/create-schedule! {:id :x :spec {:minute #{0}} :type :tool}))))

(deftest create-prompt-requires-prompt
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"must specify :prompt"
        (schedule/create-schedule! {:id :x :spec {:minute #{0}} :type :prompt}))))

(deftest create-rejects-too-frequent
  (let [decisions (atom [])]
    ;; Empty spec = every minute
    (with-redefs [prompt/policy-decision! (fn [decision]
                                            (swap! decisions conj decision)
                                            nil)]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"too frequent"
            (schedule/create-schedule!
              {:id :x :spec {} :type :tool :tool-id :x})))
      (is (some #(= {:decision-type :schedule-frequency-policy
                     :allowed? false
                     :mode :calendar-frequency
                     :min-interval-minutes 5}
                    (select-keys %
                                 [:decision-type :allowed? :mode :min-interval-minutes]))
                @decisions)))))

(deftest create-rejects-short-interval
  (let [decisions (atom [])]
    (with-redefs [prompt/policy-decision! (fn [decision]
                                            (swap! decisions conj decision)
                                            nil)]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"too frequent"
            (schedule/create-schedule!
              {:id :x :spec {:interval-minutes 2} :type :tool :tool-id :x})))
      (is (some #(= {:decision-type :schedule-frequency-policy
                     :allowed? false
                     :mode :interval-limit
                     :interval-minutes 2
                     :min-interval-minutes 5}
                    (select-keys %
                                 [:decision-type :allowed? :mode :interval-minutes :min-interval-minutes]))
                @decisions)))))

(deftest create-allows-5-min-interval
  (let [result (schedule/create-schedule!
                 {:id :every-5 :spec {:interval-minutes 5} :type :tool :tool-id :x})]
    (is (= :every-5 (:id result)))))

(deftest read-spec-rejects-reader-eval
  (is (thrown-with-msg?
        RuntimeException
        #"No dispatch macro for: ="
        (#'xia.schedule/read-spec "#=(+ 1 2)"))))

(deftest create-coerces-vectors-to-sets
  ;; Tool params arrive as JSON arrays → vectors
  (let [result (schedule/create-schedule!
                 {:id      :vec-test
                  :spec    {:minute [0 30] :hour [9 17]}
                  :type    :tool
                  :tool-id :x})]
    (is (= :vec-test (:id result)))
    (let [sched (schedule/get-schedule :vec-test)]
      (is (= #{0 30} (:minute (:spec sched))))
      (is (= #{9 17} (:hour (:spec sched)))))))

;; ---------------------------------------------------------------------------
;; Get
;; ---------------------------------------------------------------------------

(deftest get-schedule-returns-details
  (schedule/create-schedule!
    {:id          :detail-test
     :name        "Detail Test"
     :description "Testing details"
     :spec        {:minute #{0} :hour #{9}}
     :type        :tool
     :tool-id     :web-fetch
     :tool-args   {"url" "https://example.com"}})
  (let [sched (schedule/get-schedule :detail-test)]
    (is (= :detail-test (:id sched)))
    (is (= "Detail Test" (:name sched)))
    (is (= "Testing details" (:description sched)))
    (is (= #{0} (:minute (:spec sched))))
    (is (= #{9} (:hour (:spec sched))))
    (is (= :tool (:type sched)))
    (is (true? (:trusted? sched)))
    (is (= :web-fetch (:tool-id sched)))
    (is (= {"url" "https://example.com"} (:tool-args sched)))
    (is (true? (:enabled? sched)))
    (is (some? (:next-run sched)))))

(deftest get-nonexistent-returns-nil
  (is (nil? (schedule/get-schedule :nonexistent))))

;; ---------------------------------------------------------------------------
;; List
;; ---------------------------------------------------------------------------

(deftest list-schedules-test
  (schedule/create-schedule!
    {:id :sched-a :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (schedule/create-schedule!
    {:id :sched-b :spec {:minute #{0} :hour #{18}} :type :prompt :prompt "check stuff"})
  (let [scheds (schedule/list-schedules)]
    (is (= 2 (count scheds)))
    (is (every? :id scheds))
    (is (every? :spec scheds))))

;; ---------------------------------------------------------------------------
;; Update
;; ---------------------------------------------------------------------------

(deftest update-schedule-name
  (schedule/create-schedule!
    {:id :upd-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (let [updated (schedule/update-schedule! :upd-test {:name "New Name"})]
    (is (= "New Name" (:name updated)))))

(deftest update-schedule-trust
  (schedule/create-schedule!
    {:id :trust-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (let [updated (schedule/update-schedule! :trust-test {:trusted? false})]
    (is (false? (:trusted? updated)))))

(deftest update-schedule-spec-recalculates-next-run
  (schedule/create-schedule!
    {:id :upd-spec :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (let [before (:next-run (schedule/get-schedule :upd-spec))
        _      (schedule/update-schedule! :upd-spec {:spec {:minute #{0} :hour #{18}}})
        after  (:next-run (schedule/get-schedule :upd-spec))]
    (is (some? after))
    (is (not= before after))))

(deftest update-schedule-can-switch-between-prompt-and-tool
  (schedule/create-schedule!
    {:id :upd-kind :spec {:interval-minutes 30} :type :prompt :prompt "write a summary"})
  (let [tool-version (schedule/update-schedule! :upd-kind
                                                {:type :tool
                                                 :tool-id :email-list
                                                 :tool-args {:max_results 3}})
        prompt-version (schedule/update-schedule! :upd-kind
                                                  {:type :prompt
                                                   :prompt "write a new summary"})]
    (is (= :tool (:type tool-version)))
    (is (= :email-list (:tool-id tool-version)))
    (is (= {:max_results 3} (:tool-args tool-version)))
    (is (nil? (:prompt tool-version)))
    (is (= :prompt (:type prompt-version)))
    (is (= "write a new summary" (:prompt prompt-version)))
    (is (nil? (:tool-id prompt-version)))
    (is (nil? (:tool-args prompt-version)))))

(deftest update-nonexistent-throws
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"not found"
        (schedule/update-schedule! :nope {:name "x"}))))

;; ---------------------------------------------------------------------------
;; Pause / Resume
;; ---------------------------------------------------------------------------

(deftest pause-and-resume
  (schedule/create-schedule!
    {:id :pausable :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (is (true? (:enabled? (schedule/get-schedule :pausable))))
  (schedule/pause-schedule! :pausable)
  (is (false? (:enabled? (schedule/get-schedule :pausable))))
  (schedule/resume-schedule! :pausable)
  (is (true? (:enabled? (schedule/get-schedule :pausable)))))

;; ---------------------------------------------------------------------------
;; Remove
;; ---------------------------------------------------------------------------

(deftest remove-schedule-test
  (schedule/create-schedule!
    {:id :removable :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (is (some? (schedule/get-schedule :removable)))
  (schedule/remove-schedule! :removable)
  (is (nil? (schedule/get-schedule :removable))))

;; ---------------------------------------------------------------------------
;; Run history
;; ---------------------------------------------------------------------------

(deftest record-and-query-history
  (schedule/create-schedule!
    {:id :hist-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (let [now (java.util.Date.)]
    (schedule/record-run! :hist-test
      {:started-at  now
       :finished-at (java.util.Date.)
       :status      :success
       :result      "all good"})
    (schedule/record-run! :hist-test
      {:started-at  (java.util.Date.)
       :finished-at (java.util.Date.)
       :status      :error
       :error       "something broke"}))
  (let [history (schedule/schedule-history :hist-test)]
    (is (= 2 (count history)))
    ;; Most recent first
    (is (= :error (:status (first history))))
    (is (= :success (:status (second history))))))

(deftest record-run-persists-audit-actions
  (schedule/create-schedule!
    {:id :audit-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (schedule/record-run! :audit-test
    {:started-at  (java.util.Date.)
     :finished-at (java.util.Date.)
     :status      :success
     :actions     [{:tool-id "web-fetch"
                    :status "success"
                    :arguments {"url" "https://example.com"}}]
     :result      "done"})
  (let [run (first (schedule/schedule-history :audit-test))]
    (is (= [{:tool-id "web-fetch"
             :status "success"
             :arguments {"url" "https://example.com"}}]
           (:actions run)))
    (is (= "done" (:result run)))))

(deftest safe-schedule-history-redacts-audit-actions
  (schedule/create-schedule!
    {:id :audit-safe :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (schedule/record-run! :audit-safe
    {:started-at  (java.util.Date.)
     :finished-at (java.util.Date.)
     :status      :error
     :actions     [{:tool-id "browser-login" :status "blocked"}]
     :error       "sensitive failure"})
  (let [run (first (schedule/safe-schedule-history :audit-safe 1))]
    (is (= :error (:status run)))
    (is (not (contains? run :actions)))
    (is (not (contains? run :error)))))

(deftest record-run-updates-last-run
  (schedule/create-schedule!
    {:id :last-run-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (is (nil? (:last-run (schedule/get-schedule :last-run-test))))
  (schedule/record-run! :last-run-test
    {:started-at (java.util.Date.) :status :success})
  (is (some? (:last-run (schedule/get-schedule :last-run-test)))))

(deftest trim-history-keeps-recent
  (schedule/create-schedule!
    {:id :trim-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (dotimes [_ 5]
    (schedule/record-run! :trim-test
      {:started-at (java.util.Date.) :status :success :result "ok"})
    (Thread/sleep 10))
  (is (= 5 (count (schedule/schedule-history :trim-test 100))))
  (schedule/trim-history! :trim-test 2)
  (is (= 2 (count (schedule/schedule-history :trim-test 100)))))

;; ---------------------------------------------------------------------------
;; Durable task state / recovery
;; ---------------------------------------------------------------------------

(deftest task-state-builds-recovery-context
  (schedule/create-schedule!
    {:id :recoverable :spec {:minute #{0} :hour #{9}} :type :prompt :prompt "Check the dashboard"})
  (schedule/save-task-checkpoint!
    :recoverable
    {:phase :tool
     :round 1
     :summary "Opened the dashboard and attempted the primary action."
     :tool-ids ["browser-open" "browser-click"]
     :progress-status :resumable
     :stack [{:title "Check the dashboard" :progress-status :in-progress}
             {:title "Resume alternate path" :progress-status :resumable}]
     :agenda [{:item "Open dashboard" :status :completed}
              {:item "Resume alternate path" :status :resumable}
              {:item "Investigate new branch" :status :diverged}]})
  (let [state (schedule/record-task-failure! :recoverable "No element matches selector #submit")
        prompt (schedule/augment-prompt-with-recovery-context :recoverable "Check the dashboard")]
    (is (= :backoff (:status state)))
    (is (= :error (:phase state)))
    (is (= 1 (:consecutive-failures state)))
    (is (= "No element matches selector #submit" (:last-error state)))
    (is (= {:decision-type :schedule-failure-policy
            :mode :backoff
            :same-failure? false
            :consecutive-failures 1}
           (select-keys (:last-policy state)
                        [:decision-type :mode :same-failure? :consecutive-failures])))
    (is (re-find #"Recovery context from previous scheduled attempts" prompt))
    (is (re-find #"browser-query-elements" prompt))
    (is (re-find #"Opened the dashboard and attempted the primary action" prompt))
    (is (re-find #"Last progress status: resumable" prompt))
    (is (re-find #"Last execution stack: \[in-progress\] Check the dashboard > \[resumable\] Resume alternate path" prompt))
    (is (re-find #"\[resumable\] Resume alternate path" prompt))
    (is (re-find #"\[diverged\] Investigate new branch" prompt))))

(deftest repeated-identical-failures-pause-schedule
  (db/set-config! :schedule/pause-after-repeated-failures 2)
  (schedule/create-schedule!
    {:id :fragile-site :spec {:minute #{0} :hour #{9}} :type :prompt :prompt "Inspect the site"})
  (schedule/record-task-failure! :fragile-site "No element matches selector #main")
  (let [state (schedule/record-task-failure! :fragile-site "No element matches selector #main")
        sched (schedule/get-schedule :fragile-site)]
    (is (= :paused (:status state)))
    (is (= 2 (:consecutive-failures state)))
    (is (= {:decision-type :schedule-failure-policy
            :mode :pause
            :same-failure? true
            :consecutive-failures 2}
           (select-keys (:last-policy state)
                        [:decision-type :mode :same-failure? :consecutive-failures])))
    (is (false? (:enabled? sched)))))

(deftest failed-prompt-schedule-exposes-resumable-session
  (let [session-id (db/create-session! :scheduler)]
    (schedule/create-schedule!
      {:id :resume-me :spec {:minute #{0} :hour #{9}} :type :prompt :prompt "Continue the workflow"})
    (wm/create-wm! session-id)
    (schedule/save-task-checkpoint!
      :resume-me
      {:phase :tool
       :summary "Reached the target site and opened the work queue."
       :session-id session-id})
    (schedule/record-task-failure! :resume-me "Timed out waiting for dashboard")
    (is (= session-id
           (schedule/resumable-session-id :resume-me)))
    (wm/clear-wm! session-id)
    (is (nil? (schedule/resumable-session-id :resume-me))
        "A cleared working memory should force the schedule to start a fresh session")
    (schedule/record-task-success! :resume-me "Finished successfully.")
    (is (nil? (schedule/resumable-session-id :resume-me)))))

(deftest task-success-clears-backoff-state
  (schedule/create-schedule!
    {:id :stabilized :spec {:minute #{0} :hour #{9}} :type :prompt :prompt "Do the thing"})
  (schedule/record-task-failure! :stabilized "Timed out waiting for page")
  (let [state (schedule/record-task-success! :stabilized "Recovered after reloading the page.")]
    (is (= :success (:status state)))
    (is (= :complete (:phase state)))
    (is (= 0 (:consecutive-failures state)))
    (is (= "Recovered after reloading the page." (:last-success-summary state)))
    (is (nil? (:last-policy state)))
    (is (nil? (:backoff-until state)))))

;; ---------------------------------------------------------------------------
;; Due schedules
;; ---------------------------------------------------------------------------

(deftest due-schedules-query
  (schedule/create-schedule!
    {:id :due-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  ;; Set next-run to the past
  (db/transact! [{:schedule/id :due-test
                  :schedule/next-run (java.util.Date. 0)}])
  (let [due (schedule/due-schedules (java.util.Date.))]
    (is (= 1 (count due)))
    (is (= :due-test (:id (first due))))))

(deftest disabled-schedules-not-due
  (schedule/create-schedule!
    {:id :disabled-test :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})
  (db/transact! [{:schedule/id :disabled-test
                  :schedule/next-run (java.util.Date. 0)}])
  (schedule/pause-schedule! :disabled-test)
  (is (empty? (schedule/due-schedules (java.util.Date.)))))

;; ---------------------------------------------------------------------------
;; Schedule limit
;; ---------------------------------------------------------------------------

(deftest schedule-limit-enforced
  (let [decisions (atom [])]
    (dotimes [i 50]
      (schedule/create-schedule!
        {:id (keyword (str "sched-" i))
         :spec {:minute #{0} :hour #{9}}
         :type :tool
         :tool-id :x}))
    (with-redefs [prompt/policy-decision! (fn [decision]
                                            (swap! decisions conj decision)
                                            nil)]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Too many schedules"
            (schedule/create-schedule!
              {:id :one-too-many :spec {:minute #{0} :hour #{9}} :type :tool :tool-id :x})))
      (is (some #(= {:decision-type :schedule-count-policy
                     :allowed? false
                     :mode :schedule-limit
                     :current-count 50
                     :max-schedules 50}
                    (select-keys %
                                 [:decision-type :allowed? :mode :current-count :max-schedules]))
                @decisions)))))
