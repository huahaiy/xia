(ns xia.runtime-health
  "Control-plane oriented runtime health and idleness inspection."
  (:require [xia.agent :as agent]
            [xia.hippocampus :as hippo]
            [xia.llm :as llm]
            [xia.runtime-state :as runtime-state]
            [xia.scheduler :as scheduler]))

(defn idle-status
  "Return a machine-oriented snapshot of whether Xia is safe to stop.

   `:idle?` means:
   - runtime phase is `:running`
   - no active agent turns/runs
   - no scheduler executions or maintenance are in flight
   - no pending hippocampus or async LLM log background work remains"
  []
  (let [phase      (runtime-state/phase)
        agent*     (agent/runtime-activity)
        scheduler* (scheduler/runtime-activity)
        hippo*     (hippo/runtime-activity)
        llm*       (llm/runtime-activity)
        blockers   (cond-> []
                     (pos? (long (:active-session-run-count agent*)))
                     (conj {:component :agent
                            :kind :session-runs
                            :count (long (:active-session-run-count agent*))
                            :reason "agent turns are still running"})

                     (pos? (long (:active-session-turn-count agent*)))
                     (conj {:component :agent
                            :kind :session-turns
                            :count (long (:active-session-turn-count agent*))
                            :reason "session turn locks are still active"})

                     (pos? (long (:active-task-run-count agent*)))
                     (conj {:component :agent
                            :kind :task-runs
                            :count (long (:active-task-run-count agent*))
                            :reason "task runs are still active"})

                     (pos? (long (:running-schedule-count scheduler*)))
                     (conj {:component :scheduler
                            :kind :schedule-runs
                            :count (long (:running-schedule-count scheduler*))
                            :reason "scheduled work is still running"})

                     (:maintenance-running? scheduler*)
                     (conj {:component :scheduler
                            :kind :maintenance
                            :count 1
                            :reason "scheduler maintenance is still running"})

                     (pos? (long (:pending-background-task-count hippo*)))
                     (conj {:component :hippocampus
                            :kind :background-consolidation
                            :count (long (:pending-background-task-count hippo*))
                            :reason "memory consolidation is still running"})

                     (pos? (long (:pending-log-write-count llm*)))
                     (conj {:component :llm
                            :kind :background-log-writes
                            :count (long (:pending-log-write-count llm*))
                            :reason "async LLM log writes are still pending"}))
        idle?      (and (= :running phase)
                        (empty? blockers))]
    {:phase phase
     :idle? idle?
     :shutdown-allowed? idle?
     :blockers blockers
     :activity {:agent agent*
                :scheduler scheduler*
                :hippocampus hippo*
                :llm llm*}}))
