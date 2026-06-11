(ns xia.policy
  "Stable facade for runtime policy decisions.

   Policy logic lives in xia.policy.* namespaces by domain. This namespace
   keeps call sites from depending on those internal boundaries."
  (:require [xia.policy.agent :as agent]
            [xia.policy.async :as async]
            [xia.policy.browser :as browser]
            [xia.policy.http :as http]
            [xia.policy.llm :as llm]
            [xia.policy.ocr :as ocr]
            [xia.policy.plugin :as plugin]
            [xia.policy.scheduler :as scheduler]
            [xia.policy.service :as service]
            [xia.policy.tool :as tool]))

(def supervisor-max-identical-iterations agent/supervisor-max-identical-iterations)
(def supervisor-semantic-loop-threshold agent/supervisor-semantic-loop-threshold)
(def supervisor-max-restarts agent/supervisor-max-restarts)
(def supervisor-restart-backoff-ms agent/supervisor-restart-backoff-ms)
(def supervisor-restart-grace-ms agent/supervisor-restart-grace-ms)
(def task-restart-loop-limit agent/task-restart-loop-limit)
(def task-restart-loop-window-ms agent/task-restart-loop-window-ms)
(def autonomous-max-iterations agent/autonomous-max-iterations)
(def autonomous-max-stack-depth agent/autonomous-max-stack-depth)
(def max-tool-rounds agent/max-tool-rounds)
(def max-tool-calls-per-round agent/max-tool-calls-per-round)
(def parallel-tool-timeout-ms agent/parallel-tool-timeout-ms)
(def branch-task-timeout-ms agent/branch-task-timeout-ms)
(def supervisor-phase-timeout-ms agent/supervisor-phase-timeout-ms)
(def supervisor-llm-timeout-ms agent/supervisor-llm-timeout-ms)
(def supervisor-tool-timeout-ms agent/supervisor-tool-timeout-ms)
(def supervisor-worker-timeout-ms agent/supervisor-worker-timeout-ms)
(def max-user-message-chars agent/max-user-message-chars)
(def max-user-message-tokens agent/max-user-message-tokens)
(def max-branch-tasks agent/max-branch-tasks)
(def max-parallel-branches agent/max-parallel-branches)
(def max-branch-tool-rounds agent/max-branch-tool-rounds)
(def branch-error-stack-frames agent/branch-error-stack-frames)
(def llm-status-preview-chars agent/llm-status-preview-chars)
(def llm-status-update-interval-ms agent/llm-status-update-interval-ms)
(def supervisor-tick-ms agent/supervisor-tick-ms)
(def task-control-wait-ms agent/task-control-wait-ms)
(def tool-call-limit-decision agent/tool-call-limit-decision)
(def autonomy-iteration-limit-policy agent/autonomy-iteration-limit-policy)
(def tool-round-limit-decision agent/tool-round-limit-decision)
(def user-message-size-decision agent/user-message-size-decision)
(def branch-task-count-policy agent/branch-task-count-policy)
(def parallel-tool-timeout-policy agent/parallel-tool-timeout-policy)
(def branch-task-timeout-policy agent/branch-task-timeout-policy)
(def restart-policy-decision agent/restart-policy-decision)

(def async-background-max-threads async/async-background-max-threads)
(def async-background-queue-capacity async/async-background-queue-capacity)
(def async-parallel-max-threads async/async-parallel-max-threads)
(def async-parallel-queue-capacity async/async-parallel-queue-capacity)

(def browser-playwright-timeout-ms browser/browser-playwright-timeout-ms)

(def http-request-retry-config http/http-request-retry-config)
(def http-request-retry-enabled? http/http-request-retry-enabled?)
(def http-request-backoff-ms http/http-request-backoff-ms)
(def http-request-retry-decision http/http-request-retry-decision)

(def llm-max-provider-retry-rounds llm/llm-max-provider-retry-rounds)
(def llm-max-provider-retry-wait-ms llm/llm-max-provider-retry-wait-ms)
(def llm-retry-after-ms llm/llm-retry-after-ms)
(def llm-retryable-error? llm/llm-retryable-error?)
(def llm-retry-sleep-ms llm/llm-retry-sleep-ms)
(def provider-rate-limit-policy llm/provider-rate-limit-policy)

(def local-doc-ocr-timeout-ms ocr/local-doc-ocr-timeout-ms)
(def local-doc-ocr-max-tokens ocr/local-doc-ocr-max-tokens)

(def plugin-hook-timeout-ms plugin/plugin-hook-timeout-ms)
(def plugin-hook-max-code-chars plugin/plugin-hook-max-code-chars)
(def plugin-max-hooks plugin/plugin-max-hooks)
(def plugin-max-active-workers plugin/plugin-max-active-workers)

(def schedule-failure-backoff-minutes scheduler/schedule-failure-backoff-minutes)
(def schedule-max-failure-backoff-minutes scheduler/schedule-max-failure-backoff-minutes)
(def schedule-pause-after-repeated-failures scheduler/schedule-pause-after-repeated-failures)
(def max-schedules scheduler/max-schedules)
(def min-schedule-interval-minutes scheduler/min-schedule-interval-minutes)
(def scheduler-max-concurrent-runs scheduler/scheduler-max-concurrent-runs)
(def schedule-frequency-policy scheduler/schedule-frequency-policy)
(def schedule-count-policy scheduler/schedule-count-policy)
(def schedule-failure-backoff-ms scheduler/schedule-failure-backoff-ms)
(def schedule-failure-policy scheduler/schedule-failure-policy)

(def service-rate-limit-policy service/service-rate-limit-policy)

(def tool-sci-eval-timeout-ms tool/tool-sci-eval-timeout-ms)
(def tool-sci-handler-timeout-ms tool/tool-sci-handler-timeout-ms)
(def tool-max-active-sci-workers tool/tool-max-active-sci-workers)
(def tool-pipeline-timeout-ms tool/tool-pipeline-timeout-ms)
(def tool-pipeline-max-calls tool/tool-pipeline-max-calls)
(def tool-pipeline-max-code-chars tool/tool-pipeline-max-code-chars)
(def tool-id tool/tool-id)
(def tool-name tool/tool-name)
(def tool-description tool/tool-description)
(def tool-channel-compatible? tool/tool-channel-compatible?)
(def tool-channel-block-message tool/tool-channel-block-message)
(def tool-vision-compatible? tool/tool-vision-compatible?)
(def tool-vision-block-message tool/tool-vision-block-message)
(def normalize-approval-policy tool/normalize-approval-policy)
(def matching-privileged-rules tool/matching-privileged-rules)
(def inferred-tool-approval-policy tool/inferred-tool-approval-policy)
(def tool-approval-policy tool/tool-approval-policy)
(def tool-autonomous-scopes tool/tool-autonomous-scopes)
(def autonomous-tool-allowed? tool/autonomous-tool-allowed?)
(def autonomous-tool-block-message tool/autonomous-tool-block-message)
(def branch-worker-tool-allowed? tool/branch-worker-tool-allowed?)
(def tool-autonomous-allowed? tool/tool-autonomous-allowed?)
(def tool-autonomous-block-message tool/tool-autonomous-block-message)
(def tool-execution-policy-context tool/tool-execution-policy-context)
(def tool-visible? tool/tool-visible?)
(def tool-description-for-llm tool/tool-description-for-llm)
(def tool-autonomous-approval-decision tool/tool-autonomous-approval-decision)
(def tool-restart-risk-policy tool/tool-restart-risk-policy)
(def tool-execution-decision tool/tool-execution-decision)
(def tool-preflight-decision tool/tool-preflight-decision)
(def tool-execution-decision-for-approval tool/tool-execution-decision-for-approval)
