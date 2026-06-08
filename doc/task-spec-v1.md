# Task Spec V1

This document defines the initial executable task spec for Xia. It is a working
contract, not a final standard. The goal is to make tasks easy for humans to
inspect, easy for LLMs to author, and straightforward for deterministic or
hybrid runtimes to execute.

## Intent

All durable work in Xia is a task. A task may have a fully specified executable
plan, a loose interactive step, or a hybrid of deterministic and LLM-driven
steps. The authored plan is the task spec.

The old distinctions between interactive tasks, scheduled tasks, branch tasks,
and board cards are implementation details or projections. They should not be
different task types in the product model.

The core model is:

- `Task`: a durable unit of work with lifecycle state and runtime history.
- `Task Spec`: the authored executable contract for the task.
- `Task Step`: one unit of work inside the task spec.
- `Task Trigger`: why or how the task starts.
- `Task Projection`: how the task is organized or displayed, such as a board
  card.

This gives LLM planners one target: produce a task spec. The execution layer can
then run the spec deterministically, interactively, or through a hybrid LLM/tool
loop.

## Design Principles

- One durable task abstraction.
- Variation belongs in task steps, triggers, execution policy, and views, not in
  task types.
- A task spec is data. It must be authorable without running an LLM loop.
- Runtime state is separate from authored spec.
- Step ids are stable references, not display labels.
- Steps communicate through explicit inputs and outputs.
- Risky or externally visible actions require explicit approval or policy.
- A task may be partially specified. Underspecified work is represented by
  an `:llm` or `:input` step, not by escaping the model.
- Board and history views are projections over tasks, not separate stores.

## Canonical Task Shape

New task creation should converge on this shape:

```clojure
{:id ...
 :type :task
 :state :ready
 :title "Prepare weekly report"
 :summary "Prepare weekly report"

 :contract {:kind :task
            :version 1
            :goal "Prepare weekly report"
            :spec {...}}

 :meta {:trigger {:kind :user}
        :execution {:mode :hybrid}
        :runtime {...task runtime state...}
        :board {:visible? true
                :status :open
                :priority :normal}}}
```

`task.type` should be `:task` for all durable tasks. Existing concepts move to
metadata:

- Interactive task: `:meta {:execution {:mode :interactive}}`
- Scheduled task: `:meta {:trigger {:kind :schedule ...}}`
- Branch task: `:meta {:trigger {:kind :branch ...}}`
- Board card: `:meta {:board {...}}`

## Task Spec Shape

The v1 spec is an EDN map. HTTP and external APIs may expose the same structure
as JSON, with keywords serialized as strings.

```clojure
{:kind :task
 :version 1
 :goal "Prepare weekly report"
 :inputs {:report-date "2026-05-31"}
 :steps [{:id :collect-data
          :kind :tool
          :tool :database-query
          :args {:query "..."
                 :date [:input :report-date]}}

         {:id :summarize
          :kind :llm
          :prompt "Summarize the collected data."
          :inputs {:data [:output :collect-data]}}

         {:id :needs-review
          :kind :condition
          :expr [:present? [:output :summarize :risks]]}

         {:id :approve
          :kind :approval
          :when [:step-ok? :needs-review]
          :summary "Approve the report before sending."}

         {:id :send
          :kind :tool
          :tool :email-send
          :when [:or [:step-skipped? :needs-review]
                     [:step-ok? :approve]]
          :args {:subject "Weekly report"
                 :body [:output :summarize :body]}}]}
```

Top-level fields:

- `:kind`: must be `:task`.
- `:version`: integer spec version. Initial version is `1`.
- `:goal`: human-readable objective.
- `:inputs`: optional default inputs available to expressions.
- `:steps`: ordered vector of step maps.

## Step Model

Every step supports these fields:

- `:id`: required stable keyword/string id. Must be unique in the task spec.
- `:kind`: required step kind.
- `:when`: optional expression. If false, the step is skipped.
- `:summary`: optional human-readable description.
- `:inputs`: optional named inputs for the step.
- `:depends-on`: optional step id or collection of step ids that must complete
  before this step can run.
- `:timeout-ms`: optional per-attempt execution guardrail, in milliseconds.
- `:retry`: optional runner-level retry policy.

Step results have a common runtime shape:

```clojure
{:status :success | :skipped | :failed | :paused
 :output ...
 :summary "..."
 :error "..."}
```

Paused step results may include a standard pause payload:

```clojure
{:status :paused
 :pause-reason :external-wait
 :waiting-for :webhook
 :resume-token "opaque-token"
 :deadline "2026-06-02T18:00:00Z"
 :output {:request-id "req-1"}}
```

The runner normalizes these fields into `:pause` on both the task-spec runtime
state and the paused step:

```clojure
{:reason :external-wait
 :waiting-for :webhook
 :resume-token "opaque-token"
 :deadline "2026-06-02T18:00:00Z"}
```

On resume, the executor for the same non-terminal step receives the prior
`:pause`, plus any supplied `:resume-token` and `:resume-input`, in both the
executor argument map and `:context`. Runners may supply resume data as:

```clojure
(run-task! task-id
           :context {:resume-token "opaque-token"
                     :resume-input {:approved true}})
```

or as step-keyed input:

```clojure
(run-task! task-id
           :context {:resume-inputs {:step-id {:approved true}}})
```

Step outputs are addressable by later steps:

```clojure
[:output :step-id]
[:output :step-id :field]
[:output :step-id [:nested :path]]
```

### Dependencies And Scheduling

By default, ready steps run in vector order. `:depends-on` turns the step list
into a static dependency graph while keeping vector order as the tie-breaker
between ready steps.

```clojure
{:steps [{:id :join
          :kind :value
          :depends-on [:left :right]
          :value {:body [:str [:output :left] "\n" [:output :right]]}}

         {:id :left
          :kind :llm
          :depends-on :seed
          :prompt "Draft the left side."}

         {:id :right
          :kind :llm
          :depends-on :seed
          :prompt "Draft the right side."}

         {:id :seed
          :kind :value
          :value "source"}]}
```

The runner chooses the first non-terminal step whose dependencies are satisfied.
This means later ready steps may run before earlier blocked steps. Dependencies
are satisfied when the referenced step is `:success` or `:skipped`; failed
dependencies are terminal for normal execution. Unknown dependencies, self
dependencies, and dependency cycles are invalid specs.

This gives sequential DAG scheduling for fan-out/fan-in dataflow. It does not
make independent branches parallel by itself; use `:parallel` for concurrent
child tasks, `:map` for per-item child tasks, and `:loop` for repeated child
tasks.

### Validation

Task specs are validated when normalized or converted to a task contract. Hard
validation errors fail task creation. The validator checks:

- Supported task spec version.
- Step map shape, ids, uniqueness, and dependency graph integrity.
- Required fields for built-in step kinds.
- Expression operators, arity, and step references.
- Basic literal `:output-schema` shape for LLM steps.
- Literal positive guardrails such as `:timeout-ms` and `:max-iterations`.

`validate-spec` returns structured diagnostics:

```clojure
(validate-spec spec)
;; => {:valid? true
;;     :errors []
;;     :warnings [...]
;;     :spec normalized-spec}
```

Warnings are advisory. Unknown custom step kinds and missing tool registrations
are warnings because per-run executors or environment-specific tool setup may
provide them later.

### Retry And Timeout

The task-spec runner enforces `:timeout-ms` and `:retry` around executor calls.
Executors do not need to implement these guardrails themselves.

`:timeout-ms` is a positive integer. Each attempt receives its own timeout. If
an attempt exceeds the timeout, the runner cancels the attempt, records a failed
timeout result, and retries when the retry policy allows it.

`:retry` accepts:

- `true`: retry with the default policy, currently two total attempts.
- positive integer: total attempts.
- map: `{:max-attempts n}`, or `{:max-retries n}` for retries after the first
  attempt. Optional map fields are `:delay-ms`, `:initial-delay-ms`,
  `:max-delay-ms`, and `:backoff-factor`.

Only failed attempts are retried. `:success`, `:skipped`, and `:paused` are not
retried. An executor may return `{:status :failed :retryable? false}` or throw
an exception with `{:retryable? false}` in `ex-data` to make a failure terminal.

## Step Kinds

### `:value`

Evaluates `:value` as an expression or literal and stores it as the step output.

```clojure
{:id :render-title
 :kind :value
 :value [:str "Report for " [:input :report-date]]}
```

### `:emit`

Alias for `:value`. Use this when the step exists primarily to publish a named
intermediate value.

### `:condition`

Evaluates `:expr`. Truthy results mark the step `:success`; false or nil marks
the step `:skipped`.

```clojure
{:id :has-risks
 :kind :condition
 :expr [:present? [:output :summarize :risks]]}
```

### `:tool`

Calls a registered Xia tool. Tool arguments are data and may contain
expressions.

```clojure
{:id :send-email
 :kind :tool
 :tool :email-send
 :args {:to "user@example.com"
        :subject [:output :render-title]
        :body [:output :summarize :body]}}
```

### `:llm`

Runs an LLM-backed transformation or judgment step. By default this is a bounded
single-call step: the runner evaluates `:inputs`, builds a request from the
prompt and inputs, and stores the assistant result as the step output.

```clojure
{:id :summarize
 :kind :llm
 :mode :transform
 :prompt "Summarize the collected data and identify risks."
 :inputs {:data [:output :collect-data]}
 :output-schema {:type :object
                 :required [:body]
                 :properties {:body {:type :string}
                              :risks {:type :array}}}}
```

Modes:

- `:transform` or omitted: bounded language transformation.
- `:judge` or `:judgment`: bounded judgment/classification.
- `:agent` or `:interactive`: open-ended agent loop supplied by the runtime.

When `:output-schema` is present, it is treated as literal JSON-schema data. The
runner asks for JSON only, parses the response, validates required fields and
basic JSON types, and stores the parsed value as structured output. A step may
also request JSON parsing without a schema via `:output-format :json` or
`:json? true`.

Model and budget hints are data on the step. The built-in executor passes
`:provider-id` or `:provider`, `:workload`, `:model`, `:temperature`, and
`:max-tokens` / `:max-output-tokens` through the LLM request path. The active
runtime policy and budget guards decide whether the request is allowed.

### `:input`

Waits for user-provided input. Use this when required data is unavailable or
should not be guessed. The runner uses the task channel's registered input
handler, records `:input-request` and `input-response` task items, and stores
the supplied value as the step output.

```clojure
{:id :get-recipient
 :kind :input
 :label "Recipient"
 :description "Email address for the final report."
 :masked false}
```

### `:approval`

Waits for explicit approval before a sensitive action. The runner uses the task
channel's registered approval handler, records `:approval-request` and
`approval-decision` task items, and fails the step when approval is denied.

```clojure
{:id :approve-send
 :kind :approval
 :summary "Approve sending the report email."
 :description [:str "Send report to " [:output :get-recipient]]}
```

### `:branch`

Creates a child task. Use this when work can proceed independently but should
remain linked to the parent.

```clojure
{:id :research-competitors
 :kind :branch
 :mode :async
 :goal "Research competitor launches"
 :spec {...}}
```

Branch mode controls whether the parent waits:

- `:async`: creates a durable branch child task, starts it in the background,
  and lets the parent continue. The parent step output contains the child
  `:task-id` and `:status :running`.
- `:join`: runs the branch child through the task-spec runner and waits for it.
  The parent receives the same child-output shape as `:subtask`, including
  `:outputs`.

Branch children use `:channel :branch`, `:meta :trigger :kind :branch`, and
branch-worker context for policy/tool gating. If a joined branch pauses,
resuming the parent resumes the same child task instead of creating another one.

### `:subtask`

Runs a nested task spec inline. Use this for reusable task specs that should
complete before the parent continues.

```clojure
{:id :prepare-attachment
 :kind :subtask
 :inputs {:source [:output :collect-source]}
 :spec {:goal "Prepare attachment"
        :steps [...]}}
```

The runner creates a durable child task with `:meta :trigger :kind` set to
`:subtask`, links it to the parent task and parent step, and runs the nested
spec inline. The parent step output is:

```clojure
{:task-id child-task-id
 :status :completed
 :summary "..."
 :outputs {...}}
```

If the child pauses, the parent step pauses with the child task id in its output.
Resuming the parent resumes the same child task rather than creating a duplicate.

### `:parallel`

Runs a fixed set of child task specs concurrently and collects their outputs.
Each branch is a durable child task keyed by branch id, so pause/resume reuses
the same child task instead of creating duplicates.

```clojure
{:id :fanout
 :kind :parallel
 :inputs {:base [:input :base]}
 :concurrency 4
 :output-step :done
 :branches [{:id :left
             :inputs {:side "L"}
             :spec {:steps [{:id :done
                             :kind :value
                             :value [:str [:input :base] "-" [:input :side]]}]}}
            {:id :right
             :inputs {:side "R"}
             :spec {:steps [{:id :done
                             :kind :value
                             :value [:str [:input :base] "-" [:input :side]]}]}}]}
```

The step output is:

```clojure
{:branches {:left {:task-id ...
                   :status :completed
                   :outputs {...}}
            :right {...}}
 :outputs {:left "..."
           :right "..."}}
```

`:branches` may be a vector of maps with `:id`, or a map from id to branch spec.
`:concurrency` optionally caps the number of branches running at once; the
default cap is 8.
`:output-step` / `:collect-step` / `:result-step` optionally selects one child
step output to collect into the parent `:outputs` map.

### `:map`

Runs one durable child task per item and collects results in item order.

```clojure
{:id :render-items
 :kind :map
 :items [:input :items]
 :as :item
 :index-as :idx
 :output-step :done
 :spec {:steps [{:id :done
                 :kind :value
                 :value [:str [:input :idx] ": " [:input :item]]}]}}
```

The child spec receives the item under `:as` (default `:item`) and the zero-based
index under `:index-as` (default `:index`). The step output is:

```clojure
{:results [{:index 0
            :item ...
            :status :completed
            :task-id ...
            :outputs {...}
            :value "..."}]
 :outputs ["..."]}
```

### `:loop`

Repeats a child task spec until a condition stops it or `:max-iterations` is
reached. Each iteration is a durable child task keyed by iteration index.

```clojure
{:id :repeat
 :kind :loop
 :initial {:n 0}
 :while [:< [:input [:acc :n]] 3]
 :max-iterations 10
 :output-step :next
 :spec {:steps [{:id :next
                 :kind :custom-increment}]}}
```

Each iteration receives the current accumulator under `:acc` and the zero-based
iteration number under `:iteration`. Override those keys with `:acc-as` and
`:index-as`. After each completed child, the next accumulator is the selected
child `:output-step` value, or the full child output map if no collection step is
specified. `:while` is checked before each iteration; `:until` may also be used
to stop when it becomes truthy.

An iteration may also return explicit loop control from the collected value:

```clojure
{:control :break
 :value next-acc}

{:control :continue
 :value next-acc}
```

`:break` stops the loop successfully after the current iteration. `:continue`
continues with `:value` as the next accumulator. If a control map omits
`:value`, the previous accumulator is kept.

Loop output includes `:iterations`, collected `:outputs`, final `:value`, and
`:stopped`, which is `:condition`, `:max-iterations`, or `:break`.

### Future Control Steps

- `:sleep`: pause until a relative time.
- `:wait-until`: pause until an absolute time or external condition.

## Executor Registry

The runner dispatches each step by `:kind`. Executor functions receive:

```clojure
{:task-id ...
 :turn-id ...
 :state ...
 :context ...
 :step ...
 :pause ...                  ;; previous pause payload, when resuming
 :resume-token ...           ;; supplied resume token, when present
 :resume-input ...           ;; supplied resume input, when present
 :resume-input-provided? ...}
```

They return the standard step result map:

```clojure
{:status :success | :skipped | :failed | :paused
 :output ...
 :summary "..."
 :error "..."}
```

Executor resolution order is:

- Built-in executors.
- Globally registered executors.
- Per-run executor overrides supplied to the runner.

Registered executors are for stable step kinds that should be available to any
task runner. Per-run overrides are for request-scoped executors, such as an
LLM executor bound to a specific session, provider, budget guard, or turn
reservation.

## Expression Language

Expressions are vectors where the first element is an operator. Literal maps and
vectors are recursively evaluated.

Core references:

- `[:literal value]`
- `[:input path]`
- `[:output step-id]`
- `[:output step-id path]`
- `[:step-status step-id]`
- `[:step-ok? step-id]`
- `[:step-skipped? step-id]`
- `[:step-failed? step-id]`

Collection and lookup:

- `[:get target key default]`
- `[:get-in target path]`
- `[:count expr]`
- `[:contains? target value]`
- `[:empty? expr]`
- `[:present? expr]`

Comparison:

- `[:= a b]`
- `[:not= a b]`
- `[:> a b]`
- `[:>= a b]`
- `[:< a b]`
- `[:<= a b]`

Boolean logic:

- `[:and ...]`
- `[:or ...]`
- `[:not expr]`
- `[:if test then else]`

Construction:

- `[:merge ...]`
- `[:str ...]`
- `[:keyword expr]`

The expression language is intentionally small. It should be safe to evaluate
without arbitrary code execution.

## Lifecycle

Task lifecycle states:

- `:ready`: task exists but has not started.
- `:running`: runtime is actively executing steps.
- `:waiting_input`: blocked on an `:input` step.
- `:waiting_approval`: blocked on an `:approval` step.
- `:resumable`: paused at a restartable boundary.
- `:completed`: all required steps completed or skipped.
- `:failed`: terminal failure.
- `:cancelled`: stopped by user or policy.

Step lifecycle states:

- `:pending`: not started.
- `:running`: currently executing.
- `:success`: completed successfully.
- `:skipped`: condition or `:when` evaluated false.
- `:paused`: waiting for an executor, input, approval, or external condition.
- `:failed`: terminal step failure.

Resume is task-native. Resuming a task should continue from the first
non-terminal step using the same persisted spec and runtime state.

## Triggers

Triggers explain why a task exists or starts. They are metadata, not task types.

```clojure
{:kind :user
 :session-id ...}

{:kind :schedule
 :schedule-id :daily-report
 :run-id ...}

{:kind :branch
 :parent-task-id ...
 :parent-step-id :research-competitors}

{:kind :api
 :request-id ...}
```

Schedules should instantiate or resume tasks. They should not create a
separate scheduled task type.

## Board Projection

The board is a view over tasks. Board fields live in task metadata:

```clojure
{:board {:visible? true
         :status :open
         :priority :high
         :assignee "agent"
         :comments []}}
```

The board should group, filter, and claim tasks. It should not require tasks to
have a special `:board-card` type.

## Execution Modes

Execution mode describes how much autonomy the runtime has:

- `:deterministic`: only deterministic steps and tools may run.
- `:interactive`: user-visible LLM loop, suitable for loose goals.
- `:hybrid`: deterministic runner with LLM, input, and approval steps.
- `:agent`: open-ended LLM executor for underspecified steps.

Execution mode is policy and runtime configuration. It is not the task spec
itself.

## LLM Planner Rules

When an LLM authors a task spec, it should follow these rules:

- Return only a task spec, not prose, when asked for a machine plan.
- Use stable kebab-case ids for every step.
- Prefer explicit `:tool`, `:condition`, `:input`, and `:approval` steps over
  prose instructions inside one large `:llm` step.
- Use `:llm` for language-heavy transformation, judgment, or genuinely
  underspecified work.
- Do not invent tool ids. Use only tools from the provided tool catalog.
- Make tool arguments explicit data.
- Reference prior work through `[:output ...]`, not through hidden assumptions.
- Use `:input` when a required value is missing.
- Use `:approval` before irreversible, externally visible, or costly actions.
- Use `:condition` for branching.
- Keep steps small enough to inspect and retry.
- Include output schemas for `:llm` steps when downstream steps depend on their
  structure.
- Do not encode secrets in specs. Reference approved credential or service ids.

## Authoring Contract

The stable planning surface is `xia.task-spec/author-spec!` and
`xia.task-spec/repair-spec!`.

```clojure
(author-spec! "Summarize my inbox"
              :tools (task-spec-tool-catalog)
              :repair-attempts 1)

(repair-spec! "Summarize my inbox"
              invalid-spec
              validation-diagnostics
              :tools tool-catalog)
```

Both functions return:

```clojure
{:kind :task-spec-authoring-result
 :version 1
 :operation :create | :repair
 :goal "..."
 :status :success | :invalid
 :spec normalized-spec              ;; when valid
 :contract task-contract            ;; when valid
 :raw-spec parsed-model-spec
 :validation {:valid? ...
              :errors [...]
              :warnings [...]}
 :raw-response "..."}
```

`author-spec!` asks an LLM to produce `{"spec": ...}` from a user goal and a
tool catalog. If the first response fails validation, it may run repair passes
using the validation diagnostics. `repair-spec!` performs one explicit repair
pass for an existing spec. Tool ids in authored specs must come from the
provided catalog; out-of-catalog tool ids are authoring errors.

Planner JSON uses normal JSON objects and arrays. Since JSON has no keywords,
expression arrays may use string operators:

```json
["input", "topic"]
["output", "search", "content"]
```

The authoring surface normalizes those into task-spec expressions before
validation:

```clojure
[:input :topic]
[:output :search :content]
```

Planner JSON should use canonical task-spec field names such as
`output-schema`, `depends-on`, and `tool-id`. Common snake_case step aliases
such as `output_schema`, `depends_on`, and `tool_id` are accepted and normalized
away before validation. Tool `args` remain JSON-style argument objects at
execution time, so tool handlers receive string-keyed maps.

`task-spec-tool-catalog` returns JSON-friendly tool entries with stable
`:id`, `:name`, `:description`, `:parameters`, and optional policy metadata.
The planner must not invent tool ids outside that catalog.

## Examples

### Interactive Task

A loose user request can still be a task spec:

```clojure
{:kind :task
 :version 1
 :goal "Help me plan a trip"
 :steps [{:id :work-with-user
          :kind :llm
          :mode :interactive
          :prompt "Help the user plan the trip, asking for missing details."}]}
```

### Deterministic Tool Task

```clojure
{:kind :task
 :version 1
 :goal "Archive today's inbox report"
 :steps [{:id :fetch-report
          :kind :tool
          :tool :email-search
          :args {:query "subject:Daily Report newer:1d"}}

         {:id :save-artifact
          :kind :tool
          :tool :artifact-create
          :args {:name "daily-report.json"
                 :content [:output :fetch-report]}}]}
```

### Scheduled Task

The schedule is trigger metadata around a normal task:

```clojure
{:task/type :task
 :contract {:kind :task
            :version 1
            :goal "Send daily status summary"
            :spec {...}}
 :meta {:trigger {:kind :schedule
                  :schedule-id :daily-status}
        :execution {:mode :hybrid}}}
```

### Board Task

A board card is a task with board projection metadata:

```clojure
{:task/type :task
 :contract {:kind :task
            :version 1
            :goal "Fix OAuth refresh failures"
            :spec {...}}
 :meta {:trigger {:kind :user}
        :board {:visible? true
                :status :claimed
                :priority :high
                :assignee "xia"}}}
```

## Implementation Notes

The task-spec runner lives in the `xia.task-spec` namespace. Unknown step kinds
pause the task. The first implementation slice made this document the source of
truth for new task creation:

- New interactive, schedule, branch, board, and task-spec tasks use
  `:type :task`.
- Schedule, branch, board, and execution distinctions live in task metadata.
- Task-spec runtime state is stored under `:meta :task-spec`.
- Start/resume controls, scheduled runs, and branch-spawn paths enter the
  task-spec runner when the task has a spec.
- The runner dispatches each step to an executor by `:kind`; built-in
  executors handle `:value`, `:emit`, `:condition`, `:tool`, `:input`,
  `:approval`, `:llm`, `:subtask`, `:branch`, `:parallel`, `:map`, and
  `:loop`.
- Executors can be supplied through the global executor registry or per-run
  overrides. Per-run `:llm` overrides are still used for explicit
  `:mode :agent` / `:mode :interactive` steps that need the full agent loop.

Next implementation work:

- Make the Task UI render the task spec and runtime state for every task.
- Retire special product semantics for `:interactive`, `:schedule`,
  `:branch`, and `:board-card` task types.
