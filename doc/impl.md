# Xia Implementation Notes

This document collects the technical details that do not belong in the user-facing README.

For a product overview and quick start, see the [README](../README.md).

## Product Shape

Xia is an **online-first** assistant. Its primary focus is the digital world beyond your local machine: web research, form automation, API orchestration, and maintaining a persistent knowledge graph of your digital life.

Its product identity comes from the old Snail Maiden story. Xia is meant to feel less like a commanding operator and
more like a quiet household spirit for the online parts of a user's life:
present, dependable, and attentive to details.

### Hardware And Accessibility

Unlike autonomous assistants that are expected to live on a dedicated machine, Xia is designed to live on your daily computer. Because its tools are sandboxed and isolated from the local file system, it can run alongside normal work without host-level side effects. Any modern computer works.

### Database-Centric Portability

Xia keeps its state in its database, but encrypted secrets are protected by external key material.

- In passphrase mode that is your startup passphrase plus a non-secret salt file stored under `<db-path>/.xia/master.salt`.
- File-backed master keys and passphrases must live outside `<db-path>`.
- On POSIX systems, file-backed secrets should use owner-only permissions such as `0600` or `0400`.
- `xia pack` can bundle local support files into a portable archive.
- `xia backup.xia` opens that archive directly for non-technical users.

Typical portable path:

```bash
# use Xia normally
xia

# create a single-file portable archive
xia pack backup.xia

# later, on this or another machine, open it directly
xia backup.xia
```

Notes:

- No manual unzip step is required for `.xia` archives.
- Xia extracts the archive into a hidden working directory beside the archive and repacks changes back into the same `.xia` file on normal exit.
- If the archive uses passphrase mode, enter the same master passphrase when opening it.
- If you manually extract an archive that contains `db/.xia/master.key` or `db/.xia/master.passphrase`, move that file to a secure path outside the extracted DB and lock down its permissions before using `XIA_MASTER_KEY_FILE` or `XIA_MASTER_PASSPHRASE_FILE`.
- If the archive depends on a raw env-provided key such as `XIA_MASTER_KEY`, that key still must be supplied on the target machine.

Operational defaults:

- Xia does not create a log file by default.
- Warnings and errors go to stderr.
- Use `xia --log-file xia.log` or `XIA_LOG_FILE=/path/to/xia.log` when you want persistent logs.

### Xia Vs. Local Assistants

Unlike tools like Claude Code or Codex, Xia is not designed for local file system manipulation or host-level computer automation.

- **Xia:** focused on the online environment, secure credential management, and long-term memory.
- **Local tools:** better suited for editing local source code, managing files, and executing terminal commands on the host.

### Interoperability

While Xia does not grant tools direct local file access, it can orchestrate
local tools that expose an API. If you have a local service or a coding
assistant with an accessible endpoint, Xia can interact with it through its
service capability proxy without broadening host access.

## Core Capabilities

Xia is designed to be a long-lived assistant that learns from every interaction.

- **Human-inspired memory:** separate working memory, knowledge graph, and episodic memory layers.
- **Secure web and browser automation:** SSRF-protected fetch tools with DNS-pinned outbound connections and a Playwright browser runtime.
- **Authenticated online work:** stored API credentials, website logins, and first-class OAuth accounts.
- **Session-scoped local documents:** explicit uploads of text, PDF, and Office docs with chunk-preferred retrieval and summary generation.
- **Portable prompt skills:** native Xia skills plus a safe importer for a prompt-only subset of OpenClaw skills.
- **Inspectable task specs:** durable task plans with explicit steps, dataflow, approvals, pause/resume state, and runtime history.
- **Review-first skill learning:** completed tasks can generate reusable skill proposals, with LLM review allowed only for bounded agent-authored changes.
- **Autonomous task scheduling:** recurring tasks, background maintenance, and session continuity.
- **Privacy-first security:** strict credential isolation even when tools act on the user’s behalf.

## Memory Architecture

Xia's memory is modeled after human cognition, allowing it to maintain context over months of interaction without being overwhelmed by irrelevant data.

### Working Memory

Working memory holds the active, curated context for the current conversation. It is updated every turn through a retrieval pipeline:

1. **Keyword extraction:** identifies core concepts in user messages.
2. **Hybrid retrieval:** performs parallel full-text search across the knowledge graph and past episodes.
3. **Spreading activation:** expands search by one hop in the graph to activate related concepts.
4. **Relevance-based decay:** unrefreshed working-memory entities lose relevance each turn and are evicted below threshold.
5. **Topic tracking:** summarizes current focus and detects major topic shifts to segment memory.

### Knowledge Graph

The knowledge graph stores structured entities, relations, and atomic facts.

- **Structured extraction:** Xia extracts entities and properties from conversations.
- **Smart deduplication:** new information is merged with existing facts.
- **Confidence maintenance:** stale or unreinforced facts decay after a grace period.

### Episodic Memory

Every interaction is recorded as an episode. A background consolidation process reviews episodes to extract knowledge and reinforce existing patterns, moving them from raw conversation history into long-term structure.

## Local Document Ingestion

Local documents are explicit user uploads, not ambient host file access. Xia
stores them as session-scoped records in the DB so they can be recalled later
without exposing the host file system to tools.

### Supported Formats

Current ingestion supports:

- plain text and text-like formats such as Markdown, JSON, EDN, XML, YAML, CSV, TSV, logs, and source code
- PDF extraction through OpenPDF
- Office extraction for `docx`, `xlsx`, and `pptx`

### Chunking And Retrieval

Large documents are normalized and chunked at natural boundaries rather than by
blind fixed-width slicing.

- blank-line-separated blocks are preserved where possible
- short heading-like blocks are attached to the following body text
- oversized blocks split by sentence when possible, then by hard wrap as a fallback
- retrieval prefers chunk-level matches while still returning parent document metadata
- chunk hits carry parent doc name and summary so prompt assembly can stay document-aware

### Summaries

Document ingestion always stores a preview and a summary, but the summary path is configurable.

- **Default:** extractive summaries, using heading-aware and salience-aware heuristics
- **Optional local model:** Datalevin-backed local generation through the embedded llama.cpp runtime
- **Optional external model:** an OpenAI-compatible provider selected from Xia's provider config

Model-based summaries are off by default. The default experience remains fully
local and deterministic, and the admin UI exposes the summary backend and token
budget settings when users want to opt in.

## LLM Limits Layer

LLM budget accounting lives in `xia.limits`, separate from task/tool policy.
The layer creates per-turn, per-task, and per-schedule-run budget state, records
provider usage after each request, normalizes prompt/completion/total token
counts, writes a sanitized persistent usage ledger, and raises a common
`:limit-exhausted` exception with the exhausted scope attached.

This keeps model selection and spend policy from leaking into the task runtime.
The current implementation enforces call, token, wall-clock, LLM-duration, and
ledger-backed policy ceilings. Org, session, and schedule ceilings can be set
with `:limits/<scope>-max-llm-calls`, `:limits/<scope>-max-total-tokens`, and
`:limits/<scope>-max-cost-micros`, where `<scope>` is `org`, `session`, or
`schedule`. Optional cost estimation uses `:limits/model-prices`, an EDN map of
`[:provider-id "model"]` to `{:input-usd-per-1m n :output-usd-per-1m n}`.

Each ceiling supports `:limits/<scope>-warn-ratio`,
`:limits/<scope>-near-action`, and `:limits/<scope>-action`. Actions are
`:warn`, `:deny`, `:require-approval`, `:pause-schedule`, `:prefer-local`, and
`:downgrade-model`. Routing actions use `:limits/prefer-local-provider-id` or
`:limits/downgrade-provider-id` when the user has not explicitly selected a
provider.

Future model-routing improvements should extend this namespace and expose shared
policy entry points through `xia.policy`.

## Runtime Overlay Contract

Managed Xia runtimes load an optional EDN overlay through `xia.runtime-overlay`.
The v1 wire shape is closed and uses `:overlay/schema-version 1`; unknown
top-level keys are rejected at load time. Required keys are `:snapshot/id`,
`:tenant/id`, `:runtime/id`, `:generated-at`, `:config-overrides`,
`:bounded-config`, `:tx-data`, and `:forced-keys`.

`:config-overrides` contains literal replace values. `:bounded-config` contains
cap-style policy bounds that are exposed through the same config readers with
merge mode `:cap`. Secret material in overlay entities or secret config keys is
referenced as `{:secret/file "/run/..."}` and resolved in memory only.

Remote browser authentication follows the same managed-secret rule: the overlay
sets `:browser/remote-token-file`, and the remote backend reads the bearer token
from that file at request time. Xia no longer relies on inline
`:browser/remote-auth-token` for the SaaS runtime contract.

## Managed Tenant Proxy Auth

Local Xia UI auth and managed SaaS tenant auth are separate. Local routes still
use the `xia-local-session` cookie and local-origin checks. The `/local-session`
bootstrap endpoint remains local-only and must not be used by tenant-origin
browsers.

Managed tenant traffic is admitted only when Hai has already authenticated the
Better Auth session, authorized tenant membership, and `gang` forwards a signed
proxy proof to Xia over the private route. The overlay enables this mode with
`:http/managed-proxy-enabled? true`, points Xia at a shared HMAC secret through
`:http/managed-proxy-secret-file`, and may bind the public tenant origin with
`:http/managed-tenant-origin`.

The signed request headers are `X-Xia-Proxy-Mode: tenant`,
`X-Xia-Tenant-Id`, `X-Xia-Runtime-Id`, `X-Xia-User-Id`,
`X-Xia-Request-Id`, `X-Xia-Proxy-Timestamp`, and
`X-Xia-Proxy-Signature`. Xia verifies tenant/runtime against the active runtime
overlay, checks timestamp skew, rejects replayed request ids, and validates the
HMAC. `/command/*` routes keep their separate command-channel auth contract.

## Task Boundaries And Operating Envelopes

Pause, completion, and terminal task boundaries are finalized into
`:task/boundary` instead of being buried in `:task/meta`. The finalizer writes
qualified keys for the next continuation point:

- `:boundary/summary`
- `:boundary/resume-hint`
- `:boundary/next-step`
- `:boundary/stack-tip`
- `:boundary/open-questions`
- `:boundary/schedule-run-hint`

The task runtime still exposes compatibility aliases through
`task-boundary-summary` for existing UI callers, but durable storage uses the
qualified boundary document.

Goal, task, and session memory are separate scopes. A user-facing persistent
goal is the contract: intent, success criteria, constraints, preferences,
budget, and resume policy. Xia-authored tasks are execution units under that
contract: they own stack/checkpoint/runtime state and `:task/constraints`, but
they do not override the goal contract. Sessions own transient scratch, recaps,
pending prompts, and recent turn context.

Context policy is resolved by `xia.constraints/operating-envelope`. It combines
the current goal contract, explicit `:task/constraints`, user profile
preferences, and low-precedence session scratch/recap context. The merge order
is:

```clojure
org policy > goal contract > task constraints > user preferences > session scratch/context
```

The resolver applies that order by merging lowest-to-highest precedence:
session context, user preferences, task constraints, goal contract, then org
policy. Org policy is read from `:constraints/org-policy` as an EDN map. Goal
budgets are exposed through the envelope and enforced against the LLM usage
ledger when a persistent goal id is present. Agent turns attach the resolved
envelope to the execution context so model routing, tools, and inspections can
use one consistent operating envelope.

## Task Specs

Task specs are Xia's durable executable plan format. They live under a normal
task contract:

```clojure
{:type :task
 :state :ready
 :contract {:kind :task
            :version 1
            :goal "Prepare report"
            :spec {:kind :task
                   :version 1
                   :goal "Prepare report"
                   :steps [...]}}
 :meta {:trigger {...}
        :execution {...}
        :task-spec {...runtime state...}}}
```

The authored spec and runtime state are intentionally separate:

- `:contract :spec` is the user/agent-authored plan.
- `:meta :task-spec` is runner-owned state: current step, step statuses,
  outputs, pause reason, resume token, deadlines, and timestamps.
- `:type` should remain `:task`; schedule, branch, board, and execution mode
  are metadata and projections rather than separate product task types.

The implementation is split between:

- `xia.task-spec.validate`: normalization and validation for the v1 spec
  grammar.
- `xia.task-spec`: authoring, repair, built-in executors, and the bounded
  runner.
- `xia.agent.task-runtime`: shared task persistence, events, pause/resume,
  fork/branch controls, and task boundary recording.
- `xia.task-inspection` and HTTP session handlers: UI-facing task-spec progress
  and history projections.

The runner dispatches each step by `:kind`. Built-in executors currently cover
`:value`, `:emit`, `:condition`, `:tool`, `:input`, `:approval`, `:llm`,
`:subtask`, `:branch`, `:parallel`, `:map`, and `:loop`. Executors can also be
registered globally or supplied per run. Unknown or unavailable executable work
pauses rather than escaping the data model.

Task specs are also the canonical execution path for scheduled work and branch
workers. Start/resume controls check whether a task has a spec and route it
through `xia.task-spec/run-task!` when possible. The full v1 contract is in
[task-spec-v1.md](task-spec-v1.md).

## Post-Task Skill Learning

Xia's self-learning loop is deliberately review-first. A completed task may
produce reusable prompt-skill proposals, but proposal generation is separate
from applying a skill mutation.

Durable proposal state is stored with `:skill.proposal/*` attributes. The
domain API lives in `xia.skill.proposal`:

- `create-proposal!`: store a pending `:create`, `:patch`, or `:archive`
  proposal.
- `generate-proposals-for-task!`: ask an LLM to reflect on a completed task and
  store pending proposals.
- `review-proposal-with-llm!`: ask an LLM reviewer to approve or reject one
  eligible proposal.
- `generate-and-review-proposals-for-task!`: run generation and then LLM-review
  all auto-reviewable proposals.
- `apply-proposal!` and `reject-proposal!`: the only mutation points for
  proposal status and skill changes.

LLM review has a narrower authority than human review:

- `:create` approvals save a disabled `:agent-authored` draft by default.
- `:patch` and `:archive` approvals are allowed only when the target skill is
  `:agent-authored`.
- User-authored, imported, system, missing-target, and high-risk proposals are
  left pending for human review.
- Patch proposals can carry the source skill content hash; apply refuses to
  mutate if the skill changed after proposal generation.
- Review prompts instruct the reviewer to reject secrets, credentials, raw
  private messages, private URLs, personal identifiers, and one-off task
  details.

Task finalization launches this loop through `xia.agent.task-finalization`.
The launcher runs after task completion is persisted, submits background work
through `xia.async/submit-background!`, and records status under
`:meta :skill-learning`. It skips child/branch-worker tasks and prevents
duplicate in-process launches for the same task. Failures are logged and stored
as failed learning metadata; they never rethrow into task completion.

The admin HTTP surface exposes proposal review:

| Route | Purpose |
|-------|---------|
| `GET /admin/skill-proposals` | list proposals |
| `POST /admin/skill-proposals` | create a proposal |
| `POST /admin/skill-proposals/:id/approve` | human/admin approval |
| `POST /admin/skill-proposals/:id/reject` | human/admin rejection |
| `POST /admin/skill-proposals/:id/llm-review` | bounded LLM review |

## Bridge And Session Runner

User-facing channels enter the autonomous runtime through `xia.bridge`.
Terminal, HTTP/WebSocket, command, Slack, Telegram, and iMessage surfaces should
use this facade for session creation, user-message dispatch, pending
prompt/approval replies, task controls, session controls, and channel adapter
registration.

The bridge is intentionally thin today: it preserves the existing
`xia.agent/process-message` runner and `xia.prompt` interaction bus while
removing direct channel dependencies on agent internals. This gives future IDE
or support-system bridges one stable contract for status, prompts, approvals,
interrupts, and runtime events instead of each channel inventing its own control
path.

## Web, Browser, And Service Automation

Xia can interact with the live web through secure, sandboxed tools.

- **Browser runtime:** Playwright only, with first-use browser install support and an explicit Linux system-deps setup path.
- **Resumable browser sessions:** backend-specific browser state and current URL persist in Xia's DB.
- **Stealth authenticated login:** stored credentials are injected by a proxy, not exposed to the LLM.
- **Interactive login:** for MFA or complex flows, Xia can prompt the user directly and avoid storage.
- **Secure fetch and search:** SSRF-protected web fetching, structured extraction, and search. Validation and the actual HTTP connection both use the same resolved addresses to avoid DNS TOCTOU gaps.

### Authenticated Services And OAuth

For API-based online work, Xia supports:

- static service auth: `:bearer`, `:basic`, `:api-key-header`, and `:query-param`
- OAuth 2 authorization-code + PKCE accounts with stored access tokens, refresh tokens, and automatic refresh
- built-in provider presets for GitHub, Google, and Microsoft
- a local `/oauth/callback` flow for browser-based account connection
- service records backed by either static secrets or linked OAuth accounts
- admin UI prefill from OAuth accounts into matching service entries

### Local Web UI

The local browser UI is intended to be the main interface for non-technical users.

- **Chat and scratch pads:** paste local material, keep per-session notes, and copy output without direct file access.
- **Admin panel:** configure LLM providers, OAuth accounts, services, site logins, local-document summarization settings, and the notification bridge foundation.
- **OAuth templates:** start from common provider presets and edit as needed.
- **OAuth-to-service handoff:** prefill service forms from saved OAuth accounts.
- **Calendar integrations:** manage Google Calendar, Microsoft Calendar, generic CalDAV collections, and read-only iCalendar feeds for events and availability.
- **Local documents:** upload text, PDF, and Office docs, then insert summaries or excerpts into chat and notes.
- **Skill import and curation:** install Xia skills directly, import a safe prompt-only subset of OpenClaw skills from directories, zip files, or ClawHub zip URLs, track provenance/hash/trust/lifecycle metadata, check imported sources for updates without applying them, and run a curator pass that archives only stale agent-authored skills.
- **Local trust boundary:** Xia binds to localhost by default and uses a local session secret cookie, while privileged actions still go through approval policy.

## Automation And Scheduling

Xia does not only answer ad hoc prompts.

- **Background scheduler:** interval-based or calendar-based scheduled runs.
- **Maintenance jobs:** memory consolidation, knowledge graph maintenance, and session cleanup.
- **Warm starts:** resume a conversation with working memory already populated from prior context.

## Security Model

Xia runs user-installed tools: arbitrary code that the LLM can invoke via function-calling. A compromised or malicious tool must not be able to read API keys, OAuth tokens, or other configured secrets.

**Prompt-injection resilience:** the security boundary is enforced at the code-execution level through the SCI sandbox rather than through natural-language instructions alone. Even if an attacker convinces the model to call a malicious tool, that tool still runs inside a restricted environment without direct access to protected credentials.

### SCI Sandbox

Tool handlers are strings of Clojure code executed inside [SCI](https://github.com/babashka/sci) (Small Clojure Interpreter). The sandbox explicitly allows only a minimal subset of functions:

| Namespace            | Functions                                              |
|----------------------|--------------------------------------------------------|
| `xia.memory`         | Knowledge graph and episodic memory read/write         |
| `xia.working-memory` | Current session context (`get`, `pin`, `unpin`)        |
| `xia.skill`          | Skill search, section extraction, patching, curation   |
| `xia.db`             | `get-config`, `set-config!`, `q` (all secret-filtered) |
| `xia.service`        | `request`, `list-services` (capability proxy)          |
| `xia.pipeline`       | Restricted pipeline runner for whitelisted tool calls  |

### Tool Permission Gate

Every normal tool invocation goes through `xia.permission/authorize-tool!`
before the handler or plugin hooks run. The permission layer owns channel
compatibility checks, vision-model gating, branch-worker restrictions, approval
policy, autonomous bypass rules, and session-scoped approval grants.

Approval prompts are still transported through `xia.prompt` channel adapters, so
terminal, HTTP, Slack, Telegram, and iMessage share the same permission decision
path. Bridge integrations can also provide a per-invocation
`:permission/approval-callback`; Xia records the same approval request,
approval decision, and policy-decision audit events for that callback path.

### Restricted Tool Pipelines

`pipeline-run` is a bundled tool for repetitive retrieval tasks where
intermediate tool results would waste model context. The tool evaluates a short
SCI/Clojure pipeline and returns only the pipeline's final structured value to
the model.

Pipeline code receives:

- `input`: optional structured input supplied with the tool call.
- `allowed-tools`: the current pipeline whitelist as strings.
- `call-tool`: a function shaped as `(call-tool :tool-id {"arg" value})`.

The pipeline runner is intentionally narrower than normal tool execution:

- **Whitelist only:** pipelines can call only read-oriented tools such as
  `web-search`, `web-fetch`, `web-extract`, `local-doc-search`,
  `local-doc-read`, `artifact-list`, `artifact-search`, `artifact-read`,
  `workspace-list`, `workspace-read`, `board-list`, and `recent-work`.
- **No recursive pipeline calls:** `pipeline-run` is not in the whitelist.
- **No intermediate transcript:** subtool results stay inside the pipeline; the
  model receives only the final structured return value.
- **Normal tool policy still applies:** allowed subtool calls route through
  Xia's standard tool execution path, including channel checks, approval policy,
  audit logging, and result normalization.
- **Extra sandbox limits:** pipeline SCI denies dynamic eval, namespace loading,
  file I/O, futures, and namespace introspection, and validates that the final
  value is structured data.

Runtime limits are configurable through Xia config:

| Key                              | Default |
|----------------------------------|---------|
| `:tool/pipeline-timeout-ms`      | 120000  |
| `:tool/pipeline-max-calls`       | 8       |
| `:tool/pipeline-max-code-chars`  | 12000   |

Example:

```clojure
(let [r (call-tool :web-search {"query" (:query input) "max_results" 5})]
  {:titles (mapv #(get % "title") (get r "results"))})
```

### Sandboxed Plugins And Hooks

Plugins are installed from EDN manifest maps and stored in the DB. A plugin must
declare every lifecycle capability it wants before any hook can run:

```clojure
{:id :tool-auditor
 :name "Tool Auditor"
 :enabled? false
 :capabilities #{:hook/pre-tool :hook/post-tool}
 :hooks [{:id :observe-tool
          :event :post-tool
          :handler "(fn [event] {:tool (:tool-id event) :status (:status event)})"}]}
```

Supported hook events are `:pre-tool`, `:post-tool`, `:post-llm`,
`:task-state-change`, and `:schedule-run`. New plugin installs default to
disabled when `:enabled?` is omitted, so importing a manifest and enabling it are
separate review steps.

Hook handlers run only through a restricted SCI context. They receive an `event`
map and may return structured data for audit summaries, but they do not get
direct Xia namespace access, file I/O, dynamic eval, futures, namespace loading,
or namespace introspection. Hook failures are isolated, logged, and audited
without stopping the main runtime, except that `:pre-tool` hooks may deliberately
block a tool by returning `{:allow? false :reason "..."}`.

Hook execution is bounded by `:plugin/hook-timeout-ms`, which defaults to 5000.
Hook manifests are also capped by `:plugin/hook-max-code-chars`,
`:plugin/max-hooks`, and `:plugin/max-active-workers`. The admin UI exposes
installed plugins with enable/disable controls, and every hook invocation writes
a `:plugin-hook` audit event when session audit context is available.

### Credential Protection (`xia.secret`)

The `xia.db` functions exposed to the sandbox are safe wrappers that enforce access control.

- **Protected attributes:** attributes such as `:llm.provider/api-key`, `:service/auth-key`, `:oauth.account/client-secret`, `:oauth.account/access-token`, and `:oauth.account/refresh-token` are blocked.
- **Datalog query filtering:** every query is analyzed before execution; if it references a secret attribute or pattern (password, token, and similar), uses indirect attribute scans, or uses computed `:where` clauses, it is rejected.

### Master Key Handling

- **Explicit key support:** `XIA_MASTER_KEY` and `XIA_MASTER_KEY_FILE` can provide a raw 32-byte base64 key for unattended deployments.
- **Passphrase mode:** `XIA_MASTER_PASSPHRASE` and `XIA_MASTER_PASSPHRASE_FILE` derive the master key with PBKDF2. Interactive CLI startup also prompts for a passphrase for new DBs.
- **File-backed secret policy:** `XIA_MASTER_KEY_FILE` and `XIA_MASTER_PASSPHRASE_FILE` are rejected if they point inside `<db-path>`, and on POSIX systems they are rejected if group or world permissions are present.
- **Portable archives:** when a packed archive contains `db/.xia/master.key` or `db/.xia/master.passphrase`, `xia backup.xia` can use them automatically while opening the archive directly. If you extract the DB manually, move those files out of `db/.xia/` before using env-file mode. Raw env-only keys remain external by design.

### File System Isolation

Xia is designed to be safe for the host system.

- **No local file access:** the SCI sandbox does not expose file system APIs such as `java.io` or `java.nio` to tool handlers.
- **Explicit ingestion only:** user-initiated uploads and imports are processed by Xia itself and then stored in its DB or skill store, but those paths do not grant tools ambient file access.
- **Restricted storage:** Xia only has read/write access to its own database file and support files, not arbitrary host paths.

### Capability Proxy (`xia.service`)

Tools call authenticated external APIs through a proxy. The tool passes a relative path, the proxy loads credentials from the DB, injects authentication, and makes the request. The tool receives the response but never sees the token.

- **Static auth path:** service records can inject bearer tokens, basic auth, API-key headers, or query-param credentials.
- **OAuth path:** service records can point at a stored OAuth account. `xia.service/request` ensures the account is connected, refreshes expiring tokens when needed, and then injects the resulting authorization header.
