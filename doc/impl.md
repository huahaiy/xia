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

Typical portable workflow:

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
- **Calendar integrations:** manage Google Calendar and Microsoft Calendar events and availability through OAuth-backed services.
- **Local document workflows:** upload text, PDF, and Office docs, then insert summaries or excerpts into chat and notes.
- **Skill import:** install Xia skills directly and import a safe prompt-only subset of OpenClaw skills from directories, zip files, or ClawHub zip URLs.
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
| `xia.skill`          | Skill search, section extraction, patching             |
| `xia.db`             | `get-config`, `set-config!`, `q` (all secret-filtered) |
| `xia.service`        | `request`, `list-services` (capability proxy)          |

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
