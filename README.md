# xia

![Xia logo](xia-logo.png)

> "There is an old Chinese story about the 田螺姑娘, a gentle spirit who appears each day to care for a home before anyone returns. Xia (虾) is a modern echo of that tale—an autonomous AI assistant that quietly tends to the details of your digital life."

Portable personal AI assistant. Single native binary + embedded DB = zero install friction. Everything is in the database: you can take the DB file to any computer, start Xia, and resume your exact experience. Works with any OpenAI-compatible LLM (Qwen, Ollama, OpenAI, Anthropic via proxy).

## Positioning & Philosophy

Xia is an **online-first** assistant. Its primary focus is the digital world beyond your local machine: web research, form automation, API orchestration, and maintaining a persistent knowledge graph of your digital life.

### Hardware & Accessibility
Unlike other autonomous AI assistants that might require a dedicated machine (like a Mac Mini) to avoid host interference, Xia is designed to live on your daily work computer. Because its tools are strictly sandboxed and isolated from your local file system, it can run alongside your normal tasks without any risk of side effects or host-system disruption. Any modern computer works.

### Database-Centric Portability
Xia keeps its state in its database, but encrypted secrets are protected by external key material. In passphrase mode that is your startup passphrase plus a non-secret salt file stored under `<db-path>/.xia/master.salt`. File-backed master keys and passphrases must live outside `<db-path>` and, on POSIX systems, should use owner-only permissions such as `0600` or `0400`. `xia pack` can bundle local support files into a portable archive, and `xia backup.xia` opens that archive directly for non-technical users.

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

### Xia vs. Local Assistants
Unlike tools like `Claude Code` or `Codex`, Xia is not designed for local file system manipulation or host-level computer automation.
- **Xia:** Focused on the online digital environment, secure credential management, and long-term memory.
- **Local Tools:** Better suited for editing local source code, managing files, and executing terminal commands on the host.

### Interoperability
While Xia does not touch your local files directly, it can orchestrate local tools that expose an API. If you have a local service or a coding assistant with an accessible endpoint, Xia can interact with it through its standard service capability proxy, allowing it to bridge the gap between your online research and your local workspace without compromising host security.

## Core Capabilities

Xia is designed to be a long-lived, autonomous partner that learns from every interaction.

- **Human-Inspired Memory:** A multi-layered memory system that separates active focus (Working Memory) from long-term facts (Knowledge Graph) and past experiences (Episodic Memory).
- **Secure Web & Browser Automation:** A built-in headless browser and SSRF-protected web tools allow Xia to research, fill forms, and interact with the web safely within a sandboxed environment.
- **Authenticated Online Work:** Xia supports stored API credentials, website logins, and first-class OAuth accounts, so it can operate against real user services instead of only anonymous web pages.
- **Autonomous Task Scheduling:** A background engine for recurring tasks, from automated web monitoring to periodic memory consolidation and maintenance.
- **Privacy-First Security:** Rigorous credential isolation ensures that untrusted tools can never see your API keys, even while performing authenticated actions on your behalf.

---

## Memory Architecture

Xia's memory is modeled after human cognition, allowing it to maintain context over months of interaction without being overwhelmed by irrelevant data.

### Working Memory (The "Prefrontal Cortex")
Working memory holds the active, curated context for the current conversation. It is updated every turn through a high-performance retrieval pipeline:
1. **Keyword Extraction:** Identifies core concepts in your messages (zero LLM cost).
2. **Hybrid Retrieval:** Performs parallel full-text search across the Knowledge Graph and past episodes.
3. **Spreading Activation:** Expands the search by one hop in the graph to activate related concepts (e.g., thinking about "Clojure" activates "Datalevin").
4. **Relevance-Based Decay:** Unrefreshed working-memory entities lose relevance multiplicatively each turn and are evicted once they fall below threshold.
5. **Topic Tracking:** Automatically summarizes the current focus and detects major topic shifts to segment memories into focused units.

### Knowledge Graph (The "Neocortex")
The Knowledge Graph stores structured entities, relations, and atomic facts.
- **Structured Extraction:** Xia automatically extracts entities (person, place, concept) and their properties (location, role, preference) from conversations.
- **Smart Deduplication:** New information is merged with existing facts to prevent redundancy.
- **Confidence Maintenance:** Stale or unreinforced facts decay with a time-based half-life after a grace period, preventing the graph from becoming cluttered with outdated information.

### Episodic Memory (The "Hippocampus")
Every interaction is recorded as an episode. A background "consolidation" process reviews these episodes to extract new knowledge and reinforce existing patterns, moving them from short-term recording to long-term structured memory.

---

## Web & Browser Automation

Xia can interact with the live web through a suite of secure, sandboxed tools.

- **Headless Browser:** A stateful, JavaScript-enabled browser (HtmlUnit) that runs within the SCI sandbox. Tools can open sessions, navigate, read pages, wait for JS-heavy pages to settle, click elements, and fill forms.
- **Resumable Browser Sessions:** Browser sessions persist cookies and current URL into Xia's DB, so multi-step authenticated browsing can resume later instead of starting from scratch.
- **Stealth Authenticated Login:** Xia can log into sites using stored credentials without the LLM ever seeing your passwords. Credentials are injected by a secure proxy at the system level.
- **Interactive Login:** For sites with MFA or complex auth, Xia can prompt you directly in the terminal to enter credentials that are used immediately and never stored.
- **Secure Fetch & Search:** Built-in tools for SSRF-protected web fetching, structured data extraction (CSS selectors), and anonymous web search (DuckDuckGo).

### Authenticated Services & OAuth

For API-based online work, Xia supports multiple authentication models:

- **Static service auth:** `:bearer`, `:basic`, `:api-key-header`, and `:query-param` service registrations.
- **OAuth accounts:** First-class OAuth 2 authorization-code + PKCE accounts with stored access tokens, refresh tokens, expiry tracking, and automatic refresh.
- **Built-in provider presets:** The web admin UI can prefill OAuth account settings for GitHub, Google, and Microsoft, while still leaving every field editable.
- **Local callback flow:** Xia completes OAuth flows through its own local `/oauth/callback`, so a non-technical user can connect an account from the browser UI instead of manually extracting tokens.
- **Service linkage:** A service can use either a static secret or a linked OAuth account. Tools still call `xia.service/request`; the auth mechanism stays behind the proxy boundary.
- **Service prefill:** After connecting an OAuth account from a built-in preset, the Admin UI can prefill a matching service entry with the right API base URL and link it back to that account.

### Local Web UI

The local browser UI is intended to be the main interface for non-technical users:

- **Chat + scratch pads:** Paste local material, keep per-session scratch notes, and copy transcript output without giving Xia direct file access.
- **Admin panel:** Configure LLM providers, OAuth accounts, service registrations, and site logins from the browser.
- **OAuth templates:** Start from common provider presets, then enter your own client id and secret instead of assembling authorize/token endpoints manually.
- **OAuth-to-service handoff:** From a saved OAuth account, Xia can prefill the matching service form so the user only needs to review and save it.
- **No repeated login prompts:** Xia binds to localhost by default and uses a local session secret cookie for the UI, while privileged actions still go through approval policy.

---

## Automation & Scheduling

Xia doesn't just wait for you to speak; it can perform tasks in the background.

- **Background Scheduler:** Schedule tool executions or prompt-based agent runs using interval-based (e.g., "every 30 minutes") or calendar-based (cron-like) specs.
- **Maintenance Jobs:** Xia performs self-maintenance in the background, including memory consolidation, knowledge graph maintenance, and session cleanup.
- **Session Continuity:** "Warm starts" allow Xia to resume a conversation with the working memory pre-populated from your last interaction.

---

## Security Model

Xia runs user-installed **tools** — arbitrary code that the LLM can invoke via function-calling. A compromised or malicious tool must not be able to read API keys, OAuth tokens, or any other credential the user has configured.

**Resilience to Prompt Injection:** Because the security boundary is enforced at the code execution level (the SCI sandbox) rather than through natural language instructions, Xia is fundamentally resilient to prompt injection attacks aimed at credential theft. Even if an attacker successfully injects a prompt that tricks the LLM into calling a tool with malicious intent, that tool remains trapped within the sandbox. It cannot access protected database attributes, read API keys, or exfiltrate credentials, as the underlying execution environment simply does not provide the "plumbing" for such actions.

### SCI Sandbox
Tool handlers are strings of Clojure code executed inside [SCI](https://github.com/babashka/sci) (Small Clojure Interpreter). The sandbox explicitly allows only a minimal, safe subset of functions:

| Namespace            | Functions                                                     |
|----------------------|---------------------------------------------------------------|
| `xia.memory`         | Knowledge graph and episodic memory read/write                |
| `xia.working-memory` | Current session context (get, pin, unpin)                     |
| `xia.skill`          | Skill search, section extraction, patching                    |
| `xia.db`             | `get-config`, `set-config!`, `q` (all secret-filtered)        |
| `xia.service`        | `request`, `list-services` (capability proxy)                 |

### Credential Protection (`xia.secret`)
`xia.db` functions exposed to the sandbox are safe wrappers that enforce access control:
- **Protected attributes:** Attributes like `:llm.provider/api-key`, `:service/auth-key`, `:oauth.account/client-secret`, `:oauth.account/access-token`, and `:oauth.account/refresh-token` are blocked.
- **Datalog query filtering:** Every query is analyzed before execution; if it references a secret attribute or pattern (password, token, etc.), uses indirect attribute scans, or uses computed `:where` clauses, it is rejected.

### Master Key Handling
- **Explicit key support:** `XIA_MASTER_KEY` and `XIA_MASTER_KEY_FILE` can provide a raw 32-byte base64 key for unattended deployments.
- **Passphrase mode:** `XIA_MASTER_PASSPHRASE` and `XIA_MASTER_PASSPHRASE_FILE` derive the master key with PBKDF2. Interactive CLI startup also prompts for a passphrase for new DBs.
- **File-backed secret policy:** `XIA_MASTER_KEY_FILE` and `XIA_MASTER_PASSPHRASE_FILE` are rejected if they point inside `<db-path>`, and on POSIX systems they are rejected if group/world permissions are present.
- **Portable archives:** When a packed archive contains `db/.xia/master.key` or `db/.xia/master.passphrase`, `xia backup.xia` can use them automatically while opening the archive directly. If you extract the DB manually, move those files out of `db/.xia/` before using env-file mode. Raw env-only keys remain external by design.

### File System Isolation
Xia is designed to be safe for the host system.
- **No Local File Access:** The SCI sandbox does not expose any file system APIs (`java.io`, `java.nio`, etc.) to tool handlers.
- **Restricted Storage:** Xia only has read/write access to its own database file. It cannot read, modify, or delete any other files on the host system, ensuring there is no risk of accidental or malicious damage to the host environment.

### Capability Proxy (`xia.service`)
Tools call authenticated external APIs (Gmail, GitHub, etc.) through a proxy. The tool passes a relative path; the proxy loads the credentials from the DB, injects the authentication headers, and makes the call. The tool receives the response but never sees the token.

- **Static auth path:** service records can inject bearer tokens, basic auth, API-key headers, or query-param credentials.
- **OAuth path:** service records can point at a stored OAuth account. `xia.service/request` ensures the account is connected, refreshes expiring tokens when needed, and then injects the resulting `Authorization` header.

---
