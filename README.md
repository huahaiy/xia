# xia

Portable personal AI assistant. Single native binary + embedded DB = zero install friction. Everything is in the database: you can take the DB file to any computer, start Xia, and resume your exact experience. Works with any OpenAI-compatible LLM (Qwen, Ollama, OpenAI, Anthropic via proxy).

## Positioning & Philosophy

Xia is an **online-first** assistant. Its primary focus is the digital world beyond your local machine: web research, form automation, API orchestration, and maintaining a persistent knowledge graph of your digital life.

### Hardware & Accessibility
Unlike other autonomous AI assistants that might require a dedicated machine (like a Mac Mini) to avoid host interference, Xia is designed to live on your daily work computer. Because its tools are strictly sandboxed and isolated from your local file system, it can run alongside your normal tasks without any risk of side effects or host-system disruption. Any modern computer works.

### Database-Centric Portability
Xia is entirely self-contained within its database. Every memory, scheduled task, credential, and configuration is stored in a single database file. You can move this file to a different computer, start the Xia binary, and resume exactly where you left off with the same experience and full history.

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
4. **Relevance-Based Decay:** Entities stay in focus while they are relevant; they naturally decay and are evicted as the conversation shifts.
5. **Topic Tracking:** Automatically summarizes the current focus and detects major topic shifts to segment memories into focused units.

### Knowledge Graph (The "Neocortex")
The Knowledge Graph stores structured entities, relations, and atomic facts.
- **Structured Extraction:** Xia automatically extracts entities (person, place, concept) and their properties (location, role, preference) from conversations.
- **Smart Deduplication:** New information is merged with existing facts to prevent redundancy.
- **Confidence Maintenance:** Stale or unreinforced facts naturally lose confidence over time, preventing the graph from becoming cluttered with outdated information.

### Episodic Memory (The "Hippocampus")
Every interaction is recorded as an episode. A background "consolidation" process reviews these episodes to extract new knowledge and reinforce existing patterns, moving them from short-term recording to long-term structured memory.

---

## Web & Browser Automation

Xia can interact with the live web through a suite of secure, sandboxed tools.

- **Headless Browser:** A stateful, JavaScript-enabled browser (HtmlUnit) that runs within the SCI sandbox. Tools can open sessions, navigate, click elements, and fill forms.
- **Stealth Authenticated Login:** Xia can log into sites using stored credentials without the LLM ever seeing your passwords. Credentials are injected by a secure proxy at the system level.
- **Interactive Login:** For sites with MFA or complex auth, Xia can prompt you directly in the terminal to enter credentials that are used immediately and never stored.
- **Secure Fetch & Search:** Built-in tools for SSRF-protected web fetching, structured data extraction (CSS selectors), and anonymous web search (DuckDuckGo).

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
- **Protected attributes:** Attributes like `:llm.provider/api-key` and `:service/auth-key` are blocked.
- **Datalog query filtering:** Every query is analyzed before execution; if it references a secret attribute or pattern (password, token, etc.), it is rejected.

### File System Isolation
Xia is designed to be safe for the host system.
- **No Local File Access:** The SCI sandbox does not expose any file system APIs (`java.io`, `java.nio`, etc.) to tool handlers.
- **Restricted Storage:** Xia only has read/write access to its own database file. It cannot read, modify, or delete any other files on the host system, ensuring there is no risk of accidental or malicious damage to the host environment.

### Capability Proxy (`xia.service`)
Tools call authenticated external APIs (Gmail, GitHub, etc.) through a proxy. The tool passes a relative path; the proxy loads the credentials from the DB, injects the authentication headers, and makes the call. The tool receives the response but never sees the token.

---

### Key Source Files

| File | Role |
|------|------|
| `src/xia/working_memory.clj` | Working memory lifecycle and retrieval pipeline |
| `src/xia/context.clj` | Token-budgeted prompt assembly and history compaction |
| `src/xia/hippocampus.clj` | Knowledge extraction and graph consolidation |
| `src/xia/browser.clj` | Headless browser management and stealth login |
| `src/xia/scheduler.clj` | Background task execution engine |
| `src/xia/secret.clj` | Secret registry and safe DB wrappers for SCI |
| `src/xia/service.clj` | Capability proxy for authenticated HTTP |
| `src/xia/sci_env.clj` | SCI sandbox configuration and function allowlisting |
