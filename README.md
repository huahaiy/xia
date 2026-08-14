# Xia

<img src="resources/web/favicon/favicon.svg" alt="Xia logo" width="220">

> There is an old Chinese story about the Snail Maiden (田螺姑娘), a gentle
> spirit who appears each day to care for a home before anyone returns. Xia
> is a modern echo of that tale: an autonomous AI assistant that quietly
> tends to the details of your digital life.

Xia (/ɕja/, pronounced "shyah") is a secure Portable Persistent AI Assistant
(P2A2) for online work. It runs as a single local application, remembers context
across sessions, works with any LLM models, and helps with web research, browser
automation, authenticated APIs, and recurring online tasks.

## What Xia Is For

Xia is built for the parts of your digital life that live beyond your local file
system:

- keeping long-term memory about people, projects, and ongoing work
- researching the web and extracting structured information
- signing into websites and using saved site logins to do work
- calling authenticated APIs through stored service connections
- turning larger goals into durable, inspectable task plans that can pause,
  resume, branch, and run scheduled work
- running recurring background tasks on a schedule

Xia is not a local computer-control agent. It does not access arbitrary files on
your machine or drive your terminal like a coding assistant. In fact, except its
own DB/support directories and a shared workspace with other Xia instances, Xia
does not access your local file system at all. User-initiated file uploads and
imports are supported, but they stay explicit and scoped.

## Install

Install the latest Xia release from GitHub Releases:

macOS / Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/huahaiy/xia/main/script/install.sh | sh
```

Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/huahaiy/xia/main/script/install.ps1 | iex
```

Defaults:

- macOS / Linux installs to `~/.local/bin/xia`
- Windows installs to `%LOCALAPPDATA%\Programs\Xia\bin\xia.exe`
- each installer downloads the matching native release zip from GitHub Releases
- installers require and verify the release archive's SHA-256 sidecar before
  extracting or installing anything
- every release ZIP includes `SBOM.cdx.json`, and the same target-specific
  CycloneDX 1.6 SBOM is published as a separate release asset
- every ZIP, checksum, and standalone SBOM has signed GitHub build provenance;
  the matching Sigstore bundle is also published as a release asset

To pin a specific version on macOS / Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/huahaiy/xia/main/script/install.sh | sh -s -- --version v0.1.0
```

To verify that a downloaded release archive was produced by Xia's release
workflow, install the GitHub CLI and run:

```bash
gh attestation verify xia-v0.1.0-macos-arm64.zip \
  --repo huahaiy/xia \
  --signer-workflow huahaiy/xia/.github/workflows/release.binaries.yml
```

This verifies the Sigstore signature, artifact digest, source repository, and
signing workflow. The `.provenance.sigstore.json` release asset preserves the
same verification bundle for auditing and offline-verification workflows.

## Quick Start

Start Xia normally on a terminal:

```bash
xia
```
You will be asked to create a master passphrase. After that, you can go to Web
UI at `http://localhost:3008/` on your browser. There you will select and
configure the initial default LLM that powers Xia.

Create a portable archive:

```bash
xia pack backup.xia
```

Open that archive later on this or another machine:

```bash
xia backup.xia
```

Create a local safety snapshot before risky work:

```bash
xia snapshot create before-risky-work
```

Restore one later, with Xia stopped:

```bash
xia snapshot list
xia snapshot restore SNAPSHOT_ID --force
```

What to expect:

- You still provide your own LLM provider credentials, and you can use multiple
  LLMs at the same time and assign different LLM to different workloads.
- Xia treats LLM usage limits as a first-class layer, so turns, tasks, and
  scheduled runs can share budget accounting instead of relying on per-prompt
  model choices. The limits layer keeps a sanitized usage ledger for policy
  ceilings and cost accounting.
- Xia resolves a turn-level operating envelope from org policy, workspace
  constraints, task constraints, user preferences, and session scratch context,
  so preferences and guardrails live outside prompt prose.
- Xia represents durable work as task specs: explicit, inspectable plans with
  steps, inputs, outputs, approvals, retries, pauses, and runtime history. The
  task board and history views are projections over those durable tasks.
- After completed tasks, Xia can look for reusable process knowledge and create
  skill-improvement proposals. Safe agent-authored proposals may be reviewed by
  an LLM, while user-owned, imported, system, or high-risk changes stay pending
  for human review.
- Xia stores its state in its
  [database](https://github.com/datalevin/datalevin), so conversations, memory,
  settings, and saved connections travel together.
- Safety snapshots store Xia's database plus the shared workspace by default,
  and restore moves existing directories aside instead of deleting them.
- The [local web UI](http://localhost:3008) is intended to be the main
  interface for users.
- Browser automation uses [Playwright](https://playwright.dev/). On first use,
  Xia may install Playwright browser binaries.
- Semantic memory recall uses a local embedding model by default to save tokens.
  On first use, Xia will download the default
  [`nomic-embed-text-v2-moe`](https://huggingface.co/nomic-ai/nomic-embed-text-v2-moe)
  model, which is about 512 MiB.
- Local-document summarization defaults to heuristic extractive summaries. If
  you opt into model-generated summaries, Xia can use either a local model or an
  external provider.
- None of your credentials or secrets is exposed to LLM providers. Prompt
  injection cannot reveal secrets due to the lack of access. Security is
  one of Xia's main value propositions.

## Local Web UI

What the local web UI is for:

- work with Xia from a browser
- upload local text, PDF, and Office documents; Xia extracts text, chunks large
  docs along natural boundaries, and keeps summary plus chunk-level recall
- use scratch pads for copied notes and working context
- download Xia produced artifacts
- view the current task and board-backed task lanes
- inspect task plans, step progress, pauses, approvals, and task-history events
- resume paused or scheduled work from explicit task boundary summaries and
  next-step hints
- configure LLM providers, OAuth accounts, services, saved site logins, local
  document summarization settings, and so on
- author, import, update-check, and curate safe prompt-only skills from local
  bundles or ClawHub zip URLs
- review reusable skill proposals generated after completed tasks
- manage scheduled tasks and other local assistant settings
- expose an allowlisted MCP-compatible tool facade and durable coordination
  board tools for other trusted local agents through the command channel
- run restricted tool pipelines that collapse repetitive retrieval workflows
  into one final structured result
- enable or disable sandboxed plugins whose EDN manifests declare explicit
  lifecycle-hook capabilities

The server binds to `127.0.0.1` by default. Use `--bind 0.0.0.0` only when you
intentionally want to expose it beyond the local machine.

## Typical Uses

- Keep an assistant that remembers your projects, contacts, preferences, and
  prior conversations.
- Let Xia research websites, follow links, fill forms, and return structured
  results through browser automation.
- Connect services such as Google or Github through static credentials or OAuth,
  then let Xia use them without exposing secrets to tools.
- Upload PDFs, DOCX/XLSX/PPTX files, Markdown, and other text-like documents so
  Xia can find them using hybrid search (fulltext + semantic).
- Import safe OpenClaw-compatible prompt skills from ClawHub zip URLs or local
  bundles when they fit Xia's security model.
- Let Xia turn recurring or multi-step work into task specs whose progress and
  outputs remain visible and resumable.
- Schedule recurring work like checks, summaries, monitoring, and maintenance.

## Privacy And Safety

Xia is built to be useful on a daily machine without getting broad access to the
host machine, so you do not have to provision a dedicated machine for Xia:

- stored credentials and secret configuration values are encrypted at rest;
  conversations, tool payloads, audit data, schedule outputs, and other user
  content are not field-encrypted and rely on host/database access controls
- tools run inside a restricted sandbox
- every tool invocation passes through a central permission gate before handler
  code runs; terminal, HTTP, Slack, Telegram, and iMessage approvals route
  through channel adapters rather than per-tool UI code
- terminal, web, command/WebSocket, and messaging channels enter the core
  runtime through a shared bridge for prompts, approvals, status, interrupts,
  and task/session controls
- repetitive retrieval pipelines can call only whitelisted Xia tools and return
  only the final structured output
- plugin hook handlers run in restricted SCI, install disabled by default, and
  must declare explicit `:hook/...` capabilities before they can observe
  lifecycle events
- self-learning writes reviewable skill proposals first; user-authored,
  imported, system, and high-risk skills are not automatically changed
- tools do not have ambient access to your host file system
- authenticated API calls go through a capability proxy instead of exposing raw
  credentials
- the local web UI uses a local session secret and local-origin checks

## More Documentation

- Technical and implementation details: [doc/impl.md](doc/impl.md)
- Stored-data classification and retention: [doc/data-classification.md](doc/data-classification.md)
- Task spec details: [doc/task-spec-v1.md](doc/task-spec-v1.md)
- Multi-instance setup and template seeding: [doc/multi-instance.md](doc/multi-instance.md)
- MCP facade and coordination board: [doc/mcp-and-board.md](doc/mcp-and-board.md)
