# Xia

<img src="resources/web/favicon/favicon.svg" alt="Xia logo" width="220">

> There is an old Chinese story about the Snail Maiden (田螺姑娘), a gentle
> spirit who appears each day to care for a home before anyone returns. Xia
> is a modern echo of that tale: an autonomous AI assistant that quietly
> tends to the details of your digital life.

Xia (/ɕja/, pronounced "shyah") is a secure and portable personal AI assistant
for online work. It runs as a single local application, remembers context across
sessions, works with any LLM models, and helps with web research, browser
automation, authenticated APIs, and recurring online tasks.

## What Xia Is For

Xia is built for the parts of your digital life that live beyond your local file
system:

- keeping long-term memory about people, projects, and ongoing work
- researching the web and extracting structured information
- signing into websites and using saved site logins to do work
- calling authenticated APIs through stored service connections
- running recurring background tasks on a schedule

Xia is not a local computer-control agent. It does not access arbitrary files on
your machine or drive your terminal like a coding assistant. In fact, except its
own DB/support directories and a shared workspace with other Xia instances, Xia
does not access your local file system at all. User-initiated file uploads and
imports are supported, but they stay explicit and scoped.

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

What to expect:

- You still provide your own LLM provider credentials, and you can use multiple
  LLMs at the same time and assign different LLM to different workloads.
- Xia stores its state in its
  [database](https://github.com/datalevin/datalevin), so conversations, memory,
  settings, and saved connections travel together.
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
- configure LLM providers, OAuth accounts, services, saved site logins, local
  document summarization settings, and so on
- author or import safe prompt-only OpenClaw skills from local bundles or
  ClawHub zip URLs
- manage scheduled tasks and other local assistant settings

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
- Schedule recurring work like checks, summaries, monitoring, and maintenance.

## Privacy And Safety

Xia is built to be useful on a daily machine without getting broad access to the
host machine, so you do not have to provision a dedicated machine for Xia:

- secrets are encrypted at rest in the database
- tools run inside a restricted sandbox
- tools do not have ambient access to your host file system
- authenticated API calls go through a capability proxy instead of exposing raw
  credentials
- the local web UI uses a local session secret and local-origin checks

## More Documentation

- Technical and implementation details: [doc/impl.md](doc/impl.md)
- Multi-instance setup and template seeding: [doc/multi-instance.md](doc/multi-instance.md)
