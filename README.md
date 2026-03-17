# Xia

![Xia logo](xia-logo.png)

> There is an old Chinese story about the Snail Maiden, a gentle spirit who appears
> each day to care for a home before anyone returns. Xia is a modern echo of
> that tale: an autonomous AI assistant that quietly tends to the details of your digital life.

Xia (/çja/, pronounced 'shyah') is a portable personal AI assistant for online
work. It runs as a single local application, remembers context across sessions,
works with any OpenAI-compatible LLM models, and helps with web research,
browser automation, authenticated APIs, and recurring tasks.

## What Xia Is For

Xia is built for the parts of your digital life that live beyond your local file
system:

- researching the web and extracting structured information
- signing into websites and using saved site logins
- calling authenticated APIs through stored service connections
- keeping long-term memory about people, projects, and ongoing work
- running recurring background tasks on a schedule

Xia is not a local computer-control agent. It does not edit arbitrary files on
your machine or drive your terminal like a coding assistant.

## Quick Start

Start Xia normally:

```bash
xia
```

Create a portable archive:

```bash
xia pack backup.xia
```

Open that archive later on this or another machine:

```bash
xia backup.xia
```

What to expect:

- Xia stores its state in its
  [database](https://github.com/datalevin/datalevin), so conversations, memory,
  settings, and saved connections travel together.
- The local web UI is intended to be the main interface for non-technical users.
- Semantic memory recall uses a local embedding model. On first use, Xia will
download the default
[`nomic-embed-text-v2-moe`](https://huggingface.co/nomic-ai/nomic-embed-text-v2-moe)
GGUF model, which is about 512 MiB.
- You still provide your own LLM provider credentials, and you can use multiple
  LLMs at the same time.
- None of your credentials or secrets is exposed to LLM providers. Security is
  one of Xia's main value proposition.

## Local Web UI

Xia starts in terminal mode by default. To use the local web UI, start the HTTP
server:

```bash
xia --mode server
```

Or run both interfaces together:

```bash
xia --mode both
```

By default, the web UI listens on `http://localhost:3008/`.

What the local web UI is for:

- chat with Xia from a browser instead of the terminal
- upload local text-like documents and PDFs, then insert full text or a selected excerpt into chat or notes
- use scratch pads for copied notes and working context
- configure LLM providers, OAuth accounts, services, and saved site logins
- manage scheduled tasks and other local assistant settings

The server binds to `127.0.0.1` by default. Use `--bind 0.0.0.0` only when you
intentionally want to expose it beyond the local machine.

## Typical Uses

- Keep an assistant that remembers your projects, contacts, preferences, and
  prior conversations.
- Let Xia research websites, follow links, fill forms, and return structured
  results through a real Playwright browser when available, with HtmlUnit
  fallback for lighter environments. On first use, Xia can detect missing
  Playwright browser binaries and install them automatically. On Linux, Xia
  can also preview or run Playwright's system dependency setup as a separate
  explicit step.
- Connect services such as GitHub or Google through static credentials or OAuth,
  then let Xia use them without exposing secrets to tools.
- Schedule recurring work like checks, summaries, monitoring, and maintenance.

## Privacy And Safety

Xia is built to be useful on a daily machine without getting broad access to the
host machine:

- secrets are encrypted at rest in the database
- tools run inside a restricted sandbox
- authenticated API calls go through a capability proxy instead of exposing raw
  credentials
- the local web UI uses a local session secret and local-origin checks
- Xia is designed around online work, not arbitrary local file access

## More Documentation

- Technical and implementation details: [doc/impl.md](doc/impl.md)
