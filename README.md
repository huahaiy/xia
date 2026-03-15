# Xia

![Xia logo](xia-logo.png)

> There is an old Chinese story about a snail maiden, a gentle spirit who appears
> each day to care for a home before anyone returns. Xia is a modern echo of
> that tale: an autonomous AI assistant that quietly tends to the details of your digital life.

Xia is a portable personal AI assistant for online work. It runs as a single local app, remembers context across sessions, works with any OpenAI-compatible model, and helps with web research, browser automation, authenticated APIs, and recurring tasks.

## What Xia Is For

Xia is built for the parts of your digital life that live beyond your local file system:

- researching the web and extracting structured information
- signing into websites and using saved site logins
- calling authenticated APIs through stored service connections
- keeping long-term memory about people, projects, and ongoing work
- running recurring background tasks on a schedule

Xia is not a local computer-control agent. It does not edit arbitrary files on your machine or drive your terminal like a coding assistant.

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

- Xia stores its state in its database, so conversations, memory, settings, and saved connections travel together.
- The local web UI is intended to be the main interface for non-technical users.
- You still provide your own LLM provider credentials and, if enabled, your own master key or passphrase source.

## Typical Uses

- Keep an assistant that remembers your projects, contacts, preferences, and prior conversations.
- Let Xia research websites, follow links, fill forms, and return structured results.
- Connect services such as GitHub or Google through static credentials or OAuth, then let Xia use them without exposing secrets to tools.
- Schedule recurring work like checks, summaries, monitoring, and maintenance.

## Portability

Xia is designed to move with you. A normal workflow looks like this:

1. Use `xia` on your machine as usual.
2. Pack the database into a `.xia` archive with `xia pack backup.xia`.
3. Open that archive directly later with `xia backup.xia`.

You do not need to unzip `.xia` archives manually. Xia opens them directly and writes changes back on normal exit.

## Privacy And Safety

Xia is built to be useful on a daily machine without getting broad access to the host:

- secrets are encrypted at rest in the database
- tools run inside a restricted sandbox
- authenticated API calls go through a capability proxy instead of exposing raw credentials
- the local web UI uses a local session secret and local-origin checks
- Xia is designed around online work, not arbitrary local file access

## More Documentation

- Technical and implementation details: [doc/impl.md](doc/impl.md)
- The implementation notes cover memory architecture, browser and service automation internals, the scheduler, sandbox boundaries, and key-management details.
