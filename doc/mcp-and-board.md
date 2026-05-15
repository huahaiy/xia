# MCP And Coordination Board

This document tracks the first implementation slice borrowed from Hermes Agent:
an MCP-compatible command-channel facade and a narrow durable coordination
board.

## MCP Facade

Xia exposes a minimal JSON-RPC endpoint at:

```text
POST /command/mcp
```

It uses the existing command-channel authentication:

```text
Authorization: Bearer <XIA_COMMAND_TOKEN>
```

Supported methods:

- `initialize`
- `ping`
- `notifications/initialized`
- `tools/list`
- `tools/call`

The facade advertises only tools in `:mcp/tool-allowlist`. If that config key
is unset, Xia uses a conservative built-in allowlist of web, artifact,
local-document, workspace, schedule-list, peer-list, and board tools.

Tool calls still go through Xia's normal tool execution path, including approval
policy, channel checks, SCI sandboxing, and audit logging.

Example request:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/list"
}
```

## Coordination Board

The initial board implementation reuses Xia's durable task table rather than
adding a new DB schema version. Board cards are tasks with:

- `:task/type` = `:board-card`
- `:task/channel` = `:board`
- `:task/state` = `:open`, `:claimed`, `:blocked`, `:completed`, or
  `:cancelled`
- board-specific fields under `:task/meta {:board ...}`

The local web UI renders these same task records in the Task tab as board lanes.
The board is therefore a representation of tasks, not a parallel task store.

Bundled board tools:

- `board-create`
- `board-list`
- `board-claim`
- `board-update`
- `board-comment`
- `board-heartbeat`

Claimed cards receive a claim token. Updates and heartbeats on claimed cards
must provide the current token, which gives worker handoff a simple optimistic
ownership check without introducing a separate lock table.
