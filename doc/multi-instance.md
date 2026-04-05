# Multi-Instance Xia

Xia can run multiple instances on the same machine, each with its own identity,
database, support files, and HTTP port.

This is useful when you want separate assistants for different roles, such as:

- a personal assistant
- an ops or release assistant
- a research or monitoring assistant

## Instance IDs And Storage

Each instance has an instance id. The id becomes part of Xia's default on-disk
storage layout:

```text
~/.xia/instances/<instance-id>/db
```

Support files for that DB live under:

```text
<db-path>/.xia
```

That support directory holds instance-local material such as the passphrase salt
file and other runtime support files.

Managed model assets are machine-shared by default:

```text
~/.xia/models/embed
~/.xia/models/llm
~/.xia/models/ocr
```

This avoids repeated downloads when multiple Xia instances on the same machine
use the same managed embedding, local LLM, or OCR models.

Shared collaboration workspaces are also machine-shared by default:

```text
~/.xia/workspaces/<workspace-id>
```

The default shared workspace id is `default`. You can move the shared workspace
root with `XIA_WORKSPACE_ROOT`.

The DB support directory under `<db-path>/.xia` is still instance-local.

### Starting A Specific Instance

Use `--instance`:

```bash
xia --instance personal
xia --instance ops
xia --instance research
```

Or use `XIA_INSTANCE`:

```bash
XIA_INSTANCE=ops xia --mode server
```

If you do not pass `--db`, Xia uses the instance-scoped default path shown
above. If you do pass `--db`, that explicit DB path wins.

## Identity Per Instance

Each instance has its own identity settings and prompt shape, including:

- name
- role
- description
- personality
- guidelines

This means different instances can behave like distinct assistants even when
they run on the same machine.

## Running Multiple Web UIs

By default, Xia starts its HTTP/WebSocket server on port `3008`.

If that port is already taken, Xia now tries the next port number until it finds
an available one. The startup message prints the actual bound URL. Treat that
printed URL as the source of truth.

Example:

```bash
xia --instance personal --mode server
xia --instance ops --mode server
xia --instance research --mode server
```

The first instance may bind to `3008`, the next to `3009`, and the next to
`3010`, depending on what is already running.

## Command Channel Between Instances

The local web UI is not the only HTTP surface anymore. If you want one Xia
instance, script, or other local agent to drive another Xia instance, enable
the command channel with a bearer token:

```bash
XIA_COMMAND_TOKEN=shared-local-secret xia --instance ops --mode server
```

When enabled, Xia exposes token-authenticated machine routes on the same HTTP
port:

- `POST /command/sessions`
- `POST /command/chat`
- `DELETE /command/sessions/<session-id>`
- `GET /command/sessions/<session-id>/status`
- `GET /command/sessions/<session-id>/approval`
- `POST /command/sessions/<session-id>/approval`
- `GET /command/sessions/<session-id>/messages`

These routes use:

```text
Authorization: Bearer <token>
```

Unlike the browser UI routes, they do not require the local UI session cookie
or browser-origin headers.

Example:

```bash
curl -sS \
  -H 'Authorization: Bearer shared-local-secret' \
  -H 'Content-Type: application/json' \
  -d '{"message":"Summarize the last deployment and open incidents."}' \
  http://127.0.0.1:3009/command/chat
```

Command sessions are tracked separately from browser UI sessions. They support
the same status polling and approval flow, but they are not listed in the human
chat history panel.

Treat the bearer token like any other local secret. If you want to persist it
inside the instance database instead of passing `XIA_COMMAND_TOKEN` at startup,
store it under the config key `:secret/command-channel-token`.

## Letting Xia Start Another Xia

One Xia can now start and stop managed child Xia instances on the same host,
and top-level Xia processes enable that host capability by default.

By default:

- the default Xia instance starts in `Controller mode`
- other Xia instances start with `Controller mode` off until the user enables it
- managed child Xia instances always start with host-level instance management
  disabled, so the capability is not inherited automatically

If you ever need to disable host-level instance management for a specific Xia
process, start it with:

```bash
XIA_ALLOW_INSTANCE_MANAGEMENT=false xia --instance coordinator --mode server
```

By default, the parent tries to launch the `xia` executable on `PATH`. If the
binary is elsewhere, override it with:

```bash
XIA_INSTANCE_COMMAND=/absolute/path/to/xia \
xia --instance coordinator --mode server
```

Managed child instances are:

- started only on loopback addresses
- given a fresh command-channel bearer token automatically
- auto-registered in the parent instance as bearer-auth peer services
- stopped again when the parent Xia process shuts down

The capability is not inherited by child instances. Controller Xia instances
always launch managed children with host-level instance management disabled.

The local web UI now has a `Controller mode` toggle on the identity/settings
card. That toggle is instance-specific and persisted in the instance DB, so you
can designate one Xia as the controller and keep others as ordinary workers.

Important: the web toggle does not override the host guard. Effective controller
mode is only active when both are true:

- host-level instance management is enabled for this Xia process
- `Controller mode` is enabled in the web UI

If you enable `Controller mode` in the UI for another Xia instance, Xia keeps
that preference saved and activates it on each future start as long as host
instance management has not been explicitly disabled for that process.

### Managed-Instance Tools

When that capability is enabled, Xia can use these built-in tools:

- `peer-instance-list`
- `peer-instance-start`
- `peer-instance-status`
- `peer-instance-stop`

Typical flow:

1. the parent Xia calls `peer-instance-start`
2. Xia starts the child on loopback and waits for `/health`
3. Xia registers the child as a managed peer service in the parent DB
4. the parent Xia uses `peer-chat` to delegate work
5. the parent Xia stops the child with `peer-instance-stop` when finished

## Configuring A Xia Peer Service

To let one Xia instance call another without hand-writing `curl` commands,
register the target instance as a normal service in the caller instance.

For a local peer on the same machine, use:

- `Base URL`: `http://127.0.0.1:<peer-port>` or `http://localhost:<peer-port>`
- `Auth Type`: `Bearer token`
- `Auth Secret`: the peer instance's command token
- `Allow local/private-network targets`: enabled

For a remote peer exposed over HTTPS, use its `https://...` base URL and the
same bearer-token auth, but you do not need the private-network toggle.

Once configured, Xia can use the built-in peer tools:

- `peer-list` to discover configured peer service ids
- `peer-chat` to send a message to a peer Xia instance

`peer-chat` returns the peer's `session_id`. Pass that back on later calls when
you want to continue the same remote conversation instead of starting a new
peer session.

## Shared Workspace Between Instances

Peer chat alone is not enough for practical collaboration. Xia instances can
also exchange files and notes through a machine-shared workspace directory.

This workspace is filesystem-backed, not DB-backed. Instances do not share a
Datalevin database or `.xia` support directory.

By default, published items are stored under:

```text
~/.xia/workspaces/default/items/<item-id>/
```

Each item stores:

- payload bytes as a normal file
- sidecar metadata with provenance, media type, size, preview, and timestamps
- the producing instance id and producing session id

### Workspace Tools

Xia now includes built-in workspace tools:

- `workspace-list`
- `workspace-read`
- `workspace-publish-artifact`
- `workspace-publish-doc`
- `workspace-write-note`
- `workspace-import-artifact`
- `workspace-import-doc`

Typical flow:

1. one Xia writes a note or publishes an artifact/doc into the shared workspace
2. another Xia uses `workspace-list` to discover it
3. the second Xia uses `workspace-read` to inspect it
4. if needed, the second Xia imports it into its own session as a local
   artifact or local document

### Artifact Versus Local-Doc Sharing

Artifacts and local docs do not behave the same:

- published artifacts keep their original bytes when Xia still has them
- published local docs use Xia's stored normalized text
- if a local doc originally came from a PDF, Office file, or image, the shared
  export becomes a derived `.txt` file with provenance metadata

This matters because Xia does not retain the original uploaded bytes for local
documents after extraction. The shared workspace avoids pretending otherwise.

### Import Behavior

`workspace-import-artifact` copies a shared item into the current session as an
artifact.

`workspace-import-doc` imports a shared item into the current session as a
local document, so it becomes searchable and readable through Xia's local-doc
tools.

For binary items that Xia's local-doc pipeline does not support,
`workspace-import-doc` can still fail. In that case, import the item as an
artifact instead.

## Seeding A New Instance From An Existing One

When you want several instances to share the same initial setup, you can seed a
new instance from an existing instance:

```bash
xia --instance qa --template-instance base
```

You can also set the template source with `XIA_TEMPLATE_INSTANCE`.

This is intended for reducing repeated setup work, especially for:

- LLM provider definitions
- provider model selections and budgets
- OAuth account definitions
- service definitions
- saved site login definitions
- web search configuration
- local-document summarization and OCR settings
- memory and backup admin settings
- identity defaults

### What Gets Copied

The template seed copies initial admin/setup state only:

- identity fields
- selected config keys used by admin settings
- OAuth accounts
- LLM providers
- services
- site credentials
- the default provider selection

For LLM providers, this includes the configured fields Xia stores for the
provider record, such as:

- provider name
- base URL
- model id/name
- provider template id
- access mode and credential source
- linked OAuth account, when used
- workload routing
- vision capability flag
- private-network flag
- system prompt budget
- history budget
- rate limit
- default-provider selection

For local-document features, the copied config includes:

- summarization enabled/disabled
- summarization backend
- external summarization provider id
- chunk and document summary token budgets
- OCR enabled/disabled
- OCR backend
- OCR external provider id
- OCR command path
- OCR model path overrides
- OCR timeout and max-token settings

For backup/admin tuning, the copied config includes:

- web search backend settings
- conversation context limits
- memory-retention settings
- knowledge-decay settings
- backup enablement, directory, interval, and retention count

### What Does Not Get Copied

The template seed does not copy ongoing working state such as:

- conversations
- episodic memory
- knowledge graph content
- local documents
- artifacts
- schedules and run history
- browser sessions
- installed tools
- installed skills

The goal is to clone setup, not operational history.

### Settings Versus Assets

The template feature copies settings, not support files.

In particular:

- provider records are copied, but provider/browser session runtime state is not
- OCR config is copied, but support files under `<db-path>/.xia` are not copied
- managed model binaries live in the shared machine cache, not inside each
  instance DB
- each target instance still manages its own DB and its own `.xia` support
  directory

This is intentional. A template instance provides initial configuration, while
managed model binaries are shared separately at the machine level.

### Important Constraints

- The target instance must still be effectively empty. Xia will not seed over an
  already-configured instance.
- `--template-instance` currently reads from the source instance's default DB
  path under `~/.xia/instances/<instance-id>/db`.
- The source and target instance must be different.
- Template seeding is one-time initialization. It is not a live shared config
  link, and later edits in the template instance do not automatically propagate
  to existing instances.
- Template seeding currently applies to admin/setup state only. It is not a
  general instance-cloning feature.

### Secrets And Encryption

Template seeding is best-effort for secret values.

If Xia can decrypt the source instance's secrets with the current master
key/passphrase configuration, it re-encrypts them into the target instance.
If it cannot, Xia still copies the non-secret settings and skips the secret
fields instead of failing the whole startup.

In practice, template seeding works best when the instances share the same
master-key or passphrase setup.

## Recommended Pattern

One practical pattern is:

1. Create a base instance and configure shared model/provider settings.
2. Start new role-specific instances with `--template-instance base`.
3. Edit each instance's identity so the role is explicit.
4. Let each instance keep its own memory, browser state, and artifacts.

Example:

```bash
xia --instance base --mode server
xia --instance ops --template-instance base --mode server
xia --instance research --template-instance base --mode server
```

## Packing And Backups

Instance scoping also works with packaging:

```bash
xia pack --instance ops
```

That packages the DB for the selected instance.
