# Stored Data Classification

Xia uses application-level field encryption for credentials. It does not
encrypt the entire Datalevin database or every support file. The operating
system account, database directory permissions, and backup storage are part of
the trust boundary for all plaintext classes below.

## Classification

| Class | Examples | Storage | Sandbox access | Retention |
| --- | --- | --- | --- | --- |
| Credentials | LLM API keys, service auth keys, OAuth client secrets and tokens, site usernames/passwords, secret config values | AES-GCM encrypted fields; master-key material is kept outside the database | Blocked | Until the credential or owning record is replaced or deleted |
| User content | Messages, session recaps, tool arguments/results, task and memory content, imported documents, artifacts, audit payloads, schedule prompts/actions/results | Plaintext database fields or Xia support files | Sensitive transcript, audit, and schedule payload attributes are blocked; purpose-built safe views may expose summaries | Durable until the owning record is deleted or a subsystem-specific cleanup runs |
| Operational metadata | IDs, timestamps, state/status fields, provider/model names, token counts, durations, and audit envelope fields | Plaintext | Available only through the API appropriate to the subsystem | Follows the owning record; LLM-call metadata follows the diagnostic retention window |
| Disposable diagnostics | Optional duplicate LLM prompts, tool definitions, raw provider responses, and provider error text | Plaintext database fields, disabled by default | Blocked | Configurable from 1 to 3650 days; 30 days by default |

Backups and exported archives preserve the storage form of their source. In
particular, plaintext user content remains plaintext in a backup. Backups must
therefore be protected as user data even though credential fields remain
encrypted.

## Classifier Boundaries

Xia makes separate decisions for encryption, sandbox payload protection,
configuration access, and Datalog query filtering. A positive query-filter
decision does not necessarily mean that the underlying field is a credential.

| Example | Encrypted at rest | Explicit protected payload | Sandbox query | Reason |
| --- | --- | --- | --- | --- |
| `:llm.provider/api-key` | Yes | Yes | Blocked | Credential field |
| `:message/content` | No | Yes | Blocked | Durable plaintext user content |
| `:llm.log/response` | No | Yes | Blocked | Optional plaintext diagnostic |
| `:llm.provider/model` | No | No | Allowed | Public operational metadata |
| `:llm.log/prompt-tokens` | No | No | Blocked | Conservative `token` name match |
| `:site-cred/password-field` | No | No | Blocked | Conservative `password` name match |

Secret config keys use explicit keys and namespace prefixes. Privacy-boundary
settings such as `:llm/log-full-payloads?` remain readable by sandboxed code but
cannot be changed there. Ordinary settings remain readable and writable.

Both namespace and name matching deliberately prefer false positives over
possible credential disclosure. Namespace prefixes use `starts-with?`, so
namespaces such as `oauth2`, `tokenizer`, `secretary`, and `credentialed` are
treated as secret. Query-name matching also blocks harmless operational fields
containing `api-key`, `password`, `secret`, `credential`, `token`, `oauth`, or
`private-key`. These conservative matches may reduce sandbox query visibility;
they do not cause token counters, selectors, or other pattern-only matches to
be encrypted at rest.

## Explicit Decisions

- Messages are durable plaintext user content. Xia needs them for history,
  context reconstruction, and resumable tasks.
- Tool arguments and results are durable plaintext user content when stored in
  transcripts, task records, audit events, or schedule history. Sandbox queries
  cannot read the protected payload attributes directly.
- Audit payloads are plaintext because they support local inspection and
  incident reconstruction. Audit envelope metadata is not treated as secret.
- Schedule outputs and action details are plaintext and are removed when their
  run records are trimmed or the schedule is deleted.
- LLM-call metadata is a bounded operational diagnostic. Full prompt, tool,
  response, and error duplication is off by default and must be explicitly
  enabled.

Field encryption is intended to keep credentials out of raw database reads,
backups, and sandboxed tools. It is not a substitute for full-disk encryption,
host access controls, secure deletion, or protecting an unlocked Xia process.

## LLM Diagnostic Controls

The local admin UI exposes both settings under **LLM Call Log**:

- `:llm/log-full-payloads?` — defaults to `false`. When false, new LLM log
  entries retain only provider/model, status, timing, and token metadata.
- `:llm/log-retention-days` — defaults to `30`; accepted range is 1–3650 days.

The same settings can be supplied through Xia's tenant/runtime configuration
mechanisms. `POST /admin/llm-logging` changes tenant settings and immediately
enforces the resulting retention cutoff. Turning detailed capture off affects
new entries; previously captured payloads remain until their entries expire.
