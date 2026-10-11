# REST job client

`factory rest` is an opt-in CLI client for the existing versioned REST job API. The remote server remains authoritative for execution and job state; these commands do not run a local job or use the local detached-job store. Use `factory job` for locally managed detached workflows, and `factory server --config ...` to operate a server. Client configuration is separate from the server's configuration and API-key file.

## Client configuration and credentials

Create a private configuration and a separate bearer-token file. The defaults are `$XDG_CONFIG_HOME/factory/rest-client.json` (or `~/.config/factory/rest-client.json`) and a token file path relative to that JSON file. Both files must be regular files with no group/other permission bits (normally mode `0600`). The parent directory should also be private. The client config accepts only `base_url` and `token_file`:

```sh
umask 077
install -d -m 700 "${XDG_CONFIG_HOME:-$HOME/.config}/factory"
printf '%s\n' 'https://factory.example.internal' > "${XDG_CONFIG_HOME:-$HOME/.config}/factory/base-url.tmp"
# Provision the token using your secret manager; do not put it in shell history.
# Save the bearer token alone in factory/rest-api-token with mode 0600.
```

Create `rest-client.json` with an operator-selected URL and a token-file reference, for example:

```json
{
  "base_url": "https://factory.example.internal",
  "token_file": "rest-api-token"
}
```

The config may alternatively point at a private absolute token-file path. Use HTTPS except for trusted loopback/local test servers. Do not put the token in the URL, command arguments, task text, environment variables, or logs. The client reads the credential file locally and sends it only as the HTTP bearer authorization header. `--config <path>` selects this client config; it never selects the server config. Errors are sanitized and do not print response bodies or credentials.

## Commands

```text
factory rest submit --repository <configured-alias> [--idempotency-key <key>] [--json] <task>
factory rest get [--json] <job-id>
factory rest list [--limit 1..100] [--after <cursor> --snapshot <sequence>] [--json]
factory rest history [--json] <job-id>
factory rest watch [--poll-interval <duration>] [--json] <job-id>
factory rest cancel [--json] <job-id>
factory rest operations [--json]
factory rest disposition --confirm [--json] <failed|canceled> <job-id>
factory rest reconcile [--json] <job-id>
```

Add `--config <path>` to any command to select a non-default client config.

Submission uses the existing `POST /v1/jobs` route and an idempotency key; if one is omitted, the client generates a fresh key. Supply a stable key when retrying the same submission after an uncertain connection failure. `get`, `list`, and `history` use the existing authenticated inspection routes. Continue a bounded listing with both `--after` and `--snapshot` from the previous page; omit both to start a fresh snapshot. `cancel` calls the existing per-job cancellation route and is cooperative: it requests that one running executor stop; it is not forced termination and may remain pending until the executor exits. `operations` calls only the existing authenticated `GET /v1/operations` route and reports its curated aggregate counts and capacity indicators; it does not list or inspect individual jobs and performs no writes.

Recovery commands call only the server's existing authenticated, bodyless `POST` routes; they accept no evidence, PR identity, provider data, or arbitrary request payload. Before acting, inspect remote status and history with `get` and `history`. `factory rest disposition` requires both an explicit outcome (`failed` or `canceled`) and the separate `--confirm` flag. It is only eligible for a SQLite job classified by this server instance as interrupted while running, with no persisted provider attempt or outcome. It records only the chosen non-success status and a fixed audit event. It does not certify work or alter retained verification evidence. The explicit confirmation is required even though the server may still reject a stale or ineligible job; memory mode has no durable interrupted-job classification and rejects disposition.

`factory rest reconcile` requests read-only provider confirmation. The server permits only an interrupted SQLite job still classified as running or a terminal failed SQLite job, and requires persisted provider attempt/outcome identity, a configured read-only-capable provider, and a valid retained workspace. The server checks the configured checkout and persisted worktree branch/commit; it records success only if exactly one open PR matches the repository, job branch, commit, configured base branch, and Factory job identity. This command does not rerun the harness/checks or write to the provider. Ineligible, ambiguous, mismatched, unavailable, or unpersistable results remain unresolved and return a sanitized error. Neither command operates on local detached jobs (`factory job`), and neither can recover a memory-store job. See the [operator recovery guide](rest-server-operations.md#restart-and-interrupted-work) for exact server behavior and investigation cautions.

`watch` first follows the existing bounded Server-Sent Events route and resumes with `Last-Event-ID` after a stream reconnect. The server limits each connection's lifetime; the client reconnects and checks status/history between connections. If the cursor expires or streaming is unavailable, it reconciles status/history and switches to polling at the configured interval. A terminal status ends the watch. Ctrl-C cancels only the client's wait/request; it does not cancel the remote job (use `factory rest cancel` explicitly for that).

All commands print concise human-readable text by default. `--json` opts into one stable JSON value for submit/get/list/history/cancel/operations; each object has `schema_version: 1`. The operations JSON contains only the aggregate count/capacity fields returned by the existing endpoint—never job IDs, task or provider details, event messages, logs, credentials, or host paths. Other JSON output also omits task text, provider details, event messages, logs, credentials, and host paths. `watch --json` emits the terminal status and bounded curated history as one JSON object when the job completes; it does not emit a stream of JSON fragments. JSON shape changes require an intentional schema-version change.

The operations result is a read-only aggregate snapshot, not a health or readiness probe and not a correctness verdict about any job, verification result, or provider state. Use the server's unauthenticated `/healthz` and `/readyz` routes for their distinct probe purposes.

For recovery, `--json` follows the same `{"schema_version":1,"result":{"job":...}}` envelope as `get` and `cancel`. The returned job contains only ID, status, cancellation flag when present, and timestamps. Text and JSON output omit task text, provider data, history messages, logs, credentials, and host paths. HTTP failures include status and a small generic hint only; arbitrary server/provider response text and returned snapshots are never printed raw. Recovery commands make a single request and do not retry an action automatically.

The client speaks the existing `/v1` API only and does not negotiate or silently fall back across incompatible API versions. Server errors include the HTTP status and a small set of actionable generic hints for authentication, readiness, cancellation conflict, and cursor expiry; arbitrary server response text is never shown.

## Tests and trust boundary

The subprocess/integration tests use isolated in-process HTTP servers backed by the same REST handler and memory manager. They cover synthetic submission, listing, inspection, cancellation, terminal watch, operations text and JSON redaction, authenticated operations failure, readiness failure, and proof that the operations command sends only `GET /v1/operations` without changing job state. Recovery subprocess tests check pre-HTTP confirmation/argument validation, authenticated bodyless POST routes, sanitized conflicts and auth errors, SQLite disposition/reconciliation eligibility, memory-mode rejection, unchanged unrelated job state, and read-only reconciliation with no provider writes. Coverage requires no live provider, repository, GitHub credentials, or provider writes. Client event cursor/error behavior is unit-tested against `httptest` SSE responses. These deterministic tests do not validate a deployed remote service or live provider.

The bearer token grants the configured API-key authority of the remote server. Limit token access and server network exposure accordingly. A local or remote Factory process is not a sandbox; this client does not change server trust assumptions.
