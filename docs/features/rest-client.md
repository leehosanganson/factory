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
factory rest issue --repository <server-configured-alias> --issue-repository <owner/repo> [--confirm-version <sha256>] [--json] <issue-number>
factory rest get [--json] <job-id>
factory rest list [--limit 1..100] [--after <sequence> --snapshot <sequence>] [--json]
factory rest list --mine [--limit 1..100] [--cursor <opaque-cursor>] [--json]
factory rest history [--json] <job-id>
factory rest watch [--poll-interval <duration>] [--json] <job-id>
factory rest cancel [--json] <job-id>
factory rest operations [--json]
factory rest disposition --confirm [--json] <failed|canceled> <job-id>
factory rest reconcile [--json] <job-id>
```

Add `--config <path>` to any command to select a non-default client config.

Submission uses the existing `POST /v1/jobs` route and an idempotency key; if one is omitted, the client generates a fresh key. Supply a stable key when retrying the same submission after an uncertain connection failure. `get`, `list`, and `history` use the existing authenticated inspection routes. The default `list` is the operator-wide listing; continue a bounded listing with both `--after` and `--snapshot` from the previous page, and omit both to start a fresh snapshot. `list --mine` explicitly selects the authenticated submitter's own jobs, including on the first page; continue it with `--cursor` from `next_cursor`. The opaque submitter cursor cannot be combined with `--after` or `--snapshot`, and `--cursor` requires `--mine`. Submitter cursors are server-side, principal-bound process-memory state, expire after 15 minutes, and are invalid after server restart. Operator listings retain their numeric sequence pagination. `cancel` calls the existing per-job cancellation route and is cooperative: it requests that one running executor stop; it is not forced termination and may remain pending until the executor exits. `operations` calls only the existing authenticated `GET /v1/operations` route and reports its curated aggregate counts and capacity indicators; it does not list or inspect individual jobs and performs no writes.

`factory rest issue` is a two-step, explicit dispatch of one GitHub issue snapshot into the ordinary REST job lifecycle. It reads the current issue once through the existing read-only `IssueTracker` (`gh` authentication is managed outside Factory), then prints the issue identity, exact SHA-256 snapshot version, and exact generated task that would be submitted. Preview performs no REST request and admits no job. The generated task includes source repository, issue number/version, title, and body inside clearly delimited context that explicitly frames issue content as untrusted input. Terminal control characters and bidi formatting controls in issue title/body are rendered as visible `\uXXXX` escapes (ordinary joiner text such as ZWJ is preserved), and occurrences of either reserved context delimiter are escaped so issue content cannot impersonate task framing. The same escaped task is previewed and submitted; the task is rejected rather than truncated if it exceeds the REST API's 256 KiB task limit.

To dispatch, rerun with `--confirm-version <sha256>` copied from the preview. The client fetches the snapshot again and submits only if its exact version still matches and the issue remains open. A changed version, closed issue, pull request, tracker error, or invalid snapshot causes no admission; request a fresh preview and confirm that version. No interactive prompt is used. The submission is a nonempty ordinary task through the existing `POST /v1/jobs` endpoint; its `repository` field is the configured destination alias, and the numeric `issue` field matches an additive `issue_provenance` object containing only `tracker: github`, source repository, positive issue number, lowercase 64-character SHA-256 version, matching HTTPS GitHub issue URL, and sanitized title (at most 4 KiB). The issue body is not duplicated in provenance: it remains in the exact ordinary `request.task` sent by the client and is visible through the existing authenticated job-detail response. This provenance is caller-supplied; the server does not fetch GitHub or verify snapshot authenticity. A changed version maps to a new provenance identity only after this command's explicit confirmation and second fetch. The client uses the same deterministic idempotency key as before.

The idempotency key is a deterministic SHA-256 digest over an unambiguously length-prefixed tuple of destination alias, source repository, issue number, and exact issue version. The server additionally reserves the tuple `(tracker, source_repository, issue_number, version)` across idempotency keys for as long as its job record is retained. The exact same complete request returns the original job and attaches the newly supplied key; any changed task, destination, title, URL, issue context, or other request field conflicts with sanitized HTTP 409. Ordinary idempotency-key replay still compares provenance and task exactly, so omitting provenance cannot reuse a provenance-generated key. A different version is a separate identity and is only submitted through explicit confirmed dispatch. `--idempotency-key` is rejected for `issue` because an arbitrary key would break the deterministic retry mapping. Text preview shows the exact task in a bounded framed block; `--json` emits a `schema_version: 1` preview object containing the version and exact task. Confirmed dispatch uses the standard safe submission envelope (and JSON also identifies the confirmed source issue/version). The command creates no local work item, autonomous poller, or provider write.

Recovery commands call only the server's existing authenticated, bodyless `POST` routes; they accept no evidence, PR identity, provider data, or arbitrary request payload. Before acting, inspect remote status and history with `get` and `history`. `factory rest disposition` requires both an explicit outcome (`failed` or `canceled`) and the separate `--confirm` flag. It is only eligible for a SQLite job classified by this server instance as interrupted while running, with no persisted provider attempt or outcome. It records only the chosen non-success status and a fixed audit event. It does not certify work or alter retained verification evidence. The explicit confirmation is required even though the server may still reject a stale or ineligible job; memory mode has no durable interrupted-job classification and rejects disposition.

`factory rest reconcile` requests read-only provider confirmation. The server permits only an interrupted SQLite job still classified as running or a terminal failed SQLite job, and requires persisted provider attempt/outcome identity, a configured read-only-capable provider, and a valid retained workspace. The server checks the configured checkout and persisted worktree branch/commit; it records success only if exactly one open PR matches the repository, job branch, commit, configured base branch, and Factory job identity. This command does not rerun the harness/checks or write to the provider. Ineligible, ambiguous, mismatched, unavailable, or unpersistable results remain unresolved and return a sanitized error. Neither command operates on local detached jobs (`factory job`), and neither can recover a memory-store job. See the [operator recovery guide](rest-server-operations.md#restart-and-interrupted-work) for exact server behavior and investigation cautions.

`watch` first follows the existing bounded Server-Sent Events route and resumes with `Last-Event-ID` after a stream reconnect. The server limits each connection's lifetime; the client reconnects and checks status/history between connections. If the cursor expires or streaming is unavailable, it reconciles status/history and switches to polling at the configured interval. A terminal status ends the watch. Ctrl-C cancels only the client's wait/request; it does not cancel the remote job (use `factory rest cancel` explicitly for that).

All commands print concise human-readable text by default. `--json` opts into one stable JSON value for submit/issue/get/list/history/cancel/operations; each object has `schema_version: 1`. The operations JSON contains only the aggregate count/capacity fields returned by the existing endpoint—never job IDs, task or provider details, event messages, logs, credentials, or host paths. Submitter-list JSON contains only `jobs`, `has_more`, and the opaque `next_cursor`; it does not expose operator numeric sequence fields or `submitted_by`. Other JSON output also omits task text, provider details, event messages, logs, credentials, and host paths. `watch --json` emits the terminal status and bounded curated history as one JSON object when the job completes; it does not emit a stream of JSON fragments. JSON shape changes require an intentional schema-version change.

The operations result is a read-only aggregate snapshot, not a health or readiness probe and not a correctness verdict about any job, verification result, or provider state. Use the server's unauthenticated `/healthz` and `/readyz` routes for their distinct probe purposes.

For recovery, `--json` follows the same `{"schema_version":1,"result":{"job":...}}` envelope as `get` and `cancel`. The returned job contains only ID, status, cancellation flag when present, and timestamps. Text and JSON output omit task text, provider data, history messages, logs, credentials, and host paths. HTTP failures include status and a small generic hint only; arbitrary server/provider response text and returned snapshots are never printed raw. Recovery commands make a single request and do not retry an action automatically.

The client speaks the existing `/v1` API only and does not negotiate or silently fall back across incompatible API versions. Server errors include the HTTP status and a small set of actionable generic hints for authentication, readiness, cancellation conflict, and cursor expiry; arbitrary server response text is never shown.

## Tests and trust boundary

The subprocess/integration tests use isolated in-process HTTP servers backed by the same REST handler and memory manager. They cover synthetic submission; issue preview with no POST; confirmed issue dispatch through the production handler with an accepted nonempty task, matching issue number, bounded provenance wire fields without duplicated body, stable retry key/replay, separate changed-version key, stale/closed/provider-error no-admission paths, task bounds, terminal-control and bidi-control escaping, reserved-delimiter escaping, and preservation of ordinary ZWJ text; listing, inspection, cancellation, terminal watch, operations text and JSON redaction, authenticated operations failure, readiness failure, and proof that the operations command sends only `GET /v1/operations` without changing job state. Recovery subprocess tests check pre-HTTP confirmation/argument validation, authenticated bodyless POST routes, sanitized conflicts and auth errors, SQLite disposition/reconciliation eligibility, memory-mode rejection, unchanged unrelated job state, and read-only reconciliation with no provider writes. REST API handler tests cover malformed provenance and sanitized `issue_provenance_conflict`; shared memory/SQLite store contracts and SQLite reopen/cross-connection tests cover provenance uniqueness, complete-request equality, key attachment, changed versions, and retained mapping behavior. Coverage requires no live provider, repository, GitHub credentials, or provider writes. Client event cursor/error behavior is unit-tested against `httptest` SSE responses. These deterministic tests do not validate a deployed remote service or live provider.

The bearer token's server-configured role controls its authority. The shared legacy API key is an operator credential; named submitter credentials can submit jobs and inspect/list only their own jobs, while named operators retain operator-wide job access and operator actions. Use `factory rest list --mine` with a named submitter credential; the client does not infer the role. Limit token access and server network exposure accordingly. A local or remote Factory process is not a sandbox; this client does not change server trust assumptions.
