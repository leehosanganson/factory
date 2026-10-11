# REST server operations

This guide applies to the single-server REST job service. See [REST server setup and configuration](rest-server-config.md) for the current schema, [REST job server](rest-server.md) for shipped limits and trust boundaries, and the [REST API contract](../roadmap/rest-api-contract.md) for the target lifecycle. The published definition is the [OpenAPI specification](../openapi/rest-api-v1.json). Compatible API changes update the spec and info.version; incompatible API changes publish a new versioned document. For a single-host systemd deployment, see the [supervised deployment recipe](rest-server-systemd.md).

## Health and readiness

- `GET /healthz` is a liveness probe. It returns `200` while the HTTP process can answer; it does not promise that job admission can succeed.
- `GET /readyz` is an unauthenticated readiness probe. It returns `200` only after startup initialization and successful health checks of configured dependencies. During startup, store/provider failure, or shutdown it returns `503`.
- Job API routes check readiness before processing requests. For SQLite, the store probe must acquire and roll back a write transaction, so an external write lock makes readiness return `503`; SQLite's configured five-second busy timeout can bound how long that probe waits. A `503 not_ready` response means the request was not admitted; retry with the same `Idempotency-Key` after readiness returns.
- The handler admits at most four readiness checks at once across `/readyz` and all other API routes. This fixed process-local limit is not configurable. When all four slots are occupied, additional requests immediately receive the same sanitized `503` response rather than waiting for a slot. An admitted check uses a two-second request-derived context; the SQLite driver can remain in its configured five-second busy wait before returning from lock contention, after which the slot is released. Cancellation is passed to the dependency probe, but do not assume the SQLite driver's lock wait ends immediately on client disconnect. `/healthz` does not use readiness checks and remains a liveness-only response.

Keep probes on a trusted network boundary. They expose only bounded status—not credentials, filesystem paths, job contents, or provider diagnostics.

## Bounded job listing

Authenticated clients can rediscover retained job IDs with `GET /v1/jobs`. The endpoint is read-only and returns a summary containing only `id`, `status`, `created_at`, and `updated_at`; it never returns task/request text, provider details, events, logs, or paths. It includes queued, running (including interrupted SQLite jobs), and retained terminal records. SQLite retention is bounded by the configured record limit and refuses new admissions rather than evicting records; memory records may be evicted to stay within its bounded registry. Listing does not change job state, trigger recovery, or contact a provider.

Pages default to 50 entries and accept `limit=1..100`. Results are ordered oldest admission first using an immutable store admission sequence (not timestamps, which can tie). The first response captures `snapshot_sequence`; while `has_more` is true, pass its `next_sequence` as `after` and repeat the same `snapshot` and `limit`:

```text
GET /v1/jobs?limit=50
GET /v1/jobs?limit=50&after=<next_sequence>&snapshot=<snapshot_sequence>
```

The first page also returns an opaque-to-clients `next_sequence` only when another page is available. Both `after` and `snapshot` are required together, must be positive sequence values, and `after` cannot exceed `snapshot`; only `limit`, `after`, and `snapshot` are accepted. Invalid or repeated query parameters return sanitized `400 invalid_request`. Admission after the first page is excluded from that traversal, so it cannot shift page boundaries or cause duplicates; start a new request without cursors to see newer jobs. The snapshot freezes membership only: each page reports current status and `updated_at`, and records removed by memory retention may no longer appear. This is a bounded inspection view, not a transactional snapshot of all lifecycle fields.

## Following one job's events

Clients that need lower-latency updates may open authenticated, read-only `GET /v1/jobs/{id}/events` using Server-Sent Events. The route requires a known retained job and supports only `GET`. Each frame has a decimal `id` (the per-job event sequence), an allowlisted lifecycle `event` name, and JSON `data` containing only `at` and `type`. Event messages are intentionally omitted: task/request text, logs/transcripts, provider details/secrets, credentials, filesystem paths, and workspaces are never sent.

The stream rereads the bounded history every 250 ms; this avoids missed events between the initial read and continued observation without subscriber goroutines or event queues. Event sequence numbers increase monotonically for each job and are never reused when old bounded-history entries are evicted. Reconnect using `Last-Event-ID: <last-id>` to replay only later retained events. Replaying a retained cursor does not duplicate delivered IDs. If the cursor is ahead of current history, the request is invalid. If the cursor predates retained history, the route returns `409 cursor_expired`; if retention expires the cursor while connected, the server sends an `event: reset` frame and closes. If history was already truncated at initial connection, the route returns `409 cursor_expired` for an active job instead of implying a complete baseline; terminal jobs expose the available retained tail without claiming it is complete. If the terminal lifecycle event is no longer retained, the stream sends a synthetic `terminal_snapshot` control frame containing only the current terminal status, then closes. Control frames have no event ID and do not advance `Last-Event-ID`; clients should close their event source when they receive `terminal_snapshot` rather than reconnecting a completed job. In either case, fetch authenticated `GET /v1/jobs/{id}` and `GET /v1/jobs/{id}/history` to reconcile current status and the truncation marker. When the HTTP response is `409 cursor_expired`, reconnect without `Last-Event-ID` to begin from available retained events; when an in-stream `reset` frame is received, do not reconnect automatically from the expired cursor—first reconcile status/history, then reconnect without it. Terminal jobs with truncated history replay the available tail and close because no future events can appear; when the terminal lifecycle event was evicted, `terminal_snapshot` reports the current terminal status without exposing audit event content.

At most 16 streams are admitted process-wide; streams over capacity fail promptly with `503 stream_capacity`. Each stream sends a comment heartbeat every 15 seconds, expires after 25 seconds, and gives each network write a two-second deadline. Client disconnect, terminal closure, timeout, or write failure releases the slot promptly. No buffering grows with the number of events or connected clients. Stream clients should reconnect as needed; ordinary polling remains the explicit fallback and is unchanged. The same semantics are exercised against memory and SQLite stores. The OpenAPI schema and route are in the [version 1.2.0 API specification](../openapi/rest-api-v1.json).

## Aggregate operational status

Authenticated clients can request `GET /v1/operations` with the same bearer token used for job routes. It returns only aggregate counts and configured capacity indicators, for example:

```json
{
  "retained_records": 12,
  "record_limit": 1000,
  "queue_capacity": 8,
  "queued": 2,
  "running": 3,
  "succeeded": 5,
  "failed": 1,
  "canceled": 1,
  "queue_saturated": false,
  "recovery_needed": 0
}
```

The response contains no job IDs, task or repository values, event history, provider data, or credentials. `recovery_needed` counts SQLite jobs found running when this store instance opened; they may have external side effects and require explicit operator handling through the eligible disposition or provider-reconciliation actions below. Startup does not replay them. It is always zero for the volatile memory store, which starts empty. This endpoint is a snapshot for operational visibility, not a health/readiness probe or a correctness verdict. A store query error returns a generic internal error without storage details.

## Persistence and backup

Memory mode is volatile: jobs, idempotency keys, and history are lost on process restart. Use it for local development and tests, not as a durable queue.

With SQLite configured, the database file is the durable job/history store. Only one local REST server process may own a configured SQLite database at a time; another server opening that same database fails before performing startup recovery, so it cannot classify the active server's running jobs as interrupted. Ownership uses a private per-user lock file keyed by the database file's device and inode, so hard-link aliases resolve to the same owner lock without interfering with SQLite's own database locking or administrative inspection. Lock files are retained after shutdown; do not remove them while a server may be running. Ownership is released on orderly shutdown and automatically when the owning process exits. This does not restrict administrative online backup access. Per-job workspaces live separately under the server results directory and are not included in a database backup; provider reconciliation after restore requires both persisted provider identity and its retained workspace. Preserve workspace data separately under the same private access controls when recovery after restore is required. Prefer SQLite's online backup API for a live service. A filesystem snapshot is safe only after a clean shutdown; ensure it captures a consistent database state rather than copying only the main database while WAL writes are active. Store backups in access-controlled, encrypted storage and periodically verify restore by reopening a copy in a disposable directory. Do not copy a live database file alone while writes may be in progress.

SQLite retention is bounded by the configured record limit. When the retained-record cap is reached, new admissions fail rather than evicting terminal history or idempotency records. Inspect capacity and plan backup/archive operations before the cap is reached.

## Per-job cancellation

Use bodyless bearer-authenticated `POST /v1/jobs/{id}/cancel` to request cancellation of exactly one job. A queued job is canceled before claim and execution and returns `200` with the canceled snapshot. A running job returns `202` with its still-running snapshot and `cancellation_requested: true`; its worker context is canceled, but the terminal status/history event is written only after the executor exits. Repeating while cancellation is pending returns the same `202` state and does not append another request event. A terminal or otherwise ineligible job returns `409 job_not_cancelable` unchanged. Wrong methods return `405` with `Allow: POST`; nonempty bodies return `400 invalid_request`; unauthenticated requests return `401 unauthenticated` (readiness can reject first with `503 not_ready`).

Cancellation is cooperative, not process shutdown, forced termination, rollback, or restart recovery. It affects only the selected job and does not cancel other workers. Canceled and ambiguous workspaces are retained; cancellation never closes a PR or undoes provider effects. If a provider attempt may have completed, Factory does not manufacture `canceled` or `succeeded`: the job remains `failed` for inspection and may use eligible read-only SQLite provider reconciliation. A confirmed provider outcome already persisted remains authoritative. Memory cancellation state/history are volatile across restart; SQLite persists the cancellation request and audit history. Server shutdown remains a separate manager-wide lifecycle that cancels queued work and signals active workers; SQLite restart treats still-running records as interrupted and does not replay them.

## Restart and interrupted work

After restart, SQLite resumes queued jobs because they were not claimed. A job found `running` at startup is marked as needing operator handling and is never automatically replayed: its workflow or provider operation may have partially completed. Terminal records remain inspectable. Use authenticated `GET /v1/jobs/{id}` and `GET /v1/jobs/{id}/history` before deciding what to do; these actions do not accept caller-supplied evidence or PR identity.

| Action | Eligible job | Effect |
| --- | --- | --- |
| `POST /v1/jobs/{id}/disposition/failed` or `/canceled` | SQLite job this store instance classified as interrupted while `running`, with no persisted provider attempt or provider outcome | Atomically records only the chosen non-success status and fixed `operator_disposition` history event. Existing verification evidence is retained. |
| `POST /v1/jobs/{id}/reconcile` | SQLite job still `running` and classified as interrupted, or terminal `failed` job; a persisted provider attempt or outcome and a retained workspace are required | Performs read-only provider confirmation. Only an exact matching open PR can atomically record the confirmed provider outcome, success transition, and `provider_reconciled` event. |

A persisted attempt or outcome, even if not marked uncertain, rules out status-only disposition. `canceled` and other terminal statuses are not reconciliation candidates. Active `running` jobs, queued jobs, jobs without persisted provider identity, jobs without a configured read-only provider, and jobs whose workspace is missing or invalid are ineligible for provider reconciliation. Memory mode has no durable startup-recovery classification and rejects these SQLite recovery actions. The API cannot turn caller-supplied evidence or a claimed PR into success.

Both actions require bearer authentication and an empty request body; no `Idempotency-Key` is required or used by these action routes. The first-party remote client exposes these existing routes as `factory rest disposition --confirm <failed|canceled> <job-id>` and `factory rest reconcile <job-id>`; disposition requires an explicit outcome and separate confirmation, while reconciliation remains a single explicit request. See the [REST job client guide](rest-client.md) for usage and sanitized output behavior. These are REST-server operations and do not change or recover local `factory job` detached workflows. A nonempty body or invalid disposition returns `400 invalid_request`; missing/invalid authentication returns `401 unauthenticated`; readiness failure returns `503 not_ready` before authentication; and wrong methods return `405 method_not_allowed` with `Allow: POST`. With a configured reconciliation callback/provider, an unknown reconciliation job returns `404 not_found`; if reconciliation is not configured or available, the callback can reject with `409 reconciliation_not_allowed` before looking up the ID. Disposition returns `409 reconciliation_not_allowed` when the ID is unknown or not in this store's recovery-needed set. Other ineligible or unresolved cases return `409 reconciliation_not_allowed`; an unavailable reconciliation callback returns `409 reconciliation_unavailable`. Unexpected job/history reads return generic `500 internal_error`; reconciliation callback errors, including provider and atomic transition/write failures, map to `409 reconciliation_not_allowed`. Errors are sanitized and do not expose credentials, paths, or raw provider/store details.

Each successful action returns HTTP `200` with the job snapshot. The SQLite store transition is idempotent for an identical repeated disposition, but the HTTP route only permits jobs still classified as recovery-needed; after its first success that classification is cleared, so a repeated disposition request returns `409 reconciliation_not_allowed`. A conflicting disposition or stale/ineligible state also returns `409`. A successful reconciliation retry returns the already-succeeded snapshot without another provider lookup or provider write. Repeating an unresolved reconciliation may make another read-only lookup; it never replays the harness or a provider write. Reconciliation atomically records provider outcome, succeeded status, and bounded history in SQLite; disposition atomically records the selected failed/canceled status and fixed audit event. History is subject to the configured per-job event bound, with truncation indicated in the history response. SQLite does not evict terminal records or idempotency keys when its record cap is reached; it rejects new admissions instead.

For reconciliation, Factory verifies the configured repository checkout, the retained worktree's exact persisted branch and commit, and then asks the configured provider for a read-only lookup. Success requires exactly one open PR matching the configured repository, job-specific branch, persisted commit, configured base branch, and Factory job identity. It does not run the harness or checks, push a branch, or create/update a PR. A missing, malformed, multiple, mismatched, or unavailable provider result leaves an interrupted job `running` (or a previously failed job `failed`) and retains its workspace and evidence. A failed SQLite write also fails closed; it does not record success. The reconciled provider outcome, status transition, and bounded audit history are durable in SQLite. Disposition likewise records its status and audit event atomically; a repeat with the opposite disposition conflicts.

Inspect history and evidence before acting. Keep the job record and workspace; do not manually delete interrupted work or retry submission with a new key. The workspace remains separate from SQLite: the online backup command below backs up the database, not the workspace tree. A restored database without its corresponding retained workspace cannot perform provider reconciliation. Preserve workspace data separately under the same private access controls if recovery after restore is required; if it is missing, investigate rather than manufacturing a result. Reconciliation does not create the protected completion marker used by workspace cleanup, so the reconciled workspace remains retained; failed, canceled, interrupted, and unresolved workspaces are also retained. Job records/history remain subject to configured SQLite record/history bounds. Successful reconciliation is not merge approval or an independent correctness verdict; the agent and local execution are not sandboxed.

Ordinary submission does not resume failed work: the original idempotency key returns the existing record and a new key may admit duplicate work.

Select one command only after inspecting job status/history. The setup walkthrough above creates the private `/var/lib/factory/curl-headers` authorization file and sets `base_url`; use the inspected job ID. Every request is bodyless and accepts no caller evidence or provider identity. For a job with no persisted provider attempt/outcome, choose one disposition (`failed` or `canceled`):

```sh
job_id='<inspected-job-id>'
curl --fail-with-body --silent --show-error --write-out '\nHTTP %{http_code}\n' --request POST --header @/var/lib/factory/curl-headers "$base_url/v1/jobs/$job_id/disposition/failed"
```

To choose `canceled`, replace `/disposition/failed` with `/disposition/canceled`. For an eligible persisted provider attempt/outcome, including a terminal failed job, use read-only reconciliation:

```sh
job_id='<inspected-job-id>'
curl --fail-with-body --silent --show-error --write-out '\nHTTP %{http_code}\n' --request POST --header @/var/lib/factory/curl-headers "$base_url/v1/jobs/$job_id/reconcile"
```

A conflict or sanitized failure leaves unresolved work inspectable; investigate rather than retrying blindly.

### Reconciliation result and limits

Use this only after inspecting an eligible interrupted-running or terminal `failed` SQLite job and its history, confirming a recorded provider attempt/outcome, and verifying its retained workspace and expected PR identity. The walkthrough above creates the private `/var/lib/factory/curl-headers` authorization header file and sets `base_url`; set `job_id` to the inspected job ID in that same shell. The endpoint has no request body and does not accept caller-selected provider identity:

```sh
job_id='<inspected-failed-job-id>'
curl --fail-with-body --silent --show-error --write-out '\nHTTP %{http_code}\n' \
  --request POST \
  --header @/var/lib/factory/curl-headers \
  "$base_url/v1/jobs/$job_id/reconcile"
```

A successful reconciliation returns HTTP `200` with the job snapshot and confirmed provider/PR outcome. Repeating reconciliation after success returns the same snapshot without another provider lookup or write. An ineligible job or unconfirmed/unsafe provider state returns a sanitized conflict and remains unchanged (`running` for interrupted work, `failed` for an already-terminal job); investigate rather than retrying blindly. Keep the header file private and remove it when finished.

Process-level regression tests use fake providers and verify abrupt interruption, authenticated bodyless recovery actions, restart durability, retained history/evidence/workspaces, no harness replay, no repeated provider create/attempt, and read-only lookup only. A separate real server-process per-job cancellation test verifies queued work is never executed, running cancellation waits for cooperative executor exit, only the selected job is canceled, and canceled workspaces remain retained without provider writes. Mismatched identity, provider outage, and SQLite write lock leave interrupted work unresolved; a no-attempt job can receive only a failed disposition. Separately, deterministic tests exercise the production GitHub REST adapter against `httptest` endpoints using GitHub-shaped requests and responses: an accepted create with a dropped response, unavailable lookup, subsequent exact-match read-only reconciliation, token/error redaction, and no duplicate provider write. These adapter tests require no credentials and make no external writes. Together, these tests do not validate a live GitHub account; live account/repository tests remain deferred and are not required. A separate restart test abruptly stops a child server with one running and one queued job: restart resumes the queued job to success, leaves the running job inspectable without replay, and reports it through authenticated `recovery_needed` status. It confirms one harness execution for the interrupted job and one provider publication for the resumed job. Automatic reconciliation of interrupted running work also remains unimplemented.

## Limits and trusted boundary

Configure worker count, queue capacity, retained records, request/task sizes, history, execution timeout, and workspace bounds for the host's capacity. Queue saturation rejects new work; retry only after capacity becomes available, using the original idempotency key.

The service runs its configured local workflow with the server account's authority. A process, worktree, Nix shell, or container is not a security sandbox. Keep API-key holders and configured repositories trusted; protect provider credentials and state files; use loopback by default. Before non-loopback exposure, put the service behind operator-managed network controls and TLS/authentication appropriate to the environment.

## Backup and restore

### SQLite-only online backup

Create a consistent online backup while the server is running. This operation contains SQLite only; it does not snapshot the separate retained workspace tree. The destination directory must already exist and be private (owner-only permissions). Do not use the configured database path as the destination.

```sh
factory server backup --config /absolute/path/to/server.json --destination /secure/backup/path/jobs.db
```

The backup file is written with owner-only permissions. Restore by placing a copy at a new private database path, updating `persistence.path` in a disposable config, and starting the server with that config. Verify health/readiness and inspect representative job status/history. Do not point a live server at the source backup.

The process-level regression test exercises this command against a live temporary SQLite server using only synthetic requests and no provider configuration or credentials. It restores a copied backup under a separate private path and starts a second server; assertions cover terminal status, verification evidence, lifecycle history, idempotent replay, queued-job resumption, and non-replay of an interrupted running job. It also checks backup/restore file modes, CLI rejection of the live source and a non-private destination directory, and sanitized startup failure for corrupt SQLite content. Run the reproducible test with:

```sh
go test ./internal/restserver/runtime -run '^TestRESTServerBackupRestoreProcessE2E$' -count=1
```

This is automated disposable test evidence, not a claim that a production or deployed-service restore drill has been performed.

### Portable recovery bundle

A bundle pairs SQLite with the retained per-job result directories found for jobs in the database snapshot. It is a stopped-server snapshot: stop the REST server cleanly first, then run `factory server bundle create`. The command independently acquires the same database ownership locks as the server and fails closed if an owner is active or lock state cannot be verified. Stop all other processes that can modify the configured results tree as well; Factory does not coordinate a live workspace snapshot. SQLite consistency is checked with `PRAGMA integrity_check`.

The configured results base is selected from the same `XDG_STATE_HOME` (or `~/.local/state`) used by the service and includes only hashed configured repository directories and result directories corresponding to persisted job records. Retained workspace trees are copied as filesystem artifacts; `.git` worktree administrative links and repository-specific registration metadata are not made portable by this format. Unrelated paths, config, credentials, request bodies, provider identity, logs outside retained job result trees, and Git/provider server state are excluded. Per-job contents (worktree, state, output, workflow logs, and protected completion metadata when present) are included as opaque retained artifacts; the manifest contains only format version, job ID, configured repository-directory digest, status, and whether its workspace is present. Interrupted/failed/canceled jobs and their artifacts are not replayed, rewritten, or filtered out. Jobs whose artifacts were already cleaned or are absent remain represented with `workspace=false`; eligible provider reconciliation still requires a usable retained workspace and configured provider after restore.

The destination must be a new path in an existing private directory, outside both the SQLite source and results tree. The single gzip/tar bundle is written to a private temporary file, synced, and atomically published without replacing an existing destination. Restore first validates archive structure and bounded manifest references, safe relative paths, private-file permission bits, workspace containment, and SQLite integrity. It then extracts into staging and atomically publishes to a new destination under an existing private parent. For example:

```sh
factory server bundle create --config /absolute/path/to/server.json --destination /secure/backup/factory-recovery.tar.gz
factory server bundle verify --source /secure/backup/factory-recovery.tar.gz
factory server bundle inspect --source /secure/backup/factory-recovery.tar.gz
factory server bundle restore --source /secure/backup/factory-recovery.tar.gz --destination /secure/restore/new-server
```

`inspect` prints only job IDs, statuses, opaque repository-directory digests, and workspace-presence flags; it does not print request text, secrets, or host paths. Restored files are arranged beneath the new destination at `factory/rest-server/jobs.db` and `factory/rest-server/<repository-digest>/results/<job-id>/...`. To use the database, point a disposable server config at that restored SQLite path and configure its workspace/results state base to the extracted tree as appropriate; do not point a live server at or overwrite the configured production state/results directory. This portability mechanism does not rewrite Git worktree administrative metadata to register retained worktrees against another checkout. The restored artifacts remain available for inspection; provider reconciliation may reject them if the configured checkout/worktree identity checks do not pass. Never treat copying those directories as a guarantee of cross-host executable reconciliation.

The bundle CLI process test builds synthetic failed and interrupted jobs, moves the single bundle, verifies and inspects it, restores it under a distinct private path, and checks durable history and retained artifacts. Unit tests cover active ownership, unsafe destinations and paths, permissions, corruption, traversal, and failed creation leaving no published partial bundle. The command tests use a stopped synthetic SQLite store plus synthetic workspace directories; the result trees are not live Git worktrees or provider credentials. These are deterministic synthetic checks, not a production restore drill. A production restore drill must separately use an approved disposable target and verify its configured repository/worktree assumptions, service startup, representative inspection, and operational procedures; do not use production credentials or repositories for automated tests.

## Supervised single-host deployment

For a dedicated service-user unit with the installed binary, shutdown/start-rate behavior, readiness checks, and operational commands, follow the [systemd deployment recipe](rest-server-systemd.md). Keep this service as the only process owning its SQLite database; backup commands do not require starting a second server.

## Clean setup and first client request

This walkthrough starts with a trusted local checkout and an installed harness executable. Use a dedicated service account where practical: the server runs its configured workflow with that account's local authority, and neither the process nor a container is a security sandbox. Keep the repository, harness, config, API key, and any provider credentials operator-controlled.

1. Prepare an exact Git working-tree root for the configured repository alias. The server rejects paths that are not the canonical checkout root. Install the configured harness and make sure it can run non-interactively as the service account.
2. Create private state and secret directories, then generate a fresh shared API key. These commands print no credential:

   ```sh
   umask 077
   install -d -m 700 /var/lib/factory /var/lib/factory/secrets
   install -m 600 /dev/null /var/lib/factory/secrets/api-key
   openssl rand -hex 32 > /var/lib/factory/secrets/api-key
   chmod 600 /var/lib/factory/secrets/api-key
   ```

   If enabling GitHub PR publication, provision a least-privilege GitHub token into `/var/lib/factory/secrets/github-token` using the operator's secret manager. Do not put the token value in JSON, shell arguments, repository remote URLs, or task text. Both credential files must be private regular files owned by the effective service user; restart the server after rotating them.
3. Create `/var/lib/factory/server.json` using the complete [server config example](rest-server-config.md) as a template. Replace the repository checkout, harness executable, API-key path, SQLite path, and (if enabled) GitHub token path and repository mapping with operator-selected values. `/var/lib/factory` is private for the SQLite file and config. Keep the listener at its loopback default unless operator-managed network controls are in place. With `umask 077` still set, create and edit the config file:

   ```sh
   $EDITOR /var/lib/factory/server.json
   ```

   The JSON uses fixed executable/argv values, including `{system_prompt}` and `{task}` exactly once each; do not replace these placeholders with shell text. The example's verification commands are trusted operator settings, not caller-supplied checks. Validate the complete schema and startup dependencies by launching the server; invalid or unavailable settings fail before it accepts jobs, with sanitized diagnostics.
4. Start the service as the account that owns the secret files and repository. Keep it running in this terminal or under the operator's process supervisor:

   ```sh
   factory server --config /var/lib/factory/server.json
   ```

5. In another terminal, confirm liveness and readiness. Build a private curl header file without displaying the key, then submit one bounded request with a stable idempotency key:

   ```sh
   umask 077
   { printf 'Authorization: Bearer '; tr -d '\r\n' < /var/lib/factory/secrets/api-key; printf '\n'; } > /var/lib/factory/curl-headers
   chmod 600 /var/lib/factory/curl-headers
   base_url=http://127.0.0.1:8080

   curl --fail-with-body --silent --show-error "$base_url/healthz"
   curl --fail-with-body --silent --show-error "$base_url/readyz"
   curl --fail-with-body --silent --show-error --write-out '\nHTTP %{http_code}\n' \
     --header @/var/lib/factory/curl-headers \
     -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: operator-smoke-001' \
     -d '{"repository":"widget","task":"Review the configured repository and summarize the current test status."}' \
     "$base_url/v1/jobs"
   ```

   A successful submission returns HTTP `202` with a job ID and inspection links; this confirms admission, not completion. Copy the returned ID into `job_id`, then use these authenticated requests to inspect the job and its bounded history:

   ```sh
   job_id='<job-id-from-response>'
   curl --fail-with-body --silent --show-error --header @/var/lib/factory/curl-headers "$base_url/v1/jobs/$job_id"
   curl --fail-with-body --silent --show-error --header @/var/lib/factory/curl-headers "$base_url/v1/jobs/$job_id/history"
   ```

   A terminal `succeeded` status is the completed-job result. When GitHub is configured, it includes the confirmed PR outcome; without a provider, the current server does not publish a PR. If a provider outcome is uncertain, inspect the failed job and follow the [operator reconciliation recipe](#operator-reconciliation-recipe) when it meets the SQLite eligibility checks; retrying submission with the same idempotency key only returns the existing record. Remove the temporary header file when finished and never print or log it.

For durable deployments, also verify status/history after a clean restart and perform the [backup and restore drill](#backup-and-restore) using a disposable database copy. Never use production credentials or repositories for recovery tests.

This guide describes behavior available after the REST SQLite and provider changes merged. It does not claim fleet coordination, automatic recovery/replay of interrupted running work, or merge/release/deploy behavior.

For the setup example, see [REST server configuration](rest-server-config.md).
