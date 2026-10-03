# REST server operations

This guide applies to the single-server REST job service. See [REST server setup and configuration](rest-server-config.md) for the current schema, [REST job server](rest-server.md) for shipped limits and trust boundaries, and the [REST API contract](../roadmap/rest-api-contract.md) for the target lifecycle.

## Health and readiness

- `GET /healthz` is a liveness probe. It returns `200` while the HTTP process can answer; it does not promise that job admission can succeed.
- `GET /readyz` is an unauthenticated readiness probe. It returns `200` only after startup initialization and successful health checks of configured dependencies. During startup, store/provider failure, or shutdown it returns `503`.
- Job API routes check readiness before processing requests. A `503 not_ready` response means the request was not admitted; retry with the same `Idempotency-Key` after readiness returns.

Keep probes on a trusted network boundary. They expose only bounded status—not credentials, filesystem paths, job contents, or provider diagnostics.

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

The response contains no job IDs, task or repository values, event history, provider data, or credentials. `recovery_needed` counts SQLite jobs found running when this store instance opened; these may have external side effects and require operator reconciliation. It is always zero for the volatile memory store, which starts empty. This endpoint is a snapshot for operational visibility, not a health/readiness probe or a correctness verdict. A store query error returns a generic internal error without storage details.

## Persistence and backup

Memory mode is volatile: jobs, idempotency keys, and history are lost on process restart. Use it for local development and tests, not as a durable queue.

With SQLite configured, the database file is the durable job store. Prefer SQLite's online backup API for a live service. A filesystem snapshot is safe only after a clean shutdown; ensure it captures a consistent database state rather than copying only the main database while WAL writes are active. Store backups in access-controlled, encrypted storage and periodically verify restore by reopening a copy in a disposable directory. Do not copy a live database file alone while writes may be in progress.

SQLite retention is bounded by the configured record limit. When the retained-record cap is reached, new admissions fail rather than evicting terminal history or idempotency records. Inspect capacity and plan backup/archive operations before the cap is reached.

## Restart and interrupted work

On restart, queued records may be resumed because no worker claimed them. Terminal records remain available for inspection. A record that was running at interruption is classified as needing operator reconciliation; it is not replayed automatically because the workflow or provider write may have partially completed.

Inspect job status and bounded history before taking action. For an uncertain provider operation, compare the durable job identity/branch with the configured provider's live PR state before retrying. Preserve the workspace and record until the outcome is resolved. Do not manually delete a running workspace or blindly resubmit with a new idempotency key.

## Limits and trusted boundary

Configure worker count, queue capacity, retained records, request/task sizes, history, execution timeout, and workspace bounds for the host's capacity. Queue saturation rejects new work; retry only after capacity becomes available, using the original idempotency key.

The service runs its configured local workflow with the server account's authority. A process, worktree, Nix shell, or container is not a security sandbox. Keep API-key holders and configured repositories trusted; protect provider credentials and state files; use loopback by default. Before non-loopback exposure, put the service behind operator-managed network controls and TLS/authentication appropriate to the environment.

## Backup and restore

Create a consistent online backup while the server is running. The destination directory must already exist and be private (owner-only permissions). Do not use the configured database path as the destination.

```sh
factory server backup --config /absolute/path/to/server.json --destination /secure/backup/path/jobs.db
```

The backup file is written with owner-only permissions. Restore by placing a copy at a new private database path, updating `persistence.path` in a disposable config, and starting the server with that config. Verify health/readiness and inspect representative job status/history. Do not point a live server at the source backup.

## First request and recovery drill

After creating the placeholder-only [server config](rest-server-config.md), start the service:

```sh
factory server --config /absolute/path/to/server.json
```

For SQLite recovery, create an online backup without starting the server:

```sh
factory server backup --config /absolute/path/to/server.json --destination /secure/backup/path/jobs.db
```

In another terminal, confirm probes and submit a bounded job. Create a mode-`0600` curl header file containing `Authorization: Bearer` followed by the protected API key. Keep this file outside the repository, avoid logging it, and remove it when finished.

```sh
curl --fail-with-body --silent --show-error http://127.0.0.1:8080/healthz
curl --fail-with-body --silent --show-error http://127.0.0.1:8080/readyz
curl --fail-with-body --silent --show-error \\
  --header @/secure/path/factory-curl-headers \\
  -H 'Content-Type: application/json' \\
  -H 'Idempotency-Key: operator-smoke-001' \\
  -d '{"repository":"widget","task":"Review the configured repository and summarize the current test status."}' \\
  http://127.0.0.1:8080/v1/jobs
```

Record the returned job `id`, then inspect it using `GET /v1/jobs/{id}` and `GET /v1/jobs/{id}/history`, with the same bearer header. Retry an uncertain client submission with the same idempotency key, not a new one. For durable deployments, verify status/history after a clean restart and test backup restoration against a disposable copy. Never use production credentials or repositories for recovery tests.

This guide describes behavior available after the REST SQLite and provider changes merged. It does not claim fleet coordination, automatic recovery of interrupted running work, or merge/release/deploy behavior.

For the setup example, see [REST server configuration](rest-server-config.md).
