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

The response contains no job IDs, task or repository values, event history, provider data, or credentials. `recovery_needed` counts SQLite jobs found running when this store instance opened; they may have external side effects and require manual operator investigation. Startup does not replay them, and they are not eligible for the endpoint that reconciles terminal failed jobs. It is always zero for the volatile memory store, which starts empty. This endpoint is a snapshot for operational visibility, not a health/readiness probe or a correctness verdict. A store query error returns a generic internal error without storage details.

## Persistence and backup

Memory mode is volatile: jobs, idempotency keys, and history are lost on process restart. Use it for local development and tests, not as a durable queue.

With SQLite configured, the database file is the durable job store. Prefer SQLite's online backup API for a live service. A filesystem snapshot is safe only after a clean shutdown; ensure it captures a consistent database state rather than copying only the main database while WAL writes are active. Store backups in access-controlled, encrypted storage and periodically verify restore by reopening a copy in a disposable directory. Do not copy a live database file alone while writes may be in progress.

SQLite retention is bounded by the configured record limit. When the retained-record cap is reached, new admissions fail rather than evicting terminal history or idempotency records. Inspect capacity and plan backup/archive operations before the cap is reached.

## Restart and interrupted work

On restart, queued records may be resumed because no worker claimed them. Terminal records remain available for inspection. A record still marked `running` at interruption is classified as needing operator investigation; it is not replayed automatically because the workflow or provider write may have partially completed. This startup classification is distinct from the explicit reconciliation endpoint below: that endpoint accepts only terminal `failed` jobs and cannot resolve a still-running startup interruption. Inspect status/history and investigate such running work manually; do not assume it is eligible for endpoint reconciliation.

Inspect job status and bounded history before taking action. For an uncertain provider operation, compare the durable job identity, repository, and branch with the configured provider's live PR state before acting. Preserve the workspace and record until the outcome is resolved. Do not manually delete a running workspace or blindly resubmit with a new idempotency key.

A failed job cannot be resumed or changed through ordinary job submission. The original idempotency key returns the same record; a new key can admit duplicate work and is not a retry mechanism. For a SQLite-backed job, an explicit authenticated `POST /v1/jobs/{id}/reconcile` action is available only for a terminal failed job whose provider attempt identity (provider/repository/branch/commit) and retained workspace are durably recorded. The attempt need not be marked uncertain: a confirmed provider write can succeed even when recording its outcome fails. Memory mode, running jobs, and jobs without a recorded provider attempt are ineligible. Before invoking it, inspect the job's status/history and confirm the retained workspace is present. The action checks the configured repository checkout, verifies that the retained worktree is on the exact persisted branch at the exact persisted commit, then performs only a read-only lookup through the configured provider. It accepts exactly one open PR with matching configured repository, job-specific branch, commit, base branch, and Factory job identity. It does not run the harness or verification checks, push a branch, or create/update a PR. Missing, mismatched, multiple, malformed, or unavailable provider state fails closed: the job remains failed and its workspace/evidence is retained. On confirmation, Factory atomically persists the confirmed PR outcome, a `provider_reconciled` history event, and the transition to `succeeded` in SQLite. Repeating the request after success returns the same successful job without another provider write.

### Operator reconciliation recipe

Use this only after inspecting a terminal `failed` SQLite job and its history, confirming a recorded provider attempt (whether uncertain or not), and verifying its retained workspace and expected PR identity. The walkthrough above creates the private `/var/lib/factory/curl-headers` authorization header file and sets `base_url`; set `job_id` to the inspected job ID in that same shell. The endpoint has no request body and does not accept caller-selected provider identity:

```sh
job_id='<inspected-failed-job-id>'
curl --fail-with-body --silent --show-error --write-out '\nHTTP %{http_code}\n' \
  --request POST \
  --header @/var/lib/factory/curl-headers \
  "$base_url/v1/jobs/$job_id/reconcile"
```

A confirmed match returns HTTP `200` with the same job now `succeeded` and its confirmed provider/PR outcome. Repeating the request is idempotent. An ineligible job or any unconfirmed/unsafe provider state returns a conflict and remains failed; inspect status/history and evidence, then investigate manually rather than retrying blindly. Keep the header file private and remove it when finished.

Process-level regression tests use a fake provider that records one accepted PR create and then either returns an uncertain lost-response error or confirms success while SQLite is locked against outcome persistence. The failed job has no provider outcome, retains its workspace and durable identity, and survives restart without startup replay. The test then calls the authenticated endpoint on that terminal failed job; confirmation of the existing PR durably succeeds the same job with one reconciliation history event while create and harness counts remain unchanged. Repeats are idempotent. Fake-provider mismatch, absent/multiple match, malformed state, and outage scenarios fail closed. Separately, deterministic tests exercise the production GitHub REST adapter against `httptest` endpoints using GitHub-shaped requests and responses: an accepted create with a dropped response, unavailable lookup, subsequent exact-match read-only reconciliation, token/error redaction, and no duplicate provider write. These adapter tests require no credentials and make no external writes. A separate restart test abruptly stops a child server with one running and one queued job: restart resumes the queued job to success, leaves the running job inspectable without replay, and reports it through authenticated `recovery_needed` status. It confirms one harness execution for the interrupted job and one provider publication for the resumed job. These tests do not validate a live GitHub account; live account/repository validation is deferred and is not required. Automatic reconciliation of interrupted running work also remains unimplemented.

## Limits and trusted boundary

Configure worker count, queue capacity, retained records, request/task sizes, history, execution timeout, and workspace bounds for the host's capacity. Queue saturation rejects new work; retry only after capacity becomes available, using the original idempotency key.

The service runs its configured local workflow with the server account's authority. A process, worktree, Nix shell, or container is not a security sandbox. Keep API-key holders and configured repositories trusted; protect provider credentials and state files; use loopback by default. Before non-loopback exposure, put the service behind operator-managed network controls and TLS/authentication appropriate to the environment.

## Backup and restore

Create a consistent online backup while the server is running. The destination directory must already exist and be private (owner-only permissions). Do not use the configured database path as the destination.

```sh
factory server backup --config /absolute/path/to/server.json --destination /secure/backup/path/jobs.db
```

The backup file is written with owner-only permissions. Restore by placing a copy at a new private database path, updating `persistence.path` in a disposable config, and starting the server with that config. Verify health/readiness and inspect representative job status/history. Do not point a live server at the source backup.

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

This guide describes behavior available after the REST SQLite and provider changes merged. It does not claim fleet coordination, automatic recovery of interrupted running work, or merge/release/deploy behavior.

For the setup example, see [REST server configuration](rest-server-config.md).
