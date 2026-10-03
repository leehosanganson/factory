# REST job API and lifecycle contract

## Status and product boundary

This is the target contract for Factory's single REST job-service MVP. Factory has a local REST server with authenticated bounded admission, selectable volatile-memory or SQLite persistence, isolated workspaces, the configured multi-stage workflow, and optional GitHub pull-request publication. Current implementation details are in the [REST server feature guide](../features/rest-server.md), [configuration guide](../features/rest-server-config.md), and [operations guide](../features/rest-server-operations.md). The contract below captures the full target; requirements not yet implemented or verified remain proposals.

The product path is one service and one lifecycle:

**HTTP request → job admission → execution → verification → create/update PR → inspectable result**

Opening or updating a PR through the configured code-repository provider is required for a successful implementation job. Merge, release, and deployment are out of scope. Agent success alone is not a correctness verdict.

## Persistence modes

- **Memory backend:** the simple local-development and test mode. Jobs, history, idempotency keys, and execution state are volatile and lost on process restart. The service must disclose this mode and must not imply durable acceptance.
- **SQL backend:** optional and recommended for real deployments. It persists accepted jobs, idempotency, lifecycle/history, and provider-side-effect references sufficiently to recover or clearly classify interrupted work after restart. Schema/migrations, transactions, retention, concurrency, and recovery behavior must be explicit. The current single-server implementation does not persist worker leases or execution checkpoints; it does not promise resumption within a running workflow or multi-host ownership/fencing.
- When SQL is explicitly configured and unavailable or unhealthy, admission fails closed; the server must not silently fall back to memory. Deployments requiring restart durability must use SQL or another explicitly supported durable backend.

Both backends implement the same API and lifecycle contract. Tests should exercise the shared behavioral suite against memory and SQL. Recovery disposition is conservative: queued jobs have not been claimed and may resume; jobs found running at restart require operator reconciliation and must not be automatically replayed because workflow/provider side effects may have occurred; terminal jobs are retained for inspection; unknown states require operator reconciliation. This classification is not a durable worker-ownership or checkpoint mechanism. SQL acceptance requires process-restart tests, duplicate/concurrent admission tests, and interrupted-side-effect recovery tests.

## Request and API

The current route shape is the target unless a reviewed contract change supersedes it:

- `GET /healthz` and `GET /readyz` are minimal unauthenticated probes.
- `POST /v1/jobs` submits a bounded request for a configured repository alias and task; a positive issue number may be optional context, not a separate issue-polling workflow.
- `GET /v1/jobs/{id}` returns the job status.
- `GET /v1/jobs/{id}/history` returns bounded lifecycle events and a truncation indicator.
- `GET /v1/operations` returns bearer-authenticated aggregate counts and capacity indicators only; it never returns job IDs, request payloads, events, provider details, or credentials.

All job routes require bearer authentication. Requests cannot select arbitrary filesystem paths, executables, shell text, or verification commands. The server owns repository/provider configuration and enforces request, task, concurrency, time, history, and output bounds. Preserve strict JSON validation, safe generic errors, secret/path redaction, exact route/method behavior, and the current loopback listener default.

Use a client-supplied `Idempotency-Key`. An identical retry maps to the same job; reuse with a different normalized payload conflicts. In memory mode this guarantee lasts only while the process and record remain. In SQL mode the key's scope and retention period must be durable and documented; never evict or expire keys in a way that permits duplicate active work or unsafe duplicate PRs.

## Job lifecycle and provider operations

A job is not successful until Factory has:

1. admitted and recorded the request according to the selected persistence mode;
2. executed the configured requirements/implementation/review/documentation workflow in a job-specific workspace;
3. run configured verification checks and recorded results/limitations;
4. created or updated the job's PR through the configured code-repository provider; and
5. persisted the confirmed provider outcome and PR identity/URL.

The repository-provider interface is independent of issue-tracker integrations. The initial provider choice is configuration, not embedded in the canonical job model. Provider implementations must support scoped repository identity, branch and PR lookup/create/update, and enough stable identifiers to reconcile retries. Credentials are operator-configured, least-privilege, excluded from task prompts, records, logs, and API responses, and passed only to the operations that need them.

Treat timeouts, disconnects, and restarts around branch/PR writes as uncertain outcomes. Reconcile live provider state against the durable job/branch/PR identity before retrying. Do not blindly create another branch or PR. If state cannot be resolved safely, preserve the workspace and expose a recoverable/needs-operator outcome; do not report success.

Cancellation is cooperative and must record which external operation may have completed. A cancellation or issue closure must not silently delete workspaces or close/merge a PR. Ambiguous or over-policy scope pauses for human direction. Factory never merges, releases, or deploys.

## Security and trust

The service accepts work that executes agent and repository-controlled content with the server account's local-process authority. A local process, Nix shell, or container is not a security sandbox. Define the trust boundary for API-key holders, repository allowlists, provider credentials, network exposure, and untrusted issue/repository/PR content. The API key must not be passed to the harness. Keep external text and generated output as untrusted data; it cannot override user scope, policy, or credential boundaries.

Expose sanitized status/history rather than raw agent transcripts or host paths. Bound subprocess output while continuing to drain child pipes after the capture limit. Do not describe limits as RSS guarantees unless actually enforced at the OS/container level.

## Current runtime and implementation gates

The `factory server` supports memory mode; SQLite persistence and the GitHub provider are configuration-gated features. A provider-configured job is successful only after its verified branch is pushed, the matching PR is created/updated, and that outcome is recorded in the selected store. See [REST job server](../features/rest-server.md) and [server configuration](../features/rest-server-config.md) for shipped behavior.

Delivery gates for the target MVP:

1. Define persistence interfaces and durable lifecycle semantics without changing the existing memory-mode API contract. (Implemented.)
2. Add optional SQLite persistence and migrations; fail closed when configured storage is unavailable; prove restart and concurrency behavior. (Implemented; SQLite concurrent same-key admission across independent store connections is tested.)
3. Define/configure the code-repository provider interface and credential boundary. (Implemented.)
4. Implement idempotent branch/PR reconciliation, create/update, and durable recording of the confirmed result. (Implemented; see the server configuration and operations guides.)
5. Exercise end-to-end request-to-PR behavior including concurrent admission, process restart, duplicate requests, workflow/provider failure, cancellation, and durable outcome checks. (Core request-to-PR and concurrency path implemented; SQLite cross-connection concurrent admission, process-level shutdown cancellation, and a fake-provider timeout/restart/reconciliation boundary are tested; automatic recovery and live-provider uncertain-write fault injection remain.)
6. Document memory-mode volatility, SQLite setup/recovery, backup/retention, security boundaries, and provider configuration. (Configuration and operations guides available; backup/restore drill and count-only aggregate operational status are implemented.)

These are implementation gates, not claims of release dates. Completed steps describe merged behavior; remaining details above identify work not yet implemented or verified. Issue polling/reconciliation, provider events, additional hosting/fleet layers, UI, merge, release, and deployment are not prerequisites for this MVP. If issue intake is added later, it feeds the same REST job lifecycle rather than creating a second autonomous issue-to-PR product.

## REST configuration reference

[The server configuration guide](../features/rest-server-config.md) describes the implemented schema and [operations guide](../features/rest-server-operations.md) covers current readiness and recovery guidance. SQL connection settings and provider credential mechanisms beyond the configured GitHub token-file flow are outside the current implementation.