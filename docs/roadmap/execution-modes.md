# REST server execution modes

This note describes the executor boundary for Factory's single REST job-service MVP. The [REST job contract](rest-api-contract.md) is canonical for job, persistence, provider, and human-boundary semantics.

## Server and executor boundary

The Factory server owns authentication, admission, idempotency, persistence, job status/history, policy, and cancellation. An executor runs only an admitted job through a server-side interface conceptually equivalent to:

```go
Execute(ctx context.Context, job Job) (Result, error)
```

Mode and executable selection are operator-configured. A request cannot choose a local command, executable, container image, or shell string. The initial executor uses local processes for the existing multi-stage requirements/implementation/review/documentation workflow and configured checks. Each job gets a distinct workspace/session, bounded time/output/resource use, and sanitized status/history.

## Persistence is independent of execution mode

The server offers volatile in-memory state for local development and tests; process exit loses job status, history, idempotency data, and execution state.

Optional SQLite persistence is available for single-server deployments requiring restart durability. PostgreSQL persistence is also available when configured; a workers-disabled API control plane can share it with separately launched `factory worker` processes. All-in-one server workers remain the default, and there is no fleet coordinator. Both durable backends provide persisted job/history state; queued jobs are safe candidates for resumption, while jobs interrupted after entering the running state require operator investigation and are not automatically replayed. If explicitly configured storage is unavailable, the server fails closed rather than falling back to memory. Backend selection does not change the REST job contract. Durable distributed-worker recovery, stop proof, and multi-host failover/HA guarantees remain future work. See the [implemented REST server features](../features/README.md) and [PostgreSQL Store](../features/postgres-store.md) for current behavior.

## Required provider outcome

An implementation job must create or update its PR through the configured code-repository provider before reporting success. The provider is a separate boundary from the executor and any issue-tracker adapter. Record the confirmed PR identity and URL with the job outcome. Treat timeouts or interruptions around writes as uncertain; reconcile provider state before retrying and surface unresolved cases for operator action.

Credentials are configured by the operator, least-privilege, and available only to operations that need them. Do not pass API bearer credentials to the coding harness. Agent subprocesses, repository code, local execution, Nix shells, and containers are not security sandboxes.

## Later execution options

An operator may later choose an ephemeral container or Kubernetes Pod executor, provided it preserves the API/job contract and its isolation properties are explicitly threat-modeled. A container or Pod is not automatically a security boundary. PostgreSQL-backed separate workers and a workers-disabled API control plane are supported current behavior, but there is no fleet coordinator. Horizontal fleet scheduling and the distributed stop-proof/recovery semantics needed for safe automatic reassignment remain future work; see the [PostgreSQL ownership deployment contract](postgresql-ownership-deployment-contract.md).

## Lifecycle constraints

- Bound admission, concurrency, per-job time, workspace use, and captured output; continue draining subprocess output after capture limits.
- Support cooperative cancellation and report any provider operation that may have completed while cancellation was in flight.
- Preserve failed, canceled, or uncertain workspaces/evidence for inspection according to explicit retention policy.
- Do not merge PRs, release, or deploy.
- Do not report job success before verification evidence and confirmed PR outcome are durably recorded when a durable backend is selected.

Configured SQLite/PostgreSQL persistence and GitHub provider writes are implemented as documented in the [server configuration guide](../features/rest-server-config.md) and [implemented REST server features](../features/README.md). The isolated `PostgresOwnership` protocol fixture does not provide production failover/HA guarantees. Durable distributed-worker leases/checkpoints, automatic replay of interrupted running jobs, and container execution remain outside the current implementation.
