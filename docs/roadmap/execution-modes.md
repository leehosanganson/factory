# REST server execution modes

This note describes the executor boundary for Factory's single REST job-service MVP. The [REST job contract](rest-api-contract.md) is canonical for job, persistence, provider, and human-boundary semantics.

## Server and executor boundary

The Factory server owns authentication, admission, idempotency, persistence, job status/history, policy, and cancellation. An executor runs only an admitted job through a server-side interface conceptually equivalent to:

```go
Execute(ctx context.Context, job Job) (Result, error)
```

Mode and executable selection are operator-configured. A request cannot choose a local command, executable, container image, or shell string. The initial executor uses local processes for the existing multi-stage requirements/implementation/review/documentation workflow and configured checks. Each job gets a distinct workspace/session, bounded time/output/resource use, and sanitized status/history.

## Persistence is independent of execution mode

The initial local-development mode uses the in-memory job backend. It is volatile: process exit loses job status, history, idempotency data, and knowledge of active execution. This mode must not claim restart durability.

Optional SQL persistence is recommended for real deployments requiring restart durability and recovery. The SQL backend must provide transactional admission/idempotency and durable lifecycle/ownership/checkpoint state. If explicitly configured storage is unavailable, the server fails closed rather than falling back to memory. Backend selection does not change the REST job contract.

## Required provider outcome

An implementation job must create or update its PR through the configured code-repository provider before reporting success. The provider is a separate boundary from the executor and any issue-tracker adapter. Record the confirmed PR identity and URL with the job outcome. Treat timeouts or interruptions around writes as uncertain; reconcile provider state before retrying and surface unresolved cases for operator action.

Credentials are configured by the operator, least-privilege, and available only to operations that need them. Do not pass API bearer credentials to the coding harness. Agent subprocesses, repository code, local execution, Nix shells, and containers are not security sandboxes.

## Later execution options

An operator may later choose an ephemeral container or Kubernetes Pod executor, provided it preserves the API/job contract and its isolation properties are explicitly threat-modeled. A container or Pod is not automatically a security boundary. Horizontal fleet scheduling, remote workers, and a separate control plane are outside the initial service MVP and depend on proven durable ownership and recovery semantics.

## Lifecycle constraints

- Bound admission, concurrency, per-job time, workspace use, and captured output; continue draining subprocess output after capture limits.
- Support cooperative cancellation and report any provider operation that may have completed while cancellation was in flight.
- Preserve failed, canceled, or uncertain workspaces/evidence for inspection according to explicit retention policy.
- Do not merge PRs, release, or deploy.
- Do not report job success before verification evidence and confirmed PR outcome are durably recorded when a durable backend is selected.

These are target requirements and design constraints, not claims that SQL persistence, provider writes, or container execution are currently implemented.