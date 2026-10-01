# Proposed REST server execution modes

**Future direction, not implemented.** The proposed Factory Server is a single process that accepts task jobs from HTTP clients (including clients such as Hermes). Server responsibilities include admission, bounded concurrency, job IDs and status/history, and applying operator policy. An execution backend only runs an admitted job; it does not own the API or job registry. The [REST server contract](rest-api-contract.md) remains the canonical source for request, authentication, and job semantics.

## Mode selection and backend boundary

Execution mode is selected only by operator configuration. Clients cannot choose a mode, executable, or command. Keep server/job semantics independent of that choice through a minimal server-side executor boundary, conceptually:

```go
Execute(ctx context.Context, job Job) (Result, error)
```

The server supplies the admitted job and cancellation context; the selected executor runs it and reports its outcome. The executor is not a general-purpose caller-controlled command runner.

The MVP uses a local-process executor: one configured subprocess per job, with a distinct isolated workspace and session for each job. The server runs with bounded concurrency, and jobs use the same API and status semantics regardless of backend. Docker Compose runs the Factory Server for local testing and defaults to this local-process executor; it is neither Docker-per-job execution nor a security sandbox.

Future alternatives may run one ephemeral Docker container or one Kubernetes Pod per job. These are possible backends, not part of the MVP, and must retain per-job isolation and the same externally visible job semantics.

## State and lifecycle boundaries

Job status and history are process-memory-only in the MVP. PostgreSQL and durable job recovery are deferred. Graceful shutdown stops admission, cancels active work, and cleans up a job's workspace only when safe. If a Git push or PR result is uncertain, preserve workspace/evidence for reconciliation rather than risking evidence loss or repeating an external side effect. Concurrency and resource bounds are operator configuration.

Execution does not authorize merge, release, or deployment; those actions remain human-only. Neither local subprocesses nor Compose should be described as a sandbox.
