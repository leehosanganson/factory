# Proposed REST server execution modes

**Future direction, not implemented.** The proposed Factory Server is a single process that accepts task jobs from HTTP clients (including clients such as Hermes). Server responsibilities include admission, bounded concurrency, job IDs and status/history, and applying operator policy. An execution backend only runs an admitted job; it does not own the API or job registry. The [REST server contract](rest-api-contract.md) remains the canonical source for request, authentication, and job semantics.

## Mode selection and backend boundary

Execution mode is selected only by operator configuration. Clients cannot choose a mode, executable, or command. Keep server/job semantics independent of that choice through a minimal server-side executor boundary, conceptually:

```go
Execute(ctx context.Context, job Job) (Result, error)
```

The server supplies the admitted job and cancellation context; the selected executor runs it and reports its outcome. The executor is not a general-purpose caller-controlled command runner.

The proposed MVP uses a local-process executor for the existing multi-stage Factory requirements/implementation/review/documentation workflow and configured verification checks. A job gets a distinct workspace and session and may start multiple configured harness subprocesses across its stages; one job-wide deadline and aggregate output limit bound that work. Every subprocess runs with the configured server account's local-process authority. Every shared API-key holder is trusted with that authority; this is not a sandbox and makes no claim of isolation against malicious callers. Successful workspaces/results remain available for human local inspection, then become eligible for startup/periodic cleanup only after protected completion metadata in the workspace parent establishes an age greater than 24 hours. Cleanup removes only well-formed recognized successes and rejects symlink/path escapes and safe-races; failed, canceled, unknown, orphaned, unparseable, or unsafe directories remain for operator inspection. If external completion metadata is lost or corrupt, cleanup fails closed and the successful result may remain until operator action; sweeps can run after the 24-hour threshold, not exactly at it. Docker Compose runs the single Factory Server with its local executor; it is not Docker-per-job execution or a security sandbox.

Future alternatives may run one ephemeral Docker container or one Kubernetes Pod per job. These are not part of the MVP, are selected only through server-operator configuration, and must retain the same API/job contract. They do not by themselves establish isolation from malicious API-key holders.

## State and lifecycle boundaries

Job status and history are process-memory-only in the MVP. PostgreSQL and durable job recovery are deferred. Clients resubmit after process restart; successful result workspaces may remain for local inspection, but completion metadata does not restore job status/history or resume execution. External side effects must not be blindly retried. Branch push and PR create/update are forbidden in the MVP; future publication requires a separately designed, restart-safe, cross-process reconciliation gate for uncertain outcomes. Graceful shutdown stops admission and cancels active work. Retain ambiguous workspaces/evidence for operator inspection; only validated successful results older than 24 hours are eligible for automatic cleanup as described above. Concurrency and resource bounds are operator configuration.

Merge, release, and deployment are unavailable through the proposed server. Neither local subprocesses nor Compose should be described as a sandbox.
