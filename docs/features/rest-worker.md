# REST worker process

`factory worker --config /absolute/path/to/worker.json` starts a worker-only process for a PostgreSQL-backed REST job service. The process connects to the configured shared job store, claims jobs through fenced PostgreSQL leases, constructs trusted workspaces and the configured Factory workflow, and runs without an HTTP listener. It does not load `api_key_file` or named caller `principals`. The optional `factory-worker:local` container image is built alongside `factory-controller:local` from the same executable; it runs as a non-root user, includes Git, and defaults to `/factory worker --config /etc/factory/worker.json`. Its image-role guard is an operational guardrail, not a security boundary. The local Compose setup uses a workers-disabled API by default, so jobs remain queued in that mode. The opt-in `worker` profile starts this image against the same PostgreSQL store and can execute admitted jobs.

A worker config is the same strict server JSON configuration used by `factory server`, with these distinctions:

- `persistence.backend` must be `postgres` and `persistence.dsn` must point to the same database as the API control plane.
- `api_key_file` and `principals` may be omitted. The config must still include trusted repository roots, harness argv, limits, and optional verification/provider settings.
- `workers_enabled: false` on `factory server` leaves the API available while preventing local claim/execution. Its default remains enabled, preserving all-in-one behavior. In this topology the API cannot perform `POST /v1/jobs/{id}/reconcile`: retained workspaces and the provider reconciliation executor exist only in worker processes, which do not expose that callback. Shared storage alone does not supply the missing executor. Treat reconciliation as unavailable; investigate the provider and retain job/workspace artifacts rather than attempting remote or unsafe reconciliation.
- `limits.workers` is the concurrent execution cap enforced globally across all processes sharing one PostgreSQL database, not a per-process allowance or fleet-wide scheduler. All server configurations that enable local workers and every `factory worker` configuration sharing that database must use the same `limits.workers` value; mismatched values make the effective cap dependent on which worker claims. A workers-disabled API control plane must use the same value too, even though it does not execute jobs. Memory and SQLite caps are process-local (SQLite is single-server storage). PostgreSQL fencing/capacity checks enforce this shared cap, but no fleet coordinator or workflow checkpointing is provided.
- `worker_id` is an optional bounded observable label. If omitted, the process derives an identity from its PID. Each worker process generates a fresh boot identity. These values are metadata, not credentials.

Protect worker configs: the DSN and repository/harness/provider settings are operator-trusted. Provider credentials are read by the worker when configured. API caller credentials are neither required nor read. Worker processes execute configured harness and repository content with their local account authority; they are not a security sandbox.

Authenticated job detail and history expose only the safe worker/boot labels and the latest bounded stage/state summary (`requirements`, `implement`, `review`, or `document`; `started`, `completed`, or `failed`). The summary is dedicated metadata, separate from lifecycle history events. The SSE stream emits an allowlisted `workflow_progress` control event with only the stage and state. Raw logs, commands, and private observer messages are not included. Stage detail is retained as bounded PostgreSQL/SQLite metadata and does not create lifecycle history records.

A worker shutdown stops claims, cancels active executions cooperatively, and waits up to the standard shutdown deadline. A process interruption may leave a running job requiring operator investigation; running work is not automatically replayed. Run API and worker processes under appropriately restricted service accounts and network controls.

Example:

```sh
factory server --config /etc/factory/control-plane.json
factory worker --config /etc/factory/worker.json
```

Set `workers_enabled` to `false` in the control-plane config. Keep the DSN and required trusted execution/provider settings aligned between configs. The worker command is supported only with PostgreSQL; memory and SQLite are not cross-process worker stores.
