# REST server configuration

This file describes strict server JSON for the implemented `factory server` command and, with worker-mode validation, `factory worker`. Configuration is operator-selected, not caller-controlled. For `factory server`, the legacy `api_key_file` is required and remains an operator credential. Named `principals` configure additional bearer credentials by stable ID and role (`submitter` or `operator`); at least one named operator must be enabled. Every operator credential retains operator-wide job listing, inspection, operations, cancellation, and eligible recovery authority. Submitters may submit work and inspect/list only jobs attributed to their principal. A submitter has no access to operations or operator actions. Each verifier file is a private version-1 JSON digest file; raw bearer values are not stored in server JSON or the verifier. The legacy shared key maps to `legacy-shared` and remains operator-wide. A local process or container is not a security sandbox.

```json
{
  "mode": "local_process",
  "repositories": {
    "widget": "/srv/factory/checkouts/widget"
  },
  "harness": {
    "executable": "/usr/local/bin/pi",
    "args": [
      "-p",
      "--no-session",
      "--append-system-prompt",
      "{system_prompt}",
      "{task}"
    ]
  },
  "api_key_file": "/run/secrets/factory-api-key",
  "principals": [
    {
      "id": "alice",
      "role": "submitter",
      "token_file": "/run/secrets/factory-alice-verifier.json",
      "enabled": true
    },
    {
      "id": "factory-operator",
      "role": "operator",
      "token_file": "/run/secrets/factory-operator-verifier.json",
      "enabled": true
    }
  ],
  "persistence": {
    "backend": "sqlite",
    "path": "/var/lib/factory/jobs.db"
  },
  "web_ui": {
    "enabled": true,
    "operator_id": "factory-operator"
  },
  "provider": {
    "backend": "github",
    "token_file": "/run/secrets/github-token",
    "base_branch": "main",
    "repositories": {
      "widget": "acme/widget"
    }
  },
  "verification_checks": [
    ["go", "test", "./..."],
    ["go", "vet", "./..."]
  ],
  "parallel_subtasks": {
    "enabled": true,
    "max_concurrency": 4
  },
  "limits": {
    "request_body_bytes": 524288,
    "task_bytes": 262144,
    "queue_capacity": 32,
    "workers": 2,
    "max_records": 1000,
    "max_events_per_job": 200,
    "registry_bytes": 268435456,
    "job_timeout": "30m",
    "harness_output_bytes": 1048576
  }
}
```

For `factory server`, `workers_enabled` defaults to `true`; set it to `false` only for a PostgreSQL-backed API control plane intended to share the database with separately launched workers. `factory worker` uses the same strict JSON schema but requires `persistence.backend: "postgres"`; its config may omit `api_key_file` and `principals` because the worker opens no HTTP API. Keep trusted repository, harness, limits, provider, and PostgreSQL settings consistent across processes. In particular, every process sharing the database must use the same `limits.workers` value, which is the database-wide execution cap. Worker deployments and limitations are detailed in [REST worker process](rest-worker.md).

`web_ui` is optional and disabled by default. When enabled, `operator_id` must identify an enabled named operator; the UI is served on the same listener and uses only that named credential. HTTPS is required for its Secure session cookie. See [the embedded REST operator dashboard guide](rest-web-ui.md) for the narrow proxy routes, sessions, CSRF, and container image. `listen_address` defaults to `127.0.0.1:8080`; explicitly binding a non-loopback address exposes the API on that interface and requires suitable network controls. `repositories` maps a validated alias to an absolute checkout root; callers cannot supply paths. Before listening, the server verifies each configured path is an existing directory and the exact canonical Git working-tree root. Subdirectories, bare repositories, and symlink/path-indirected roots are rejected; linked worktree roots are accepted. Git must be installed. Validation errors identify the alias without echoing configured paths.

`harness` is a fixed executable and argv, never shell text. Arguments must contain `{task}` and `{system_prompt}` exactly once; other placeholders are rejected. `verification_checks` is an optional list of trusted operator-configured argv arrays invoked after workflow stages; callers cannot provide or change checks. Empty/omitted checks mean no external checks run. The strict schema bounds this list to 16 commands, 32 nonempty argv elements per command, and 16 KiB total argv bytes per command. Only `mode: local_process` is accepted.

A named principal's `token_file` points to a private verifier JSON file such as `{"version":1,"principals":[{"id":"alice","token_sha256":"<64 lowercase hex SHA-256 digest>"}]}`. The `id` must match the configured principal ID. Generate tokens with a cryptographically secure secret manager and store only their lowercase SHA-256 digests in this file; never put raw tokens in JSON, arguments, task text, or logs. Token files must be owned by the effective service user, regular, non-symlink files, owner-readable, and inaccessible to group/others. All named-token verification data is loaded at startup, and rotation/enablement changes require restart. Keep at least one separately held named operator credential enabled when configuring named principals. Do not disable the only operator or assume the shared legacy key is disabled by named-principal configuration.

`parallel_subtasks` is optional and disabled when omitted or when `enabled` is false. Set `enabled: true` to use Factory's existing planner-driven parallel implementation during the implementation stage of one REST job; it validates declared file scope and dependency waves, isolates workers, integrates successful changes, and rolls back integration on workflow failure. `max_concurrency` is optional (default 4) and, when specified, must be an integer from 1 through 8. It limits simultaneous subtasks within each job. This is separate from `limits.workers`, which limits simultaneous REST jobs; total subtask activity can therefore scale with both settings. Parallel execution is attempted only where the existing workflow considers the checkout eligible; otherwise the existing serial implementation path is used. Planner and agent output remain untrusted, and parallel workflow guards do not make local processes or containers a security sandbox. Only enable this for trusted repositories and harnesses.

The loader rejects unknown/duplicate JSON fields and trailing JSON. Use `factory server doctor --config /absolute/path/to/server.json` for a local-only, read-only preflight of server config, credential-file permissions, SQLite path metadata, configured Git checkout roots, and harness executable availability. It does not test provider reachability or PostgreSQL connectivity, open/create SQLite, start a listener, or run the harness. Its diagnostics name fields/categories only and omit configured paths and values. Start the service with `factory server --config /absolute/path/to/server.json`; no other argument ordering or config source is accepted. Resource limits are positive and bounded: request body 2 MiB, task 256 KiB, queue 1024, workers 64, retained records 1,000, events per job 200, logical registry budget 256 MiB, job timeout 24 hours, and captured output 16 MiB maximum. Defaults are 512 KiB body, 256 KiB task, queue 32, two workers, 1,000 records, 200 events, 256 MiB registry budget, 30 minutes, and 1 MiB output. Task limits count UTF-8 bytes.

The registry budget is a deterministic retained-state estimate, not a Go heap/RSS ceiling. Each record charges 256 fixed bytes plus normalized task, repository alias, and idempotency-key bytes; each event charges 64 fixed bytes plus event type and message bytes. Replays add no bytes. The memory backend applies that aggregate budget and may evict oldest terminal records or truncate old events under pressure. SQLite rejects admissions when its configured retained-record cap is reached, and PostgreSQL enforces its configured retained-record and shared queue capacities; durable stores do not evict terminal jobs or their idempotency keys. Per-job event history is bounded. Database/WAL/disk usage is governed by filesystem or database capacity and operator maintenance. Admission fails with `registry_full` when bounded memory capacity cannot be reclaimed, or with a generic storage error if durable persistence fails.

The server captures no more than the configured combined stdout/stderr limit per job across workflow and verification subprocesses, continues draining child output after the cap, and does not expose raw transcripts over the HTTP API. The `memory` registry, jobs, history, and idempotency data do not survive process restart; SQLite and PostgreSQL retain these records and classify startup work for recovery. Set `persistence.backend` to `postgres` and provide `persistence.dsn` to opt into PostgreSQL; configured PostgreSQL fails closed if unreachable. PostgreSQL may be shared by an API control plane and separate workers; SQLite remains single-server storage. See [PostgreSQL Store](postgres-store.md) and [REST worker process](rest-worker.md). New durable databases use principal-scoped idempotency keys and record `submitted_by`. Idempotency key reuse across different named principals is independent; within one principal, exact retry replays and different payloads conflict. Issue-provenance tuple uniqueness remains global across principals: a tuple reserved by another principal/job returns a generic sanitized conflict and does not disclose who owns it or which job was reserved. The schema migration backfills every existing job and associated idempotency key to the reserved `legacy-shared` principal, because the former single shared credential cannot identify individual historical callers. This attribution is a migration identity, not evidence of the actual human submitter. Upgrade the server binary and config schema together: normal `factory server` configuration requires a `principals` array containing at least one enabled named operator, so the legacy key alone is no longer sufficient for startup. Worker mode does not load caller credentials; `api_key_file` and `principals` may be omitted from its config. Keep `api_key_file` provisioned for compatibility and emergency operator access. Queued jobs are candidates for resumption; running jobs require operator reconciliation and are never automatically replayed.

`persistence.backend` defaults to `memory`, which is volatile and loses jobs, history, and idempotency records on restart. Set `backend` to `sqlite` and provide an absolute `path` for single-server durable storage, or set it to `postgres` and provide a PostgreSQL URL in `dsn` for durable storage that can be shared across processes. SQLite's parent directory must already exist and be private (no group/other permissions); new database files are created with owner-only access. SQLite/PostgreSQL schema upgrades run before the listener starts. Configured durable-store failures are fatal—there is no fallback to memory. SQLite must not be shared across hosts or network filesystems. The CLI backup and portable recovery-bundle commands remain SQLite-only; use PostgreSQL-native backup/restore procedures for PostgreSQL. See [PostgreSQL Store](postgres-store.md) and the [REST server operations guide](rest-server-operations.md). No down migration is provided because downgrading would risk destroying job data.

`provider` is optional. When configured, `backend` must be `github`, `token_file` must point to an absolute private regular file, `base_branch` selects the PR target, and `repositories` maps every configured alias to its exact GitHub `owner/repository`. Startup checks the GitHub token and provider API; readiness continues probing the selected store. The provider pushes only to the matching configured GitHub remote and creates or updates one PR per job-specific branch. If branch push or PR state is uncertain, the job fails and its workspace is retained; it does not report success. Memory mode stores provider outcomes only for the life of the process, so use SQLite or PostgreSQL when restart durability is required. Never use credentials in repository remote URLs.

With SQLite or PostgreSQL selected, queued jobs are eligible for resumption and running jobs require operator investigation; the server does not replay possibly ambiguous provider operations automatically. Confirmed PR identity and URL are returned by authenticated `GET /v1/jobs/{id}` and retained in job history. For a failed job with a persisted provider attempt, the explicit authenticated `POST /v1/jobs/{id}/reconcile` action performs read-only provider confirmation and durably records success without rerunning work when the API runtime has the required executor. In a workers-disabled control plane backed by separate workers, this endpoint cannot reconcile because the worker processes do not expose that callback. The attempt need not be marked uncertain: provider publication may succeed even when persisting its outcome fails. Repeating the original idempotency key after restart returns the original job rather than creating a second one. Provider failure, an unconfirmed provider write, workflow failure, and cancellation do not produce a successful job result.

Key files must be regular, non-symlink files owned by the effective user, owner-readable, and inaccessible to group/others. The shared key and provider token are read at startup; rotation requires restart. Raw secrets are redacted in formatting and are not passed to the workflow harness.
