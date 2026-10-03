# REST server configuration

This file configures the implemented `factory server` command. Configuration is server-only and operator-selected, not caller-controlled. The current runtime uses a local-process harness and one shared API key; every key holder is trusted with the configured server account's local-process authority. A local process or container is not a security sandbox.

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
  "persistence": {
    "backend": "sqlite",
    "path": "/var/lib/factory/jobs.db"
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

`listen_address` defaults to `127.0.0.1:8080`; explicitly binding a non-loopback address exposes the API on that interface and requires suitable network controls. `repositories` maps a validated alias to an absolute checkout root; callers cannot supply paths. Before listening, the server verifies each configured path is an existing directory and the exact canonical Git working-tree root. Subdirectories, bare repositories, and symlink/path-indirected roots are rejected; linked worktree roots are accepted. Git must be installed. Validation errors identify the alias without echoing configured paths.

`harness` is a fixed executable and argv, never shell text. Arguments must contain `{task}` and `{system_prompt}` exactly once; other placeholders are rejected. `verification_checks` is an optional list of trusted operator-configured argv arrays invoked after workflow stages; callers cannot provide or change checks. Empty/omitted checks mean no external checks run. The strict schema bounds this list to 16 commands, 32 nonempty argv elements per command, and 16 KiB total argv bytes per command. Only `mode: local_process` is accepted.

The loader rejects unknown/duplicate JSON fields and trailing JSON. Start the service with `factory server --config /absolute/path/to/server.json`; no other argument ordering or config source is accepted. Resource limits are positive and bounded: request body 2 MiB, task 256 KiB, queue 1024, workers 64, retained records 1,000, events per job 200, logical registry budget 256 MiB, job timeout 24 hours, and captured output 16 MiB maximum. Defaults are 512 KiB body, 256 KiB task, queue 32, two workers, 1,000 records, 200 events, 256 MiB registry budget, 30 minutes, and 1 MiB output. Task limits count UTF-8 bytes.

The registry budget is a deterministic retained-state estimate, not a Go heap/RSS ceiling. Each record charges 256 fixed bytes plus normalized task, repository alias, and idempotency-key bytes; each event charges 64 fixed bytes plus event type and message bytes. Replays add no bytes. The memory backend applies that aggregate budget and may evict oldest terminal records or truncate old events under pressure. SQLite rejects new admissions with `registry_full` when its configured retained-record cap is reached; it does not evict terminal jobs or their idempotency keys. Per-job event history is bounded. Database/WAL disk usage is governed by filesystem capacity and operator maintenance. Admission fails with `registry_full` when bounded memory capacity cannot be reclaimed, or with a generic storage error if SQLite persistence fails.

The server captures no more than the configured combined stdout/stderr limit per job across workflow and verification subprocesses, continues draining child output after the cap, and does not expose raw transcripts over the HTTP API. The `memory` registry, jobs, history, and idempotency data do not survive process restart; SQLite retains these records and classifies startup work for recovery. Queued jobs are candidates for resumption; running jobs require operator reconciliation and are never automatically replayed.

`persistence.backend` defaults to `memory`, which is volatile and loses jobs, history, and idempotency records on restart. Set `backend` to `sqlite` and provide an absolute `path` for restart-durable storage. The parent directory must already exist and be private (no group/other permissions); new database files are created with owner-only access. SQLite schema upgrades run before the listener starts. Failures are fatal—there is no fallback to memory. The database is single-server storage; do not share it across hosts/network filesystems. Use `factory server backup --config <absolute-path> --destination <absolute-path>` for a live, consistent copy and restore drill; see the [REST server operations guide](rest-server-operations.md). No down migration is provided because downgrading would risk destroying job data.

`provider` is optional. When configured, `backend` must be `github`, `token_file` must point to an absolute private regular file, `base_branch` selects the PR target, and `repositories` maps every configured alias to its exact GitHub `owner/repository`. Startup checks the GitHub token and provider API; readiness continues probing the selected store. The provider pushes only to the matching configured GitHub remote and creates or updates one PR per job-specific branch. If branch push or PR state is uncertain, the job fails and its workspace is retained; it does not report success. Memory mode stores provider outcomes only for the life of the process, so use SQLite when restart durability is required. Never use credentials in repository remote URLs.

With SQLite selected, startup reports queued jobs as resumable and running jobs as requiring operator reconciliation; it does not replay possibly ambiguous provider operations automatically. Confirmed PR identity and URL are returned by authenticated `GET /v1/jobs/{id}` and retained in job history. For a failed job with a persisted uncertain provider attempt, the explicit authenticated `POST /v1/jobs/{id}/reconcile` action performs read-only provider confirmation and durably records success without rerunning work. Repeating the original idempotency key after restart returns the original job rather than creating a second one. Provider failure, an unconfirmed provider write, workflow failure, and cancellation do not produce a successful job result.

Key files must be regular, non-symlink files owned by the effective user, owner-readable, and inaccessible to group/others. The shared key and provider token are read at startup; rotation requires restart. Raw secrets are redacted in formatting and are not passed to the workflow harness.
