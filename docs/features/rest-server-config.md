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

The registry budget is a deterministic retained-state estimate, not a Go heap/RSS ceiling. Each record charges 256 fixed bytes plus normalized task, repository alias, and idempotency-key bytes; each event charges 64 fixed bytes plus event type and message bytes. Replays add no bytes. Event histories may be truncated under per-job or aggregate pressure, with truncation reported. Oldest terminal records may be evicted to admit new work; queued/running records are never evicted. If neither eviction nor history truncation can free sufficient capacity, admission fails with `registry_full` and does not reserve the attempted key. Lifecycle changes still complete if history cannot retain the event and mark history truncated.

The server captures no more than the configured combined stdout/stderr limit per job across workflow and verification subprocesses, continues draining child output after the cap, and does not expose raw transcripts over the HTTP API. The current in-memory registry, jobs, history, and idempotency data do not survive process restart.

The target MVP adds optional SQL persistence and provider PR create/update; neither is configured by this schema today. The durable-storage interface, SQL connection options, provider choice, and provider credential configuration remain to be designed before implementation. Do not add speculative fields to this example. See the [REST job contract](../roadmap/rest-api-contract.md) for the target requirements.

Key files must be regular, non-symlink files owned by the effective user, owner-readable, and inaccessible to group/others. The shared key is read at startup; rotation requires restart. Raw keys are never represented in JSON or formatting. API credentials are not passed to the harness.
