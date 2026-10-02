# REST server configuration

This file configures the implemented `factory server` command. Configuration is server-only, operator-selected, and not caller-controlled. The MVP uses local-process harness execution; a shared API-key holder is trusted with the server account's local-process authority. Neither a local process nor a container is a security sandbox. PAT loading/publication, branch push, and PR writes are not implemented.

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

`listen_address` defaults to `127.0.0.1:8080` when omitted. Operators may explicitly bind another address, including a non-loopback interface; doing so exposes the API on that interface and requires appropriate network controls. `repositories` maps a validated alias to an explicitly configured absolute checkout root; requests cannot provide paths. Before opening a listener, `factory server` calls `Config.ValidateRepositoryRoots`. That method requires each path to be an existing directory and the exact canonical root reported by `git -C <root> rev-parse --show-toplevel`; subdirectories, bare repositories, and symlink/path-indirected roots are rejected. Linked worktree roots are accepted. Git must be installed. Validation errors identify the repository alias without echoing configured paths. `harness` is a fixed executable and argument vector, never shell text. Arguments must contain `{task}` and `{system_prompt}` exactly once each; other placeholders are rejected. `verification_checks` is an optional array of trusted operator-configured argv arrays. Each array is invoked directly without shell parsing after the workflow stages; callers cannot provide or alter checks. Omitted or empty checks means no external verification checks run. The strict schema limits this field to 16 commands, at most 32 nonempty argv elements (including the executable) per command, and 16 KiB total argv bytes per command. Only `mode: local_process` is accepted.

The loader rejects unknown/duplicate JSON fields and trailing JSON. Start the service with `factory server --config /absolute/path/to/server.json`; the command accepts no other argument ordering or config source. See [REST job server](rest-server.md) for runtime and shutdown behavior. Resource limits are positive and bounded: request body at 2 MiB, task at 256 KiB, queue at 1024, workers at 64, at most 1,000 retained records, at most 200 events per job, a 256 MiB logical registry budget, job timeout at 24 hours, and captured harness output at 16 MiB maximum. Omitted limits default to 512 KiB body, 256 KiB task, queue 32, two workers, 1,000 records, 200 events, 256 MiB registry budget, 30 minutes, and 1 MiB output. The task limit is byte-based (UTF-8), and the request body default leaves room for JSON and headers around the maximum task.

The registry budget is a deterministic retained-state estimate, not a Go heap or RSS ceiling. For each record, accounting charges 256 fixed bytes plus normalized task, repository alias, and idempotency key UTF-8 bytes. For each retained event, it charges 64 fixed bytes plus event type and message bytes. Status/timestamp fields, maps, slices, and other Go runtime overhead are covered only by those conservative fixed allowances. An idempotency replay adds no bytes. When an event history reaches its per-job cap, the oldest event is removed before its replacement is charged; under aggregate pressure, oldest retained events may also be truncated, setting `truncated`. If retained terminal records can free capacity, they are evicted oldest-first (and their idempotency keys become reusable); queued and running records are never evicted. If no terminal record or reclaimable history allows an admission to fit within either limit, admission fails with the existing `registry_full` error and does not reserve the attempted key. Lifecycle transitions attempt to retain their event; if the budget cannot accommodate it after reclaiming older terminal records/history events, the status transition still completes and history is marked truncated.

The server captures at most the configured combined stdout/stderr limit per job across workflow stages and verification subprocesses, continues draining child output after the cap, and does not expose raw transcripts through the HTTP API.

Key files must be regular, non-symlink files owned by the effective user, owner-readable, and inaccessible to group/others. Only one shared key is loaded into memory; restart/reload is required to apply file rotation. No raw key is represented in JSON or formatting.
