# Proposed REST server configuration example

**Proposed, not currently implemented.** This example documents the server-only configuration boundary from REST MVP Slice 1. It does not add a server entry point, HTTP routes, job manager, or worker execution. The MVP direction is local-process harness execution only; configuration is operator-selected, not caller-controlled. A shared API-key holder is trusted with the server account's local-process authority. Neither a local process nor a container is a security sandbox. PAT loading/publication, branch push, and PR writes are out of scope pending separately approved restart-safe reconciliation.

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
  "limits": {
    "request_body_bytes": 131072,
    "task_bytes": 65536,
    "queue_capacity": 32,
    "workers": 2,
    "job_timeout": "30m",
    "harness_output_bytes": 1048576
  }
}
```

`listen_address` defaults to `127.0.0.1:8080` when omitted. Operators may explicitly bind another address, including a non-loopback interface; doing so exposes the API on that interface and requires appropriate network controls. `repositories` maps a validated alias to an explicitly configured absolute checkout root; requests cannot provide paths. Before opening a listener, a future server startup path must call `Config.ValidateRepositoryRoots`. That method requires each path to be an existing directory and the exact canonical root reported by `git -C <root> rev-parse --show-toplevel`; subdirectories, bare repositories, and symlink/path-indirected roots are rejected. Linked worktree roots are accepted. Git must be installed. Validation errors identify the repository alias without echoing configured paths. `harness` is a fixed executable and argument vector, never shell text. Arguments must contain `{task}` and `{system_prompt}` exactly once each; other placeholders are rejected. Only `mode: local_process` is accepted.

The loader rejects unknown/duplicate JSON fields and trailing JSON. Resource limits are positive and bounded: request body at 2 MiB, task at 1 MiB, queue at 1024, workers at 64, job timeout at 24 hours, and captured harness output at 16 MiB maximum. Omitted limits default to 128 KiB body, 64 KiB task, queue 32, two workers, 30 minutes, and 1 MiB output. Key files must be regular, non-symlink files owned by the effective user, owner-readable, and inaccessible to group/others. Only one shared key is loaded into memory; restart/reload is required to apply file rotation. No raw key is represented in JSON or formatting.
