# REST worker processes

Factory now supports two separate worker modes. Do not mix their configuration or claim paths:

- [`factory worker remote --config <absolute-path>`](#standalone-http-worker) runs a standalone Worker that uses the Controller's HTTP Worker API. The Worker needs no database connection.
- [`factory worker --config <absolute-path>`](#legacy-direct-postgresql-worker) is the legacy direct-PostgreSQL Worker. It connects to the shared job Store and claims through PostgreSQL.

The all-in-one `factory server` still runs workers by default. Either separate worker mode is optional; Factory does not provide a fleet coordinator. Neither mode is a security sandbox. The local Compose `worker` profile remains the legacy direct-PostgreSQL mode.

## Standalone HTTP Worker

Run the standalone Worker with:

```sh
factory worker remote --config /etc/factory/remote-worker.json
```

This process polls the Controller over HTTP, receives assigned work, runs it with local repository and harness settings, and reports cancellation checks and bounded terminal results through heartbeat and finish calls. It has no database configuration and does not connect to PostgreSQL. The Controller must use PostgreSQL and explicitly enable `remote_worker_api`. This is a separate path from the legacy Worker below; enabling it does not disable the legacy mode or the all-in-one server workers.

### Controller configuration and transport

Add this strict block to the Controller server JSON:

```json
"remote_worker_api": {
  "enabled": true,
  "secret_file": "/run/secrets/factory-remote-worker"
}
```

`remote_worker_api` is strict: it accepts only `enabled` and `secret_file`. When enabled it requires the absolute `secret_file` path; when disabled, omit `secret_file`. It requires `persistence.backend: "postgres"`. The secret file must be an effective-user-owned, non-symlink regular file, owner-readable with no group/other access, and contain 32–4096 bytes. This shared secret authenticates only the Worker API; it is separate from `api_key_file`, named submitter/operator credentials, and worker-session credentials. Keep it out of job data, URLs, logs, and source control. The Controller's API listener does not terminate TLS. Put TLS on a trusted HTTPS reverse proxy or another TLS-terminating boundary. The standalone Worker accepts HTTPS URLs and permits plain HTTP only for loopback origins by default. The strict Worker config has an optional boolean `allow_insecure_http`, which defaults to `false`; set it to `true` only when the operator explicitly trusts the network carrying the shared secret. The local Compose-generated config sets it to `true` for `http://api:8080` on its private Compose network. Do not use that opt-in for public or untrusted networks. Redirects are not followed, so Worker credentials are not sent to redirect targets.

The Worker API exposes only these operations:

| Method and route | Purpose | Successful response |
| --- | --- | --- |
| `POST /v1/worker/claims` | Claim one queued job, or retry a prior claim request | `200` with the assigned job and fenced attempt, or `204` when no job/capacity is available |
| `POST /v1/worker/attempts/{id}/heartbeat` | Renew the active attempt and check cancellation | `200` with `cancellation_requested` |
| `POST /v1/worker/attempts/{id}/finish` | Record the terminal result and bounded evidence | `200` with `{"status":"finished"}` |

All three routes require exactly one `Authorization: Bearer <shared-secret>` header. This auth scheme is not the ordinary API bearer credential. Requests must use `application/json`, contain one JSON value, have no duplicate or unknown fields, and be no larger than 512 KiB (or the lower configured API request limit). The server authenticates before route and body validation. The route details, wire schemas, and worker-only auth scheme are in the [OpenAPI specification](../openapi/rest-api-v1.json).

A claim request contains `claim_id`, `worker_id`, and `boot_id`; each is 1–128 ASCII characters matching `[A-Za-z0-9][A-Za-z0-9._:-]*`. It also contains `repositories`: a non-empty set of up to 16 repository aliases allowed by the Worker configuration. Each alias is at most 64 characters and matches `[A-Za-z0-9][A-Za-z0-9_-]*`. The Worker sends aliases that at least one configured harness profile can execute. This is capability metadata only; it does not select a harness/profile. The Controller atomically claims the oldest queued job whose repository is in that set. The Worker generates a fresh boot ID when it starts and retries a claim with the same claim ID and exact same repository set. An identical active claim retry from the same worker and boot returns the same job and owner token only when the repository capability set is unchanged; changing that set or reusing the claim ID from another worker/boot conflicts. A claim response contains a job ID, request data (`repository`, task up to 256 KiB, and optional issue/provenance fields), and an attempt (`job_id`, positive `generation`, 64-character lowercase-hex `owner_token`, `worker_id`, and `boot_id`). The opaque owner token appears only in the authenticated Worker claim response and subsequent Worker mutation requests. It is not part of public job or ordinary human API schemas.

Heartbeat and finish requests include the attempt `generation`, `owner_token`, `worker_id`, and `boot_id`; the path job ID must match that fenced attempt. Heartbeat returns whether cancellation was requested; this signal is not an error. Finish accepts a terminal `status` (`succeeded`, `failed`, or `canceled`) and required `evidence`: up to 16 ordered checks named `check-01` through `check-16`, each with outcome `not_completed`, `passed`, `failed`, or `canceled`, and up to four limitation strings (each at most 128 bytes) selected from `No operator-configured verification checks were run.` and `Agent-reported completion is not an independent correctness verdict.`. A canceled result is accepted only after a cancellation request; a non-canceled result is rejected while cancellation is pending. An identical finish retry succeeds after commit, while a different result for the same attempt conflicts. Stale or invalid ownership cannot mutate the job.

The Worker creates a fresh random claim ID for each new poll and retries ambiguous/transient claim failures with that same ID after a one-second delay. Empty claims are polled again after one second. During execution it sends heartbeats every five seconds with a 15-second HTTP timeout. Claim and finish requests retry transport failures, HTTP 5xx, and HTTP 429 after one second; heartbeat failure stops execution and leaves the job inspectable rather than being treated as a cancellation signal. A finish retry sends the exact same terminal status and evidence; rejected non-retryable results are not rewritten into a different result. For cancellation, the Worker stops its execution context when heartbeat reports a request, then reports `canceled` only if execution confirms cooperative cancellation. A pending cancellation blocks a success result. If cancellation races with completion, is not observed, or execution does not confirm stop, the Worker leaves the assigned job inspectable instead of claiming an unsupported terminal outcome. A heartbeat/network failure also stops new work and leaves the assigned job inspectable; heartbeat loss alone is not proof of process stop. Interrupted or uncertain running work is not automatically replayed or reassigned by this protocol. This is not a guarantee that a Worker or external process has stopped; operators must inspect uncertain work before taking action.

### Standalone Worker configuration

The standalone Worker uses a separate strict JSON file. Unknown or duplicate fields, malformed/trailing JSON, and database or server-only fields are rejected. Required fields are `controller_base_url`, `shared_secret_file`, `worker_id`, `repositories`, `harness_profiles`, and `limits`; `verification_checks` is optional.

```json
{
  "controller_base_url": "https://factory.example",
  "shared_secret_file": "/run/secrets/factory-remote-worker",
  "allow_insecure_http": false,
  "worker_id": "worker-west-1",
  "repositories": {
    "widget": "/srv/checkouts/widget"
  },
  "harness_profiles": {
    "fast": {
      "executable": "/usr/local/bin/pi",
      "args": ["-p", "--no-session", "--append-system-prompt", "{system_prompt}", "{task}"],
      "repositories": ["widget"]
    }
  },
  "verification_checks": [["go", "test", "./..."]],
  "limits": {
    "job_timeout": "30m",
    "harness_output_bytes": 1048576
  }
}
```

`controller_base_url` is an HTTPS origin by default; a loopback HTTP origin is allowed for local development. The optional strict boolean `allow_insecure_http` defaults to `false`; when `true`, it permits non-loopback HTTP and sends the shared secret without transport encryption. Use it only on a private, trusted network such as the local Compose network for `api:8080`; production deployments should use HTTPS. Redirects are not followed, which prevents credentials from being sent to a redirect target. `shared_secret_file` and repository roots must be absolute paths. `worker_id` is 1–128 characters matching `[A-Za-z0-9][A-Za-z0-9._:-]*`. `repositories` maps local aliases to absolute checkout roots, which must be valid canonical Git worktree roots when used. Each `harness_profiles` entry has a name matching `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`, a fixed executable, argv, and one or more configured repository aliases. Its argv is a direct argument vector, not shell text, and must contain `{task}` and `{system_prompt}` exactly once. Optional `verification_checks` are operator-defined argv arrays, bounded to 16 commands, 32 nonempty arguments per command, and 16 KiB of argument bytes per command; request data cannot select commands or profiles. `limits.job_timeout` is a positive duration up to 24 hours; `limits.harness_output_bytes` is between 1 byte and 16 MiB. The standalone Worker also enforces the server's 256 KiB task bound.

For each job, the Worker filters profiles to those configured for the assigned repository, sorts their names, and selects one deterministically using the SHA-256 digest of the job ID. A request cannot select a profile, executable, repository path, or credential. The worker runs the existing local Factory workflow with automatic publication disabled. It does not require a provider and does not create or update a pull request; configured verification results and limitations are reported as bounded evidence. This differs from the REST server's ordinary worker execution: when a GitHub provider is configured there, confirmed pull-request publication is required for job success. Agent-reported completion is not an independent correctness verdict.

The shared-secret file uses the same private regular-file protections as the Controller's configured Worker secret: effective-user-owned non-symlink regular file, owner-readable with no group/other permissions, and 32–4096 bytes. The Worker and its harness execute with their local account authority. Factory does not provide sandboxing or isolation; protect the host, repository credentials, network, and secret accordingly.

## Legacy direct-PostgreSQL Worker

`factory worker --config /absolute/path/to/worker.json` starts the legacy worker-only process for a PostgreSQL-backed REST job service. It connects directly to the configured shared job Store, claims jobs through fenced PostgreSQL leases, constructs trusted workspaces and the configured Factory workflow, and runs without an HTTP listener. This config extends the strict server configuration with `worker_controller`; the worker controller block configures the separate mTLS registration/advisory-heartbeat listener and is not the remote HTTP Worker API. It does not load `api_key_file` or named caller `principals`.

The worker requires `persistence.backend: "postgres"` and the same `persistence.dsn` as the API. Every process sharing the database must use the same `limits.workers` value. The legacy Worker does not use `/v1/worker/claims`, `/heartbeat`, or `/finish`; it owns lifecycle operations through PostgreSQL. Its `worker_controller` config requires an HTTPS origin and absolute client certificate, private-key, and server CA paths; the server-only config rejects this client block. The client key must be an effective-user-owned regular file, readable by its owner only, with no symlink path components and a 1 MiB size limit. Factory does not issue these credentials or manage rotation/revocation. The Worker registers a fresh boot ID over mutual TLS before it can claim. That session supports registration and advisory heartbeat only; its bearer capability stays in process memory. Session registration/heartbeat failure prevents new claims. The heartbeat does not prove liveness or process stop, and session retirement is not automatic at shutdown. A stale active session may need operator retirement before a later boot can register. This is distinct from the `remote_worker_api` shared secret and standalone HTTP Worker protocol. Its session heartbeat runs every five minutes with a ten-second request timeout; the 15-minute capability is renewed at half its remaining lifetime. On graceful shutdown the Worker stops claims and drains active executions while heartbeats continue; after the shutdown call returns the heartbeat stops, even if an executor is still unwinding. After heartbeat failure, new claims stop while already-running work drains without a deadline. These behaviors do not establish process stop, clean workspaces, or reassign a job. When the legacy Worker has a provider configured, PR publication remains part of its successful job workflow; this does not apply to the standalone HTTP Worker.

All-in-one workers remain enabled by default; `workers_enabled: false` is for a PostgreSQL API control plane that shares its database with separately launched legacy workers. The local Compose `worker` profile starts this legacy mode. It is not a fleet coordinator. After interruption, queued durable-store jobs may resume, but interrupted running jobs remain for operator investigation and are not replayed or automatically reassigned. See [server configuration](rest-server-config.md), [server operations](rest-server-operations.md), and [local Compose setup](compose-local-control-plane.md).


For the legacy direct-PostgreSQL Worker, `persistence.dsn` must point to the same database as the API control plane. `api_key_file` and `principals` may be omitted; the config still needs trusted repository roots, harness argv, limits, and optional verification/provider settings. With `workers_enabled: false`, the control plane cannot perform the read-only provider lookup for `POST /v1/jobs/{id}/reconcile`: the retained workspace and provider lookup executor exist only in Worker processes, which do not expose that callback. Treat the route as unavailable in this topology and investigate the provider while retaining workspace artifacts.

`limits.workers` is the concurrent execution cap shared across PostgreSQL processes, not a per-process allowance or fleet scheduler. All server configurations that enable local workers, all direct-PostgreSQL Worker configs, and the workers-disabled API control plane must use the same value. Mismatched values can make the effective cap depend on which process claims. PostgreSQL fencing/capacity checks enforce the shared cap, but provide no fleet coordinator or workflow checkpointing. `worker_id` is an optional bounded observable label; if omitted, it is derived from the process ID. Each worker creates a fresh boot identity. These values are metadata, not credentials.

The legacy Worker uses the PostgreSQL Store for claims and lifecycle changes; its Controller session endpoint carries no job claims or ownership mutations. The session is not retired automatically during shutdown. Shutdown or heartbeat loss cannot prove external processes stopped. A stale active session may block registration from a later boot until an operator retires it through the authenticated Controller. Capability expiry is not evidence of process stop or session retirement. The DSN, repository/harness/provider settings, client key, and provider credentials are operator-trusted. Worker processes execute configured harness and repository content with their local account authority; they are not a security sandbox.

The worker-only HTTP API and legacy direct-PostgreSQL mode both have conservative recovery limits. Queued durable-store jobs can resume after restart; interrupted running jobs require operator investigation and are not automatically replayed. A Worker heartbeat or claim lease is not proof that external execution stopped. Use appropriately restricted service accounts and network controls.

Example legacy deployment:

```sh
factory server --config /etc/factory/control-plane.json
factory worker --config /etc/factory/worker.json
```

Set `workers_enabled` to `false` in the control-plane config. The legacy worker command is supported only with PostgreSQL; memory and SQLite are not cross-process worker stores. For its local Compose example, see [local Compose setup](compose-local-control-plane.md).
