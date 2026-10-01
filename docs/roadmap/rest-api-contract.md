# Proposed REST server MVP contract

## Status and scope

**Proposed, not implemented.** Factory currently has no REST server, in-memory remote job service, or shared-PAT server integration. This document records a planned single-host service that uses existing Factory workflow/business logic to start jobs through a server-configured coding harness. It is not an implementation claim or delivery commitment.

The service accepts bounded task requests for configured repository aliases. The server selects one operator-configured coding-harness command and fixed argument template (for example, `pi`, `claude`, or `codex`); each job launches and supervises its own harness process in an isolated workspace/session. Callers cannot select an executable or submit command arguments, shell text, or a generic command. A bounded in-memory queue and configured worker concurrency limit govern execution. On successful workflow completion, automatically pushing the branch and creating or updating a pull request is in scope. Merge, release, and deployment remain human-only. See the [proposed execution-mode architecture](execution-modes.md) for the server/executor boundary and local-process MVP direction.

## Authentication and GitHub authority

- API requests use one configured shared bearer API key. All holders have identical access to every endpoint and job; there is no per-caller identity, isolation, or ownership boundary. Do not accept caller-supplied principal identities. The API key is supplied through a protected secret file, loaded only into server memory, and never persisted or exposed to Pi.
- Outbound GitHub operations use one shared fine-grained personal access token (PAT); a dedicated bot/service account is recommended. The PAT owner's GitHub authority applies to all server requests. OAuth and GitHub App authorization are deferred and may be revisited later.
- Never store raw PAT bytes in job records, logs, prompts, or Pi child-process environment. The PAT is supplied through a separate protected secret file and held in server memory. Server integration must ensure the PAT is not inherited by Pi, while narrowly provisioning it only to Factory-owned Git/GitHub operations that require it; test this process-environment boundary before enabling publication.
- Select only the least GitHub permissions required by the actual clone/fetch, branch push, and PR create/update operations. Verify and test the chosen permissions and Git transport behavior before production use. Do not grant merge permissions.
- Encryption-key design in the [credential-custody proposal](credential-custody-design.md) concerns future OAuth grant custody. OAuth is deferred, so that encryption key is not an MVP prerequisite.

## Client, request, and admission

A remote client (for example, a Hermes agent) submits a normal authenticated HTTP task request to Factory Server. The client is not a worker manager and does not launch or control the configured coding harness directly; Factory Server owns job admission, scheduling, execution, status, tests, and publication. Client technology is not otherwise part of the server contract.


A proposed `POST /v1/jobs` request contains:

```json
{
  "repository": "widget",
  "task": "Add a bounded note to the README; do not change code.",
  "issue": 42
}
```

- `repository` is a configured repository alias, not an arbitrary repository URL. Server configuration maps each alias to an approved local Git checkout root and its GitHub repository identity; callers cannot submit filesystem paths. `task` is a required nonblank task description. `issue` is an optional positive issue number in the alias's configured GitHub repository.
- Issue URLs are not accepted in the initial contract. The task description—not an issue—is the requested work; the issue is supplemental context only and does not start autonomous polling, reconciliation, or issue tracking.
- The request must not accept arbitrary shell, command, or executable fields.
- Bound request body and task sizes before decoding/processing. Reject malformed JSON, unknown fields, unsupported media types, invalid UTF-8, and blank task descriptions. Exact limits are implementation choices to specify before deployment.
- Admission validates and creates the job in the process-local registry and bounded in-memory queue before returning acceptance. Require a client-generated `Idempotency-Key` header. Repeating the same key with an identical normalized request while retained returns the existing job; reusing a key with a different request returns `409 Conflict`. Idempotency keys are forgotten when the process exits or their terminal record is evicted.
- Configure a maximum total registry record count and a maximum event count per job. Never evict queued or active jobs. At the record cap, evict the oldest terminal job records as needed; if all retained records are queued/active and the cap is reached, reject admission. Eviction makes prior status/history return `404` and forgets its idempotency key. When an individual job exceeds its event cap, drop oldest events and mark its history as truncated.
- If the bounded pending queue has no capacity, return `503 Service Unavailable` with a stable capacity error and do not create a job record. A later client retry must reuse its `Idempotency-Key`.

A successful admission may return `202 Accepted` with an opaque job ID, initial status, and links to status/history; an idempotent replay returns that same job rather than queueing another. Proposed read endpoints are `GET /v1/jobs/{id}` and `GET /v1/jobs/{id}/history`. Since API credentials are shared, any valid key holder can inspect any retained job; responses must still be bounded and sanitized and must not expose credentials, sensitive filesystem paths, or unfiltered agent output. Status/history for evicted jobs returns `404`. History responses report whether older events were truncated.

## Process-local jobs and lifecycle

- Job records, execution status, idempotency keys, and history exist only in the running server process's memory. No PostgreSQL, SQL migration, file-backed job store, or durable job history is required by this MVP.
- The service runs accepted jobs in bounded in-process workers. Each worker starts one configured coding-harness process per job using an argument-vector invocation rather than a caller-controlled shell command. Jobs receive separate workspaces and agent sessions; do not reuse a persistent harness session or carry one job's prompt/context into another. The server must remove the shared PAT and unrelated server secrets from the child environment, and provide credentials only to Factory-owned Git/GitHub operations. Worker execution and job tracking share one process lifetime; this is a single-host/single-process service, not a distributed queue or horizontally scalable server.
- A process crash or forced termination loses all job records, history, idempotency data, and knowledge of active execution. The server cannot report prior jobs as interrupted after restart, resume them, or reconcile their outcomes from its own state. Clients must resubmit after restart. Any Git branch/PR/workspace side effects that occurred before a crash may remain externally visible and must not be blindly repeated; publication behavior must reconcile live Git/GitHub state or stop for human inspection before any retry.
- On graceful shutdown, first stop accepting requests, cancel active jobs and their harness processes, allow bounded cancellation/cleanup, then exit. The selected direction is to remove server-created job workspaces after their workers have exited; cleanup must be restricted to the exact server-created worktree, must never use a broad or unvalidated path, and must report cleanup failures. If a Git push or PR result is uncertain, retain the workspace/evidence until the external state is reconciled rather than deleting evidence that may be needed to prevent a duplicate side effect. All process-local job records disappear on exit, including completed history. The shutdown deadline and cancellation/cleanup bounds must be configured and tested.
- Do not claim durable acceptance, restart recovery, exactly-once execution, or recovery of process/agent memory. This MVP explicitly accepts these limitations in exchange for avoiding a database and migrations.

## Local deployment testing

Provide a Docker Compose setup for local deployment testing once the server entry point exists. It should run a single Factory Server process, mount only configured repository checkouts and protected API-key/PAT secret files, and configure the coding harness on the server side. It must not require PostgreSQL for this memory-only MVP. Compose is a test/development topology, not a security sandbox or a production hardening claim. The remote caller can be any HTTP client, including an agent such as Hermes; credentials must travel over an appropriately protected connection in deployed use.

## Ordered implementation slices

These slices are dependency-ordered; they are planning gates, not claims of existing behavior or delivery dates. Keep each independently reviewable and preserve the CLI's current behavior.

1. **Server configuration and runtime boundaries.** Define server-only configuration for listener, repository aliases, checkout roots, harness executable/argument template, resource limits, and protected API-key/PAT file paths. Validate exact Git roots and strict secret-file ownership/mode/no-symlink rules. Add startup loading and ensure the coding-harness child environment excludes API key and PAT. No HTTP or job execution yet.
2. **Volatile job manager.** Implement a concurrency-safe process-local registry, bounded pending queue and worker slots, stable opaque IDs, in-process idempotency, status transitions, and bounded history. Define queue-full/repeated-idempotency behavior and tests for concurrent admission, status/history inspection, and memory/history limits. No database, disk queue, or recovery behavior.
3. **Authenticated HTTP API.** Add a separately testable `net/http` handler for readiness/health as narrowly specified, `POST /v1/jobs`, `GET /v1/jobs/{id}`, and bounded `GET /v1/jobs/{id}/history`. Require the shared bearer API key on job routes; validate request size/schema/UTF-8/unknown fields and issue-number semantics. Verify authorization, status codes, safe errors, redaction, queue-full handling, and job IDs with handler tests.
4. **In-process worker and harness lifecycle.** Connect accepted jobs to bounded workers. Each job gets a distinct server-created workspace/session and one server-selected harness subprocess invoked without caller-controlled shell parsing. Implement cancellation propagation, process-group cleanup, graceful shutdown admission stop, bounded worker exit, and safe workspace cleanup; retain evidence whenever Git/PR effects are uncertain. Test competing jobs, cancellation, shutdown, cleanup failure, and workspace isolation with fake harness executables.
5. **Factory workflow integration.** Adapt the existing requirements/implementation/review/document/pipeline stages behind an injectable job execution interface while preserving CLI state and behavior. Validate configured checkout root, isolate task branches/worktrees, bound execution time/output, and expose only sanitized status/history—not raw unfiltered logs or host paths. Test Pi and another compatible harness contract where practical; unsupported harness differences must fail clearly rather than run caller-supplied commands.
6. **Shared-PAT Git/PR publication.** Add Factory-owned Git/GitHub operations that receive the PAT only for the necessary calls; remove it from the harness environment and prevent it appearing in arguments, remotes, prompts, logs, records, or artifacts. Validate clone/fetch, branch push, PR create/update, exact least permissions, idempotent branch/PR association, and uncertain-outcome reconciliation. Merge, release, and deployment must remain unavailable.
7. **Local Compose and end-to-end acceptance.** Package a single server process and one configured coding harness with Docker Compose for local testing, without PostgreSQL. Mount explicit repository/config/secret inputs; document image build, port exposure, secret provisioning, and that Compose/containerization is not a sandbox. Exercise authenticated submission through tests and PR preparation using a disposable local repository and non-production GitHub setup. Verify process restart loses registry state as specified and graceful shutdown cancels workers safely.

**MVP gate:** the full path from HTTP admission to inspectable in-process status/history and a completed, verified task must work under configured bounds. Process exit loses records; external side effects are never blindly repeated; authentication and PAT isolation tests pass; no merge/release/deploy path exists; the documented local Compose flow works without a database.

## Limits, errors, and deferred controls

- Return safe generic errors without stack traces, API keys, PATs, authorization headers, raw provider payloads, or sensitive filesystem paths. Never log API-key or PAT values. Define stable response schemas and status codes during implementation.
- Rate limiting is deferred. This proposal does not promise rate limits, `429`, or a `Retry-After` contract. Request/body input bounds and bounded in-memory job/history capacity remain necessary and do not constitute rate limiting.
- Concurrency, queue capacity, total retained job count, per-job event count, timeouts, harness executable/arguments, and resource bounds are server configuration, not caller-controlled job fields. The configured coding harness is launched once per job; process-pool/session reuse is not part of this direction.
- PostgreSQL-backed persistence, database migrations, backup/restore, and durable restart recovery are explicitly deferred. Revisit them only if product requirements change to require jobs or history to survive process exit.

## Explicit human boundary

The worker may push its implementation branch and create/update a PR after a successful workflow. It must never merge a PR, release artifacts, or deploy software. A PR or successful process exit is not an independent correctness verdict; verification evidence and limitations should remain reviewable by a human.
