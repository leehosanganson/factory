# Proposed REST server MVP contract

## Status and scope

**Proposed, not implemented.** Factory currently has no REST server or remote job service. This document records a planned single-host service that uses existing Factory workflow/business logic to run jobs through a server-configured coding harness. It is not an implementation claim or delivery commitment.

The service accepts bounded task requests for configured repository aliases and runs the configured harness as a local subprocess under the server account. Callers cannot select an executable or submit command arguments, shell text, or a generic command. A bounded in-memory queue and configured worker concurrency limit govern execution. Every API-key holder is trusted with the configured server account's local-process authority: this design is not a sandbox and does not claim isolation from malicious callers. Branch push and PR create/update, including shared PAT use, are deferred from this MVP until a separately designed, restart-safe, cross-process reconciliation gate can safely resolve uncertain side effects. Merge, release, and deployment remain unavailable. See the [proposed execution-mode architecture](execution-modes.md) for the server/executor boundary and local-process MVP direction.

## Authentication and GitHub authority

- API requests use one configured shared bearer API key. All holders have identical access to every endpoint and job; there is no per-caller identity, isolation, or ownership boundary. Do not accept caller-supplied principal identities. Key provisioning and rotation remain to be specified; the key must not be exposed to the harness.
- The MVP does not select or require a GitHub credential and does not perform branch pushes or PR create/update. A future design for those writes must define credential custody and least privilege as part of the restart-safe, cross-process reconciliation gate; OAuth, GitHub App, and shared-PAT approaches are not selected by this contract.
- The [credential-custody proposal](credential-custody-design.md) describes possible future OAuth grant custody, not an MVP credential requirement.

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

- `repository` is a configured repository alias, not an arbitrary repository URL. Server configuration maps each alias to an approved local Git checkout root and its GitHub repository identity; callers cannot submit filesystem paths. `task` is a required nonblank task description. `issue` is optional supplemental context and accepts only a positive issue number in the alias's configured repository. Issue URLs are excluded from this MVP.
- The task description—not an issue—is the requested work; the optional issue number does not start autonomous polling, reconciliation, or issue tracking.
- The request must not accept arbitrary shell, command, or executable fields.
- Bound request body and task sizes before decoding/processing. Reject malformed JSON, unknown fields, unsupported media types, invalid UTF-8, and blank task descriptions. Exact limits are implementation choices to specify before deployment.
- Admission validates and creates the job in the process-local registry and bounded in-memory queue before returning acceptance. Require a client-generated `Idempotency-Key` header. Repeating the same key with an identical normalized request while retained returns the existing job; reusing a key with a different request returns `409 Conflict`. Idempotency keys are forgotten when the process exits or their terminal record is evicted.
- Configure a maximum total registry record count and a maximum event count per job. Never evict queued or active jobs. At the record cap, evict the oldest terminal job records as needed; if all retained records are queued/active and the cap is reached, reject admission. Eviction makes prior status/history return `404` and forgets its idempotency key. When an individual job exceeds its event cap, drop oldest events and mark its history as truncated.
- If the bounded pending queue has no capacity, return `503 Service Unavailable` with a stable capacity error and do not create a job record. A later client retry must reuse its `Idempotency-Key`.

A successful admission may return `202 Accepted` with an opaque job ID, initial status, and links to status/history; an idempotent replay returns that same job rather than queueing another. Proposed read endpoints are `GET /v1/jobs/{id}` and `GET /v1/jobs/{id}/history`. Since API credentials are shared, any valid key holder can inspect any retained job; responses must still be bounded and sanitized and must not expose credentials, sensitive filesystem paths, or unfiltered agent output. Status/history for evicted jobs returns `404`. History responses report whether older events were truncated.

## Process-local jobs and lifecycle

- Job records, execution status, idempotency keys, and history exist only in the running server process's memory. No PostgreSQL, SQL migration, file-backed job store, or durable job history is required by this MVP.
- The service runs accepted jobs in bounded in-process workers. Each worker starts one configured coding-harness subprocess per job using an argument-vector invocation rather than caller-controlled shell text. Jobs receive separate workspaces and agent sessions. Every subprocess runs with the configured server account's local-process authority; it is not a sandbox. Worker execution and job tracking share one process lifetime, so this is not a distributed or horizontally scalable service.
- A process crash or forced termination loses all job records, history, idempotency data, and knowledge of active execution. Clients must resubmit after restart; the server cannot resume jobs or recover their status. The MVP performs no branch push or PR create/update. No external side effect may be blindly retried; before any future retry, a separately designed restart-safe, cross-process reconciliation gate must resolve what happened or require human inspection.
- On graceful shutdown, stop accepting requests, cancel active jobs and their harness processes, allow bounded cancellation/cleanup, then exit. Remove server-created workspaces only after workers exit and only through validated paths. Preserve evidence if an external side effect is uncertain. All process-local job records disappear on exit, including completed history. Configure and test shutdown, cancellation, and cleanup bounds.
- Do not claim durable acceptance, restart recovery, exactly-once execution, or recovery of process/agent memory. This MVP explicitly accepts these limitations in exchange for avoiding a database and migrations.

## Local deployment testing

Provide a Docker Compose setup for local deployment testing once the server entry point exists. It should run a single Factory Server with its configured local-subprocess executor, mount configured repository checkouts and protected API-key files, and configure the harness server-side. Compose does not run a per-job container and is not a security sandbox or production-hardening claim. Future ephemeral Docker-container or Kubernetes-Pod execution remains operator-configured and must retain the same API/job contract; neither is an isolation guarantee against malicious API-key holders. The remote caller can be any HTTP client, including an agent such as Hermes; credentials must travel over an appropriately protected connection in deployed use.

## Ordered implementation slices

These slices are dependency-ordered; they are planning gates, not claims of existing behavior or delivery dates. Keep each independently reviewable and preserve the CLI's current behavior.

1. **Server configuration and runtime boundaries.** Define server-only configuration for listener, repository aliases, checkout roots, harness executable/argument template, resource limits, and protected API-key file path. Validate exact Git roots and strict API-key file ownership/mode/no-symlink rules. Add startup loading and ensure the coding-harness child environment excludes the API key. No HTTP or job execution yet.
2. **Volatile job manager.** Implement a concurrency-safe process-local registry, bounded pending queue and worker slots, stable opaque IDs, in-process idempotency, status transitions, and bounded history. Define queue-full/repeated-idempotency behavior and tests for concurrent admission, status/history inspection, and memory/history limits. No database, disk queue, or recovery behavior.
3. **Authenticated HTTP API.** Add a separately testable `net/http` handler for readiness/health as narrowly specified, `POST /v1/jobs`, `GET /v1/jobs/{id}`, and bounded `GET /v1/jobs/{id}/history`. Require the shared bearer API key on job routes; validate request size/schema/UTF-8/unknown fields and issue-number semantics. Verify authorization, status codes, safe errors, redaction, queue-full handling, and job IDs with handler tests.
4. **In-process worker and harness lifecycle.** Connect accepted jobs to bounded workers. Each job gets a distinct server-created workspace/session and one server-selected harness subprocess invoked without caller-controlled shell parsing. The process runs with the server account's local-process authority; it is not a sandbox. Implement cancellation propagation, process-group cleanup, graceful shutdown admission stop, bounded worker exit, and safe workspace cleanup. Test competing jobs, cancellation, shutdown, cleanup failure, and workspace handling with fake harness executables.
5. **Factory workflow integration.** Adapt supported Factory implementation jobs behind an injectable job execution interface while preserving CLI state and behavior. Validate configured checkout root, isolate task workspaces, bound execution time/output, and expose only sanitized status/history—not raw unfiltered logs or host paths. Test supported harness contracts where practical; unsupported differences must fail clearly rather than run caller-supplied commands.
6. **Safe side-effect design gate (deferred from MVP).** Before enabling shared PAT use, branch push, or PR create/update, design and test restart-safe cross-process reconciliation for uncertain outcomes. Specify credential custody, idempotent association, and operator escalation when state cannot be established. Do not blindly retry external side effects. This is a separate gate, not an MVP implementation slice.
7. **Local Compose and end-to-end acceptance.** Package one Factory Server with its configured local-subprocess executor in Docker Compose for local testing, without PostgreSQL. Mount explicit repository/config/API-key inputs; document image build, port exposure, and that Compose/containerization is not a sandbox. Exercise authenticated submission through status/history, and verify process restart loses registry state as specified and graceful shutdown cancels workers safely.

**MVP gate:** the full path from HTTP admission to inspectable in-process status/history and a completed, verified task must work under configured bounds. Process exit loses records; clients resubmit after restart; no external side effect is blindly retried; authentication is tested; branch push, PR create/update, merge, release, and deployment are unavailable; the documented local Compose flow works without a database.

## Limits, errors, and deferred controls

- Return safe generic errors without stack traces, API keys, authorization headers, raw provider payloads, or sensitive filesystem paths. Never log API-key values. Define stable response schemas and status codes during implementation.
- Rate limiting is deferred. This proposal does not promise rate limits, `429`, or a `Retry-After` contract. Request/body input bounds and bounded in-memory job/history capacity remain necessary and do not constitute rate limiting.
- Concurrency, queue capacity, total retained job count, per-job event count, timeouts, harness executable/arguments, and resource bounds are server configuration, not caller-controlled job fields. The configured coding harness is launched once per job; process-pool/session reuse is not part of this direction.
- PostgreSQL-backed persistence, database migrations, backup/restore, and durable restart recovery are explicitly deferred. Revisit them only if product requirements change to require jobs or history to survive process exit.

## Explicit human boundary

The MVP worker does not push an implementation branch or create/update a PR; those writes require the separate reconciliation gate above. Merge, release, and deployment are unavailable. Successful process exit is not an independent correctness verdict; verification evidence and limitations should remain reviewable by a human.
