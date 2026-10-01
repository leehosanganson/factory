# Proposed REST server MVP contract

## Status and scope

**Proposed, not implemented.** Factory currently has REST configuration, request validation, an authenticated HTTP handler, and process-local job admission/inspection foundations, but no REST server entry point or remote job execution integration. This document records the planned single-host service and result lifecycle; it is not an implementation claim or delivery commitment.

The service accepts bounded task requests for configured repository aliases and executes the existing multi-stage Factory requirements, implementation, review, and documentation workflow, followed by configured verification checks. A job may launch multiple configured harness subprocesses across stages; it is not restricted to one literal harness invocation. A job-wide deadline and aggregate output limit bound all stages and subprocesses. The listener defaults to loopback (`127.0.0.1:8080`); operators may explicitly configure another address. Callers cannot select an executable or submit command arguments, shell text, or a generic command. A bounded in-memory queue and configured worker concurrency limit govern execution. Every API-key holder is trusted with the configured server account's local-process authority: this design is not a sandbox and does not claim isolation from malicious callers. Publication is forbidden in this MVP: jobs must not create commits, push branches, create/update PRs, or use a PAT. Merge, release, and deployment remain unavailable. See the [proposed execution-mode architecture](execution-modes.md) for the server/executor boundary and local-process MVP direction.

## Authentication and GitHub authority

- All `/v1/jobs` routes require one configured shared bearer API key. `/healthz` and `/readyz` are the only unauthenticated routes and return minimal health/readiness information without sensitive details. All key holders share the same trust domain and can inspect every retained job; there is no per-caller identity, isolation, or ownership boundary. Do not accept caller-supplied principal identities. The key must not be exposed to the harness.
- The MVP does not select or require a GitHub credential and does not perform branch pushes or PR create/update. A future design for those writes must define credential custody and least privilege as part of the restart-safe, cross-process reconciliation gate; OAuth, GitHub App, and shared-PAT approaches are not selected by this contract.
- The [credential-custody proposal](credential-custody-design.md) describes possible future OAuth grant custody, not an MVP credential requirement.

## Client, request, and admission

A remote client (for example, a Hermes agent) submits a normal authenticated HTTP task request to Factory Server. The client is not a worker manager and does not launch or control the configured coding harness directly; Factory Server owns job admission, scheduling, execution, status, and history. Publication is explicitly excluded from the MVP as described below. Client technology is not otherwise part of the server contract.


A proposed `POST /v1/jobs` request contains:

```json
{
  "repository": "widget",
  "task": "Add a bounded note to the README; do not change code.",
  "issue": 42
}
```

- `repository` is a configured repository alias, not an arbitrary repository URL. Server configuration maps each alias to an approved local Git checkout root and its repository identity; callers cannot submit filesystem paths. `task` is a required nonblank task description. `issue` is optional supplemental context and accepts only a positive issue number in the alias's configured repository. Issue URLs are excluded from this MVP.
- The task description—not an issue—is the requested work; the optional issue number does not start autonomous polling, reconciliation, or issue tracking.
- The request must not accept arbitrary shell, command, or executable fields.
- Bound request body and task sizes before decoding/processing. Require `Content-Type: application/json` for job submission. Reject malformed JSON, unknown fields, invalid UTF-8, blank task descriptions, invalid issue numbers, and unknown repository aliases. The implementation uses configured request/task limits; oversized request bodies are rejected before admission.
- Admission validates and creates the job in the process-local registry and bounded in-memory queue before returning acceptance. Require a client-generated `Idempotency-Key` header. Repeating the same key with an identical normalized request while retained returns the existing job; reusing a key with a different request returns `409 Conflict`. Idempotency keys are forgotten when the process exits or their terminal record is evicted.
- Configure a maximum total registry record count and a maximum event count per job. The proposed MVP values are `max_records: 1000` and `max_events_per_job: 200`; the proposed task limit is `task_bytes: 262144`. These are planned configuration values, not claims about current defaults or runtime configuration. Never evict queued or active jobs. At the record cap, evict the oldest terminal job records as needed; if all retained records are queued/active and the cap is reached, reject admission. Eviction makes prior status/history return `404` and forgets its idempotency key. When an individual job exceeds its event cap, drop oldest events and mark its history as truncated.
- If the bounded pending queue has no capacity, return `503 Service Unavailable` with a stable capacity error and do not create a job record. A later client retry must reuse its `Idempotency-Key`.

A successful admission returns `202 Accepted` with an opaque job ID, status snapshot, and links to status/history; an idempotent replay returns that same job rather than queueing another. The supported job routes are `POST /v1/jobs`, `GET /v1/jobs/{id}`, and `GET /v1/jobs/{id}/history`. These routes require bearer authentication before route-specific handling. `/healthz` reports process liveness and `/readyz` reports whether configuration is valid and the server is accepting requests; both are unauthenticated, minimal, and disclose no sensitive data. Readiness is not a claim that workers are idle or that an external dependency is healthy.

Representative JSON DTOs (RFC 3339 UTC timestamps; optional `issue` is omitted when absent):

```json
{
  "job": {
    "id": "7ac42bd3-67fb-47be-b4c2-68269438d4bd",
    "request": {"repository": "widget", "task": "Add a bounded note.", "issue": 42},
    "status": "queued",
    "created_at": "2026-10-02T12:00:00Z",
    "updated_at": "2026-10-02T12:00:00Z"
  },
  "replayed": false,
  "links": {
    "self": "/v1/jobs/7ac42bd3-67fb-47be-b4c2-68269438d4bd",
    "history": "/v1/jobs/7ac42bd3-67fb-47be-b4c2-68269438d4bd/history"
  }
}
```

`GET /v1/jobs/{id}` returns the `job` object itself. The submitted repository, task, and optional issue are included in both admission and status responses. Task text is intentionally visible to every key holder: holders share the trusted server-account authority and are not isolated from one another. This is not a per-user/private task store.

```json
{
  "job_id": "7ac42bd3-67fb-47be-b4c2-68269438d4bd",
  "events": [
    {"at": "2026-10-02T12:00:00Z", "type": "queued", "message": "Job admitted"},
    {"at": "2026-10-02T12:01:00Z", "type": "running", "message": "Job started"}
  ],
  "truncated": false
}
```

History fields follow the current `internal/restjobs` model: bounded timestamp, type, and optional message entries, plus a truncation flag. The manager's lifecycle values are `queued`, `running`, `succeeded`, `failed`, and `canceled`; its corresponding built-in event types use those values. Do not add HTTP-only lifecycle states or promise other event types. Each event has a type of at most 64 bytes and an optional message of at most 4,096 bytes; history is bounded per job by server configuration, drops oldest events on overflow, and sets `truncated`. Sanitize messages before exposing them; never include agent output, host paths, executable names, credentials/secrets, or stack traces. Since API credentials are shared, any valid key holder can inspect any retained job. Status/history for unknown or evicted jobs returns `404`; history reports whether older events were truncated.

Path matching is exact. Only `GET /healthz` and `GET /readyz` are supported for probe paths; other methods return `405 Method Not Allowed` with `Allow: GET`. Unsupported methods on recognized job paths return `405` with `Allow`; unknown paths return `404`. All paths under `/v1/jobs`, including unsupported methods and unknown job subpaths, pass bearer authentication before route/method resolution. Do not redirect API routes.

## Process-local jobs and lifecycle

- Job records, execution status, idempotency keys, and history exist only in the running server process's memory. No PostgreSQL, SQL migration, file-backed job store, or durable job history is required by this MVP.
- The service runs accepted jobs in bounded in-process workers. A worker executes the existing multi-stage Factory requirements/implementation/review/documentation workflow and configured verification checks; multiple configured harness subprocesses may be started during one job. Subprocesses use argument-vector invocations rather than caller-controlled shell text. Each job receives a separate workspace and agent session, with one job-wide deadline and aggregate output limit across every stage and subprocess. Every subprocess runs with the configured server account's local-process authority; it is not a sandbox. Worker execution and job tracking share one process lifetime, so this is not a distributed or horizontally scalable service.
- A process crash or forced termination loses all job records, history, idempotency data, and knowledge of active execution. Clients must resubmit after restart; the server cannot resume jobs or recover their status. A recognized successful workspace may remain on disk for inspection, but its completion marker is cleanup metadata only and does not recover the job record, history, or execution status. The MVP performs no branch push or PR create/update; publication is forbidden. No external side effect may be blindly retried; before any future retry, a separately designed restart-safe, cross-process reconciliation gate must resolve what happened or require human inspection.
- On graceful shutdown, stop accepting requests, cancel active jobs and their harness processes, allow bounded cancellation/cleanup, then exit. Successful workspaces/results remain available for human local inspection after completion. On verified success, write protected, validated completion metadata in the workspace parent recording the successful outcome and completion time. Automatic cleanup is permitted only when that metadata establishes an age strictly greater than 24 hours. A periodic sweep runs at startup and in the background; therefore deletion may occur later than the 24-hour threshold, not exactly at it. It removes only well-formed, recognized successful-result directories, after rejecting symlink/path escapes and revalidating against safe races. Failed, canceled, unknown, orphaned, or unparseable directories are retained for operator inspection, including after restart. A failed check, unsafe path/race, or missing/corrupt/lost external completion metadata must fail closed and retain the directory; loss of metadata can therefore leave successful results uncollected until operator action. Remove only through validated paths and only after workers exit. All process-local job records disappear on exit, including completed history.
- Do not claim durable acceptance, restart recovery, exactly-once execution, or recovery of process/agent memory. This MVP explicitly accepts these limitations in exchange for avoiding a database and migrations.

## Local deployment testing

Provide a Docker Compose setup for local deployment testing once the server entry point exists. It should run a single Factory Server with its configured local-subprocess executor, mount configured repository checkouts and protected API-key files, and configure the harness server-side. Compose does not run a per-job container and is not a security sandbox or production-hardening claim. Future ephemeral Docker-container or Kubernetes-Pod execution remains operator-configured and must retain the same API/job contract; neither is an isolation guarantee against malicious API-key holders. The remote caller can be any HTTP client, including an agent such as Hermes; credentials must travel over an appropriately protected connection in deployed use.

## Ordered implementation slices

These slices are dependency-ordered; they are planning gates, not claims of existing behavior or delivery dates. Keep each independently reviewable and preserve the CLI's current behavior.

1. **Server configuration and runtime boundaries.** Define server-only configuration for listener, repository aliases, checkout roots, harness executable/argument template, resource limits, and protected API-key file path. Validate exact Git roots and strict API-key file ownership/mode/no-symlink rules. Add startup loading and ensure the coding-harness child environment excludes the API key. No HTTP or job execution yet.
2. **Volatile job manager.** Implement a concurrency-safe process-local registry, bounded pending queue and worker slots, stable opaque IDs, in-process idempotency, status transitions, and bounded history. Define queue-full/repeated-idempotency behavior and tests for concurrent admission, status/history inspection, and memory/history limits. No database, disk queue, or recovery behavior.
3. **Authenticated HTTP API.** Add a separately testable `net/http` handler for unauthenticated minimal `GET /healthz` and `GET /readyz`, `POST /v1/jobs`, `GET /v1/jobs/{id}`, and bounded `GET /v1/jobs/{id}/history`. Default the listener to loopback. Require the shared bearer API key on every `/v1/jobs` path; validate request size/schema/UTF-8/unknown fields and issue-number semantics. Implement the error envelope and mappings below. Verify authorization, exact method/path behavior, status codes, safe errors, redaction, queue-full handling, and DTOs with handler tests.
4. **In-process worker and harness lifecycle.** Connect accepted jobs to bounded workers. Each job gets a distinct server-created workspace/session; Factory's multi-stage workflow may start multiple server-selected harness subprocesses, all bounded by a job-wide deadline and aggregate output limit. Invocations use no caller-controlled shell parsing. Processes run with the server account's local-process authority; this is not a sandbox. Implement cancellation propagation, process-group cleanup, graceful shutdown admission stop, bounded worker exit, and safe workspace handling. Preserve successful results for human inspection and implement protected completion metadata plus startup/periodic age-based cleanup as specified above. Sweep only well-formed recognized successes older than 24 hours; reject symlinks/path escapes and safe-races, and retain failed/canceled/unknown/orphaned/unparseable directories. Test competing jobs, multi-invocation limits, cancellation, shutdown, cleanup failure, and workspace handling with fake harness executables.
5. **Factory workflow integration.** Adapt the existing multi-stage requirements/implementation/review/documentation workflow and configured verification checks behind an injectable job execution interface while preserving CLI state and behavior. A job may invoke the configured harness multiple times across workflow stages; enforce one job-wide deadline and aggregate output limit rather than treating a subprocess invocation as the job. Validate configured checkout root, isolate task workspaces, and expose only sanitized status/history—not raw unfiltered logs or host paths. Test supported harness contracts where practical; unsupported differences must fail clearly rather than run caller-supplied commands.
6. **Safe side-effect design gate (deferred from MVP).** Before enabling shared PAT use, branch push, or PR create/update, design and test restart-safe cross-process reconciliation for uncertain outcomes. Specify credential custody, idempotent association, and operator escalation when state cannot be established. Do not blindly retry external side effects. This is a separate gate, not an MVP implementation slice.
7. **Local Compose and end-to-end acceptance.** Package one Factory Server with its configured local-subprocess executor in Docker Compose for local testing, without PostgreSQL. Mount explicit repository/config/API-key inputs; document image build, port exposure, and that Compose/containerization is not a sandbox. Exercise authenticated submission through status/history, and verify process restart loses registry state as specified and graceful shutdown cancels workers safely.

**MVP gate:** the full path from HTTP admission through the existing multi-stage requirements/implementation/review/documentation workflow and configured verification checks to inspectable in-process status/history and a completed, verified task must work under configured job-wide deadline and aggregate-output bounds. A job may use multiple harness subprocesses. Successful workspaces/results remain locally inspectable and are eligible for automatic cleanup only after protected completion metadata establishes an age greater than 24 hours; startup/periodic cleanup is fail-closed and preserves ambiguous or unsafe results for operator inspection. Process exit loses job records; clients resubmit after restart; no publication or external side effect is allowed; authentication is tested; merge, release, and deployment are unavailable; the documented local Compose flow works without a database.

## Limits, errors, and deferred controls

- Job-route failures use one stable JSON envelope: `{"error":{"code":"invalid_request","message":"Request is invalid."}}`. `message` is safe, generic text; clients branch on `code`, not message. Never return stack traces, API keys, authorization headers, raw provider payloads, sensitive filesystem paths, executable names, or agent output; never log API-key values.
- Stable code-to-status mapping: `400 malformed_json`, `400 unknown_field`, and `400 invalid_request` (including invalid/unknown repository alias, blank or invalid task, invalid issue, invalid UTF-8, and malformed/missing idempotency key); `401 unauthenticated`; `404 not_found` (unknown/evicted job or unknown path); `409 idempotency_conflict`; `503 queue_full` and `503 registry_full`; `405 method_not_allowed`; `415 unsupported_media_type`; `413 body_too_large`; `500 internal_error`. Do not return validation details that reveal server configuration. Responses for `/healthz` and `/readyz` are minimal and contain no error detail or sensitive data.
- `GET /healthz` returns `200 {"status":"ok"}` while the process is alive. `GET /readyz` returns `200 {"status":"ready"}` only while configuration is valid and the server is accepting requests; while not ready (including graceful shutdown), it returns `503 {"status":"not_ready"}`. Both are unauthenticated and disclose no job, configuration, host, or dependency details.
- Rate limiting is deferred. This proposal does not promise rate limits, `429`, or a `Retry-After` contract. Request/body input bounds and bounded in-memory job/history capacity remain necessary and do not constitute rate limiting.
- Concurrency, queue capacity, total retained job count, per-job event count, task/body bounds, job-wide deadline, aggregate output limit, harness executable/arguments, and resource bounds are server configuration, not caller-controlled job fields. The proposed configuration values include `max_records: 1000`, `max_events_per_job: 200`, and `task_bytes: 262144`. The configured harness may be launched multiple times within one job for the multi-stage workflow; process-pool/session reuse is not part of this direction.
- PostgreSQL-backed persistence, database migrations, backup/restore, and durable restart recovery are explicitly deferred. Revisit them only if product requirements change to require jobs or history to survive process exit.

## Explicit human boundary

The MVP worker does not push an implementation branch or create/update a PR; those writes require the separate reconciliation gate above. Merge, release, and deployment are unavailable. Successful process exit is not an independent correctness verdict; verification evidence and limitations should remain reviewable by a human.
