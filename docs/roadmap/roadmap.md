# Factory roadmap

## Product direction

Factory is a RESTful job service for agent-driven repository work. A client submits a bounded job request; Factory executes it against an operator-approved repository, records its lifecycle, and creates or updates a pull request through the configured code-repository provider. Humans retain authority over merge, release, and deployment. Verification evidence and limitations remain visible; agent or process success is not an independent correctness verdict.

The target MVP has one service and one job lifecycle—not separate REST-task and autonomous issue-to-PR products. Its core path is:

**request → durable admission → bounded execution → verification → provider PR create/update → inspectable outcome**

Issue details may be supplied as context or added through a future intake adapter, but issue polling/reconciliation is not a separate MVP. Provider-neutral interfaces keep repository hosting replaceable; each supported provider must implement the same scoped branch/PR behavior.

## Persistence and runtime

The service starts with a simple in-memory backend for local development, demonstrations, and tests. It is intentionally volatile: process restart loses jobs, history, idempotency records, and active execution state. It must never be described as durable across restarts.

Persistence is behind a backend interface. The implemented optional SQLite backend provides restart-persistent job/history/idempotency state, transactional admission, bounded retention, and conservative startup recovery for a single server; it fails closed when explicitly configured storage is unavailable. Queued jobs resume, while interrupted running jobs are retained for explicit operator disposition or read-only provider reconciliation and are never replayed. Without persisted provider attempts/outcomes, an operator may mark an interrupted SQLite job failed or canceled; with a persisted provider identity and retained workspace, the operator may confirm an exact matching open PR through a read-only lookup. Mismatches, provider outages, and persistence failures remain unresolved for operator investigation. Durable worker leases/checkpoints, multi-host ownership, automatic recovery/replay, and fleet coordination are not implemented. Deployments requiring restart durability must configure SQLite; memory mode is not an implicit fallback when the configured durable store fails.

The first deployment is a single Factory server with bounded workers and operator-configured repositories, agent harness, credentials, and limits. Local-process execution and containers are not security sandboxes. Horizontal scaling, a fleet coordinator, remote worker placement, and a web UI are later options, not MVP prerequisites.

## Pull-request and human boundary

Creating or updating a PR with the configured code-repository provider is a required part of a successful implementation job, not an optional publication feature. A job must not report success until the provider operation is confirmed and its PR identity/URL is recorded. Retries after timeouts or process interruption must reconcile live provider state before repeating a write; uncertain outcomes are surfaced for recovery rather than blindly creating another branch or PR.

Factory may commit and push only within the job's approved repository and scope. It does not merge PRs, release artifacts, or deploy software. Approval gates, policy escalation, bounded retries, visible verification results, and safe cancellation preserve human control without making PR creation itself an approval decision.

## Current implementation versus target

Factory has a Go CLI and a local REST job server. The server accepts authenticated bounded requests, executes configured workflows in isolated workspaces, and exposes status/history. It supports volatile memory or optional SQLite persistence and configured GitHub PR publication. SQLite resumes queued jobs after restart, retains interrupted `running` jobs for operator investigation without replay, and exposes their count in authenticated aggregate status. The authenticated bodyless operator routes can disposition an eligible interrupted SQLite job as failed/canceled only when no provider attempt/outcome exists, or reconcile an eligible interrupted/failed job through read-only provider confirmation of its persisted identity and retained workspace. Unresolved, mismatched, unavailable, or unpersistable results remain operator recovery cases. These actions do not rerun the harness or write to the provider. Setup, readiness, backup/restore, recovery, and operational-status guidance is documented, and the server provides readiness probes, a consistent database backup command, and count-only aggregate status. Backup does not include separate workspace trees. Merged PR #128 adds deterministic tests of the production GitHub adapter against GitHub REST-shaped `httptest` responses, including accepted-create/lost-response reconciliation; process-level provider recovery tests use a fake provider. Neither establishes behavior against a live GitHub account. Merged PR #155 adds a manual-only, guarded live App harness, and merged PR #157 adds atomic compare-and-swap cleanup; these do not establish a live account/provider validation run. That run remains unverified and owner-gated by open issue #134; #135 is also open. Merged PR #131 adds SQLite backup/restore process E2E coverage, not a production restore drill. Merged PR #133 superseded closed issue #117 and adds startup logging of the selected persistence mode after listener readiness; it does not expose the SQLite path or secrets. No automatic recovery/replay of interrupted running work, worker leases/checkpoints, or fleet ownership is implemented. These limits are not separate product directions. The separate CLI now provides first-use `factory doctor` diagnostics (#138) and a process test for detached-job reconnect via fresh `get`/`logs` commands with retained recovery artifacts (#140); this evidence does not mean Factory replaces Pi or completes the broader primary-engine vision.

The CLI's `implement`, `tidy`, `monitor`, detached jobs, and `work` commands remain available and are documented under [implemented features](../features/README.md). They are current interfaces, not additional MVP architectures. The local `work` issue-observation commands do not start an autonomous issue-to-PR worker.

## Delivery sequence

1. **Define durable job semantics (implemented):** canonical job/request and lifecycle, backend interface, idempotent admission, bounded history, retention, cancellation, and conservative recovery behavior. Durable worker leases, checkpoints, and multi-host ownership are not provided.
2. **Add SQL persistence (implemented):** SQLite transactions and migrations for jobs, events, idempotency, and execution state; configured-store failures fail closed. Concurrent admission and restart behavior are tested. Queued jobs resume after restart; interrupted running jobs remain inspectable and are not replayed.
3. **Complete REST job operation (implemented):** status/history, safe errors, limits, readiness, and recovery semantics are available across the supported backends; SQLite retains inspectable records across restart. #124 adds authenticated operator actions for explicit interrupted-job disposition and read-only reconciliation without replay.
4. **Add repository-provider operations (implemented):** configured GitHub operations publish a scoped branch and create/update a PR, recording the confirmed PR identity. Eligible interrupted-running or terminal failed SQLite jobs with persisted provider identity and retained workspaces can be reconciled through read-only provider confirmation; uncertain writes are not blindly retried.
5. **Integrate and validate the request-to-PR path (core gates implemented; validation remains):** process-level tests cover request-to-PR execution, concurrent admission, duplicate/idempotent requests, shutdown cancellation, queued-job restart resumption, interrupted-running retention, and fake-provider failure/lost-response and explicit operator reconciliation. Merged PR #128 adds production GitHub adapter tests against GitHub REST-shaped `httptest` endpoints for an accepted PR create with a lost response, failed lookups, exact read-only reconciliation mapping, token redaction, and no duplicate write. These deterministic tests do not establish live GitHub account/repository behavior; a live account/provider run remains unverified and owner-gated by open issue #134 (#135 is also open). Merged PR #155 adds a manual-only, guarded live App harness, while merged PR #157 adds atomic compare-and-swap cleanup; neither constitutes that live account/provider validation. Automatic recovery/reconciliation of interrupted running jobs, worker leases/checkpoints, and fleet ownership remain unimplemented.
6. **Operational hardening (core documentation and support implemented; validation remains):** setup/configuration and security boundaries, health/readiness, SQLite recovery and retention, a consistent database backup command with restore guidance, and authenticated count-only operational status are documented and shipped. PR #131's backup/restore process E2E does not constitute a production restore drill. Merged PR #133 superseded #117 and adds startup logging of the selected persistence mode after listener readiness; it does not disclose the SQLite path or secrets. Database backup excludes separate workspaces. This is operational support, not general metrics/telemetry; unresolved interrupted work still needs operator investigation.

These are dependency gates, not dates. CI/CD feedback, issue polling/reconciliation, provider events, additional code hosts, fleet coordination, reusable workflow catalogs, and deployment orchestration can be considered after the single-server request-to-PR lifecycle is proven.

## Explicit exclusions

- No merge, release, or deployment authority.
- No general arbitrary-command endpoint or caller-selected executable.
- No claim that an agent, process, Nix shell, or container is sandboxed.
- No second MVP for autonomous issue polling; it can later feed the same job API/lifecycle.
- No horizontal fleet or distributed broker requirement for the initial service.

See the [REST job contract](rest-api-contract.md) for API and reliability requirements and [execution modes](execution-modes.md) for the server/executor boundary.

## Feedback loop

Use observed user and operational feedback to revise the direction. Keep observations distinct from verified causes, preserve evidence, and update documentation when behavior or product decisions change. This roadmap is directional, not a release-date commitment.
