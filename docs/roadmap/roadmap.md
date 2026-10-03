# Factory roadmap

## Product direction

Factory is a RESTful job service for agent-driven repository work. A client submits a bounded job request; Factory executes it against an operator-approved repository, records its lifecycle, and creates or updates a pull request through the configured code-repository provider. Humans retain authority over merge, release, and deployment. Verification evidence and limitations remain visible; agent or process success is not an independent correctness verdict.

The target MVP has one service and one job lifecycle—not separate REST-task and autonomous issue-to-PR products. Its core path is:

**request → durable admission → bounded execution → verification → provider PR create/update → inspectable outcome**

Issue details may be supplied as context or added through a future intake adapter, but issue polling/reconciliation is not a separate MVP. Provider-neutral interfaces keep repository hosting replaceable; each supported provider must implement the same scoped branch/PR behavior.

## Persistence and runtime

The service starts with a simple in-memory backend for local development, demonstrations, and tests. It is intentionally volatile: process restart loses jobs, history, idempotency records, and active execution state. It must never be described as durable across restarts.

Persistence is behind a backend interface. The implemented optional SQLite backend provides restart-persistent job/history/idempotency state, transactional admission, bounded retention, and conservative startup recovery for a single server; it fails closed when explicitly configured storage is unavailable. Queued jobs resume, while interrupted running jobs are retained for operator investigation rather than replayed. Durable worker leases/checkpoints, multi-host ownership, and automatic recovery of interrupted running work are not implemented. Deployments requiring restart durability must configure SQLite; memory mode is not an implicit fallback when the configured durable store fails.

The first deployment is a single Factory server with bounded workers and operator-configured repositories, agent harness, credentials, and limits. Local-process execution and containers are not security sandboxes. Horizontal scaling, a fleet coordinator, remote worker placement, and a web UI are later options, not MVP prerequisites.

## Pull-request and human boundary

Creating or updating a PR with the configured code-repository provider is a required part of a successful implementation job, not an optional publication feature. A job must not report success until the provider operation is confirmed and its PR identity/URL is recorded. Retries after timeouts or process interruption must reconcile live provider state before repeating a write; uncertain outcomes are surfaced for recovery rather than blindly creating another branch or PR.

Factory may commit and push only within the job's approved repository and scope. It does not merge PRs, release artifacts, or deploy software. Approval gates, policy escalation, bounded retries, visible verification results, and safe cancellation preserve human control without making PR creation itself an approval decision.

## Current implementation versus target

Factory has a Go CLI and a local REST job server. The server accepts authenticated bounded requests, executes configured workflows in isolated workspaces, and exposes status/history. It supports volatile memory or optional SQLite persistence and configured GitHub PR publication. SQLite resumes queued jobs after restart, retains interrupted `running` jobs for operator investigation without replay, and exposes their count in authenticated aggregate status. An explicit authenticated reconciliation action can confirm an existing PR read-only for eligible terminal failed SQLite jobs with a recorded provider attempt; it does not resume interrupted running work. Setup, readiness, backup/restore, and operational-status guidance is documented, and the server provides readiness probes, a consistent backup command, and count-only aggregate status. Remaining target-MVP validation gaps include automatic recovery/reconciliation of interrupted running jobs and live account-level GitHub validation. Deterministic tests exercise the production GitHub adapter against GitHub REST-shaped `httptest` responses, including accepted-create/lost-response reconciliation; process-level provider recovery tests use a fake provider. No live GitHub write, shared/production repository, or live account validation is part of the required test suite. These limits are not separate product directions.

The CLI's `implement`, `tidy`, `monitor`, detached jobs, and `work` commands remain available and are documented under [implemented features](../features/README.md). They are current interfaces, not additional MVP architectures. The local `work` issue-observation commands do not start an autonomous issue-to-PR worker.

## Delivery sequence

1. **Define durable job semantics (implemented):** canonical job/request and lifecycle, backend interface, idempotent admission, bounded history, retention, cancellation, and conservative recovery behavior. Durable worker leases, checkpoints, and multi-host ownership are not provided.
2. **Add SQL persistence (implemented):** SQLite transactions and migrations for jobs, events, idempotency, and execution state; configured-store failures fail closed. Concurrent admission and restart behavior are tested. Queued jobs resume after restart; interrupted running jobs remain inspectable and are not replayed.
3. **Complete REST job operation (implemented):** status/history, safe errors, limits, readiness, and recovery semantics are available across the supported backends; SQLite retains inspectable records across restart.
4. **Add repository-provider operations (implemented):** configured GitHub operations publish a scoped branch and create/update a PR, recording the confirmed PR identity. Eligible failed SQLite jobs with recorded provider attempts can be reconciled through a read-only provider confirmation; uncertain writes are not blindly retried.
5. **Integrate and validate the request-to-PR path (core gates implemented; validation remains):** process-level tests cover request-to-PR execution, concurrent admission, duplicate/idempotent requests, shutdown cancellation, queued-job restart resumption, interrupted-running retention, and fake-provider failure/lost-response and explicit operator reconciliation. Production GitHub adapter tests use the real HTTP adapter against `httptest` endpoints with GitHub REST request/response shapes to verify an accepted PR create whose response is lost, failed lookups, exact read-only reconciliation mapping, token redaction, and no duplicate write. These deterministic tests are required and do not establish live GitHub/account behavior. Live account-level validation is deferred: there is no safe disposable-repository mechanism in this scope, and no test creates, pushes, or cleans up remote resources. Automatic recovery/reconciliation of interrupted running jobs also remains unimplemented.
6. **Operational hardening (core documentation and support implemented):** setup/configuration and security boundaries, health/readiness, SQLite recovery and retention, a consistent backup command with restore guidance, and authenticated count-only operational status are documented and shipped. This is operational support, not a completed deployment/restore drill or general metrics/telemetry; interrupted running work still needs manual investigation.

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
