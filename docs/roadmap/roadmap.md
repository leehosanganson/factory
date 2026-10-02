# Factory roadmap

## Product direction

Factory is a RESTful job service for agent-driven repository work. A client submits a bounded job request; Factory executes it against an operator-approved repository, records its lifecycle, and creates or updates a pull request through the configured code-repository provider. Humans retain authority over merge, release, and deployment. Verification evidence and limitations remain visible; agent or process success is not an independent correctness verdict.

The target MVP has one service and one job lifecycle—not separate REST-task and autonomous issue-to-PR products. Its core path is:

**request → durable admission → bounded execution → verification → provider PR create/update → inspectable outcome**

Issue details may be supplied as context or added through a future intake adapter, but issue polling/reconciliation is not a separate MVP. Provider-neutral interfaces keep repository hosting replaceable; each supported provider must implement the same scoped branch/PR behavior.

## Persistence and runtime

The service starts with a simple in-memory backend for local development, demonstrations, and tests. It is intentionally volatile: process restart loses jobs, history, idempotency records, and active execution state. It must never be described as durable across restarts.

Persistence is behind a backend interface. An optional SQL backend is recommended for real deployments that need restart durability, retained job history, and reliable idempotency. The SQL design must define schema/migrations, transactions, retention, concurrency/worker ownership, and recovery of interrupted jobs. A deployment requiring restart durability must configure SQL (or another explicitly approved durable backend); memory mode is not an implicit fallback when the configured durable store fails.

The first deployment is a single Factory server with bounded workers and operator-configured repositories, agent harness, credentials, and limits. Local-process execution and containers are not security sandboxes. Horizontal scaling, a fleet coordinator, remote worker placement, and a web UI are later options, not MVP prerequisites.

## Pull-request and human boundary

Creating or updating a PR with the configured code-repository provider is a required part of a successful implementation job, not an optional publication feature. A job must not report success until the provider operation is confirmed and its PR identity/URL is recorded. Retries after timeouts or process interruption must reconcile live provider state before repeating a write; uncertain outcomes are surfaced for recovery rather than blindly creating another branch or PR.

Factory may commit and push only within the job's approved repository and scope. It does not merge PRs, release artifacts, or deploy software. Approval gates, policy escalation, bounded retries, visible verification results, and safe cancellation preserve human control without making PR creation itself an approval decision.

## Current implementation versus target

Factory currently has a Go CLI and a local REST job server. The server accepts authenticated bounded requests, runs configured multi-stage workflows in isolated workspaces, and exposes status/history. Its registry and idempotency records are in memory and are lost on restart. It does not yet create or update PRs through a configurable code-repository provider. These are known gaps to the target MVP, not separate product directions.

The CLI's `implement`, `tidy`, `monitor`, detached jobs, and `work` commands remain available and are documented under [implemented features](../features/README.md). They are current interfaces, not additional MVP architectures. The local `work` issue-observation commands do not start an autonomous issue-to-PR worker.

## Delivery sequence

1. **Define durable job semantics:** canonical job/request and lifecycle, backend interface, idempotent admission, bounded history, retention, worker ownership, cancellation, and recovery behavior.
2. **Add SQL persistence:** transactions and migrations for jobs, events, idempotency, and execution state; test restart recovery and concurrent admission. Keep the in-memory backend for tests and local development.
3. **Complete REST job operation:** ensure status/history, safe errors, limits, and recovery semantics behave consistently across backends. A job must have an inspectable outcome after restart when SQL is configured.
4. **Add provider-neutral repository operations:** configure a code-repository provider and credentials; implement scoped branch and PR create/update, idempotency/reconciliation for uncertain writes, and persist the associated PR identity.
5. **Integrate and validate the full path:** execute the existing workflow through the service, verify changes, create/update the PR, exercise cancellation/failures/restarts, and prove that no job reports success without a confirmed PR outcome.
6. **Operational hardening:** document deployment, secret handling, retention/backups, resource bounds, observability, and the limits of local-process/container isolation.

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
