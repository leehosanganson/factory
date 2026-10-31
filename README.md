# Factory

Factory is a Go service and CLI for agent-driven repository jobs. Its product direction is one RESTful job service: accept a bounded request, run the configured workflow, verify the result, and create or update a pull request through the configured code-repository provider. Factory does not merge, release, or deploy.

The REST server supports volatile memory mode and optional SQLite or PostgreSQL persistence, with a configured GitHub provider for job PR publication and authenticated, no-replay operator recovery actions for eligible interrupted durable jobs: failed/canceled disposition without provider attempts, or read-only provider confirmation for persisted provider identity. A dedicated `factory issue-poller` process can admit allowlisted GitHub issues into the same PostgreSQL job queue; it does not introduce a second execution queue or issue lifecycle. `factory server bundle create|verify|inspect|restore` packages a stopped-server SQLite snapshot with retained per-job workspace artifacts into a private, verifiable portable bundle. It does not replay work or preserve external provider state, and bundle verification is not a production restore drill. See the [REST server operations guide](docs/features/rest-server-operations.md) for probe semantics, backup/restore, restart recovery, reconciliation, and operator limits; the [REST job contract](docs/roadmap/rest-api-contract.md) describes remaining target-MVP requirements. A separate manual-only GitHub App live-provider workflow is opt-in and restricted to a protected disposable sandbox. Invoke it only from Actions on `main` with explicit sandbox confirmation after #134 provisioning; the [operator guide](docs/features/github-app-live-test.md) lists required `FACTORY_LIVE_GITHUB_APP_ID`, `FACTORY_LIVE_GITHUB_PRIVATE_KEY`, and `FACTORY_LIVE_GITHUB_INSTALLATION_ID` secrets and the exact repository variables. Live validation remains unverified until provisioning and a successful manual run.

## Documentation

- [Implemented features](docs/features/README.md) — current CLI and REST server capabilities.
- [Primary Factory-to-Pi workflow](docs/features/primary-workflow.md) — setup, implementation, detached-job handoff, and recovery.
- [REST server operations](docs/features/rest-server-operations.md) — health/readiness, backup/restore, restart recovery, limits, and trusted-network operations.
- [REST operator dashboard](docs/features/rest-web-ui.md) — optional embedded dashboard and control-plane container image.
- [REST job client](docs/features/rest-client.md) — separate protected client configuration and remote job commands.
- [Autonomous GitHub issue poller](docs/features/issue-poller.md) — dedicated PostgreSQL-backed admission into the shared REST job queue.
- [Supervised systemd deployment](docs/features/rest-server-systemd.md) — a single-host service-user unit and lifecycle walkthrough.
- [Manual GitHub App live-provider test](docs/features/github-app-live-test.md) — protected opt-in sandbox validation; manual Actions invocation, required secrets/variables including the explicitly approved organization identity check, cleanup limits, and unverified status pending issue #134 provisioning.
- [REST job contract](docs/roadmap/rest-api-contract.md) — target request-to-PR lifecycle, persistence modes, and delivery gates.
- [Agent guidance](AGENTS.md) — repository workflow and verification conventions.
- [Factory operating skill](.agents/skills/factory/SKILL.md) — agent workflow guidance and intended product direction.
- [Documentation skill](.agents/skills/documentation/SKILL.md) — documentation conventions.
- [Contributing](CONTRIBUTING.md) — branch naming and pull request conventions.
- [GitHub issues](https://github.com/leehosanganson/factory/issues) — categorized, actionable product follow-ups and tracked work.
- [Roadmap](docs/roadmap/roadmap.md) — unified product direction and later possibilities.
- [Factory-unit MVP architecture](docs/roadmap/factory-unit-mvp.md) — target controller, Thread, Job, Factory-unit, scheduler, and harness boundaries; not shipped behavior.
- [Thread model](docs/roadmap/thread-model.md) — target ingress, Job membership, aggregate status, notification, and explicit user completion rules.
- [Issue context integration](docs/roadmap/rest-job-lifecycle.md) — broader issue-intake direction beyond the implemented poller.

## Issue tracking

Actionable product follow-ups and categorized usability findings are tracked in [GitHub Issues](https://github.com/leehosanganson/factory/issues). The former `docs/issues/` finding and session-observation archive was migrated to issue #292 and existing focused trackers; it is no longer maintained in this repository.

## First task

See the [primary Factory-to-Pi workflow](docs/features/primary-workflow.md) for setup diagnostics, project-appropriate checks, implementation, job handoff, and recovery guidance.

## Build and verify

Go 1.26.3 or later is required. Linux and macOS are supported. The Makefile provides `make build`, `make test`, `make fmt`, `make vet`, `make clean`, and `make help` (which lists the targets and their descriptions).

```sh
make build
./bin/factory help
make test
make vet
```

The binary is written to `./bin/factory`. Configuration and state are stored outside the target repository by default; see [`config.json.example`](config.json.example) and the [feature documentation](docs/features/README.md). The optional CLI setting `detached_job_max_concurrency` caps active detached implementation, tidy, and monitor jobs sharing state (1–64); omission preserves unlimited admission. The separate `factory issue-poller` process admits allowlisted GitHub issues into the canonical PostgreSQL REST job Store; see the [poller guide](docs/features/issue-poller.md). It is separate from `parallel_implementation.max_concurrency`, which controls subtasks within one implementation workflow. CI builds, tests, and vets on Linux and macOS. Releases are created separately from explicit stable version tags; merging to `main` does not itself publish a release.

## Local REST server

The current server accepts authenticated job requests, executes configured workflows with the server account's available authority in per-job workspaces, and exposes status, bounded history, safe workflow progress, and structured verification evidence with explicit limitations. Job inspection includes the confirmed PR identity/URL when configured; it never exposes raw logs, check output, or host paths. It supports volatile in-memory state or configured SQLite/PostgreSQL persistence; provider publication is optional to configure, but PR create/update and outcome recording are required for Job success when GitHub is configured. PostgreSQL can be shared between an API control plane with `workers_enabled: false` and separately launched `factory worker` processes. Existing all-in-one server behavior remains the default; separate PostgreSQL workers do not provide the future controller Thread model, simple scheduler, or unit-local coordinator adapter described in [Factory-unit MVP architecture](docs/roadmap/factory-unit-mvp.md). An optional embedded operator dashboard is served from that same API listener and authenticates only through a configured named operator; it is disabled by default. A dedicated `factory issue-poller` can admit allowlisted GitHub issues into that same PostgreSQL job store. See [REST operator dashboard](docs/features/rest-web-ui.md), [PostgreSQL Store](docs/features/postgres-store.md), [REST worker process](docs/features/rest-worker.md), and the [issue poller guide](docs/features/issue-poller.md). `make container-build` creates a control-plane-only image requiring PostgreSQL persistence and `workers_enabled: false`; the image does not include worker execution. SQLite remains single-server storage. See [REST server operations](docs/features/rest-server-operations.md) for readiness, backup/restore, restart recovery, and capacity guidance.

```sh
factory server doctor --config /absolute/path/to/server.json
factory server --config /absolute/path/to/server.json
factory rest help
```

Run `factory server doctor --config /absolute/path/to/server.json` for a local-only, read-only preflight of strict configuration and local prerequisites. It does not test provider reachability or PostgreSQL connectivity, open/create SQLite, start a listener, or run the workflow. See [REST job server](docs/features/rest-server.md) for current setup and limits. Do not expose the service beyond a trusted network without appropriate authentication and network controls. Factory does not provide or guarantee sandboxing, security isolation, or protection from repository/model/harness behavior; users/operators own those risks. Repositories, enabled harness profiles, credentials, and bounded job policy remain operator-configured; request data cannot select arbitrary configured executables, repositories, or credentials. Actual process access to network and local authority remains dependent on the execution account and deployment environment.

### PostgreSQL ownership protocol tests

The PostgreSQL Store, worker, ownership, and issue-poller integration tests require PostgreSQL 18. With Docker Compose available, run `make test-postgres`; this starts the disposable service defined in [`docker-compose.postgres-test.yml`](docker-compose.postgres-test.yml), waits for readiness, and runs the focused integration suite. Stop it with `make test-postgres-down`. The service binds only to loopback on port `55432` and stores data in a dedicated named test volume; `make test-postgres-down` removes the container and that volume/data. This test service is not a production persistence backend; it supports tests of PostgreSQL runtime integration against disposable PostgreSQL 18.
