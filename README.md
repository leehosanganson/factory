# Factory

Factory is a Go service and CLI for agent-driven repository jobs. Its product direction is one RESTful job service: accept a bounded request, run the configured workflow, verify the result, and create or update a pull request through the configured code-repository provider. Factory does not merge, release, or deploy.

The REST server supports volatile memory mode and optional SQLite persistence, with a configured GitHub provider for job PR publication and authenticated, no-replay operator recovery actions for eligible interrupted SQLite jobs: failed/canceled disposition without provider attempts, or read-only provider confirmation for persisted provider identity. See the [REST server operations guide](docs/features/rest-server-operations.md) for probe semantics, backup/restore, restart recovery, reconciliation, and operator limits; the [REST job contract](docs/roadmap/rest-api-contract.md) describes remaining target-MVP requirements. A separate manual-only GitHub App live-provider workflow is opt-in and restricted to a protected disposable sandbox. Invoke it only from Actions on `main` with explicit sandbox confirmation after #134 provisioning; the [operator guide](docs/features/github-app-live-test.md) lists required `FACTORY_LIVE_GITHUB_APP_ID`, `FACTORY_LIVE_GITHUB_PRIVATE_KEY`, and `FACTORY_LIVE_GITHUB_INSTALLATION_ID` secrets and the exact repository variables. Live validation remains unverified until provisioning and a successful manual run.

## Documentation

- [Implemented features](docs/features/README.md) — current CLI and REST server capabilities.
- [Primary Factory-to-Pi workflow](docs/features/primary-workflow.md) — setup, implementation, detached-job handoff, and recovery.
- [REST server operations](docs/features/rest-server-operations.md) — health/readiness, backup/restore, restart recovery, limits, and trusted-network operations.
- [Manual GitHub App live-provider test](docs/features/github-app-live-test.md) — protected opt-in sandbox validation; manual Actions invocation, required secrets/variables including the explicitly approved organization identity check, cleanup limits, and unverified status pending issue #134 provisioning.
- [REST job contract](docs/roadmap/rest-api-contract.md) — target request-to-PR lifecycle, persistence modes, and delivery gates.
- [Agent guidance](AGENTS.md) — repository workflow and verification conventions.
- [Factory operating skill](.agents/skills/factory/SKILL.md) — agent workflow guidance and intended product direction.
- [Documentation skill](.agents/skills/documentation/SKILL.md) — documentation conventions.
- [Contributing](CONTRIBUTING.md) — branch naming and pull request conventions.
- [Issues and usability observations](docs/issues/README.md) — issue-recording guidance and usability findings.
- [Roadmap](docs/roadmap/roadmap.md) — unified product direction and later possibilities.
- [Issue context integration](docs/roadmap/rest-job-lifecycle.md) — later issue-intake option feeding the same job lifecycle.

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

The binary is written to `./bin/factory`. Configuration and state are stored outside the target repository by default; see [`config.json.example`](config.json.example) and the [feature documentation](docs/features/README.md). CI builds, tests, and vets on Linux and macOS. Releases are created separately from explicit stable version tags; merging to `main` does not itself publish a release.

## Local REST server

The server accepts authenticated job requests, executes configured workflows in isolated workspaces, and exposes status, bounded history, and safe structured verification evidence with explicit limitations. Job inspection includes the confirmed PR identity/URL when configured; it never exposes raw logs, check output, or host paths. It supports volatile in-memory state or configured SQLite persistence, plus optional GitHub pull-request publication. See [REST server operations](docs/features/rest-server-operations.md) for readiness, backup/restore, restart recovery, and capacity guidance.

```sh
factory server doctor --config /absolute/path/to/server.json
factory server --config /absolute/path/to/server.json
```

Run `factory server doctor --config /absolute/path/to/server.json` for a local-only, read-only preflight of strict configuration and local prerequisites. It does not test provider reachability and does not open/create SQLite, start a listener, or run the workflow. See [REST job server](docs/features/rest-server.md) for current setup and limits. Do not expose the service beyond a trusted network without appropriate authentication and network controls; local-process/container execution is not a security sandbox.
