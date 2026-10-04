# Factory

Factory is a Go service and CLI for agent-driven repository jobs. Its product direction is one RESTful job service: accept a bounded request, run the configured workflow, verify the result, and create or update a pull request through the configured code-repository provider. Factory does not merge, release, or deploy.

The REST server supports volatile memory mode and optional SQLite persistence, with a configured GitHub provider for job PR publication and authenticated, no-replay operator recovery actions for eligible interrupted SQLite jobs: failed/canceled disposition without provider attempts, or read-only provider confirmation for persisted provider identity. See the [REST server operations guide](docs/features/rest-server-operations.md) for probe semantics, backup/restore, restart recovery, reconciliation, and operator limits; the [REST job contract](docs/roadmap/rest-api-contract.md) describes remaining target-MVP requirements.

## Documentation

- [Implemented features](docs/features/README.md) — current CLI and REST server capabilities.
- [REST server operations](docs/features/rest-server-operations.md) — health/readiness, backup/restore, restart recovery, limits, and trusted-network operations.
- [REST job contract](docs/roadmap/rest-api-contract.md) — target request-to-PR lifecycle, persistence modes, and delivery gates.
- [Agent guidance](AGENTS.md) — repository workflow and verification conventions.
- [Factory operating skill](.agents/skills/factory/SKILL.md) — agent workflow guidance and intended product direction.
- [Documentation skill](.agents/skills/documentation/SKILL.md) — documentation conventions.
- [Contributing](CONTRIBUTING.md) — branch naming and pull request conventions.
- [Issues and usability observations](docs/issues/README.md) — issue-recording guidance and usability findings.
- [Roadmap](docs/roadmap/roadmap.md) — unified product direction and later possibilities.
- [Issue context integration](docs/roadmap/rest-job-lifecycle.md) — later issue-intake option feeding the same job lifecycle.

## First task

Install the configured agent (the default is `pi`), then copy `config.json.example` to `~/.config/factory/config.json` and adjust it if needed. Run the read-only setup check before starting a task:

```sh
factory doctor
factory implement "Describe the change and how it should be verified"
```

`factory doctor` validates the config and checks whether the configured agent executable and `git` are on `PATH`; it checks `gh` only when `auto_publish` is enabled. It does not invoke those tools, contact GitHub, create state, or access the target checkout. See [implementation workflow](docs/features/implementation.md) for publication behavior and configuration details.

## Build and verify

Go 1.26.3 or later is required. Linux and macOS are supported. The Makefile provides `make build`, `make test`, `make fmt`, `make vet`, and `make clean`.

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
factory server --config /absolute/path/to/server.json
```

See [REST job server](docs/features/rest-server.md) for current setup and limits. Do not expose the service beyond a trusted network without appropriate authentication and network controls; local-process/container execution is not a security sandbox.
