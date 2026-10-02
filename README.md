# Factory

Factory is a Go service and CLI for agent-driven repository jobs. Its product direction is one RESTful job service: accept a bounded request, run the configured workflow, verify the result, and create or update a pull request through the configured code-repository provider. Factory does not merge, release, or deploy.

The REST server is implemented with an in-memory job backend. Memory mode is simple but volatile: process restart loses jobs and history. Optional SQL persistence is the recommended deployment path when jobs must survive restarts; SQL support and restart recovery are not implemented yet. See the [REST job contract](docs/roadmap/rest-api-contract.md) for the target MVP and [implemented features](docs/features/README.md) for current behavior.

## Documentation

- [Implemented features](docs/features/README.md) — current CLI and REST server capabilities.
- [REST job contract](docs/roadmap/rest-api-contract.md) — target request-to-PR lifecycle, persistence modes, and delivery gates.
- [Agent guidance](AGENTS.md) — repository workflow and verification conventions.
- [Factory operating skill](.agents/skills/factory/SKILL.md) — agent workflow guidance and intended product direction.
- [Documentation skill](.agents/skills/documentation/SKILL.md) — documentation conventions.
- [Contributing](CONTRIBUTING.md) — branch naming and pull request conventions.
- [Issues and usability observations](docs/issues/README.md) — issue-recording guidance and usability findings.
- [Roadmap](docs/roadmap/roadmap.md) — unified product direction and later possibilities.
- [Issue context integration](docs/roadmap/rest-job-lifecycle.md) — later issue-intake option feeding the same job lifecycle.

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

The current server accepts authenticated job requests, executes configured workflows in isolated workspaces, and exposes status and bounded history. Its job registry is in memory, so accepted jobs and history do not survive a process restart. A successful job does not yet create or update a provider pull request; both SQL persistence and provider PR operations are target-MVP gaps.

```sh
factory server --config /absolute/path/to/server.json
```

See [REST job server](docs/features/rest-server.md) for current setup and limits. Do not expose the service beyond a trusted network without appropriate authentication and network controls; local-process/container execution is not a security sandbox.
