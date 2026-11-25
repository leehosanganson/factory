# Factory

Factory is building a Compose-deployable service for long-running, agent-driven work. Users submit work in a browser; a **Controller** manages Threads, Jobs, schedules, Workers, and outcomes; separate **Workers** execute assigned Jobs using their locally configured harnesses; PostgreSQL stores durable state.

```text
Browser → Controller ↔ PostgreSQL
                    ↕
                  Workers → configured harness
```

The [local Compose task board](docs/features/compose-local-control-plane.md) runs one Pi Worker and an HTTPS browser UI. The [submitter ticket board](docs/features/rest-web-ui.md#submitter-ticket-board) keeps drafts in the current tab until you queue them as Threads, and lets you browse older Threads you own. See the Compose guide for Pi credentials and setup. GitHub is not required. The [Factory Worker MVP](docs/roadmap/factory-unit-mvp.md) describes the broader product direction.

## Run Factory's local CLI

Factory also contains a separate local developer CLI for agent-assisted repository workflows. This is not the target user-facing workflow for the Controller/Worker MVP. To use the existing CLI, install and configure its agent (Pi by default), then run from a named branch in the repository you want to change:

```sh
factory doctor
factory implement "Add input validation; test empty and malformed values"
```

The CLI can run requirements, implementation, review, documentation, and configured checks in a separate worktree. Depending on configuration, it can commit, push, and open a pull request. A pull request is a human-review handoff, not proof of correctness or permission to merge. See the [primary workflow guide](docs/features/primary-workflow.md) for setup and current behavior.

## Build and test

Factory is a Go CLI. Go 1.26.3 or later is required; Linux and macOS are supported.

```sh
make build
make test
make vet
```

The binary is written to `./bin/factory`. See [Contributing](CONTRIBUTING.md).

## Documentation

- [Factory Worker MVP](docs/roadmap/factory-unit-mvp.md) — target product, architecture, and scope.
- [Local Compose setup](docs/features/compose-local-control-plane.md) — current development topology and smoke test.
- [Operator dashboard](docs/features/rest-web-ui.md) — browser Job inspection and read-only recovery visibility.
- [REST server operations](docs/features/rest-server-operations.md) — current API, persistence, and recovery behavior.
- [Implemented features](docs/features/README.md) — index of current CLI and service capabilities.
- [Roadmap](docs/roadmap/roadmap.md) — short index separating current behavior from target direction.
