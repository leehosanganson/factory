# Factory

Factory is a standard-library Go CLI for durable, agent-driven repository workflows. It currently provides sequential implementation workflows, repository review/fix/document/verify (`tidy`), detached PR monitoring, durable detached jobs, and opt-in parallel implementation for eligible clean Git worktrees. See the documentation for what is implemented today and what remains directional.

## Documentation

- [Implemented features](docs/features/README.md) — current CLI capabilities, behavior, and architecture.
- [Agent guidance](AGENTS.md) — repository-specific workflow, coding, and verification conventions.
- [Factory operating skill](.agents/skills/factory/SKILL.md) — when and how agents should use Factory.
- [Documentation skill](.agents/skills/documentation/SKILL.md) — how to make accurate documentation changes.
- [Issues and usability observations](docs/issues/README.md) — issue-recording guidance and usability findings.
- [Roadmap](docs/roadmap/roadmap.md) — long-term direction, explicitly not a delivery commitment.
- [Fleet manager design](docs/roadmap/fleet-manager-design.md) — an aspirational architecture note.

## Build and verify

Go 1.26.3 or later is required. Linux and macOS are supported. The Makefile provides `make build`, `make test`, `make fmt`, `make vet`, and `make clean`.

```sh
make build
./bin/factory help
make test
make vet
```

The binary is written to `./bin/factory`. Configuration and state are stored outside the target repository by default; see [`config.json.example`](config.json.example) and the [feature documentation](docs/features/README.md). CI builds, tests, and vets on Linux and macOS. Releases are created separately from explicit stable version tags; merging to `main` does not itself publish a release.
