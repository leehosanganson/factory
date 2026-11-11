# Factory

Factory runs agent-driven engineering workflows in a local Git checkout. Give it a bounded task; it runs requirements, implementation, review, and documentation, then runs the checks you configured. It can hand the result to GitHub as a pull request. This gives you one place to start work and inspect its changes and check results.

Factory does not guarantee correctness, merge pull requests, or release or deploy software. Its local processes run with your account's authority: a worktree, shell, or container is not a security sandbox. Only use it with repositories and tools you trust.

## Run a local coding task

Install and configure Factory's agent (Pi by default), then run these commands from the repository you want to change, on a named Git branch. See the [setup guide](docs/features/primary-workflow.md#check-setup-and-verification) for configuration details.

```sh
factory doctor
factory implement "Add input validation; test empty and malformed values"
```

Factory runs the workflow in a separate worktree. When automatic publication is enabled (the default), it runs the configured `pipeline_checks`; configure checks that the project supports. Doctor checks local configuration and required executables, but does not run your checks or inspect the target checkout.

By default, Factory attempts to commit, push, and create a GitHub pull request after successful workflow stages and checks. Automatic publication also requires `gh`, an `origin` remote, and a clean starting checkout. Use `factory implement --detach "..."` to return immediately and inspect the job with `factory job list` and `factory job get <job-id>`. Review the diff and check evidence yourself. A pull request is a handoff for human review, not proof that the change is correct or permission to merge.

See [the primary workflow guide](docs/features/primary-workflow.md) for setup, checks, detached jobs, publication, and recovery details.

## REST job service

Factory also has a separate mode: `factory server` runs a local REST job service, and `factory rest` submits or inspects jobs on that service. It is distinct from the local `factory implement` workflow above. The server has its own configuration, authentication, persistence, and provider settings; it can use volatile memory or configured SQLite/PostgreSQL storage. See [REST server operations](docs/features/rest-server-operations.md) for current behavior and limits, and the [REST job contract](docs/roadmap/rest-api-contract.md) for target requirements.

## Build and test

Factory is a Go CLI. Go 1.26.3 or later is required; Linux and macOS are supported. Build and check it with:

```sh
make build
make test
make vet
```

The binary is written to `./bin/factory`. See [Contributing](CONTRIBUTING.md) for contribution guidance.

## Documentation

- [Implemented features](docs/features/README.md) — current CLI and REST service behavior.
- [Primary workflow](docs/features/primary-workflow.md) — local setup, implementation, checks, and recovery.
- [REST server operations](docs/features/rest-server-operations.md) — setup, persistence, and operational limits.
- [Roadmap](docs/roadmap/roadmap.md) — product direction, separate from shipped behavior.
