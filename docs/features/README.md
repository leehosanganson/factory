# Implemented features

This section describes implemented behavior, distinct from the remaining target MVP requirements in the [roadmap](../roadmap/roadmap.md). Factory currently provides a CLI and a local REST job server with volatile memory or optional SQLite/PostgreSQL persistence and configured provider PR operations. PostgreSQL also supports separately launched workers and a workers-disabled API control plane; Factory does not provide a fleet coordinator, generic arbitrary-command `run`, or job chaining.

- [Primary Factory-to-Pi workflow](primary-workflow.md) — setup, implementation, detached-job handoff, and recovery.
- [Implementation workflow](implementation.md) — requirements, implementation, review, and documentation, with optional approval gates and eligible parallel implementation.
- [Tidy workflow](tidy.md) — repository review/fix/document/verify, with foreground publishing safeguards and a nonpublishing detached mode.
- [Detached jobs](jobs.md) — durable lifecycle for detached implementation, tidy, and monitor jobs.
- [PR monitor](monitor.md) — detached, guarded maintenance of an existing open pull request.
- [Issue work intake and tracking](work.md) — local work commands do not start an autonomous worker; this feature is separate from the REST issue poller.
- [REST job server](rest-server.md) — current authenticated job execution, persistence modes, provider PR writes, readiness, graceful runtime lifecycle, and local-only read-only configuration preflight.
- [REST server configuration](rest-server-config.md) — strict server JSON, memory/SQLite/PostgreSQL selection, worker-mode settings, and operational limits.
- [PostgreSQL Store](postgres-store.md) — PostgreSQL-backed REST persistence and shared-store behavior.
- [REST worker process](rest-worker.md) — separate PostgreSQL worker and workers-disabled API control-plane operation.
- [Embedded REST operator dashboard](rest-web-ui.md) — optional same-listener browser UI and control-plane container image.
- [Autonomous GitHub issue poller](issue-poller.md) — dedicated PostgreSQL-backed admission into the shared REST job queue.
- [REST server operations](rest-server-operations.md) — health/readiness probes, authenticated aggregate status, SQLite backup and portable recovery bundles, restart recovery classification, limits, and trusted-network operations.
- [REST job client](rest-client.md) — separate protected client configuration and CLI commands for remote submit, inspect, watch, cooperative cancellation, read-only aggregate operations, and explicit SQLite interrupted-job recovery actions.
- [Supervised systemd deployment](rest-server-systemd.md) — dedicated service account, private paths, installed binary unit, lifecycle, and single-host operations.
- [Manual GitHub App live-provider test](github-app-live-test.md) — guarded opt-in live verification in a dedicated sandbox; not run by default CI.

## Shared behavior

Factory is a standard-library Go CLI. Its agent adapter is configurable; the default is `pi`. Foreground workflow run records and detached jobs are separate state models and command families. State defaults under the user's XDG state directory (or `~/.local/state`) and can be relocated through configuration; it is kept outside the target repository. Agents and their tools are not sandboxed by Factory, so process-level workflow safeguards do not replace review.

Build and check the CLI with `make build`, `make test`, and `make vet`. See the [README](../../README.md) for prerequisites and build/release overview, and the [roadmap](../roadmap/roadmap.md) for clearly labeled future direction.
