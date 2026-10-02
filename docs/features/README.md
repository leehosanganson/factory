# Implemented features

This section describes implemented behavior, distinct from the target MVP in the [roadmap](../roadmap/roadmap.md). Factory currently provides a CLI and a local REST job server. The server uses volatile in-memory job state and does not yet create/update pull requests through a configurable repository provider. Optional SQL persistence and provider PR operations are target-MVP requirements, not current features. Factory does not provide a fleet coordinator, generic arbitrary-command `run`, or job chaining.

- [Implementation workflow](implementation.md) — requirements, implementation, review, and documentation, with optional approval gates and eligible parallel implementation.
- [Tidy workflow](tidy.md) — repository review/fix/document/verify, with foreground publishing safeguards and a nonpublishing detached mode.
- [Detached jobs](jobs.md) — durable lifecycle for detached implementation, tidy, and monitor jobs.
- [PR monitor](monitor.md) — detached, guarded maintenance of an existing open pull request.
- [Issue work intake and tracking](work.md) — local durable submission and inspection of provider-neutral issue work requests, one-shot refresh/history, opt-in foreground GitHub issue polling, and version-pinned human-direction records; requests remain queued because no autonomous worker is implemented.
- [REST job server](rest-server.md) — current authenticated, bounded in-memory job execution, isolated workspaces, and graceful runtime lifecycle. SQL persistence and provider PR writes are target-MVP gaps.

## Shared behavior

Factory is a standard-library Go CLI. Its agent adapter is configurable; the default is `pi`. Foreground workflow run records and detached jobs are separate state models and command families. State defaults under the user's XDG state directory (or `~/.local/state`) and can be relocated through configuration; it is kept outside the target repository. Agents and their tools are not sandboxed by Factory, so process-level workflow safeguards do not replace review.

Build and check the CLI with `make build`, `make test`, and `make vet`. See the [README](../../README.md) for prerequisites and build/release overview, and the [roadmap](../roadmap/roadmap.md) for clearly labeled future direction.
