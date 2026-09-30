# Implemented features

This section describes Factory's current Go CLI behavior, not the aspirational roadmap. Factory does not currently provide a server, fleet coordinator, generic arbitrary-command `run`, or job chaining.

- [Implementation workflow](implementation.md) — requirements, implementation, review, and documentation, with optional approval gates and eligible parallel implementation.
- [Tidy workflow](tidy.md) — repository review/fix/document/verify, with foreground publishing safeguards and a nonpublishing detached mode.
- [Detached jobs](jobs.md) — durable lifecycle for detached implementation, tidy, and monitor jobs.
- [PR monitor](monitor.md) — detached, guarded maintenance of an existing open pull request.
- [Issue work intake](work.md) — local durable submission and inspection of provider-neutral issue work requests; requests remain queued because no issue provider or worker is implemented.

## Shared behavior

Factory is a standard-library Go CLI. Its agent adapter is configurable; the default is `pi`. Foreground workflow run records and detached jobs are separate state models and command families. State defaults under the user's XDG state directory (or `~/.local/state`) and can be relocated through configuration; it is kept outside the target repository. Agents and their tools are not sandboxed by Factory, so process-level workflow safeguards do not replace review.

Build and check the CLI with `make build`, `make test`, and `make vet`. See the [README](../../README.md) for prerequisites and build/release overview, and the [roadmap](../roadmap/roadmap.md) for clearly labeled future direction.
