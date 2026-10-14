# Issue context as a future job-intake adapter

This note preserves the useful issue-driven workflow concepts without defining a second MVP. Factory's product direction is the REST job service and request-to-PR lifecycle in the [roadmap](roadmap.md) and [REST job contract](rest-api-contract.md).

The target MVP accepts a bounded job request through the REST API, executes it, verifies the result, and creates or updates a pull request through the configured code-repository provider. An issue number or text may be supplemental request context; it does not create a separate autonomous issue-polling product.

## Possible later integration

A future issue-tracker adapter could watch a source issue, record versioned observations, reconcile meaningful changes into the existing job, and pause for human direction when scope is ambiguous or exceeds policy. Its output must enter the same canonical job lifecycle and durable persistence backend as ordinary REST submissions. It must not create a parallel queue, worker model, or separate request-to-PR MVP.

The reviewed form of this integration is an explicit, opt-in dispatch: after inspecting a selected, versioned issue snapshot and the configured repository alias, the user deliberately requests one bounded REST job. The submission reuses the existing REST job API and idempotency contract; it does not add a second execution queue or worker, and `factory work` reads (`refresh`, `watch`, `issue`) never submit. A durable mapping from tracker/repository/issue/version to the REST job ID makes duplicate dispatches idempotent while a changed issue version requires explicit renewed confirmation rather than mutating or replaying existing work. Scoped provenance (issue URL/number/version and sanitized title) is retained in bounded inspection without exposing full issue bodies, secrets, or local paths.

The bounded client-side snapshot-confirmation flow is implemented on the approved branch for issues #244/#246: `factory rest issue --repository <server-configured-alias> --issue-repository <owner/repo> [--confirm-version <sha256>] [--json] <issue-number>`. Without confirmation it fetches one current snapshot and shows a terminal-safe preview of the exact generated, bounded task, with no REST admission. With confirmation it fetches again and uses ordinary nonempty `POST /v1/jobs` submission only when the exact version matches and the issue is open. The task frames source identity, version, title, and body as untrusted delimited context. A length-prefixed SHA-256 idempotency key deterministically maps destination/source/number/version; changed versions require a new preview/confirmation and map to another key. The client does not add a request field, submit through `SubmitIssue`, persist source provenance, or poll. The durable server-side issue-version-to-job mapping from the broader reviewed design in [issue #237](https://github.com/leehosanganson/factory/issues/237), automatic polling/reconciliation, and provenance-aware inspection remain future work tracked in [issue #244](https://github.com/leehosanganson/factory/issues/244).

Issue trackers and code-repository providers remain separate interfaces: a source issue may live on a different system from the target repository. Provider-specific types must not leak into the canonical job model. Issue closure stops further implementation activity but does not authorize closing or merging a PR or deleting work needed for recovery.

Before adding this integration, define issue identity/versioning, polling cadence, reconciliation semantics, cancellation/closure behavior, credential scope, and recovery from uncertain provider operations. This is later direction, not an MVP requirement or implementation claim.

## Concepts retained from the earlier proposal

- Record source evidence and stable issue versions so changed requirements can be explained.
- Preserve one associated PR across legitimate follow-up changes rather than opening duplicates.
- Continue observing while paused only if product requirements justify the operational cost.
- Treat issue text, comments, and generated output as untrusted input.
- Preserve workspaces and evidence when closure or uncertainty interrupts a job.

No issue polling, reconciliation worker, or issue-driven engineering lifecycle is implied by this note. The implemented `factory work` commands are documented in [current features](../features/work.md).