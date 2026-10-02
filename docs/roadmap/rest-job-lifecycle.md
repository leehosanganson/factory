# Issue context as a future job-intake adapter

This note preserves the useful issue-driven workflow concepts without defining a second MVP. Factory's product direction is the REST job service and request-to-PR lifecycle in the [roadmap](roadmap.md) and [REST job contract](rest-api-contract.md).

The target MVP accepts a bounded job request through the REST API, executes it, verifies the result, and creates or updates a pull request through the configured code-repository provider. An issue number or text may be supplemental request context; it does not create a separate autonomous issue-polling product.

## Possible later integration

A future issue-tracker adapter could watch a source issue, record versioned observations, reconcile meaningful changes into the existing job, and pause for human direction when scope is ambiguous or exceeds policy. Its output must enter the same canonical job lifecycle and durable persistence backend as ordinary REST submissions. It must not create a parallel queue, worker model, or separate request-to-PR MVP.

Issue trackers and code-repository providers remain separate interfaces: a source issue may live on a different system from the target repository. Provider-specific types must not leak into the canonical job model. Issue closure stops further implementation activity but does not authorize closing or merging a PR or deleting work needed for recovery.

Before adding this integration, define issue identity/versioning, polling cadence, reconciliation semantics, cancellation/closure behavior, credential scope, and recovery from uncertain provider operations. This is later direction, not an MVP requirement or implementation claim.

## Concepts retained from the earlier proposal

- Record source evidence and stable issue versions so changed requirements can be explained.
- Preserve one associated PR across legitimate follow-up changes rather than opening duplicates.
- Continue observing while paused only if product requirements justify the operational cost.
- Treat issue text, comments, and generated output as untrusted input.
- Preserve workspaces and evidence when closure or uncertainty interrupts a job.

No issue polling, reconciliation worker, or issue-driven engineering lifecycle is implied by this note. The implemented `factory work` commands are documented in [current features](../features/work.md).