# Issue #86 persistence contract review

- Compared #86 acceptance criteria with merged PRs #91, #92, #103, and #105 at `c99dca5`; no substantive issue-specific code gap remains. Shared store parity and concurrent SQLite admission/capacity-key behavior are covered by tests.
- Clarified the canonical REST contract: current recovery dispositions are queued/resume, running/operator reconciliation, terminal/retain, and unknown/operator reconciliation. Explicitly stated that SQLite does not provide durable worker leases, execution checkpoints, in-workflow resumption, or multi-host ownership/fencing.
- Reviewed open issue #90 against the current operator guide and merged #95–#100; the clean setup path, readiness/fail-closed behavior, backup/restore, aggregate status, and sanitized diagnostics are already documented or tested. No additional #90 change made.
- Documentation-only; no code or GitHub issue state changed.
