# Session observations: detached-job history scan

- Factory's existing `--limit` already limits displayed rows, but reconciliation remains a separate full-history step; introducing a distinct opt-in `--scan-limit` makes that distinction explicit in syntax and output.
- Full `factory job --help` is routed before config loading, which makes bounded-list guidance available even when local configuration is invalid.
- Filesystem directory enumeration still exposes every entry even when job JSON reads and recovery reconciliation are capped; help/docs should avoid presenting this as a total filesystem-cost bound. `ReadDir(1)` uses filesystem directory iteration order, which is not guaranteed to be lexical; describe the actual order rather than promising stable ID ordering.
- The bounded listing should remain a partial operator view only. A direct `get <id>` and unbounded list provide routes to omitted records without pruning them or changing recovery behavior.
- A maximum accepted integer is a useful edge case for user-controlled scan sizes: the scan must not allocate capacity directly from that value. The focused regression test caught and then verified removal of that unsafe preallocation.
- The focused regression tests and the full `make test`, `make vet`, and `make build` targets passed after the fixes; built-in job help was smoke-tested without starting an agent workflow.
