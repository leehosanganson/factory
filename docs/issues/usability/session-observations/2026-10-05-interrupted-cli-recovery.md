# Session observations: interrupted CLI recovery

- The existing phase-aware `job watch` hint tests made it straightforward to add an exact-text regression case and verify task text is not included.
- The detached-job guidance already documented retained worktree inspection, but it did not explain interruption classification delay or no-replay semantics. The orphan-reconciliation test allows checking stale classification without a test that sleeps for the full grace period.
- `make help` is not a defined target; the available targets are visible in the Makefile. No dedicated detached worker-crash test was found in the job test files during this task.
