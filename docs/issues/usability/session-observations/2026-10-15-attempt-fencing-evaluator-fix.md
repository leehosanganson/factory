# Session observations — 2026-10-15

- **Verified observation:** `make test`, `make vet`, and `make build` passed after tightening FactoryExecutor's direct-call attempt guard, requiring attempt-aware provider outcomes in the coordinator, and migrating the watch history fixture to an attempt-aware executor event.
- **Verified observation:** Focused race checks passed for `internal/restworker` and the watch history-fallback process test. A broader race run also reported races in two existing REST client tests and the reconnect watch test; those were outside this task's scope and were left unchanged.
- **Friction / idea:** The broad race run mixes package coverage with process-level tests and revealed unrelated shared-test-state reports. Consider making those fixtures synchronize their handler state before including them in a broad race target.
- These are session observations, not user-reported product findings.
