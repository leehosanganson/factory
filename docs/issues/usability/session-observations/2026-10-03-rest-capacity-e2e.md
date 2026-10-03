# REST queue-capacity process E2E

- **Scope:** Reviewed merged #89 process-level E2E coverage against SQLite fail-closed startup, queue saturation, and provider-uncertain outcomes. SQLite startup and provider uncertain-create/restart/reconciliation paths are directly represented; queue rejection/retry and saturation visibility were covered only by store/HTTP unit tests, not a real server process.
- **Change:** Added one process-level test to `internal/restserver/runtime/runtime_e2e_test.go`: occupy the sole worker, fill the one-slot queue, verify the next authenticated request is rejected as `queue_full` without a job ID, inspect `queue_saturated`, and retry the same idempotency key successfully after capacity frees.
- **Evidence:** Focused test passed 5 consecutive runs; `make vet`, `make build`, and `umask 000 make test` passed. `gofmt -d` and `git diff --check` were clean.
- **Boundary:** SQL-unavailable startup and uncertain provider outcome coverage were already present in process-level or runtime tests; no additional cases were added for them. The provider uncertainty E2E uses a fake provider and does not claim live GitHub coverage or automatic recovery.
