# Issue #90 SQLite reconcile lock E2E review

- **Scope:** Reassessed remaining issue #90 acceptance for SQLite failure during `POST /v1/jobs/{id}/reconcile` on the refreshed branch.
- **Verified:** The existing process E2E holds `BEGIN IMMEDIATE` on a second SQLite connection, expects a sanitized 409, releases the lock, and proves successful retry with one provider create/attempt and unchanged harness-run count. It also retains the failed job, uncertain provider attempt, and workspace.
- **Gap closed:** Previously, persisted status/history invariants were checked only after releasing the lock, and history was checked only for absence of a reconciliation event. The test now compares job, complete history, and uncertain attempt against their pre-lock values while the write lock remains held; it also checks workspace, harness-run count, and provider create/attempt counts before releasing the lock.
- **Observed test constraint:** HTTP job inspection returned 503 while the lock was held because server readiness probes the SQLite store. The test therefore checks persisted invariants through its already-open SQLite store connection during the lock; no database files are moved or unlinked.
- **Changes:** Test-only; no runtime defect was demonstrated. Focused E2E passed once and with `-count=3`; `umask 000 make test`, `make vet`, `make build`, and `git diff --check` passed.
