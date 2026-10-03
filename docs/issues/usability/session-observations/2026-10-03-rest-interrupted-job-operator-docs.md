# REST interrupted-job operator documentation

- **Scope:** Implemented docs-only issue #125 on the clean `origin/main` baseline `89b62bb` (including #129). Re-read live issues #123/#124 and checked REST handler/store/executor/provider source, route tests, process E2E, operations guide, roadmap, backup behavior, and trust boundary.
- **Documented behavior:** Separated no-provider-record failed/canceled disposition from read-only provider reconciliation; documented interrupted/failed eligibility, provider identity checks, API/auth/error behavior, no replay, durable status/history/evidence, and recovery limitations. Examples use bodyless requests and placeholders.
- **Cross-checks:** Confirmed PR #131 is now merged, while #128 and #117 remain open. PR #128's REST-shaped `httptest` coverage is not live-account validation. SQLite backup excludes separate workspaces; reconciliation does not complete/expire a workspace. Corrected roadmap claims to retain these distinctions and the absence of leases/checkpoints/fleet ownership.
- **Friction:** `make help` is not defined in the Makefile; available targets are listed directly in the file. Review of the runtime callback was needed to distinguish store-level repeated-disposition idempotency from the HTTP route's recovery-needed eligibility check.
- **Status:** Documentation and observation only; no source or test files changed.
