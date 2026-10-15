# Session observations — 2026-10-15

- **Verified observation:** Package-scoped Go tests and race tests exposed the important compatibility boundary: legacy claim APIs need an explicitly unfenced claim mode, while claims from `ClaimNextAttempt` must reject ID-only mutations. The shared in-memory/SQLite store-contract tests now exercise that distinction, including expired leases.
- **Friction / idea:** Changing executor persistence callbacks to take an attempt handle surfaced direct-entry callers that previously ran with callbacks wired but no ownership token. A pre-workflow guard allows such direct calls to fail without workspace or workflow side effects, while standalone execution with no persistence callbacks remains available.
- These are session observations, not user-reported product findings.
