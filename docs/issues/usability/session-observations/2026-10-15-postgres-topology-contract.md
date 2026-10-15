# Session observations: PostgreSQL topology contract

- Verified PR #266 merged as `bcf4a9a`; Ubuntu, macOS, and PostgreSQL 18 CI passed. Issue #263 was updated with the merged outcome and closed.
- For #258, documented the test-only PostgreSQL 18, single-primary envelope; failover, replication, backup/restore and production runtime integration remain prohibited/unverified. Clarified ambiguous commit handling, fencing before restore, restore-point loss, schema upgrade limits, and gates before runtime integration.
- Checked claims against PostgreSQL 18 official failover and replication documentation. Independent review found the documentation direction sound and asked that #248 be updated; posted the deployment/failover gate and remaining blockers to #248.
- No code, protocol, runtime, or deployment support changed. Documentation edits remain uncommitted on `design/postgres-topology-contract`; local Markdown links and `git diff --check` were verified. No usability friction was observed.
