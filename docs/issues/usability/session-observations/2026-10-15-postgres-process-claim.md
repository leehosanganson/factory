# Session observations: PostgreSQL process-level claim test

- The existing PostgreSQL 18 integration test entry point and CI job were reusable for the independent-process claim test; a local PostgreSQL 18 Docker run passed.
- The protocol currently persists one `factory_job_ownership` row per job and has no separate PostgreSQL attempt-history table. The test verifies the winner's generation-1 row and the running job without adding a parallel test-only history representation.
- A temporary-file readiness/release barrier ensured both child test processes had opened their own schema-scoped database connection before racing through `ClaimNext`.
- No Factory CLI workflow was started; this was a bounded test-only change. No additional Factory CLI usability friction was observed.
