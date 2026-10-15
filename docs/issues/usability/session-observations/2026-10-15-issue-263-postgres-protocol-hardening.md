# Session observations: PostgreSQL protocol hardening

- The existing isolated-schema PostgreSQL integration tests support deterministic trigger-based failures without production test hooks. Migration failure after the backfill is observable at version upgrade, allowing checks for transactional rollback and a clean retry.
- The focused package tests passed. Although `FACTORY_TEST_POSTGRES_URL` was initially unset, Docker and a local `postgres:18` image were available; running the full PostgreSQL ownership integration suite against a temporary PostgreSQL 18 container passed, including the independent-process tests and the new injected failures.
- Independent review found that v1 migration also needed to reject `running` jobs with no ownership row; a fail-closed check and regression test were added and the PostgreSQL 18 suite passed again.
- Work stayed in the isolated worktree; the existing independent-process tests and protocol boundaries were left unchanged. No additional Factory usability friction was observed.
