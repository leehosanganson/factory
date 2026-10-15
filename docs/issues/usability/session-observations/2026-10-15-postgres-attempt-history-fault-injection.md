# Session observations: PostgreSQL attempt-history fault injection

- The existing PostgreSQL 18 integration harness uses an isolated schema per subtest and was suitable for scoped trigger-based failure injection. A local PostgreSQL 18.6 Docker container ran the integration suite, including the existing independent-process contention and stale-generation fencing cases.
- Schema-local triggers filtered by the designated job ID allowed deterministic claim-insert and finish-update failures. The tests confirmed transaction rollback, generation-1 retry, and successful finish after the injected fault was removed.
- No production test hooks or runtime integration were added. No Factory CLI workflow was started; this was a bounded repository test change. No additional CLI usability friction was observed.
