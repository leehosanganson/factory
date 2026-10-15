# Session observation: PostgreSQL stale heartbeat fencing

- **Type:** Verified engineering observation; no user report.
- **Scope:** Isolated PostgreSQL ownership protocol test coverage only.
- **Observation:** Before this change, the independent-process fencing test held generation 1 across an explicit test-operator requeue, let generation 2 claim, and verified generation 1's stale `Finish` was rejected. It did not invoke a generation-1 `Heartbeat` after generation 2 claimed.
- **Change:** Extended that process test with barriers for both generations' heartbeats. It now verifies generation 1 is rejected without changing its retained history lease or generation 2's ownership/history lease; generation 2 can renew and its history lease remains synchronized. The test still does not establish that generation 1 stopped, and the test-only requeue is not a recovery implementation.
- **Verification:** `make test-postgres`, `make test`, `make vet`, and `make build` passed locally on PostgreSQL 18.
- **Boundary:** No runtime, Store, authentication, reassignment, recovery, sandbox, provider, or production topology behavior changed. The PostgreSQL protocol remains test-only.
