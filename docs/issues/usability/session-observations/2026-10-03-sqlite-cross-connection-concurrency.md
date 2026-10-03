# SQLite cross-connection admission concurrency

- **Scope:** While assessing acceptance criteria for #86, found `TestSQLiteStoreConcurrentConnectionsShareIdempotency` performed sequential writes, while shared-store concurrent admission coverage only instantiated the in-memory store.
- **Evidence:** Running 16 simultaneous identical `Admit` calls across two open SQLite stores repeatedly failed with `persist SQLite job`. Using SQLite immediate transactions serialized these transactions; the focused test passed 10 repetitions and checks a single new job ID plus changed-payload conflict.
- **What worked/friction:** The existing cross-connection test was a focused place to encode this gap. SQLite write contention became visible only when admission actually raced across connections.