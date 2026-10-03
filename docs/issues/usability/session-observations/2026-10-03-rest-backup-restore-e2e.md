# Session observations: REST backup/restore E2E

- **Verified task:** added and ran a process-level disposable SQLite backup/restore regression test for issue #130.
- **What worked:** existing runtime helper and HTTP request/snapshot utilities were reusable. The actual CLI backup path could be exercised from the integration test by building a temporary binary, while running isolated server helper processes.
- **Friction observed:** server startup readiness helper used by other tests calls `t.Fatalf` without stopping its child process, so this test uses a local readiness loop that kills and waits on a premature child exit/timeout. An initial harness fixture needed to account for task placement inside the generated system prompt; restricting task recognition across argv made job runs deterministic.
- **Concrete improvement idea:** consider a shared process fixture that owns child startup, bounded readiness, logs, and cleanup without weakening failure diagnostics.
- **Evidence limits:** all data, checkout, server configuration, SQLite files, logs, and processes used by the added test are temporary and synthetic. No production/deployed restore or provider integration was exercised.
