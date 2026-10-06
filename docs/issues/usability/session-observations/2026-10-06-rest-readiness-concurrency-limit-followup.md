# REST readiness concurrency limit follow-up

- **Scope:** Continued issue #162 in the existing worktree; corrected readiness handling for injected handlers without a `Ready` callback.
- **Verified behavior:** `/readyz` remains unavailable when its readiness callback is nil. Other routes skip readiness checks only when no `Ready` callback is configured; configured server handlers continue to use the bounded readiness path. Focused readiness tests, the REST API suite, the SQLite process test, `umask 000 make test`, `make vet`, and `make build` passed.
- **Observed friction:** The test helper intentionally uses a nil readiness callback for unrelated route tests, which exposed a compatibility distinction between test handler construction and real server configuration. Treating the unconfigured callback as a no-op on other routes restored those tests without weakening readiness assertions.
- **Concrete idea:** Keep dedicated probe tests for nil readiness separate from unrelated handler route tests so this test-helper convention remains visible.
