# REST store contract parity test session

- The shared `Store` contract already covered sequential idempotency and lifecycle behavior, but concurrent same-key assertions were isolated to `LocalManager`; extending the shared suite exercised both registered backends with no production changes.
- An initial capacity-reuse test placed in the common suite did not fit its existing queue-capacity fixture. Moving that scenario to a dedicated SQLite configuration made the intended boundary explicit and also verified a different-payload retry is accepted after capacity frees.
- Focused repeated tests and the repository Makefile checks completed successfully.
