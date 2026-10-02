# Session observation: SQLite REST job persistence

- Implemented a SQLite store against the shared `restjobs.Store` contract, with embedded forward migrations, durable requests/idempotency/events, bounded history, and conservative restart classification.
- Added strict operator config selection (`memory` or `sqlite` with an absolute path), runtime startup failure on database errors (no memory fallback), readiness probing, and shutdown close.
- Test setup needs explicitly private temp DB parent directories because the environment umask is restrictive; terminal layout tests likewise pin TERM/size. Full-suite run is in progress.
