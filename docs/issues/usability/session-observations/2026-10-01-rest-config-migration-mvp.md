# REST configuration and migration MVP slice

- **Type:** Engineering session observation; not a user-reported finding.
- **Contract checked:** The current `main` REST MVP contract selects one shared bearer API key plus one shared fine-grained PAT. Principal/OAuth and GitHub App grant custody are deferred. Implemented config stores paths only for both secrets, and startup file loading enforces protected regular-file rules.
- **What changed:** Added an isolated `internal/restserver` package for strict versioned config parsing, listener/DSN/repository/path validation, secret-file loading, and an injectable `golang-migrate` runner using PostgreSQL and file-source drivers. Added an initial jobs/history schema migration and documented non-goals.
- **Scope boundary:** No HTTP serving, API authentication, SQL job-store implementation, durable admission, worker/Pi execution, provider operations, or CLI integration was added. The migration runner is available but is not invoked by the current CLI.
- **Verification/friction:** `make test`, `make vet`, and `make build` results are recorded in the task summary. `make help` is not provided by this repository's Makefile; available targets were read directly from the file.
