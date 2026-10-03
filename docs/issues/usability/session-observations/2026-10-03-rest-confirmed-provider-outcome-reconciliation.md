# Session observations: confirmed provider outcome reconciliation

- The checkout was clean on `91cf747` and the focused REST packages gave direct feedback for this change.
- `make help` is not a defined target; the Makefile itself lists the available `build`, `test`, `fmt`, `vet`, and `clean` targets.
- Two local `umask 000 make test` attempts each saw an unrelated, intermittent `internal/factory` failure: first the secondary-status invocation count differed (2 vs 1), then a worker metadata file was absent. A third local full run passed. The REST packages, including the process E2E, passed in each run. The macOS CI test run also first failed at the secondary-status count, then passed when CI reran failed jobs; Linux CI passed.
- The process-level fault test holds its SQLite write lock past the configured five-second busy timeout to expose the confirmed-write/outcome-persistence-failure state; its focused three-run repeat passed.
