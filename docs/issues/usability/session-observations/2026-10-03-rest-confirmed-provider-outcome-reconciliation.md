# Session observations: confirmed provider outcome reconciliation

- The checkout was clean on `91cf747` and the focused REST packages gave direct feedback for this change.
- `make help` is not a defined target; the Makefile itself lists the available `build`, `test`, `fmt`, `vet`, and `clean` targets.
- `make help` is not a defined target; the Makefile itself lists the available `build`, `test`, `fmt`, `vet`, and `clean` targets.
- Two `umask 000 make test` runs both failed in untouched `internal/factory` tests, but with different observations: `TestSecondaryStatusRunsDuringActiveStageAndPersistsSanitizedUpdate` saw 2 status invocations instead of 1 on the first run; `TestDuplicateJobWorkerCannotRerunOrReplaceWorkerIdentity` could not find `worker.json` on the second. The REST packages, including the process E2E, passed in those runs. The failures were not investigated beyond a focused check; the full test target therefore did not pass.
- The process-level fault test holds its SQLite write lock past the configured five-second busy timeout to expose the confirmed-write/outcome-persistence-failure state; its focused three-run repeat passed.
