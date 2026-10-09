# SQLite hard-link ownership pass

## Verified observations

- The hard-link process regression failed against the original implementation: a competitor opened a second server store through an alias while the owner held a running job.
- Ownership now uses retained per-user lock files keyed by device/inode identity; the implementation is platform-specific for Linux and Darwin and does not take SQLite's database-file lock. The operator guide records the behavior.
- The focused process regression passed five repetitions, including same-path rejection, hard-link alias rejection, live-job preservation, and unclean-exit recovery. Darwin/arm64 test binary cross-compilation passed; Darwin runtime behavior awaits macOS CI.
- The first full `make test` run had one timing-sensitive failure in `TestSecondaryStatusRunsDuringActiveStageAndPersistsSanitizedUpdate`. That test passed 10 isolated repetitions and the complete suite passed on rerun. `make vet` and `make build` passed.
- PR #198 is open as a draft at head `d97408765cf13205e441ca5adccd03167b674eb7`; current Linux and macOS Actions checks were pending at the last status query. No readiness change or merge was made.

## Friction

- Existing lock-path documentation described only adjacent path sidecars; correcting the operator guide was necessary when changing the identity mapping.
- Local Darwin runtime validation is unavailable on this Linux runner, so the macOS Actions result is the outstanding platform check.
