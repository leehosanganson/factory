# SQLite server lock Darwin regression session

## Verified observations

- The original server ownership lock used `flock` on the SQLite database inode itself. That overlapped the inode used by SQLite's Darwin locking implementation and matches the reported platform-specific conflict; Linux focused ownership tests did not reproduce it. Darwin runtime behavior could not be executed on this Linux host.
- The Darwin-only regression test opens the owner store, admits and claims a job while ownership is held, and opens an administrative store to inspect the live running job. It cross-compiles for `darwin/arm64`; macOS CI must execute it to confirm runtime behavior.
- Ownership now uses a stable private `<canonical database path>.server-lock` sidecar. This keeps the SQLite inode available for database locking and admin inspection. The lock remains exclusive through close/process exit, and the sidecar is retained. Focused tests passed for orderly release, competing-process rejection, admin reads, and unclean-exit recovery.
- Focused ownership tests, `make vet`, `make build`, and Darwin/arm64 test cross-compilation passed. `make test` failed four `cmd/factory` assertions: list/watch cases observed existing job/run records, while the monitor-list case returned no expected record. Retrying with a temporary `XDG_STATE_HOME` produced the same failures. The cause was not established; all other Go packages passed.

## Friction and idea

- The local environment cannot run Darwin binaries, so macOS runtime confirmation depends on the existing macOS CI runner. A cross-compile verifies build-tag and API compatibility but not Darwin lock behavior; keeping a Darwin-specific behavior test in the suite gives CI an executable regression check.
- The full suite's CLI failures persisted across an isolated `XDG_STATE_HOME` retry, so the all-suite result remains inconclusive until those state records/configuration are inspected.
