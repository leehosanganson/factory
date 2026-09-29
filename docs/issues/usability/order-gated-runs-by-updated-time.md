# Order gated runs by their most recent update

- **Finding:** `factory run list` emits managed runs in directory enumeration
  order rather than recency order.
- **Evidence:** Source inspection of `internal/factory/managed_run.go` showed
  directory entries were appended directly to output rows; run IDs are not a
  recency field. The local state did not contain gated-run records, so this was
  not reproduced against saved runs.
- **Desired outcome:** List the most recently updated gated runs first, using
  the timestamp recorded for run state updates.
- **Acceptance criteria:** `factory run list` orders managed runs by
  `UpdatedAt` descending, with deterministic ordering for equal timestamps;
  tests seed deliberately conflicting timestamps and assert display order.
  Unmanaged records remain excluded and run persistence is unchanged.
- **Status:** Implemented in `internal/factory/managed_run.go`; list rows are
  sorted by `UpdatedAt` descending and then ID ascending for ties. Tests assert
  both recency order and tie order while confirming unmanaged records remain
  excluded.
