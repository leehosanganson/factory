# Show update time in gated-run lists

- **Finding:** `factory run list` orders records by recency but omits the update
  timestamp used for that order. Users cannot tell how recently each gated run
  progressed from the list alone.
- **Evidence:** The list output shows only `ID`, `STATUS`, `LIVENESS`, and
  `STAGE`, despite each persisted `State` having an `UpdatedAt` field. The run
  list already sorts by `UpdatedAt`; local CLI output confirms the timestamp is
  absent.
- **Desired outcome:** Show each run's update time in the list so users can
  interpret the recency ordering without opening each record.
- **Acceptance criteria:** The list has an `UPDATED` column showing each
  managed run's `UpdatedAt` in RFC3339; tests verify values and existing
  newest-first ordering; empty output, liveness, and run inspection remain
  unchanged.
- **Status:** Implemented in `internal/factory/managed_run.go`: list headers
  and rows now include `UPDATED` formatted as RFC3339. Tests verify the
  timestamp values along with newest-first and deterministic tie ordering.
