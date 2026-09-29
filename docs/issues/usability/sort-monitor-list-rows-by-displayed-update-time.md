# Sort monitor-list rows by displayed update time

- **Finding:** `factory monitor list` displays an `UPDATED` timestamp but orders
  rows by monitor creation time.
- **Evidence:** Source inspection showed `loadJobs` in
  `internal/factory/monitor.go` sorting by `CreatedAt`, while the list renderer
  displays `UpdatedAt`. In the audited local records, creation and update order
  happened to agree, so the mismatch was not reproduced from that snapshot.
- **Desired outcome:** Order monitor-list results by the same update timestamp
  shown in the table, newest first.
- **Acceptance criteria:** Monitor list sorting uses `UpdatedAt` descending;
  tests include records whose creation and update order differ and assert the
  displayed order; other monitor commands and persisted records are unchanged.
- **Status:** Implemented in `internal/factory/monitor.go`; behavioral coverage
  seeds monitor records with opposing creation and update order and asserts the
  rendered list order.
