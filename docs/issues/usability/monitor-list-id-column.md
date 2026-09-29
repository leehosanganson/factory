# Align monitor IDs in list output

- **Finding:** `factory monitor list` IDs have different formats and widths.
  Current list output mixes 36-character UUIDs from legacy records with newer
  29-character timestamp-prefixed IDs, so status and other columns start at
  inconsistent positions.
- **Evidence:** Running `./bin/factory monitor list --limit 5` against the local
  store showed both formats in adjacent rows. Source inspection confirms the
  `ID` column uses `%-29s`; UUID IDs overflow that width by seven characters.
- **Impact:** Rows in the same status/repository/PR columns do not align, making
  a multi-monitor list harder to scan.
- **Desired outcome:** Keep all remaining monitor columns aligned for either ID
  format without truncating or changing IDs.
- **Acceptance criteria:** The ID column width accommodates the longest
  supported ID format; tests cover UUID and timestamp-prefixed IDs in the same
  list and assert column positions and unchanged ID values; individual get
  output and persisted IDs remain unchanged.
- **Status:** Implemented in `internal/factory/monitor.go`: list headers and
  rows reserve 36 characters for IDs without changing the IDs themselves.
  Behavior tests verify UUID and timestamp-prefixed records remain complete and
  the status column starts at the same position for both.
