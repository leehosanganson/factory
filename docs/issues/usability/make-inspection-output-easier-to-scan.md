# Make inspection output easier to scan

- **Finding:** Individual job inspection prints multiple labeled fields, including
  a long process-count explanation, rather than a compact row like `job list`.
- **Desired outcome:** `factory job get <id>` shows one compact table row by
  default, with the description and latest activity bounded as in `job list`;
  `--details` continues to show full metadata and logs.
- **Status:** Implemented in `internal/factory/job.go`: default `factory job
  get <id>` uses the `writeJobTable` format for a single bounded row; `--details`
  retains the full summary, metadata, and logs. Behavioral coverage verifies
  one header plus one row, bounded description/activity, ID/type/status/target
  values, and full details/log output. Verified with the focused `go test
  ./internal/factory` cases for job inspection and trace display.
