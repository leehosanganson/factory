# Keep compact job-list rows within a useful width

- **Finding:** Detached-job list rows append the full target path, so long
  paths can dominate otherwise compact output.
- **Evidence:** Running `./bin/factory job list --limit 30` against the local
  state store produced a 260-character data row and a 179-character header.
  The target column is written without a width bound by `writeJobTable`.
- **Desired outcome:** Keep target paths in the default list bounded and
  recognizable, while preserving the complete path in detailed job inspection.
- **Acceptance criteria:** Long target paths do not cause unbounded list rows;
  common short paths remain unchanged; `factory job get <id> --details` retains
  the full target path; tests cover both short and long paths.
- **Status:** Implemented in `internal/factory/job.go`: compact rows truncate
  target paths longer than 80 Unicode code points to a recognizable prefix
  ending in an ellipsis. The stored path and detailed output remain complete.
  Behavioral tests cover unchanged short paths, bounded long paths, and complete
  `--details` output.
