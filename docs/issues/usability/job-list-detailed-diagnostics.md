# Show job diagnostics in detailed inspection

- **Finding:** The current job-list/get table does not show status-call or direct
  Pi subprocess counts, but the feature documentation says compact inspection
  includes both counts.
- **Evidence:** `./bin/factory job list --limit 5` rendered columns for ID, type,
  status, description, activity, publication, and target only. Source inspection
  confirms `writeJobTable` does not render `StatusCalls` or `ActivePi`, while
  `writeJobSummary` prints both in detailed output.
- **Desired outcome:** Describe the current compact and detailed inspection
  output accurately, so users know where to find the process-count diagnostics.
- **Acceptance criteria:** Feature documentation states that compact rows show
  bounded identity/status/activity/target information, while detailed job
  inspection includes status-call and direct Pi subprocess counts; does not
  claim counts are in list/get compact rows.
- **Status:** Implemented in `docs/features/jobs.md`; compact output and the
  `--details` location for process-count diagnostics now match the CLI.
