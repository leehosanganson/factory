# Limit job-list output on request

- **Finding:** `factory job list` prints every persisted job, making recent jobs
  difficult to find in a long history.
- **Evidence:** A local CLI audit ran `./bin/factory job list` against the
  configured state store and observed dozens of historical records, including
  records whose targets were temporary paths.
- **Desired outcome:** Allow users to request only the newest N jobs without
  changing the existing unbounded default or altering persisted job records.
- **Acceptance criteria:** `factory job list --limit <n>` prints at most the N
  newest records in the existing newest-first order; an omitted limit preserves
  current output; invalid limits return a clear usage error; command help and
  behavioral tests document these guarantees.
- **Status:** Implemented in `internal/factory/job.go`; all jobs are reconciled
  and reconciliation errors aggregated before output is truncated. Job list
  help documents the positive integer limit. Behavioral tests cover newest-N
  ordering, the unchanged unbounded default, invalid limit arguments, and
  reconciliation of jobs beyond the output limit.
