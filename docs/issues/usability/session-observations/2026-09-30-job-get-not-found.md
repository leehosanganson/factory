# Session observation: missing detached job feedback

- Running `factory job get missing` against an empty local state root returned
  an `lstat` filesystem error containing the internal state path. Inspection
  traced it to `JobStore.GetJob` forwarding a missing directory error.
- The fix makes a valid but absent ID a clear job-not-found result while
  preserving invalid-ID and symlink-path rejection. Focused tests check all
  three outcomes; full verification is recorded with the PR.
