# Report missing job records clearly

- **Finding:** Requesting a nonexistent detached job can surface a low-level
  filesystem error instead of identifying the missing job.
- **Evidence:** With an empty state directory,
  `XDG_STATE_HOME=/tmp/factory-nonexistent-state ./bin/factory job get missing`
  printed `factory: lstat .../detached-jobs/missing: no such file or directory`.
  The same message comes from `JobStore.GetJob` because it returns the
  `Lstat` error for an absent record directory.
- **Desired outcome:** Return a concise, command-level not-found error for a
  well-formed but absent job ID, while retaining validation/security errors for
  invalid IDs and symlinked or malformed state paths.
- **Acceptance criteria:** Missing IDs produce `job not found: <id>` whether
  the store root or only that job is absent; invalid IDs and unsafe paths still
  fail safely; tests cover those cases.
- **Status:** Implemented in `JobStore.GetJob`: well-formed missing records
  now report `job not found: <id>`, while invalid IDs and unsafe paths retain
  their validation errors. Tests cover missing records and symlink rejection.
