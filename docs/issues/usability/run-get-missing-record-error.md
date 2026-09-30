# Report missing gated-run records clearly

- **Finding:** Looking up a nonexistent gated run can expose a low-level
  filesystem error and state path instead of identifying the missing run.
- **Evidence:** With an empty state root,
  `XDG_STATE_HOME=/tmp/factory-nonexistent-state ./bin/factory run get missing`
  printed `lstat .../factory/runs/missing: no such file or directory`.
  `managedRunByID` passes the absent-directory error from `ensureRealDirectory`
  directly to the caller.
- **Desired outcome:** Report a concise not-found error for a valid but absent
  run ID, while retaining invalid-ID validation and symlink/path safety checks.
- **Acceptance criteria:** `factory run get <id>` and management commands using
  `managedRunByID` return `run not found: <id>` for absent well-formed IDs;
  invalid IDs and symlinked state directories remain rejected. Tests cover both
  cases.
- **Status:** Implemented in `managedRunByID`: valid but absent runs now return
  `run not found: <id>`, while invalid IDs and symlink paths retain their safety
  errors. Tests cover these outcomes.
