# Session observation: missing gated-run feedback

- `factory run get missing` with an empty state root displayed the full internal
  `lstat` state path. Source inspection showed `managedRunByID` forwarded a
  missing-directory error from its directory validation step.
- The change reports a valid but absent run as `run not found: <id>` and retains
  malformed-ID/symlink rejections. A separate detached-job counterpart is in
  PR #30; this change remains independently scoped to managed gated runs.
