# 2026-10-18 recovery-bundle CI follow-up

- **Evidence:** PR #291's macOS CI failed `TestRecoveryBundleFailureNeverRemovesConcurrentDestination`; the test reported that the concurrent destination no longer existed after failed bundle creation. The recovery bundle publisher rolled back the destination pathname after publication when directory sync or post-publication validation failed. A concurrent writer can replace that pathname after the bundle hard-link is published, so cleanup may remove the other writer's file.
- **Change:** validate the completed temporary archive before publication, check cancellation before publication, and never remove the destination path as rollback after `os.Link` succeeds. Failures after publication leave the destination for operator inspection rather than risking deletion of a concurrent replacement.
- **Verification so far:** the focused race test passed 50 consecutive Linux runs; full cross-platform CI is still needed to verify the reported macOS failure.
- **Scope:** no changes to REST job lifecycle, worker progress, or dashboard behavior.
