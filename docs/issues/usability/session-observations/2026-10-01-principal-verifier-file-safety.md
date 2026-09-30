# Principal verifier file safety follow-up

- PR #42 remains open for human review; the exact-schema review finding was fixed and both Ubuntu and macOS CI jobs passed on the rerun.
- Review of the loader identified a check-then-open symlink race and no ownership check. The loader now opens with `O_NOFOLLOW`, validates metadata on the opened file, and requires ownership by the effective UID. A wrong-owner test skips only when the test environment cannot change file ownership.
- `make test`, `make vet`, `make build`, focused safety/schema tests (10 runs), a Darwin/arm64 test cross-compile, and `git diff --check` passed locally. The cross-compile checks compilation only; tests were not executed on macOS locally.
- The already-open PR cannot contain this follow-up until the changes are committed and pushed. Keep the change scoped to PR #42; do not begin another enhancement before this PR is completed or explicitly paused.