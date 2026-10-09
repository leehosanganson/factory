# Subtask integration rollback pass

## Verified observations

- On refreshed `main` (`cb5b4bb4924854cf9027fad8d1b8355447cdac00`), a focused regression test showed that a missing later staged source was accepted after an earlier output had been applied.
- Validating each non-deletion staged source before applying it now fails closed and runs the existing rollback. Ten focused repetitions, the `internal/factory` package, and full `make test` passed; `make vet`, `make build`, and `git diff --check` passed.
- The test verifies original bytes and mode, unchanged unrelated content, retained staging input, and removal of newly created parent directories. No provider or network activity was used.

## Friction

- The PR listing tool returned an empty result while authenticated `gh` independently showed no open PRs; issue discovery through the GitHub issue tool succeeded. Cross-checking prevents treating an empty integration response as authoritative.
- A newly created worktree from current `origin/main` provided a clean base; the main checkout stayed clean.
- The repository has no `.github/pull_request_template.md`; the PR description will follow `CONTRIBUTING.md` instead.

## Status

Issue #184 is the focused rollback-test task. Implementation was verified locally; publication/CI state is recorded in the task report.
