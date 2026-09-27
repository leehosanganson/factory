# Tidy workflow

`factory tidy` runs the repository-wide review, fix, documentation, and verification workflow. Its built-in checks are `make fmt`, `make test`, and `make vet`. Agent stages run once and fail on invocation error; successful exit is not a correctness verdict. Successful-stage approval prompts are skipped by default; `factory tidy --gate` restores explicit gates.

Foreground tidy has two modes:

- **Pristine mode:** when the index, tracked worktree, and untracked-file set are initially empty, successful work may be committed and pushed only after safeguards and an interactive exact lowercase `yes` confirmation. It requires an eligible branch/upstream configuration. A synced no-op does not create a commit or push.
- **Dirty safe mode:** when any staged, unstaged, or untracked changes exist at startup, tidy warns that agents and formatters may affect existing work. It runs the workflow and checks but does not stage, commit, or push files. The warning is not a guarantee that existing work is untouched.

`factory tidy --detach [description...]` starts a detached tidy job. Detached tidy is always nonpublishing: generated changes remain in the target checkout and are reported as unpublished. It cannot be combined with `--gate`.

Neither mode is a security sandbox. Review the resulting diff and check output. See [detached jobs](jobs.md) and the [operating skill](../../.agents/skills/factory/SKILL.md) for details.
