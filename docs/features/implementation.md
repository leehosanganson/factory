# Implementation workflow

`factory implement [description...]` runs a persisted workflow through requirements, implementation, review, and documentation. With no description, it prompts for task text and requires an interactive terminal. Bare `factory` starts the same interactive implementation workflow. Each stage invokes the configured agent once; nonzero exit or invocation failure stops the workflow. Successful exit is not an independent correctness evaluation.

By default `implement` starts a detached implementation job and attaches to its output. `--detach` starts and returns immediately; manage it with `factory job`. `--gate` selects the foreground workflow and enables exact `yes` approvals between successful stages; it cannot be combined with detached mode. Foreground `factory run` records are managed separately from detached jobs. The implementation workflow itself does not create a branch or commit.

## Opt-in parallel implementation

Parallel implementation is disabled by default. Set `parallel_implementation.enabled` to `true` in config; `max_concurrency` is optional, from 1 through 8 (default 4). During the implementation stage only, Factory requests a JSON subtask plan, validates its dependency graph and disjoint exact file scopes, and executes dependency waves in isolated worktrees. Completed outputs are checked and staged externally; only after all waves and final baseline/scope validation does Factory apply the combined output to the target. Planning/scope failures fail closed, and an unsuccessful apply is rolled back. Dirty targets, non-Git targets, and non-root worktrees use the single-agent implementation path.

This is guarded output integration, not a security sandbox: agents and external processes may have access beyond worker worktrees. Parallel implementation is not general concurrency control for arbitrary tasks or chained jobs.

See the [configuration example](../../config.json.example), [job lifecycle](jobs.md), and [Factory operating skill](../../.agents/skills/factory/SKILL.md).
