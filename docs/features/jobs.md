# Detached jobs

`factory job` manages detached implementation, tidy, and monitor jobs. Jobs persist outside the target repository under `${XDG_STATE_HOME:-~/.local/state}/factory/detached-jobs`, or the configured state directory. `factory job start` supports these types only; foreground/gated `factory run` records are separate and do not appear in the detached job list.

```sh
factory job start implementation <description>
factory job start tidy <description>
factory job start monitor <description>
factory job list [--limit <n>]
factory job get <id> [--details]
factory job logs <id> [--session workflow] [--follow]
factory job attach <id>
factory job stop <id>
factory job watch <id>...
```

A start returns after launching a worker. Implementation and tidy workers serialize against other Factory jobs targeting the same filesystem location after resolving absolute paths and existing symlinks. Attach follows worker output; Ctrl-C detaches the observer without stopping the worker. `stop` records a cooperative cancellation request, which the worker checks; it is not a promise of immediate forced termination. Logs and workflow events are retained. Compact job list/get rows keep the full ID while bounding type, status, description, activity, publication, and target fields to keep rows within 120 characters; use `factory job get <id> --details` to inspect complete field values. Status-call and direct Pi subprocess counts are shown in detailed inspection, not compact rows. `factory job list --limit <n>` optionally displays only the newest N jobs in the existing order; N must be a positive integer. Omitting the option preserves the unbounded list, and all jobs are still reconciled before output is limited.

`factory job watch <id>...` refreshes only the selected jobs' status and latest recorded activity once per second. Each snapshot includes a safe next-action hint derived from persisted job type, status, and phase: active jobs need no action, pending monitor proposals show the inspect/approve/reject commands, recoverable monitors show the guarded reset command, failed or completed jobs show where to inspect results, and interrupted implementation/tidy jobs explicitly state that Factory will not replay them and point to details, workflow logs, and any retained worktree/branch for inspection. Hints do not include task descriptions, proposal text, or log contents. For monitor jobs the watch also shows the current phase, latest successful PR/check query time and concise check result, plus a bounded recent transition trail. Repeated IDs are shown once, in their first-supplied order. On a terminal it redraws the selected-job snapshot; when output is not a terminal, it prints labeled snapshots instead. It exits after all selected jobs reach a terminal status, or when interrupted/canceled. Watching is read-only with respect to worker lifecycle: it does not request cancellation, and it never displays job or session logs. A missing ID is reported as an error.

Implementation jobs in a Git repository run in an isolated worktree and retain their output there; before creating the worktree, Factory asks the configured agent adapter for a concise slug and normalizes the bounded single-line response. If that proposal fails, Factory uses a deterministic slug derived from the task description. The worktree folder uses the first four hexadecimal ID characters and a safe slug, while the branch and persisted job identity retain the full ID. The optional `worktree_parent` config is a path template shared by implementation and monitor worktrees; `{repo}` expands to the primary checkout basename, relative paths resolve from that checkout, and absolute paths are accepted. Its default is `../{repo}.worktrees`. Factory rejects worktree parents inside either the primary or invoking checkout, resolving symlinks before validation. A folder-name collision extends the ID prefix deterministically and refuses to reuse an existing unverified path. Factory does not automatically integrate or remove that output. When an implementation or tidy worker crashes, Factory waits for the existing 30-second stale-activity grace period before classifying its job as `interrupted`; the status is not a retry or resume signal, and Factory does not replay interrupted work. Inspect and preserve its artifacts without force-removing a worktree or deleting a branch. Use `factory job get <id> --details` to find any recorded `Worktree` path and `Work branch` (implementation jobs in Git repositories use a worktree; tidy jobs may not), then inspect the worker's changes where those artifacts exist, for example:

```sh
factory job get <id> --details
factory job logs <id> --session workflow
git -C <worktree-path> status --short
git -C <worktree-path> diff
```

Review and preserve any changes you want before cleanup. Only remove a completed or otherwise terminal job's worktree after confirming its path and branch from the job details. From the repository that owns the worktree, run `git worktree remove <worktree-path>`; Git refuses removal when there are uncommitted changes, so do not force removal unless you intentionally want to discard them. Once the worktree is removed, remove its branch if no longer needed with `git branch -d <work-branch>`. Job records, logs, and other persisted results are in `<state-dir>/factory/detached-jobs/<id>` (`<state-dir>` defaults to `${XDG_STATE_HOME:-~/.local/state}`). After verifying the job is terminal and saving anything needed, remove only that job's directory manually if desired, for example `rm -rf -- <state-dir>/factory/detached-jobs/<id>`. Removing the job data does not remove its Git worktree or branch, and removing the worktree does not remove the stored job logs/results.

Detached tidy never commits or pushes. The monitor job uses the PR-specific monitoring engine and its duplicate guard; see [PR monitor](monitor.md). Current detached types are not a generic arbitrary command runner, and jobs cannot be chained together.
