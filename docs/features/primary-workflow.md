# Primary Factory-to-Pi workflow

Use this guide for a bounded engineering task in a repository you trust. It connects setup, implementation, publication, and recovery; the [implementation workflow](implementation.md) and [detached jobs](jobs.md) guides remain the references for full behavior and options.

## Check setup and verification

Install the configured agent (Pi by default), copy [`config.json.example`](../../config.json.example) to `~/.config/factory/config.json`, and set the agent command/arguments and checks appropriate to the project. For a repository with a `make test` target, set the top-level `pipeline_checks` property to `[ ["make", "test"] ]`. Choose commands the repository actually supports; do not assume every project has `make test`. Then run:

```sh
factory doctor
```

Doctor validates Factory's configuration and checks the configured agent executable and `git`; it checks `gh` when `auto_publish` is enabled. It reports how many `pipeline_checks` are configured, but does not display or run them. With publication enabled, zero checks is an advisory about verification confidence, not a blocker. Doctor does not access the target checkout, run tools, contact GitHub, or create state. Correct any invalid configuration or missing prerequisite before starting work.

## Run an implementation

From the target repository root on a named Git branch, give Factory a bounded task with observable acceptance criteria. By default, `factory implement` starts a detached implementation job and attaches to its output:

```sh
factory implement "Add input validation; cover empty and malformed values with tests"
```

The workflow runs requirements, implementation, review, and documentation. With default `auto_publish: true`, it also runs configured pipeline checks and attempts to commit, push, and create a GitHub PR. The invoking checkout must be clean for automatic publication. A successful agent/workflow exit is not an independent correctness verdict; inspect the diff and check evidence. Factory does not merge, release, or deploy.

If you want the command to return immediately, start detached:

```sh
factory implement --detach "Add input validation; cover empty and malformed values with tests"
```

In another process/terminal, use the job ID printed by the start command:

```sh
factory job list
factory job get <job-id>
factory job watch <job-id>
factory job logs <job-id> --session workflow
factory job attach <job-id>
```

`get` shows the current status and publication outcome; use `--details` for the worktree, branch, publication summary, and retained logs. `watch` refreshes status/activity until the job is terminal; it does not show logs or control the worker. `logs` reads the workflow session, and `attach` follows worker output. Ctrl-C while attached or watching detaches/stops only that observer; the worker continues. For gated foreground approvals, use `factory implement --gate "..."` in an interactive terminal; `--gate` cannot be combined with `--detach` and its records are managed with `factory run`, not `factory job`.

When publication succeeds, the worker reports the PR URL and `factory job get <job-id> --details` retains the published outcome/summary. Review the PR and configured-check evidence yourself. A PR is a handoff for human review, not proof of correctness or permission to merge.

## Stop or inspect incomplete work

Request cancellation with:

```sh
factory job stop <job-id>
```

Stop is cooperative, not an immediate kill. The worker observes the request; work or an external operation may already have completed, and local artifacts may remain. Inspect the outcome rather than assuming it stopped cleanly:

```sh
factory job get <job-id> --details
factory job logs <job-id> --session workflow
```

For a failed workflow or completed-but-unpublished implementation, inspect the recorded worktree and branch before deciding what to preserve:

```sh
git -C <worktree-path> status --short
git -C <worktree-path> diff
```

Details and the publication summary identify retained recovery artifacts and the reason publication did not complete. No automatic retry occurs. Keep needed changes and coordinate any manual recovery; do not assume a repeated push/PR operation is safe. A no-op makes no commit or PR. Successful publication normally removes its temporary worktree; failed work and unpublished output are retained for inspection. Avoid removing a worktree until its contents and any uncommitted changes have been reviewed.

If a detached implementation or tidy worker crashes, Factory waits until its last recorded activity is at least 30 seconds old before classifying the job as `interrupted`. This delay allows an active worker to refresh its activity; `interrupted` is not a retry/resume signal, and Factory will not replay the job. Inspect before deciding what to preserve:

```sh
factory job get <job-id> --details
factory job logs <job-id> --session workflow
git -C <worktree-path> status --short
git -C <worktree-path> diff
```

Use the details to confirm whether a worktree and branch were retained; do not assume these artifacts exist for every job. Review and preserve useful changes. Do not force-remove the worktree or delete its branch as part of interruption recovery.

Factory runs agents and repository-controlled content with the local process account's authority. A process, worktree, Nix shell, or container is not a security sandbox; only use repositories and tools you trust. `factory work` commands submit and inspect issue-work requests or record human direction; they do not start engineering or create PRs.
