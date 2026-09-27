# Factory

`factory` is a standard-library-only Go CLI with task (`factory implement`), review/fix/document/verify (`factory tidy`), and detached PR monitor (`factory monitor` or `factory job start monitor`) workflows. Detached `factory job` is the lifecycle for implement, tidy, and monitor tasks; foreground and approval-gated `factory run` records remain separate and do not appear in the job list. `implement` starts an implementation job and attaches to its output by default; `--gate` retains the interactive foreground workflow so approvals remain usable. Running bare `factory` starts the interactive implementation workflow. The implementation workflow does not create branches or commits; foreground `tidy` publishes generated changes only after interactive exact-yes approval in pristine mode and has a non-publishing safe mode for initially dirty worktrees.

Factory’s primary value is managing a durable workflow through sequential stages, from requirements to implementation, review, and documentation. Pi subagents complement that lifecycle by handling focused, bounded slices of work—often independent tasks that can run in parallel—and can be used within a Factory workflow where appropriate. Factory also supports opt-in parallel implementation; that capability does not replace its durable stage orchestration.

## Detached implementation jobs

`factory job` lists only detached `implementation`, `tidy`, and `monitor` jobs. Detached job state is stored under `${XDG_STATE_HOME:-~/.local/state}/factory/detached-jobs`, or `<state_dir>/factory/detached-jobs` when configured. Implement and tidy jobs serialize work against other Factory jobs for the same canonical target; monitor jobs use the detached monitoring engine and its PR-specific duplicate guard.

```sh
factory job start implementation <description>
factory job start tidy <description>
factory job start monitor <description>
factory job list  # includes latest activity, status-call count, and observed Pi count
factory job get <id> [--details]
factory job logs <id>
factory job logs <id> --session workflow
factory job logs <id> --follow
factory job attach <id>
factory job stop <id>
```

`start` returns after launching the worker. Workflow events and worker output are retained in job/session logs; implement and tidy jobs use a `workflow` session. Detached tidy runs review/fix/document and verification but never commits or pushes, even when pristine or when local commits are ahead. Generated changes remain in the target checkout and are reported as unpublished. `attach` follows worker output to terminal state; Ctrl-C detaches only the observer, and the worker continues. Reattach later with the same command or use `job logs --follow`. `stop` writes a durable cooperative cancellation request; the worker polls it and cancels active work. Implementation sessions use one job-level `workflow` session. Monitor jobs use the monitor ID for both the detached job and its `monitor` session; `factory monitor stop/reset` operate on that same job and session.

Inspect jobs, runs, or monitors with their respective `get` commands; each accepts `--details` for long descriptions, records, proposals, and logs. Job list/get includes the latest recorded workflow or monitor activity, the count of secondary status invocations (including failed and canceled calls), and a count of currently live Factory-launched direct Pi subprocesses. Pi counts are best-effort direct-process observations only: they do not include descendants and are not used for liveness or stuck-job decisions. List output is tabulated; individual `get` output is concise labeled fields.

## Requirements

- Go 1.26.3 or later to build and test
- Linux or macOS
- `git` and GitHub CLI (`gh`) for `factory monitor`
- A configured agent executable; the default adapter uses `pi`

## Build, version, and release

Development builds report `dev`. The `factory version` command prints the version embedded in the binary; release builds set it from the Git tag with a linker flag.

```sh
make build
./bin/factory version
make test
make vet
```

The binary is written to `./bin/factory`. Build a versioned local binary with:

```sh
go build -ldflags "-X main.version=v1.2.3" -o bin/factory ./cmd/factory
```

CI runs on pull requests targeting `main` and pushes to `main`. Releases are separate: pushing a stable `vMAJOR.MINOR.PATCH` tag (for example, `v1.2.3`) runs tests and builds Linux and macOS archives for amd64 and arm64, with SHA-256 checksums, then publishes a GitHub release. Prerelease and build-metadata tags are rejected. Merging to `main` does not create a release; release tags must be created explicitly.

## Interactive task workflow

Run `factory implement` from the repository to change. By default it starts a durable implementation job and attaches to its output until completion. `factory implement --detach <description>` (or `-d`) starts the same job and returns immediately; use `factory job list/get/logs/attach/stop` to manage it. `--gate` remains a foreground run and cannot be combined with `--detach`. Ctrl-C detaches the terminal observer without stopping the job; use `factory job attach <id>` to reattach or `factory job stop <id>` to request cancellation. Gated foreground runs are managed with `factory run list/get/events/stop`; stop requests are cooperative and never signal a PID. With no description arguments, the implementation workflow requires an interactive terminal and prompts for task text (one line per paragraph; a line containing only `.` ends input). Interactive input lines are limited to 64 KiB. Bare `factory` starts the same interactive implementation workflow. To provide the task directly without a task-entry prompt, pass its description as arguments; Factory joins them with spaces:

```sh
factory implement add a small feature
factory implement --detach add a small feature
factory implement --gate add a small feature
factory --gate implement add a small feature
```

The implementation workflow runs the requirements, implementation, review, and documentation agent stages in order. Each stage invokes its agent exactly once; a nonzero exit or invocation error fails the workflow without retrying. There is no independent evaluator or correctness verdict, so successful exit does not certify that the result meets the task. Run the checks appropriate to the change and inspect the resulting diff. `--gate` uses the interactive foreground workflow and preserves exact `yes` approvals after successful stages; it is not detached because detached workers cannot safely proxy approval input.

Each implementation stage has a 30-minute active agent-execution budget (including parallel implementation workers). The budget pauses while waiting for human approval and other non-agent work; it is not a whole-job deadline.

Parallel implementation is disabled by default. Set `parallel_implementation` to `{"enabled": true}` in config to opt in; optionally set `max_concurrency` from 1 through 8 (default 4). During the implementation stage only, Factory asks the configured agent for a JSON plan, validates its dependency DAG and disjoint exact file scopes, and runs subtasks concurrently in detached isolated worktrees up to the configured bound. Each completed dependency wave is scope-validated and staged outside the target checkout; later waves receive prior staged files. Factory applies staged output to the target only after every wave and final baseline/scope validation succeeds, and rolls back an unsuccessful apply. Undeclared and ignored outputs and unsafe file types are rejected; file contents and permission modes are preserved. A first worker error cancels the remaining workers in that wave. Planning or scope failures fail closed; dirty repositories, non-Git directories, and non-root Git worktrees retain the existing single implementation agent. This protects the target from scheduler-controlled partial integration but is not a security boundary against external concurrent writers or agent processes with access beyond their worktrees. Plan, per-subtask status, worktree/log paths, and outcomes are retained in run state and events. This opt-in mode does not make agents a security sandbox.

The requirements agent's working directory is the run-state directory outside the target repository; it receives the target path as task context. While any primary agent stage remains active, Factory launches a separate status-only invocation about once per minute. The latest bounded, sanitized status appears in progress activity and timestamped updates are persisted in a workflow job session when present. Status errors/timeouts do not affect the primary workflow. The built-in Pi adapter adds `--no-tools` to status calls only; custom adapters run as configured and are not guaranteed read-only or sandboxed. Later stage agents run with the target repository as their working directory. Implementation workflow agent processes use the configured `agent_timeout` (default `60m`) as a per-process timeout, capped in practice by the 30-minute active execution budget for each stage. The stage budget pauses during approvals and other non-agent work.

Foreground run records, task text, and logs are stored outside the target repository. The run directory is printed at startup. Gated `implement --gate` runs persist an owner PID and heartbeat alongside the run state; `factory run list` and `factory run get <id>` classify liveness conservatively from the persisted heartbeat freshness; they do not infer that a process is dead from a PID alone. These controls apply only to gated runs. `factory run events <id> [--follow]` reads the persisted workflow event stream; `factory run stop <id>` writes a durable cooperative stop request checked during the gated workflow, including approval input. On a TTY, each active stage uses a full-screen operations console in the terminal's alternate screen, with a prominent stage and elapsed-time status, recent log activity, and log path. The original screen and cursor are restored when that stage completes, fails, or panics; the completion summary is then printed on the original screen. The console adapts to the terminal viewport and uses color unless `NO_COLOR` is set. At startup, `TERM=dumb`, terminals narrower than 40 columns or shorter than 6 rows, and non-TTY output use plain progress updates with `Stage`, `Elapsed`, and `Latest` fields; if an active TTY is resized below 6 rows, the console switches to a compact layout to keep the stage and log location visible. Stage progress and completion output identify the log path; use it to inspect full output (for example, `tail -f <log-path>`). Ctrl-C or SIGTERM cancels foreground agent runs and interactive task/approval waits. Workflow agent implementations must honor the supplied context through `RunWithContext`; an agent that only implements synchronous `Run` is rejected rather than being allowed to block cancellation. Factory does not create a branch or commit for the foreground implementation workflow. This is not a security sandbox: configured agents and their tools may still access or modify the repository. Factory does not copy its program source into the target.

## Tidy workflow

`factory tidy` has two modes. Both run review, fix, documentation, and `make fmt`, `make test`, and `make vet`; successful-stage approval prompts are skipped by default, while `factory tidy --gate` restores explicit approval gates. `factory tidy --detach [description...]` (or `-d`) starts a detached `tidy` job managed with `factory job`; detached tidy never publishes. `--gate` cannot be combined with `--detach`. Each agent stage invokes its agent once and fails on an invocation error; stages are not retried. No independent evaluator is run. Each stage has a 30-minute active agent-execution budget, which pauses during approvals, checks, and other non-agent work; this is not an overall job deadline.

**Pristine mode** applies when the index, tracked worktree, and untracked-file set are initially empty. It requires a non-detached branch, uses the configured non-local upstream when present, and requires the captured upstream commit to be an ancestor of local HEAD; upstream-ahead and diverged branches are refused. Without an upstream, it uses `origin/<current-branch>` only when `origin` has exactly one push URL and that remote branch does not exist. Before publishing, Factory inspects the raw configured push URL (or the remote URL when no push URL is configured) and refuses applicable `url.*.insteadOf` or `url.*.pushInsteadOf` rewrites; it also refuses if Git configuration cannot be inspected. Both fallback push paths use an empty expected-value lease for that ref, so if another writer creates it after validation, Git rejects Factory's push rather than replacing the competing ref. This is create-only protection, not a force-push; configured-upstream pushes do not use the fallback lease. A synced no-op creates no commit or push; existing local commits and any verified cleanup commit are pushed only after review and checks succeed. Pristine publication always requires an interactive terminal and the exact lowercase response `yes`, regardless of `--gate`; a negative response or missing TTY means no commit or push. It stages only run-created paths and checks branch/destination and saved content snapshots.

**Dirty safe mode** applies when staged, unstaged, and/or untracked changes exist at startup. It warns before running agents that agents and formatters may affect existing work. It then runs review, fix, documentation, and all three checks, but does not stage, commit, or push any files. This mode does not require an upstream or `origin`, because it does not publish. The warning is not a guarantee that existing work will remain unchanged; back up important changes first. Neither mode is a security sandbox: configured agents and formatters can access the repository. `factory tidy` is distinct from `make clean`, which only removes local build artifacts.

## Detached PR monitor

Start monitoring the open PR associated with the current checkout's branch:

```sh
factory monitor <description>
```

Each monitor agent action has a 30-minute active agent-execution budget, capped by the configured per-process `agent_timeout` (default `60m`). The budget pauses during approval waits, GitHub checks/snapshot queries, polling, and other non-agent work; there is no overall job deadline. A GitHub PR/check snapshot query has a separate two-minute timeout. Consecutive snapshot failures use bounded exponential backoff (starting at one second and capped at one minute); after eight failures the job enters `recoverable_failure` instead of retrying indefinitely.

The current directory must be a Git checkout with a clean working tree, a checked-out branch, and an `origin` that matches the PR head repository. The local branch head must equal the validated open PR head. The GitHub CLI must be able to identify and read the current PR. Factory rejects a second active monitor for the same PR. The same start path is used by `factory job start monitor <description>` and `factory monitor`.

Factory creates a detached worker and an isolated Git worktree; it does not run the monitor agent in the user's checkout. The monitor polls the PR and its checks (30 seconds by default; configurable with `FACTORY_MONITOR_POLL_INTERVAL`, which must be at least `1s`). It can respond to failed checks or a changed PR/check snapshot; a new comment can also trigger an initial response. Monitoring ends when the PR is merged or closed. If the PR head changes outside the monitor or repository identity/baseline checks fail, it refuses stale work rather than applying it.

A monitor agent's `FIXED` response signals successful completion of its action. Factory derives the complete changed-path set from Git, then revalidates the target checkout, worker worktree/branch/baseline and origin, safe relative file paths, and the live PR/check snapshot before committing and pushing. It stages only the Git-derived changed paths and does not force-push. There is no independent evaluator or correctness check: successful agent exit is not a correctness verdict. Agent errors, unsafe paths, a changed/stale snapshot, failed guardrails, or a stop request prevent the corresponding commit/push. A cooperative stop after a local commit but before push can leave that commit in the isolated worktree without pushing it.

If the agent requests human review, the job pauses with a proposal. After three automatic actions against an unchanged snapshot, the job also requires explicit approval. Approval requires the exact lowercase answer `y` and non-empty task text defining the approved scope; the approval is tied to the current PR/check snapshot and is invalidated if that snapshot changes. Rejecting a proposal resumes monitoring without repeating that action. Monitor approval authorizes a scoped agent action, but does not bypass Git-derived path selection or commit/push guardrails.

Manage jobs with these commands (use the job ID printed at startup or shown by `list`):

```sh
factory monitor list
factory monitor get <id> [--details]
factory monitor approve <id>
factory monitor reject <id>
factory monitor stop <id>
factory monitor reset <id>

```

Use `get <id>` for concise output and `get <id> --details` for proposals, actions, logs, and long values. List output is tabulated; individual monitor `get` output uses concise labeled fields. `stop` cancels any active agent subprocess (including its process group) and asks the worker to stop at a safe point; it does not forcibly kill the detached worker itself. `reset` is available only for a `recoverable_failure` job whose worker is no longer running. It clears the snapshot-failure count and restarts the detached worker, allowing monitoring to resume after the eight-failure cap.

## Configuration and state

Configuration is read from `${XDG_CONFIG_HOME:-~/.config}/factory/config.json`. If no file exists, Factory uses the embedded defaults. See [`config.json.example`](config.json.example). A configuration has a non-empty `command`, an `args` array, and optional `prompt_dir`, absolute `state_dir`, `agent_timeout`, `pipeline_checks`, and `parallel_implementation` values. `pipeline_checks` is an array of argv arrays (for example, `[ ["make", "test"], ["go", "vet", "./..."] ]`); each check is executed directly without a shell, sequentially after all workflow stages have passed. Full stdout/stderr transcripts are stored in the external run directory; a failed check error identifies its transcript path. The local `workflow-events.jsonl` stream records typed `check.started` and `check.completed` events, also sent to configured workflow observers. Each JSONL record has `version` (integer), `timestamp` (RFC 3339 timestamp), `runID` (string), and `type` (string); optional event fields are `stage` (string), `message` (string), `command` (string array), `transcript` (run-relative path), `outcome` (string), `exit_code` (integer), `started_at` and `ended_at` (timestamps). Check events include command and transcript; completion additionally reports outcome (`success`, `failure`, or `canceled`), exit code, and both timestamps. Check stdout/stderr belong in the transcript, not event messages. `agent_timeout` is a Go duration string controlling each individual agent process timeout; it defaults to `60m`. Implementation and tidy agent execution is additionally limited by a 30-minute active budget per stage, and monitor by a 30-minute active budget per agent action. Budgets pause during approval waits, tidy checks, GitHub snapshot queries, polling, and other non-agent work. They are not whole-job deadlines. `agent_timeout` does not affect the two-minute GitHub snapshot timeout or tidy verification commands (`make fmt`, `make test`, and `make vet`). Command and arguments are passed directly to `os/exec`, without a shell. `{system_prompt}` and `{task}` must each occur exactly once across the command and arguments; `{workdir}` and `{stage}` are optional placeholders. The default adapter is:

```text
pi -p --no-session --append-system-prompt {system_prompt} {task}
```

Embedded prompts can be overridden by stage-name files in `prompt_dir`, including `requirements.md`, `implement.md`, `review.md`, `fix.md`, `document.md`, `monitor.md`, and `status.md`.

Foreground run state is stored under `${XDG_STATE_HOME:-~/.local/state}/factory/runs` (or `<state_dir>/runs` when configured). Detached jobs are stored under `${XDG_STATE_HOME:-~/.local/state}/factory/detached-jobs` (or `<state_dir>/factory/detached-jobs`). State directories must resolve outside the target repository. Monitor polling is controlled by `FACTORY_MONITOR_POLL_INTERVAL` and must be at least `1s`.

Use `factory help` (or `factory -h` / `factory --help`) for concise CLI help. Command-specific help (for example, `factory monitor help`) lists that command's commands and options.

## Go package layout

`cmd/factory` contains the CLI entry point and terminal-specific helpers. `internal/factory` contains the implementation: configuration and prompts, agent process execution, the human-gated workflow and run state, and detached monitoring and safeguards. The prompt templates are embedded from `internal/factory/prompts`.

## Safety notes

These safeguards constrain Factory's own workflow; they are not a security sandbox for agent processes or their tools. A configured agent may have broader access than the task requires. Review the agent configuration, prompt overrides, repository permissions, and generated proposals/logs accordingly. Foreground agents can modify the target checkout but Factory itself does not branch or commit in that workflow. Monitor worktree, snapshot, Git-derived path, and stop checks limit automatic writes, but routine changes that pass those checks may be committed and pushed to the PR branch. These safeguards do not independently verify correctness.
