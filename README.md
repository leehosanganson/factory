# Factory

`factory` is a standard-library-only Go CLI with three workflows: a task workflow (`factory pipeline`), a clean/review/verify workflow (`factory clean`), and a detached pull-request babysitter (`factory babysit`). Both foreground workflows run a configured agent in fresh processes. The pipeline does not create branches or commits; `factory clean` publishes generated changes only in pristine mode and has a non-publishing safe mode for initially dirty worktrees. Running `factory` with no subcommand remains an interactive alias for `factory pipeline`.

## Requirements

- Go 1.26.3 or later to build and test
- Linux or macOS
- `git` and GitHub CLI (`gh`) for `factory babysit`
- A configured agent executable; the default adapter uses `pi`

## Build and test

```sh
make build
make test
make vet
```

The binary is written to `./bin/factory`.

## Interactive task workflow

Run `factory pipeline` from the repository to change. With no description arguments, it requires an interactive terminal and prompts for task text (one line per paragraph; a line containing only `.` ends input). Interactive input lines, including gated approval and retry responses, are limited to 64 KiB. The no-argument `factory` alias behaves the same. To provide the task directly without a task-entry prompt, pass its description as arguments; Factory joins them with spaces:

```sh
factory pipeline implement a small feature
factory pipeline --gate implement a small feature
factory --gate implement a small feature
```

Without `--gate`, passing stages proceed automatically and failed stages/evaluations retry without prompting (up to four attempts). Evaluators must still emit exact `PASS` as their first non-empty stdout line. Add `--gate` to either pipeline form to require the existing exact `yes` approval after each passing stage and before each retry.

Both invocation forms run these stages in order:

1. Requirements, then a fresh evaluator.
2. Implementation, then a fresh evaluator.
3. Review, then a fresh evaluator.
4. Documentation, then a fresh evaluator.

With `--gate`, an exact `yes` approval is also required after each passing evaluation, including the final stage.

The requirements agent's working directory is the run-state directory outside the target repository; it receives the target path as task context. Evaluators and later stage agents run with the target repository as their working directory. Evaluators must exit successfully and emit exactly `PASS` as their first non-empty stdout line. Evaluator stdout and stderr are both retained in the combined log, but only stdout is checked for the PASS protocol. With `--gate`, a passing evaluator requires exact `yes` approval to continue; retries (up to four attempts) likewise require exact `yes`. Any other gated response stops the run.

Foreground run records, task text, and logs are stored outside the target repository. The run directory is printed at startup. On a TTY, each active stage uses a full-screen operations console in the terminal's alternate screen, with a prominent stage, attempt and elapsed-time status, recent log activity, and log path. The original screen and cursor are restored when that stage completes, fails, or panics; the completion summary is then printed on the original screen. The console adapts to the terminal viewport and uses color unless `NO_COLOR` is set. At startup, `TERM=dumb`, terminals narrower than 40 columns or shorter than 6 rows, and non-TTY output use plain progress updates with separate `Stage`, `Attempt`, `Elapsed`, and `Latest` fields; if an active TTY is resized below 6 rows, the console switches to a compact layout to keep the stage, attempt, and log location visible. Stage progress and completion output identify the log path; use it to inspect full output (for example, `tail -f <log-path>`). Ctrl-C or SIGTERM cancels foreground agent runs and interactive task/approval/retry waits. Workflow agent implementations must honor the supplied context through `RunWithContext`; an agent that only implements synchronous `Run` is rejected rather than being allowed to block cancellation. This progress is informational only: an evaluator must exit successfully and emit exactly `PASS` as its first non-empty stdout line. Both streams remain in the log, but only stdout is checked for the PASS protocol. Factory does not create a branch or commit for the pipeline. Agent and evaluator processes use the configured `agent_timeout` (default `60m`); this applies to pipeline, clean, and babysit invocations. This is not a security sandbox: configured agents and their tools may still access or modify the repository. Evaluators run in the repository too. Factory does not copy its program source into the target.

## Clean workflow

`factory clean` has two modes. Both run review, fix, documentation, and `make fmt`, `make test`, and `make vet`; successful stage and retry approval prompts are skipped by default, while `factory clean --gate` restores explicit approval gates. Evaluators must still pass with exact `PASS` first, and each stage has at most four attempts.

**Pristine mode** applies when the index, tracked worktree, and untracked-file set are initially empty. It requires a non-detached branch, uses the configured non-local upstream when present, and requires the captured upstream commit to be an ancestor of local HEAD; upstream-ahead and diverged branches are refused. Without an upstream, it uses `origin/<current-branch>` only when `origin` has exactly one push URL and that remote branch does not exist. Before publishing, Factory inspects the raw configured push URL (or the remote URL when no push URL is configured) and refuses applicable `url.*.insteadOf` or `url.*.pushInsteadOf` rewrites; it also refuses if Git configuration cannot be inspected. Both fallback push paths use an empty expected-value lease for that ref, so if another writer creates it after validation, Git rejects Factory's push rather than replacing the competing ref. This is create-only protection, not a force-push; configured-upstream pushes do not use the fallback lease. A synced no-op creates no commit or push; existing local commits and any verified cleanup commit are pushed only after review and checks succeed. It stages only run-created paths and checks branch/destination and saved content snapshots.

**Dirty safe mode** applies when staged, unstaged, and/or untracked changes exist at startup. It warns before running agents that agents and formatters may affect existing work. It then runs review, fix, documentation, and all three checks, but does not stage, commit, or push any files. This mode does not require an upstream or `origin`, because it does not publish. The warning is not a guarantee that existing work will remain unchanged; back up important changes first. Neither mode is a security sandbox: configured agents and formatters can access the repository. `factory clean` is distinct from `make clean`, which only removes local build artifacts.

## Detached PR babysitter

Start monitoring the open PR associated with the current checkout's branch:

```sh
factory babysit <description>
```

The babysit agent and its independent evaluator use the configured `agent_timeout` (default `60m`). A GitHub PR/check snapshot query has a separate two-minute timeout; `agent_timeout` does not change it. Consecutive snapshot failures use bounded exponential backoff (starting at one second and capped at one minute); after eight failures the job enters `recoverable_failure` instead of retrying indefinitely.

The current directory must be a Git checkout with a clean working tree, a checked-out branch, and an `origin` that matches the PR head repository. The local branch head must equal the validated open PR head. The GitHub CLI must be able to identify and read the current PR. Factory rejects a second active babysitter for the same PR.

Factory creates a detached worker and an isolated Git worktree on a `factory-babysit/...` branch; it does not run the babysit agent in the user's checkout. The monitor polls the PR and its checks (30 seconds by default; configurable with `FACTORY_BABYSIT_POLL_INTERVAL`, which must be at least `1s`). It can respond to failed checks or a changed PR/check snapshot; a new comment can also trigger an initial response. Monitoring ends when the PR is merged or closed. If the PR head changes outside the babysitter or repository identity/baseline checks fail, it refuses stale work rather than applying it.

A babysit agent's `FIXED` response is only a proposal. Factory does not commit or push unless there are changes, a separate evaluator exits successfully with exact `PASS` as its first non-empty stdout line, and that evaluator's authorized file list exactly matches the complete changed-path set. The evaluator log retains both stdout and stderr; only stdout is used for the verdict. Before committing and pushing, Factory revalidates the target checkout, worker worktree/branch/baseline and origin, safe relative file paths, and the live PR/check snapshot. It stages only the authorized paths and does not force-push. Agent/evaluator errors, missing or invalid protocol output, an evaluator failure, a changed snapshot, or a stop request prevent the corresponding commit/push. A cooperative stop after a local commit but before push can leave that commit in the isolated worktree without pushing it.

If the agent or evaluator requests human review, the job pauses with a proposal. After three automatic attempts against an unchanged snapshot, the job also requires explicit approval. Approval requires the exact lowercase answer `y` and non-empty task text defining the approved scope; the approval is tied to the current PR/check snapshot and is invalidated if that snapshot changes. Rejecting a proposal resumes monitoring without repeating that action. Babysit approval authorizes a scoped agent action, but commit/push still requires the independent evaluator and commit/push guards above.

Manage jobs with these commands (use the job ID printed at startup or shown by `list`):

```sh
factory babysit list
factory babysit describe <id>
factory babysit approve <id>
factory babysit reject <id>
factory babysit stop <id>
factory babysit reset <id>
```

`describe` shows job details and available actions/logs. `stop` cancels any active agent/evaluator subprocess (including its process group) and asks the worker to stop at a safe point; it does not forcibly kill the detached worker itself. `reset` is available only for a `recoverable_failure` job whose worker is no longer running. It clears the snapshot-failure count and restarts the detached worker, allowing monitoring to resume after the eight-failure cap.

## Configuration and state

Configuration is read from `${XDG_CONFIG_HOME:-~/.config}/factory/config.json`. If no file exists, Factory uses the embedded defaults. See [`config.json.example`](config.json.example). A configuration has a non-empty `command`, an `args` array, and optional `prompt_dir`, absolute `state_dir`, and `agent_timeout` values. `agent_timeout` is a Go duration string controlling agent and evaluator process timeouts for pipeline, clean, and babysit; it defaults to `60m`. It does not affect the two-minute GitHub snapshot timeout or clean verification commands (`make fmt`, `make test`, and `make vet`). Command and arguments are passed directly to `os/exec`, without a shell. `{system_prompt}` and `{task}` must each occur exactly once across the command and arguments; `{workdir}` and `{stage}` are optional placeholders. The default adapter is:

```text
pi -p --no-session --append-system-prompt {system_prompt} {task}
```

Embedded prompts can be overridden by stage-name files in `prompt_dir`, including `requirements.md`, `implement.md`, `review.md`, `document.md`, `evaluate.md`, and `babysit.md`.

By default, foreground run state is stored under `${XDG_STATE_HOME:-~/.local/state}/factory/runs`; babysit job state and logs are under `${XDG_STATE_HOME:-~/.local/state}/factory/jobs`. When `state_dir` is configured, foreground runs use `<state_dir>/runs` and babysit jobs use `<state_dir>/factory/jobs`. State directories must resolve outside the target repository. Babysit polling can be changed with `FACTORY_BABYSIT_POLL_INTERVAL`.

Use `factory help` (or `factory -h`) for the CLI usage summary. `factory babysit help` lists babysit-specific commands, including recovery with `reset`.

## Go package layout

`cmd/factory` contains the CLI entry point and terminal-specific helpers. `internal/factory` contains the implementation: configuration and prompts, agent process execution, the human-gated workflow and run state, and detached babysit monitoring and safeguards. The prompt templates are embedded from `internal/factory/prompts`.

## Safety notes

These safeguards constrain Factory's own workflow; they are not a security sandbox for agent processes or their tools. A configured agent may have broader access than the task requires. Review the agent configuration, prompt overrides, repository permissions, and generated proposals/logs accordingly. Foreground agents can modify the target checkout but Factory itself does not branch or commit in that workflow. Babysit branch, worktree, evaluator, snapshot, path, and stop checks limit automatic writes, but routine changes that pass those checks may be committed and pushed to the PR branch.
