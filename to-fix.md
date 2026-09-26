# Actionable findings

1. **No background pipeline mode — Medium**
   - **Evidence:** `cmd/factory/main.go:43-50,75-77` invokes the foreground pipeline directly; `cmd/factory/main.go:80-82` only adds signal cancellation. No detach/background option is exposed in the CLI usage at `cmd/factory/main.go:272-289`.
   - **Impact:** Long runs hold the invoking terminal/session open and cannot be started and left running through a supported pipeline command.
   - **Suggested improvement:** Add an explicit detached pipeline mode with persisted worker ownership, lifecycle handling, and a way to retrieve its run ID.

2. **Fixed sequential stages; no concurrent subtasks — Medium**
   - **Evidence:** `internal/factory/workflow.go:15` defines one ordered stage list, and `internal/factory/workflow.go:70-86` iterates each stage and waits for `runStage` before advancing. Each attempt likewise runs its agent then evaluator in sequence (`internal/factory/workflow.go:145-180`).
   - **Impact:** Independent parts of a task cannot be split and executed concurrently, increasing latency and limiting throughput.
   - **Suggested improvement:** Support explicit dependency-aware task decomposition and bounded parallel execution for independent subtasks while preserving ordered integration/evaluation where needed.

3. **Run state models only one stage — Low**
   - **Evidence:** `internal/factory/state.go:13-20` stores a single `Stage` string and overall `Status`; `internal/factory/workflow.go:74-86` overwrites that stage as it iterates.
   - **Impact:** Persisted state cannot represent per-stage attempts, outcomes, or concurrently active work, limiting useful inspection and recovery.
   - **Suggested improvement:** Persist per-stage records (including attempts and outcomes) while retaining a derived overall run status.

4. **No foreground-run control plane — Medium**
   - **Evidence:** The CLI help lists `babysit list/describe/stop` but no analogous foreground-run commands (`cmd/factory/main.go:269-285`). Pipeline creates a run and prints its directory (`internal/factory/workflow.go:61-65`) but exposes no listing, status, follow, or stop command for it.
   - **Impact:** Users cannot discover or manage a foreground run through Factory after launch, especially if output is lost or the process is detached externally.
   - **Suggested improvement:** Add foreground-run list/status/follow/stop commands backed by run IDs and managed process handles.

5. **Stale foreground runs look active — Medium**
   - **Evidence:** `internal/factory/state.go:13-20` has no PID or heartbeat field. `internal/factory/state.go:86-102` initializes `running` and updates `UpdatedAt` only when state is written; workflow writes stage transitions and terminal outcomes at `internal/factory/workflow.go:74-122`, not periodic liveness updates.
   - **Impact:** A crash can leave `status=running` indefinitely, indistinguishable from a live but slow run without manually interpreting timestamps and logs.
   - **Suggested improvement:** Record process identity and periodically refreshed liveness metadata, and expose a stale-state classification that avoids treating PID reuse as proof of activity.

6. **Progress is terminal/log-tail based, not structured events — Low**
   - **Evidence:** `internal/factory/progress.go:78-82,131-138` emits formatted text heartbeats from the latest log line; `internal/factory/progress.go:499-536` reads only a bounded log tail (up to three lines). No event stream or structured progress record is produced.
   - **Impact:** Other tools cannot reliably consume stage transitions, evaluator results, or progress without parsing human-oriented terminal text and logs.
   - **Suggested improvement:** Emit versioned structured progress events to a durable run event stream, keeping the terminal display as a presentation layer.

7. **Pipeline has no deterministic validation-command stage — Medium**
   - **Evidence:** `internal/factory/workflow.go:15,74-86` runs only the configured stages and evaluators; `internal/factory/workflow.go:125-180` invokes agent/evaluator processes but no test, lint, or build command. By contrast, `internal/factory/clean.go:128-142` explicitly runs `make fmt`, `make test`, and `make vet`.
   - **Impact:** A pipeline can pass its stage evaluators without Factory itself running repository checks, so the same task may complete without reproducible build/test evidence.
   - **Suggested improvement:** Allow explicit, configured validation commands after relevant stages and record their exact commands and exit results; do not assume every repository uses Make targets.

8. **No tracked CI workflow — Medium**
   - **Evidence:** The tracked repository contains no `.github/workflows/*`, `.gitlab-ci.yml`, `.circleci/*`, or `Jenkinsfile` (`git ls-files` check). `Makefile:1-14` provides local build/test/vet targets, and `README.md:12-18` documents local commands, but neither configures CI.
   - **Impact:** Repository changes have no checked-in automation guaranteeing build, tests, or vet run in CI.
   - **Suggested improvement:** Add a CI workflow for the supported Go version(s) that runs build, tests, and vet on pull requests and the default branch.

9. **Pristine clean inventories changes after agents and checks — Medium**
   - **Evidence:** `internal/factory/clean.go:81-107` captures the initially clean baseline and runs review/fix/document stages; `internal/factory/clean.go:128-150` runs format/test/vet and only then calls `cleanChangedPaths` to inventory tracked and untracked changes. The content snapshot is taken afterward at `internal/factory/clean.go:174-182`, then protects that inventory through staging/commit (`internal/factory/clean.go:187-218`).
   - **Impact:** In pristine mode, a file changed or added externally during the run can be included if it exists when the post-check inventory is collected; snapshot checks protect the captured contents thereafter, but do not prove the changes were produced by this run. Such files may be committed and pushed with the run's changes.
   - **Suggested improvement:** Establish run ownership before execution (for example, an isolated worktree or continuously reconciled change baseline) and reject changes that cannot be attributed to the run before publishing.

10. **Pristine clean publishes without default human approval; completion precedes checks and checks lack durable transcript — High**
    - **Evidence:** The clean CLI passes `Gate` from the optional `--gate` flag (`cmd/factory/main.go:51-61,133-151`); `internal/factory/clean.go:101-108` runs stages with that flag, and `internal/factory/workflow.go:208-234` only asks for approval when `w.Gate` is true. The workflow sets and persists `status=complete` immediately after stages (`internal/factory/workflow.go:110-122`), before clean runs `make fmt/test/vet` (`internal/factory/clean.go:128-142`); a later failure changes status to `failed` or `interrupted` via the defer (`internal/factory/clean.go:110-126`), but successful state has no per-check results. Check output is sent to `w.Out` (`internal/factory/clean.go:90-95`) rather than a durable check transcript.
    - **Impact:** In pristine mode, passing agent evaluations can lead to commit and push without a human approval by default; the persisted state can temporarily report completion before checks finish, and after success it does not retain an auditable per-check transcript. `factory clean --gate` is the explicit approval opt-in.
    - **Suggested improvement:** Make publish authorization an explicit human decision by default (or require an equally deliberate documented policy), keep run state nonterminal until all checks and publication finish, and persist each check's command, output/log reference, and exit status.
