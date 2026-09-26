# Findings audit

The ten prior findings below were audited and closed after checking current
implementation, CLI documentation, and tests. A separately reproduced gated
implementation UX issue is recorded below and is now fixed:

1. Detached implementation jobs and attach/stop/list/show/log controls are
   implemented (`internal/factory/job.go`, `internal/factory/job_attach.go`,
   `cmd/factory/main.go`) and documented in `README.md`.
2. Parallel implementation is an explicit opt-in feature with dependency-aware
   subtasks and bounded concurrency (`internal/factory/subtasks.go`;
   `internal/factory/subtasks_test.go`).
3. Per-stage history, check results, and subtask state are persisted
   (`internal/factory/state.go`; `internal/factory/workflow_test.go`).
4. Gated foreground runs have list/show/events/stop controls
   (`internal/factory/managed_run.go`; `internal/factory/managed_run_test.go`).
5. Gated-run heartbeat/liveness metadata is present and tested
   (`internal/factory/managed_run.go`; `internal/factory/managed_run_test.go`).
6. Workflow lifecycle and check progress are persisted as typed JSONL events
   (`internal/factory/workflow.go`; `internal/factory/pipeline_check_test.go`).
7. Pipeline validation commands are configurable and record durable results
   (`internal/factory/config.go`, `internal/factory/workflow.go`;
   `internal/factory/pipeline_check_test.go`).
8. Checked-in CI runs formatting, build, tests, and vet on pull requests and
   pushes (`.github/workflows/ci.yml`).
9. Pristine tidy uses an isolated worktree, validates it, and refuses unexpected
   worktree changes during checks (`internal/factory/clean.go`;
   `internal/factory/clean_test.go`).
10. Tidy requires exact interactive publication confirmation, keeps run state
    nonterminal through checks/publication, and persists check logs/results
    (`internal/factory/clean.go`; `internal/factory/clean_test.go`).

11. **Noninteractive gated implementation runs execute requirements before approval input fails — Medium**
    - **Reproduction:** Run `factory implement --gate "<task>"` with stdin or stdout redirected/piped, for example `printf '' | factory implement --gate "<task>"`. Before the fix, Factory ran the requirements agent and only then blocked/faulted at the approval prompt; the run was left stopped/interrupted rather than rejected up front.
    - **Evidence:** `cmd/factory/main.go` previously passed `Gate: true` into `Workflow.RunContext` without rejecting a non-terminal; unlike the neighboring `runCleanContext` guard, the gated pipeline had no early terminal check. `TestGatedImplementRejectsNonInteractiveBeforeAgentOrRunState` in `cmd/factory/main_test.go` checks agent-marker and `<state_dir>/runs` absence in separate stdin-non-TTY and stdout-non-TTY cases, with the opposite stream treated as a terminal in each case. `TestCleanRequiresTerminalForPublicationApproval` in `internal/factory/clean_test.go` covers the corresponding tidy terminal requirement.
    - **Impact:** A command that cannot provide interactive stage approvals performs agent work and leaves misleading persisted run state before it discovers that approvals cannot be collected.
    - **Status:** Fixed by rejecting `factory implement --gate` unless both stdin and stdout are terminals, before task execution. The existing gated workflow construction and stage/approval flow remain unchanged when both are terminals.
