# Usability issues

This note records user-reported findings and factual observations from a
session. Findings remain separate; observations do not imply root causes or set
priorities. Status refers to the actual CLI/worker implementation, not to
intentions or documentation alone. See the [issues index](README.md) for
recording guidance.

## User-reported findings

### 1. Add command-specific help

- **Finding:** Root help is broad when users need instructions for a particular
  workflow.
- **Desired outcome:** Layered help so, for example, `factory tidy --help`
  documents `tidy` without unrelated workflow details.
- **Status:** Implemented in the current CLI source: `main.go` recognizes
  command-specific `-h`/`--help`/`help` requests for workflow and management
  commands. The integrated tree is verified by the current `make test` and
  `make vet` checks.

### 2. Make full use of Factory easier to learn

- **Finding:** Users want guidance for fuller use of Factory's capabilities,
  including running multiple jobs in parallel.
- **Desired outcome:** Discoverable workflow guidance that explains job
  concurrency and its limits.
- **Status:** Implemented in `.agents/skills/factory/SKILL.md`; multiple
  detached jobs are only claimed concurrently for different canonical
  implementation targets, while same-target implementation admission rejects
  an active duplicate. This documentation does not imply universal parallel
  support. The integrated tree is verified by the current `make test` and
  `make vet` checks.

### 3. Rename `show` to `get`

- **Finding:** The `show` command name does not match the requested vocabulary.
- **Desired outcome:** Use `get`. The original finding did not specify command
  families.
- **Status:** Implemented in the current CLI source for job, run, and monitor
  inspection. `get` is canonical and `show` (job/run) or `describe` (monitor)
  remains accepted as a compatibility alias. The integrated tree is verified
  by the current `make test` and `make vet` checks.

### 4. Make inspection output easier to scan

- **Finding:** Individual job inspection prints multiple labeled fields, including
  a long process-count explanation, rather than a compact row like `job list`.
- **Desired outcome:** `factory job get <id>` shows one compact table row by
  default, with the description and latest activity bounded as in `job list`;
  `--details` continues to show full metadata and logs.
- **Status:** Implemented in `internal/factory/job.go`: default `factory job
  get <id>` uses the `writeJobTable` format for a single bounded row; `--details`
  retains the full summary, metadata, and logs. Behavioral coverage verifies
  one header plus one row, bounded description/activity, ID/type/status/target
  values, and full details/log output. Verified with the focused `go test
  ./internal/factory` cases for job inspection and trace display.

### 5. Explore a kubectl-inspired CLI experience

- **Finding:** The user values kubectl's CLI design and wants Factory to draw
  inspiration from it.
- **Desired outcome:** Evaluate which CLI design principles could improve
  Factory's consistency, discoverability, and ease of use, adapting them to
  Factory's workflows rather than copying kubectl's commands or behavior.
- **Status:** Open; recorded for future CLI design and usability work. No
  particular command redesign or compatibility change has been approved.

## Session observations

These are observations from a session, not claims about underlying causes. The
compile and test failures in the historical sequence below are not current: the
integrated tree now passes `make test` and `make vet`.

### Historical symlink-`TMPDIR` test observations (not current failures)

During that session, `TMPDIR` was set to a symlink to `/tmp` before running the
following commands from the repository root (the temporary symlink was removed
afterward):

```sh
link=/tmp/factory-tmpdir-link-$$
mkdir "$link" && rmdir "$link"
ln -s /tmp "$link"
trap 'rm -f "$link"' EXIT
TMPDIR="$link" make test
TMPDIR="$link" go test ./internal/factory
```

Both commands failed while `TMPDIR` pointed through the symlink. The first
attempts, while Go files were changing, failed to compile: `make test` reported
undefined `commandHelpRequested` and `printCommandHelp` in `cmd/factory/main.go`,
and `go test ./internal/factory` reported invalid multi-character rune literals
in `internal/factory/managed_run.go` and `internal/factory/monitor.go`. Those
compiler failures are historical session output, not current failures. After
the source compiled, that historical `TMPDIR="$link" make test` run reported
failures in
`TestCommandHelpRoutesBeforeConfigAndWorkflowDispatch/monitor_canonical`,
`.../babysit_alias` (a historical assertion expecting the phrase `describe
remains an alias` on a removed CLI alias route; this is stale and is not current
user-facing behavior—`monitor describe` remains a compatibility alias for
`monitor get`),
and `TestBabysitListAndDescribePersistedMetadata` (expected persisted metadata
and log details). `go test ./internal/factory` passed in that run. These results
show failures observed with symlinked `TMPDIR`, not proof that the symlink caused
them; the same full test failure was observed without the symlink. Recent test
changes canonicalize paths because Go may resolve temporary paths differently
under symlinked `TMPDIR`; those test edits are not CLI worker implementation.

### Monitor workspace accumulation

Observed local Git metadata contained eight `factory-babysit/*` branches and only
the main checkout in `git worktree list`. The branches included names associated
with multiple monitor attempts; monitor code stores per-attempt worktrees under
job event directories and contains cleanup for some transitions. Interpretation:
branch refs can remain after worktrees are removed, so branch count is not proof
of eight extant checkouts or active monitors. The observation alone does not
establish storage growth, a leak, or a root cause. Reproduce the inventory with
`git branch --list 'factory-babysit/*'` and `git worktree list --porcelain`.

### Monitor approval prompt lacks proposal context

- **Status:** Approval-gated improvement implemented in the separate
  `feature/monitor-usability` worktree; awaiting verification and review.
- **Reproduction before change:** For a monitor with a pending proposal, run
  `factory monitor approve <id>`. The CLI immediately asks for exact lowercase
  `y` and then asks for scope text, without displaying the proposal it is asking
  the user to approve. The proposal is only visible via `factory monitor get
  <id> --details`.
- **Impact:** The approval action is hard to evaluate in context and may require
  stopping to issue a separate inspection command.
- **Evidence:** In `internal/factory/monitor.go`, the `approve` branch checked
  that `PendingSignature` and `Proposal` were populated but printed only the
  confirmation question. The approval regression test used a proposal
  (`Scope requested`) and now asserts it appears before confirmation.
- **Desired outcome:** Display the exact pending proposal before the yes/no
  confirmation and scope prompt.
- **Acceptance criteria:** The proposal text is labeled and printed before the
  confirmation prompt; approval remains bound to its existing snapshot and
  explicit non-empty scope; test verifies proposal-before-confirmation output;
  `make test`, `make vet`, and `make build` pass.

### Job inspection hides implementation publication outcome

- **Status:** Open; source inspection confirms `JobRecord` persists
  `PublicationStatus` and `PublicationSummary`, but default `factory job get`
  renders only the compact job table. The outcome is visible with `--details`.
- **Reproduction:** Complete an implementation job, then run
  `factory job get <id>` and `factory job get <id> --details`.
- **Impact:** Users cannot tell from the routine status view whether an
  implementation job created a PR, completed without publishing, or produced
  no changes. A next-action hint can therefore send users to inspect details
  even when the publication outcome is the important result.
- **Evidence:** `internal/factory/job.go` writes publication status and summary
  in `writeJobSummary` but not `writeJobTable`; publication outcomes are stored
  by `RunJobWorker` and the detached job publication tests cover published,
  unpublished, and no-op cases.
- **Desired outcome:** Show a compact publication outcome for implementation
  jobs in the default `job get` view without restoring verbose summary output.
- **Acceptance criteria:** The default output retains its compact table format
  and communicates publication status for implementation jobs that have an
  outcome; other job types and jobs without an outcome remain unchanged;
  behavioral tests cover published, unpublished, and no-op statuses;
  `make test`, `make vet`, and `make build` pass.

### Other observed friction

- During PR CI monitoring, the root help output was broad.
- Monitor `describe` output contained a large JSON record and lengthy signatures
  and paths, making the output difficult to scan.
- A narrow test-only fix was reported as `FIXED`; subsequent macOS CI still
  failed in other path-comparison tests. This records the sequence only and
  does not attribute the failures to a cause.
- A monitor attempt was blocked with `target checkout is no longer clean`. No
  cause for that guard result is inferred here.
