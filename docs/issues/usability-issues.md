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
  inspection. `get` is canonical; the former `show` (job/run) and `describe`
  (monitor) aliases are rejected with guidance to use the canonical command.
  The integrated tree is verified by the current `make test` and `make vet`
  checks.

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

### 6. Job inspection hides implementation publication outcome

- **Finding:** The compact default `factory job get`/`job list` table omits the
  persisted publication outcome for completed implementation jobs.
- **Desired outcome:** Keep the default table compact while showing publication
  status when an implementation job has one; leave other rows unchanged.
- **Status:** Implemented in `internal/factory/job.go` with a `PUBLICATION`
  column containing the outcome for implementation rows only. Rows without an
  outcome and non-implementation rows keep this cell blank. Behavioral tests
  cover `published`, `unpublished`, `no-op`, absent outcomes, both default
  commands, and the unchanged one-header/one-row `job get` shape. Verified by
  `make test`, `make vet`, and `make build`.
- **Reproduction:** Inspect completed implementation jobs with
  `factory job get <id>` and `factory job list`.
- **Impact:** Users cannot distinguish successfully published work from
  unpublished or no-op results without opening detailed output.

### 7. Limit job-list output on request

- **Finding:** `factory job list` prints every persisted job, making recent jobs
  difficult to find in a long history.
- **Evidence:** A local CLI audit ran `./bin/factory job list` against the
  configured state store and observed dozens of historical records, including
  records whose targets were temporary paths.
- **Desired outcome:** Allow users to request only the newest N jobs without
  changing the existing unbounded default or altering persisted job records.
- **Acceptance criteria:** `factory job list --limit <n>` prints at most the N
  newest records in the existing newest-first order; an omitted limit preserves
  current output; invalid limits return a clear usage error; command help and
  behavioral tests document these guarantees.
- **Status:** Implemented in `internal/factory/job.go`; all jobs are reconciled
  and reconciliation errors aggregated before output is truncated. Job list
  help documents the positive integer limit. Behavioral tests cover newest-N
  ordering, the unchanged unbounded default, invalid limit arguments, and
  reconciliation of jobs beyond the output limit.

### 8. Sort monitor-list rows by displayed update time

- **Finding:** `factory monitor list` displays an `UPDATED` timestamp but orders
  rows by monitor creation time.
- **Evidence:** Source inspection showed `loadJobs` in
  `internal/factory/monitor.go` sorting by `CreatedAt`, while the list renderer
  displays `UpdatedAt`. In the audited local records, creation and update order
  happened to agree, so the mismatch was not reproduced from that snapshot.
- **Desired outcome:** Order monitor-list results by the same update timestamp
  shown in the table, newest first.
- **Acceptance criteria:** Monitor list sorting uses `UpdatedAt` descending;
  tests include records whose creation and update order differ and assert the
  displayed order; other monitor commands and persisted records are unchanged.
- **Status:** Implemented in `internal/factory/monitor.go`; behavioral coverage
  seeds monitor records with opposing creation and update order and asserts the
  rendered list order.

### 9. Limit monitor-list output on request

- **Finding:** `factory monitor list` prints every persisted monitor, making
  recent monitor jobs harder to find in a long history.
- **Evidence:** A local CLI audit ran `./bin/factory monitor list` against the
  configured state store and observed 12 historical monitor records.
- **Desired outcome:** Allow users to request only the newest N monitor jobs
  without changing the existing unbounded default or persisted records.
- **Acceptance criteria:** `factory monitor list --limit <n>` prints at most the
  N newest monitors in newest-first order; omission preserves existing output;
  invalid limits return a clear usage error; help and behavioral tests cover
  the option.
- **Status:** Implemented in `internal/factory/monitor.go`; the complete
  persisted monitor list is loaded and sorted newest-first before output is
  truncated. Both command-specific and canonical monitor help document the
  positive integer option. Behavioral tests cover truncation, order, unlimited
  default, and invalid arguments.

### 10. Order gated runs by their most recent update

- **Finding:** `factory run list` emits managed runs in directory enumeration order rather than recency order.
- **Evidence:** Source inspection of `internal/factory/managed_run.go` showed directory entries were appended directly to output rows; run IDs are not a recency field. The local state did not contain gated-run records, so this was not reproduced against saved runs.
- **Desired outcome:** List the most recently updated gated runs first, using the timestamp recorded for run state updates.
- **Acceptance criteria:** `factory run list` orders managed runs by `UpdatedAt` descending, with deterministic ordering for equal timestamps; tests seed deliberately conflicting timestamps and assert display order. Unmanaged records remain excluded and run persistence is unchanged.
- **Status:** Implemented in `internal/factory/managed_run.go`; list rows are sorted by `UpdatedAt` descending and then ID ascending for ties. Tests assert both recency order and tie order while confirming unmanaged records remain excluded.

### 11. Improve unit-test seams with mock dependencies

- **Finding:** The user wants modules and tests structured for unit testing
  with mocked dependencies.
- **Desired outcome:** Add narrow dependency seams where they enable
  deterministic unit tests, while retaining integration tests for actual
  process, filesystem, Git, and CLI contracts. Avoid a broad rewrite or mocks
  that erase important end-to-end behavior.
- **Status:** Pilot implemented in `internal/factory/workflow.go`: pipeline
  orchestration accepts a narrow runner, and the production process adapter is
  supplied by both workflow and automatic-publication call sites. The adapter
  retains context-aware subprocess execution and platform process cancellation;
  orchestration retains transcript, state, and event handling. No wider
  interfaces or refactors were introduced.
- **Acceptance criteria:** Fake-runner unit tests cover success, nonzero exit,
  process-start failure, cancellation classification, and exact
  argument/workdir forwarding. Existing subprocess integration tests remain
  and continue to cover real process output, start/nonzero failures,
  cancellation, workdir, argument ordering, and shell-free execution.
- **Evidence:** The fake-runner cases verify persisted check outcomes,
  completed lifecycle events, transcript output, and exact command/workdir
  forwarding. A missing-executable workflow integration test verifies the
  start-failure result and persisted lifecycle. Existing subprocess tests in
  `pipeline_check_test.go` remain. `make test`, `make vet`, `make build`, and
  `git diff --check` pass.
- **Desired outcome:** Demonstrate a focused, maintainable unit-test seam before
  considering wider module or test restructuring.

## Session observations

These are observations from a session, not claims about underlying causes.

- For issue 10, the run-list ordering test seeds managed records with an order
  that conflicts with their update timestamps, equal timestamps for tie-break
  coverage, and an unmanaged record. The focused test and full prescribed
  checks passed; local run state contained no gated runs for interactive
  reproduction.

- The job-list limit tests verified newest-first truncation, the unbounded
  default, validation errors, and aggregated reconciliation errors beyond the
  displayed limit. Focused tests, `make test`, `make vet`, and `make build`
  passed. `make help` is not defined; targets are listed in the Makefile.

- For the pipeline-check seam pilot, focused fake-runner and existing
  subprocess integration tests passed. The first `make test` run reported a
  `TempDir` cleanup error in
  `TestDetachedTidyCLIUsesDefaultDescriptionAndNeverPublishes`; an immediate
  standalone rerun of `make test` passed. `make vet` and `make build` passed.

- The `PUBLICATION` column keeps default job inspection to one table row while
  making persisted implementation outcomes visible; focused tests and `make
  test`, `make vet`, and `make build` passed. `make help` is not defined; the
  Makefile lists its targets directly.
- Focused monitor tests plus `make vet` and `make build` succeeded. A full test
  run exposed stale assertions in
  `TestDetachedImplementationPublicationOutcomePersistsAndCleansOnlyOnPublish`:
  it expected publication details from default `job get`, but those details are
  behind `--details` since the compact-output change. The assertions now request
  `--details`, matching the existing CLI contract.
- For monitor-list limits, focused behavioral tests cover newest-first
  truncation, unchanged unlimited output, and malformed, missing, duplicate,
  zero, negative, noninteger, and unknown arguments. `make help` is unavailable
  in this repository; the Makefile lists the supported targets directly.

### Historical symlink-`TMPDIR` test observations

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
remains an alias` on a removed CLI alias route; this assertion is stale because
`monitor describe` is rejected by current command routing), and
`TestBabysitListAndDescribePersistedMetadata` (expected persisted metadata
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

- **Status:** Implemented and merged in PR #6 (`feature/monitor-usability`);
  verification and review are complete.
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

- Monitor-list audit verification found 12 saved monitor records. For this
  snapshot, `CreatedAt` and `UpdatedAt` happened to be in the same order, so no
  user-visible ordering error was reproduced; source inspection confirmed that
  sorting and the displayed timestamp use different fields. The regression
  test uses deliberately conflicting times.

### Monitor inspection output is too verbose by default

- **Status:** Implemented on PR #11: default `factory monitor get <id>` shows
  identity, status, phase, latest successful PR/check query, and pending
  approval state without events or verbose metadata. `--details` retains the
  event trail, full record, job metadata, and logs. Focused and full tests, vet,
  and build passed; the change is merged to `main`.
- **Reproduction:** Run `factory monitor get <id>` and then
  `factory monitor get <id> --details` for an existing monitor.
- **Impact:** Routine status checks can expose more operational details than
  needed and make the key phase/check status harder to scan. The existing
  `--details` switch provides a natural place for full diagnostics.
- **Evidence:** `internal/factory/monitor.go` prints the concise status fields
  for both modes and writes recent events, marshaled job JSON, and job details
  only when `--details` is selected. `internal/factory/monitor_test.go` and
  `internal/factory/monitor_job_test.go` exercise both modes, including
  suppression and presence of the event trail and logs.
- **Desired outcome:** Keep default `monitor get` concise and scannable while
  retaining complete diagnostics behind `--details`.
- **Acceptance criteria:** Default output reports identity, status, phase,
  latest successful PR/check result, and pending approval state without the
  event trail or verbose fields; `--details` retains the full diagnostics.

### Job inspection hides implementation publication outcome

- **Status:** Implemented in `internal/factory/job.go` with a `PUBLICATION`
  column containing the outcome for implementation rows only. Rows without an
  outcome and non-implementation rows keep this cell blank. Behavioral tests
  cover `published`, `unpublished`, `no-op`, absent outcomes, both default
  commands, and the unchanged one-header/one-row `job get` shape. Verified by
  `make test`, `make vet`, and `make build`.
- **Reproduction:** Inspect completed implementation jobs with
  `factory job get <id>` and `factory job list`.
- **Impact:** Users cannot distinguish successfully published work from
  unpublished or no-op results without opening detailed output.

### Monitor inspection output is too verbose by default

- **Status:** Implemented on PR #11: default `factory monitor get <id>` shows
  identity, status, phase, latest successful PR/check query, and pending
  approval state without events or verbose metadata. `--details` retains the
  event trail, full record, job metadata, and logs. Focused and full tests, vet,
  and build passed; the change is merged to `main`.
- **Reproduction:** Run `factory monitor get <id>` and then
  `factory monitor get <id> --details` for an existing monitor.
- **Impact:** Routine status checks can expose more operational details than
  needed and make the key phase/check status harder to scan. The existing
  `--details` switch provides a natural place for full diagnostics.
- **Evidence:** `internal/factory/monitor.go` prints the concise status fields
  for both modes and writes recent events, marshaled job JSON, and job details
  only when `--details` is selected. `internal/factory/monitor_test.go` and
  `internal/factory/monitor_job_test.go` exercise both modes, including
  suppression and presence of the event trail and logs.
- **Desired outcome:** Keep default `monitor get` concise and scannable while
  retaining complete diagnostics behind `--details`.
- **Acceptance criteria:** Default output reports identity, status, phase,
  latest successful PR/check result, and pending approval state without the
  event trail or verbose fields; `--details` retains the full diagnostics.

### Other observed friction

- During PR CI monitoring, the root help output was broad.
- Monitor `describe` output contained a large JSON record and lengthy signatures
  and paths, making the output difficult to scan.
- A narrow test-only fix was reported as `FIXED`; subsequent macOS CI still
  failed in other path-comparison tests. This records the sequence only and
  does not attribute the failures to a cause.
- A monitor attempt was blocked with `target checkout is no longer clean`. No
  cause for that guard result is inferred here.
