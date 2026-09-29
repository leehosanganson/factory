# Archived session observations

These observations were recorded before findings and observations were split into independently maintained files. Historical claims and caveats are retained verbatim.

## Session observations

These are observations from a session, not claims about underlying causes.

- For issue 11, focused next-action tests passed. An initial `make test` run
  caught a CLI snapshot expectation for completed jobs; keeping the existing
  generic hint for implementation jobs without worktree metadata preserved the
  CLI output contract. After that adjustment, `make test`, `make vet`,
  `make build`, and `git diff --check` passed.

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
- The compact job-target display test covers unchanged short paths, a long path
  shortened with a visible ellipsis, and full path retention in `job get
  --details`. Focused tests, `make test`, `make vet`, `make build`, and
  `git diff --check` passed.
- For the macOS long-target-path regression, the test now derives its bounded
  display and details expectations from the path persisted by the store, and
  checks that it matches the canonical resolved input. Focused test,
  `make test`, `make vet`, `make build`, and `git diff --check` passed.

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
