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

## Session observations

These are observations from a session, not claims about underlying causes.

- Focused monitor tests plus `make vet` and `make build` succeeded. A full test
  run exposed stale assertions in
  `TestDetachedImplementationPublicationOutcomePersistsAndCleansOnlyOnPublish`:
  it expected publication details from default `job get`, but those details are
  behind `--details` since the compact-output change. The assertions now request
  `--details`, matching the existing CLI contract.

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

### Monitor inspection output is too verbose by default

- **Status:** Implementation and focused regression tests are complete:
  default `factory monitor get <id>` shows identity, status, phase, latest
  successful PR/check query, and actionable pending approval state without
  events or verbose metadata. `--details` retains the event trail, full JSON
  record, job metadata, and logs. Focused monitor tests, `make vet`, and
  `make build` pass. Stale job-output assertions were updated to use
  `--details`; the full suite will be rerun before publication.
- **Reproduction:** Run `factory monitor get <id>` and then
  `factory monitor get <id> --details` for an existing monitor.
- **Impact:** Routine status checks can expose more operational details than
  needed and make the key phase/check status harder to scan. The existing
  `--details` switch provides a natural place for full diagnostics.
- **Evidence:** `internal/factory/monitor.go` prints the concise status fields
  for both modes and writes recent events, marshaled job JSON, and job details
  only when `--details` is selected. `internal/factory/monitor_test.go` and
  `internal/factory/monitor_job_test.go` exercise the default and detailed
  outputs, including suppression and presence of event trail and logs.
- **Desired outcome:** Keep default `monitor get` concise and scannable while
  retaining complete diagnostics behind `--details`.
- **Acceptance criteria:** Default output reports identity, status, phase,
  latest successful PR/check result, and actionable pending approval state
  without dumping recent events or verbose fields; `--details` retains the
  current event trail, full record, and metadata/logs; regression tests cover
  both modes; `make test`, `make vet`, and `make build` pass.

### Other observed friction

- During PR CI monitoring, the root help output was broad.
- Monitor `describe` output contained a large JSON record and lengthy signatures
  and paths, making the output difficult to scan.
- A narrow test-only fix was reported as `FIXED`; subsequent macOS CI still
  failed in other path-comparison tests. This records the sequence only and
  does not attribute the failures to a cause.
- A monitor attempt was blocked with `target checkout is no longer clean`. No
  cause for that guard result is inferred here.
