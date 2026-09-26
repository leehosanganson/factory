# Usability issues

This note records four user-reported findings and factual observations from a
session. Findings remain separate; observations do not imply root causes or set
priorities. Status refers to the actual CLI/worker implementation, not to
intentions or documentation alone.

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
- **Status:** Implemented in `.agents/skills/factory-usage/SKILL.md`; multiple
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

- **Finding:** Show/describe output is difficult to scan when long strings and
  detailed records are inline.
- **Desired outcome:** Concise tables by default, with long values and full
  records available through a details option.
- **Status:** Partially implemented in the current CLI source: list views are
  tabulated; individual `get` commands default to concise summaries; and
  `--details` includes long descriptions, records, proposals, and logs. The
  individual summaries are labeled fields rather than tables, so the agreed
  tabular-default design is incomplete. The integrated implementation is
  verified by the current `make test` and `make vet` checks.

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

### Other observed friction

- During PR CI monitoring, the root help output was broad.
- Monitor `describe` output contained a large JSON record and lengthy signatures
  and paths, making the output difficult to scan.
- A narrow test-only fix was reported as `FIXED`; subsequent macOS CI still
  failed in other path-comparison tests. This records the sequence only and
  does not attribute the failures to a cause.
- A monitor attempt was blocked with `target checkout is no longer clean`. No
  cause for that guard result is inferred here.
