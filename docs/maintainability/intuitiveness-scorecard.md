# Codebase intuitiveness scorecard

This scorecard is a repeatable, evaluator-rated baseline for the question: **How easy is it for a maintainer unfamiliar with a specific area to locate its responsibilities, follow a representative behavior path, and identify the right tests?**

It is a qualitative diagnostic, not a usability study, code-quality grade, or automated metric. Use the same rubric and slice boundaries after each completed refactoring iteration. Record evidence and confidence; do not treat small score movements as meaningful without concrete evidence.

## Evaluation protocol

1. Evaluate a named revision (commit SHA) from a clean checkout. Record evaluator, date, and any relevant task context.
2. Use the same maintainer task for each slice: **Starting from the repository structure and canonical docs, locate the slice's main entry point, follow one representative behavior through its key dependencies, and find the tests that protect it.**
3. Inspect source, callers, tests, and relevant docs. Record paths and symbols that support the score, including specific points of friction or clarity. Do not rate from filenames or directory structure alone.
4. Score each slice independently using the anchored rubric below. Then score the overall codebase based on cross-slice navigation and the largest material ownership/coupling obstacles; do not average slice scores mechanically.
5. Assign confidence: **High** when source, callers, docs, and tests were examined; **Medium** when representative paths were examined but coverage is incomplete; **Low** when evidence is mainly structural or indirect. Low confidence requires an explicit limitation.
6. Compare with the relevant prior scorecard. Sequential iterations compare against the immediately preceding refactored revision. Parallel PRs based on the same source revision must each compare against that shared base; do not imply one contains another. State the comparison base explicitly. A score change must cite changed evidence. A numerical increase without evidence is not an improvement claim. Keep slice definitions stable; if boundaries genuinely change, document the change and score the old/new boundaries for one transition iteration.

For a comparable assessment, use one evaluator and equivalent inspection depth where practical. Independent evaluators may also score separately; retain disagreements and evidence rather than averaging away meaningful differences.

## Anchored 1–5 rubric

| Score | Anchor | Observable guidance |
|---:|---|---|
| 1 | Very difficult | Responsibilities or ownership are hard to locate; a representative flow requires substantial guesswork or broad searches; tests do not provide a clear behavioral entry point. |
| 2 | Difficult | Some boundaries are visible, but important behavior is scattered, central files/interfaces obscure ownership, or it is unclear which tests protect the contract. |
| 3 | Mixed / workable | Main entry points and tests can be found, but material cross-cutting dependencies, indirection, or broad files make some changes hard to reason about. |
| 4 | Mostly clear | Responsibilities, representative flows, and contract tests are easy to locate; coupling is explicit and usually localized. |
| 5 | Very clear | A maintainer can quickly explain ownership, trace representative behavior, identify preserved invariants and tests, with little unnecessary indirection or hidden coupling. |

Scores are ordinal judgments. Do not interpret a difference such as 3.2 versus 3.4 as precise measurement.

## Stable vertical slices

Keep these slices fixed across iterations unless the evaluation-boundary rule above applies.

| ID | Slice | Starting scope |
|---|---|---|
| CLI | CLI routing and local workflows/jobs | `cmd/factory/`; `internal/factory/` |
| WF | Shared workflow execution | `internal/workflow/`, including its local CLI and REST-worker call boundaries |
| API | REST API | `internal/api/`, including authentication, routing, endpoint behavior, projections and relevant API tests |
| JOB | Job domain and persistence | `internal/jobs/`, including in-memory, SQLite and PostgreSQL implementations and shared store-contract tests |
| WORKER | Worker execution and provider effects | `internal/worker/`, `internal/provider/`, and their job/workspace/workflow integration points |
| RUNTIME | Server composition and lifecycle | `internal/server/`, especially `internal/server/runtime/` |
| WS | Workspace lifecycle | `internal/workspace/` and its worker/runtime callers |
| INTAKE | Issue intake and polling | `internal/issue/`, `internal/issue/poller/`, and canonical job admission integration |
| CLIENT | REST client and operator dashboard | `internal/client/`, `internal/web/`, and `frontend/` including generated-asset boundary |

When scoring cross-slice intuitiveness, consider top-level docs and package navigation in addition to each slice's source. Avoid double-counting one issue as proof that every adjacent slice has the same problem; explain its impact at each boundary.

## Scorecard template

Copy this section for every evaluation. Keep completed scorecards under `docs/maintainability/assessments/` with a date and short revision identifier in the filename, e.g. `2026-01-15-1e5832e.md`. Store the rubric and scorecard updates on the dedicated scorecard documentation branch/worktree, not in source-refactor branches, so code PRs remain independently reviewable.

### Evaluation metadata

- Revision:
- Date:
- Evaluator(s):
- Inspection depth / task context:
- Prior scorecard (or `baseline`):

### Scores and evidence

For each row, give a score from 1–5, confidence (High/Medium/Low), and concise evidence: representative paths/symbols, what was easy to understand, and the main friction. Include tests and docs considered where relevant.

| ID | Score (1–5) | Confidence | Evidence and friction |
|---|---:|---|---|
| CLI | | | |
| WF | | | |
| API | | | |
| JOB | | | |
| WORKER | | | |
| RUNTIME | | | |
| WS | | | |
| INTAKE | | | |
| CLIENT | | | |
| **Overall** | | | Rate cross-slice navigation/ownership; explain why, do not mechanically average. |

### Iteration comparison

- Refactoring iteration / PR(s):
- Slices directly affected:
- Scores changed and evidence for each change:
- Scores unchanged or lower and why:
- New friction, regressions, or unresolved ownership questions:
- Next refactoring hypothesis (not an implementation commitment):

## Baseline

The first scorecard is recorded at [`assessments/2026-11-10-1e5832e.md`](assessments/2026-11-10-1e5832e.md) against baseline revision `1e5832e83f96b0f0968e0e5aeba3b66dfc32ddb7`, before any refactoring iteration. Iteration records use the same stable rubric; see [`assessments/2026-11-10-iteration-1-api-event-stream.md`](assessments/2026-11-10-iteration-1-api-event-stream.md) and [`assessments/2026-11-10-iteration-2-runtime-workflow-config.md`](assessments/2026-11-10-iteration-2-runtime-workflow-config.md). Both code PRs are open; no merge is authorized. The prior conversation's rough estimates are not the canonical baseline: they used a different evidence pass and did not record per-slice confidence or complete supporting evidence. Preserve the baseline unchanged for subsequent comparisons.

## Use after each refactoring iteration

A refactoring iteration is one completed, reviewable slice/PR (or a clearly bounded set delivered together). At its completion:

- Re-evaluate all stable slices at comparable depth, not only the edited slice, to detect navigation or coupling regressions.
- Pay particular attention to directly changed slices and their callers/tests.
- Record unchanged scores when evidence indicates no intuitiveness change; do not force a score increase.
- Do not combine unrelated refactorings merely to improve a score. Prioritize changes based on concrete friction in the evidence.
- Treat the scorecard as an anchor for planning the next iteration, not as an optimization target that can be gamed by moving files or shrinking line counts.
