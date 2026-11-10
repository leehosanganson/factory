# Codebase intuitiveness assessment — iteration 2: REST workflow configuration

- **Revision assessed:** `b85c4259d7a814dede5529544b578a356c32a086` (`b85c425`), commit `refactor(runtime): share workflow configuration setup`, based on `1e5832e83f96b0f0968e0e5aeba3b66dfc32ddb7`.
- **Date:** 2026-11-10
- **Evaluator:** Independent evaluator
- **Inspection depth / task context:** Same maintainer task as baseline. Representative source, callers, docs, and tests across all stable slices; runtime workflow-config helper inspected in detail. No tests were run by the evaluator; implementer and independent reviewer verification is recorded below.
- **Comparison base:** Shared source baseline `1e5832e83f96b0f0968e0e5aeba3b66dfc32ddb7`; this is an independent PR iteration parallel to iteration 1, not a follow-on commit containing its changes.
- **Baseline scorecard:** [`2026-11-10-1e5832e.md`](2026-11-10-1e5832e.md). Related parallel iteration record: [`2026-11-10-iteration-1-api-event-stream.md`](2026-11-10-iteration-1-api-event-stream.md), also based directly on the shared source baseline.

This is an evaluator judgment, not an objective metric. Scores are ordinal; confidence describes evidence depth.

## Scores and evidence

| ID | Score (1–5) | Confidence | Evidence and friction |
|---|---:|---|---|
| CLI | 3 | Medium | Unchanged. `README.md`, `cmd/factory/main.go`, and `internal/factory/` locate local workflows, but command routing remains broad and local jobs remain distinct from REST jobs. |
| WF | 4 | Medium | Unchanged. `internal/workflow/workflow.go`, `config.go`, and workflow tests give a clear shared workflow area; REST and local CLI callers use different adapters. The candidate centralizes configuration policy but does not alter workflow ownership. |
| API | 3 | Medium | Unchanged. API handler, tests, and OpenAPI tests locate the surface, while the handler and specialized files split concerns unevenly. Candidate does not touch API. |
| JOB | 3 | Medium | Unchanged. `internal/jobs/manager.go`, SQL stores, and `store_contract_test.go` are findable, but persistence behavior remains distributed across stores and migrations. |
| WORKER | 3 | Medium | Unchanged. `internal/worker/worker.go` and `factory_executor.go` show coordination/execution; the executor still crosses workflow, workspace, provider, and job state. |
| **RUNTIME** | **3** | **Medium** | `internal/server/runtime/runtime.go:run` and `worker.go:RunWorker` are the server/worker composition entry points. Both now call `newWorkflowConfig` in `runtime.go`, which owns workflow defaults, disables local publication/worktree placement, clones verification checks, and applies optional parallel settings. Focused tests cover defaults, disabled/omitted/enabled parallel settings, explicit/default concurrency, and deep-copy behavior. This centralizes a shared policy and avoids divergent configuration, but runtime still composes stores, providers, workspaces, listeners, workers, dispatchers, and shutdown. The slice remains Mixed/workable rather than Mostly clear. |
| WS | 3 | Medium | Unchanged. `internal/workspace/manager.go`, runtime/executor callers, and manager tests expose workspace ownership; attempt/recovery still crosses worker and job state. |
| INTAKE | 4 | Medium | Unchanged. Poller runtime and methods, tests, and its feature guide locate issue admission; several entry paths still converge on canonical job admission. |
| CLIENT | 3 | Medium | Unchanged. Client, web, frontend, and tests are findable, but UI behavior can span CLI/client/API/web/generated assets. |
| **Overall** | **3** | **Medium** | No slice or overall score increase is justified: the helper makes one policy easier to find and keeps two callers consistent, but does not materially alter cross-slice navigation or runtime composition. Overall is assessed independently, not averaged. |

## Iteration comparison

- **Refactoring iteration / PR(s):** Iteration 2, REST server/worker workflow-config centralization; [PR #372](https://github.com/leehosanganson/factory/pull/372), open at the time this scorecard was recorded.
- **Slices directly affected:** RUNTIME (with the shared workflow configuration boundary).
- **Scores changed and evidence for each change:** None. Runtime and overall remain 3; shared policy is easier to locate, but broader composition remains.
- **Scores unchanged or lower and why:** All other slices remain at baseline; their code is unchanged.
- **New friction, regressions, or unresolved ownership questions:** None evident in the independent review. Server and worker retain distinct resource, executor, persistence callback, and lifecycle logic.
- **Next refactoring hypothesis:** Inspect the WS score-3 evidence for a concrete navigation task before implementing. Candidate discovery suggests source-revision capture/materialization may be a bounded responsibility, but a file split alone is insufficient reason; validate that the same task is observably difficult first.

## Implementer verification and independent review

- Focused `go test ./internal/server/runtime` — passed.
- `make test` — passed (`go test ./...`).
- `make vet` — passed (`go vet ./...`).
- `make build` — passed.
- `git diff --check` — passed.
- Independent review ran `go test ./internal/server/runtime` and `git diff --check`; both passed and no actionable findings were reported.

## Limitations

The evaluator assessed the working-tree diff before commit; commit `b85c425` contains that reviewed and tested diff. The evaluator used representative paths, not a full-file audit. The main agent reran the checks above before delivery.
