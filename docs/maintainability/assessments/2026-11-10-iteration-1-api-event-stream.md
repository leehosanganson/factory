# Codebase intuitiveness assessment — iteration 1: API event stream

- **Revision assessed:** `098430055ebc385bc7af40846b20027ce6e1195f` (`0984300`), commit `refactor(api): isolate event stream handlers and tests`, based on `1e5832e83f96b0f0968e0e5aeba3b66dfc32ddb7`.
- **Date:** 2026-11-10
- **Evaluator:** Independent evaluator (same evaluator as baseline)
- **Inspection depth / task context:** Same maintainer navigation task as baseline, representative source/callers/docs/tests for all stable slices, with deeper inspection of the changed API path. No tests were run by the evaluator; implementer verification is recorded below.
- **Comparison base:** Source baseline `1e5832e83f96b0f0968e0e5aeba3b66dfc32ddb7`.
- **Baseline scorecard:** [`2026-11-10-1e5832e.md`](2026-11-10-1e5832e.md)

This is a qualitative evaluator judgment, not an objective metric. Scores are ordinal. Confidence describes evidence depth, not probability that a score is correct.

## Scores and evidence

| ID | Score (1–5) | Confidence | Evidence and friction |
|---|---:|---|---|
| CLI | 3 | Medium | Unchanged from baseline. `cmd/factory/main.go:run` and `runPipelineContext`, `internal/factory.StartImplementationJob`, and `cmd/factory/main_test.go` locate main local paths. The central command router remains broad and local jobs remain distinct from REST jobs. |
| WF | 4 | High | Unchanged. `internal/workflow.Workflow.RunContext`, `Runner.RunContext`, and workflow tests remain clear entry points. CLI and REST worker adapters still differ. |
| API | 3 | Medium | `internal/api/event_stream.go:streamEvents` and `streamEventsForPrincipal` now group stream behavior; `internal/api/event_stream_test.go` contains all nine `TestJobEventStream*` cases plus stream-only test helpers. Tests visibly cover ordered replay, reconnect, SQLite, cursor validation/expiry, truncation/reset, authentication/capacity. Route dispatch remains in `handler.go`, and `safeEventType` remains shared with history. The file move improves locating this particular concern, but broader API responsibilities remain in `handler.go`; no slice-level score increase is supported. |
| JOB | 3 | High | Unchanged. `internal/jobs/manager.go` contracts and `store_contract_test.go` provide entry points, while behavior remains distributed across in-memory and SQL stores. |
| WORKER | 3 | High | Unchanged. Coordinator/executor entry points and worker/executor tests remain findable; a job still crosses several packages. |
| RUNTIME | 3 | Medium | Unchanged. `runtime.go:run`, `worker.go:RunWorker`, runtime tests, and docs locate startup paths, but composition and lifecycle still require following multiple integrations. |
| WS | 3 | Medium | Unchanged. `workspace.Manager` creation methods, manager tests, and executor callers expose the core; attempt/recovery behavior still spans worker and job state. |
| INTAKE | 4 | High | Unchanged. Poller `Run`, `PollOnce`, `dispatch`, tests, and feature docs provide a relatively clear path; manual, polling, and webhook entry paths remain distinct. |
| CLIENT | 3 | Medium | Unchanged. Client, CLI remote commands, web handler, frontend and tests remain locatable, but UI changes still cross multiple surfaces and generated assets. |
| **Overall** | **3** | **Medium** | Unchanged. The event-stream path is easier to locate, but this one API extraction does not materially change larger cross-slice flow tracing or ownership costs. Overall is judged independently, not averaged from slice scores. |

## Iteration comparison

- **Refactoring iteration / PR(s):** Iteration 1, API event-stream source/test extraction; [PR #371](https://github.com/leehosanganson/factory/pull/371), open at the time this scorecard was recorded.
- **Slices directly affected:** API.
- **Scores changed and evidence for each change:** None. API and overall remain 3; locating the event-stream behavior improved, but the evaluator did not find enough slice-wide or system-wide change to move an ordinal score.
- **Scores unchanged or lower and why:** All other slice scores are unchanged; their ownership and representative paths were not modified. No lower score or regression was identified.
- **New friction, regressions, or unresolved ownership questions:** Route dispatch remains in `handler.go`; `safeEventType` is shared with history and remains there. These were considered appropriate shared boundaries for this iteration. No material regression was identified by diff review.
- **Next refactoring hypothesis (not an implementation commitment):** Continue to use scored evidence to choose a bounded boundary. Revisit the API handler's remaining concerns only if a concrete maintainer task shows route/authorization/projection navigation is still difficult; alternatively evaluate JOB persistence ownership, where baseline evidence reports distributed memory/SQLite/PostgreSQL behavior and shared contract constraints.

## Implementer verification

- `go test ./internal/api -run '^TestJobEventStream' -count=1` — passed.
- `go test ./internal/api` — passed.
- `make test` — passed (`go test ./...`).
- `make vet` — passed (`go vet ./...`).
- `make build` — passed.
- `git diff --check` — passed.
- Independent code review found no actionable issue. Evaluator tests were inspected, not run.

## Limitations

The assessor evaluated the working-tree diff before commit; the resulting commit `0984300` contains that reviewed and tested diff. Unchanged slices were assessed at representative depth rather than a full-file audit. The review found no behavior change by inspecting the diff; test results above are from the implementer.
