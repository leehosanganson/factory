# Session observations — 2026-10-09

- **Verified observation:** The existing capacity regression test launches two independent CLI processes against different repositories and a shared state directory; with a cap of one, exactly one start succeeds, only one job/agent invocation is recorded, and a subsequent start succeeds after that job reaches a terminal state.
- **Friction:** No issue-specific requirement checklist was present in the worktree, so acceptance was assessed against the task scope and traced admission/configuration paths plus existing tests.
- **Improvement idea:** Keep a concise, canonical issue acceptance checklist available alongside cross-cutting concurrency changes so reviewers can map each criterion to its test or documentation evidence quickly.
