# REST workspace context-aware creation session observation

- `internal/restworkspace.Manager.CreateContext` now receives the caller's deadline through the injected Git runner for `git worktree add`; `Create` remains a background-context compatibility wrapper.
- Tests cover cancellation before result-directory creation, cancellation reported after Git has created a worktree, and successful creation with a live context. The cancellation case retains uncertain results and does not create completion metadata.
- The task started from a separate worktree at freshly fetched `origin/main`, preserving unrelated roadmap and observation changes in the original checkout. PR #78 was already merged; open PRs #79 and #80 had no overlapping workspace files.

These are task-session observations, not user-reported issues.
